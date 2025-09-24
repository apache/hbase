/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.crypto;

import static org.apache.hadoop.hbase.io.crypto.KeymetaTestUtils.PASSWORD;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider.KEY_METADATA_ALIAS;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider.KEY_METADATA_CUST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestManagedKeyProvider.TestManagedKeyStoreKeyProvider.class,
  TestManagedKeyProvider.TestManagedKeyProviderDefault.class, })
@Category({ MiscTests.class, SmallTests.class })
public class TestManagedKeyProvider {
  @RunWith(Parameterized.class)
  @Category({ MiscTests.class, SmallTests.class })
  public static class TestManagedKeyStoreKeyProvider {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestManagedKeyStoreKeyProvider.class);

    private static final String SYSTEM_KEY_ALIAS = "system-alias";

    static final HBaseCommonTestingUtil TEST_UTIL = new HBaseCommonTestingUtil();
    static byte[] KEY;

    @Parameterized.Parameter(0)
    public boolean withPasswordOnAlias;
    @Parameterized.Parameter(1)
    public boolean withPasswordFile;

    @Parameterized.Parameters(name = "withPasswordOnAlias={0} withPasswordFile={1}")
    public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][] {
        { Boolean.TRUE, Boolean.TRUE },
        { Boolean.TRUE, Boolean.FALSE },
        { Boolean.FALSE, Boolean.TRUE },
        { Boolean.FALSE, Boolean.FALSE },
      });
    }

    // TestManagedKeyStoreKeyProvider specific fields
    private Configuration conf = HBaseConfiguration.create();
    private int nCustodians = 2;
    private ManagedKeyProvider managedKeyProvider;
    private Map<Bytes, Bytes> cust2key = new HashMap<>();
    private Map<Bytes, String> cust2alias = new HashMap<>();
    private String clusterId;
    private byte[] systemKey;

    @Before
    public void setUp() throws Exception {
      String providerParams = KeymetaTestUtils.setupTestKeyStore(TEST_UTIL, withPasswordOnAlias,
        withPasswordFile, store -> { return new Properties(); });
      managedKeyProvider = (ManagedKeyProvider) createProvider();
      managedKeyProvider.initConfig(conf, providerParams);
    }

    protected KeyProvider createProvider() {
      return new ManagedKeyStoreKeyProvider();
    }

    protected void addCustomEntries(KeyStore store, Properties passwdProps) throws Exception {
      // TestManagedKeyStoreKeyProvider specific entries
      for (int i = 0; i < nCustodians; ++i) {
        String custodian = "custodian+ " + i;
        String alias = custodian + "-alias";
        KeymetaTestUtils.addEntry(conf, 256, store, alias, custodian, withPasswordOnAlias, cust2key,
          cust2alias, passwdProps);
      }

      clusterId = UUID.randomUUID().toString();
      KeymetaTestUtils.addEntry(conf, 256, store, SYSTEM_KEY_ALIAS, clusterId, withPasswordOnAlias,
        cust2key, cust2alias, passwdProps);
      systemKey = cust2key.get(new Bytes(clusterId.getBytes())).get();
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY, SYSTEM_KEY_ALIAS);

      KeymetaTestUtils.addEntry(conf, 256, store, "global-cust-alias", "*", withPasswordOnAlias,
        cust2key, cust2alias, passwdProps);
    }

    @Test
    public void testMissingConfig() throws Exception {
      managedKeyProvider.initConfig(null, null);
      RuntimeException ex =
        assertThrows(RuntimeException.class, () -> managedKeyProvider.getSystemKey(null));
      assertEquals("initConfig is not called or config is null", ex.getMessage());
    }

    @Test
    public void testGetManagedKey() throws Exception {
      for (Bytes cust : cust2key.keySet()) {
        ManagedKeyData keyData =
          managedKeyProvider.getManagedKey(cust.get(), ManagedKeyData.KEY_SPACE_GLOBAL);
        assertKeyData(keyData, ManagedKeyState.ACTIVE, cust2key.get(cust).get(), cust.get(),
          cust2alias.get(cust));
      }
    }

    @Test
    public void testGetGlobalCustodianKey() throws Exception {
      byte[] globalCustodianKey = cust2key.get(new Bytes(KEY_GLOBAL_CUSTODIAN_BYTES)).get();
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(KEY_GLOBAL_CUSTODIAN_BYTES,
        ManagedKeyData.KEY_SPACE_GLOBAL);
      assertKeyData(keyData, ManagedKeyState.ACTIVE, globalCustodianKey, KEY_GLOBAL_CUSTODIAN_BYTES,
        "global-cust-alias");
    }

    @Test
    public void testGetInactiveKey() throws Exception {
      Bytes firstCust = cust2key.keySet().iterator().next();
      String encCust = Base64.getEncoder().encodeToString(firstCust.get());
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encCust + ".active", "false");
      ManagedKeyData keyData =
        managedKeyProvider.getManagedKey(firstCust.get(), ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.INACTIVE, cust2key.get(firstCust).get(),
        firstCust.get(), cust2alias.get(firstCust));
    }

    @Test
    public void testGetInvalidKey() throws Exception {
      byte[] invalidCustBytes = "invalid".getBytes();
      ManagedKeyData keyData =
        managedKeyProvider.getManagedKey(invalidCustBytes, ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.FAILED, null, invalidCustBytes, null);
    }

    @Test
    public void testGetDisabledKey() throws Exception {
      byte[] invalidCust = new byte[] { 1, 2, 3 };
      String invalidCustEnc = ManagedKeyProvider.encodeToStr(invalidCust);
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + invalidCustEnc + ".active",
        "false");
      ManagedKeyData keyData =
        managedKeyProvider.getManagedKey(invalidCust, ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.DISABLED, null, invalidCust, null);
    }

    @Test
    public void testGetSystemKey() throws Exception {
      ManagedKeyData clusterKeyData = managedKeyProvider.getSystemKey(clusterId.getBytes());
      assertKeyData(clusterKeyData, ManagedKeyState.ACTIVE, systemKey, clusterId.getBytes(),
        SYSTEM_KEY_ALIAS);
      conf.unset(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY);
      RuntimeException ex =
        assertThrows(RuntimeException.class, () -> managedKeyProvider.getSystemKey(null));
      assertEquals("No alias configured for system key", ex.getMessage());
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY, "non_existing_alias");
      ex = assertThrows(RuntimeException.class, () -> managedKeyProvider.getSystemKey(null));
      assertTrue(ex.getMessage().startsWith("Unable to find system key with alias:"));
    }

    @Test
    public void testUnwrapInvalidKey() throws Exception {
      String invalidAlias = "invalidAlias";
      byte[] invalidCust = new byte[] { 1, 2, 3 };
      String invalidCustEnc = ManagedKeyProvider.encodeToStr(invalidCust);
      String invalidMetadata =
        ManagedKeyStoreKeyProvider.generateKeyMetadata(invalidAlias, invalidCustEnc);
      ManagedKeyData keyData = managedKeyProvider.unwrapKey(invalidMetadata, null);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.FAILED, null, invalidCust, invalidAlias);
    }

    @Test
    public void testUnwrapDisabledKey() throws Exception {
      String invalidAlias = "invalidAlias";
      byte[] invalidCust = new byte[] { 1, 2, 3 };
      String invalidCustEnc = ManagedKeyProvider.encodeToStr(invalidCust);
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + invalidCustEnc + ".active",
        "false");
      String invalidMetadata =
        ManagedKeyStoreKeyProvider.generateKeyMetadata(invalidAlias, invalidCustEnc);
      ManagedKeyData keyData = managedKeyProvider.unwrapKey(invalidMetadata, null);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.DISABLED, null, invalidCust, invalidAlias);
    }

    private void assertKeyData(ManagedKeyData keyData, ManagedKeyState expKeyState, byte[] key,
      byte[] custBytes, String alias) throws Exception {
      assertNotNull(keyData);
      assertEquals(expKeyState, keyData.getKeyState());
      if (key == null) {
        assertNull(keyData.getTheKey());
      } else {
        byte[] keyBytes = keyData.getTheKey().getEncoded();
        assertEquals(key.length, keyBytes.length);
        assertEquals(new Bytes(key), keyBytes);
      }
      Map keyMetadata =
        GsonUtil.getDefaultInstance().fromJson(keyData.getKeyMetadata(), HashMap.class);
      assertNotNull(keyMetadata);
      assertEquals(new Bytes(custBytes), keyData.getKeyCustodian());
      assertEquals(alias, keyMetadata.get(KEY_METADATA_ALIAS));
      assertEquals(Base64.getEncoder().encodeToString(custBytes),
        keyMetadata.get(KEY_METADATA_CUST));
      assertEquals(keyData, managedKeyProvider.unwrapKey(keyData.getKeyMetadata(), null));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MiscTests.class, SmallTests.class })
  public static class TestManagedKeyProviderDefault {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestManagedKeyProviderDefault.class);

    @Test
    public void testEncodeToStr() {
      byte[] input = { 72, 101, 108, 108, 111 }; // "Hello" in ASCII
      String expected = "SGVsbG8=";
      String actual = ManagedKeyProvider.encodeToStr(input);

      assertEquals("Encoded string should match expected Base64 representation", expected, actual);
    }

    @Test
    public void testDecodeToBytes() throws Exception {
      String input = "SGVsbG8="; // "Hello" in Base64
      byte[] expected = { 72, 101, 108, 108, 111 };
      byte[] actual = ManagedKeyProvider.decodeToBytes(input);

      assertTrue("Decoded bytes should match expected ASCII representation",
        Arrays.equals(expected, actual));
    }

    @Test
    public void testEncodeToStrAndDecodeToBytes() throws Exception {
      byte[] originalBytes = { 1, 2, 3, 4, 5 };
      String encoded = ManagedKeyProvider.encodeToStr(originalBytes);
      byte[] decoded = ManagedKeyProvider.decodeToBytes(encoded);

      assertTrue("Decoded bytes should match original bytes",
        Arrays.equals(originalBytes, decoded));
    }

    @Test(expected = Exception.class)
    public void testDecodeToBytes_InvalidInput() throws Exception {
      String invalidInput = "This is not a valid Base64 string!";
      ManagedKeyProvider.decodeToBytes(invalidInput);
    }

    @Test
    public void testRoundTrip_LargeInput() throws Exception {
      byte[] largeInput = new byte[1000];
      for (int i = 0; i < largeInput.length; i++) {
        largeInput[i] = (byte) (i % 256);
      }

      String encoded = ManagedKeyProvider.encodeToStr(largeInput);
      byte[] decoded = ManagedKeyProvider.decodeToBytes(encoded);

      assertTrue("Large input should survive round-trip encoding and decoding",
        Arrays.equals(largeInput, decoded));
    }
  }
}
