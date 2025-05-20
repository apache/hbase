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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
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
import javax.crypto.spec.SecretKeySpec;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider.KEY_METADATA_ALIAS;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider.KEY_METADATA_CUST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestManagedKeyProvider.TestManagedKeyStoreKeyProvider.class,
  TestManagedKeyProvider.TestManagedKeyProviderDefault.class, })
@Category({ MiscTests.class, SmallTests.class })
public class TestManagedKeyProvider {
  @RunWith(Parameterized.class)
  @Category({ MiscTests.class, SmallTests.class })
  public static class TestManagedKeyStoreKeyProvider extends TestKeyStoreKeyProvider {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestManagedKeyStoreKeyProvider.class);

    private static final String SYSTEM_KEY_ALIAS = "system-alias";

    private Configuration conf = HBaseConfiguration.create();
    private int nPrefixes = 2;
    private ManagedKeyProvider managedKeyProvider;
    private Map<Bytes, Bytes> prefix2key = new HashMap<>();
    private Map<Bytes, String> prefix2alias = new HashMap<>();
    private String clusterId;
    private byte[] systemKey;

    @Before
    public void setUp() throws Exception {
      super.setUp();;
      managedKeyProvider = (ManagedKeyProvider) provider;
      managedKeyProvider.initConfig(conf);
    }

    protected KeyProvider createProvider() {
      return new ManagedKeyStoreKeyProvider();
    }

    protected void addCustomEntries(KeyStore store, Properties passwdProps) throws Exception {
      super.addCustomEntries(store, passwdProps);
      for (int i = 0; i < nPrefixes; ++i) {
        String prefix = "prefix+ " + i;
        String alias = prefix + "-alias";
        byte[] key = MessageDigest.getInstance("SHA-256").digest(Bytes.toBytes(alias));
        prefix2alias.put(new Bytes(prefix.getBytes()), alias);
        prefix2key.put(new Bytes(prefix.getBytes()), new Bytes(key));
        store.setEntry(alias, new KeyStore.SecretKeyEntry(new SecretKeySpec(key, "AES")),
          new KeyStore.PasswordProtection(
            withPasswordOnAlias ? PASSWORD.toCharArray() : new char[0]));

        String encPrefix = Base64.getEncoder().encodeToString(prefix.getBytes());
        String confKey = HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encPrefix + "."
          + "alias";
        conf.set(confKey, alias);

        passwdProps.setProperty(alias, PASSWORD);

        clusterId = UUID.randomUUID().toString();
        systemKey = MessageDigest.getInstance("SHA-256").digest(
          Bytes.toBytes(SYSTEM_KEY_ALIAS));
        store.setEntry(SYSTEM_KEY_ALIAS, new KeyStore.SecretKeyEntry(
            new SecretKeySpec(systemKey, "AES")),
          new KeyStore.PasswordProtection(withPasswordOnAlias ? PASSWORD.toCharArray() :
            new char[0]));

        conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY, SYSTEM_KEY_ALIAS);

        passwdProps.setProperty(SYSTEM_KEY_ALIAS, PASSWORD);
      }
    }

    private void addEntry(String alias, String prefix) {
      String encPrefix = Base64.getEncoder().encodeToString(prefix.getBytes());
      String confKey = HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encPrefix + "."
      + "alias";
      conf.set(confKey, alias);
    }

    @Test
    public void testMissingConfig() throws Exception {
      managedKeyProvider.initConfig(null);
      RuntimeException ex = assertThrows(RuntimeException.class,
        () -> managedKeyProvider.getSystemKey(null));
      assertEquals("initConfig is not called or config is null", ex.getMessage());
    }

    @Test
    public void testGetManagedKey() throws Exception {
      for (Bytes prefix : prefix2key.keySet()) {
        ManagedKeyData keyData = managedKeyProvider.getManagedKey(prefix.get(),
          ManagedKeyData.KEY_SPACE_GLOBAL);
        assertKeyData(keyData, ManagedKeyState.ACTIVE, prefix2key.get(prefix).get(), prefix.get(),
          prefix2alias.get(prefix));
      }
    }

    @Test
    public void testGetInactiveKey() throws Exception {
      Bytes firstPrefix = prefix2key.keySet().iterator().next();
      String encPrefix = Base64.getEncoder().encodeToString(firstPrefix.get());
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encPrefix + ".active",
        "false");
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(firstPrefix.get(),
        ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.INACTIVE, prefix2key.get(firstPrefix).get(),
        firstPrefix.get(), prefix2alias.get(firstPrefix));
    }

    @Test
    public void testGetInvalidKey() throws Exception {
      byte[] invalidPrefixBytes = "invalid".getBytes();
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(invalidPrefixBytes,
        ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.FAILED, null, invalidPrefixBytes, null);
    }

    @Test
    public void testGetDisabledKey() throws Exception {
      byte[] invalidPrefix = new byte[] { 1, 2, 3 };
      String invalidPrefixEnc = ManagedKeyProvider.encodeToStr(invalidPrefix);
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + invalidPrefixEnc + ".active",
        "false");
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(invalidPrefix,
        ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.DISABLED, null,
        invalidPrefix, null);
    }

    @Test
    public void testGetSystemKey() throws Exception {
      ManagedKeyData clusterKeyData = managedKeyProvider.getSystemKey(clusterId.getBytes());
      assertKeyData(clusterKeyData, ManagedKeyState.ACTIVE, systemKey, clusterId.getBytes(),
        SYSTEM_KEY_ALIAS);
      conf.unset(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY);
      RuntimeException ex = assertThrows(RuntimeException.class,
        () -> managedKeyProvider.getSystemKey(null));
      assertEquals("No alias configured for system key", ex.getMessage());
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY, "non_existing_alias");
      ex = assertThrows(RuntimeException.class,
        () -> managedKeyProvider.getSystemKey(null));
      assertTrue(ex.getMessage().startsWith("Unable to find system key with alias:"));
    }

    @Test
    public void testUnwrapInvalidKey() throws Exception {
      String invalidAlias = "invalidAlias";
      byte[] invalidPrefix = new byte[] { 1, 2, 3 };
      String invalidPrefixEnc = ManagedKeyProvider.encodeToStr(invalidPrefix);
      String invalidMetadata = ManagedKeyStoreKeyProvider.generateKeyMetadata(invalidAlias,
        invalidPrefixEnc);
      ManagedKeyData keyData = managedKeyProvider.unwrapKey(invalidMetadata);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.FAILED, null, invalidPrefix,
        invalidAlias);
    }

    @Test
    public void testUnwrapDisabledKey() throws Exception {
      String invalidAlias = "invalidAlias";
      byte[] invalidPrefix = new byte[] { 1, 2, 3 };
      String invalidPrefixEnc = ManagedKeyProvider.encodeToStr(invalidPrefix);
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + invalidPrefixEnc + ".active",
        "false");
      String invalidMetadata = ManagedKeyStoreKeyProvider.generateKeyMetadata(invalidAlias,
        invalidPrefixEnc);
      ManagedKeyData keyData = managedKeyProvider.unwrapKey(invalidMetadata);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.DISABLED, null, invalidPrefix, invalidAlias);
    }

    private void assertKeyData(ManagedKeyData keyData, ManagedKeyState expKeyState, byte[] key,
      byte[] prefixBytes, String alias) throws Exception {
      assertNotNull(keyData);
      assertEquals(expKeyState, keyData.getKeyState());
      if (key == null) {
        assertNull(keyData.getTheKey());
      }
      else {
        byte[] keyBytes = keyData.getTheKey().getEncoded();
        assertEquals(key.length, keyBytes.length);
        assertEquals(new Bytes(key), keyBytes);
      }
      Map keyMetadata = GsonUtil.getDefaultInstance().fromJson(keyData.getKeyMetadata(),
        HashMap.class);
      assertNotNull(keyMetadata);
      assertEquals(new Bytes(prefixBytes), keyData.getKeyCustodian());
      assertEquals(alias, keyMetadata.get(KEY_METADATA_ALIAS));
      assertEquals(Base64.getEncoder().encodeToString(prefixBytes),
        keyMetadata.get(KEY_METADATA_CUST));
      assertEquals(keyData, managedKeyProvider.unwrapKey(keyData.getKeyMetadata()));
    }
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MiscTests.class, SmallTests.class })
  public static class TestManagedKeyProviderDefault {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestManagedKeyProviderDefault.class);

    @Test public void testEncodeToStr() {
      byte[] input = { 72, 101, 108, 108, 111 }; // "Hello" in ASCII
      String expected = "SGVsbG8=";
      String actual = ManagedKeyProvider.encodeToStr(input);

      assertEquals("Encoded string should match expected Base64 representation", expected, actual);
    }

    @Test public void testDecodeToBytes() throws Exception {
      String input = "SGVsbG8="; // "Hello" in Base64
      byte[] expected = { 72, 101, 108, 108, 111 };
      byte[] actual = ManagedKeyProvider.decodeToBytes(input);

      assertTrue("Decoded bytes should match expected ASCII representation",
        Arrays.equals(expected, actual));
    }

    @Test public void testEncodeToStrAndDecodeToBytes() throws Exception {
      byte[] originalBytes = { 1, 2, 3, 4, 5 };
      String encoded = ManagedKeyProvider.encodeToStr(originalBytes);
      byte[] decoded = ManagedKeyProvider.decodeToBytes(encoded);

      assertTrue("Decoded bytes should match original bytes",
        Arrays.equals(originalBytes, decoded));
    }

    @Test(expected = Exception.class) public void testDecodeToBytes_InvalidInput()
      throws Exception {
      String invalidInput = "This is not a valid Base64 string!";
      ManagedKeyProvider.decodeToBytes(invalidInput);
    }

    @Test public void testRoundTrip_LargeInput() throws Exception {
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
