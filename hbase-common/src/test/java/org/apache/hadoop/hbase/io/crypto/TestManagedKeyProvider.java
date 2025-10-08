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

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider.KEY_METADATA_ALIAS;
import static org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider.KEY_METADATA_CUST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.security.KeyStore;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
      return Arrays
        .asList(new Object[][] { { Boolean.TRUE, Boolean.TRUE }, { Boolean.TRUE, Boolean.FALSE },
          { Boolean.FALSE, Boolean.TRUE }, { Boolean.FALSE, Boolean.FALSE }, });
    }

    // TestManagedKeyStoreKeyProvider specific fields
    private Configuration conf = HBaseConfiguration.create();
    private int nCustodians = 2;
    private ManagedKeyProvider managedKeyProvider;
    private Map<Bytes, Bytes> cust2key = new HashMap<>();
    private Map<Bytes, String> cust2alias = new HashMap<>();
    private Map<Bytes, Bytes> namespaceCust2key = new HashMap<>();
    private Map<Bytes, String> namespaceCust2alias = new HashMap<>();
    private String clusterId;
    private byte[] systemKey;

    @Before
    public void setUp() throws Exception {
      String providerParams = KeymetaTestUtils.setupTestKeyStore(TEST_UTIL, withPasswordOnAlias,
        withPasswordFile, store -> {
          Properties passwdProps = new Properties();
          try {
            addCustomEntries(store, passwdProps);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return passwdProps;
        });
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

      // Add custom namespace entries for testing
      String customNamespace1 = "table1/cf1";
      String customNamespace2 = "table2";
      for (int i = 0; i < 2; ++i) {
        String custodian = "ns-custodian+ " + i;
        String alias = custodian + "-alias";
        String namespace = (i == 0) ? customNamespace1 : customNamespace2;
        KeymetaTestUtils.addEntry(conf, 256, store, alias, custodian, withPasswordOnAlias,
          namespaceCust2key, namespaceCust2alias, passwdProps, namespace);
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
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encCust + ".*.active",
        "false");
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
      // For disabled keys, we need to configure both alias and active status
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + invalidCustEnc + ".*.alias",
        "disabled-alias");
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + invalidCustEnc + ".*.active",
        "false");
      ManagedKeyData keyData =
        managedKeyProvider.getManagedKey(invalidCust, ManagedKeyData.KEY_SPACE_GLOBAL);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.DISABLED, null, invalidCust, "disabled-alias");
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
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + invalidCustEnc + ".*.active",
        "false");
      String invalidMetadata = ManagedKeyStoreKeyProvider.generateKeyMetadata(invalidAlias,
        invalidCustEnc, ManagedKeyData.KEY_SPACE_GLOBAL);
      ManagedKeyData keyData = managedKeyProvider.unwrapKey(invalidMetadata, null);
      assertNotNull(keyData);
      assertKeyData(keyData, ManagedKeyState.DISABLED, null, invalidCust, invalidAlias);
    }

    @Test
    public void testGetManagedKeyWithCustomNamespace() throws Exception {
      String customNamespace1 = "table1/cf1";
      String customNamespace2 = "table2";
      int index = 0;
      for (Bytes cust : namespaceCust2key.keySet()) {
        String namespace = (index == 0) ? customNamespace1 : customNamespace2;
        ManagedKeyData keyData = managedKeyProvider.getManagedKey(cust.get(), namespace);
        assertKeyDataWithNamespace(keyData, ManagedKeyState.ACTIVE,
          namespaceCust2key.get(cust).get(), cust.get(), namespaceCust2alias.get(cust), namespace);
        index++;
      }
    }

    @Test
    public void testGetManagedKeyWithCustomNamespaceInactive() throws Exception {
      Bytes firstCust = namespaceCust2key.keySet().iterator().next();
      String customNamespace = "table1/cf1";
      String encCust = Base64.getEncoder().encodeToString(firstCust.get());
      // Set active status to false using the new namespace-aware format
      conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encCust + "." + customNamespace
        + ".active", "false");

      ManagedKeyData keyData = managedKeyProvider.getManagedKey(firstCust.get(), customNamespace);
      assertNotNull(keyData);
      assertKeyDataWithNamespace(keyData, ManagedKeyState.INACTIVE,
        namespaceCust2key.get(firstCust).get(), firstCust.get(), namespaceCust2alias.get(firstCust),
        customNamespace);
    }

    @Test
    public void testGetManagedKeyWithInvalidCustomNamespace() throws Exception {
      byte[] invalidCustBytes = "invalid".getBytes();
      String customNamespace = "invalid/namespace";
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(invalidCustBytes, customNamespace);
      assertNotNull(keyData);
      assertKeyDataWithNamespace(keyData, ManagedKeyState.FAILED, null, invalidCustBytes, null,
        customNamespace);
    }

    @Test
    public void testNamespaceMismatchReturnsFailedKey() throws Exception {
      // Use existing namespace key but request with different namespace
      Bytes firstCust = namespaceCust2key.keySet().iterator().next();
      String requestedNamespace = "table2/cf2"; // Different namespace from what's configured!

      // Request key with different namespace - should fail
      ManagedKeyData keyData =
        managedKeyProvider.getManagedKey(firstCust.get(), requestedNamespace);

      assertNotNull(keyData);
      assertEquals(ManagedKeyState.FAILED, keyData.getKeyState());
      assertNull(keyData.getTheKey());
      assertEquals(requestedNamespace, keyData.getKeyNamespace());
      assertEquals(firstCust, keyData.getKeyCustodian());
    }

    @Test
    public void testNamespaceMatchReturnsKey() throws Exception {
      // Use existing namespace key and request with matching namespace
      Bytes firstCust = namespaceCust2key.keySet().iterator().next();
      String configuredNamespace = "table1/cf1"; // This matches our test setup

      ManagedKeyData keyData =
        managedKeyProvider.getManagedKey(firstCust.get(), configuredNamespace);

      assertNotNull(keyData);
      assertEquals(ManagedKeyState.ACTIVE, keyData.getKeyState());
      assertNotNull(keyData.getTheKey());
      assertEquals(configuredNamespace, keyData.getKeyNamespace());
    }

    @Test
    public void testGlobalKeyAccessedWithWrongNamespaceFails() throws Exception {
      // Get a global key (one from cust2key)
      Bytes globalCust = cust2key.keySet().iterator().next();

      // Try to access it with a custom namespace - should fail
      String wrongNamespace = "table1/cf1";
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(globalCust.get(), wrongNamespace);

      assertNotNull(keyData);
      assertEquals(ManagedKeyState.FAILED, keyData.getKeyState());
      assertNull(keyData.getTheKey());
      assertEquals(wrongNamespace, keyData.getKeyNamespace());
    }

    @Test
    public void testNamespaceKeyAccessedAsGlobalFails() throws Exception {
      // Get a namespace-specific key
      Bytes namespaceCust = namespaceCust2key.keySet().iterator().next();

      // Try to access it as global - should fail
      ManagedKeyData keyData =
        managedKeyProvider.getManagedKey(namespaceCust.get(), ManagedKeyData.KEY_SPACE_GLOBAL);

      assertNotNull(keyData);
      assertEquals(ManagedKeyState.FAILED, keyData.getKeyState());
      assertNull(keyData.getTheKey());
      assertEquals(ManagedKeyData.KEY_SPACE_GLOBAL, keyData.getKeyNamespace());
    }

    @Test
    public void testMultipleNamespacesForSameCustodianFail() throws Exception {
      // Use existing namespace custodian
      Bytes namespaceCust = namespaceCust2key.keySet().iterator().next();
      String configuredNamespace = "table1/cf1"; // This matches our test setup
      String differentNamespace = "table2";

      // Verify we can access with configured namespace
      ManagedKeyData keyData1 =
        managedKeyProvider.getManagedKey(namespaceCust.get(), configuredNamespace);
      assertEquals(ManagedKeyState.ACTIVE, keyData1.getKeyState());
      assertEquals(configuredNamespace, keyData1.getKeyNamespace());

      // But accessing with different namespace should fail (even though it's the same custodian)
      ManagedKeyData keyData2 =
        managedKeyProvider.getManagedKey(namespaceCust.get(), differentNamespace);
      assertEquals(ManagedKeyState.FAILED, keyData2.getKeyState());
      assertEquals(differentNamespace, keyData2.getKeyNamespace());
    }

    @Test
    public void testNullNamespaceDefaultsToGlobal() throws Exception {
      // Get a global key (one from cust2key)
      Bytes globalCust = cust2key.keySet().iterator().next();

      // Call getManagedKey with null namespace - should default to global and succeed
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(globalCust.get(), null);

      assertNotNull(keyData);
      assertEquals(ManagedKeyState.ACTIVE, keyData.getKeyState());
      assertNotNull(keyData.getTheKey());
      assertEquals(ManagedKeyData.KEY_SPACE_GLOBAL, keyData.getKeyNamespace());
    }

    @Test
    public void testFailedKeyContainsProperMetadataWithAlias() throws Exception {
      // Use existing namespace key but request with different namespace
      Bytes firstCust = namespaceCust2key.keySet().iterator().next();
      String wrongNamespace = "wrong/namespace";

      // Request with wrong namespace - should fail but have proper metadata
      ManagedKeyData keyData = managedKeyProvider.getManagedKey(firstCust.get(), wrongNamespace);

      assertNotNull(keyData);
      assertEquals(ManagedKeyState.FAILED, keyData.getKeyState());
      assertNull(keyData.getTheKey());
      assertEquals(wrongNamespace, keyData.getKeyNamespace());

      // Verify the failed key metadata - should have null alias since wrong namespace is requested
      // This is the correct security behavior - don't leak alias information across namespaces
      String expectedEncodedCust = Base64.getEncoder().encodeToString(firstCust.get());
      assertMetadataMatches(keyData.getKeyMetadata(), null, expectedEncodedCust, wrongNamespace);
    }

    @Test
    public void testBackwardsCompatibilityForGenerateKeyMetadata() {
      String alias = "test-alias";
      String encodedCust = "dGVzdA==";

      // Test the old method (should default to global namespace)
      String oldMetadata = ManagedKeyStoreKeyProvider.generateKeyMetadata(alias, encodedCust);

      // Test the new method with explicit global namespace
      String newMetadata = ManagedKeyStoreKeyProvider.generateKeyMetadata(alias, encodedCust,
        ManagedKeyData.KEY_SPACE_GLOBAL);

      assertEquals(
        "Old and new metadata generation should produce same result for global namespace",
        oldMetadata, newMetadata);

      // Verify both contain the namespace field
      Map<String, Object> oldMap = parseKeyMetadata(oldMetadata);
      Map<String, Object> newMap = parseKeyMetadata(newMetadata);

      assertEquals(ManagedKeyData.KEY_SPACE_GLOBAL,
        oldMap.get(ManagedKeyStoreKeyProvider.KEY_METADATA_NAMESPACE));
      assertEquals(ManagedKeyData.KEY_SPACE_GLOBAL,
        newMap.get(ManagedKeyStoreKeyProvider.KEY_METADATA_NAMESPACE));
    }

    private void assertKeyData(ManagedKeyData keyData, ManagedKeyState expKeyState, byte[] key,
      byte[] custBytes, String alias) throws Exception {
      assertKeyDataWithNamespace(keyData, expKeyState, key, custBytes, alias,
        ManagedKeyData.KEY_SPACE_GLOBAL);
    }

    /**
     * Helper method to parse key metadata JSON string into a Map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseKeyMetadata(String keyMetadataStr) {
      return GsonUtil.getDefaultInstance().fromJson(keyMetadataStr, HashMap.class);
    }

    /**
     * Helper method to assert metadata contents
     */
    private void assertMetadataContains(Map<String, Object> metadata, String expectedAlias,
      String expectedEncodedCust, String expectedNamespace) {
      assertNotNull("Metadata should not be null", metadata);
      assertEquals("Metadata should contain expected alias", expectedAlias,
        metadata.get(KEY_METADATA_ALIAS));
      assertEquals("Metadata should contain expected encoded custodian", expectedEncodedCust,
        metadata.get(KEY_METADATA_CUST));
      assertEquals("Metadata should contain expected namespace", expectedNamespace,
        metadata.get(ManagedKeyStoreKeyProvider.KEY_METADATA_NAMESPACE));
    }

    /**
     * Helper method to parse and assert metadata contents in one call
     */
    private void assertMetadataMatches(String keyMetadataStr, String expectedAlias,
      String expectedEncodedCust, String expectedNamespace) {
      Map<String, Object> metadata = parseKeyMetadata(keyMetadataStr);
      assertMetadataContains(metadata, expectedAlias, expectedEncodedCust, expectedNamespace);
    }

    private void assertKeyDataWithNamespace(ManagedKeyData keyData, ManagedKeyState expKeyState,
      byte[] key, byte[] custBytes, String alias, String expectedNamespace) throws Exception {
      assertNotNull(keyData);
      assertEquals(expKeyState, keyData.getKeyState());
      assertEquals(expectedNamespace, keyData.getKeyNamespace());
      if (key == null) {
        assertNull(keyData.getTheKey());
      } else {
        byte[] keyBytes = keyData.getTheKey().getEncoded();
        assertEquals(key.length, keyBytes.length);
        assertEquals(new Bytes(key), keyBytes);
      }

      // Use helper method instead of duplicated parsing logic
      String encodedCust = Base64.getEncoder().encodeToString(custBytes);
      assertMetadataMatches(keyData.getKeyMetadata(), alias, encodedCust, expectedNamespace);

      assertEquals(new Bytes(custBytes), keyData.getKeyCustodian());
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
