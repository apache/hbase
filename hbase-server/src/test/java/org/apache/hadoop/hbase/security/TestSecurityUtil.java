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
package org.apache.hadoop.hbase.security;

import static org.apache.hadoop.hbase.io.crypto.ManagedKeyData.KEY_SPACE_GLOBAL_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.util.Arrays;
import java.util.Collection;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.MockAesKeyProvider;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.keymeta.KeyIdentitySingleArrayBacked;
import org.apache.hadoop.hbase.keymeta.ManagedKeyDataCache;
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentityUtils;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestSecurityUtil.TestBasic.class,
  TestSecurityUtil.TestCreateEncryptionContext_ForWrites.class,
  TestSecurityUtil.TestCreateEncryptionContext_ForReads.class,
  TestSecurityUtil.TestCreateEncryptionContext_WithoutKeyManagement_UnwrapKeyException.class, })
@Category({ SecurityTests.class, SmallTests.class })
public class TestSecurityUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSecurityUtil.class);

  // Test constants to eliminate magic strings and improve maintainability
  protected static final String TEST_NAMESPACE = "test-namespace";
  protected static final byte[] TEST_NAMESPACE_BYTES = Bytes.toBytes(TEST_NAMESPACE);
  protected static final String TEST_FAMILY = "test-family";
  /** Namespace used in read-path tests (trailer KEK identity and cache lookup). */
  protected static final String READ_PATH_TEST_NAMESPACE = "test:table/" + TEST_FAMILY;
  protected static final byte[] READ_PATH_TEST_NAMESPACE_BYTES =
    Bytes.toBytes(READ_PATH_TEST_NAMESPACE);
  protected static final String HBASE_KEY = "hbase";
  protected static final String TEST_KEK_METADATA = "test-kek-metadata";
  protected static final byte[] TEST_KEK_IDENTITY = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
  protected static final String TEST_KEY_16_BYTE = "test-key-16-byte";
  protected static final String TEST_DEK_16_BYTE = "test-dek-16-byte";
  protected static final String INVALID_KEY_DATA = "invalid-key-data";
  protected static final String INVALID_WRAPPED_KEY_DATA = "invalid-wrapped-key-data";
  protected static final String INVALID_SYSTEM_KEY_DATA = "invalid-system-key-data";
  protected static final String UNKNOWN_CIPHER = "UNKNOWN_CIPHER";
  protected static final String AES_CIPHER = "AES";
  protected static final String DES_CIPHER = "DES";

  protected Configuration conf;
  protected HBaseTestingUtil testUtil;
  protected Path testPath;
  protected ColumnFamilyDescriptor mockFamily;
  protected TableDescriptor mockTableDescriptor;
  protected ManagedKeyDataCache mockManagedKeyDataCache;
  protected SystemKeyCache mockSystemKeyCache;
  protected FixedFileTrailer mockTrailer;
  protected ManagedKeyData mockManagedKeyData;
  protected Key testKey;
  protected byte[] testWrappedKey;
  protected Key kekKey;

  /**
   * Configuration builder for setting up different encryption test scenarios.
   */
  protected static class TestConfigBuilder {
    private boolean encryptionEnabled = true;
    private boolean keyManagementEnabled = false;
    private boolean localKeyGenEnabled = false;
    private String cipherProvider = "org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider";
    private String keyProvider = MockAesKeyProvider.class.getName();
    private String masterKeyName = HBASE_KEY;

    public TestConfigBuilder withEncryptionEnabled(boolean enabled) {
      this.encryptionEnabled = enabled;
      return this;
    }

    public TestConfigBuilder withKeyManagement(boolean localKeyGen) {
      this.keyManagementEnabled = true;
      this.localKeyGenEnabled = localKeyGen;
      return this;
    }

    public TestConfigBuilder withNullCipherProvider() {
      this.cipherProvider = NullCipherProvider.class.getName();
      return this;
    }

    public void apply(Configuration conf) {
      conf.setBoolean(Encryption.CRYPTO_ENABLED_CONF_KEY, encryptionEnabled);
      conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, keyProvider);
      conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, masterKeyName);
      conf.set(HConstants.CRYPTO_KEYPROVIDER_PARAMETERS_KEY, "true");
      conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, cipherProvider);

      if (keyManagementEnabled) {
        conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
        conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY,
          localKeyGenEnabled);
      } else {
        conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);
      }
    }
  }

  protected static TestConfigBuilder configBuilder() {
    return new TestConfigBuilder();
  }

  protected void setUpEncryptionConfig() {
    // Set up real encryption configuration using default AES cipher
    conf.setBoolean(Encryption.CRYPTO_ENABLED_CONF_KEY, true);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, HBASE_KEY);
    // Enable key caching
    conf.set(HConstants.CRYPTO_KEYPROVIDER_PARAMETERS_KEY, "true");
    // Use DefaultCipherProvider for real AES encryption functionality
    conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY,
      "org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider");
  }

  protected void setUpEncryptionConfigWithNullCipher() {
    configBuilder().withNullCipherProvider().apply(conf);
  }

  // ==== Mock Setup Helpers ====

  protected void setupManagedKeyDataCache(byte[] namespace, ManagedKeyData keyData) {
    when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES),
      eq(new Bytes(namespace)))).thenReturn(keyData);
  }

  protected void setupManagedKeyDataCache(Bytes namespace, ManagedKeyData keyData) {
    when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES),
      eq(namespace))).thenReturn(keyData);
  }

  protected void setupManagedKeyDataCache(byte[] namespace, byte[] globalSpace,
    ManagedKeyData keyData) {
    when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES),
      eq(new Bytes(namespace)))).thenReturn(null);
    when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES),
      eq(new Bytes(globalSpace)))).thenReturn(keyData);
  }

  protected void setupTrailerMocks(byte[] keyBytes, String metadata, byte[] kekIdentity,
    String namespace) {
    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(metadata);
    if (kekIdentity != null && kekIdentity.length > 0) {
      when(mockTrailer.getKekIdentity()).thenReturn(kekIdentity);
    }
  }

  protected void setupSystemKeyCache(byte[] fullIdentity, ManagedKeyData keyData) {
    when(mockSystemKeyCache.getSystemKeyByIdentity(fullIdentity)).thenReturn(keyData);
  }

  protected void setupSystemKeyCache(ManagedKeyData latestKey) {
    when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(latestKey);
  }

  /**
   * Mocks managed key cache getEntry for read path: 3-arg getEntry(FullKeyIdentity, metadata,
   * null). Matches when the identity equals the one built from {@code kekIdentity}.
   */
  protected void setupManagedKeyDataCacheEntry(byte[] kekIdentity, String metadata,
    ManagedKeyData keyData) throws IOException, KeyException {
    when(mockManagedKeyDataCache.getEntry(
      argThat(identity -> new KeyIdentitySingleArrayBacked(kekIdentity).equals(identity)),
      eq(metadata), eq(null))).thenReturn(keyData);
  }

  /**
   * Mocks managed key cache getEntry for read path when using full identity: 3-arg getEntry
   * (FullKeyIdentity, metadata, null). Builds kekIdentity from custodian, namespace,
   * partialIdentity.
   */
  protected void setupManagedKeyDataCacheEntryForFullIdentity(byte[] custodian, byte[] namespace,
    byte[] partialIdentity, String metadata, ManagedKeyData keyData)
    throws IOException, KeyException {
    byte[] kekIdentity =
      ManagedKeyIdentityUtils.constructRowKeyForIdentity(custodian, namespace, partialIdentity);
    when(mockManagedKeyDataCache.getEntry(
      argThat(identity -> new KeyIdentitySingleArrayBacked(kekIdentity).equals(identity)),
      eq(metadata), eq(null))).thenReturn(keyData);
  }

  // ==== Exception Testing Helpers ====

  protected <T extends Exception> void assertExceptionContains(Class<T> expectedType,
    String expectedMessage, Runnable testCode) {
    T exception = assertThrows(expectedType, () -> testCode.run());
    assertTrue("Exception message should contain: " + expectedMessage,
      exception.getMessage().contains(expectedMessage));
  }

  protected void assertEncryptionContextThrowsForWrites(Class<? extends Exception> expectedType,
    String expectedMessage) {
    Exception exception = assertThrows(Exception.class, () -> {
      SecurityUtil.createEncryptionContext(conf, mockTableDescriptor, mockFamily,
        mockManagedKeyDataCache, mockSystemKeyCache);
    });
    assertTrue("Expected exception type: " + expectedType.getName() + ", but got: "
      + exception.getClass().getName(), expectedType.isInstance(exception));
    assertTrue("Exception message should contain: " + expectedMessage,
      exception.getMessage().contains(expectedMessage));
  }

  protected void assertEncryptionContextThrowsForReads(Class<? extends Exception> expectedType,
    String expectedMessage) {
    Exception exception = assertThrows(Exception.class, () -> {
      SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
        mockSystemKeyCache);
    });
    assertTrue("Expected exception type: " + expectedType.getName() + ", but got: "
      + exception.getClass().getName(), expectedType.isInstance(exception));
    assertTrue("Exception message should contain: " + expectedMessage,
      exception.getMessage().contains(expectedMessage));
  }

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    testUtil = new HBaseTestingUtil(conf);
    testPath = testUtil.getDataTestDir("test-file");

    // Setup mocks (only for objects that don't have encryption logic)
    mockFamily = mock(ColumnFamilyDescriptor.class);
    mockTableDescriptor = mock(TableDescriptor.class);
    mockManagedKeyDataCache = mock(ManagedKeyDataCache.class);
    mockSystemKeyCache = mock(SystemKeyCache.class);
    mockTrailer = mock(FixedFileTrailer.class);
    mockManagedKeyData = mock(ManagedKeyData.class);

    // Use a real test key with exactly 16 bytes for AES-128
    testKey = new SecretKeySpec(TEST_KEY_16_BYTE.getBytes(), AES_CIPHER);

    // Configure mocks
    when(mockFamily.getEncryptionType()).thenReturn(AES_CIPHER);
    when(mockFamily.getNameAsString()).thenReturn(TEST_FAMILY);
    when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(null); // Default to null for
                                                                        // fallback
    // logic
    when(mockTableDescriptor.getTableName()).thenReturn(TableName.valueOf("test:table"));
    when(mockManagedKeyData.getTheKey()).thenReturn(testKey);

    // Set up default encryption config
    setUpEncryptionConfig();

    // Create test wrapped key
    KeyProvider keyProvider = Encryption.getKeyProvider(conf);
    kekKey = keyProvider.getKey(HBASE_KEY);
    Key key = keyProvider.getKey(TEST_DEK_16_BYTE);
    testWrappedKey = EncryptionUtil.wrapKey(conf, null, key, kekKey);
  }

  private static byte[] createRandomWrappedKey(Configuration conf) throws IOException {
    Cipher cipher = Encryption.getCipher(conf, "AES");
    Key key = cipher.getRandomKey();
    return EncryptionUtil.wrapKey(conf, HBASE_KEY, key);
  }

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestBasic extends TestSecurityUtil {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBasic.class);

    @Test
    public void testGetUserFromPrincipal() {
      // Test with slash separator
      assertEquals("user1", SecurityUtil.getUserFromPrincipal("user1/host@REALM"));
      assertEquals("user2", SecurityUtil.getUserFromPrincipal("user2@REALM"));

      // Test with no realm
      assertEquals("user3", SecurityUtil.getUserFromPrincipal("user3"));

      // Test with multiple slashes
      assertEquals("user4", SecurityUtil.getUserFromPrincipal("user4/host1/host2@REALM"));
    }

    @Test
    public void testGetPrincipalWithoutRealm() {
      // Test with realm
      assertEquals("user1/host", SecurityUtil.getPrincipalWithoutRealm("user1/host@REALM"));
      assertEquals("user2", SecurityUtil.getPrincipalWithoutRealm("user2@REALM"));

      // Test without realm
      assertEquals("user3", SecurityUtil.getPrincipalWithoutRealm("user3"));
      assertEquals("user4/host", SecurityUtil.getPrincipalWithoutRealm("user4/host"));
    }

    @Test
    public void testIsKeyManagementEnabled() {
      Configuration conf = HBaseConfiguration.create();

      // Test default behavior (should be false)
      assertFalse(SecurityUtil.isKeyManagementEnabled(conf));

      // Test with key management enabled
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
      assertTrue(SecurityUtil.isKeyManagementEnabled(conf));

      // Test with key management disabled
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);
      assertFalse(SecurityUtil.isKeyManagementEnabled(conf));
    }
  }

  // Tests for the first createEncryptionContext method (for ColumnFamilyDescriptor)
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestCreateEncryptionContext_ForWrites extends TestSecurityUtil {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCreateEncryptionContext_ForWrites.class);

    @Test
    public void testWithNoEncryptionOnFamily() throws IOException {
      when(mockFamily.getEncryptionType()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      assertEquals(Encryption.Context.NONE, result);
    }

    @Test
    public void testWithEncryptionDisabled() throws IOException {
      configBuilder().withEncryptionEnabled(false).apply(conf);
      assertEncryptionContextThrowsForWrites(IllegalStateException.class,
        "encryption feature is disabled");
    }

    @Test
    public void testWithKeyManagement_LocalKeyGen() throws IOException {
      when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(new Bytes(TEST_NAMESPACE_BYTES));
      configBuilder().withKeyManagement(true).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, mockManagedKeyData);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
    }

    @Test
    public void testWithKeyManagement_NoActiveKey_NoSystemKeyCache() throws IOException {
      // Test backwards compatibility: when no active key found and system cache is null, should
      // throw
      configBuilder().withKeyManagement(false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, KEY_SPACE_GLOBAL_BYTES.copyBytes(), null);
      when(mockFamily.getEncryptionKey()).thenReturn(null);

      // With null system key cache, should still throw IOException
      Exception exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockTableDescriptor, mockFamily,
          mockManagedKeyDataCache, null);
      });
      assertTrue("Should reference system key cache",
        exception.getMessage().contains("SystemKeyCache"));
    }

    @Test
    public void testWithKeyManagement_NoActiveKey_WithSystemKeyCache() throws IOException {
      // Test backwards compatibility: when no active key found but system cache available, should
      // work
      configBuilder().withKeyManagement(false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, KEY_SPACE_GLOBAL_BYTES.copyBytes(), null);
      setupSystemKeyCache(mockManagedKeyData);
      when(mockFamily.getEncryptionKey()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      // Should use system key as KEK and generate random DEK
      assertEquals(mockManagedKeyData, result.getKEKData());
    }

    @Test
    public void testWithKeyManagement_LocalKeyGen_WithUnknownKeyCipher() throws IOException {
      when(mockFamily.getEncryptionType()).thenReturn(UNKNOWN_CIPHER);
      Key unknownKey = mock(Key.class);
      when(unknownKey.getAlgorithm()).thenReturn(UNKNOWN_CIPHER);
      when(mockManagedKeyData.getTheKey()).thenReturn(unknownKey);

      when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(new Bytes(TEST_NAMESPACE_BYTES));
      configBuilder().withKeyManagement(true).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, mockManagedKeyData);
      assertEncryptionContextThrowsForWrites(RuntimeException.class,
        "Cipher 'UNKNOWN_CIPHER' is not");
    }

    @Test
    public void testWithKeyManagement_LocalKeyGen_WithKeyAlgorithmMismatch() throws IOException {
      Key desKey = mock(Key.class);
      when(desKey.getAlgorithm()).thenReturn(DES_CIPHER);
      when(mockManagedKeyData.getTheKey()).thenReturn(desKey);

      when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(new Bytes(TEST_NAMESPACE_BYTES));
      configBuilder().withKeyManagement(true).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, mockManagedKeyData);
      assertEncryptionContextThrowsForWrites(IllegalStateException.class,
        "Encryption for family 'test-family' configured with type 'AES' but key specifies "
          + "algorithm 'DES'");
    }

    @Test
    public void testWithKeyManagement_UseSystemKeyWithNSSpecificActiveKey() throws IOException {
      when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(new Bytes(TEST_NAMESPACE_BYTES));
      configBuilder().withKeyManagement(false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, mockManagedKeyData);
      setupSystemKeyCache(mockManagedKeyData);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
    }

    @Test
    public void testWithKeyManagement_UseSystemKeyWithoutNSSpecificActiveKey() throws IOException {
      configBuilder().withKeyManagement(false).apply(conf);
      setupManagedKeyDataCache(KEY_SPACE_GLOBAL_BYTES, mockManagedKeyData);
      setupSystemKeyCache(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
    }

    @Test
    public void testWithoutKeyManagement_WithFamilyProvidedKey() throws Exception {
      byte[] wrappedKey = createRandomWrappedKey(conf);
      when(mockFamily.getEncryptionKey()).thenReturn(wrappedKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result, false);
    }

    @Test
    public void testWithoutKeyManagement_KeyAlgorithmMismatch() throws Exception {
      // Create a key with different algorithm and wrap it
      Key differentKey = new SecretKeySpec(TEST_KEY_16_BYTE.getBytes(), DES_CIPHER);
      byte[] wrappedDESKey = EncryptionUtil.wrapKey(conf, HBASE_KEY, differentKey);
      when(mockFamily.getEncryptionKey()).thenReturn(wrappedDESKey);

      assertEncryptionContextThrowsForWrites(IllegalStateException.class,
        "Encryption for family 'test-family' configured with type 'AES' but key specifies "
          + "algorithm 'DES'");
    }

    @Test
    public void testWithUnavailableCipher() throws IOException {
      when(mockFamily.getEncryptionType()).thenReturn(UNKNOWN_CIPHER);
      setUpEncryptionConfigWithNullCipher();
      assertEncryptionContextThrowsForWrites(IllegalStateException.class,
        "Cipher 'UNKNOWN_CIPHER' is not available");
    }

    // ---- New backwards compatibility test scenarios ----

    @Test
    public void testBackwardsCompatibility_Scenario1_FamilyKeyWithKeyManagement()
      throws IOException {
      // Scenario 1: Family has encryption key -> use as DEK, latest STK as KEK
      when(mockFamily.getEncryptionKey()).thenReturn(testWrappedKey);
      configBuilder().withKeyManagement(false).apply(conf);
      setupSystemKeyCache(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      // Verify that system key is used as KEK
      assertEquals(mockManagedKeyData, result.getKEKData());
    }

    @Test
    public void testBackwardsCompatibility_Scenario2a_ActiveKeyAsDeK() throws IOException {
      // Scenario 2a: Active key exists, local key gen disabled -> use active key as DEK, latest STK
      // as KEK
      when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(new Bytes(TEST_NAMESPACE_BYTES));
      configBuilder().withKeyManagement(false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, mockManagedKeyData);
      ManagedKeyData mockSystemKey = mock(ManagedKeyData.class);
      when(mockSystemKey.getTheKey()).thenReturn(kekKey);
      setupSystemKeyCache(mockSystemKey);
      when(mockFamily.getEncryptionKey()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      // Verify that active key is used as DEK and system key as KEK
      assertEquals(testKey, result.getKey()); // Active key should be the DEK
      assertEquals(mockSystemKey, result.getKEKData()); // System key should be the KEK
    }

    @Test
    public void testBackwardsCompatibility_Scenario2b_ActiveKeyAsKekWithLocalKeyGen()
      throws IOException {
      // Scenario 2b: Active key exists, local key gen enabled -> use active key as KEK, generate
      // random DEK
      when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(new Bytes(TEST_NAMESPACE_BYTES));
      configBuilder().withKeyManagement(true).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, mockManagedKeyData);
      when(mockFamily.getEncryptionKey()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      // Verify that active key is used as KEK and a generated key as DEK
      assertNotNull("DEK should be generated", result.getKey());
      assertEquals(mockManagedKeyData, result.getKEKData()); // Active key should be the KEK
    }

    @Test
    public void testBackwardsCompatibility_Scenario3a_NoActiveKeyGenerateLocalKey()
      throws IOException {
      // Scenario 3: No active key -> generate random DEK, latest STK as KEK
      configBuilder().withKeyManagement(false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE_BYTES, KEY_SPACE_GLOBAL_BYTES.copyBytes(), null); // No
                                                                                                // active
      // key
      setupSystemKeyCache(mockManagedKeyData);
      when(mockFamily.getEncryptionKey()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      // Verify that a random key is generated as DEK and system key as KEK
      assertNotNull("DEK should be generated", result.getKey());
      assertEquals(mockManagedKeyData, result.getKEKData()); // System key should be the KEK
    }

    @Test
    public void testWithoutKeyManagement_Scenario3b_WithRandomKeyGeneration() throws IOException {
      when(mockFamily.getEncryptionKey()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result, false);
    }

    @Test
    public void test_CFKeyNamespaceAttribute() throws IOException {
      // Test Rule 1: Column family has KEY_NAMESPACE attribute
      String cfKeyNamespace = "cf-specific-namespace";
      when(mockFamily.getEncryptionKeyNamespaceBytes())
        .thenReturn(new Bytes(Bytes.toBytes(cfKeyNamespace)));
      when(mockFamily.getEncryptionKey()).thenReturn(null);
      configBuilder().withKeyManagement(false).apply(conf);

      // Mock managed key data cache to return active key only for CF namespace
      setupManagedKeyDataCache(Bytes.toBytes(cfKeyNamespace), mockManagedKeyData);
      setupSystemKeyCache(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(testKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
    }

    @Test
    public void testFallback_GlobalNamespace() throws IOException {
      // Fall back to global namespace when CF namespace is null or has no key
      when(mockFamily.getEncryptionKeyNamespaceBytes()).thenReturn(null); // No CF namespace
      when(mockFamily.getEncryptionKey()).thenReturn(null);
      configBuilder().withKeyManagement(false).apply(conf);

      setupManagedKeyDataCache(KEY_SPACE_GLOBAL_BYTES, mockManagedKeyData);
      setupSystemKeyCache(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(testKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
    }

    @Test
    public void testFallbackRuleOrder() throws IOException {
      // Test that candidates are tried in order: CF namespace first, then global
      String cfKeyNamespace = "cf-namespace";

      when(mockFamily.getEncryptionKeyNamespaceBytes())
        .thenReturn(new Bytes(Bytes.toBytes(cfKeyNamespace)));
      when(mockFamily.getEncryptionKey()).thenReturn(null);
      configBuilder().withKeyManagement(false).apply(conf);

      // CF namespace fails, global succeeds
      when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES),
        eq(new Bytes(Bytes.toBytes(cfKeyNamespace))))).thenReturn(null);
      when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES),
        eq(KEY_SPACE_GLOBAL_BYTES))).thenReturn(mockManagedKeyData);

      setupSystemKeyCache(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(testKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
    }

    @Test
    public void testBackwardsCompatibility_Scenario1_FamilyKeyWithoutKeyManagement()
      throws IOException {
      // Scenario 1 variation: Family has encryption key but key management disabled -> use as DEK,
      // no KEK
      byte[] wrappedKey = createRandomWrappedKey(conf);
      when(mockFamily.getEncryptionKey()).thenReturn(wrappedKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockTableDescriptor,
        mockFamily, mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result, false); // No key management, so no KEK data
    }

    @Test
    public void testWithKeyManagement_FamilyKey_UnwrapKeyException() throws Exception {
      // Test for KeyException->IOException wrapping when family has key bytes with key management
      // enabled
      // This covers the exception block at lines 103-105 in SecurityUtil.java

      // Create a properly wrapped key first, then corrupt it to cause unwrapping failure
      Key wrongKek = new SecretKeySpec("bad-kek-16-bytes".getBytes(), AES_CIPHER); // Exactly 16
                                                                                   // bytes
      byte[] validWrappedKey = EncryptionUtil.wrapKey(conf, null, testKey, wrongKek);

      when(mockFamily.getEncryptionKey()).thenReturn(validWrappedKey);
      configBuilder().withKeyManagement(false).apply(conf);
      setupSystemKeyCache(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey); // Different KEK for unwrapping

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockTableDescriptor, mockFamily,
          mockManagedKeyDataCache, mockSystemKeyCache);
      });

      // The IOException should wrap a KeyException from the unwrapping process
      assertNotNull("Exception should have a cause", exception.getCause());
      assertTrue("Exception cause should be a KeyException",
        exception.getCause() instanceof KeyException);
    }

    // Tests for the second createEncryptionContext method (for reading files)

    @Test
    public void testWithNoKeyMaterial() throws IOException {
      when(mockTrailer.getEncryptionKey()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);

      assertEquals(Encryption.Context.NONE, result);
    }
  }

  // Tests for the second createEncryptionContext method (for reading files)
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestCreateEncryptionContext_ForReads extends TestSecurityUtil {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCreateEncryptionContext_ForReads.class);

    @Test
    public void testWithKEKMetadata_STKLookupFirstThenManagedKey() throws Exception {
      // Test new logic: STK lookup happens first, then metadata lookup if STK fails
      // Set up scenario where both checksum and metadata are available
      setupTrailerMocks(testWrappedKey, TEST_KEK_METADATA, TEST_KEK_IDENTITY, null);
      configBuilder().withKeyManagement(false).apply(conf);

      // STK lookup should succeed and be used (first priority)
      ManagedKeyData stkKeyData = mock(ManagedKeyData.class);
      when(stkKeyData.getTheKey()).thenReturn(kekKey);
      setupSystemKeyCache(TEST_KEK_IDENTITY, stkKeyData);

      // Also set up managed key cache (but it shouldn't be used since STK succeeds)
      setupManagedKeyDataCacheEntry(TEST_KEK_IDENTITY, TEST_KEK_METADATA, mockManagedKeyData);
      when(mockManagedKeyData.getTheKey())
        .thenThrow(new RuntimeException("This should not be called"));

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      // Should use STK data, not managed key data
      assertEquals(stkKeyData, result.getKEKData());
    }

    @Test
    public void testWithKEKMetadata_STKFailsThenManagedKeySucceeds() throws Exception {
      // Test fallback: STK lookup fails, managed key lookup by full identity succeeds
      byte[] partialIdentity = ManagedKeyIdentityUtils.constructMetadataHash(TEST_KEK_METADATA);
      byte[] kekIdentity = ManagedKeyIdentityUtils.constructRowKeyForIdentity(
        ManagedKeyData.KEY_SPACE_GLOBAL_BYTES.get(), READ_PATH_TEST_NAMESPACE_BYTES,
        partialIdentity);
      setupTrailerMocks(testWrappedKey, TEST_KEK_METADATA, kekIdentity, READ_PATH_TEST_NAMESPACE);
      configBuilder().withKeyManagement(true).apply(conf);

      when(mockSystemKeyCache.getSystemKeyByIdentity(kekIdentity)).thenReturn(null);
      setupManagedKeyDataCacheEntryForFullIdentity(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES.get(),
        READ_PATH_TEST_NAMESPACE_BYTES, partialIdentity, TEST_KEK_METADATA, mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      assertEquals(mockManagedKeyData, result.getKEKData());
    }

    @Test
    public void testWithKeyManagement_KEKMetadataAndChecksumFailure()
      throws IOException, KeyException {
      // Test scenario: STK fails, managed key getEntry returns null, latest system key is null
      byte[] keyBytes = "test-encrypted-key".getBytes();
      String kekMetadata = "test-kek-metadata";
      byte[] partialIdentity = ManagedKeyIdentityUtils.constructMetadataHash(kekMetadata);
      byte[] kekIdentity = ManagedKeyIdentityUtils.constructRowKeyForIdentity(
        ManagedKeyData.KEY_SPACE_GLOBAL_BYTES.get(), Bytes.toBytes("test-namespace"),
        partialIdentity);
      configBuilder().withKeyManagement(true).apply(conf);

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(kekMetadata);
      when(mockTrailer.getKekIdentity()).thenReturn(kekIdentity);

      when(mockSystemKeyCache.getSystemKeyByIdentity(kekIdentity)).thenReturn(null);
      when(mockManagedKeyDataCache.getEntry(
        argThat(identity -> new KeyIdentitySingleArrayBacked(kekIdentity).equals(identity)),
        eq(kekMetadata), eq(null))).thenReturn(null);
      when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(null);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
          mockSystemKeyCache);
      });

      assertTrue(exception.getMessage().contains("Failed to get latest system key"));
    }

    @Test
    public void testWithKeyManagement_UseSystemKey() throws IOException {
      // Test STK lookup by identity (first priority in new logic)
      setupTrailerMocks(testWrappedKey, null, TEST_KEK_IDENTITY, null);
      configBuilder().withKeyManagement(false).apply(conf);
      setupSystemKeyCache(TEST_KEK_IDENTITY, mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      assertEquals(mockManagedKeyData, result.getKEKData());
    }

    @Test
    public void testBackwardsCompatibility_WithKeyManagement_LatestSystemKeyNotFound()
      throws IOException {
      // Test when both STK lookup by checksum fails and latest system key is null
      byte[] keyBytes = "test-encrypted-key".getBytes();

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);

      // Enable key management
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

      // Both checksum lookup and latest system key lookup should fail
      when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(null);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
          mockSystemKeyCache);
      });

      assertTrue(exception.getMessage().contains("Failed to get latest system key"));
    }

    @Test
    public void testBackwardsCompatibility_FallbackToLatestSystemKey() throws IOException {
      // Test fallback to latest system key when both checksum and metadata are unavailable
      setupTrailerMocks(testWrappedKey, null, new byte[0], TEST_NAMESPACE); // No identity, no
                                                                            // metadata
      configBuilder().withKeyManagement(false).apply(conf);

      ManagedKeyData latestSystemKey = mock(ManagedKeyData.class);
      when(latestSystemKey.getTheKey()).thenReturn(kekKey);
      when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(latestSystemKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result);
      assertEquals(latestSystemKey, result.getKEKData());
    }

    @Test
    public void testWithoutKeyManagemntEnabled() throws IOException {
      byte[] wrappedKey = createRandomWrappedKey(conf);
      when(mockTrailer.getEncryptionKey()).thenReturn(wrappedKey);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result, false);
    }

    @Test
    public void testKeyManagementBackwardsCompatibility() throws Exception {
      when(mockTrailer.getEncryptionKey()).thenReturn(testWrappedKey);
      when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);
      configBuilder().withKeyManagement(false).apply(conf);

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);

      verifyContext(result, true);
    }

    @Test
    public void testWithoutKeyManagement_UnwrapFailure() throws IOException {
      byte[] invalidKeyBytes = INVALID_KEY_DATA.getBytes();
      when(mockTrailer.getEncryptionKey()).thenReturn(invalidKeyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);

      Exception exception = assertThrows(Exception.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
          mockSystemKeyCache);
      });

      // The exception should indicate that unwrapping failed - could be IOException or
      // RuntimeException
      assertNotNull(exception);
    }

    @Test
    public void testCreateEncryptionContext_WithoutKeyManagement_UnavailableCipher()
      throws Exception {
      // Create a DES key and wrap it first with working configuration
      Key desKey = new SecretKeySpec("test-key-16-byte".getBytes(), "DES");
      byte[] wrappedDESKey = EncryptionUtil.wrapKey(conf, HBASE_KEY, desKey);

      when(mockTrailer.getEncryptionKey()).thenReturn(wrappedDESKey);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);

      // Disable key management and use null cipher provider
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);
      setUpEncryptionConfigWithNullCipher();

      RuntimeException exception = assertThrows(RuntimeException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
          mockSystemKeyCache);
      });

      assertTrue(exception.getMessage().contains("Cipher 'AES' not available"));
    }

    @Test
    public void testCreateEncryptionContext_WithKeyManagement_NullKeyManagementCache()
      throws IOException {
      byte[] keyBytes = "test-encrypted-key".getBytes();
      String kekMetadata = "test-kek-metadata";
      byte[] partialIdentity = ManagedKeyIdentityUtils.constructMetadataHash(kekMetadata);
      byte[] kekIdentity = ManagedKeyIdentityUtils.constructRowKeyForIdentity(
        ManagedKeyData.KEY_SPACE_GLOBAL_BYTES.get(), Bytes.toBytes("test-namespace"),
        partialIdentity);

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(kekMetadata);
      when(mockTrailer.getKekIdentity()).thenReturn(kekIdentity);
      when(mockSystemKeyCache.getSystemKeyByIdentity(kekIdentity)).thenReturn(null);

      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, null, mockSystemKeyCache);
      });

      assertTrue(exception.getMessage().contains("ManagedKeyDataCache is null"));
    }

    @Test
    public void testCreateEncryptionContext_WithKeyManagement_NullSystemKeyCache()
      throws IOException {
      byte[] keyBytes = "test-encrypted-key".getBytes();

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);

      // Enable key management
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
          null);
      });

      assertTrue(exception.getMessage()
        .contains("SystemKeyCache can't be null when using key management feature"));
    }
  }

  @RunWith(Parameterized.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestCreateEncryptionContext_WithoutKeyManagement_UnwrapKeyException
    extends TestSecurityUtil {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
      .forClass(TestCreateEncryptionContext_WithoutKeyManagement_UnwrapKeyException.class);

    @Parameter(0)
    public boolean isKeyException;

    @Parameterized.Parameters(name = "{index},isKeyException={0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] { { true }, { false }, });
    }

    @Test
    public void testWithDEK() throws IOException, KeyException {
      byte[] wrappedKey = createRandomWrappedKey(conf);
      MockAesKeyProvider keyProvider = (MockAesKeyProvider) Encryption.getKeyProvider(conf);
      keyProvider.clearKeys(); // Let a new key be instantiated and cause a unwrap failure.

      // No KEK identity so getEntry is never called; key comes from config and unwrap fails
      setupTrailerMocks(wrappedKey, null, new byte[0], null);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
          mockSystemKeyCache);
      });

      assertTrue(exception.getMessage().contains("Key was not successfully unwrapped"));
      // The root cause should be some kind of parsing/unwrapping exception
      assertNotNull(exception.getCause());
    }

    @Test
    public void testWithSystemKey() throws IOException {
      // Use invalid key bytes to trigger unwrapping failure
      byte[] invalidKeyBytes = INVALID_SYSTEM_KEY_DATA.getBytes();

      setupTrailerMocks(invalidKeyBytes, null, TEST_KEK_IDENTITY, null);
      configBuilder().withKeyManagement(false).apply(conf);
      setupSystemKeyCache(TEST_KEK_IDENTITY, mockManagedKeyData);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
          mockSystemKeyCache);
      });

      assertTrue(
        exception.getMessage().contains("Failed to unwrap key with KEK identity (length: 8)"));
      // The root cause should be some kind of parsing/unwrapping exception
      assertNotNull(exception.getCause());
    }
  }

  protected void verifyContext(Encryption.Context context) {
    verifyContext(context, true);
  }

  protected void verifyContext(Encryption.Context context, boolean withKeyManagement) {
    assertNotNull(context);
    assertNotNull("Context should have a cipher", context.getCipher());
    assertNotNull("Context should have a key", context.getKey());
    if (withKeyManagement) {
      assertNotNull("Context should have KEK data when key management is enabled",
        context.getKEKData());
    } else {
      assertNull("Context should not have KEK data when key management is disabled",
        context.getKEKData());
    }
  }

  /**
   * Null cipher provider for testing error cases.
   */
  public static class NullCipherProvider implements CipherProvider {
    private Configuration conf;

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public String getName() {
      return "null";
    }

    @Override
    public String[] getSupportedCiphers() {
      return new String[0];
    }

    @Override
    public Cipher getCipher(String name) {
      return null; // Always return null to simulate unavailable cipher
    }
  }
}
