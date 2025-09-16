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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.MockAesKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.keymeta.ManagedKeyDataCache;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
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
  protected static final String TEST_FAMILY = "test-family";
  protected static final String HBASE_KEY = "hbase";
  protected static final String TEST_KEK_METADATA = "test-kek-metadata";
  protected static final long TEST_KEK_CHECKSUM = 12345L;
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

    public TestConfigBuilder withKeyManagement(boolean enabled, boolean localKeyGen) {
      this.keyManagementEnabled = enabled;
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
        conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY, localKeyGenEnabled);
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
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    // Enable key caching
    conf.set(HConstants.CRYPTO_KEYPROVIDER_PARAMETERS_KEY, "true");
    // Use DefaultCipherProvider for real AES encryption functionality
    conf.set(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, "org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider");
  }

  protected void setUpEncryptionConfigWithNullCipher() {
    configBuilder().withNullCipherProvider().apply(conf);
  }

  protected byte[] createTestWrappedKey() throws Exception {
    // Create a test key and wrap it using real encryption utils
    KeyProvider keyProvider = Encryption.getKeyProvider(conf);
    kekKey = keyProvider.getKey(HBASE_KEY);
    Key key = keyProvider.getKey(TEST_DEK_16_BYTE);
    return EncryptionUtil.wrapKey(conf, null, key, kekKey);
  }

  // ==== Mock Setup Helpers ====

  protected void setupManagedKeyDataCache(String namespace, ManagedKeyData keyData) {
    when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES),
      eq(namespace))).thenReturn(keyData);
  }

  protected void setupManagedKeyDataCache(String namespace, String globalSpace, ManagedKeyData keyData) {
    when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES),
      eq(namespace))).thenReturn(null);
    when(mockManagedKeyDataCache.getActiveEntry(eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES),
      eq(globalSpace))).thenReturn(keyData);
  }

  protected void setupTrailerMocks(byte[] keyBytes, String metadata, Long checksum, String namespace) {
    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(metadata);
    if (checksum != null) {
      when(mockTrailer.getKEKChecksum()).thenReturn(checksum);
    }
    when(mockTrailer.getKeyNamespace()).thenReturn(namespace);
  }

  protected void setupSystemKeyCache(Long checksum, ManagedKeyData keyData) {
    when(mockSystemKeyCache.getSystemKeyByChecksum(checksum)).thenReturn(keyData);
  }

  protected void setupSystemKeyCache(ManagedKeyData latestKey) {
    when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(latestKey);
  }

  protected void setupManagedKeyDataCacheEntry(String namespace, String metadata,
      byte[] keyBytes, ManagedKeyData keyData) throws IOException, KeyException {
    when(mockManagedKeyDataCache.getEntry(eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES),
      eq(namespace), eq(metadata), eq(keyBytes))).thenReturn(keyData);
  }

  // ==== Exception Testing Helpers ====

  protected <T extends Exception> void assertExceptionContains(
      Class<T> expectedType, String expectedMessage, Runnable testCode) {
    T exception = assertThrows(expectedType, () -> testCode.run());
    assertTrue("Exception message should contain: " + expectedMessage,
        exception.getMessage().contains(expectedMessage));
  }

  protected void assertEncryptionContextThrowsForWrites(Class<? extends Exception> expectedType,
      String expectedMessage) {
    Exception exception = assertThrows(Exception.class, () -> {
      SecurityUtil.createEncryptionContext(conf, mockFamily, mockManagedKeyDataCache,
        mockSystemKeyCache, TEST_NAMESPACE);
    });
    assertTrue("Expected exception type: " + expectedType.getName() + ", but got: " + exception.getClass().getName(),
        expectedType.isInstance(exception));
    assertTrue("Exception message should contain: " + expectedMessage,
        exception.getMessage().contains(expectedMessage));
  }

  protected void assertEncryptionContextThrowsForReads(Class<? extends Exception> expectedType,
      String expectedMessage) {
    Exception exception = assertThrows(Exception.class, () -> {
      SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
        mockManagedKeyDataCache, mockSystemKeyCache);
    });
    assertTrue("Expected exception type: " + expectedType.getName() + ", but got: " + exception.getClass().getName(),
        expectedType.isInstance(exception));
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
    mockManagedKeyDataCache = mock(ManagedKeyDataCache.class);
    mockSystemKeyCache = mock(SystemKeyCache.class);
    mockTrailer = mock(FixedFileTrailer.class);
    mockManagedKeyData = mock(ManagedKeyData.class);

    // Use a real test key with exactly 16 bytes for AES-128
    testKey = new SecretKeySpec(TEST_KEY_16_BYTE.getBytes(), AES_CIPHER);

    // Configure mocks
    when(mockFamily.getEncryptionType()).thenReturn(AES_CIPHER);
    when(mockFamily.getNameAsString()).thenReturn(TEST_FAMILY);
    when(mockManagedKeyData.getTheKey()).thenReturn(testKey);

    // Set up default encryption config
    setUpEncryptionConfig();

    // Create test wrapped key
    testWrappedKey = createTestWrappedKey();
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

      Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockFamily,
        mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      assertEquals(Encryption.Context.NONE, result);
    }

    @Test
    public void testWithEncryptionDisabled() throws IOException {
      configBuilder().withEncryptionEnabled(false).apply(conf);
      assertEncryptionContextThrowsForWrites(IllegalStateException.class, "encryption feature is disabled");
    }

    @Test
    public void testWithKeyManagement_LocalKeyGen() throws IOException {
      configBuilder().withKeyManagement(true, true).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE, mockManagedKeyData);

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockFamily,
        mockManagedKeyDataCache, mockSystemKeyCache, TEST_NAMESPACE);

        verifyContext(result);
    }

    @Test
    public void testWithKeyManagement_NoActiveKey() throws IOException {
      configBuilder().withKeyManagement(true, false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE, ManagedKeyData.KEY_SPACE_GLOBAL, null);
      assertEncryptionContextThrowsForWrites(IOException.class, "No active key found");
    }

    @Test
    public void testWithKeyManagement_LocalKeyGen_WithUnknownKeyCipher() throws IOException {
      when(mockFamily.getEncryptionType()).thenReturn(UNKNOWN_CIPHER);
      Key unknownKey = mock(Key.class);
      when(unknownKey.getAlgorithm()).thenReturn(UNKNOWN_CIPHER);
      when(mockManagedKeyData.getTheKey()).thenReturn(unknownKey);

      configBuilder().withKeyManagement(true, true).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE, mockManagedKeyData);
      assertEncryptionContextThrowsForWrites(RuntimeException.class, "Cipher 'UNKNOWN_CIPHER' is not");
    }

    @Test
    public void testWithKeyManagement_LocalKeyGen_WithKeyAlgorithmMismatch() throws IOException {
      Key desKey = mock(Key.class);
      when(desKey.getAlgorithm()).thenReturn(DES_CIPHER);
      when(mockManagedKeyData.getTheKey()).thenReturn(desKey);

      configBuilder().withKeyManagement(true, true).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE, mockManagedKeyData);
      assertEncryptionContextThrowsForWrites(IllegalStateException.class,
        "Encryption for family 'test-family' configured with type 'AES' but key specifies algorithm 'DES'");
    }

    @Test
    public void testWithKeyManagement_UseSystemKeyWithNSSpecificActiveKey() throws IOException {
      configBuilder().withKeyManagement(true, false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE, mockManagedKeyData);
      setupSystemKeyCache(mockManagedKeyData);

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockFamily,
        mockManagedKeyDataCache, mockSystemKeyCache, TEST_NAMESPACE);

        verifyContext(result);
    }

    @Test
    public void testWithKeyManagement_UseSystemKeyWithoutNSSpecificActiveKey() throws IOException {
      configBuilder().withKeyManagement(true, false).apply(conf);
      setupManagedKeyDataCache(TEST_NAMESPACE, ManagedKeyData.KEY_SPACE_GLOBAL, mockManagedKeyData);
      setupSystemKeyCache(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockFamily,
        mockManagedKeyDataCache, mockSystemKeyCache, TEST_NAMESPACE);

        verifyContext(result);
    }

    @Test
    public void testWithoutKeyManagement_WithFamilyProvidedKey() throws Exception {
      when(mockFamily.getEncryptionKey()).thenReturn(testWrappedKey);
      configBuilder().withKeyManagement(false, false).apply(conf);

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockFamily,
        mockManagedKeyDataCache, mockSystemKeyCache, TEST_NAMESPACE);

        verifyContext(result, false);
    }

    @Test
    public void testWithoutKeyManagement_KeyAlgorithmMismatch() throws Exception {
      // Create a key with different algorithm and wrap it
      Key differentKey = new SecretKeySpec(TEST_KEY_16_BYTE.getBytes(), DES_CIPHER);
      byte[] wrappedDESKey = EncryptionUtil.wrapKey(conf, HBASE_KEY, differentKey);
      when(mockFamily.getEncryptionKey()).thenReturn(wrappedDESKey);

      configBuilder().withKeyManagement(false, false).apply(conf);
      assertEncryptionContextThrowsForWrites(IllegalStateException.class,
        "Encryption for family 'test-family' configured with type 'AES' but key specifies algorithm 'DES'");
    }

    @Test
    public void testWithoutKeyManagement_WithRandomKeyGeneration() throws IOException {
      when(mockFamily.getEncryptionKey()).thenReturn(null);
      configBuilder().withKeyManagement(false, false).apply(conf);

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, mockFamily,
        mockManagedKeyDataCache, mockSystemKeyCache, TEST_NAMESPACE);

        verifyContext(result, false);
    }

    @Test
    public void testWithUnavailableCipher() throws IOException {
      when(mockFamily.getEncryptionType()).thenReturn(UNKNOWN_CIPHER);
      setUpEncryptionConfigWithNullCipher();
      assertEncryptionContextThrowsForWrites(IllegalStateException.class, "Cipher 'UNKNOWN_CIPHER' is not available");
    }

    // Tests for the second createEncryptionContext method (for reading files)

    @Test
    public void testWithNoKeyMaterial() throws IOException {
      when(mockTrailer.getEncryptionKey()).thenReturn(null);
      when(mockTrailer.getKeyNamespace()).thenReturn(TEST_NAMESPACE);

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
    public void testWithKEKMetadata() throws Exception {
      setupTrailerMocks(testWrappedKey, TEST_KEK_METADATA,
TEST_KEK_CHECKSUM, TEST_NAMESPACE);
      setupManagedKeyDataCacheEntry(TEST_NAMESPACE, TEST_KEK_METADATA,
        testWrappedKey, mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath,
          mockTrailer, mockManagedKeyDataCache, mockSystemKeyCache);

        verifyContext(result);
    }

    @Test
    public void testWithKeyManagement_KEKMetadataFailure() throws IOException, KeyException {
      byte[] keyBytes = "test-encrypted-key".getBytes();
      String kekMetadata = "test-kek-metadata";

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(kekMetadata);
      when(mockTrailer.getKeyNamespace()).thenReturn("test-namespace");

      when(mockManagedKeyDataCache.getEntry(eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES),
        eq("test-namespace"), eq(kekMetadata), eq(keyBytes)))
        .thenThrow(new IOException("Key not found"));

        IOException exception = assertThrows(IOException.class, () -> {
          SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
            mockSystemKeyCache);
        });

        assertTrue(exception.getMessage().contains("Failed to get key data"));
    }

    @Test
    public void testWithKeyManagement_UseSystemKey() throws IOException {
      setupTrailerMocks(testWrappedKey, null, TEST_KEK_CHECKSUM, TEST_NAMESPACE);
      configBuilder().withKeyManagement(true, false).apply(conf);
      setupSystemKeyCache(TEST_KEK_CHECKSUM, mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath,
          mockTrailer, mockManagedKeyDataCache, mockSystemKeyCache);

        verifyContext(result);
    }

    @Test
    public void testWithKeyManagement_SystemKeyNotFound() throws IOException {
      byte[] keyBytes = "test-encrypted-key".getBytes();
      long kekChecksum = 12345L;

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);
      when(mockTrailer.getKEKChecksum()).thenReturn(kekChecksum);
      when(mockTrailer.getKeyNamespace()).thenReturn("test-namespace");

      // Enable key management
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

      when(mockSystemKeyCache.getSystemKeyByChecksum(kekChecksum)).thenReturn(null);

        IOException exception = assertThrows(IOException.class, () -> {
          SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
            mockSystemKeyCache);
        });

        assertTrue(exception.getMessage().contains("Failed to get system key"));
    }

    @Test
    public void testWithoutKeyManagemntEnabled() throws IOException {
      when(mockTrailer.getEncryptionKey()).thenReturn(testWrappedKey);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);
      when(mockTrailer.getKeyNamespace()).thenReturn(TEST_NAMESPACE);
      configBuilder().withKeyManagement(false, false).apply(conf);
      // TODO: Get the key provider to return kek when getKeys() is called.

        Encryption.Context result = SecurityUtil.createEncryptionContext(conf, testPath,
          mockTrailer, mockManagedKeyDataCache, mockSystemKeyCache);

        verifyContext(result, false);
    }

    @Test
    public void testWithoutKeyManagement_UnwrapFailure() throws IOException {
      byte[] invalidKeyBytes = INVALID_KEY_DATA.getBytes();
      when(mockTrailer.getEncryptionKey()).thenReturn(invalidKeyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);
      when(mockTrailer.getKeyNamespace()).thenReturn(TEST_NAMESPACE);
      configBuilder().withKeyManagement(false, false).apply(conf);

      Exception exception = assertThrows(Exception.class, () -> {
          SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
            mockSystemKeyCache);
        });

      // The exception should indicate that unwrapping failed - could be IOException or RuntimeException
      assertNotNull(exception);
    }

    @Test
    public void testCreateEncryptionContext_WithoutKeyManagement_UnavailableCipher()
      throws Exception {
      // Create a DES key and wrap it first with working configuration
      Key desKey = new SecretKeySpec("test-key-16-byte".getBytes(), "DES");
      byte[] wrappedDESKey = EncryptionUtil.wrapKey(conf, "hbase", desKey);

      when(mockTrailer.getEncryptionKey()).thenReturn(wrappedDESKey);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);
      when(mockTrailer.getKeyNamespace()).thenReturn("test-namespace");

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

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(kekMetadata);
      when(mockTrailer.getKeyNamespace()).thenReturn("test-namespace");

      // Enable key management
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

        IOException exception = assertThrows(IOException.class, () -> {
          SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, null,
            mockSystemKeyCache);
        });

        assertTrue(exception.getMessage().contains("ManagedKeyDataCache is null"));
    }

    @Test
    public void testCreateEncryptionContext_WithKeyManagement_NullSystemKeyCache()
      throws IOException {
      byte[] keyBytes = "test-encrypted-key".getBytes();

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
      when(mockTrailer.getKEKMetadata()).thenReturn(null);
      when(mockTrailer.getKeyNamespace()).thenReturn("test-namespace");

      // Enable key management
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

        IOException exception = assertThrows(IOException.class, () -> {
          SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
            null);
        });

        assertTrue(exception.getMessage().contains("SystemKeyCache is null"));
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
    public void test() throws IOException {
    }

    @Test
    public void testWithDEK() throws IOException, KeyException {
      // This test is challenging because we need to create a scenario where unwrapping fails
      // with either KeyException or IOException. We'll create invalid wrapped data.
      byte[] invalidKeyBytes = INVALID_WRAPPED_KEY_DATA.getBytes();

      setupTrailerMocks(invalidKeyBytes, TEST_KEK_METADATA,
TEST_KEK_CHECKSUM, TEST_NAMESPACE);
      setupManagedKeyDataCacheEntry(TEST_NAMESPACE, TEST_KEK_METADATA,
        invalidKeyBytes, mockManagedKeyData);

        IOException exception = assertThrows(IOException.class, () -> {
          SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
            mockSystemKeyCache);
        });

        assertTrue(exception.getMessage().contains(
        "Failed to unwrap key with KEK checksum: " + TEST_KEK_CHECKSUM + ", metadata: " + TEST_KEK_METADATA));
      // The root cause should be some kind of parsing/unwrapping exception
      assertNotNull(exception.getCause());
    }

    @Test
    public void testWithSystemKey() throws IOException {
      // Use invalid key bytes to trigger unwrapping failure
      byte[] invalidKeyBytes = INVALID_SYSTEM_KEY_DATA.getBytes();

      setupTrailerMocks(invalidKeyBytes, null, TEST_KEK_CHECKSUM, TEST_NAMESPACE);
      configBuilder().withKeyManagement(true, false).apply(conf);
      setupSystemKeyCache(TEST_KEK_CHECKSUM, mockManagedKeyData);

        IOException exception = assertThrows(IOException.class, () -> {
          SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer, mockManagedKeyDataCache,
            mockSystemKeyCache);
        });

        assertTrue(exception.getMessage()
        .contains("Failed to unwrap key with KEK checksum: " + TEST_KEK_CHECKSUM + ", metadata: null"));
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
      assertNotNull("Context should have KEK data when key management is enabled", context.getKEKData());
    } else {
      assertNull("Context should not have KEK data when key management is disabled", context.getKEKData());
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
