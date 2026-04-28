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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.util.Arrays;
import java.util.Collection;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentity;
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

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.EncryptionProtos;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestSecurityUtil.TestBasic.class,
  TestSecurityUtil.TestCreateEncryptionContext_ForWrites.class,
  TestSecurityUtil.TestDeserializeEncryptionContext.class,
  TestSecurityUtil.TestDeserialize_WithoutKeyManagement_UnwrapKeyException.class,
  TestSecurityUtil.TestKeyWrapping.class, TestSecurityUtil.TestKeyWrappingWithHashAlgorithms.class,
  TestSecurityUtil.TestHashAlgorithmMismatch.class,
  TestSecurityUtil.TestCipherNamePropagation.class,
  TestSecurityUtil.TestEncryptDecryptRoundTrip.class, })
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
  protected static final String INVALID_KEY_DATA = "invalid-key-data";
  protected static final String INVALID_WRAPPED_KEY_DATA = "invalid-wrapped-key-data";
  protected static final String INVALID_SYSTEM_KEY_DATA = "invalid-system-key-data";
  protected static final String UNKNOWN_CIPHER = "UNKNOWN_CIPHER";
  protected static final String AES_CIPHER = "AES";
  protected static final String DES_CIPHER = "DES";

  protected Configuration conf;
  protected HBaseTestingUtil testUtil;
  protected ColumnFamilyDescriptor mockFamily;
  protected TableDescriptor mockTableDescriptor;
  protected ManagedKeyDataCache mockManagedKeyDataCache;
  protected SystemKeyCache mockSystemKeyCache;
  protected FixedFileTrailer mockTrailer;
  protected ManagedKeyData mockManagedKeyData;
  protected Key testKey;
  protected byte[] testWrappedKey;
  protected Key kekKey;
  protected ManagedKeyData kekData;

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
    byte[] enrichedKeyBytes = enrichWrappedKeyWithMetadata(keyBytes, kekIdentity, metadata);
    when(mockTrailer.getEncryptionKey()).thenReturn(enrichedKeyBytes);
  }

  /**
   * Re-serialize a WrappedKey protobuf with additional kek_identity and kek_metadata fields. Since
   * KEK metadata moved from the trailer to the WrappedKey protobuf, tests that need
   * identity/metadata must embed them in the serialized bytes.
   */
  private static byte[] enrichWrappedKeyWithMetadata(byte[] keyBytes, byte[] kekIdentity,
    String kekMetadata) {
    boolean hasIdentity = kekIdentity != null && kekIdentity.length > 0;
    if (!hasIdentity && kekMetadata == null) {
      return keyBytes;
    }
    try {
      EncryptionProtos.WrappedKey original =
        EncryptionProtos.WrappedKey.parser().parseDelimitedFrom(new ByteArrayInputStream(keyBytes));
      EncryptionProtos.WrappedKey.Builder builder = original.toBuilder();
      if (hasIdentity) {
        builder.setKekIdentity(UnsafeByteOperations.unsafeWrap(kekIdentity));
      }
      if (kekMetadata != null) {
        builder.setKekMetadata(kekMetadata);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      builder.build().writeDelimitedTo(out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to enrich wrapped key with metadata", e);
    }
  }

  /**
   * Create a syntactically valid WrappedKey protobuf with garbage encrypted data and the given
   * kek_identity. Used to test unwrap failure paths when KEK identity is present.
   */
  private static byte[] createCorruptWrappedKeyWithIdentity(Configuration conf,
    byte[] kekIdentity) {
    try {
      String hashAlgorithm = Encryption.getConfiguredHashAlgorithm(conf);
      Cipher cipher = Encryption.getCipher(conf, "AES");
      EncryptionProtos.WrappedKey.Builder builder = EncryptionProtos.WrappedKey.newBuilder();
      builder.setAlgorithm("AES");
      builder.setLength(16);
      builder.setHashAlgorithm(hashAlgorithm);
      builder.setHash(UnsafeByteOperations.unsafeWrap(new byte[32]));
      builder.setData(UnsafeByteOperations.unsafeWrap(new byte[16]));
      if (cipher.getIvLength() > 0) {
        builder.setIv(UnsafeByteOperations.unsafeWrap(new byte[cipher.getIvLength()]));
      }
      if (kekIdentity != null && kekIdentity.length > 0) {
        builder.setKekIdentity(UnsafeByteOperations.unsafeWrap(kekIdentity));
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      builder.build().writeDelimitedTo(out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
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
      SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(),
        mockSystemKeyCache, mockManagedKeyDataCache);
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
    kekData = createManagedKeyDataMock(kekKey, TEST_KEK_IDENTITY, TEST_KEK_METADATA);
    testWrappedKey = SecurityUtil.wrapKey(conf, testKey, kekData);
  }

  private static ManagedKeyData createManagedKeyDataMock(Key kekKey, byte[] kekIdentity,
    String kekMetadata) {
    ManagedKeyData kekData = mock(ManagedKeyData.class);
    when(kekData.getTheKey()).thenReturn(kekKey);
    ManagedKeyIdentity keyIdentity = mock(ManagedKeyIdentity.class);
    when(kekData.getKeyIdentity()).thenReturn(keyIdentity);
    when(keyIdentity.getFullIdentityView()).thenReturn(new Bytes(kekIdentity));
    if (kekMetadata != null) {
      when(kekData.getKeyMetadata()).thenReturn(kekMetadata);
    }
    return kekData;
  }

  private static byte[] createRandomWrappedKey(Configuration conf) throws IOException {
    Cipher cipher = Encryption.getCipher(conf, "AES");
    Key key = cipher.getRandomKey();
    return SecurityUtil.wrapKey(conf, HBASE_KEY, key);
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
        "Encryption for family 'test-family' is configured with cipher type 'AES' but the cipher algorithm 'AES' "
          + "doesn't match the key algorithm 'DES'");
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
      byte[] wrappedDESKey = SecurityUtil.wrapKey(conf, HBASE_KEY, differentKey);
      when(mockFamily.getEncryptionKey()).thenReturn(wrappedDESKey);

      assertEncryptionContextThrowsForWrites(IllegalStateException.class,
        "Encryption for family 'test-family' is configured with cipher type 'AES' but the cipher algorithm 'AES' "
          + "doesn't match the key algorithm 'DES'");
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
      ManagedKeyData wrongKekData =
        createManagedKeyDataMock(wrongKek, TEST_KEK_IDENTITY, TEST_KEK_METADATA);
      byte[] validWrappedKey = SecurityUtil.wrapKey(conf, testKey, wrongKekData);

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

  }

  // Tests for deserializeEncryptionContext (deserialization of WrappedKey protobuf)
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestDeserializeEncryptionContext extends TestSecurityUtil {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDeserializeEncryptionContext.class);

    @Test
    public void testWithNullValue() throws IOException {
      Encryption.Context result = SecurityUtil.deserializeEncryptionContext(conf, null, null,
        mockSystemKeyCache, mockManagedKeyDataCache);

      assertEquals(Encryption.Context.NONE, result);
    }

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
      setupSystemKeyCache(stkKeyData);

      // Also set up managed key cache (but it shouldn't be used since STK succeeds)
      setupManagedKeyDataCacheEntry(TEST_KEK_IDENTITY, TEST_KEK_METADATA, mockManagedKeyData);
      when(mockManagedKeyData.getTheKey())
        .thenThrow(new RuntimeException("This should not be called"));

      Encryption.Context result = SecurityUtil.deserializeEncryptionContext(conf, null,
        mockTrailer.getEncryptionKey(), mockSystemKeyCache, mockManagedKeyDataCache);

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

      Encryption.Context result = SecurityUtil.deserializeEncryptionContext(conf, null,
        mockTrailer.getEncryptionKey(), mockSystemKeyCache, mockManagedKeyDataCache);

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
      configBuilder().withKeyManagement(true).apply(conf);

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);

      when(mockSystemKeyCache.getSystemKeyByIdentity(any())).thenReturn(null);
      when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(null);

      assertThrows(Exception.class, () -> {
        SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(),
          mockSystemKeyCache, mockManagedKeyDataCache);
      });
    }

    @Test
    public void testBackwardsCompatibility_WithKeyManagement_FallbackToLatestSTK()
      throws IOException {
      // Test when no KEK identity in WrappedKey — should fall back to latest STK for unwrapping
      setupTrailerMocks(testWrappedKey, null, new byte[0], null);
      configBuilder().withKeyManagement(false).apply(conf);

      ManagedKeyData latestStk = mock(ManagedKeyData.class);
      when(latestStk.getTheKey()).thenReturn(kekKey);
      when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(latestStk);

      Encryption.Context result = SecurityUtil.deserializeEncryptionContext(conf, null,
        mockTrailer.getEncryptionKey(), mockSystemKeyCache, mockManagedKeyDataCache);

      verifyContext(result);
      assertEquals("Context should use latest STK as KEK", latestStk, result.getKEKData());
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

      Encryption.Context result = SecurityUtil.deserializeEncryptionContext(conf, null,
        mockTrailer.getEncryptionKey(), mockSystemKeyCache, mockManagedKeyDataCache);

      verifyContext(result);
      assertEquals(latestSystemKey, result.getKEKData());
    }

    @Test
    public void testWithoutKeyManagemntEnabled() throws IOException {
      byte[] wrappedKey = createRandomWrappedKey(conf);
      when(mockTrailer.getEncryptionKey()).thenReturn(wrappedKey);

      Encryption.Context result = SecurityUtil.deserializeEncryptionContext(conf, null,
        mockTrailer.getEncryptionKey(), mockSystemKeyCache, mockManagedKeyDataCache);

      verifyContext(result, false);
    }

    @Test
    public void testKeyManagementBackwardsCompatibility() throws Exception {
      when(mockTrailer.getEncryptionKey()).thenReturn(testWrappedKey);
      when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(mockManagedKeyData);
      when(mockManagedKeyData.getTheKey()).thenReturn(kekKey);
      configBuilder().withKeyManagement(false).apply(conf);

      Encryption.Context result = SecurityUtil.deserializeEncryptionContext(conf, null,
        mockTrailer.getEncryptionKey(), mockSystemKeyCache, mockManagedKeyDataCache);

      verifyContext(result, true);
    }

    @Test
    public void testWithoutKeyManagement_UnwrapFailure() throws IOException {
      byte[] invalidKeyBytes = INVALID_KEY_DATA.getBytes();
      when(mockTrailer.getEncryptionKey()).thenReturn(invalidKeyBytes);

      Exception exception = assertThrows(Exception.class, () -> {
        SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(),
          mockSystemKeyCache, mockManagedKeyDataCache);
      });

      // The exception should indicate that unwrapping failed - could be IOException or
      // RuntimeException
      assertNotNull(exception);
    }

    @Test
    public void testWithoutKeyManagement_UnavailableCipher() throws Exception {
      // Create a DES key and wrap it first with working configuration
      Key desKey = new SecretKeySpec("test-key-16-byte".getBytes(), "DES");
      byte[] wrappedDESKey = SecurityUtil.wrapKey(conf, HBASE_KEY, desKey);

      when(mockTrailer.getEncryptionKey()).thenReturn(wrappedDESKey);

      // Disable key management and use null cipher provider
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);
      setUpEncryptionConfigWithNullCipher();

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(),
          mockSystemKeyCache, mockManagedKeyDataCache);
      });

      assertTrue(exception.getMessage().contains("not available"));
    }

    @Test
    public void testWithKeyManagement_NullKeyManagementCache() throws IOException {
      byte[] keyBytes = "test-encrypted-key".getBytes();

      when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);

      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

      assertThrows(Exception.class, () -> {
        SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(),
          mockSystemKeyCache, null);
      });
    }

    @Test
    public void testWithKeyManagement_NullSystemKeyCache() throws IOException {
      byte[] wrappedKey = createRandomWrappedKey(conf);
      when(mockTrailer.getEncryptionKey()).thenReturn(wrappedKey);

      // Enable key management
      conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(), null,
          mockManagedKeyDataCache);
      });

      assertTrue(exception.getMessage()
        .contains("SystemKeyCache can't be null when using key management feature"));
    }
  }

  @RunWith(Parameterized.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestDeserialize_WithoutKeyManagement_UnwrapKeyException
    extends TestSecurityUtil {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDeserialize_WithoutKeyManagement_UnwrapKeyException.class);

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
        SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(),
          mockSystemKeyCache, mockManagedKeyDataCache);
      });

      assertNotNull(exception.getCause());
      assertTrue("Root cause should indicate key unwrap failure",
        exception.getCause().getMessage().contains("Key was not successfully unwrapped"));
    }

    @Test
    public void testWithSystemKey() throws IOException {
      // Create a valid WrappedKey protobuf with garbage data and embedded KEK identity
      byte[] corruptKeyBytes = createCorruptWrappedKeyWithIdentity(conf, TEST_KEK_IDENTITY);
      when(mockTrailer.getEncryptionKey()).thenReturn(corruptKeyBytes);
      configBuilder().withKeyManagement(false).apply(conf);
      setupSystemKeyCache(TEST_KEK_IDENTITY, mockManagedKeyData);
      setupSystemKeyCache(mockManagedKeyData);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.deserializeEncryptionContext(conf, null, mockTrailer.getEncryptionKey(),
          mockSystemKeyCache, mockManagedKeyDataCache);
      });

      assertNotNull(exception.getCause());
      assertTrue("Root cause should indicate key unwrap failure",
        exception.getCause().getMessage().contains("Key was not successfully unwrapped"));
      // The root cause should be some kind of parsing/unwrapping exception
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

  /**
   * Tests for key wrapping/unwrapping methods moved from EncryptionUtil.
   */
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestKeyWrapping extends TestSecurityUtil {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestKeyWrapping.class);

    @Test
    public void testKeyWrappingRoundTrip() throws Exception {
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Key key = new SecretKeySpec(keyBytes, algorithm);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertNotNull(wrappedKeyBytes);

      Key unwrappedKey = SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
      assertTrue(unwrappedKey instanceof SecretKeySpec);
      assertTrue("Unwrapped key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }

    @Test
    public void testUnwrapWithIncorrectKeyFails() throws Exception {
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      Key key = new SecretKeySpec(keyBytes, HConstants.CIPHER_AES);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertThrows(KeyException.class, () -> SecurityUtil.unwrapKey(conf, "other", wrappedKeyBytes));
    }

    @Test
    public void testWALKeyWrappingRoundTrip() throws Exception {
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      String algorithm = conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Key key = new SecretKeySpec(keyBytes, algorithm);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertNotNull(wrappedKeyBytes);

      Key unwrappedKey = SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
      assertTrue("Unwrapped WAL key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }

    @Test
    public void testUnwrapKeyIgnoresGlobalAlgorithmConfig() throws Exception {
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      Key key = new SecretKeySpec(keyBytes, HConstants.CIPHER_AES);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertNotNull(wrappedKeyBytes);

      // Change global, WAL, and alternate algorithm configs to a non-existent cipher.
      // Unwrap should still succeed because the cipher is read from the WrappedKey protobuf.
      conf.set(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, "NO_SUCH_CIPHER");
      conf.set(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, "NO_SUCH_CIPHER");
      conf.set(HConstants.CRYPTO_ALTERNATE_KEY_ALGORITHM_CONF_KEY, "NO_SUCH_CIPHER");

      Key unwrappedKey = SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
      assertTrue("Unwrapped key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }

    @Test
    public void testUnwrapKeyWithKekIgnoresGlobalAlgorithmConfig() throws Exception {
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      Key key = new SecretKeySpec(keyBytes, HConstants.CIPHER_AES);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, key, kekData);
      assertNotNull(wrappedKeyBytes);

      conf.set(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, "NO_SUCH_CIPHER");
      conf.set(HConstants.CRYPTO_ALTERNATE_KEY_ALGORITHM_CONF_KEY, "NO_SUCH_CIPHER");

      Key unwrappedKey = SecurityUtil.unwrapKey(conf, null, wrappedKeyBytes, kekKey);
      assertNotNull(unwrappedKey);
      assertTrue("Unwrapped key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }

    @Test
    public void testUnwrapKeyAutoMasterKeyIgnoresGlobalAlgorithmConfig() throws Exception {
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      Key key = new SecretKeySpec(keyBytes, HConstants.CIPHER_AES);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertNotNull(wrappedKeyBytes);

      conf.set(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, "NO_SUCH_CIPHER");
      conf.set(HConstants.CRYPTO_ALTERNATE_KEY_ALGORITHM_CONF_KEY, "NO_SUCH_CIPHER");

      Key unwrappedKey = SecurityUtil.unwrapKey(conf, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
      assertTrue("Unwrapped key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }

    @Test
    public void testUnwrapKeyAutoMasterKey() throws Exception {
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      Key key = new SecretKeySpec(keyBytes, HConstants.CIPHER_AES);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      Key unwrappedKey = SecurityUtil.unwrapKey(conf, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
      assertTrue("Unwrapped key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }

    @Test
    public void testKeyWrappingWithInvalidHashAlg() throws Exception {
      conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY,
        "this-hash-algorithm-not-exists hopefully... :)");
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Key key = new SecretKeySpec(keyBytes, algorithm);

      assertThrows(RuntimeException.class, () -> SecurityUtil.wrapKey(conf, HBASE_KEY, key));
    }
  }

  /**
   * Parameterized tests that exercise key wrapping/unwrapping with different hash algorithms. Each
   * parameter specifies the hash algorithm to use (MD5, SHA-256, SHA-384), or "use-default" for the
   * default. Both the standard key algorithm (CRYPTO_KEY_ALGORITHM_CONF_KEY) and WAL key algorithm
   * (CRYPTO_WAL_ALGORITHM_CONF_KEY) paths are tested for each hash algorithm.
   */
  @RunWith(Parameterized.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestKeyWrappingWithHashAlgorithms extends TestSecurityUtil {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestKeyWrappingWithHashAlgorithms.class);

    private static final String USE_DEFAULT = "use-default";

    @Parameter(0)
    public String hashAlgorithm;

    @Parameterized.Parameters(name = "hashAlgorithm={0}")
    public static Collection<Object[]> data() {
      return Arrays
        .asList(new Object[][] { { USE_DEFAULT }, { "MD5" }, { "SHA-256" }, { "SHA-384" }, });
    }

    @Override
    public void setUp() throws Exception {
      super.setUp();
      if (!hashAlgorithm.equals(USE_DEFAULT)) {
        conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, hashAlgorithm);
      }
    }

    @Test
    public void testKeyWrappingRoundTrip() throws Exception {
      byte[] keyBytes = new byte[Cipher.KEY_LENGTH];
      Bytes.secureRandom(keyBytes);
      String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Key key = new SecretKeySpec(keyBytes, algorithm);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertNotNull(wrappedKeyBytes);

      Key unwrappedKey = SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
      assertTrue(unwrappedKey instanceof SecretKeySpec);
      assertTrue("Unwrapped key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }

    @Test
    public void testUnwrapWithIncorrectKeyFails() throws Exception {
      byte[] keyBytes = new byte[Cipher.KEY_LENGTH];
      Bytes.secureRandom(keyBytes);
      String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Key key = new SecretKeySpec(keyBytes, algorithm);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertThrows(KeyException.class,
        () -> SecurityUtil.unwrapKey(conf, "other", wrappedKeyBytes));
    }

    @Test
    public void testWALKeyWrappingRoundTrip() throws Exception {
      byte[] keyBytes = new byte[Cipher.KEY_LENGTH];
      Bytes.secureRandom(keyBytes);
      String algorithm = conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Key key = new SecretKeySpec(keyBytes, algorithm);

      byte[] wrappedKeyBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);
      assertNotNull(wrappedKeyBytes);

      Key unwrappedKey = SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
      assertTrue(unwrappedKey instanceof SecretKeySpec);
      assertTrue("Unwrapped WAL key bytes do not match original",
        Bytes.equals(keyBytes, unwrappedKey.getEncoded()));
    }
  }

  /**
   * Tests for hash algorithm mismatch behavior during key unwrapping. Verifies that the
   * CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY configuration flag correctly controls whether
   * reading data encrypted with a different hash algorithm fails or succeeds.
   */
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestHashAlgorithmMismatch extends TestSecurityUtil {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHashAlgorithmMismatch.class);

    private byte[] wrapKeyWithMD5() throws Exception {
      conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, "MD5");
      byte[] keyBytes = new byte[Cipher.KEY_LENGTH];
      Bytes.secureRandom(keyBytes);
      String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Key key = new SecretKeySpec(keyBytes, algorithm);
      return SecurityUtil.wrapKey(conf, HBASE_KEY, key);
    }

    @Test
    public void testHashAlgorithmMismatchWhenFailExpected() throws Exception {
      byte[] wrappedKeyBytes = wrapKeyWithMD5();
      conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, "SHA-384");
      conf.setBoolean(Encryption.CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY, true);
      assertThrows(KeyException.class,
        () -> SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes));
    }

    @Test
    public void testHashAlgorithmMismatchWhenFailNotExpected() throws Exception {
      byte[] wrappedKeyBytes = wrapKeyWithMD5();
      conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, "SHA-384");
      conf.setBoolean(Encryption.CRYPTO_KEY_FAIL_ON_ALGORITHM_MISMATCH_CONF_KEY, false);
      Key unwrappedKey = SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
    }

    @Test
    public void testHashAlgorithmMismatchShouldNotFailWithDefaultConfig() throws Exception {
      byte[] wrappedKeyBytes = wrapKeyWithMD5();
      conf.set(Encryption.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, "SHA-384");
      Key unwrappedKey = SecurityUtil.unwrapKey(conf, HBASE_KEY, wrappedKeyBytes);
      assertNotNull(unwrappedKey);
    }
  }

  /**
   * Tests for cipher name propagation in serialize/deserializeEncryptionContext.
   */
  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestCipherNamePropagation extends TestSecurityUtil {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCipherNamePropagation.class);

    @Test
    public void testSerializeEncryptionContextStoresCipherName() throws Exception {
      Cipher cipher = Encryption.getCipher(conf, HConstants.CIPHER_AES);
      assertNotNull(cipher);
      Key key = cipher.getRandomKey();

      Encryption.Context context = Encryption.newContext(conf);
      context.setCipher(cipher);
      context.setKey(key);

      byte[] wrappedBytes = SecurityUtil.serializeEncryptionContext(context);
      assertNotNull(wrappedBytes);

      // Deserialize and verify cipher_name is set
      java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(wrappedBytes);
      org.apache.hadoop.hbase.shaded.protobuf.generated.EncryptionProtos.WrappedKey wrappedKey =
        org.apache.hadoop.hbase.shaded.protobuf.generated.EncryptionProtos.WrappedKey.parser()
          .parseDelimitedFrom(bais);
      assertTrue("cipher_name should be set", wrappedKey.hasCipherName());
      assertEquals("AES", wrappedKey.getCipherName());
    }

    @Test
    public void testDeserializeEncryptionContextPopulatesContext() throws Exception {
      Cipher cipher = Encryption.getCipher(conf, HConstants.CIPHER_AES);
      assertNotNull(cipher);
      Key key = cipher.getRandomKey();

      Encryption.Context wrapCtx = Encryption.newContext(conf);
      wrapCtx.setCipher(cipher);
      wrapCtx.setKey(key);

      byte[] wrappedBytes = SecurityUtil.serializeEncryptionContext(wrapCtx);

      Encryption.Context readCtx =
        SecurityUtil.deserializeEncryptionContext(conf, null, wrappedBytes, null, null);

      assertNotNull("Cipher should be populated on context", readCtx.getCipher());
      assertEquals("AES", readCtx.getCipher().getName());
      assertNotNull("Key should be populated on context", readCtx.getKey());
      assertTrue("Key bytes should match",
        Bytes.equals(key.getEncoded(), readCtx.getKey().getEncoded()));
    }

    @Test
    public void testBackwardCompatWithoutCipherName() throws Exception {
      // Old WrappedKey without cipher_name should fall back to algorithm field
      byte[] keyBytes = new byte[16];
      Bytes.secureRandom(keyBytes);
      Key key = new SecretKeySpec(keyBytes, HConstants.CIPHER_AES);

      byte[] wrappedBytes = SecurityUtil.wrapKey(conf, HBASE_KEY, key);

      Encryption.Context readCtx =
        SecurityUtil.deserializeEncryptionContext(conf, null, wrappedBytes, null, null);

      assertNotNull("Cipher should be populated from algorithm fallback", readCtx.getCipher());
      assertEquals("AES", readCtx.getCipher().getName());
    }
  }

  /**
   * Parameterized integration tests that exercise the full
   * createEncryptionContext-encrypt-serialize-deserialize-decrypt round trip, verifying that
   * changing the global cipher config after writing does not break reads. Each parameter pair
   * specifies the cipher used at write time and the global cipher config at read time.
   */
  @RunWith(Parameterized.class)
  @Category({ SecurityTests.class, SmallTests.class })
  public static class TestEncryptDecryptRoundTrip extends TestSecurityUtil {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestEncryptDecryptRoundTrip.class);

    @Parameter(0)
    public String writeCipher;

    @Parameter(1)
    public String globalCipherAtReadTime;

    @Parameterized.Parameters(name = "write={0},globalAtRead={1}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] { { HConstants.CIPHER_AES, "NO_SUCH_CIPHER" },
        { HConstants.CIPHER_AES_256_GCM, "NO_SUCH_CIPHER" }, });
    }

    private byte[] encryptData(Encryption.Context ctx, byte[] plaintext) throws IOException {
      byte[] iv = new byte[ctx.getCipher().getIvLength()];
      Bytes.secureRandom(iv);
      ByteArrayOutputStream cipherOut = new ByteArrayOutputStream();
      cipherOut.write(iv.length);
      cipherOut.write(iv);
      Encryption.encrypt(cipherOut, plaintext, 0, plaintext.length, ctx, iv);
      return cipherOut.toByteArray();
    }

    private byte[] decryptData(Encryption.Context ctx, byte[] encrypted, int plaintextLength)
      throws IOException {
      ByteArrayInputStream in = new ByteArrayInputStream(encrypted);
      int ivLen = in.read();
      byte[] iv = new byte[ivLen];
      in.read(iv);
      byte[] decrypted = new byte[plaintextLength];
      Encryption.decrypt(decrypted, 0, in, plaintextLength, ctx, iv);
      return decrypted;
    }

    @Test
    public void testDecryptAfterGlobalCipherChange() throws Exception {
      // Write path: create context, encrypt, serialize
      when(mockFamily.getEncryptionType()).thenReturn(writeCipher);
      when(mockFamily.getEncryptionKey()).thenReturn(null);
      Encryption.Context writeCtx =
        SecurityUtil.createEncryptionContext(conf, mockTableDescriptor, mockFamily, null, null);
      assertEquals(writeCipher, writeCtx.getCipher().getName());

      byte[] plaintext = Bytes.toBytes("Round-trip test data for cipher migration");
      byte[] encrypted = encryptData(writeCtx, plaintext);
      byte[] serialized = SecurityUtil.serializeEncryptionContext(writeCtx);

      // Simulate cipher config change between write and read
      conf.set(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, globalCipherAtReadTime);

      // Read path: deserialize should resolve the original cipher from the protobuf
      Encryption.Context readCtx =
        SecurityUtil.deserializeEncryptionContext(conf, null, serialized, null, null);

      assertEquals("Deserialized cipher should match the write cipher, not the global config",
        writeCipher, readCtx.getCipher().getName());
      assertTrue("Deserialized key should match original",
        Bytes.equals(writeCtx.getKeyBytes(), readCtx.getKeyBytes()));

      byte[] decrypted = decryptData(readCtx, encrypted, plaintext.length);
      assertTrue("Decrypted data should match original plaintext",
        Bytes.equals(plaintext, decrypted));
    }
  }
}
