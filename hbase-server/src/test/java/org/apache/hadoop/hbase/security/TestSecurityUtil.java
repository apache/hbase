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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Key;
import java.security.KeyException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@Category({ SecurityTests.class, SmallTests.class })
public class TestSecurityUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestSecurityUtil.class);

  private Configuration conf;
  private HBaseTestingUtil testUtil;
  private Path testPath;
  private ColumnFamilyDescriptor mockFamily;
  private ManagedKeyDataCache mockManagedKeyDataCache;
  private SystemKeyCache mockSystemKeyCache;
  private FixedFileTrailer mockTrailer;
  private ManagedKeyData mockManagedKeyData;
  private Key mockKey;
  private Cipher mockCipher;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    testUtil = new HBaseTestingUtil(conf);
    testPath = testUtil.getDataTestDir("test-file");

    // Setup mocks
    mockFamily = mock(ColumnFamilyDescriptor.class);
    mockManagedKeyDataCache = mock(ManagedKeyDataCache.class);
    mockSystemKeyCache = mock(SystemKeyCache.class);
    mockTrailer = mock(FixedFileTrailer.class);
    mockManagedKeyData = mock(ManagedKeyData.class);
    // Use a proper 16-byte key for AES (AES-128)
    mockKey = new SecretKeySpec("test-key-16-bytes".getBytes(), "AES");
    mockCipher = mock(Cipher.class);

    // Configure mocks
    when(mockFamily.getEncryptionType()).thenReturn("AES");
    when(mockFamily.getNameAsString()).thenReturn("test-family");
    when(mockCipher.getRandomKey()).thenReturn(mockKey);
    when(mockCipher.getName()).thenReturn("AES");
    when(mockManagedKeyData.getTheKey()).thenReturn(mockKey);
  }

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

  // Tests for the first createEncryptionContext method (for ColumnFamilyDescriptor)

  @Test
  public void testCreateEncryptionContext_WithNoEncryptionOnFamily() throws IOException {
    when(mockFamily.getEncryptionType()).thenReturn(null);

    Encryption.Context result = SecurityUtil.createEncryptionContext(
        conf, mockFamily, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

    assertEquals(Encryption.Context.NONE, result);
  }

  @Test
  public void testCreateEncryptionContext_WithEncryptionDisabled() throws IOException {
    // Mock Encryption.isEncryptionEnabled to return false
    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(false);

      IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockFamily, mockManagedKeyDataCache,
            mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("encryption feature is disabled"));
    }
  }

  @Test
  public void testCreateEncryptionContext_WithKeyManagement_LocalKeyGen() throws IOException {
    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY, true);

    when(mockManagedKeyDataCache.getActiveEntry(
        eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES), eq("test-namespace")))
        .thenReturn(mockManagedKeyData);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "AES")).thenReturn(mockCipher);

      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);

      Encryption.Context result = SecurityUtil.createEncryptionContext(
          conf, mockFamily, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      verifyContext(result);
    }
  }

  @Test
  public void testCreateEncryptionContext_WithKeyManagement_NoActiveKey() throws IOException {
    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

    when(mockManagedKeyDataCache.getActiveEntry(
        eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES), eq("test-namespace")))
        .thenReturn(null);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockFamily, mockManagedKeyDataCache,
            mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("No active key found"));
    }
  }

  @Test
  public void testCreateEncryptionContext_WithKeyManagement_LocalKeyGen_WithUnknownKeyCipher()
    throws IOException {
    when(mockFamily.getEncryptionType()).thenReturn("UNKNOWN_CIPHER");
    mockKey = mock(Key.class);
    when(mockKey.getAlgorithm()).thenReturn("UNKNOWN_CIPHER");
    when(mockManagedKeyData.getTheKey()).thenReturn(mockKey);

    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY, true);

    when(mockManagedKeyDataCache.getActiveEntry(
        eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES), eq("test-namespace")))
        .thenReturn(mockManagedKeyData);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);

      IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockFamily, mockManagedKeyDataCache,
            mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("Cipher 'UNKNOWN_CIPHER' is not available"));
    }
  }

  @Test
  public void testCreateEncryptionContext_WithKeyManagement_LocalKeyGen_WithKeyAlgorithmMismatch()
    throws IOException {
    mockKey = mock(Key.class);
    when(mockKey.getAlgorithm()).thenReturn("DES");
    when(mockManagedKeyData.getTheKey()).thenReturn(mockKey);

    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY, true);

    when(mockManagedKeyDataCache.getActiveEntry(
        eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES), eq("test-namespace")))
        .thenReturn(mockManagedKeyData);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);

      IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockFamily, mockManagedKeyDataCache,
            mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().equals("Encryption for family 'test-family' configured "
        + "with type 'AES' but key specifies algorithm 'DES'"));
    }
  }

  @Test
  public void testCreateEncryptionContext_WithKeyManagement_UseSystemKey() throws IOException {
    // Enable key management, but disable local key generation
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY, false);

    when(mockManagedKeyDataCache.getActiveEntry(
        eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES), eq("test-namespace")))
        .thenReturn(mockManagedKeyData);
    when(mockSystemKeyCache.getLatestSystemKey()).thenReturn(mockManagedKeyData);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "AES")).thenReturn(mockCipher);

      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);

      Encryption.Context result = SecurityUtil.createEncryptionContext(
          conf, mockFamily, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      verifyContext(result);
    }
  }

  @Test
  public void testCreateEncryptionContext_WithoutKeyManagement_WithFamilyProvidedKey() throws IOException {
    when(mockFamily.getEncryptionKey()).thenReturn("test-encrypted-key".getBytes());

    // Disable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class);
         MockedStatic<EncryptionUtil> mockedEncryptionUtil = Mockito.mockStatic(EncryptionUtil.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "AES")).thenReturn(mockCipher);

      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);
      mockedEncryptionUtil.when(() -> EncryptionUtil.unwrapKey(eq(conf), any(byte[].class)))
          .thenReturn(mockKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(
          conf, mockFamily, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      verifyContext(result, false);
    }
  }

  @Test
  public void testCreateEncryptionContext_WithoutKeyManagement_KeyAlgorithmMismatch() throws IOException {
    when(mockFamily.getEncryptionKey()).thenReturn("test-encrypted-key".getBytes());

    // Disable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);

    // Create a key with different algorithm
    Key differentKey = new SecretKeySpec("test-key-32-bytes-long-key-data".getBytes(), "DES");
    Cipher differentCipher = mock(Cipher.class);
    when(differentCipher.getName()).thenReturn("DES");

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class);
         MockedStatic<EncryptionUtil> mockedEncryptionUtil = Mockito.mockStatic(EncryptionUtil.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "DES")).thenReturn(differentCipher);
      mockedEncryptionUtil.when(() -> EncryptionUtil.unwrapKey(eq(conf), any(byte[].class)))
          .thenReturn(differentKey);

      IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockFamily, mockManagedKeyDataCache,
            mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().equals("Encryption for family 'test-family' configured "
        + "with type 'AES' but key specifies algorithm 'DES'"));
    }
  }

  @Test
  public void testCreateEncryptionContext_WithoutKeyManagement_WithRandomKeyGeneration() throws IOException {
    when(mockFamily.getEncryptionKey()).thenReturn(null);

    // Disable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "AES")).thenReturn(mockCipher);

      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);

      Encryption.Context result = SecurityUtil.createEncryptionContext(
          conf, mockFamily, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      verifyContext(result, false);
    }
  }

  @Test
  public void testCreateEncryptionContext_WithUnavailableCipher() throws IOException {
    when(mockFamily.getEncryptionType()).thenReturn("UNKNOWN_CIPHER");

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.isEncryptionEnabled(conf)).thenReturn(true);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "UNKNOWN_CIPHER")).thenReturn(null);

      IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, mockFamily, mockManagedKeyDataCache,
            mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("Cipher 'UNKNOWN_CIPHER' is not available"));
    }
  }

  // Tests for the second createEncryptionContext method (for reading files)

  @Test
  public void testCreateEncryptionContextForFile_WithNoKeyMaterial() throws IOException {
    when(mockTrailer.getEncryptionKey()).thenReturn(null);

    Encryption.Context result = SecurityUtil.createEncryptionContext(
        conf, testPath, mockTrailer, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

    assertNull(result);
  }

  @Test
  public void testCreateEncryptionContextForFile_WithKEKMetadata() throws Exception {
    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(256);
    SecretKey theKey = keyGen.generateKey();
    byte[] keyBytes = theKey.getEncoded();
    String kekMetadata = "test-kek-metadata";

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(kekMetadata);
    when(mockTrailer.getKEKChecksum()).thenReturn(12345L);

    when(mockManagedKeyDataCache.getEntry(
        eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES), eq("test-namespace"),
        eq(kekMetadata), eq(keyBytes)))
        .thenReturn(mockManagedKeyData);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class);
         MockedStatic<EncryptionUtil> mockedEncryptionUtil = Mockito.mockStatic(EncryptionUtil.class)) {
      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "AES")).thenReturn(mockCipher);
      mockedEncryptionUtil.when(() -> EncryptionUtil.unwrapKey(eq(conf), eq(null), eq(keyBytes), eq(mockKey)))
          .thenReturn(mockKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(
          conf, testPath, mockTrailer, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      verifyContext(result);
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithKeyManagement_KEKMetadataFailure() throws IOException, KeyException {
    byte[] keyBytes = "test-encrypted-key".getBytes();
    String kekMetadata = "test-kek-metadata";

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(kekMetadata);

    when(mockManagedKeyDataCache.getEntry(
        eq(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES), eq("test-namespace"),
        eq(kekMetadata), eq(keyBytes)))
        .thenThrow(new IOException("Key not found"));

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(Encryption.Context.NONE);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
            mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("Failed to get key data"));
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithKeyManagement_UseSystemKey() throws IOException {
    byte[] keyBytes = "test-encrypted-key".getBytes();
    long kekChecksum = 12345L;

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(null);
    when(mockTrailer.getKEKChecksum()).thenReturn(kekChecksum);

    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

    when(mockSystemKeyCache.getSystemKeyByChecksum(kekChecksum)).thenReturn(mockManagedKeyData);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class);
         MockedStatic<EncryptionUtil> mockedEncryptionUtil = Mockito.mockStatic(EncryptionUtil.class)) {
      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "AES")).thenReturn(mockCipher);
      mockedEncryptionUtil.when(() -> EncryptionUtil.unwrapKey(eq(conf), eq(null), eq(keyBytes), eq(mockKey)))
          .thenReturn(mockKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(
          conf, testPath, mockTrailer, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      verifyContext(result);
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithKeyManagement_SystemKeyNotFound() throws IOException {
    byte[] keyBytes = "test-encrypted-key".getBytes();
    long kekChecksum = 12345L;

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(null);
    when(mockTrailer.getKEKChecksum()).thenReturn(kekChecksum);

    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

    when(mockSystemKeyCache.getSystemKeyByChecksum(kekChecksum)).thenReturn(null);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(Encryption.Context.NONE);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
            mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("Failed to get system key"));
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithoutKeyManagemntEnabled() throws IOException {
    byte[] keyBytes = "test-encrypted-key".getBytes();

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(null);

    // Disable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class);
         MockedStatic<EncryptionUtil> mockedEncryptionUtil = Mockito.mockStatic(EncryptionUtil.class)) {
      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "AES")).thenReturn(mockCipher);
      mockedEncryptionUtil.when(() -> EncryptionUtil.unwrapKey(eq(conf), eq(keyBytes))).thenReturn(mockKey);

      Encryption.Context result = SecurityUtil.createEncryptionContext(
          conf, testPath, mockTrailer, mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");

      verifyContext(result, false);
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithoutKeyManagement_UnwrapFailure() throws IOException {
    byte[] keyBytes = "test-encrypted-key".getBytes();

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(null);

    // Disable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class);
         MockedStatic<EncryptionUtil> mockedEncryptionUtil = Mockito.mockStatic(EncryptionUtil.class)) {
      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);
      mockedEncryptionUtil.when(() -> EncryptionUtil.unwrapKey(eq(conf), eq(keyBytes)))
          .thenThrow(new IOException("Invalid key"));

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
            mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("Invalid key"));
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithoutKeyManagement_UnavailableCipher() throws IOException {
    byte[] keyBytes = "test-encrypted-key".getBytes();

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(null);

    // Disable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);

    // Create a key with different algorithm
    Key differentKey = new SecretKeySpec("test-key-16-bytes".getBytes(), "DES");

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class);
         MockedStatic<EncryptionUtil> mockedEncryptionUtil = Mockito.mockStatic(EncryptionUtil.class)) {
      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);
      mockedEncryption.when(() -> Encryption.getCipher(conf, "DES")).thenReturn(null);
      mockedEncryptionUtil.when(() -> EncryptionUtil.unwrapKey(eq(conf), eq(keyBytes))).thenReturn(differentKey);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
            mockManagedKeyDataCache, mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("not available"));
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithKeyManagement_NullKeyManagementCache() throws IOException {
    byte[] keyBytes = "test-encrypted-key".getBytes();
    String kekMetadata = "test-kek-metadata";

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(kekMetadata);

    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
            null, mockSystemKeyCache, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("ManagedKeyDataCache is null"));
    }
  }

  @Test
  public void testCreateEncryptionContextForFile_WithKeyManagement_NullSystemKeyCache() throws IOException {
    byte[] keyBytes = "test-encrypted-key".getBytes();

    when(mockTrailer.getEncryptionKey()).thenReturn(keyBytes);
    when(mockTrailer.getKEKMetadata()).thenReturn(null);

    // Enable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);

    try (MockedStatic<Encryption> mockedEncryption = Mockito.mockStatic(Encryption.class)) {
      // Create a proper encryption context
      Encryption.Context mockContext = mock(Encryption.Context.class);
      mockedEncryption.when(() -> Encryption.newContext(conf)).thenReturn(mockContext);

      IOException exception = assertThrows(IOException.class, () -> {
        SecurityUtil.createEncryptionContext(conf, testPath, mockTrailer,
            mockManagedKeyDataCache, null, "test-namespace");
      });

      assertTrue(exception.getMessage().contains("SystemKeyCache is null"));
    }
  }

  private void verifyContext(Encryption.Context mockContext) {
    verifyContext(mockContext, true);
  }

  private void verifyContext(Encryption.Context mockContext, boolean withKeyManagement) {
    assertNotNull(mockContext);
    verify(mockContext).setCipher(mockCipher);
    verify(mockContext).setKey(mockKey);
    if (withKeyManagement) {
      verify(mockContext).setKEKData(mockManagedKeyData);
    } else {
      verify(mockContext).setKEKData(null);
    }
  }
}
