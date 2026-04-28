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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.keymeta.KeyIdentitySingleArrayBacked;
import org.apache.hadoop.hbase.keymeta.ManagedKeyDataCache;
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentity;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.EncryptionProtos;

/**
 * Security related generic utility methods.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class SecurityUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

  private SecurityUtil() {
    // Utility class
  }

  /**
   * Get the user name from a principal
   */
  public static String getUserFromPrincipal(final String principal) {
    int i = principal.indexOf("/");
    if (i == -1) {
      i = principal.indexOf("@");
    }
    return (i > -1) ? principal.substring(0, i) : principal;
  }

  /**
   * Get the user name from a principal
   */
  public static String getPrincipalWithoutRealm(final String principal) {
    int i = principal.indexOf("@");
    return (i > -1) ? principal.substring(0, i) : principal;
  }

  /**
   * Protect a key by encrypting it with the secret key of the given subject. The configuration must
   * be set up correctly for key alias resolution.
   * @param conf      configuration
   * @param key       the raw key bytes
   * @param algorithm the algorithm to use with this key material
   * @return the encrypted key bytes
   */
  public static byte[] wrapKey(Configuration conf, byte[] key, String algorithm)
    throws IOException {
    return wrapKey(conf,
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),
      new SecretKeySpec(key, algorithm));
  }

  /**
   * Protect a key by encrypting it with the secret key of the given subject. The configuration must
   * be set up correctly for key alias resolution.
   * @param conf    configuration
   * @param subject subject key alias
   * @param key     the key
   * @return the encrypted key bytes
   */
  public static byte[] wrapKey(Configuration conf, String subject, Key key) throws IOException {
    return wrapKeyAndSerialize(conf, null, subject, key, null, false);
  }

  /**
   * Protect a key by encrypting it with the secret key of the given subject or kek. The
   * configuration must be set up correctly for key alias resolution.
   * @param conf    configuration
   * @param key     the key
   * @param kekData the kek data
   * @return the encrypted key bytes
   */
  public static byte[] wrapKey(Configuration conf, Key key, ManagedKeyData kekData)
    throws IOException {
    return wrapKeyAndSerialize(conf, null, null, key, kekData, false);
  }

  /**
   * Core key-wrapping logic shared by the public {@code wrapKey} variants and
   * {@link #serializeEncryptionContext}. Builds a self-contained WrappedKey protobuf that includes
   * the cipher name and optional KEK identity/metadata.
   * @param conf              configuration
   * @param cipher            the cipher to use for encrypting the key material, if @{code null} it
   *                          is derived from the global configuration
   * @param subject           subject key alias, may be null if {@code kek} is provided
   * @param key               the data encryption key to wrap
   * @param kekData           the kek, may be null if {@code subject} is provided
   * @param encodeKekMetadata whether to encode the kek metadata
   * @return the serialized WrappedKey protobuf bytes
   */
  private static byte[] wrapKeyAndSerialize(Configuration conf, Cipher cipher, String subject,
    Key key, ManagedKeyData kekData, boolean encodeKekMetadata) throws IOException {
    if (cipher == null) {
      String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      cipher = Encryption.getCipher(conf, algorithm);
      if (cipher == null) {
        throw new RuntimeException("Cipher '" + algorithm + "' not available");
      }
    }
    EncryptionProtos.WrappedKey.Builder builder = EncryptionProtos.WrappedKey.newBuilder();
    builder.setAlgorithm(key.getAlgorithm());
    builder.setCipherName(cipher.getName());
    Key kek = null;
    if (kekData != null) {
      kek = kekData.getTheKey();
      Bytes kekIdentity = kekData.getKeyIdentity().getFullIdentityView();
      builder.setKekIdentity(UnsafeByteOperations.unsafeWrap(kekIdentity.get(),
        kekIdentity.getOffset(), kekIdentity.getLength()));
      if (encodeKekMetadata) {
        builder.setKekMetadata(kekData.getKeyMetadata());
      }
    } else {
      subject =
        conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName());
    }
    byte[] iv = null;
    if (cipher.getIvLength() > 0) {
      iv = new byte[cipher.getIvLength()];
      Bytes.secureRandom(iv);
      builder.setIv(UnsafeByteOperations.unsafeWrap(iv));
    }
    byte[] keyBytes = key.getEncoded();
    builder.setLength(keyBytes.length);
    builder.setHashAlgorithm(Encryption.getConfiguredHashAlgorithm(conf));
    builder
      .setHash(UnsafeByteOperations.unsafeWrap(Encryption.computeCryptoKeyHash(conf, keyBytes)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    if (kek != null) {
      Encryption.encryptWithGivenKey(kek, out, new ByteArrayInputStream(keyBytes), cipher, iv);
    } else {
      Encryption.encryptWithSubjectKey(out, new ByteArrayInputStream(keyBytes), subject, conf,
        cipher, iv);
    }
    builder.setData(UnsafeByteOperations.unsafeWrap(out.toByteArray()));
    out.reset();
    builder.build().writeDelimitedTo(out);
    return out.toByteArray();
  }

  /**
   * Helper for {@link #unwrapKey(Configuration, String, byte[])} which automatically uses the
   * configured master and alternative keys, rather than having to specify a key type to unwrap
   * with. The configuration must be set up correctly for key alias resolution.
   * @param conf            the current configuration
   * @param wrappedKeyBytes the key encrypted by master (or alternative) to unwrap
   * @return the key bytes, decrypted
   * @throws IOException if the key cannot be unwrapped
   */
  public static Key unwrapKey(Configuration conf, byte[] wrappedKeyBytes) throws IOException {
    Key key;
    String masterKeyName =
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName());
    try {
      key = unwrapKey(conf, masterKeyName, wrappedKeyBytes);
    } catch (KeyException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to unwrap key with current master key '" + masterKeyName + "'");
      }
      String alternateKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
      if (alternateKeyName != null) {
        try {
          key = unwrapKey(conf, alternateKeyName, wrappedKeyBytes);
        } catch (KeyException ex) {
          throw new IOException(ex);
        }
      } else {
        throw new IOException(e);
      }
    }
    return key;
  }

  /**
   * Unwrap a key by decrypting it with the secret key of the given subject. The configuration must
   * be set up correctly for key alias resolution.
   * @param conf            configuration
   * @param subject         subject key alias
   * @param wrappedKeyBytes the encrypted key bytes
   * @return the raw key bytes
   */
  public static Key unwrapKey(Configuration conf, String subject, byte[] wrappedKeyBytes)
    throws IOException, KeyException {
    return unwrapKey(conf, subject, wrappedKeyBytes, null);
  }

  /**
   * Unwrap a key by decrypting it with the secret key of the given subject. The configuration must
   * be set up correctly for key alias resolution. Only one of the {@code subject} or {@code kek}
   * needs to be specified and the other one can be {@code null}.
   * @param conf            configuration
   * @param subject         subject key alias
   * @param wrappedKeyBytes the encrypted key bytes
   * @param kek             the key encryption key
   * @return the raw key bytes
   */
  public static Key unwrapKey(Configuration conf, String subject, byte[] wrappedKeyBytes, Key kek)
    throws IOException, KeyException {
    EncryptionProtos.WrappedKey wrappedKey = EncryptionProtos.WrappedKey.parser()
      .parseDelimitedFrom(new ByteArrayInputStream(wrappedKeyBytes));
    Cipher cipher = getCipherFromWrappedKey(conf, wrappedKey);
    return getUnwrapKey(conf, subject, wrappedKey, cipher, kek);
  }

  private static Key getUnwrapKey(Configuration conf, String subject,
    EncryptionProtos.WrappedKey wrappedKey, Cipher cipher, Key kek)
    throws IOException, KeyException {
    String configuredHashAlgorithm = Encryption.getConfiguredHashAlgorithm(conf);
    String wrappedHashAlgorithm = wrappedKey.getHashAlgorithm().trim();
    if (!configuredHashAlgorithm.equalsIgnoreCase(wrappedHashAlgorithm)) {
      String msg = String.format("Unexpected encryption key hash algorithm: %s (expecting: %s)",
        wrappedHashAlgorithm, configuredHashAlgorithm);
      if (Encryption.failOnHashAlgorithmMismatch(conf)) {
        throw new KeyException(msg);
      }
      LOG.debug(msg);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] iv = wrappedKey.hasIv() ? wrappedKey.getIv().toByteArray() : null;
    if (kek != null) {
      Encryption.decryptWithGivenKey(kek, out, wrappedKey.getData().newInput(),
        wrappedKey.getLength(), cipher, iv);
    } else {
      Encryption.decryptWithSubjectKey(out, wrappedKey.getData().newInput(), wrappedKey.getLength(),
        subject, conf, cipher, iv);
    }
    byte[] keyBytes = out.toByteArray();
    if (wrappedKey.hasHash()) {
      if (
        !Bytes.equals(wrappedKey.getHash().toByteArray(),
          Encryption.hashWithAlg(wrappedHashAlgorithm, keyBytes))
      ) {
        throw new KeyException("Key was not successfully unwrapped");
      }
    }
    return new SecretKeySpec(keyBytes, wrappedKey.getAlgorithm());
  }

  /**
   * Resolve the cipher that was used to wrap a key, reading the cipher name from the WrappedKey
   * protobuf itself (falls back to algorithm field for legacy files without cipher_name).
   */
  private static Cipher getCipherFromWrappedKey(Configuration conf,
    EncryptionProtos.WrappedKey wrappedKey) {
    String cipherName =
      wrappedKey.hasCipherName() ? wrappedKey.getCipherName() : wrappedKey.getAlgorithm();
    Cipher cipher = Encryption.getCipher(conf, cipherName);
    if (cipher == null) {
      throw new RuntimeException("Cipher '" + cipherName + "' not available");
    }
    return cipher;
  }

  /**
   * Helper to create an encyption context with current encryption key, suitable for writes.
   * @param conf                The current configuration.
   * @param tableDescriptor     The table descriptor.
   * @param family              The current column descriptor.
   * @param managedKeyDataCache The managed key data cache.
   * @param systemKeyCache      The system key cache.
   * @return The created encryption context.
   * @throws IOException           if an encryption key for the column cannot be unwrapped
   * @throws IllegalStateException in case of encryption related configuration errors
   */
  public static Encryption.Context createEncryptionContext(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor family,
    ManagedKeyDataCache managedKeyDataCache, SystemKeyCache systemKeyCache) throws IOException {
    Encryption.Context cryptoContext = Encryption.Context.NONE;
    boolean isKeyManagementEnabled = isKeyManagementEnabled(conf);
    String cipherName = family.getEncryptionType();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating encryption context for table: {} and column family: {}",
        tableDescriptor.getTableName().getNameAsString(), family.getNameAsString());
    }
    if (cipherName != null) {
      if (!Encryption.isEncryptionEnabled(conf)) {
        throw new IllegalStateException("Encryption for family '" + family.getNameAsString()
          + "' configured with type '" + cipherName + "' but the encryption feature is disabled");
      }
      if (isKeyManagementEnabled && systemKeyCache == null) {
        throw new IOException("Key management is enabled, but SystemKeyCache is null");
      }
      Cipher cipher = null;
      Key key = null;
      ManagedKeyData kekKeyData =
        isKeyManagementEnabled ? systemKeyCache.getLatestSystemKey() : null;

      // Scenario 1: If family has a key, unwrap it and use that as CEK.
      byte[] familyKeyBytes = family.getEncryptionKey();
      if (familyKeyBytes != null) {
        try {
          if (isKeyManagementEnabled) {
            // Scenario 1a: If key management is enabled, use STK for both unwrapping and KEK.
            key = unwrapKey(conf, null, familyKeyBytes, kekKeyData.getTheKey());
          } else {
            // Scenario 1b: If key management is disabled, unwrap the key using master key.
            key = unwrapKey(conf, familyKeyBytes);
          }
          LOG.debug("Scenario 1: Use family key for cipher: {} key management enabled: {}",
            cipherName, isKeyManagementEnabled);
        } catch (KeyException e) {
          throw new IOException(e);
        }
      } else {
        if (isKeyManagementEnabled) {
          boolean localKeyGenEnabled =
            conf.getBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY,
              HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_DEFAULT_ENABLED);
          Bytes keyNamespaceCFAttribute = family.getEncryptionKeyNamespaceBytes();
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "Looking for active key for table: {} and column family: {} with "
                + "encryption key namespace: {} and global namespace(custodian: {}, namespace: {})",
              tableDescriptor.getTableName().getNameAsString(), family.getNameAsString(),
              keyNamespaceCFAttribute, ManagedKeyData.GLOBAL_CUST_ENCODED,
              ManagedKeyData.KEY_SPACE_GLOBAL);
          }
          // Implement 2-step fallback logic for key namespace resolution in the order of
          // 1. CF KEY_NAMESPACE attribute
          // 2. Global namespace
          ManagedKeyData activeKeyData = keyNamespaceCFAttribute != null
            ? managedKeyDataCache.getActiveEntry(ManagedKeyData.KEY_SPACE_GLOBAL_BYTES,
              keyNamespaceCFAttribute)
            : null;
          if (activeKeyData == null) {
            activeKeyData = managedKeyDataCache.getActiveEntry(
              ManagedKeyData.KEY_SPACE_GLOBAL_BYTES, ManagedKeyData.KEY_SPACE_GLOBAL_BYTES);
          }

          // Scenario 2: There is an active key
          if (activeKeyData != null) {
            if (!localKeyGenEnabled) {
              // Scenario 2a: Use active key as CEK and latest STK as KEK
              key = activeKeyData.getTheKey();
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                  "Scenario 2a: Use active key as CEK with (custodian: {}, namespace: {}) and "
                    + " STK as KEK for cipher: {} for table: {} and column family: {}",
                  activeKeyData.getKeyCustodianEncoded(), activeKeyData.getKeyNamespace(),
                  cipherName, tableDescriptor.getTableName().getNameAsString(),
                  family.getNameAsString());
              }
            } else {
              // Scenario 2b: Use active key (DEK) as KEK and let a local key be generated as CEK
              // later.
              kekKeyData = activeKeyData;
              // TODO: Use the active key as a seed to generate the local key instead of
              // random generation
              cipher = getCipherIfValid(conf, cipherName, activeKeyData.getTheKey(),
                family.getNameAsString());
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                  "Scenario 2b: Use random key as CEK and active key as KEK with (custodian: {}, "
                    + "namespace: {}) for cipher: {} for table: {} and column family: {}",
                  activeKeyData.getKeyCustodianEncoded(), activeKeyData.getKeyNamespace(),
                  cipherName, tableDescriptor.getTableName().getNameAsString(),
                  family.getNameAsString());
              }
            }
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Scenario 3a: No active key found for table: {} and column family: {}",
                tableDescriptor.getTableName().getNameAsString(), family.getNameAsString());
            }
            // Scenario 3a: Do nothing, let a random key be generated as CEK and if key management
            // is enabled, let STK be used as KEK.
          }
        } else {
          // Scenario 3b: Do nothing, let a random key be generated as CEK, let STK be used as KEK.
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "Scenario 3b: Key management is disabled and no ENCRYPTION_KEY attribute "
                + "set for table: {} and column family: {}",
              tableDescriptor.getTableName().getNameAsString(), family.getNameAsString());
          }
        }
      }
      if (LOG.isDebugEnabled() && kekKeyData != null) {
        LOG.debug(
          "Usigng KEK with (custodian: {}, namespace: {}), checksum: {} and metadata " + "hash: {}",
          kekKeyData.getKeyCustodianEncoded(), kekKeyData.getKeyNamespace(),
          kekKeyData.getKeyChecksum(), kekKeyData.getPartialIdentityEncoded());
      }

      if (cipher == null) {
        cipher =
          getCipherIfValid(conf, cipherName, key, key == null ? null : family.getNameAsString());
      }
      if (key == null) {
        key = cipher.getRandomKey();
      }
      cryptoContext = Encryption.newContext(conf);
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
      cryptoContext.setKEKData(kekKeyData);
    }
    return cryptoContext;
  }

  /**
   * Deserialize a WrappedKey protobuf back into a fully-populated encryption context, suitable for
   * read. Resolves the KEK from the embedded identity/metadata, unwraps the data encryption key,
   * and sets the cipher. Returns {@link Encryption.Context#NONE} if {@code value} is null (no
   * encryption). The inverse operation is {@link #serializeEncryptionContext}.
   * @param conf                   the current configuration
   * @param path                   the file path (used for debug logging only, may be null)
   * @param encryptionContextBytes the serialized WrappedKey protobuf bytes, or null if unencrypted
   * @param systemKeyCache         the system key cache (may be null if key management is disabled)
   * @param managedKeyDataCache    the managed key data cache (may be null)
   * @return a fully-populated encryption context, or {@link Encryption.Context#NONE}
   * @throws IOException if the key cannot be unwrapped
   */
  public static Encryption.Context deserializeEncryptionContext(Configuration conf, Path path,
    byte[] encryptionContextBytes, SystemKeyCache systemKeyCache,
    ManagedKeyDataCache managedKeyDataCache) throws IOException {
    if (encryptionContextBytes == null) {
      return Encryption.Context.NONE;
    }
    LOG.debug("Creating encryption context for path: {}", path);
    EncryptionProtos.WrappedKey wrappedKey = EncryptionProtos.WrappedKey.parser()
      .parseDelimitedFrom(new ByteArrayInputStream(encryptionContextBytes));

    boolean isKeyManagementEnabled = isKeyManagementEnabled(conf);
    ManagedKeyData kekKeyData = null;
    boolean hasKekIdentity = wrappedKey.hasKekIdentity();
    byte[] kekIdentity = hasKekIdentity ? wrappedKey.getKekIdentity().toByteArray() : null;
    if ((hasKekIdentity || isKeyManagementEnabled) && systemKeyCache == null) {
      throw new IOException("SystemKeyCache can't be null when using key management feature");
    }
    if (hasKekIdentity && !isKeyManagementEnabled) {
      throw new IOException("Seeing WrappedKey with KEK identity, but key management is disabled");
    }

    // Try STK lookup first if full identity is available
    if (hasKekIdentity) {
      LOG.debug("Looking for System Key by identity (length: {})", kekIdentity.length);
      kekKeyData = systemKeyCache.getSystemKeyByIdentity(kekIdentity);
    }

    // If STK lookup failed, try managed key lookup by partial identity and
    // metadata (if available)
    if (kekKeyData == null && hasKekIdentity) {
      if (managedKeyDataCache == null) {
        throw new IOException("KEK identity is available, but ManagedKeyDataCache is null");
      }
      ManagedKeyIdentity keyIdentity = new KeyIdentitySingleArrayBacked(kekIdentity);
      String kekMetadata = wrappedKey.hasKekMetadata() ? wrappedKey.getKekMetadata() : null;
      kekKeyData = managedKeyDataCache.getEntry(keyIdentity, kekMetadata, null);
    }

    if (kekKeyData == null && isKeyManagementEnabled) {
      // No identity or metadata available, fall back to latest system key for backwards
      // compatibility
      kekKeyData = systemKeyCache.getLatestSystemKey();
    }

    // Resolve the data cipher from cipher_name (fall back to algorithm for old files)
    String cipherName =
      wrappedKey.hasCipherName() ? wrappedKey.getCipherName() : wrappedKey.getAlgorithm();
    Cipher dataCipher = Encryption.getCipher(conf, cipherName);
    if (dataCipher == null) {
      throw new IOException("Cipher '" + cipherName + "' is not available");
    }

    // Unwrap the DEK using the cipher that was recorded in the WrappedKey protobuf.
    // Try KEK-based unwrap first if available, then fall back to subject-based unwrapping
    // (master key, then alternate). The fallback is important during migration from legacy
    // encryption to managed key encryption: old HFiles written before key management was
    // enabled have no KEK identity, so the code above optimistically sets the KEK to the
    // latest STK, but the file may have been wrapped with the old master key.
    Key key = null;
    if (kekKeyData != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Unwrapping key with KEK (custodian: {}, namespace: {}), checksum: {} and metadata "
            + "hash: {}",
          kekKeyData.getKeyCustodianEncoded(), kekKeyData.getKeyNamespace(),
          kekKeyData.getKeyChecksum(), kekKeyData.getPartialIdentityEncoded());
      }
      try {
        key = getUnwrapKey(conf, null, wrappedKey, dataCipher, kekKeyData.getTheKey());
      } catch (KeyException e) {
        LOG.debug("Unable to unwrap key with KEK, falling back to subject-based unwrapping", e);
      }
    }
    if (key == null) {
      key = unwrapKey(conf, encryptionContextBytes);
    }

    // Prefer the latest KEK on the context to facilitate key rotation; fall back to the KEK
    // that was used for unwrapping when no latest KEK is available.
    Encryption.Context context = Encryption.newContext(conf);
    context.setCipher(dataCipher);
    context.setKey(key);
    context.setKEKData(kekKeyData);
    return context;
  }

  /**
   * Serialize an encryption context into a self-contained WrappedKey protobuf that includes the
   * encrypted data key, cipher name, and KEK identity/metadata. The inverse operation is
   * {@link #deserializeEncryptionContext}.
   * @param context the encryption context containing the key, cipher, and optional KEK data
   * @return the serialized WrappedKey protobuf bytes
   */
  public static byte[] serializeEncryptionContext(Encryption.Context context) throws IOException {
    Configuration conf = context.getConf();
    Key encKey = context.getKey();
    if (encKey == null) {
      throw new IOException("Context does not have a key to wrap");
    }
    Cipher dataCipher = context.getCipher();
    if (dataCipher == null) {
      throw new IOException("Context does not have a cipher");
    }

    ManagedKeyData kekData = context.getKEKData();
    String subject = null;
    if (kekData == null) {
      subject =
        conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName());
    }

    return wrapKeyAndSerialize(conf, dataCipher, subject, encKey, kekData, true);
  }

  /**
   * Get the cipher if the cipher name is valid, otherwise throw an exception.
   * @param conf       the configuration
   * @param cipherName the cipher name to check
   * @param key        the key to check
   * @param familyName the family name
   * @return the cipher if the cipher name is valid
   * @throws IllegalStateException if the cipher name is not valid
   */
  private static Cipher getCipherIfValid(Configuration conf, String cipherName, Key key,
    String familyName) {
    // Use the algorithm the key wants
    Cipher cipher = Encryption.getCipher(conf, cipherName);
    if (cipher == null) {
      throw new IllegalStateException("Cipher '" + cipherName + "' is not available");
    }
    // Fail if misconfigured.
    // We use the encryption type specified in the column schema as a sanity check
    // on what the wrapped key is telling us
    if (key != null && !key.getAlgorithm().equalsIgnoreCase(cipher.getKeyAlgorithm())) {
      throw new IllegalStateException(
        "Encryption for family '" + familyName + "' is configured with cipher type '" + cipherName
          + "' but the cipher algorithm '" + cipher.getKeyAlgorithm()
          + "' doesn't match the key algorithm '" + key.getAlgorithm() + "'");
    }
    return cipher;
  }

  /**
   * From the given configuration, determine if key management is enabled.
   * @param conf the configuration to check
   * @return true if key management is enabled
   */
  public static boolean isKeyManagementEnabled(Configuration conf) {
    return conf.getBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY,
      HConstants.CRYPTO_MANAGED_KEYS_DEFAULT_ENABLED);
  }
}
