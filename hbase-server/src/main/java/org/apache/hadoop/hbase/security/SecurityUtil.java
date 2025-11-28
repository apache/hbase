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

import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.keymeta.KeyNamespaceUtil;
import org.apache.hadoop.hbase.keymeta.ManagedKeyDataCache;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    String keyNamespace = null; // Will be set by fallback logic
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

      // Scenario 1: If family has a key, unwrap it and use that as DEK.
      byte[] familyKeyBytes = family.getEncryptionKey();
      if (familyKeyBytes != null) {
        try {
          if (isKeyManagementEnabled) {
            // Scenario 1a: If key management is enabled, use STK for both unwrapping and KEK.
            key = EncryptionUtil.unwrapKey(conf, null, familyKeyBytes, kekKeyData.getTheKey());
          } else {
            // Scenario 1b: If key management is disabled, unwrap the key using master key.
            key = EncryptionUtil.unwrapKey(conf, familyKeyBytes);
          }
          LOG.debug("Scenario 1: Use family key for namespace {} cipher: {} "
            + "key management enabled: {}", keyNamespace, cipherName, isKeyManagementEnabled);
        } catch (KeyException e) {
          throw new IOException(e);
        }
      } else {
        if (isKeyManagementEnabled) {
          boolean localKeyGenEnabled =
            conf.getBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY,
              HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_DEFAULT_ENABLED);
          // Implement 4-step fallback logic for key namespace resolution in the order of
          // 1. CF KEY_NAMESPACE attribute
          // 2. Constructed namespace
          // 3. Table name
          // 4. Global namespace
          String[] candidateNamespaces = { family.getEncryptionKeyNamespace(),
            KeyNamespaceUtil.constructKeyNamespace(tableDescriptor, family),
            tableDescriptor.getTableName().getNameAsString(), ManagedKeyData.KEY_SPACE_GLOBAL };

          ManagedKeyData activeKeyData = null;
          for (String candidate : candidateNamespaces) {
            if (candidate != null) {
              // Log information on the table and column family we are looking for the active key in
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                  "Looking for active key for table: {} and column family: {} with "
                    + "(custodian: {}, namespace: {})",
                  tableDescriptor.getTableName().getNameAsString(), family.getNameAsString(),
                  ManagedKeyData.KEY_GLOBAL_CUSTODIAN, candidate);
              }
              activeKeyData = managedKeyDataCache
                .getActiveEntry(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES, candidate);
              if (activeKeyData != null) {
                keyNamespace = candidate;
                break;
              }
            }
          }

          // Scenario 2: There is an active key
          if (activeKeyData != null) {
            if (!localKeyGenEnabled) {
              // Scenario 2a: Use active key as DEK and latest STK as KEK
              key = activeKeyData.getTheKey();
            } else {
              // Scenario 2b: Use active key as KEK and generate local key as DEK
              kekKeyData = activeKeyData;
              // TODO: Use the active key as a seed to generate the local key instead of
              // random generation
              cipher = getCipherIfValid(conf, cipherName, activeKeyData.getTheKey(),
                family.getNameAsString());
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                "Scenario 2: Use active key with (custodian: {}, namespace: {}) for cipher: {} "
                  + "localKeyGenEnabled: {} for table: {} and column family: {}",
                activeKeyData.getKeyCustodianEncoded(), activeKeyData.getKeyNamespace(), cipherName,
                localKeyGenEnabled, tableDescriptor.getTableName().getNameAsString(),
                family.getNameAsString());
            }
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Scenario 3a: No active key found for table: {} and column family: {}",
                tableDescriptor.getTableName().getNameAsString(), family.getNameAsString());
            }
            // Scenario 3a: Do nothing, let a random key be generated as DEK and if key management
            // is enabled, let STK be used as KEK.
          }
        } else {
          // Scenario 3b: Do nothing, let a random key be generated as DEK, let STK be used as KEK.
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
          kekKeyData.getKeyChecksum(), kekKeyData.getKeyMetadataHashEncoded());
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
      cryptoContext.setKeyNamespace(keyNamespace);
      cryptoContext.setKEKData(kekKeyData);
    }
    return cryptoContext;
  }

  /**
   * Create an encryption context from encryption key found in a file trailer, suitable for read.
   * @param conf                The current configuration.
   * @param path                The path of the file.
   * @param trailer             The file trailer.
   * @param managedKeyDataCache The managed key data cache.
   * @param systemKeyCache      The system key cache.
   * @return The created encryption context or null if no key material is available.
   * @throws IOException if an encryption key for the file cannot be unwrapped
   */
  public static Encryption.Context createEncryptionContext(Configuration conf, Path path,
    FixedFileTrailer trailer, ManagedKeyDataCache managedKeyDataCache,
    SystemKeyCache systemKeyCache) throws IOException {
    ManagedKeyData kekKeyData = null;
    byte[] keyBytes = trailer.getEncryptionKey();
    Encryption.Context cryptoContext = Encryption.Context.NONE;
    LOG.debug("Creating encryption context for path: {}", path);
    // Check for any key material available
    if (keyBytes != null) {
      cryptoContext = Encryption.newContext(conf);
      Key kek = null;

      // When there is key material, determine the appropriate KEK
      boolean isKeyManagementEnabled = isKeyManagementEnabled(conf);
      if (((trailer.getKEKChecksum() != 0L) || isKeyManagementEnabled) && systemKeyCache == null) {
        throw new IOException("SystemKeyCache can't be null when using key management feature");
      }
      if ((trailer.getKEKChecksum() != 0L && !isKeyManagementEnabled)) {
        throw new IOException(
          "Seeing newer trailer with KEK checksum, but key management is disabled");
      }

      // Try STK lookup first if checksum is available.
      if (trailer.getKEKChecksum() != 0L) {
        LOG.debug("Looking for System Key with checksum: {}", trailer.getKEKChecksum());
        ManagedKeyData systemKeyData =
          systemKeyCache.getSystemKeyByChecksum(trailer.getKEKChecksum());
        if (systemKeyData != null) {
          kek = systemKeyData.getTheKey();
          kekKeyData = systemKeyData;
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "Found System Key with (custodian: {}, namespace: {}), checksum: {} and "
                + "metadata hash: {}",
              systemKeyData.getKeyCustodianEncoded(), systemKeyData.getKeyNamespace(),
              systemKeyData.getKeyChecksum(), systemKeyData.getKeyMetadataHashEncoded());
          }
        }
      }

      // If STK lookup failed or no checksum available, try managed key lookup using metadata
      if (kek == null && trailer.getKEKMetadata() != null) {
        if (managedKeyDataCache == null) {
          throw new IOException("KEK metadata is available, but ManagedKeyDataCache is null");
        }
        Throwable cause = null;
        try {
          kekKeyData = managedKeyDataCache.getEntry(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES,
            trailer.getKeyNamespace(), trailer.getKEKMetadata(), keyBytes);
        } catch (KeyException | IOException e) {
          cause = e;
        }
        // When getEntry returns null we treat it the same as exception case.
        if (kekKeyData == null) {
          throw new IOException(
            "Failed to get key data for KEK metadata: " + trailer.getKEKMetadata(), cause);
        }
        kek = kekKeyData.getTheKey();
      } else if (kek == null && isKeyManagementEnabled) {
        // No checksum or metadata available, fall back to latest system key for backwards
        // compatibility
        ManagedKeyData systemKeyData = systemKeyCache.getLatestSystemKey();
        if (systemKeyData == null) {
          throw new IOException("Failed to get latest system key");
        }
        kek = systemKeyData.getTheKey();
        kekKeyData = systemKeyData;
      }

      Key key;
      if (kek != null) {
        try {
          key = EncryptionUtil.unwrapKey(conf, null, keyBytes, kek);
        } catch (KeyException | IOException e) {
          throw new IOException("Failed to unwrap key with KEK checksum: "
            + trailer.getKEKChecksum() + ", metadata: " + trailer.getKEKMetadata(), e);
        }
      } else {
        key = EncryptionUtil.unwrapKey(conf, keyBytes);
      }
      // Use the algorithm the key wants
      Cipher cipher = getCipherIfValid(conf, key.getAlgorithm(), key, null);
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
      cryptoContext.setKeyNamespace(trailer.getKeyNamespace());
      cryptoContext.setKEKData(kekKeyData);
    }
    return cryptoContext;
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
    // Fail if misconfigured
    // We use the encryption type specified in the column schema as a sanity check
    // on
    // what the wrapped key is telling us
    if (key != null && !key.getAlgorithm().equalsIgnoreCase(cipherName)) {
      throw new IllegalStateException(
        "Encryption for family '" + familyName + "' configured with type '" + cipherName
          + "' but key specifies algorithm '" + key.getAlgorithm() + "'");
    }
    // Use the algorithm the key wants
    Cipher cipher = Encryption.getCipher(conf, cipherName);
    if (cipher == null) {
      throw new IllegalStateException("Cipher '" + cipherName + "' is not available");
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
