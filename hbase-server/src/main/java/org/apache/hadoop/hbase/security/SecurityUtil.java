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
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.keymeta.ManagedKeyDataCache;
import org.apache.hadoop.hbase.keymeta.SystemKeyCache;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Security related generic utility methods.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SecurityUtil {
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
   * @param family              The current column descriptor.
   * @param managedKeyDataCache The managed key data cache.
   * @param systemKeyCache      The system key cache.
   * @param keyNamespace        The key namespace.
   * @return The created encryption context.
   * @throws IOException           if an encryption key for the column cannot be unwrapped
   * @throws IllegalStateException in case of encryption related configuration errors
   */
  public static Encryption.Context createEncryptionContext(Configuration conf,
    ColumnFamilyDescriptor family, ManagedKeyDataCache managedKeyDataCache,
    SystemKeyCache systemKeyCache, String keyNamespace) throws IOException {
    Encryption.Context cryptoContext = Encryption.Context.NONE;
    String cipherName = family.getEncryptionType();
    if (cipherName != null) {
      if (!Encryption.isEncryptionEnabled(conf)) {
        throw new IllegalStateException("Encryption for family '" + family.getNameAsString()
          + "' configured with type '" + cipherName + "' but the encryption feature is disabled");
      }
      Cipher cipher = null;
      Key key = null;
      ManagedKeyData kekKeyData = null;
      if (isKeyManagementEnabled(conf)) {
        kekKeyData = managedKeyDataCache.getActiveEntry(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES,
          keyNamespace);
        // If no active key found in the specific namespace, try the global namespace
        if (kekKeyData == null) {
          kekKeyData = managedKeyDataCache.getActiveEntry(ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES,
            ManagedKeyData.KEY_SPACE_GLOBAL);
          keyNamespace = ManagedKeyData.KEY_SPACE_GLOBAL;
        }
        if (kekKeyData == null) {
          throw new IOException(
            "No active key found for custodian: " + ManagedKeyData.KEY_GLOBAL_CUSTODIAN
              + " in namespaces: " + keyNamespace + " and " + ManagedKeyData.KEY_SPACE_GLOBAL);
        }
        if (
          conf.getBoolean(HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY,
            HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_DEFAULT_ENABLED)
        ) {
          cipher =
            getCipherIfValid(conf, cipherName, kekKeyData.getTheKey(), family.getNameAsString());
        } else {
          key = kekKeyData.getTheKey();
          kekKeyData = systemKeyCache.getLatestSystemKey();
        }
      } else {
        byte[] keyBytes = family.getEncryptionKey();
        if (keyBytes != null) {
          // Family provides specific key material
          key = EncryptionUtil.unwrapKey(conf, keyBytes);
        } else {
          cipher = getCipherIfValid(conf, cipherName, null, null);
        }
      }
      if (key != null || cipher != null) {
        if (key == null) {
          // Family does not provide key material, create a random key
          key = cipher.getRandomKey();
        }
        if (cipher == null) {
          cipher = getCipherIfValid(conf, cipherName, key, family.getNameAsString());
        }
        cryptoContext = Encryption.newContext(conf);
        cryptoContext.setCipher(cipher);
        cryptoContext.setKey(key);
        cryptoContext.setKeyNamespace(keyNamespace);
        cryptoContext.setKEKData(kekKeyData);
      }
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
    // Check for any key material available
    if (keyBytes != null) {
      cryptoContext = Encryption.newContext(conf);
      Key kek = null;
      // When the KEK medata is available, we will try to unwrap the encrypted key using the KEK,
      // otherwise we will use the system keys starting from the latest to the oldest.
      if (trailer.getKEKMetadata() != null) {
        if (managedKeyDataCache == null) {
          throw new IOException("Key management is enabled, but ManagedKeyDataCache is null");
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
      } else {
        if (SecurityUtil.isKeyManagementEnabled(conf)) {
          if (systemKeyCache == null) {
            throw new IOException("Key management is enabled, but SystemKeyCache is null");
          }
          ManagedKeyData systemKeyData =
            systemKeyCache.getSystemKeyByChecksum(trailer.getKEKChecksum());
          if (systemKeyData == null) {
            throw new IOException(
              "Failed to get system key by checksum: " + trailer.getKEKChecksum());
          }
          kek = systemKeyData.getTheKey();
          kekKeyData = systemKeyData;
        }
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
      Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());
      if (cipher == null) {
        throw new IOException(
          "Cipher '" + key.getAlgorithm() + "' is not available" + ", path=" + path);
      }
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
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
