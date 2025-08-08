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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.Server;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Security related generic utility methods.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SecurityUtil {
  private static Boolean isKeyManagementEnabled;

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
   * Helper to create an encyption context.
   * @param conf   The current configuration.
   * @param family The current column descriptor.
   * @return The created encryption context.
   * @throws IOException           if an encryption key for the column cannot be unwrapped
   * @throws IllegalStateException in case of encryption related configuration errors
   */
  public static Encryption.Context createEncryptionContext(Configuration conf, Server server,
      TableDescriptor tableDescriptor, ColumnFamilyDescriptor family) throws IOException {
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
      if (server != null && isKeyManagementEnabled(conf)) {
        String keyNamespace = constructKeyNamespace(tableDescriptor, family);
        kekKeyData = server.getManagedKeyDataCache().getActiveEntry(
          ManagedKeyData.KEY_GLOBAL_CUSTODIAN_BYTES, keyNamespace);
        if (kekKeyData == null) {
          throw new IOException("No active key found for custodian: "
            + ManagedKeyData.KEY_GLOBAL_CUSTODIAN + " namespace: " + keyNamespace);
        }
        if (conf.getBoolean(
                HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY,
                HConstants.CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_DEFAULT_ENABLED)) {
          cipher = Encryption.getCipher(conf, kekKeyData.getTheKey().getAlgorithm());
          if (cipher == null) {
            throw new IllegalStateException("Cipher '" + cipherName + "' is not available");
          }
          key = cipher.getRandomKey();
        }
        else {
          key = kekKeyData.getTheKey();
          kekKeyData = server.getSystemKeyCache().getLatestSystemKey();
        }
      } else {
        byte[] keyBytes = family.getEncryptionKey();
        if (keyBytes != null) {
          // Family provides specific key material
          key = EncryptionUtil.unwrapKey(conf, keyBytes);
          // Use the algorithm the key wants
          cipher = Encryption.getCipher(conf, key.getAlgorithm());
          if (cipher == null) {
            throw new IllegalStateException("Cipher '" + key.getAlgorithm() + "' is not available");
          }
          // Fail if misconfigured
          // We use the encryption type specified in the column schema as a sanity check
          // on
          // what the wrapped key is telling us
          if (!cipher.getName().equalsIgnoreCase(cipherName)) {
            throw new IllegalStateException(
                "Encryption for family '" + family.getNameAsString() + "' configured with type '"
                    + cipherName + "' but key specifies algorithm '" + cipher.getName() + "'");
          }
        } else {
          // Family does not provide key material, create a random key
          cipher = Encryption.getCipher(conf, cipherName);
          if (cipher == null) {
            throw new IllegalStateException("Cipher '" + cipherName + "' is not available");
          }
          key = cipher.getRandomKey();
        }
      }
      if (key != null) {
        cryptoContext = Encryption.newContext(conf);
        cryptoContext.setCipher(cipher);
        cryptoContext.setKey(key);
        cryptoContext.setKEKData(kekKeyData);
      }
    }
    return cryptoContext;
  }

  public static String constructKeyNamespace(TableDescriptor tableDescriptor,
      ColumnFamilyDescriptor family) {
    return tableDescriptor.getTableName().getNamespaceAsString() + "/"
        + family.getNameAsString();
  }

  public static String constructKeyNamespace(StoreContext storeContext) {
    return storeContext.getTableName().getNamespaceAsString() + "/"
        + storeContext.getFamily().getNameAsString();
  }

  /**
   * From the given configuration, determine if key management is enabled.
   * @param conf the configuration to check
   * @return true if key management is enabled
   */
  public static boolean isKeyManagementEnabled(Configuration conf) {
    if (isKeyManagementEnabled == null) {
      isKeyManagementEnabled = conf.getBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_DEFAULT_ENABLED);
    }
    return isKeyManagementEnabled;
  }
}
