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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
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
   * Helper to create an encryption context with current encryption key, suitable for writes. STUB
   * IMPLEMENTATION - Key management not yet implemented. Cache parameters are placeholders for
   * future implementation.
   * @param conf                The current configuration.
   * @param tableDescriptor     The table descriptor.
   * @param family              The current column descriptor.
   * @param managedKeyDataCache The managed key data cache (unused in stub).
   * @param systemKeyCache      The system key cache (unused in stub).
   * @return The created encryption context.
   * @throws IOException           if an encryption key for the column cannot be unwrapped
   * @throws IllegalStateException in case of encryption related configuration errors
   */
  public static Encryption.Context createEncryptionContext(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor family,
    ManagedKeyDataCache managedKeyDataCache, SystemKeyCache systemKeyCache) throws IOException {
    Encryption.Context cryptoContext = Encryption.Context.NONE;
    String cipherName = family.getEncryptionType();

    if (cipherName != null) {
      if (!Encryption.isEncryptionEnabled(conf)) {
        throw new IllegalStateException("Encryption for family '" + family.getNameAsString()
          + "' configured with type '" + cipherName + "' but the encryption feature is disabled");
      }

      Key key = null;
      byte[] familyKeyBytes = family.getEncryptionKey();

      // Unwrap family key if present
      if (familyKeyBytes != null) {
        key = EncryptionUtil.unwrapKey(conf, familyKeyBytes);
      }

      Cipher cipher = Encryption.getCipher(conf, cipherName);
      if (cipher == null) {
        throw new IllegalStateException("Cipher '" + cipherName + "' is not available");
      }

      // Generate random key if none provided
      if (key == null) {
        key = cipher.getRandomKey();
      }

      cryptoContext = Encryption.newContext(conf);
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
    }
    return cryptoContext;
  }

  /**
   * Create an encryption context from encryption key found in a file trailer, suitable for read.
   * STUB IMPLEMENTATION - Key management not yet implemented. Cache parameters are placeholders for
   * future implementation.
   * @param conf                The current configuration.
   * @param path                The path of the file.
   * @param trailer             The file trailer.
   * @param managedKeyDataCache The managed key data cache (unused in stub).
   * @param systemKeyCache      The system key cache (unused in stub).
   * @return The created encryption context or null if no key material is available.
   * @throws IOException if an encryption key for the file cannot be unwrapped
   */
  public static Encryption.Context createEncryptionContext(Configuration conf, Path path,
    FixedFileTrailer trailer, ManagedKeyDataCache managedKeyDataCache,
    SystemKeyCache systemKeyCache) throws IOException {
    byte[] keyBytes = trailer.getEncryptionKey();
    Encryption.Context cryptoContext = Encryption.Context.NONE;

    if (keyBytes != null) {
      cryptoContext = Encryption.newContext(conf);
      Key key = EncryptionUtil.unwrapKey(conf, keyBytes);
      Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());

      if (cipher == null) {
        throw new IllegalStateException("Cipher '" + key.getAlgorithm() + "' is not available");
      }

      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
    }
    return cryptoContext;
  }

  /**
   * Check if key management is enabled in configuration. STUB - Always returns false in precursor.
   * @param conf the configuration to check
   * @return false in stub implementation
   */
  public static boolean isKeyManagementEnabled(Configuration conf) {
    return false;
  }
}
