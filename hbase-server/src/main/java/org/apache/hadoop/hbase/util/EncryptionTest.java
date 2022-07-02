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
package org.apache.hadoop.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.DefaultCipherProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyStoreKeyProvider;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class EncryptionTest {
  private static final Logger LOG = LoggerFactory.getLogger(EncryptionTest.class);

  static final Map<String, Boolean> keyProviderResults = new ConcurrentHashMap<>();
  static final Map<String, Boolean> cipherProviderResults = new ConcurrentHashMap<>();
  static final Map<String, Boolean> cipherResults = new ConcurrentHashMap<>();

  private EncryptionTest() {
  }

  /**
   * Check that the configured key provider can be loaded and initialized, or throw an exception. nn
   */
  public static void testKeyProvider(final Configuration conf) throws IOException {
    String providerClassName =
      conf.get(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyStoreKeyProvider.class.getName());
    Boolean result = keyProviderResults.get(providerClassName);
    if (result == null) {
      try {
        Encryption.getKeyProvider(conf);
        keyProviderResults.put(providerClassName, true);
      } catch (Exception e) { // most likely a RuntimeException
        keyProviderResults.put(providerClassName, false);
        throw new IOException(
          "Key provider " + providerClassName + " failed test: " + e.getMessage(), e);
      }
    } else if (!result) {
      throw new IOException("Key provider " + providerClassName + " previously failed test");
    }
  }

  /**
   * Check that the configured cipher provider can be loaded and initialized, or throw an exception.
   * nn
   */
  public static void testCipherProvider(final Configuration conf) throws IOException {
    String providerClassName =
      conf.get(HConstants.CRYPTO_CIPHERPROVIDER_CONF_KEY, DefaultCipherProvider.class.getName());
    Boolean result = cipherProviderResults.get(providerClassName);
    if (result == null) {
      try {
        Encryption.getCipherProvider(conf);
        cipherProviderResults.put(providerClassName, true);
      } catch (Exception e) { // most likely a RuntimeException
        cipherProviderResults.put(providerClassName, false);
        throw new IOException(
          "Cipher provider " + providerClassName + " failed test: " + e.getMessage(), e);
      }
    } else if (!result) {
      throw new IOException("Cipher provider " + providerClassName + " previously failed test");
    }
  }

  /**
   * Check that the specified cipher can be loaded and initialized, or throw an exception. Verifies
   * key and cipher provider configuration as a prerequisite for cipher verification. Also verifies
   * if encryption is enabled globally.
   * @param conf   HBase configuration
   * @param cipher chiper algorith to use for the column family
   * @param key    encryption key
   * @throws IOException in case of encryption configuration error
   */
  public static void testEncryption(final Configuration conf, final String cipher, byte[] key)
    throws IOException {
    if (cipher == null) {
      return;
    }
    if (!Encryption.isEncryptionEnabled(conf)) {
      String message =
        String.format("Cipher %s failed test: encryption is disabled on the cluster", cipher);
      throw new IOException(message);
    }
    testKeyProvider(conf);
    testCipherProvider(conf);
    Boolean result = cipherResults.get(cipher);
    if (result == null) {
      try {
        Encryption.Context context = Encryption.newContext(conf);
        context.setCipher(Encryption.getCipher(conf, cipher));
        if (key == null) {
          // Make a random key since one was not provided
          context.setKey(context.getCipher().getRandomKey());
        } else {
          // This will be a wrapped key from schema
          context.setKey(EncryptionUtil.unwrapKey(conf,
            conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase"), key));
        }
        byte[] iv = null;
        if (context.getCipher().getIvLength() > 0) {
          iv = new byte[context.getCipher().getIvLength()];
          Bytes.secureRandom(iv);
        }
        byte[] plaintext = new byte[1024];
        Bytes.random(plaintext);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encryption.encrypt(out, new ByteArrayInputStream(plaintext), context, iv);
        byte[] ciphertext = out.toByteArray();
        out.reset();
        Encryption.decrypt(out, new ByteArrayInputStream(ciphertext), plaintext.length, context,
          iv);
        byte[] test = out.toByteArray();
        if (!Bytes.equals(plaintext, test)) {
          throw new IOException("Did not pass encrypt/decrypt test");
        }
        cipherResults.put(cipher, true);
      } catch (Exception e) {
        cipherResults.put(cipher, false);
        throw new IOException("Cipher " + cipher + " failed test: " + e.getMessage(), e);
      }
    } else if (!result) {
      throw new IOException("Cipher " + cipher + " previously failed test");
    }
  }
}
