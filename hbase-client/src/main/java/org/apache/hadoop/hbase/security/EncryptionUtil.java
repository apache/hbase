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
import java.security.SecureRandom;

import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.protobuf.generated.EncryptionProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Some static utility methods for encryption uses in hbase-client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EncryptionUtil {

  static private final SecureRandom RNG = new SecureRandom();

  /**
   * Protect a key by encrypting it with the secret key of the given subject.
   * The configuration must be set up correctly for key alias resolution.
   * @param conf configuration
   * @param key the raw key bytes
   * @param algorithm the algorithm to use with this key material
   * @return the encrypted key bytes
   * @throws IOException
   */
  public static byte[] wrapKey(Configuration conf, byte[] key, String algorithm)
      throws IOException {
    return wrapKey(conf,
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),
      new SecretKeySpec(key, algorithm));
  }

  /**
   * Protect a key by encrypting it with the secret key of the given subject.
   * The configuration must be set up correctly for key alias resolution. Keys
   * are always wrapped using AES.
   * @param conf configuration
   * @param subject subject key alias
   * @param key the key
   * @return the encrypted key bytes
   */
  public static byte[] wrapKey(Configuration conf, String subject, Key key)
      throws IOException {
    // Wrap the key with AES
    Cipher cipher = Encryption.getCipher(conf, "AES");
    if (cipher == null) {
      throw new RuntimeException("Cipher 'AES' not available");
    }
    EncryptionProtos.WrappedKey.Builder builder = EncryptionProtos.WrappedKey.newBuilder();
    builder.setAlgorithm(key.getAlgorithm());
    byte[] iv = null;
    if (cipher.getIvLength() > 0) {
      iv = new byte[cipher.getIvLength()];
      RNG.nextBytes(iv);
      builder.setIv(ByteStringer.wrap(iv));
    }
    byte[] keyBytes = key.getEncoded();
    builder.setLength(keyBytes.length);
    builder.setHash(ByteStringer.wrap(Encryption.hash128(keyBytes)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encryption.encryptWithSubjectKey(out, new ByteArrayInputStream(keyBytes), subject,
      conf, cipher, iv);
    builder.setData(ByteStringer.wrap(out.toByteArray()));
    // Build and return the protobuf message
    out.reset();
    builder.build().writeDelimitedTo(out);
    return out.toByteArray();
  }

  /**
   * Unwrap a key by decrypting it with the secret key of the given subject.
   * The configuration must be set up correctly for key alias resolution. Keys
   * are always unwrapped using AES.
   * @param conf configuration
   * @param subject subject key alias
   * @param value the encrypted key bytes
   * @return the raw key bytes
   * @throws IOException
   * @throws KeyException
   */
  public static Key unwrapKey(Configuration conf, String subject, byte[] value)
      throws IOException, KeyException {
    EncryptionProtos.WrappedKey wrappedKey = EncryptionProtos.WrappedKey.PARSER
        .parseDelimitedFrom(new ByteArrayInputStream(value));
    Cipher cipher = Encryption.getCipher(conf, "AES");
    if (cipher == null) {
      throw new RuntimeException("Algorithm 'AES' not available");
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] iv = wrappedKey.hasIv() ? wrappedKey.getIv().toByteArray() : null;
    Encryption.decryptWithSubjectKey(out, wrappedKey.getData().newInput(),
      wrappedKey.getLength(), subject, conf, cipher, iv);
    byte[] keyBytes = out.toByteArray();
    if (wrappedKey.hasHash()) {
      if (!Bytes.equals(wrappedKey.getHash().toByteArray(), Encryption.hash128(keyBytes))) {
        throw new KeyException("Key was not successfully unwrapped");
      }
    }
    return new SecretKeySpec(keyBytes, wrappedKey.getAlgorithm());
  }

}
