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
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.EncryptionProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

/**
 * Some static utility methods for encryption uses in hbase-client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class EncryptionUtil {
  static private final Logger LOG = LoggerFactory.getLogger(EncryptionUtil.class);

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private EncryptionUtil() {
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
    return wrapKey(conf, subject, key, null);
  }

  /**
   * Protect a key by encrypting it with the secret key of the given subject or kek. The
   * configuration must be set up correctly for key alias resolution. Only one of the
   * {@code subject} or {@code kek} needs to be specified and the other one can be {@code null}.
   * @param conf    configuration
   * @param subject subject key alias
   * @param key     the key
   * @param kek     the key encryption key
   * @return the encrypted key bytes
   */
  public static byte[] wrapKey(Configuration conf, String subject, Key key, Key kek)
      throws IOException {
    // Wrap the key with the configured encryption algorithm.
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Cipher cipher = Encryption.getCipher(conf, algorithm);
    if (cipher == null) {
      throw new RuntimeException("Cipher '" + algorithm + "' not available");
    }
    EncryptionProtos.WrappedKey.Builder builder = EncryptionProtos.WrappedKey.newBuilder();
    builder.setAlgorithm(key.getAlgorithm());
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
    }
    else {
      Encryption.encryptWithSubjectKey(out, new ByteArrayInputStream(keyBytes), subject, conf,
        cipher, iv);
    }
    builder.setData(UnsafeByteOperations.unsafeWrap(out.toByteArray()));
    // Build and return the protobuf message
    out.reset();
    builder.build().writeDelimitedTo(out);
    return out.toByteArray();
  }

  /**
   * Unwrap a key by decrypting it with the secret key of the given subject. The configuration must
   * be set up correctly for key alias resolution.
   * @param conf    configuration
   * @param subject subject key alias
   * @param value   the encrypted key bytes
   * @return the raw key bytes
   */
  public static Key unwrapKey(Configuration conf, String subject, byte[] value)
    throws IOException, KeyException {
    return unwrapKey(conf, subject, value, null);
  }

  /**
   * Unwrap a key by decrypting it with the secret key of the given subject. The configuration must
   * be set up correctly for key alias resolution. Only one of the {@code subject} or {@code kek}
   * needs to be specified and the other one can be {@code null}.
   * @param conf    configuration
   * @param subject subject key alias
   * @param value   the encrypted key bytes
   * @param kek   the key encryption key
   * @return the raw key bytes
   */
  public static Key unwrapKey(Configuration conf, String subject, byte[] value, Key kek)
    throws IOException, KeyException {
    EncryptionProtos.WrappedKey wrappedKey =
      EncryptionProtos.WrappedKey.parser().parseDelimitedFrom(new ByteArrayInputStream(value));
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Cipher cipher = Encryption.getCipher(conf, algorithm);
    if (cipher == null) {
      throw new RuntimeException("Cipher '" + algorithm + "' not available");
    }
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
    }
    else {
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
   * Unwrap a wal key by decrypting it with the secret key of the given subject. The configuration
   * must be set up correctly for key alias resolution.
   * @param conf    configuration
   * @param subject subject key alias
   * @param value   the encrypted key bytes
   * @return the raw key bytes
   * @throws IOException  if key is not found for the subject, or if some I/O error occurs
   * @throws KeyException if fail to unwrap the key
   */
  public static Key unwrapWALKey(Configuration conf, String subject, byte[] value)
    throws IOException, KeyException {
    EncryptionProtos.WrappedKey wrappedKey =
      EncryptionProtos.WrappedKey.parser().parseDelimitedFrom(new ByteArrayInputStream(value));
    String algorithm = conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Cipher cipher = Encryption.getCipher(conf, algorithm);
    if (cipher == null) {
      throw new RuntimeException("Cipher '" + algorithm + "' not available");
    }
    return getUnwrapKey(conf, subject, wrappedKey, cipher, null);
  }

  /**
   * Helper for {@link #unwrapKey(Configuration, String, byte[])} which automatically uses the
   * configured master and alternative keys, rather than having to specify a key type to unwrap
   * with. The configuration must be set up correctly for key alias resolution.
   * @param conf     the current configuration
   * @param keyBytes the key encrypted by master (or alternative) to unwrap
   * @return the key bytes, decrypted
   * @throws IOException if the key cannot be unwrapped
   */
  public static Key unwrapKey(Configuration conf, byte[] keyBytes) throws IOException {
    Key key;
    String masterKeyName =
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName());
    try {
      // First try the master key
      key = unwrapKey(conf, masterKeyName, keyBytes);
    } catch (KeyException e) {
      // If the current master key fails to unwrap, try the alternate, if
      // one is configured
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to unwrap key with current master key '" + masterKeyName + "'");
      }
      String alternateKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
      if (alternateKeyName != null) {
        try {
          key = unwrapKey(conf, alternateKeyName, keyBytes);
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
   * Helper to create an instance of CryptoAES.
   * @param conf             The current configuration.
   * @param cryptoCipherMeta The metadata for create CryptoAES.
   * @return The instance of CryptoAES.
   * @throws IOException if create CryptoAES failed
   */
  public static CryptoAES createCryptoAES(RPCProtos.CryptoCipherMeta cryptoCipherMeta,
    Configuration conf) throws IOException {
    Properties properties = new Properties();
    // the property for cipher class
    properties.setProperty(CryptoCipherFactory.CLASSES_KEY,
      conf.get("hbase.rpc.crypto.encryption.aes.cipher.class",
        "org.apache.commons.crypto.cipher.JceCipher"));
    // create SaslAES for client
    return new CryptoAES(cryptoCipherMeta.getTransformation(), properties,
      cryptoCipherMeta.getInKey().toByteArray(), cryptoCipherMeta.getOutKey().toByteArray(),
      cryptoCipherMeta.getInIv().toByteArray(), cryptoCipherMeta.getOutIv().toByteArray());
  }
}
