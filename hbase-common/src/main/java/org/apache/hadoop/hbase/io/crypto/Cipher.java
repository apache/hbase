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
package org.apache.hadoop.hbase.io.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.SecureRandom;
import javax.crypto.spec.SecretKeySpec;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common interface for a cryptographic algorithm.
 */
@InterfaceAudience.Public
public abstract class Cipher {

  private static final Logger LOG = LoggerFactory.getLogger(Cipher.class);

  public static final int KEY_LENGTH = 16;
  public static final int KEY_LENGTH_BITS = KEY_LENGTH * 8;
  public static final int BLOCK_SIZE = 16;
  public static final int IV_LENGTH = 16;

  public static final String RNG_ALGORITHM_KEY = "hbase.crypto.algorithm.rng";
  public static final String RNG_PROVIDER_KEY = "hbase.crypto.algorithm.rng.provider";

  private final CipherProvider provider;
  private final SecureRandom rng;

  public Cipher(CipherProvider provider) {
    this.provider = provider;
    String rngAlgorithm = provider.getConf().get(RNG_ALGORITHM_KEY, "SHA1PRNG");
    String rngProvider = provider.getConf().get(RNG_PROVIDER_KEY);
    SecureRandom instance;
    try {
      instance = (rngProvider != null)
        ? SecureRandom.getInstance(rngAlgorithm, rngProvider)
        : SecureRandom.getInstance(rngAlgorithm);
    } catch (GeneralSecurityException e) {
      LOG.warn("Could not instantiate specified RNG, falling back to default", e);
      instance = new SecureRandom();
    }
    this.rng = instance;
  }

  /**
   * Return the provider for this Cipher
   */
  public CipherProvider getProvider() {
    return provider;
  }

  /** Return the {@link SecureRandom} instance used by this cipher. */
  protected SecureRandom getRNG() {
    return rng;
  }

  /**
   * Return this Cipher's name
   */
  public abstract String getName();

  /**
   * Return the JCE algorithm name to use when constructing a
   * {@link javax.crypto.spec.SecretKeySpec} for this cipher (e.g. "AES").
   */
  public abstract String getKeyAlgorithm();

  /**
   * Return the key length required by this cipher, in bytes
   */
  public abstract int getKeyLength();

  /**
   * Return the expected initialization vector length, in bytes, or 0 if not applicable
   */
  public abstract int getIvLength();

  /**
   * Return the authentication tag length in bytes for authenticated encryption modes (e.g. GCM).
   * The tag is appended to ciphertext by the encryptor but consumed internally by the decryptor, so
   * callers that size buffers for the decrypted output must subtract this overhead. Defaults to 0
   * for non-authenticated modes like CTR.
   */
  public int getAuthTagLength() {
    return 0;
  }

  /**
   * Create a random symmetric key. Generates {@link #getKeyLength()} random bytes and wraps them in
   * a {@link SecretKeySpec} using {@link #getKeyAlgorithm()}.
   * @return the random symmetric key
   */
  public Key getRandomKey() {
    byte[] keyBytes = new byte[getKeyLength()];
    rng.nextBytes(keyBytes);
    return new SecretKeySpec(keyBytes, getKeyAlgorithm());
  }

  /**
   * Get an encryptor for encrypting data.
   */
  public abstract Encryptor getEncryptor();

  /**
   * Return a decryptor for decrypting data.
   */
  public abstract Decryptor getDecryptor();

  /**
   * Create an encrypting output stream given a context and IV
   * @param out     the output stream to wrap
   * @param context the encryption context
   * @param iv      initialization vector
   * @return the encrypting wrapper
   */
  public OutputStream createEncryptionStream(OutputStream out, Context context, byte[] iv)
    throws IOException {
    Encryptor e = getEncryptor();
    e.setKey(context.getKey());
    e.setIv(iv);
    return e.createEncryptionStream(out);
  }

  /**
   * Create an encrypting output stream given an initialized encryptor
   * @param out       the output stream to wrap
   * @param encryptor the encryptor
   * @return the encrypting wrapper
   */
  public OutputStream createEncryptionStream(OutputStream out, Encryptor encryptor)
    throws IOException {
    return encryptor.createEncryptionStream(out);
  }

  /**
   * Create a decrypting input stream given a context and IV
   * @param in      the input stream to wrap
   * @param context the encryption context
   * @param iv      initialization vector
   * @return the decrypting wrapper
   */
  public InputStream createDecryptionStream(InputStream in, Context context, byte[] iv)
    throws IOException {
    Decryptor d = getDecryptor();
    d.setKey(context.getKey());
    d.setIv(iv);
    return d.createDecryptionStream(in);
  }

  /**
   * Create a decrypting output stream given an initialized decryptor
   * @param in        the input stream to wrap
   * @param decryptor the decryptor
   * @return the decrypting wrapper
   */
  public InputStream createDecryptionStream(InputStream in, Decryptor decryptor)
    throws IOException {
    return decryptor.createDecryptionStream(in);
  }

}
