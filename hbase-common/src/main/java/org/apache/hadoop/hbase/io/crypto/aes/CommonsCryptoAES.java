/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.crypto.aes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.Properties;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.Context;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CommonsCryptoAES extends Cipher {

  private static final Logger LOG = LoggerFactory.getLogger(CommonsCryptoAES.class);

  public static final String CIPHER_MODE_KEY = "hbase.crypto.commons.mode";
  public static final String CIPHER_CLASSES_KEY = "hbase.crypto.commons.cipher.classes";
  public static final String CIPHER_JCE_PROVIDER_KEY = "hbase.crypto.commons.cipher.jce.provider";

  private final String cipherMode;
  private Properties props;
  private final String rngAlgorithm;
  private SecureRandom rng;

  public CommonsCryptoAES(CipherProvider provider) {
    super(provider);
    // The mode for Commons Crypto Ciphers
    cipherMode = provider.getConf().get(CIPHER_MODE_KEY, "AES/CTR/NoPadding");
    // Reads Commons Crypto properties from HBase conf
    props = readCryptoProps(provider.getConf());
    // RNG algorithm
    rngAlgorithm = provider.getConf().get(RNG_ALGORITHM_KEY, "SHA1PRNG");
    // RNG provider, null if default
    String rngProvider = provider.getConf().get(RNG_PROVIDER_KEY);
    try {
      if (rngProvider != null) {
        rng = SecureRandom.getInstance(rngAlgorithm, rngProvider);
      } else {
        rng = SecureRandom.getInstance(rngAlgorithm);
      }
    } catch (GeneralSecurityException e) {
      LOG.warn("Could not instantiate specified RNG, falling back to default", e);
      rng = new SecureRandom();
    }
  }

  private static Properties readCryptoProps(Configuration conf) {
    Properties props = new Properties();

    props.setProperty(CryptoCipherFactory.CLASSES_KEY, conf.get(CIPHER_CLASSES_KEY, ""));
    props.setProperty(CryptoCipherFactory.JCE_PROVIDER_KEY, conf.get(CIPHER_JCE_PROVIDER_KEY, ""));

    return props;
  }

  @Override
  public String getName() {
    return "AES";
  }

  @Override
  public int getKeyLength() {
    return KEY_LENGTH;
  }

  @Override
  public int getIvLength() {
    return IV_LENGTH;
  }

  @Override
  public Key getRandomKey() {
    byte[] keyBytes = new byte[getKeyLength()];
    rng.nextBytes(keyBytes);
    return new SecretKeySpec(keyBytes, getName());
  }

  @Override
  public Encryptor getEncryptor() {
    return new CommonsCryptoAESEncryptor(cipherMode, props, rng);
  }

  @Override
  public Decryptor getDecryptor() {
    return new CommonsCryptoAESDecryptor(cipherMode, props);
  }

  @Override
  public OutputStream createEncryptionStream(OutputStream out, Context context,
                                             byte[] iv) throws IOException {
    Preconditions.checkNotNull(context);
    Preconditions.checkState(context.getKey() != null, "Context does not have a key");
    Preconditions.checkNotNull(iv);
    Encryptor e = getEncryptor();
    e.setKey(context.getKey());
    e.setIv(iv);
    return e.createEncryptionStream(out);
  }

  @Override
  public OutputStream createEncryptionStream(OutputStream out,
                                             Encryptor encryptor) throws
      IOException {
    return encryptor.createEncryptionStream(out);
  }

  @Override
  public InputStream createDecryptionStream(InputStream in, Context context,
                                            byte[] iv) throws IOException {
    Preconditions.checkNotNull(context);
    Preconditions.checkState(context.getKey() != null, "Context does not have a key");
    Preconditions.checkNotNull(iv);
    Decryptor d = getDecryptor();
    d.setKey(context.getKey());
    d.setIv(iv);
    return d.createDecryptionStream(in);
  }

  @Override
  public InputStream createDecryptionStream(InputStream in,
                                            Decryptor decryptor) throws
      IOException {
    Preconditions.checkNotNull(decryptor);
    return decryptor.createDecryptionStream(in);
  }

  SecureRandom getRNG() {
    return rng;
  }
}
