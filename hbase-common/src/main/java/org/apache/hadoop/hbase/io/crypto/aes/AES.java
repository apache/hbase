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
package org.apache.hadoop.hbase.io.crypto.aes;

import java.security.GeneralSecurityException;
import org.apache.hadoop.hbase.io.crypto.CipherProvider;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * AES-128, provided by the JCE
 * <p>
 * Algorithm instances are pooled for reuse, so the cipher provider and mode are configurable but
 * fixed at instantiation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AES extends AESCipher {

  public static final String CIPHER_MODE_KEY = "hbase.crypto.algorithm.aes.mode";
  public static final String CIPHER_PROVIDER_KEY = "hbase.crypto.algorithm.aes.provider";

  private final String cipherMode;
  private final String cipherProvider;

  public AES(CipherProvider provider) {
    super(provider);
    cipherMode = provider.getConf().get(CIPHER_MODE_KEY, "AES/CTR/NoPadding");
    cipherProvider = provider.getConf().get(CIPHER_PROVIDER_KEY);
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
  public Encryptor getEncryptor() {
    return new AESEncryptor(getJCECipherInstance(), getRNG());
  }

  @Override
  public Decryptor getDecryptor() {
    return new AESDecryptor(getJCECipherInstance());
  }

  private javax.crypto.Cipher getJCECipherInstance() {
    try {
      if (cipherProvider != null) {
        return javax.crypto.Cipher.getInstance(cipherMode, cipherProvider);
      }
      return javax.crypto.Cipher.getInstance(cipherMode);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

}
