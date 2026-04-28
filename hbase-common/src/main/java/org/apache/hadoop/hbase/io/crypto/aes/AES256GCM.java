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
 * AES-256-GCM authenticated encryption, provided by the JCE.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AES256GCM extends AESCipher {

  public static final int KEY_LENGTH = 32;
  public static final int KEY_LENGTH_BITS = KEY_LENGTH * 8;
  /** GCM nonce length in bytes (96 bits as recommended by NIST SP 800-38D). */
  public static final int NONCE_LENGTH = 12;
  public static final int TAG_LENGTH_BITS = 128;

  private static final String CIPHER_MODE = "AES/GCM/NoPadding";

  public AES256GCM(CipherProvider provider) {
    super(provider);
  }

  @Override
  public String getName() {
    return "AES_256_GCM";
  }

  @Override
  public int getKeyLength() {
    return KEY_LENGTH;
  }

  @Override
  public int getIvLength() {
    return NONCE_LENGTH;
  }

  @Override
  public int getAuthTagLength() {
    return TAG_LENGTH_BITS / Byte.SIZE;
  }

  @Override
  public Encryptor getEncryptor() {
    return new AES256GCMEncryptor(getJCECipherInstance(), getRNG());
  }

  @Override
  public Decryptor getDecryptor() {
    return new AES256GCMDecryptor(getJCECipherInstance());
  }

  private javax.crypto.Cipher getJCECipherInstance() {
    try {
      return javax.crypto.Cipher.getInstance(CIPHER_MODE);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }
}
