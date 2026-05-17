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

import java.io.OutputStream;
import java.security.SecureRandom;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AESEncryptor extends AESCodecBase implements Encryptor {

  private SecureRandom rng;

  public AESEncryptor(javax.crypto.Cipher cipher, SecureRandom rng) {
    super(cipher);
    this.rng = rng;
  }

  @Override
  public void setIv(byte[] iv) {
    if (iv != null) {
      Preconditions.checkArgument(iv.length == getIvLength(), "Invalid IV length");
    }
    this.iv = iv;
  }

  @Override
  public OutputStream createEncryptionStream(OutputStream out) {
    if (!initialized) {
      init();
    }
    return new javax.crypto.CipherOutputStream(out, getCipher());
  }

  @Override
  protected void initIv() {
    if (iv == null) {
      iv = new byte[getIvLength()];
      rng.nextBytes(iv);
    }
  }

  @Override
  protected int getOperationMode() {
    return javax.crypto.Cipher.ENCRYPT_MODE;
  }
}
