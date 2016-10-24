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

import com.google.common.base.Preconditions;
import org.apache.commons.crypto.stream.CryptoOutputStream;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.io.crypto.Encryptor;

import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.security.SecureRandom;
import java.util.Properties;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CommonsCryptoAESEncryptor implements Encryptor {

  private String cipherMode;
  private Properties properties;
  private Key key;
  private byte[] iv;
  private boolean initialized = false;
  private SecureRandom rng;

  public CommonsCryptoAESEncryptor(String cipherMode, Properties properties, SecureRandom rng) {
    this.cipherMode = cipherMode;
    this.properties = properties;
    this.rng = rng;
  }

  @Override
  public void setKey(Key key) {
    this.key = key;
  }

  @Override
  public int getIvLength() {
    return CommonsCryptoAES.IV_LENGTH;
  }

  @Override
  public int getBlockSize() {
    return CommonsCryptoAES.BLOCK_SIZE;
  }

  @Override
  public byte[] getIv() {
    return iv;
  }

  @Override
  public void setIv(byte[] iv) {
    Preconditions.checkNotNull(iv, "IV cannot be null");
    Preconditions.checkArgument(iv.length == CommonsCryptoAES.IV_LENGTH, "Invalid IV length");
    this.iv = iv;
  }

  @Override
  public OutputStream createEncryptionStream(OutputStream out) {
    if (!initialized) {
      reset();
    }
    try {
      return new CryptoOutputStream(cipherMode, properties, out, key,  new
          IvParameterSpec(iv));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
    if (iv == null) {
      iv = new byte[getIvLength()];
      rng.nextBytes(iv);
    }
    initialized = true;
  }
}
