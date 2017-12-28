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
import java.security.Key;
import java.util.Properties;
import javax.crypto.spec.IvParameterSpec;

import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CommonsCryptoAESDecryptor implements Decryptor {

  private String cipherMode;
  private Properties properties;
  private Key key;
  private byte[] iv;

  public CommonsCryptoAESDecryptor(String cipherMode, Properties properties) {
    this.cipherMode = cipherMode;
    this.properties = properties;
  }

  @Override
  public void setKey(Key key) {
    Preconditions.checkNotNull(key, "Key cannot be null");
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
  public void setIv(byte[] iv) {
    Preconditions.checkNotNull(iv, "IV cannot be null");
    Preconditions.checkArgument(iv.length == CommonsCryptoAES.IV_LENGTH, "Invalid IV length");
    this.iv = iv;
  }

  @Override
  public InputStream createDecryptionStream(InputStream in) {
    try {
      return new CryptoInputStream(cipherMode, properties, in, key, new
          IvParameterSpec(iv));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
  }
}
