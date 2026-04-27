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

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import org.apache.hadoop.hbase.io.crypto.CipherOperator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AESCodecBase implements CipherOperator {

  protected javax.crypto.Cipher cipher;
  protected Key key;
  protected byte[] iv;
  protected boolean initialized = false;
  protected AlgorithmParameterSpec algorithmParameter;
  private boolean hasCustomAlgorithmParameter = false;

  public AESCodecBase(javax.crypto.Cipher cipher) {
    this.cipher = cipher;
  }

  javax.crypto.Cipher getCipher() {
    return cipher;
  }

  @Override
  public void setKey(Key key) {
    Preconditions.checkNotNull(key, "Key cannot be null");
    this.key = key;
  }

  @Override
  public int getIvLength() {
    return AES.IV_LENGTH;
  }

  @Override
  public int getBlockSize() {
    return AES.BLOCK_SIZE;
  }

  @Override
  public byte[] getIv() {
    return iv;
  }

  @Override
  public void setIv(byte[] iv) {
    Preconditions.checkNotNull(iv, "IV cannot be null");
    Preconditions.checkArgument(iv.length == getIvLength(), "Invalid IV length");
    this.iv = iv;
  }

  @Override
  public void setAlgorithmParameter(AlgorithmParameterSpec algorithmParameter) {
    this.algorithmParameter = algorithmParameter;
    hasCustomAlgorithmParameter = true;
  }

  @Override
  public void reset() {
    init();
  }

  protected void init() {
    try {
      if (iv == null) {
        initIv();
      }
      if (!hasCustomAlgorithmParameter) {
        initAlgorithmParameter();
      }
      cipher.init(getOperationMode(), key, algorithmParameter);
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new RuntimeException(e);
    }
    initialized = true;
  }

  protected abstract void initIv();

  protected abstract int getOperationMode();

  protected void initAlgorithmParameter() {
    algorithmParameter = new IvParameterSpec(getIv());
  }
}
