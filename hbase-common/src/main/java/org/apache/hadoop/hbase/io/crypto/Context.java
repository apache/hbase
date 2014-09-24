/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.io.crypto;

import java.security.Key;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.google.common.base.Preconditions;

/**
 * Crypto context. Encapsulates an encryption algorithm and its key material.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Context implements Configurable {
  private Configuration conf;
  private Cipher cipher;
  private Key key;
  private String keyHash;

  Context(Configuration conf) {
    this.conf = conf;
  }

  Context() {
    this(HBaseConfiguration.create());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return "cipher=" + (cipher != null ? cipher.getName() : "NONE")
        + " keyHash=" + (keyHash != null ? keyHash.substring(0, 8) + "..." : "NONE");
  }

  public Cipher getCipher() {
    return cipher;
  }

  public Context setCipher(Cipher cipher) {
    this.cipher = cipher;
    return this;
  }

  public byte[] getKeyBytes() {
    return key.getEncoded();
  }

  public String getKeyBytesHash() {
    return keyHash;
  }

  public String getKeyFormat() {
    return key.getFormat();
  }

  public Key getKey() {
    return key;
  }

  public Context setKey(Key key) {
    Preconditions.checkNotNull(cipher, "Context does not have a cipher");
    // validate the key length
    byte[] encoded = key.getEncoded();
    if (encoded.length != cipher.getKeyLength()) {
      throw new RuntimeException("Illegal key length, have=" + encoded.length +
        ", want=" + cipher.getKeyLength());
    }
    this.key = key;
    this.keyHash = MD5Hash.getMD5AsHex(encoded);
    return this;
  }
}
