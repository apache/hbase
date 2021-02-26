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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.crypto.aes.CommonsCryptoAES;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The default cipher provider. Supports AES via the Commons Crypto.
 */
@InterfaceAudience.Public
public final class CryptoCipherProvider implements CipherProvider {

  private static CryptoCipherProvider instance;

  public static CryptoCipherProvider getInstance() {
    if (instance != null) {
      return instance;
    }
    instance = new CryptoCipherProvider();
    return instance;
  }

  private Configuration conf = HBaseConfiguration.create();

  // Prevent instantiation
  private CryptoCipherProvider() { }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String getName() {
    return "commons";
  }

  @Override
  public Cipher getCipher(String name) {
    if (name.equalsIgnoreCase("AES")) {
      return new CommonsCryptoAES(this);
    }
    throw new RuntimeException("Cipher '" + name + "' is not supported by provider '" +
        getName() + "'");
  }

  @Override
  public String[] getSupportedCiphers() {
    return new String[] { "AES" };
  }

}
