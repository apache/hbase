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
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Return a fixed secret key for AES for testing.
 */
public class KeyProviderForTesting implements KeyProvider {

  private final Configuration config;

  public KeyProviderForTesting() {
    config = new Configuration();
    config.set(HConstants.CRYPTO_KEY_HASH_ALGORITHM_CONF_KEY, "MD5");
  }

  @Override
  public void init(String parameters) { }

  @Override
  public Key getKey(String name) {
    byte[] hash = Encryption.computeHash(config, Bytes.toBytes(name));
    return new SecretKeySpec(hash, "AES");
  }

  @Override
  public Key[] getKeys(String[] aliases) {
    Key[] result = new Key[aliases.length];
    for (int i = 0; i < aliases.length; i++) {
      result[i] = getKey(aliases[i]);
    }
    return result;
  }
}
