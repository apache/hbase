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
package org.apache.hadoop.hbase.io.crypto;

import java.security.Key;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.spec.SecretKeySpec;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Return a fixed secret key for AES for testing.
 */
@InterfaceAudience.Private
public class MockAesKeyProvider implements KeyProvider {

  private Map<String, Key> keys = new HashMap<>();

  private boolean cacheKeys = false;

  @Override
  public void init(String parameters) {
    cacheKeys = Boolean.parseBoolean(parameters);
  }

  @Override
  public Key getKey(String name) {
    return new SecretKeySpec(Encryption.hash128(name), "AES");
  }

  @Override
  public Key[] getKeys(String[] aliases) {
    Key[] result = new Key[aliases.length];
    for (int i = 0; i < aliases.length; i++) {
      if (keys.containsKey(aliases[i])) {
        result[i] = keys.get(aliases[i]);
      } else {
        // When not caching keys, we want to make the key generation deterministic.
        result[i] = new SecretKeySpec(
          Encryption.hash128(
            cacheKeys ? aliases[i] + "-" + String.valueOf(System.currentTimeMillis()) : aliases[i]),
          "AES");
        if (cacheKeys) {
          keys.put(aliases[i], result[i]);
        }
      }
    }
    return result;
  }

  public void clearKeys() {
    keys.clear();
  }
}
