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

import java.io.IOException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.KeyGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple implementation of ManagedKeyProvider for testing. It generates a key on demand given a
 * prefix. One can control the state of a key by calling setKeyStatus and can rotate a key by
 * calling setKey.
 */
public class MockManagedKeyProvider extends MockAesKeyProvider implements ManagedKeyProvider {
  protected static final Logger LOG = LoggerFactory.getLogger(MockManagedKeyProvider.class);

  public Map<String, Key> keys = new HashMap<>();
  public Map<String, ManagedKeyStatus> keyStatus = new HashMap<>();
  private String systemKeyAlias = "default_system_key_alias";

  @Override public void initConfig(Configuration conf) {
   // NO-OP
  }

  @Override public ManagedKeyData getSystemKey(byte[] systemId) throws IOException {
    return getKey(systemId, systemKeyAlias);
  }

  @Override public ManagedKeyData getManagedKey(byte[] key_cust, String key_namespace)
    throws IOException {
    return getKey(key_cust);
  }

  @Override public ManagedKeyData unwrapKey(String keyMetadata) throws IOException {
    String[] meta_toks = keyMetadata.split(":");
    if (keys.containsKey(meta_toks[1])) {
      return getKey(meta_toks[0].getBytes(), meta_toks[1]);
    }
    return null;
  }

  /**
   * Lookup the key data for the given key_cust from keys. If missing, initialize one using
   * generateSecretKey().
   */
  public ManagedKeyData getKey(byte[] key_cust) {
    String alias = Bytes.toString(key_cust);
    return getKey(key_cust, alias);
  }

  public void setKeyStatus(String alias, ManagedKeyStatus status) {
    keyStatus.put(alias, status);
  }

  public void setKey(String alias, Key key) {
    keys.put(alias, key);
  }

  public void setCluterKeyAlias(String alias) {
    this.systemKeyAlias = alias;
  }

  public String getSystemKeyAlias() {
    return this.systemKeyAlias;
  }

  /**
   * Generate a new secret key.
   * @return the key
   */
  public static Key generateSecretKey() {
    KeyGenerator keyGen = null;
    try {
      keyGen = KeyGenerator.getInstance("AES");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    keyGen.init(256);
    return keyGen.generateKey();
  }

  private ManagedKeyData getKey(byte[] key_cust, String alias) {
    ManagedKeyStatus keyStatus = this.keyStatus.get(alias);
    Key key = null;
    if (keyStatus != ManagedKeyStatus.FAILED && keyStatus != ManagedKeyStatus.DISABLED) {
      key = keys.get(alias);
      if (key == null) {
        key = generateSecretKey();
        keys.put(alias, key);
      }
    }
    return new ManagedKeyData(key_cust, ManagedKeyData.KEY_SPACE_GLOBAL, key,
      keyStatus == null ? ManagedKeyStatus.ACTIVE : keyStatus,
      Bytes.toString(key_cust)+":"+alias);
  }
}
