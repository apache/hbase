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
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.KeyGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple implementation of PBEKeyProvider for testing. It generates a key on demand given a
 * prefix. One can control the state of a key by calling setKeyStatus and can rotate a key by
 * calling setKey.
 */
public class MockPBEKeyProvider extends MockAesKeyProvider implements PBEKeyProvider {
  protected static final Logger LOG = LoggerFactory.getLogger(MockPBEKeyProvider.class);

  public Map<String, Key> keys = new HashMap<>();
  public Map<String, PBEKeyStatus> keyStatus = new HashMap<>();
  private String systemKeyAlias = "default_system_key_alias";

  @Override public void initConfig(Configuration conf) {
   // NO-OP
  }

  @Override public PBEKeyData getSystemKey(byte[] systemId) throws IOException {
    return getKey(systemId, systemKeyAlias);
  }

  @Override public PBEKeyData getPBEKey(byte[] pbe_prefix, String key_namespace)
    throws IOException {
    return getKey(pbe_prefix);
  }

  @Override public PBEKeyData unwrapKey(String keyMetadata) throws IOException {
    String[] meta_toks = keyMetadata.split(":");
    if (keys.containsKey(meta_toks[1])) {
      return getKey(meta_toks[0].getBytes(), meta_toks[1]);
    }
    return null;
  }

  /**
   * Lookup the key data for the given prefix from keys. If missing, initialize one using generateSecretKey().
   */
  public PBEKeyData getKey(byte[] prefix_bytes) {
    String alias = Bytes.toString(prefix_bytes);
    return getKey(prefix_bytes, alias);
  }

  public PBEKeyData getKey(byte[] prefix_bytes, String alias) {
    Key key = keys.get(alias);
    if (key == null) {
      key = generateSecretKey();
      keys.put(alias, key);
    }
    PBEKeyStatus keyStatus = this.keyStatus.get(alias);
    return new PBEKeyData(prefix_bytes, PBEKeyData.KEY_NAMESPACE_GLOBAL, key,
      keyStatus == null ? PBEKeyStatus.ACTIVE : keyStatus,
      Bytes.toString(prefix_bytes)+":"+alias);
  }

  public void setKeyStatus(String alias, PBEKeyStatus status) {
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
}
