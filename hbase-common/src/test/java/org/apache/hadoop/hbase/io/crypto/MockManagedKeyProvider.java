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
 * prefix. One can control the state of a key by calling setKeyState and can rotate a key by
 * calling setKey.
 */
public class MockManagedKeyProvider extends MockAesKeyProvider implements ManagedKeyProvider {
  protected static final Logger LOG = LoggerFactory.getLogger(MockManagedKeyProvider.class);

  private boolean multikeyGenMode;
  private Map<String,Map<String, Key>> keys = new HashMap<>();
  private Map<String, Map<String, ManagedKeyData>> lastGenKeyData = new HashMap<>();
  // Keep references of all generated keys by their full and partial metadata.
  private Map<String, Key> allGeneratedKeys = new HashMap<>();
  private Map<String, ManagedKeyState> keyState = new HashMap<>();
  private String systemKeyAlias = "default_system_key_alias";

  @Override
  public void initConfig(Configuration conf) {
   // NO-OP
  }

  @Override
  public ManagedKeyData getSystemKey(byte[] systemId) throws IOException {
    return getKey(systemId, systemKeyAlias, ManagedKeyData.KEY_SPACE_GLOBAL);
  }

  @Override
  public ManagedKeyData getManagedKey(byte[] key_cust, String key_namespace)
    throws IOException {
    String alias = Bytes.toString(key_cust);
    return getKey(key_cust, alias, key_namespace);
  }

  @Override
  public ManagedKeyData unwrapKey(String keyMetadata) throws IOException {
    if (allGeneratedKeys.containsKey(keyMetadata)) {
      String[] meta_toks = keyMetadata.split(":");
      ManagedKeyState keyState = this.keyState.get(meta_toks[1]);
      ManagedKeyData managedKeyData =
        new ManagedKeyData(meta_toks[0].getBytes(), ManagedKeyData.KEY_SPACE_GLOBAL,
          allGeneratedKeys.get(keyMetadata),
          keyState == null ? ManagedKeyState.ACTIVE : keyState, keyMetadata);
      return registerKeyData(meta_toks[1], managedKeyData);
    }
    return null;
  }

  public ManagedKeyData getLastGeneratedKeyData(String alias, String keyNamespace) {
    if (! lastGenKeyData.containsKey(keyNamespace)) {
      return null;
    }
    return lastGenKeyData.get(keyNamespace).get(alias);
  }

  private ManagedKeyData registerKeyData(String alias, ManagedKeyData managedKeyData) {
    if (! lastGenKeyData.containsKey(managedKeyData.getKeyNamespace())) {
      lastGenKeyData.put(managedKeyData.getKeyNamespace(), new HashMap<>());
    }
    lastGenKeyData.get(managedKeyData.getKeyNamespace()).put(alias,
      managedKeyData);
    return managedKeyData;
  }

  public void setMultikeyGenMode(boolean multikeyGenMode) {
    this.multikeyGenMode = multikeyGenMode;
  }

  public void setMockedKeyState(String alias, ManagedKeyState status) {
    keyState.put(alias, status);
  }

  public void setMockedKey(String alias, Key key, String keyNamespace) {
    if (! keys.containsKey(keyNamespace)) {
      keys.put(keyNamespace, new HashMap<>());
    }
    Map<String, Key> keysForSpace = keys.get(keyNamespace);
    keysForSpace.put(alias, key);
  }

  public Key getMockedKey(String alias, String keySpace) {
    Map<String, Key> keysForSpace = keys.get(keySpace);
    return keysForSpace != null ? keysForSpace.get(alias) : null;
  }

  public void setClusterKeyAlias(String alias) {
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

  private ManagedKeyData getKey(byte[] key_cust, String alias, String key_namespace) {
    ManagedKeyState keyState = this.keyState.get(alias);
    if (! keys.containsKey(key_namespace)) {
      keys.put(key_namespace, new HashMap<>());
    }
    Map<String, Key> keySpace = keys.get(key_namespace);
    Key key = null;
    if (keyState != ManagedKeyState.FAILED && keyState != ManagedKeyState.DISABLED) {
      if (multikeyGenMode || ! keySpace.containsKey(alias)) {
        key = generateSecretKey();
        keySpace.put(alias, key);
      }
      key = keySpace.get(alias);
      if (key == null) {
        return null;
      }
    }
    long checksum = key == null ? 0 : ManagedKeyData.constructKeyChecksum(key.getEncoded());
    String partialMetadata = Bytes.toString(key_cust) + ":" + alias;
    String keyMetadata = partialMetadata + ":" + key_namespace + ":" + checksum;
    allGeneratedKeys.put(partialMetadata, key);
    allGeneratedKeys.put(keyMetadata, key);
    ManagedKeyData managedKeyData =
      new ManagedKeyData(key_cust, ManagedKeyData.KEY_SPACE_GLOBAL, key,
        keyState == null ? ManagedKeyState.ACTIVE : keyState, keyMetadata);
    return registerKeyData(alias, managedKeyData);
  }
}
