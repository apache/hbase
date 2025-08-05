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

import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class ManagedKeyStoreKeyProvider extends KeyStoreKeyProvider implements ManagedKeyProvider {
  public static final String KEY_METADATA_ALIAS = "KeyAlias";
  public static final String KEY_METADATA_CUST = "KeyCustodian";

  private static final java.lang.reflect.Type KEY_METADATA_TYPE =
    new TypeToken<HashMap<String, String>>(){}.getType();

  private Configuration conf;

  @Override
  public void initConfig(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public ManagedKeyData getSystemKey(byte[] clusterId) {
    checkConfig();
    String systemKeyAlias = conf.get(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY,
      null);
    if (systemKeyAlias == null) {
      throw new RuntimeException("No alias configured for system key");
    }
    Key key = getKey(systemKeyAlias);
    if (key == null) {
      throw new RuntimeException("Unable to find system key with alias: " + systemKeyAlias);
    }
    // Encode clusterId too for consistency with that of key custodian.
    String keyMetadata = generateKeyMetadata(systemKeyAlias,
      ManagedKeyProvider.encodeToStr(clusterId));
    return new ManagedKeyData(clusterId, ManagedKeyData.KEY_SPACE_GLOBAL, key,
        ManagedKeyState.ACTIVE, keyMetadata);
  }

  @Override
  public ManagedKeyData getManagedKey(byte[] key_cust, String key_namespace) throws IOException {
    checkConfig();
    String encodedCust = ManagedKeyProvider.encodeToStr(key_cust);
    String aliasConfKey = HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encodedCust + "." +
      "alias";
    String keyMetadata = generateKeyMetadata(conf.get(aliasConfKey, null), encodedCust);
    return unwrapKey(keyMetadata, null);
  }

  @Override
  public ManagedKeyData unwrapKey(String keyMetadataStr, byte[] wrappedKey) throws IOException {
    Map<String, String> keyMetadata = GsonUtil.getDefaultInstance().fromJson(keyMetadataStr,
      KEY_METADATA_TYPE);
    String encodedCust = keyMetadata.get(KEY_METADATA_CUST);
    String activeStatusConfKey = HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encodedCust +
      ".active";
    boolean isActive = conf.getBoolean(activeStatusConfKey, true);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(encodedCust);
    String alias = keyMetadata.get(KEY_METADATA_ALIAS);
    Key key = alias != null ? getKey(alias) : null;
    if (key != null) {
      return new ManagedKeyData(key_cust, ManagedKeyData.KEY_SPACE_GLOBAL, key,
        isActive ? ManagedKeyState.ACTIVE : ManagedKeyState.INACTIVE, keyMetadataStr);
    }
    return new ManagedKeyData(key_cust, ManagedKeyData.KEY_SPACE_GLOBAL, null,
      isActive ? ManagedKeyState.FAILED : ManagedKeyState.DISABLED, keyMetadataStr);
  }

  private void checkConfig() {
    if (conf == null) {
      throw new IllegalStateException("initConfig is not called or config is null");
    }
  }

  public static String generateKeyMetadata(String aliasName, String encodedCust) {
    Map<String, String> metadata = new HashMap<>(2);
    metadata.put(KEY_METADATA_ALIAS, aliasName);
    metadata.put(KEY_METADATA_CUST, encodedCust);
    return GsonUtil.getDefaultInstance().toJson(metadata, HashMap.class);
  }
}
