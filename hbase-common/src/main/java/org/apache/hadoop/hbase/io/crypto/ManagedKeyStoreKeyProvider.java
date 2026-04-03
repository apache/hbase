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
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentity;
import org.apache.hadoop.hbase.keymeta.ManagedKeyIdentityUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class ManagedKeyStoreKeyProvider extends KeyStoreKeyProvider implements ManagedKeyProvider {
  public static final String KEY_METADATA_ALIAS = "KeyAlias";
  public static final String KEY_METADATA_CUST = "KeyCustodian";
  public static final String KEY_METADATA_NAMESPACE = "KeyNamespace";

  private static final java.lang.reflect.Type KEY_METADATA_TYPE =
    new TypeToken<HashMap<String, String>>() {
    }.getType();

  private Configuration conf;

  @Override
  public void initConfig(Configuration conf, String providerParameters) {
    this.conf = conf;
    if (providerParameters != null) {
      super.init(providerParameters);
    }
  }

  @Override
  public ManagedKeyData getSystemKey(byte[] clusterId) {
    checkConfig();
    String systemKeyAlias =
      conf.get(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY, null);
    if (systemKeyAlias == null) {
      throw new RuntimeException("No alias configured for system key");
    }
    Key key = getKey(systemKeyAlias);
    if (key == null) {
      throw new RuntimeException("Unable to find system key with alias: " + systemKeyAlias);
    }
    // Encode clusterId too for consistency with that of key custodian.
    String keyMetadata = generateKeyMetadata(systemKeyAlias,
      ManagedKeyProvider.encodeToStr(clusterId), ManagedKeyData.KEY_SPACE_GLOBAL);
    return new ManagedKeyData(clusterId, ManagedKeyData.KEY_SPACE_GLOBAL_BYTES.copyBytes(), key,
      ManagedKeyState.ACTIVE, keyMetadata);
  }

  @Override
  public ManagedKeyData getManagedKey(ManagedKeyIdentity keyIdentity) throws IOException {
    checkConfig();
    String encodedCust = keyIdentity.getCustodianEncoded();
    String key_namespace = keyIdentity.getNamespaceString();
    if (key_namespace == null) {
      key_namespace = ManagedKeyData.KEY_SPACE_GLOBAL;
    }

    // Get alias configuration for the specific custodian+namespace combination
    String aliasConfKey = buildAliasConfKey(encodedCust, key_namespace);
    String alias = conf.get(aliasConfKey, null);

    // Generate metadata with actual alias (used for both success and failure cases)
    String keyMetadata = generateKeyMetadata(alias, encodedCust, key_namespace);

    // If no alias is configured for this custodian+namespace combination, treat as key not found
    if (alias == null) {
      return new ManagedKeyData(keyIdentity.getCustodianView(), keyIdentity.getNamespaceView(),
        null, ManagedKeyState.FAILED, keyMetadata);
    }

    // Namespaces match, proceed to get the key
    return unwrapKey(keyIdentity, keyMetadata, null);
  }

  @Override
  public ManagedKeyData unwrapKey(ManagedKeyIdentity keyIdentity, String keyMetadataStr,
    byte[] wrappedKey) throws IOException {
    Map<String, String> keyMetadata =
      GsonUtil.getDefaultInstance().fromJson(keyMetadataStr, KEY_METADATA_TYPE);
    String encodedCust = keyMetadata.get(KEY_METADATA_CUST);
    String namespace = keyMetadata.get(KEY_METADATA_NAMESPACE);
    if (namespace == null) {
      // For backwards compatibility, default to global namespace
      namespace = ManagedKeyData.KEY_SPACE_GLOBAL;
    }
    String activeStatusConfKey = buildActiveStatusConfKey(encodedCust, namespace);
    boolean isActive = conf.getBoolean(activeStatusConfKey, true);
    String alias = keyMetadata.get(KEY_METADATA_ALIAS);
    Key key = alias != null ? getKey(alias) : null;
    ManagedKeyIdentity fullKeyIdentity;
    if (keyIdentity == null) {
      fullKeyIdentity = ManagedKeyIdentityUtils.buildIdentityFromMetadata(
        ManagedKeyProvider.decodeToBytes(encodedCust), Bytes.toBytes(namespace), keyMetadataStr);
    } else {
      fullKeyIdentity = keyIdentity.getPartialIdentityLength() > 0
        ? keyIdentity
        : ManagedKeyIdentityUtils.buildIdentityFromMetadata(keyIdentity.getCustodianView(),
          keyIdentity.getNamespaceView(), keyMetadataStr);
    }
    if (key != null) {
      return new ManagedKeyData(fullKeyIdentity, key,
        isActive ? ManagedKeyState.ACTIVE : ManagedKeyState.INACTIVE, keyMetadataStr);
    }
    return new ManagedKeyData(fullKeyIdentity, null,
      isActive ? ManagedKeyState.FAILED : ManagedKeyState.DISABLED, keyMetadataStr);
  }

  private void checkConfig() {
    if (conf == null) {
      throw new IllegalStateException("initConfig is not called or config is null");
    }
  }

  public static String generateKeyMetadata(String aliasName, String encodedCust) {
    return generateKeyMetadata(aliasName, encodedCust, ManagedKeyData.KEY_SPACE_GLOBAL);
  }

  public static String generateKeyMetadata(String aliasName, String encodedCust, String namespace) {
    Map<String, String> metadata = new HashMap<>(3);
    metadata.put(KEY_METADATA_ALIAS, aliasName);
    metadata.put(KEY_METADATA_CUST, encodedCust);
    metadata.put(KEY_METADATA_NAMESPACE, namespace);
    return GsonUtil.getDefaultInstance().toJson(metadata, HashMap.class);
  }

  private String buildAliasConfKey(String encodedCust, String namespace) {
    return HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encodedCust + "." + namespace
      + ".alias";
  }

  private String buildActiveStatusConfKey(String encodedCust, String namespace) {
    return HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encodedCust + "." + namespace
      + ".active";
  }
}
