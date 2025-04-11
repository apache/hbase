package org.apache.hadoop.hbase.io.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Public
public class ManagedKeyStoreKeyProvider extends KeyStoreKeyProvider implements ManagedKeyProvider {
  public static final String KEY_METADATA_ALIAS = "KeyAlias";
  public static final String KEY_METADATA_CUST = "KeyCustodian";

  private Configuration conf;

  @Override
  public void initConfig(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public ManagedKeyData getSystemKey(byte[] clusterId) {
    checkConfig();
    String masterKeyAlias = conf.get(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY, null);
    if (masterKeyAlias == null) {
      throw new RuntimeException("No alias configured for master key");
    }
    Key key = getKey(masterKeyAlias);
    if (key == null) {
      throw new RuntimeException("Unable to find cluster key with alias: " + masterKeyAlias);
    }
    // Encode clusterId too for consistency with that of key custodian.
    String keyMetadata = generateKeyMetadata(masterKeyAlias,
      ManagedKeyProvider.encodeToStr(clusterId));
    return new ManagedKeyData(clusterId, ManagedKeyData.KEY_SPACE_GLOBAL, key, ManagedKeyStatus.ACTIVE,
      keyMetadata);
  }

  @Override
  public ManagedKeyData getManagedKey(byte[] key_cust, String key_namespace) throws IOException {
    checkConfig();
    String encodedCust = ManagedKeyProvider.encodeToStr(key_cust);
    String aliasConfKey = HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encodedCust + "." +
      "alias";
    String keyMetadata = generateKeyMetadata(conf.get(aliasConfKey, null), encodedCust);
    return unwrapKey(keyMetadata);
  }

  @Override
  public ManagedKeyData unwrapKey(String keyMetadataStr) throws IOException {
    Map<String, String> keyMetadata = GsonUtil.getDefaultInstance().fromJson(keyMetadataStr,
      HashMap.class);
    String encodedCust = keyMetadata.get(KEY_METADATA_CUST);
    String activeStatusConfKey = HConstants.CRYPTO_MANAGED_KEY_STORE_CONF_KEY_PREFIX + encodedCust +
      ".active";
    boolean isActive = conf.getBoolean(activeStatusConfKey, true);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(encodedCust);
    String alias = keyMetadata.get(KEY_METADATA_ALIAS);
    Key key = alias != null ? getKey(alias) : null;
    if (key != null) {
      return new ManagedKeyData(key_cust, ManagedKeyData.KEY_SPACE_GLOBAL, key,
        isActive ? ManagedKeyStatus.ACTIVE : ManagedKeyStatus.INACTIVE, keyMetadataStr);
    }
    return new ManagedKeyData(key_cust, ManagedKeyData.KEY_SPACE_GLOBAL, null,
      isActive ? ManagedKeyStatus.FAILED : ManagedKeyStatus.DISABLED, keyMetadataStr);
  }

  private void checkConfig() {
    if (conf == null) {
      throw new IllegalStateException("initConfig is not called or config is null");
    }
  }

  public static String generateKeyMetadata(String aliasName, String encodedCust) {
    return GsonUtil.getDefaultInstance().toJson(new HashMap<String, String>() {{
      put(KEY_METADATA_ALIAS, aliasName);
      put(KEY_METADATA_CUST, encodedCust);
    }}, HashMap.class);
  }

}
