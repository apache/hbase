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
  public static final String KEY_METADATA_PREFIX = "PBE_PREFIX";

  private Configuration conf;

  @Override
  public void initConfig(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public ManagedKeyData getSystemKey(byte[] clusterId) {
    checkConfig();
    String masterKeyAlias = conf.get(HConstants.CRYPTO_PBE_MASTERKEY_NAME_CONF_KEY, null);
    if (masterKeyAlias == null) {
      throw new RuntimeException("No alias configured for master key");
    }
    Key key = getKey(masterKeyAlias);
    if (key == null) {
      throw new RuntimeException("Unable to find cluster key with alias: " + masterKeyAlias);
    }
    // Encode clusterId too for consistency with that of PBE prefixes.
    String keyMetadata = generateKeyMetadata(masterKeyAlias,
      ManagedKeyProvider.encodeToStr(clusterId));
    return new ManagedKeyData(clusterId, ManagedKeyData.KEY_NAMESPACE_GLOBAL, key, ManagedKeyStatus.ACTIVE,
      keyMetadata);
  }

  @Override
  public ManagedKeyData getManagedKey(byte[] key_cust, String key_namespace) throws IOException {
    checkConfig();
    String encodedPrefix = ManagedKeyProvider.encodeToStr(key_cust);
    String aliasConfKey = HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + encodedPrefix + "." +
      "alias";
    String keyMetadata = generateKeyMetadata(conf.get(aliasConfKey, null), encodedPrefix);
    return unwrapKey(keyMetadata);
  }

  @Override
  public ManagedKeyData unwrapKey(String keyMetadataStr) throws IOException {
    Map<String, String> keyMetadata = GsonUtil.getDefaultInstance().fromJson(keyMetadataStr,
      HashMap.class);
    String encodedPrefix = keyMetadata.get(KEY_METADATA_PREFIX);
    String activeStatusConfKey = HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + encodedPrefix +
      ".active";
    boolean isActive = conf.getBoolean(activeStatusConfKey, true);
    byte[] key_cust = ManagedKeyProvider.decodeToBytes(encodedPrefix);
    String alias = keyMetadata.get(KEY_METADATA_ALIAS);
    Key key = alias != null ? getKey(alias) : null;
    if (key != null) {
      return new ManagedKeyData(key_cust, ManagedKeyData.KEY_NAMESPACE_GLOBAL, key,
        isActive ? ManagedKeyStatus.ACTIVE : ManagedKeyStatus.INACTIVE, keyMetadataStr);
    }
    return new ManagedKeyData(key_cust, ManagedKeyData.KEY_NAMESPACE_GLOBAL, null,
      isActive ? ManagedKeyStatus.FAILED : ManagedKeyStatus.DISABLED, keyMetadataStr);
  }

  private void checkConfig() {
    if (conf == null) {
      throw new IllegalStateException("initConfig is not called or config is null");
    }
  }

  public static String generateKeyMetadata(String aliasName, String encodedPrefix) {
    return GsonUtil.getDefaultInstance().toJson(new HashMap<String, String>() {{
      put(KEY_METADATA_ALIAS, aliasName);
      put(KEY_METADATA_PREFIX, encodedPrefix);
    }}, HashMap.class);
  }

}
