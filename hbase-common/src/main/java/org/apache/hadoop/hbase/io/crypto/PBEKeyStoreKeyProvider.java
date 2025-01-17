package org.apache.hadoop.hbase.io.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import java.security.Key;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@InterfaceAudience.Public
public class PBEKeyStoreKeyProvider extends KeyStoreKeyProvider implements PBEKeyProvider {
  public static final String KEY_METADATA_ALIAS = "KeyAlias";
  public static final String KEY_METADATA_PREFIX = "PBE_PREFIX";

  private Configuration conf;

  @Override public void initConfig(Configuration conf) {
    this.conf = conf;
  }

  @Override public PBEKeyData getClusterKey(byte[] clusterId) {
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
      Base64.getEncoder().encodeToString(clusterId));
    return new PBEKeyData(clusterId, key, PBEKeyStatus.ACTIVE, keyMetadata);
  }

  @Override public PBEKeyData getPBEKey(byte[] pbe_prefix) {
    checkConfig();
    String encodedPrefix = Base64.getEncoder().encodeToString(pbe_prefix);
    String aliasConfKey = HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + encodedPrefix + "." +
      "alias";
    String keyAlias = conf.get(aliasConfKey, null);
    if (keyAlias != null) {
      String keyMetadata = generateKeyMetadata(keyAlias, encodedPrefix);
      return unwrapKey(keyMetadata);
    }
    return null;
  }

  @Override public PBEKeyData unwrapKey(String keyMetadataStr) {
    Map<String, String> keyMetadata = GsonUtil.getDefaultInstance().fromJson(keyMetadataStr,
      HashMap.class);
    String alias = keyMetadata.get(KEY_METADATA_ALIAS);
    Key key = getKey(alias);
    if (key != null) {
      String encodedPrefix = keyMetadata.get(KEY_METADATA_PREFIX);
      String activeStatusConfKey = HConstants.CRYPTO_PBE_PREFIX_CONF_KEY_PREFIX + encodedPrefix +
        ".active";
      boolean isActive = conf.getBoolean(activeStatusConfKey, true);
      return new PBEKeyData(Base64.getDecoder().decode(encodedPrefix), key,
        isActive ? PBEKeyStatus.ACTIVE : PBEKeyStatus.INACTIVE, keyMetadataStr);
    }
    return null;
  }

  private String generateKeyMetadata(String aliasName, String encodedPrefix) {
    return GsonUtil.getDefaultInstance().toJson(new HashMap<String, String>() {{
      put(KEY_METADATA_ALIAS, aliasName);
      put(KEY_METADATA_PREFIX, encodedPrefix);
    }}, HashMap.class);
  }

  private void checkConfig() {
    if (conf == null) {
      throw new IllegalStateException("initConfig is not called or config is null");
    }
  }
}
