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
package org.apache.hadoop.hbase.keymeta;

import java.io.IOException;
import java.security.KeyException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.security.SecurityUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for all keymeta accessor/manager implementations.
 */
@InterfaceAudience.Private
public abstract class KeyManagementBase {
  protected static final Logger LOG = LoggerFactory.getLogger(KeyManagementBase.class);

  private KeyManagementService keyManagementService;
  private final Configuration configuration;

  private Boolean isDynamicLookupEnabled;
  private Boolean isKeyManagementEnabled;

  /**
   * Construct with a server instance. Configuration is derived from the server.
   * @param server the server instance
   */
  public KeyManagementBase(KeyManagementService keyManagementService) {
    this(keyManagementService.getConfiguration());
    this.keyManagementService = keyManagementService;
  }

  /**
   * Construct with a custom configuration and no server.
   * @param configuration the configuration instance
   */
  public KeyManagementBase(Configuration configuration) {
    if (configuration == null) {
      throw new IllegalArgumentException("Configuration must be non-null");
    }
    this.configuration = configuration;
  }

  protected KeyManagementService getKeyManagementService() {
    return keyManagementService;
  }

  protected Configuration getConfiguration() {
    return configuration;
  }

  /**
   * A utility method for getting the managed key provider.
   * @return the managed key provider
   * @throws RuntimeException if no provider is configured
   */
  protected ManagedKeyProvider getKeyProvider() {
    return Encryption.getManagedKeyProvider(getConfiguration());
  }

  /**
   * A utility method for checking if dynamic lookup is enabled.
   * @return true if dynamic lookup is enabled
   */
  protected boolean isDynamicLookupEnabled() {
    if (isDynamicLookupEnabled == null) {
      isDynamicLookupEnabled = getConfiguration().getBoolean(
        HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_ENABLED_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_DEFAULT_ENABLED);
    }
    return isDynamicLookupEnabled;
  }

  /**
   * Check if key management is enabled, otherwise throw exception.
   * @throws IOException if key management is not enabled.
   */
  protected void assertKeyManagementEnabled() throws IOException {
    if (!isKeyManagementEnabled()) {
      throw new IOException("Key manage is currently not enabled in HBase configuration");
    }
  }

  protected boolean isKeyManagementEnabled() {
    if (isKeyManagementEnabled == null) {
      isKeyManagementEnabled = SecurityUtil.isKeyManagementEnabled(getConfiguration());
    }
    return isKeyManagementEnabled;
  }

  /**
   * Utility function to retrieves a managed key from the key provider. If an existing key is
   * provided and the retrieved key is the same as the existing key, it will be ignored.
   * @param encKeyCust        the encoded key custodian
   * @param key_cust          the key custodian
   * @param keyNamespace      the key namespace
   * @param accessor          the accessor to use to persist the key. If null, the key will not be
   *                          persisted.
   * @param existingActiveKey the existing key, typically the active key already retrieved from the
   *                          key provider, can be null.
   * @return the retrieved key, or null if no key could be retrieved
   * @throws IOException  if an error occurs
   * @throws KeyException if an error occurs
   */
  protected ManagedKeyData retrieveActiveKey(String encKeyCust, byte[] key_cust,
    String keyNamespace, KeymetaTableAccessor accessor, ManagedKeyData existingActiveKey)
    throws IOException, KeyException {
    ManagedKeyProvider provider = getKeyProvider();
    ManagedKeyData pbeKey = provider.getManagedKey(key_cust, keyNamespace);
    if (pbeKey == null) {
      throw new IOException("Invalid null managed key received from key provider");
    }
    /*
     * Will be useful when refresh API is implemented. if (existingActiveKey != null &&
     * existingActiveKey.equals(pbeKey)) {
     * LOG.info("retrieveManagedKey: no change in key for (custodian: {}, namespace: {}",
     * encKeyCust, keyNamespace); return null; } // TODO: If existingActiveKey is not null, we
     * should update the key state to INACTIVE.
     */
    LOG.info(
      "retrieveManagedKey: got managed key with status: {} and metadata: {} for "
        + "(custodian: {}, namespace: {})",
      pbeKey.getKeyState(), pbeKey.getKeyMetadata(), encKeyCust, pbeKey.getKeyNamespace());
    if (accessor != null) {
      accessor.addKey(pbeKey);
    }
    return pbeKey;
  }
}
