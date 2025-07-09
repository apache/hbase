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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
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

  private Server server;
  private final Configuration configuration;

  private Boolean isDynamicLookupEnabled;
  private Boolean isKeyManagementEnabled;
  private Integer perCustNamespaceActiveKeyCount;

  /**
   * Construct with a server instance. Configuration is derived from the server.
   *
   * @param server the server instance
   */
  public KeyManagementBase(Server server) {
    this(server.getConfiguration());
    this.server = server;
  }

  /**
   * Construct with a custom configuration and no server.
   *
   * @param configuration the configuration instance
   */
  public KeyManagementBase(Configuration configuration) {
    if (configuration == null) {
      throw new IllegalArgumentException("Configuration must be non-null");
    }
    this.configuration = configuration;
  }

  protected Server getServer() {
    return server;
  }

  protected Configuration getConfiguration() {
    return configuration;
  }

  /**
   * A utility method for getting the managed key provider.
   * @return the key provider
   * @throws RuntimeException if no provider is configured or if the configured provider is not an
   *   instance of ManagedKeyProvider
   */
  protected ManagedKeyProvider getKeyProvider() {
    KeyProvider provider = Encryption.getKeyProvider(getConfiguration());
    if (!(provider instanceof ManagedKeyProvider)) {
      throw new RuntimeException("KeyProvider: " + provider.getClass().getName()
          + " expected to be of type ManagedKeyProvider");
    }
    return (ManagedKeyProvider) provider;
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

  protected int getPerCustodianNamespaceActiveKeyConfCount() throws IOException {
    if (perCustNamespaceActiveKeyCount == null) {
      perCustNamespaceActiveKeyCount = getConfiguration().getInt(
        HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_COUNT,
        HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_DEFAULT_COUNT);
    }
    if (perCustNamespaceActiveKeyCount <= 0) {
      throw new IOException("Invalid value: " + perCustNamespaceActiveKeyCount + " configured for: "
          + HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_COUNT);
    }
    return perCustNamespaceActiveKeyCount;
  }

  /**
   * Retrieves specified number of managed keys from the key provider. An attempt is made to
   * retrieve the specified number of keys, but the real number of keys retrieved may be less than
   * the specified number if the key provider is not capable of producing multiple active keys. If
   * existing keys are provided, it will be used to ensure that keys retrieved are not the same as
   * those that are already retrieved.
   *
   * @param encKeyCust the encoded key custodian
   * @param key_cust the key custodian
   * @param keyNamespace the key namespace
   * @param nKeysToRetrieve the number of keys to retrieve
   * @param existingKeys the existing keys, typically the active keys already retrieved from the
   *   key provider.
   * @return the retrieved keys
   * @throws IOException if an error occurs
   * @throws KeyException if an error occurs
   */
  protected Set<ManagedKeyData> retrieveManagedKeys(String encKeyCust, byte[] key_cust,
      String keyNamespace, int nKeysToRetrieve, Set<ManagedKeyData> existingKeys)
      throws IOException, KeyException {
    Set<ManagedKeyData> retrievedKeys = new HashSet<>(nKeysToRetrieve);
    ManagedKeyProvider provider = getKeyProvider();
    for (int i = 0; i < nKeysToRetrieve; ++i) {
      ManagedKeyData pbeKey = provider.getManagedKey(key_cust, keyNamespace);
      if (pbeKey == null) {
        throw new IOException("Invalid null managed key received from key provider");
      }
      if (retrievedKeys.contains(pbeKey) || existingKeys.contains(pbeKey)) {
        // This typically means, the key provider is not capable of producing multiple active keys.
        LOG.info("enableManagedKeys: specified (custodian: {}, namespace: {}) is configured "
            + " to have {} active keys, but received only {} unique keys.",
          encKeyCust, keyNamespace, existingKeys.size() + nKeysToRetrieve,
          existingKeys.size() + retrievedKeys.size());
        break;
      }
      retrievedKeys.add(pbeKey);
      LOG.info("enableManagedKeys: got managed key with status: {} and metadata: {} for "
          + "(custodian: {}, namespace: {})", pbeKey.getKeyState(), pbeKey.getKeyMetadata(),
        encKeyCust, keyNamespace);
      if (pbeKey.getKeyState() != ManagedKeyState.ACTIVE) {
        LOG.info("enableManagedKeys: received non-ACTIVE key with status: {} with metadata: {} for "
            + "(custodian: {}, namespace: {})",
          pbeKey.getKeyState(), pbeKey.getKeyMetadata(), encKeyCust, keyNamespace);
        break;
      }
    }
    return retrievedKeys;
  }
}
