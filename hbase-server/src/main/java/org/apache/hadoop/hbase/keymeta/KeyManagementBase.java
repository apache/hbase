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
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;

/**
 * A base class for all keymeta accessor/manager implementations.
 */
@InterfaceAudience.Private
public abstract class KeyManagementBase {
  protected static final Logger LOG = LoggerFactory.getLogger(KeyManagementBase.class);

  private final Server server;

  private Boolean keyManagementEnabled;
  private Integer perCustNamespaceActiveKeyCount;

  public KeyManagementBase(Server server) {
    this.server = server;
  }

  protected Server getServer() {
    return server;
  }

  /**
   * A utility method for getting the managed key provider.
   * @return the key provider
   * @throws RuntimeException if no provider is configured or if the configured provider is not an
   * instance of ManagedKeyProvider
   */
  protected ManagedKeyProvider getKeyProvider() {
    KeyProvider provider = Encryption.getKeyProvider(getServer().getConfiguration());
    if (!(provider instanceof ManagedKeyProvider)) {
      throw new RuntimeException("KeyProvider: " + provider.getClass().getName()
          + " expected to be of type ManagedKeyProvider");
    }
    return (ManagedKeyProvider) provider;
  }

  /**
   * A utility method for checking if key management is enabled.
   * @return true if key management is enabled
   */
  protected boolean isKeyManagementEnabled() {
    if (keyManagementEnabled == null) {
      keyManagementEnabled = Server.isKeyManagementEnabled(getServer());
    }
    return keyManagementEnabled;
  }

  /**
   * A utility method for checking if dynamic lookup is enabled.
   * @return true if dynamic lookup is enabled
   */
  protected boolean isDynamicLookupEnabled() {
    return getServer().getConfiguration().getBoolean(
      HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_ENABLED_CONF_KEY,
      HConstants.CRYPTO_MANAGED_KEYS_DYNAMIC_LOOKUP_DEFAULT_ENABLED);
  }

  /**
   * Check if key management is enabled, otherwise throw exception.
   * @throws IOException if key management is not enabled.
   */
  protected void assertKeyManagementEnabled() throws IOException {
    if (! isKeyManagementEnabled()) {
      throw new IOException("Key manage is currently not enabled in HBase configuration");
    }
  }

  protected int getPerCustodianNamespaceActiveKeyConfCount() throws IOException {
    if (perCustNamespaceActiveKeyCount == null) {
      perCustNamespaceActiveKeyCount = getServer().getConfiguration().getInt(
        HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_COUNT,
        HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_DEFAULT_COUNT);
    }
    if (perCustNamespaceActiveKeyCount <= 0) {
      throw new IOException("Invalid value: " + perCustNamespaceActiveKeyCount + " configured for: "
          + HConstants.CRYPTO_MANAGED_KEYS_PER_CUST_NAMESPACE_ACTIVE_KEY_COUNT);
    }
    return perCustNamespaceActiveKeyCount;
  }
}
