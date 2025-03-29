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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeyProvider;
import org.apache.hadoop.hbase.io.crypto.PBEKeyProvider;
import org.apache.hadoop.hbase.Server;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * A base class for all keymeta accessor/manager implementations.
 */
@InterfaceAudience.Private
public abstract class PBEKeyAccessorBase {
  protected static final Logger LOG = LoggerFactory.getLogger(PBEKeyAccessorBase.class);

  protected final Server server;

  private Boolean pbeEnabled;
  private Integer perPrefixActiveKeyCount;

  public PBEKeyAccessorBase(Server server) {
    this.server = server;
  }

  /**
   * A utility method for getting the PBE key provider.
   * @return the key provider
   * @throws RuntimeException if no provider is configured or if the configured provider is not an
   * instance of PBEKeyProvider
   */
  protected PBEKeyProvider getKeyProvider() {
    KeyProvider provider = Encryption.getKeyProvider(server.getConfiguration());
    if (!(provider instanceof PBEKeyProvider)) {
      throw new RuntimeException(
        "KeyProvider: " + provider.getClass().getName() + " expected to be of type PBEKeyProvider");
    }
    return (PBEKeyProvider) provider;
  }

  /**
   * A utility method for checking if PBE is enabled.
   * @return true if PBE is enabled
   */
  protected boolean isPBEEnabled() {
    if (pbeEnabled == null) {
      pbeEnabled = Server.isPBEEnabled(server);
    }
    return pbeEnabled;
  }

  /**
   * Check if PBE is enabled, otherwise throw exception.
   * @throws IOException if PBE is not enabled.
   */
  protected void checkPBEEnabled() throws IOException {
    if (! isPBEEnabled()) {
      throw new IOException("PBE is currently not enabled in HBase configuration");
    }
  }

  protected int getPerPrefixActiveKeyConfCount() throws IOException {
    if (perPrefixActiveKeyCount == null) {
      perPrefixActiveKeyCount = server.getConfiguration().getInt(
        HConstants.CRYPTO_PBE_PER_PREFIX_ACTIVE_KEY_COUNT,
        HConstants.CRYPTO_PBE_PER_PREFIX_ACTIVE_KEY_DEFAULT_COUNT);
    }
    if (perPrefixActiveKeyCount <= 0) {
      throw new IOException("Invalid value: " + perPrefixActiveKeyCount + " configured for: " +
        HConstants.CRYPTO_PBE_PER_PREFIX_ACTIVE_KEY_COUNT);
    }
    return perPrefixActiveKeyCount;
  }
}
