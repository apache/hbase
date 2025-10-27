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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class KeymetaAdminImpl extends KeymetaTableAccessor implements KeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaAdminImpl.class);

  public KeymetaAdminImpl(Server server) {
    super(server);
  }

  @Override
  public ManagedKeyData enableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = ManagedKeyProvider.encodeToStr(keyCust);
    LOG.info("Trying to enable key management on custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Check if (cust, namespace) pair is already enabled and has an active key.
    ManagedKeyData activeKey = getActiveKey(keyCust, keyNamespace);
    if (activeKey != null) {
      LOG.info(
        "enableManagedKeys: specified (custodian: {}, namespace: {}) already has "
          + "an active managed key with metadata: {}",
        encodedCust, keyNamespace, activeKey.getKeyMetadata());
      return activeKey;
    }

    // Retrieve a single key from provider
    ManagedKeyData retrievedKey =
      KeyManagementUtils.retrieveActiveKey(this, encodedCust, keyCust, keyNamespace, null);
    return retrievedKey;
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    if (LOG.isInfoEnabled()) {
      LOG.info("Getting key statuses for custodian: {} under namespace: {}",
        ManagedKeyProvider.encodeToStr(keyCust), keyNamespace);
    }
    return getAllKeys(keyCust, keyNamespace);
  }

  @Override
  public boolean rotateSTK() throws IOException {
    assertKeyManagementEnabled();
    if (!(getServer() instanceof MasterServices)) {
      throw new IOException("rotateSTK can only be called on master");
    }
    MasterServices master = (MasterServices) getServer();

    LOG.info("Checking if System Key is rotated");
    boolean rotated = master.rotateSystemKeyIfChanged();

    if (!rotated) {
      LOG.info("No change in System Key is detected");
      return false;
    }

    Set<ServerName> regionServers = master.getServerManager().getOnlineServers().keySet();

    LOG.info("System Key is rotated, initiating cache refresh on all region servers");
    try {
      FutureUtils.get(getAsyncAdmin(master).refreshSystemKeyCacheOnServers(regionServers));
    } catch (Exception e) {
      throw new IOException(
        "Failed to initiate System Key cache refresh on one or more region servers", e);
    }

    LOG.info("System Key rotation and cache refresh completed successfully");
    return true;
  }

  /**
   * Eject a specific managed key entry from the managed key data cache on all specified region
   * servers.
   * @param regionServers   the set of region servers to eject the managed key entry from
   * @param keyCustodian    the key custodian
   * @param keyNamespace    the key namespace
   * @param keyMetadataHash the hash of the key metadata
   * @throws IOException if the operation fails
   */
  public void ejectManagedKeyDataCacheEntryOnAllServers(Set<ServerName> regionServers,
    byte[] keyCustodian, String keyNamespace, byte[] keyMetadataHash) throws IOException {
    assertKeyManagementEnabled();
    if (!(getServer() instanceof MasterServices)) {
      throw new IOException(
        "ejectManagedKeyDataCacheEntryOnAllServers can only be called on master");
    }
    MasterServices master = (MasterServices) getServer();

    LOG.info("Ejecting managed key data cache entry on all region servers");
    try {
      FutureUtils.get(getAsyncAdmin(master).ejectManagedKeyDataCacheEntryOnAllServers(regionServers,
        keyCustodian, keyNamespace, keyMetadataHash));
    } catch (Exception e) {
      throw new IOException(e);
    }

    LOG.info("Successfully ejected managed key data cache entry on all region servers");
  }

  /**
   * Clear all entries in the managed key data cache on all specified region servers.
   * @param regionServers the set of region servers to clear the managed key data cache on
   * @throws IOException if the operation fails
   */
  public void clearManagedKeyDataCacheOnAllServers(Set<ServerName> regionServers)
    throws IOException {
    assertKeyManagementEnabled();
    if (!(getServer() instanceof MasterServices)) {
      throw new IOException("clearManagedKeyDataCacheOnAllServers can only be called on master");
    }
    MasterServices master = (MasterServices) getServer();

    LOG.info("Clearing managed key data cache on all region servers");
    try {
      FutureUtils.get(getAsyncAdmin(master).clearManagedKeyDataCacheOnAllServers(regionServers));
    } catch (Exception e) {
      throw new IOException(e);
    }

    LOG.info("Successfully cleared managed key data cache on all region servers");
  }

  protected AsyncAdmin getAsyncAdmin(MasterServices master) {
    return master.getAsyncClusterConnection().getAdmin();
  }
}
