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
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
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
    ManagedKeyData retrievedKey = KeyManagementUtils.retrieveActiveKey(getKeyProvider(), this,
      encodedCust, keyCust, keyNamespace, null);
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

  @Override
  public void ejectManagedKeyDataCacheEntry(byte[] keyCustodian, String keyNamespace,
    String keyMetadata) throws IOException {
    assertKeyManagementEnabled();
    if (!(getServer() instanceof MasterServices)) {
      throw new IOException("ejectManagedKeyDataCacheEntry can only be called on master");
    }
    MasterServices master = (MasterServices) getServer();

    Set<ServerName> regionServers = master.getServerManager().getOnlineServers().keySet();

    LOG.info("Ejecting managed key data cache entry on all region servers");
    try {
      FutureUtils.get(getAsyncAdmin(master).ejectManagedKeyDataCacheEntryOnServers(regionServers,
        keyCustodian, keyNamespace, keyMetadata));
    } catch (Exception e) {
      throw new IOException(e);
    }

    LOG.info("Successfully ejected managed key data cache entry on all region servers");
  }

  @Override
  public void clearManagedKeyDataCache() throws IOException {
    assertKeyManagementEnabled();
    if (!(getServer() instanceof MasterServices)) {
      throw new IOException("clearManagedKeyDataCache can only be called on master");
    }
    MasterServices master = (MasterServices) getServer();

    Set<ServerName> regionServers = master.getServerManager().getOnlineServers().keySet();

    LOG.info("Clearing managed key data cache on all region servers");
    try {
      FutureUtils.get(getAsyncAdmin(master).clearManagedKeyDataCacheOnServers(regionServers));
    } catch (Exception e) {
      throw new IOException(e);
    }

    LOG.info("Successfully cleared managed key data cache on all region servers");
  }

  @Override
  public List<ManagedKeyData> disableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = ManagedKeyProvider.encodeToStr(keyCust);
    LOG.info("Disabling key management for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Get all keys for the specified custodian and namespace
    List<ManagedKeyData> allKeys = getAllKeys(keyCust, keyNamespace);

    // Disable keys with non-null metadata
    for (ManagedKeyData keyData : allKeys) {
      if (keyData.getKeyMetadataHash() != null) {
        LOG.info("Disabling key with metadata: {} for custodian: {} under namespace: {}",
          keyData.getKeyMetadata(), encodedCust, keyNamespace);
        disableKey(keyCust, keyNamespace, keyData.getKeyMetadata());
        ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, keyData.getKeyMetadata());
      }
    }

    // Retrieve and return updated keys
    List<ManagedKeyData> updatedKeys = getAllKeys(keyCust, keyNamespace);
    LOG.info("Successfully disabled key management for custodian: {} under namespace: {}",
      encodedCust, keyNamespace);
    return updatedKeys;
  }

  @Override
  public ManagedKeyData disableManagedKey(byte[] keyCust, String keyNamespace, String keyMetadata)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = ManagedKeyProvider.encodeToStr(keyCust);
    LOG.info("Disabling managed key with metadata: {} for custodian: {} under namespace: {}",
      keyMetadata, encodedCust, keyNamespace);

    // Disable the key
    disableKey(keyCust, keyNamespace, keyMetadata);

    // Eject from cache on all region servers
    ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, keyMetadata);

    // Retrieve and return the disabled key
    ManagedKeyData disabledKey = getKey(keyCust, keyNamespace, keyMetadata);
    LOG.info("Successfully disabled managed key with metadata: {} for custodian: {} under "
      + "namespace: {}", keyMetadata, encodedCust, keyNamespace);
    return disabledKey;
  }

  @Override
  public ManagedKeyData rotateManagedKey(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = ManagedKeyProvider.encodeToStr(keyCust);
    LOG.info("Rotating managed key for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Attempt rotation
    ManagedKeyData rotatedKey =
      KeyManagementUtils.rotateActiveKey(getKeyProvider(), this, keyCust, keyNamespace);

    // If rotation resulted in a DISABLED key, eject from cache
    if (rotatedKey != null && rotatedKey.getKeyState() == ManagedKeyState.DISABLED) {
      LOG.info("Rotated key is DISABLED, ejecting from cache");
      ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, rotatedKey.getKeyMetadata());
    }

    if (rotatedKey != null) {
      LOG.info("Successfully rotated managed key for custodian: {} under namespace: {}, new "
        + "key metadata: {}", encodedCust, keyNamespace, rotatedKey.getKeyMetadata());
    } else {
      LOG.info("No rotation occurred for custodian: {} under namespace: {}", encodedCust,
        keyNamespace);
    }

    return rotatedKey;
  }

  @Override
  public void refreshManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = ManagedKeyProvider.encodeToStr(keyCust);
    LOG.info("Refreshing managed keys for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Get all keys for the specified custodian and namespace
    List<ManagedKeyData> allKeys = getAllKeys(keyCust, keyNamespace);

    // Refresh keys with non-null metadata
    for (ManagedKeyData keyData : allKeys) {
      if (keyData.getKeyMetadata() != null) {
        LOG.debug("Refreshing key with metadata: {} for custodian: {} under namespace: {}",
          keyData.getKeyMetadata(), encodedCust, keyNamespace);
        try {
          KeyManagementUtils.refreshKey(this, keyData);
        } catch (Exception e) {
          LOG.error("Failed to refresh key with metadata: {} for custodian: {} under namespace: {}",
            keyData.getKeyMetadata(), encodedCust, keyNamespace, e);
          // Continue refreshing other keys
        }
      }
    }

    LOG.info("Successfully refreshed managed keys for custodian: {} under namespace: {}",
      encodedCust, keyNamespace);
  }

  protected AsyncAdmin getAsyncAdmin(MasterServices master) {
    return master.getAsyncClusterConnection().getAdmin();
  }
}
