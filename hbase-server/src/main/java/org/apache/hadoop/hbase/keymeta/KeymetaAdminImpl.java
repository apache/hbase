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
    ManagedKeyData markerKey = getKeyManagementStateMarker(keyCust, keyNamespace);
    if (markerKey != null && markerKey.getKeyState() == ManagedKeyState.ACTIVE) {
      LOG.info(
        "enableManagedKeys: specified (custodian: {}, namespace: {}) already has "
          + "an active managed key with metadata: {}",
        encodedCust, keyNamespace, markerKey.getKeyMetadata());
      // ACTIVE marker contains the full key data, so we can return it directly.
      return markerKey;
    }

    // Retrieve an active key from provider if this is the first time enabling key management or
    // the previous attempt left it in a non-ACTIVE state. This may or may not succeed. When fails,
    // it can leave the key management state in FAILED or DISABLED.
    return KeyManagementUtils.retrieveActiveKey(getKeyProvider(), this, encodedCust, keyCust,
      keyNamespace, null);
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

    List<ServerName> regionServers = master.getServerManager().getOnlineServersList();

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

    List<ServerName> regionServers = master.getServerManager().getOnlineServersList();

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

    List<ServerName> regionServers = master.getServerManager().getOnlineServersList();

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
    String encodedCust = LOG.isInfoEnabled() ? ManagedKeyProvider.encodeToStr(keyCust) : null;
    LOG.info("disableKeyManagement started for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Add key management state marker for the specified (keyCust, keyNamespace) combination
    addKeyManagementStateMarker(keyCust, keyNamespace, ManagedKeyState.DISABLED);

    // Get all keys for the specified custodian and namespace
    List<ManagedKeyData> allKeys = getAllKeys(keyCust, keyNamespace);

    // Disable keys with non-null metadata
    for (ManagedKeyData keyData : allKeys) {
      if (keyData.getKeyState().getExternalState() != ManagedKeyState.DISABLED) {
        String encodedHash =
          LOG.isInfoEnabled() ? ManagedKeyProvider.encodeToStr(keyData.getKeyMetadataHash()) : null;
        LOG.info("Disabling key with metadata hash: {} for custodian: {} under namespace: {}",
          encodedHash, encodedCust, keyNamespace);
        disableKey(keyData);
        ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, keyData.getKeyMetadata());
      }
    }

    LOG.info("disableKeyManagement completed for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);
    // Retrieve and return updated keys
    List<ManagedKeyData> updatedKeys = getAllKeys(keyCust, keyNamespace);
    return updatedKeys;
  }

  @Override
  public ManagedKeyData disableManagedKey(byte[] keyCust, String keyNamespace,
    byte[] keyMetadataHash) throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = LOG.isInfoEnabled() ? ManagedKeyProvider.encodeToStr(keyCust) : null;
    String encodedHash =
      LOG.isInfoEnabled() ? ManagedKeyProvider.encodeToStr(keyMetadataHash) : null;
    LOG.info("Disabling managed key with metadata hash: {} for custodian: {} under namespace: {}",
      encodedHash, encodedCust, keyNamespace);

    // First retrieve the key to verify it exists and get the full metadata for cache ejection
    ManagedKeyData existingKey = getKey(keyCust, keyNamespace, keyMetadataHash);
    if (existingKey == null) {
      throw new IOException("Key not found for (custodian: " + encodedCust + ", namespace: "
        + keyNamespace + ") with metadata hash: " + encodedHash);
    }
    if (existingKey.getKeyState().getExternalState() == ManagedKeyState.DISABLED) {
      throw new IOException("Key is already disabled for (custodian: " + encodedCust
        + ", namespace: " + keyNamespace + ") with metadata hash: " + encodedHash);
    }

    disableKey(existingKey);
    // Eject from cache on all region servers (requires full metadata)
    ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, existingKey.getKeyMetadata());

    LOG.info("Successfully disabled managed key with metadata hash: {} for custodian: {} under "
      + "namespace: {}", encodedHash, encodedCust, keyNamespace);
    // Retrieve and return the disabled key
    ManagedKeyData disabledKey = getKey(keyCust, keyNamespace, keyMetadataHash);
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
    return KeyManagementUtils.rotateActiveKey(getKeyProvider(), this, encodedCust, keyCust,
      keyNamespace);
  }

  @Override
  public void refreshManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    String encodedCust = ManagedKeyProvider.encodeToStr(keyCust);
    LOG.info("refreshManagedKeys started for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // First, get all keys for the specified custodian and namespace and refresh those that have a
    // non-null metadata.
    List<ManagedKeyData> allKeys = getAllKeys(keyCust, keyNamespace);
    IOException refreshException = null;
    for (ManagedKeyData keyData : allKeys) {
      if (keyData.getKeyMetadata() == null) {
        continue;
      }
      LOG.debug(
        "Refreshing key with metadata hash: {} for custodian: {} under namespace: {} with state: {}",
        keyData.getKeyMetadataHashEncoded(), encodedCust, keyNamespace, keyData.getKeyState());
      try {
        ManagedKeyData refreshedKey =
          KeyManagementUtils.refreshKey(getKeyProvider(), this, keyData);
        if (refreshedKey == keyData) {
          LOG.debug("Key with metadata hash: {} for custodian: {} under namespace: {} is unchanged",
            keyData.getKeyMetadataHashEncoded(), encodedCust, keyNamespace);
        } else {
          if (refreshedKey.getKeyState().getExternalState() == ManagedKeyState.DISABLED) {
            LOG.info("Refreshed key is DISABLED, ejecting from cache");
            ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, refreshedKey.getKeyMetadata());
          } else {
            LOG.info(
              "Successfully refreshed key with metadata hash: {} for custodian: {} under namespace: {}",
              refreshedKey.getKeyMetadataHashEncoded(), encodedCust, keyNamespace);
          }
        }
      } catch (IOException | KeyException e) {
        LOG.error(
          "Failed to refresh key with metadata hash: {} for custodian: {} under namespace: {}",
          keyData.getKeyMetadataHashEncoded(), encodedCust, keyNamespace, e);
        if (refreshException == null) {
          refreshException = new IOException("Key refresh failed for (custodian: " + encodedCust
            + ", namespace: " + keyNamespace + ")", e);
        }
        // Continue refreshing other keys
      }
    }
    if (refreshException != null) {
      throw refreshException;
    }

    ManagedKeyData markerKey = getKeyManagementStateMarker(keyCust, keyNamespace);
    if (markerKey != null && markerKey.getKeyState() == ManagedKeyState.FAILED) {
      LOG.info("Found FAILED marker for (custodian: " + encodedCust + ", namespace: " + keyNamespace
        + ") indicating previous attempt to enable, reattempting to enable key management");
      enableKeyManagement(keyCust, keyNamespace);
    }

    LOG.info("refreshManagedKeys completed for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);
  }

  protected AsyncAdmin getAsyncAdmin(MasterServices master) {
    return master.getAsyncClusterConnection().getAdmin();
  }
}
