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
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link KeymetaAdmin} interface that builds on top of
 * {@link KeymetaTableAccessor} to provide management interface to avoid going through the
 * {@link KeymetaDataCache}.
 */
@InterfaceAudience.Private
public class KeymetaAdminImpl extends KeyManagementBase implements KeymetaAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KeymetaAdminImpl.class);
  private final Server server;
  private final KeymetaTableAccessor accessor;

  public KeymetaAdminImpl(Server server) {
    this(server, new KeymetaTableAccessor(server));
  }

  public KeymetaAdminImpl(Server server, KeymetaTableAccessor accessor) {
    super(server.getKeyManagementService());
    this.server = server;
    this.accessor = accessor;
  }

  protected Server getServer() {
    return server;
  }

  @Override
  public ManagedKeyData enableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    ManagedKeyIdentity custNamespacePrefix =
      new KeyIdentityPrefixBytesBacked(new Bytes(keyCust), new Bytes(Bytes.toBytes(keyNamespace)));
    String encodedCust = custNamespacePrefix.getCustodianEncoded();
    LOG.info("Trying to enable key management on custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Check if (cust, namespace) pair is already enabled and has an active key.
    ManagedKeyData markerKey = accessor.getKeyManagementStateMarker(custNamespacePrefix);
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
    return KeyManagementUtils.retrieveActiveKey(getKeyProvider(), accessor, encodedCust,
      custNamespacePrefix, null);
  }

  @Override
  public List<ManagedKeyData> getManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    ManagedKeyIdentity custNamespacePrefix =
      new KeyIdentityPrefixBytesBacked(new Bytes(keyCust), new Bytes(Bytes.toBytes(keyNamespace)));
    if (LOG.isInfoEnabled()) {
      LOG.info("Getting key statuses for custodian: {} under namespace: {}",
        custNamespacePrefix.getCustodianEncoded(), keyNamespace);
    }
    return accessor.getAllKeys(custNamespacePrefix, false);
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
  public ManagedKeyData disableKeyManagement(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    Bytes keyCustBytes = new Bytes(keyCust);
    Bytes keyNamespaceBytes = new Bytes(Bytes.toBytes(keyNamespace));
    ManagedKeyIdentity custNamespacePrefix =
      new KeyIdentityPrefixBytesBacked(keyCustBytes, keyNamespaceBytes);
    String encodedCust = LOG.isInfoEnabled() ? custNamespacePrefix.getCustodianEncoded() : null;
    LOG.info("disableKeyManagement started for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // First add the DISABLED state marker so no new ACTIVE key is retrieved for this scope.
    accessor.addKeyManagementStateMarker(custNamespacePrefix, ManagedKeyState.DISABLED);

    // Enumerate all keys for this (cust, namespace) and mark each as INACTIVE.
    // but without deleting the cust+namespace row so that the above update doesn't get cleared.
    List<ManagedKeyData> allKeys = accessor.getAllKeys(custNamespacePrefix, false);
    for (ManagedKeyData keyData : allKeys) {
      if (
        keyData.getKeyMetadata() == null
          || keyData.getKeyState().getExternalState() == ManagedKeyState.DISABLED
      ) {
        continue;
      }
      LOG
        .debug(
          "disableKeyManagement: marking key as INACTIVE with metadata hash: {} for custodian: {} "
            + "under namespace: {}",
          keyData.getPartialIdentityEncoded(), encodedCust, keyNamespace);
      accessor.updateActiveState(keyData, ManagedKeyState.INACTIVE, true);
      if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
        ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, keyData.getKeyMetadata());
      }
    }

    ManagedKeyData marker = accessor.getKeyManagementStateMarker(custNamespacePrefix);
    // If read-back is null, return a synthetic DISABLED marker so callers never see null.
    // Diagnostic logging above (KeymetaTableAccessor.getKey / parseFromResult) will indicate why.
    if (marker == null) {
      marker = new ManagedKeyData(custNamespacePrefix, ManagedKeyState.DISABLED);
    }
    LOG.info("disableKeyManagement completed for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);
    return marker;

  }

  @Override
  public ManagedKeyData disableManagedKey(byte[] keyCust, String keyNamespace, String keyMetadata)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    if (keyMetadata == null || keyMetadata.isEmpty()) {
      throw new IOException("key_metadata must not be empty");
    }
    ManagedKeyIdentity fullKeyIdentity = ManagedKeyIdentityUtils.fullKeyIdentityFromMetadata(
      new Bytes(keyCust), new Bytes(Bytes.toBytes(keyNamespace)), keyMetadata);
    String encodedCust = LOG.isInfoEnabled() ? fullKeyIdentity.getCustodianEncoded() : null;
    String encodedHash = LOG.isInfoEnabled() ? fullKeyIdentity.getPartialIdentityEncoded() : null;
    LOG.info("Disabling managed key with metadata hash: {} for custodian: {} under namespace: {}",
      encodedHash, encodedCust, keyNamespace);

    // First retrieve the key to verify it exists and get the full metadata for cache ejection
    ManagedKeyData existingKey = accessor.getKey(fullKeyIdentity);
    if (existingKey == null) {
      throw new IOException("Key not found for (custodian: " + encodedCust + ", namespace: "
        + keyNamespace + ") with metadata hash: " + encodedHash);
    }
    if (existingKey.getKeyState().getExternalState() == ManagedKeyState.DISABLED) {
      throw new IOException("Key is already disabled for (custodian: " + encodedCust
        + ", namespace: " + keyNamespace + ") with metadata hash: " + encodedHash);
    }

    accessor.disableKey(existingKey);
    ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, keyMetadata);

    LOG.info("Successfully disabled managed key with metadata hash: {} for custodian: {} under "
      + "namespace: {}", encodedHash, encodedCust, keyNamespace);
    // Retrieve and return the disabled key
    ManagedKeyData disabledKey = accessor.getKey(fullKeyIdentity);
    return disabledKey;
  }

  @Override
  public ManagedKeyData rotateManagedKey(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    ManagedKeyIdentity custNamespacePrefix =
      new KeyIdentityPrefixBytesBacked(new Bytes(keyCust), new Bytes(Bytes.toBytes(keyNamespace)));
    String encodedCust = custNamespacePrefix.getCustodianEncoded();
    LOG.info("Rotating managed key for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    // Attempt rotation
    return KeyManagementUtils.rotateActiveKey(getKeyProvider(), accessor, encodedCust,
      custNamespacePrefix);
  }

  @Override
  public ManagedKeyData setManagedKey(byte[] keyCust, String keyNamespace, String keyMetadata)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    ManagedKeyIdentity fullKeyIdentity = ManagedKeyIdentityUtils.fullKeyIdentityFromMetadata(
      new Bytes(keyCust), new Bytes(Bytes.toBytes(keyNamespace)), keyMetadata);
    ManagedKeyIdentity custNamespacePrefix = fullKeyIdentity.getKeyIdentityPrefix();
    if (LOG.isInfoEnabled()) {
      LOG.info("setManagedKey for custodian: {} namespace: {} metadata hash (encoded): {}",
        fullKeyIdentity.getCustodianEncoded(), keyNamespace,
        fullKeyIdentity.getPartialIdentityEncoded());
    }
    ManagedKeyData currentActiveKey = accessor.getKeyManagementStateMarker(custNamespacePrefix);
    ManagedKeyData keyData = KeyManagementUtils.retrieveKey(getKeyProvider(), accessor,
      custNamespacePrefix, keyMetadata, null);
    if (keyData.getKeyState() == ManagedKeyState.ACTIVE && currentActiveKey != null) {
      ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, currentActiveKey.getKeyMetadata());
    }
    LOG.info("setManagedKey completed for custodian: {} namespace: {} ",
      fullKeyIdentity.getCustodianEncoded(), keyNamespace);
    return keyData;
  }

  @Override
  public void refreshManagedKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    ManagedKeyIdentity custNamespacePrefix =
      new KeyIdentityPrefixBytesBacked(new Bytes(keyCust), new Bytes(Bytes.toBytes(keyNamespace)));
    String encodedCust = LOG.isInfoEnabled() ? custNamespacePrefix.getCustodianEncoded() : null;
    LOG.info("refreshManagedKeys started for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);

    ManagedKeyData markerKey = accessor.getKeyManagementStateMarker(custNamespacePrefix);
    if (markerKey != null && markerKey.getKeyState() == ManagedKeyState.DISABLED) {
      LOG.info(
        "refreshManagedKeys skipping since key management is disabled for custodian: {} under "
          + "namespace: {}",
        encodedCust, keyNamespace);
      return;
    }
    // First, get all keys for the specified custodian and namespace and refresh those that have a
    // non-null metadata.
    List<ManagedKeyData> allKeys = accessor.getAllKeys(custNamespacePrefix, false);
    IOException refreshException = null;
    for (ManagedKeyData keyData : allKeys) {
      if (keyData.getKeyMetadata() == null) {
        continue;
      }
      LOG.debug(
        "refreshManagedKeys: Refreshing key with metadata hash: {} for custodian: {} under "
          + "namespace: {} with state: {}",
        keyData.getPartialIdentityEncoded(), encodedCust, keyNamespace, keyData.getKeyState());
      try {
        ManagedKeyData refreshedKey =
          KeyManagementUtils.refreshKey(getKeyProvider(), accessor, keyData);
        if (refreshedKey == keyData) {
          LOG.debug(
            "refreshManagedKeys: Key with metadata hash: {} for custodian: {} under "
              + "namespace: {} is unchanged",
            keyData.getPartialIdentityEncoded(), encodedCust, keyNamespace);
        } else {
          if (refreshedKey.getKeyState().getExternalState() == ManagedKeyState.DISABLED) {
            LOG.info("refreshManagedKeys: Refreshed key is DISABLED, ejecting from cache");
            ejectManagedKeyDataCacheEntry(keyCust, keyNamespace, refreshedKey.getKeyMetadata());
          } else {
            LOG.info(
              "refreshManagedKeys: Successfully refreshed key with metadata hash: {} for "
                + "custodian: {} under namespace: {}",
              refreshedKey.getPartialIdentityEncoded(), encodedCust, keyNamespace);
          }
        }
      } catch (IOException | KeyException e) {
        LOG.error(
          "refreshManagedKeys: Failed to refresh key with metadata hash: {} for custodian: {} "
            + "under namespace: {}",
          keyData.getPartialIdentityEncoded(), encodedCust, keyNamespace, e);
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

    if (markerKey != null && markerKey.getKeyState() == ManagedKeyState.FAILED) {
      LOG.info(
        "refreshManagedKeys: Found FAILED marker for custodian: {} under namespace: {}"
          + ") indicating previous attempt to enable, reattempting to enable key management",
        encodedCust, keyNamespace);
      enableKeyManagement(keyCust, keyNamespace);
    }

    LOG.info("refreshManagedKeys: Completed for custodian: {} under namespace: {}", encodedCust,
      keyNamespace);
  }

  protected AsyncAdmin getAsyncAdmin(MasterServices master) {
    return master.getAsyncClusterConnection().getAdmin();
  }
}
