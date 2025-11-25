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
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
public class KeyManagementUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KeyManagementUtils.class);

  /**
   * Utility function to retrieves a managed key from the key provider. If an existing key is
   * provided and the retrieved key is the same as the existing key, it will be ignored.
   * @param provider          the managed key provider
   * @param accessor          the accessor to use to persist the key. If null, the key will not be
   *                          persisted.
   * @param encKeyCust        the encoded key custodian
   * @param key_cust          the key custodian
   * @param keyNamespace      the key namespace
   * @param existingActiveKey the existing key, typically the active key already retrieved from the
   *                          key provider, can be null.
   * @return the retrieved key, or null if no key could be retrieved
   * @throws IOException  if an error occurs
   * @throws KeyException if an error occurs
   */
  public static ManagedKeyData retrieveActiveKey(ManagedKeyProvider provider,
    KeymetaTableAccessor accessor, String encKeyCust, byte[] key_cust, String keyNamespace,
    ManagedKeyData existingActiveKey) throws IOException, KeyException {
    Preconditions.checkArgument(
      existingActiveKey == null || existingActiveKey.getKeyState() == ManagedKeyState.ACTIVE,
      "Expected existing active key to be null or having ACTIVE state"
        + (existingActiveKey == null ? "" : ", but got: " + existingActiveKey.getKeyState()));
    ManagedKeyData keyData;
    try {
      keyData = provider.getManagedKey(key_cust, keyNamespace);
    } catch (IOException e) {
      keyData = new ManagedKeyData(key_cust, keyNamespace, ManagedKeyState.FAILED);
    }
    if (keyData == null) {
      throw new IOException("Invalid null managed key received from key provider");
    }
    if (keyData.getKeyMetadata() != null && keyData.getKeyState() == ManagedKeyState.INACTIVE) {
      throw new IOException(
        "Expected key to be ACTIVE, but got an INACTIVE key with metadata hash: "
          + keyData.getKeyMetadataHashEncoded() + " for (custodian: " + encKeyCust + ", namespace: "
          + keyNamespace + ")");
    }

    if (existingActiveKey != null && existingActiveKey.equals(keyData)) {
      LOG.info("retrieveActiveKey: no change in active key for (custodian: {}, namespace: {}",
        encKeyCust, keyNamespace);
      return existingActiveKey;
    }

    LOG.info(
      "retrieveActiveKey: got key with state: {} and metadata: {} for custodian: {} namespace: {}",
      keyData.getKeyState(), keyData.getKeyMetadataHashEncoded(), encKeyCust,
      keyData.getKeyNamespace());
    if (accessor != null) {
      if (keyData.getKeyMetadata() != null) {
        accessor.addKey(keyData);
      } else {
        accessor.addKeyManagementStateMarker(keyData.getKeyCustodian(), keyData.getKeyNamespace(),
          keyData.getKeyState());
      }
    }
    return keyData;
  }

  /**
   * Retrieves a key from the key provider for the specified metadata.
   * @param provider     the managed key provider
   * @param accessor     the accessor to use to persist the key. If null, the key will not be
   *                     persisted.
   * @param encKeyCust   the encoded key custodian
   * @param keyCust      the key custodian
   * @param keyNamespace the key namespace
   * @param keyMetadata  the key metadata
   * @param wrappedKey   the wrapped key, if available, can be null.
   * @return the retrieved key that is guaranteed to be not null and have non-null metadata.
   * @throws IOException  if an error occurs while retrieving or persisting the key
   * @throws KeyException if an error occurs while retrieving or validating the key
   */
  public static ManagedKeyData retrieveKey(ManagedKeyProvider provider,
    KeymetaTableAccessor accessor, String encKeyCust, byte[] keyCust, String keyNamespace,
    String keyMetadata, byte[] wrappedKey) throws IOException, KeyException {
    ManagedKeyData keyData = provider.unwrapKey(keyMetadata, wrappedKey);
    // Do some validation of the resposne, as we can't trust that all providers honour the contract.
    // If the key is disabled, we expect a more specific key state to be used, not the generic
    // DISABLED state.
    if (
      keyData == null || keyData.getKeyMetadata() == null
        || !keyData.getKeyMetadata().equals(keyMetadata)
        || keyData.getKeyState() == ManagedKeyState.DISABLED
    ) {
      throw new KeyException(
        "Invalid key that is null or having invalid metadata or state received from key provider "
          + "for (custodian: " + encKeyCust + ", namespace: " + keyNamespace
          + ") and metadata hash: "
          + ManagedKeyProvider.encodeToStr(ManagedKeyData.constructMetadataHash(keyMetadata)));
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(
        "retrieveKey: got key with state: {} and metadata: {} for (custodian: {}, "
          + "namespace: {}) and metadata hash: {}",
        keyData.getKeyState(), keyData.getKeyMetadata(), encKeyCust, keyNamespace,
        ManagedKeyProvider.encodeToStr(ManagedKeyData.constructMetadataHash(keyMetadata)));
    }
    if (accessor != null) {
      try {
        accessor.addKey(keyData);
      } catch (IOException e) {
        LOG.warn(
          "retrieveKey: Failed to add key to L2 for metadata hash: {}, for custodian: {}, "
            + "namespace: {}",
          ManagedKeyProvider.encodeToStr(ManagedKeyData.constructMetadataHash(keyMetadata)),
          encKeyCust, keyNamespace, e);
      }
    }
    return keyData;
  }

  /**
   * Refreshes the specified key from the configured managed key provider to confirm it is still
   * valid.
   * @param provider the managed key provider
   * @param accessor the accessor to use to persist changes
   * @param keyData  the key data to refresh
   * @return the refreshed key data, or the original if unchanged
   * @throws IOException  if an error occurs
   * @throws KeyException if an error occurs
   */
  public static ManagedKeyData refreshKey(ManagedKeyProvider provider,
    KeymetaTableAccessor accessor, ManagedKeyData keyData) throws IOException, KeyException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "refreshKey: entry with keyData state: {}, metadata hash: {} for (custodian: {}, "
          + "namespace: {})",
        keyData.getKeyState(), keyData.getKeyMetadataHashEncoded(),
        ManagedKeyProvider.encodeToStr(keyData.getKeyCustodian()), keyData.getKeyNamespace());
    }

    Preconditions.checkArgument(keyData.getKeyMetadata() != null,
      "Key metadata should be non-null for key to be refreshed");

    ManagedKeyData result;
    // NOTE: Even FAILED keys can have metadata that is good enough for refreshing from provider.
    // Refresh key using unwrapKey
    ManagedKeyData newKeyData;
    try {
      newKeyData = provider.unwrapKey(keyData.getKeyMetadata(), null);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "refreshKey: unwrapped key with state: {}, metadata hash: {} for (custodian: "
            + "{}, namespace: {})",
          newKeyData.getKeyState(), newKeyData.getKeyMetadataHashEncoded(),
          ManagedKeyProvider.encodeToStr(newKeyData.getKeyCustodian()),
          newKeyData.getKeyNamespace());
      }
    } catch (IOException e) {
      LOG.warn("refreshKey: Failed to unwrap key for (custodian: {}, namespace: {})",
        ManagedKeyProvider.encodeToStr(keyData.getKeyCustodian()), keyData.getKeyNamespace(), e);
      newKeyData = new ManagedKeyData(keyData.getKeyCustodian(), keyData.getKeyNamespace(), null,
        ManagedKeyState.FAILED, keyData.getKeyMetadata());
    }

    // Validate metadata hasn't changed
    if (!keyData.getKeyMetadata().equals(newKeyData.getKeyMetadata())) {
      throw new KeyException("Key metadata changed during refresh: current metadata hash: "
        + keyData.getKeyMetadataHashEncoded() + ", got metadata hash: "
        + newKeyData.getKeyMetadataHashEncoded() + " for (custodian: "
        + ManagedKeyProvider.encodeToStr(keyData.getKeyCustodian()) + ", namespace: "
        + keyData.getKeyNamespace() + ")");
    }

    // Check if state changed
    if (keyData.getKeyState() == newKeyData.getKeyState()) {
      // No change, return original
      result = keyData;
    } else if (newKeyData.getKeyState() == ManagedKeyState.FAILED) {
      // Ignore if new state is FAILED, let us just keep the existing key data as is as this is
      // most likely a transitional issue with KMS.
      result = keyData;
    } else {
      if (newKeyData.getKeyState().getExternalState() == ManagedKeyState.DISABLED) {
        // Handle DISABLED state change specially.
        accessor.disableKey(keyData);
      } else {
        // Rest of the state changes are only ACTIVE and INACTIVE..
        accessor.updateActiveState(keyData, newKeyData.getKeyState());
      }
      result = newKeyData;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "refreshKey: completed with result state: {}, metadata hash: {} for (custodian: "
          + "{}, namespace: {})",
        result.getKeyState(), result.getKeyMetadataHashEncoded(),
        ManagedKeyProvider.encodeToStr(result.getKeyCustodian()), result.getKeyNamespace());
    }

    return result;
  }

  /**
   * Rotates the ACTIVE key for the specified (custodian, namespace) combination.
   * @param provider     the managed key provider
   * @param accessor     the accessor to use to persist changes
   * @param keyCust      the key custodian
   * @param keyNamespace the key namespace
   * @return the new active key, or null if no rotation happened
   * @throws IOException  if an error occurs
   * @throws KeyException if an error occurs
   */
  public static ManagedKeyData rotateActiveKey(ManagedKeyProvider provider,
    KeymetaTableAccessor accessor, String encKeyCust, byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    // Get current active key
    ManagedKeyData currentActiveKey = accessor.getKeyManagementStateMarker(keyCust, keyNamespace);
    if (currentActiveKey == null || currentActiveKey.getKeyState() != ManagedKeyState.ACTIVE) {
      throw new IOException("No active key found, key management not yet enabled for (custodian: "
        + encKeyCust + ", namespace: " + keyNamespace + ") ?");
    }

    // Retrieve new key from provider, We pass null accessor to skip default persistence logic,
    // because a failure to rotate shouldn't make the current active key invalid.
    ManagedKeyData newKey = retrieveActiveKey(provider, null,
      ManagedKeyProvider.encodeToStr(keyCust), keyCust, keyNamespace, currentActiveKey);
    if (newKey == null || newKey.equals(currentActiveKey)) {
      LOG.warn(
        "rotateActiveKey: failed to retrieve new active key for (custodian: {}, namespace: {})",
        encKeyCust, keyNamespace);
      return null;
    }

    // If rotation succeeds in generating a new active key, persist the new key and mark the current
    // active key as inactive.
    if (newKey.getKeyState() == ManagedKeyState.ACTIVE) {
      try {
        accessor.addKey(newKey);
        accessor.updateActiveState(currentActiveKey, ManagedKeyState.INACTIVE);
        return newKey;
      } catch (IOException e) {
        LOG.warn("rotateActiveKey: failed to persist new active key to L2 for (custodian: {}, "
          + "namespace: {})", encKeyCust, keyNamespace, e);
        return null;
      }
    } else {
      LOG.warn(
        "rotateActiveKey: ignoring new key with state {} without metadata hash: {} for "
          + "(custodian: {}, namespace: {})",
        newKey.getKeyState(), newKey.getKeyMetadataHashEncoded(), encKeyCust, keyNamespace);
      return null;
    }
  }
}
