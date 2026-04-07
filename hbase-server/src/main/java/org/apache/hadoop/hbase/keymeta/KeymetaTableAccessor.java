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
import java.security.Key;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Accessor for keymeta table as part of key management.
 */
@InterfaceAudience.Private
public class KeymetaTableAccessor extends KeyManagementBase {
  private static final String KEY_META_INFO_FAMILY_STR = "info";

  public static final byte[] KEY_META_INFO_FAMILY = Bytes.toBytes(KEY_META_INFO_FAMILY_STR);

  public static final TableName KEY_META_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "keymeta");

  public static final String DEK_METADATA_QUAL_NAME = "m";
  public static final byte[] DEK_METADATA_QUAL_BYTES = Bytes.toBytes(DEK_METADATA_QUAL_NAME);

  public static final String DEK_CHECKSUM_QUAL_NAME = "c";
  public static final byte[] DEK_CHECKSUM_QUAL_BYTES = Bytes.toBytes(DEK_CHECKSUM_QUAL_NAME);

  public static final String DEK_WRAPPED_BY_STK_QUAL_NAME = "w";
  public static final byte[] DEK_WRAPPED_BY_STK_QUAL_BYTES =
    Bytes.toBytes(DEK_WRAPPED_BY_STK_QUAL_NAME);

  public static final String STK_CHECKSUM_QUAL_NAME = "s";
  public static final byte[] STK_CHECKSUM_QUAL_BYTES = Bytes.toBytes(STK_CHECKSUM_QUAL_NAME);

  public static final String REFRESHED_TIMESTAMP_QUAL_NAME = "t";
  public static final byte[] REFRESHED_TIMESTAMP_QUAL_BYTES =
    Bytes.toBytes(REFRESHED_TIMESTAMP_QUAL_NAME);

  public static final String KEY_STATE_QUAL_NAME = "k";
  public static final byte[] KEY_STATE_QUAL_BYTES = Bytes.toBytes(KEY_STATE_QUAL_NAME);

  public static final TableDescriptorBuilder TABLE_DESCRIPTOR_BUILDER =
    TableDescriptorBuilder.newBuilder(KEY_META_TABLE_NAME).setRegionReplication(1)
      .setPriority(HConstants.SYSTEMTABLE_QOS)
      .setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(KeymetaTableAccessor.KEY_META_INFO_FAMILY)
        .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setMaxVersions(1).setInMemory(true).build());

  private Server server;

  public KeymetaTableAccessor(Server server) {
    super(server.getKeyManagementService());
    this.server = server;
  }

  public Server getServer() {
    return server;
  }

  /**
   * Add the specified key to the keymeta table.
   * @param keyData The key data.
   * @throws IOException              when there is an underlying IOException.
   * @throws IllegalArgumentException if key custodian or key namespace exceeds max length (255).
   */
  public void addKey(ManagedKeyData keyData) throws IOException {
    assertKeyManagementEnabled();
    List<Put> puts = new ArrayList<>(2);
    if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      puts.add(addMutationColumns(
        new Put(keyData.getKeyIdentity().getIdentityPrefixView().copyBytesIfNecessary()), keyData));
    }
    final Put putForMetadata = addMutationColumns(
      new Put(keyData.getKeyIdentity().getFullIdentityView().copyBytesIfNecessary()), keyData);
    puts.add(putForMetadata);
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.put(puts);
    }
  }

  /**
   * Get all the keys for the specified keyCust and key_namespace.
   * @param fullKeyIdentity The full key identity.
   * @param includeMarkers  Whether to include key management state markers in the result.
   * @return a list of key data, one for each key, can be empty when none were found.
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public List<ManagedKeyData> getAllKeys(ManagedKeyIdentity fullKeyIdentity, boolean includeMarkers)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    byte[] prefixForScan = fullKeyIdentity.getIdentityPrefixView().copyBytesIfNecessary();
    PrefixFilter prefixFilter = new PrefixFilter(prefixForScan);
    Scan scan = new Scan();
    scan.setFilter(prefixFilter);
    scan.addFamily(KEY_META_INFO_FAMILY);

    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      ResultScanner scanner = table.getScanner(scan);
      Set<ManagedKeyData> allKeys = new LinkedHashSet<>();
      for (Result result : scanner) {
        ManagedKeyData keyData =
          parseFromResult(getKeyManagementService(), fullKeyIdentity, result);
        if (keyData != null && (includeMarkers || keyData.getKeyMetadata() != null)) {
          allKeys.add(keyData);
        }
      }
      return allKeys.stream().collect(Collectors.toList());
    }
  }

  /**
   * Get the key management state marker for the specified keyCust and key_namespace.
   * @param keyCust      The prefix
   * @param keyNamespace The namespace
   * @return the key management state marker data, or null if no key management state marker found
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getKeyManagementStateMarker(ManagedKeyIdentity keyIdentity)
    throws IOException, KeyException {
    Preconditions.checkNotNull(keyIdentity.getPartialIdentityLength() == 0,
      "fullKeyIdentity must have no partial identity");
    return getKey(keyIdentity);
  }

  /**
   * Get the specific key identified by keyCust, keyNamespace and partialIdentity.
   * @param keyIdentity The full key identity.
   * @return the key or {@code null}
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getKey(ManagedKeyIdentity keyIdentity) throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      byte[] rowKey = keyIdentity.getPartialIdentityLength() > 0
        ? keyIdentity.getFullIdentityView().copyBytesIfNecessary()
        : keyIdentity.getIdentityPrefixView().copyBytesIfNecessary();
      Result result = table.get(new Get(rowKey));
      return parseFromResult(getKeyManagementService(), keyIdentity, result);
    }
  }

  /**
   * Disables a key by removing the wrapped key and updating its state to DISABLED.
   * @param keyData The key data to disable.
   * @throws IOException when there is an underlying IOException.
   */
  public void disableKey(ManagedKeyData keyData) throws IOException {
    disableKey(keyData, true);
  }

  /**
   * Disables a key by removing the wrapped key and updating its state to DISABLED. When
   * {@code deleteCustNamespaceRow} is true and the key is ACTIVE, the (custodian, namespace) row is
   * deleted so no key is marked ACTIVE for that scope. When false, that row is left unchanged (e.g.
   * when disabling all keys as part of disableKeyManagement, the DISABLED marker has already been
   * written to that row).
   * @param keyData                The key data to disable.
   * @param deleteCustNamespaceRow If true, delete the (custodian, namespace) row when key is
   *                               ACTIVE; if false, skip that delete.
   * @throws IOException when there is an underlying IOException.
   */
  public void disableKey(ManagedKeyData keyData, boolean deleteCustNamespaceRow)
    throws IOException {
    assertKeyManagementEnabled();
    Preconditions.checkNotNull(keyData.getKeyMetadata(), "Key metadata cannot be null");

    List<Mutation> mutations = new ArrayList<>(3); // Max possible mutations.

    if (deleteCustNamespaceRow && keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      // Delete the CustNamespace row
      byte[] rowKeyForCustNamespace =
        keyData.getKeyIdentity().getIdentityPrefixView().copyBytesIfNecessary();
      mutations.add(new Delete(rowKeyForCustNamespace).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.SYSTEMTABLE_QOS));
    }

    // Update state to DISABLED and timestamp on Identity row
    byte[] rowKeyForIdentity =
      keyData.getKeyIdentity().getFullIdentityView().copyBytesIfNecessary();
    addMutationsForKeyDisabled(mutations, rowKeyForIdentity, keyData.getKeyMetadata(),
      keyData.getKeyState() == ManagedKeyState.ACTIVE
        ? ManagedKeyState.ACTIVE_DISABLED
        : ManagedKeyState.INACTIVE_DISABLED,
      keyData.getKeyState());

    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.batch(mutations, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while disabling key", e);
    }
  }

  // Also used for adding key management state marker.
  private void addMutationsForKeyDisabled(List<Mutation> mutations, byte[] rowKey, String metadata,
    ManagedKeyState targetState, ManagedKeyState currentState) {
    Put put = new Put(rowKey);
    if (metadata != null) {
      put.addColumn(KEY_META_INFO_FAMILY, DEK_METADATA_QUAL_BYTES, metadata.getBytes());
    }
    Put putForState = addMutationColumnsForState(put, targetState);
    mutations.add(putForState);

    // Delete wrapped key columns from Metadata row
    if (currentState == null || ManagedKeyState.isUsable(currentState)) {
      Delete deleteWrappedKey = new Delete(rowKey).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.SYSTEMTABLE_QOS)
        .addColumns(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES)
        .addColumns(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES)
        .addColumns(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES);
      if (metadata == null) {
        deleteWrappedKey.addColumns(KEY_META_INFO_FAMILY, DEK_METADATA_QUAL_BYTES);
      }
      mutations.add(deleteWrappedKey);
    }
  }

  /**
   * Adds a key management state marker to the specified (keyCust, keyNamespace) combination. It
   * also adds delete markers for the columns unrelates to marker, in case the state is
   * transitioning from ACTIVE to DISABLED or FAILED. This method is only used for setting the state
   * to DISABLED or FAILED. For ACTIVE state, the addKey() method implicitly adds the marker.
   * @param keyCust      The key custodian.
   * @param keyNamespace The namespace.
   * @param state        The key management state to add.
   * @throws IOException when there is an underlying IOException.
   */
  public void addKeyManagementStateMarker(ManagedKeyIdentity keyIdentity, ManagedKeyState state)
    throws IOException {
    assertKeyManagementEnabled();
    Preconditions.checkArgument(ManagedKeyState.isKeyManagementState(state),
      "State must be a key management state, got: " + state);
    List<Mutation> mutations = new ArrayList<>(2);
    byte[] rowKey = keyIdentity.getIdentityPrefixView().copyBytesIfNecessary();
    addMutationsForKeyDisabled(mutations, rowKey, null, state, null);
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.batch(mutations, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while adding key management state marker", e);
    }
  }

  /**
   * Updates the state of a key to one of the ACTIVE or INACTIVE states. The current state can be
   * any state, but if it the same, it becomes a no-op.
   * <p>
   * When transitioning from ACTIVE to INACTIVE, this method normally deletes the (custodian,
   * namespace) row so that no key is marked ACTIVE for that scope. During key rotation and
   * disablement, the caller has already written the marker key before demoting the old key; in that
   * case pass {@code skipActiveRowDelete == true} to skip the delete and avoid removing the new
   * active key's row.
   * @param keyData             The key data.
   * @param newState            The new state (must be ACTIVE or INACTIVE).
   * @param skipActiveRowDelete If true, do not delete the (custodian, namespace) row when
   *                            transitioning from ACTIVE to INACTIVE. Use only when the caller has
   *                            already updated that row with a new ACTIVE key (e.g. during
   *                            rotateActiveKey) to avoid removing the new active key's row.
   * @throws IOException when there is an underlying IOException.
   */
  public void updateActiveState(ManagedKeyData keyData, ManagedKeyState newState,
    boolean skipActiveRowDelete) throws IOException {
    assertKeyManagementEnabled();
    ManagedKeyState currentState = keyData.getKeyState();

    // Validate states
    Preconditions.checkArgument(ManagedKeyState.isUsable(newState),
      "New state must be ACTIVE or INACTIVE, got: " + newState);
    // Even for FAILED keys, we expect the metadata to be non-null.
    Preconditions.checkNotNull(keyData.getKeyMetadata(), "Key metadata cannot be null");

    // No-op if states are the same
    if (currentState == newState) {
      return;
    }

    List<Row> mutations = new ArrayList<>(2);
    Bytes rowKeyForCustNamespace = keyData.getKeyIdentity().getIdentityPrefixView();

    // First take care of the active key specific row.
    if (newState == ManagedKeyState.ACTIVE) {
      // INACTIVE -> ACTIVE: Add CustNamespace row and update Metadata row
      mutations
        .add(addMutationColumns(new Put(rowKeyForCustNamespace.copyBytesIfNecessary()), keyData));
    }
    if (currentState == ManagedKeyState.ACTIVE && !skipActiveRowDelete) {
      // ACTIVE -> INACTIVE: Delete CustNamespace row (unless it was already overwritten by
      // a new ACTIVE key, e.g. during rotation).
      mutations.add(new Delete(rowKeyForCustNamespace.copyBytesIfNecessary())
        .setDurability(Durability.SKIP_WAL).setPriority(HConstants.SYSTEMTABLE_QOS));
    }

    Bytes rowKeyForIdentity = keyData.getKeyIdentity().getFullIdentityView();
    // Now take care of the key specific row (for point gets by identity).
    if (!ManagedKeyState.isUsable(currentState)) {
      // For DISABLED and FAILED keys, we don't expect cached key material, so add all columns
      // similar to what addKey() does.
      mutations.add(addMutationColumns(new Put(rowKeyForIdentity.copyBytesIfNecessary()), keyData));
    } else {
      // We expect cached key material, so only update the state and timestamp columns.
      mutations.add(
        addMutationColumnsForState(new Put(rowKeyForIdentity.copyBytesIfNecessary()), newState));
    }

    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.batch(mutations, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while updating active state", e);
    }
  }

  private Put addMutationColumnsForState(Put put, ManagedKeyState newState) {
    return addMutationColumnsForState(put, newState, EnvironmentEdgeManager.currentTime());
  }

  /**
   * Add only state and timestamp columns to the given Put.
   */
  private Put addMutationColumnsForState(Put put, ManagedKeyState newState, long timestamp) {
    return put.setDurability(Durability.SKIP_WAL).setPriority(HConstants.SYSTEMTABLE_QOS)
      .addColumn(KEY_META_INFO_FAMILY, KEY_STATE_QUAL_BYTES, new byte[] { newState.getVal() })
      .addColumn(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES, Bytes.toBytes(timestamp));
  }

  /**
   * Updates only the refresh timestamp column for the given key to the current time. Used when a
   * key is refreshed but unchanged, so that the stored timestamp reflects the last refresh time.
   * @param keyData The key data whose refresh timestamp should be updated.
   * @throws IOException when there is an underlying IOException.
   */
  public void updateRefreshTimestamp(ManagedKeyData keyData) throws IOException {
    assertKeyManagementEnabled();
    Preconditions.checkNotNull(keyData.getKeyMetadata(), "Key metadata cannot be null");
    long now = EnvironmentEdgeManager.currentTime();
    List<Mutation> mutations = new ArrayList<>(2);
    byte[] rowKeyForIdentity =
      keyData.getKeyIdentity().getFullIdentityView().copyBytesIfNecessary();
    Put putMetadata = new Put(rowKeyForIdentity).setDurability(Durability.SKIP_WAL)
      .setPriority(HConstants.SYSTEMTABLE_QOS)
      .addColumn(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES, Bytes.toBytes(now));
    mutations.add(putMetadata);
    if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      byte[] rowKeyForCustNamespace =
        keyData.getKeyIdentity().getIdentityPrefixView().copyBytesIfNecessary();
      Put putCustNamespace = new Put(rowKeyForCustNamespace).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.SYSTEMTABLE_QOS)
        .addColumn(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES, Bytes.toBytes(now));
      mutations.add(putCustNamespace);
    }
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.batch(mutations, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while updating refresh timestamp", e);
    }
  }

  /**
   * Add the mutation columns to the given Put that are derived from the keyData.
   */
  private Put addMutationColumns(Put put, ManagedKeyData keyData) throws IOException {
    ManagedKeyData latestSystemKey =
      getKeyManagementService().getSystemKeyCache().getLatestSystemKey();
    if (keyData.getTheKey() != null) {
      byte[] dekWrappedBySTK = EncryptionUtil.wrapKey(getConfiguration(), null, keyData.getTheKey(),
        latestSystemKey.getTheKey());
      put
        .addColumn(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES,
          Bytes.toBytes(keyData.getKeyChecksum()))
        .addColumn(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES, dekWrappedBySTK)
        .addColumn(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES,
          latestSystemKey.getKeyIdentity().getFullIdentityView().copyBytesIfNecessary());
    }
    Put result =
      addMutationColumnsForState(put, keyData.getKeyState(), keyData.getRefreshTimestamp());

    // Only add metadata column if metadata is not null
    String metadata = keyData.getKeyMetadata();
    if (metadata != null) {
      result.addColumn(KEY_META_INFO_FAMILY, DEK_METADATA_QUAL_BYTES, metadata.getBytes());
    }

    return result;
  }

  @InterfaceAudience.Private
  public static ManagedKeyData parseFromResult(KeyManagementService keyManagementService,
    ManagedKeyIdentity originalFullKeyIdentity, Result result) throws IOException, KeyException {
    if (result == null || result.isEmpty()) {
      LOG.warn(
        "parseFromResult: returning null because result is null or empty "
          + "(keyCust={}, keyCust length={}, keyNamespace={})",
        originalFullKeyIdentity.getCustodianEncoded(), originalFullKeyIdentity.getCustodianLength(),
        originalFullKeyIdentity.getNamespaceString());
      return null;
    }
    ManagedKeyState keyState =
      ManagedKeyState.forValue(result.getValue(KEY_META_INFO_FAMILY, KEY_STATE_QUAL_BYTES)[0]);
    String dekMetadata =
      Bytes.toString(result.getValue(KEY_META_INFO_FAMILY, DEK_METADATA_QUAL_BYTES));
    byte[] dekWrappedByStk = result.getValue(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES);
    if (
      (keyState == ManagedKeyState.ACTIVE || keyState == ManagedKeyState.INACTIVE)
        && dekWrappedByStk == null
    ) {
      throw new IOException(keyState + " key must have a wrapped key");
    }
    Key dek = null;
    if (dekWrappedByStk != null) {
      byte[] stkIdentityBytes = result.getValue(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES);
      ManagedKeyData clusterKey =
        keyManagementService.getSystemKeyCache().getSystemKeyByIdentity(stkIdentityBytes);
      if (clusterKey == null) {
        LOG.error(
          "Dropping key with metadata: {} as STK with identity is unavailable"
            + "(keyCust={}, keyCust length={}, keyNamespace={})",
          dekMetadata, originalFullKeyIdentity.getCustodianEncoded(),
          originalFullKeyIdentity.getCustodianLength(),
          originalFullKeyIdentity.getNamespaceString());
        return null;
      }
      dek = EncryptionUtil.unwrapKey(keyManagementService.getConfiguration(), null, dekWrappedByStk,
        clusterKey.getTheKey());
    }
    long refreshedTimestamp =
      Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES));
    ManagedKeyData dekKeyData;
    if (dekMetadata != null) {
      // We do nnot have a partial identity in the originalFullKeyIdentity in the following cases
      // and we need to create a full identity from the metadata:
      // - When it is a point get of an ACTIVE marker
      // - When it is a scan for all keys and the key state is usable.
      ManagedKeyIdentity fullKeyIdentity = originalFullKeyIdentity.getPartialIdentityLength() > 0
        ? originalFullKeyIdentity
        : ManagedKeyIdentityUtils.fullKeyIdentityFromMetadata(
          originalFullKeyIdentity.getCustodianView(), originalFullKeyIdentity.getNamespaceView(),
          dekMetadata);
      dekKeyData =
        new ManagedKeyData(fullKeyIdentity, dek, keyState, dekMetadata, refreshedTimestamp);
      if (dek != null) {
        long dekChecksum =
          Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES));
        if (dekKeyData.getKeyChecksum() != dekChecksum) {
          LOG.error(
            "Dropping key, current key checksum: {} didn't match the expected checksum: {}"
              + " for key with metadata: {} (keyCust={}, keyCust length={}, keyNamespace={})",
            dekKeyData.getKeyChecksum(), dekChecksum, dekMetadata,
            fullKeyIdentity.getCustodianEncoded(), fullKeyIdentity.getCustodianLength(),
            fullKeyIdentity.getNamespaceString());
          dekKeyData = null;
        }
      }
    } else {
      if (originalFullKeyIdentity.getPartialIdentityLength() != 0) {
        throw new IllegalArgumentException(
          "Partial identity length must be 0 for key management marker, got: "
            + originalFullKeyIdentity.getPartialIdentityLength() + " (keyCust="
            + originalFullKeyIdentity.getCustodianEncoded() + ", keyCust length="
            + originalFullKeyIdentity.getCustodianLength() + ", keyNamespace="
            + originalFullKeyIdentity.getNamespaceString() + ")");
      }
      dekKeyData = new ManagedKeyData(originalFullKeyIdentity, keyState, refreshedTimestamp);
    }
    return dekKeyData;
  }
}
