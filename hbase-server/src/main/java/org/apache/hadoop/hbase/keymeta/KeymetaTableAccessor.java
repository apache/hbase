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
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
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
   * @throws IOException when there is an underlying IOException.
   */
  public void addKey(ManagedKeyData keyData) throws IOException {
    assertKeyManagementEnabled();
    List<Put> puts = new ArrayList<>(2);
    if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      puts.add(addMutationColumns(new Put(constructRowKeyForCustNamespace(keyData)), keyData));
    }
    final Put putForMetadata =
      addMutationColumns(new Put(constructRowKeyForMetadata(keyData)), keyData);
    puts.add(putForMetadata);
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.put(puts);
    }
  }

  /**
   * Get all the keys for the specified keyCust and key_namespace.
   * @param keyCust      The key custodian.
   * @param keyNamespace The namespace
   * @return a list of key data, one for each key, can be empty when none were found.
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.UNITTEST)
  public List<ManagedKeyData> getAllKeys(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    byte[] prefixForScan = constructRowKeyForCustNamespace(keyCust, keyNamespace);
    PrefixFilter prefixFilter = new PrefixFilter(prefixForScan);
    Scan scan = new Scan();
    scan.setFilter(prefixFilter);
    scan.addFamily(KEY_META_INFO_FAMILY);

    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      ResultScanner scanner = table.getScanner(scan);
      Set<ManagedKeyData> allKeys = new LinkedHashSet<>();
      for (Result result : scanner) {
        ManagedKeyData keyData =
          parseFromResult(getKeyManagementService(), keyCust, keyNamespace, result);
        if (keyData != null) {
          allKeys.add(keyData);
        }
      }
      return allKeys.stream().toList();
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
  public ManagedKeyData getKeyManagementStateMarker(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    return getKey(keyCust, keyNamespace, null);
  }

  /**
   * Get the specific key identified by keyCust, keyNamespace and keyMetadataHash.
   * @param keyCust         The prefix.
   * @param keyNamespace    The namespace.
   * @param keyMetadataHash The metadata hash.
   * @return the key or {@code null}
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getKey(byte[] keyCust, String keyNamespace, byte[] keyMetadataHash)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      byte[] rowKey = keyMetadataHash != null
        ? constructRowKeyForMetadata(keyCust, keyNamespace, keyMetadataHash)
        : constructRowKeyForCustNamespace(keyCust, keyNamespace);
      Result result = table.get(new Get(rowKey));
      return parseFromResult(getKeyManagementService(), keyCust, keyNamespace, result);
    }
  }

  /**
   * Disables a key by removing the wrapped key and updating its state to DISABLED.
   * @param keyData The key data to disable.
   * @throws IOException when there is an underlying IOException.
   */
  public void disableKey(ManagedKeyData keyData) throws IOException {
    assertKeyManagementEnabled();
    byte[] keyCust = keyData.getKeyCustodian();
    String keyNamespace = keyData.getKeyNamespace();
    byte[] keyMetadataHash = keyData.getKeyMetadataHash();

    List<Row> mutations = new ArrayList<>(3); // Max possible mutations.

    if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
      // Delete the CustNamespace row
      byte[] rowKeyForCustNamespace = constructRowKeyForCustNamespace(keyCust, keyNamespace);
      mutations.add(new Delete(rowKeyForCustNamespace).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.SYSTEMTABLE_QOS));
    }

    // Update state to DISABLED and timestamp on Metadata row
    byte[] rowKeyForMetadata = constructRowKeyForMetadata(keyCust, keyNamespace, keyMetadataHash);
    addDeleteMutationsForKeyDisabled(mutations, rowKeyForMetadata,
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

  private void addDeleteMutationsForKeyDisabled(List<Row> mutations, byte[] rowKey,
    ManagedKeyState targetState, ManagedKeyState currentState) {
    Put putForState = addMutationColumnsForState(new Put(rowKey), targetState);
    mutations.add(putForState);

    // Delete wrapped key columns from Metadata row
    if (currentState == null || ManagedKeyState.isUsable(currentState)) {
      Delete deleteWrappedKey = new Delete(rowKey).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.SYSTEMTABLE_QOS)
        .addColumns(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES)
        .addColumns(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES)
        .addColumns(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES);
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
  public void addKeyManagementStateMarker(byte[] keyCust, String keyNamespace,
    ManagedKeyState state) throws IOException {
    assertKeyManagementEnabled();
    Preconditions.checkArgument(ManagedKeyState.isKeyManagementState(state),
      "State must be a key management state, got: " + state);
    List<Row> mutations = new ArrayList<>(2);
    byte[] rowKey = constructRowKeyForCustNamespace(keyCust, keyNamespace);
    addDeleteMutationsForKeyDisabled(mutations, rowKey, state, null);
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.batch(mutations, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while adding key management state marker", e);
    }
  }

  /**
   * Removes the key management state marker from the specified (keyCust, keyNamespace) combination.
   * @param keyCust      The key custodian.
   * @param keyNamespace The namespace.
   * @throws IOException when there is an underlying IOException.
   */
  public void removeKeyManagementStateMarker(byte[] keyCust, String keyNamespace)
    throws IOException {
    assertKeyManagementEnabled();
    Delete delete = new Delete(constructRowKeyForCustNamespace(keyCust, keyNamespace))
      .setDurability(Durability.SKIP_WAL).setPriority(HConstants.SYSTEMTABLE_QOS);
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.delete(delete);
    }
  }

  /**
   * Updates the state of a key to one of the ACTIVE or INACTIVE states. The current state can be
   * any state, but if it the same, it becomes a no-op.
   * @param keyData  The key data.
   * @param newState The new state (must be ACTIVE or INACTIVE).
   * @throws IOException when there is an underlying IOException.
   */
  public void updateActiveState(ManagedKeyData keyData, ManagedKeyState newState)
    throws IOException {
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
    byte[] rowKeyForCustNamespace = constructRowKeyForCustNamespace(keyData);
    byte[] rowKeyForMetadata = constructRowKeyForMetadata(keyData);

    // First take care of the active key specific row.
    if (newState == ManagedKeyState.ACTIVE) {
      // INACTIVE -> ACTIVE: Add CustNamespace row and update Metadata row
      mutations.add(addMutationColumns(new Put(rowKeyForCustNamespace), keyData));
    } else if (currentState == ManagedKeyState.ACTIVE) {
      mutations.add(new Delete(rowKeyForCustNamespace).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.SYSTEMTABLE_QOS));
    }

    // Now take care of the key specific row (for point gets by metadata).
    if (!ManagedKeyState.isUsable(currentState)) {
      // For DISABLED and FAILED keys, we don't expect cached key material, so add all columns
      // similar to what addKey() does.
      mutations.add(addMutationColumns(new Put(rowKeyForMetadata), keyData));
    } else {
      // We expect cached key material, so only update the state and timestamp columns.
      mutations.add(addMutationColumnsForState(new Put(rowKeyForMetadata), newState));
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
          Bytes.toBytes(latestSystemKey.getKeyChecksum()));
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
  public static byte[] constructRowKeyForMetadata(ManagedKeyData keyData) {
    byte[] keyMetadataHash;
    // NOTE: only some FAILED keys may have null metadata.
    if (keyData.getKeyMetadata() == null) {
      // For FAILED state with null metadata, use state as metadata
      keyMetadataHash = new byte[] { keyData.getKeyState().getVal() };
    } else {
      keyMetadataHash = keyData.getKeyMetadataHash();
    }
    return constructRowKeyForMetadata(keyData.getKeyCustodian(), keyData.getKeyNamespace(),
      keyMetadataHash);
  }

  @InterfaceAudience.Private
  public static byte[] constructRowKeyForMetadata(byte[] keyCust, String keyNamespace,
    byte[] keyMetadataHash) {
    return Bytes.add(constructRowKeyForCustNamespace(keyCust, keyNamespace), keyMetadataHash);
  }

  @InterfaceAudience.Private
  public static byte[] constructRowKeyForCustNamespace(ManagedKeyData keyData) {
    return constructRowKeyForCustNamespace(keyData.getKeyCustodian(), keyData.getKeyNamespace());
  }

  @InterfaceAudience.Private
  public static byte[] constructRowKeyForCustNamespace(byte[] keyCust, String keyNamespace) {
    int custLength = keyCust.length;
    return Bytes.add(Bytes.toBytes(custLength), keyCust, Bytes.toBytes(keyNamespace));
  }

  @InterfaceAudience.Private
  public static ManagedKeyData parseFromResult(KeyManagementService keyManagementService,
    byte[] keyCust, String keyNamespace, Result result) throws IOException, KeyException {
    if (result == null || result.isEmpty()) {
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
      long stkChecksum =
        Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES));
      ManagedKeyData clusterKey =
        keyManagementService.getSystemKeyCache().getSystemKeyByChecksum(stkChecksum);
      if (clusterKey == null) {
        LOG.error("Dropping key with metadata: {} as STK with checksum: {} is unavailable",
          dekMetadata, stkChecksum);
        return null;
      }
      dek = EncryptionUtil.unwrapKey(keyManagementService.getConfiguration(), null, dekWrappedByStk,
        clusterKey.getTheKey());
    }
    long refreshedTimestamp =
      Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES));
    ManagedKeyData dekKeyData =
      new ManagedKeyData(keyCust, keyNamespace, dek, keyState, dekMetadata, refreshedTimestamp);
    if (dek != null) {
      long dekChecksum =
        Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES));
      if (dekKeyData.getKeyChecksum() != dekChecksum) {
        LOG.error("Dropping key, current key checksum: {} didn't match the expected checksum: {}"
          + " for key with metadata: {}", dekKeyData.getKeyChecksum(), dekChecksum, dekMetadata);
        return null;
      }
    }
    return dekKeyData;
  }
}
