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
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

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
   * @param keyCust     The key custodian.
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
   * Get the active key for the specified keyCust and key_namespace.
   * @param keyCust     The prefix
   * @param keyNamespace The namespace
   * @return the active key data, or null if no active key found
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getActiveKey(byte[] keyCust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    byte[] rowkeyForGet = constructRowKeyForCustNamespace(keyCust, keyNamespace);
    Get get = new Get(rowkeyForGet);

    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      Result result = table.get(get);
      return parseFromResult(getKeyManagementService(), keyCust, keyNamespace, result);
    }
  }

  /**
   * Get the specific key identified by keyCust, keyNamespace and keyState.
   * @param keyCust     The prefix.
   * @param keyNamespace The namespace.
   * @param keyState     The state of the key.
   * @return the key or {@code null}
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getKey(byte[] keyCust, String keyNamespace, ManagedKeyState keyState)
    throws IOException, KeyException {
    return getKeyInternal(keyCust, keyNamespace, new byte[] { keyState.getVal() });
  }

  /**
   * Get the specific key identified by keyCust, keyNamespace and keyMetadata.
   * @param keyCust     The prefix.
   * @param keyNamespace The namespace.
   * @param keyMetadata  The metadata.
   * @return the key or {@code null}
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getKey(byte[] keyCust, String keyNamespace, String keyMetadata)
    throws IOException, KeyException {
    return getKeyInternal(keyCust, keyNamespace,
      ManagedKeyData.constructMetadataHash(keyMetadata));
  }

  /**
   * Internal helper method to get a key using the provided metadata hash.
   * @param keyCust        The prefix.
   * @param keyNamespace    The namespace.
   * @param keyMetadataHash The metadata hash or state value.
   * @return the key or {@code null}
   * @throws IOException  when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  private ManagedKeyData getKeyInternal(byte[] keyCust, String keyNamespace,
    byte[] keyMetadataHash) throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      byte[] rowKey = constructRowKeyForMetadata(keyCust, keyNamespace, keyMetadataHash);
      Result result = table.get(new Get(rowKey));
      return parseFromResult(getKeyManagementService(), keyCust, keyNamespace, result);
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
          Bytes.toBytes(latestSystemKey.getKeyChecksum()));
    }
    Put result = put.setDurability(Durability.SKIP_WAL).setPriority(HConstants.SYSTEMTABLE_QOS)
      .addColumn(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES,
        Bytes.toBytes(keyData.getRefreshTimestamp()))
      .addColumn(KEY_META_INFO_FAMILY, KEY_STATE_QUAL_BYTES,
        new byte[] { keyData.getKeyState().getVal() });

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
    if (keyData.getKeyState() == ManagedKeyState.FAILED && keyData.getKeyMetadata() == null) {
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
