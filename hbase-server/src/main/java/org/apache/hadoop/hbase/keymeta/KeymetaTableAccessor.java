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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyState;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.List;

/**
 * Accessor for keymeta table as part of key management.
 */
@InterfaceAudience.Private
public class KeymetaTableAccessor extends KeyManagementBase {
  private static final String KEY_META_INFO_FAMILY_STR = "info";

  public static final byte[] KEY_META_INFO_FAMILY = Bytes.toBytes(KEY_META_INFO_FAMILY_STR);

  public static final TableName KEY_META_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "keymeta");

  public static final String DEK_METADATA_QUAL_NAME = "dek_metadata";
  public static final byte[] DEK_METADATA_QUAL_BYTES = Bytes.toBytes(DEK_METADATA_QUAL_NAME);

  public static final String DEK_CHECKSUM_QUAL_NAME = "dek_checksum";
  public static final byte[] DEK_CHECKSUM_QUAL_BYTES = Bytes.toBytes(DEK_CHECKSUM_QUAL_NAME);

  public static final String DEK_WRAPPED_BY_STK_QUAL_NAME = "dek_wrapped_by_stk";
  public static final byte[] DEK_WRAPPED_BY_STK_QUAL_BYTES =
      Bytes.toBytes(DEK_WRAPPED_BY_STK_QUAL_NAME);

  public static final String STK_CHECKSUM_QUAL_NAME = "stk_checksum";
  public static final byte[] STK_CHECKSUM_QUAL_BYTES = Bytes.toBytes(STK_CHECKSUM_QUAL_NAME);

  public static final String REFRESHED_TIMESTAMP_QUAL_NAME = "refreshed_timestamp";
  public static final byte[] REFRESHED_TIMESTAMP_QUAL_BYTES =
      Bytes.toBytes(REFRESHED_TIMESTAMP_QUAL_NAME);

  public static final String KEY_STATE_QUAL_NAME = "key_state";
  public static final byte[] KEY_STATE_QUAL_BYTES = Bytes.toBytes(KEY_STATE_QUAL_NAME);

  public static final String READ_OP_COUNT_QUAL_NAME = "read_op_count";
  public static final byte[] READ_OP_COUNT_QUAL_BYTES = Bytes.toBytes(READ_OP_COUNT_QUAL_NAME);

  public static final String WRITE_OP_COUNT_QUAL_NAME = "write_op_count";
  public static final byte[] WRITE_OP_COUNT_QUAL_BYTES = Bytes.toBytes(WRITE_OP_COUNT_QUAL_NAME);

  public KeymetaTableAccessor(Server server) {
    super(server);
  }

  /**
   * Add the specified key to the keymeta table.
   * @param keyData The key data.
   * @throws IOException when there is an underlying IOException.
   */
  public void addKey(ManagedKeyData keyData) throws IOException {
    assertKeyManagementEnabled();
    final Put putForMetadata = addMutationColumns(new Put(constructRowKeyForMetadata(keyData)),
      keyData);
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.put(putForMetadata);
    }
  }

  /**
   * Get all the keys for the specified key_cust and key_namespace.
   *
   * @param key_cust     The key custodian.
   * @param keyNamespace The namespace
   * @return a list of key data, one for each key, can be empty when none were found.
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.UNITTEST)
  public List<ManagedKeyData> getAllKeys(byte[] key_cust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    byte[] prefixForScan = Bytes.add(Bytes.toBytes(key_cust.length), key_cust,
      Bytes.toBytes(keyNamespace));
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      PrefixFilter prefixFilter = new PrefixFilter(prefixForScan);
      Scan scan = new Scan();
      scan.setFilter(prefixFilter);
      scan.addFamily(KEY_META_INFO_FAMILY);

      ResultScanner scanner = table.getScanner(scan);
      List<ManagedKeyData> allKeys = new ArrayList<>();
      for (Result result : scanner) {
        ManagedKeyData keyData = parseFromResult(getServer(), key_cust, keyNamespace, result);
        if (keyData != null) {
          allKeys.add(keyData);
        }
      }
      return allKeys;
    }
  }

  /**
   * Get all the active keys for the specified key_cust and key_namespace.
   *
   * @param key_cust The prefix
   * @param keyNamespace The namespace
   * @return a list of key data, one for each active key, can be empty when none were found.
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public List<ManagedKeyData> getActiveKeys(byte[] key_cust, String keyNamespace)
    throws IOException, KeyException {
    assertKeyManagementEnabled();
    List<ManagedKeyData> activeKeys = new ArrayList<>();
    for (ManagedKeyData keyData : getAllKeys(key_cust, keyNamespace)) {
      if (keyData.getKeyState() == ManagedKeyState.ACTIVE) {
        activeKeys.add(keyData);
      }
    }
    return activeKeys;
  }

  /**
   * Get the specific key identified by key_cust, keyNamespace and keyState.
   *
   * @param key_cust    The prefix.
   * @param keyNamespace The namespace.
   * @param keyState    The state of the key.
   * @return the key or {@code null}
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getKey(byte[] key_cust, String keyNamespace, ManagedKeyState keyState)
    throws IOException, KeyException {
    return getKeyInternal(key_cust, keyNamespace, new byte[] { keyState.getVal() });
  }

  /**
   * Get the specific key identified by key_cust, keyNamespace and keyMetadata.
   *
   * @param key_cust    The prefix.
   * @param keyNamespace The namespace.
   * @param keyMetadata  The metadata.
   * @return the key or {@code null}
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public ManagedKeyData getKey(byte[] key_cust, String keyNamespace, String keyMetadata)
    throws IOException, KeyException {
    return getKeyInternal(key_cust, keyNamespace,
        ManagedKeyData.constructMetadataHash(keyMetadata));
  }

  /**
   * Internal helper method to get a key using the provided metadata hash.
   *
   * @param key_cust        The prefix.
   * @param keyNamespace    The namespace.
   * @param keyMetadataHash The metadata hash or state value.
   * @return the key or {@code null}
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  private ManagedKeyData getKeyInternal(byte[] key_cust, String keyNamespace,
      byte[] keyMetadataHash) throws IOException, KeyException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      byte[] rowKey = constructRowKeyForMetadata(key_cust, keyNamespace, keyMetadataHash);
      Result result = table.get(new Get(rowKey));
      return parseFromResult(getServer(), key_cust, keyNamespace, result);
    }
  }

  /**
   * Report read or write operation count on the specific key identified by key_cust, keyNamespace
   * and keyMetadata. The reported value is added to the existing operation count using the
   * Increment mutation.
   * @param key_cust    The prefix.
   * @param keyNamespace The namespace.
   * @param keyMetadata  The metadata.
   * @throws IOException when there is an underlying IOException.
   */
  public void reportOperation(byte[] key_cust, String keyNamespace, String keyMetadata, long count,
      boolean isReadOperation) throws IOException {
    assertKeyManagementEnabled();
    Connection connection = getServer().getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      byte[] rowKey = constructRowKeyForMetadata(key_cust, keyNamespace,
        ManagedKeyData.constructMetadataHash(keyMetadata));
      Increment incr = new Increment(rowKey)
        .addColumn(KEY_META_INFO_FAMILY,
          isReadOperation ? READ_OP_COUNT_QUAL_BYTES : WRITE_OP_COUNT_QUAL_BYTES,
          count);
      table.increment(incr);
    }
  }

  /**
   * Add the mutation columns to the given Put that are derived from the keyData.
   */
  private Put addMutationColumns(Put put, ManagedKeyData keyData) throws IOException {
    ManagedKeyData latestSystemKey = getServer().getSystemKeyCache().getLatestSystemKey();
    if (keyData.getTheKey() != null) {
      byte[] dekWrappedBySTK = EncryptionUtil.wrapKey(getServer().getConfiguration(), null,
        keyData.getTheKey(), latestSystemKey.getTheKey());
      put.addColumn(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES,
          Bytes.toBytes(keyData.getKeyChecksum()))
         .addColumn(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES, dekWrappedBySTK)
         .addColumn(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES,
           Bytes.toBytes(latestSystemKey.getKeyChecksum()))
         ;
    }
    Put result = put.setDurability(Durability.SKIP_WAL)
      .setPriority(HConstants.SYSTEMTABLE_QOS)
      .addColumn(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES,
        Bytes.toBytes(keyData.getRefreshTimestamp()))
      .addColumn(KEY_META_INFO_FAMILY, KEY_STATE_QUAL_BYTES,
        new byte[] { keyData.getKeyState().getVal() })
      ;

    // Only add metadata column if metadata is not null
    String metadata = keyData.getKeyMetadata();
    if (metadata != null) {
      result.addColumn(KEY_META_INFO_FAMILY, DEK_METADATA_QUAL_BYTES, metadata.getBytes());
    }

    return result;
  }

  @VisibleForTesting
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

  @VisibleForTesting
  public static byte[] constructRowKeyForMetadata(byte[] key_cust, String keyNamespace,
      byte[] keyMetadataHash) {
    int custLength = key_cust.length;
    return Bytes.add(Bytes.toBytes(custLength), key_cust, Bytes.toBytesBinary(keyNamespace),
      keyMetadataHash);
  }

  @VisibleForTesting
  public static ManagedKeyData parseFromResult(Server server, byte[] key_cust, String keyNamespace,
      Result result) throws IOException, KeyException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    ManagedKeyState keyState = ManagedKeyState.forValue(
      result.getValue(KEY_META_INFO_FAMILY, KEY_STATE_QUAL_BYTES)[0]);
    String dekMetadata = Bytes.toString(result.getValue(KEY_META_INFO_FAMILY,
      DEK_METADATA_QUAL_BYTES));
    byte[] dekWrappedByStk = result.getValue(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES);
    if ((keyState == ManagedKeyState.ACTIVE || keyState == ManagedKeyState.INACTIVE)
        && dekWrappedByStk == null) {
      throw new IOException(keyState + " key must have a wrapped key");
    }
    Key dek = null;
    if (dekWrappedByStk != null) {
      long stkChecksum =
        Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES));
      ManagedKeyData clusterKey = server.getSystemKeyCache().getSystemKeyByChecksum(stkChecksum);
      if (clusterKey == null) {
        LOG.error("Dropping key with metadata: {} as STK with checksum: {} is unavailable",
          dekMetadata, stkChecksum);
        return null;
      }
      dek = EncryptionUtil.unwrapKey(server.getConfiguration(), null, dekWrappedByStk,
        clusterKey.getTheKey());
    }
    long refreshedTimestamp = Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY,
      REFRESHED_TIMESTAMP_QUAL_BYTES));
    byte[] readOpValue = result.getValue(KEY_META_INFO_FAMILY, READ_OP_COUNT_QUAL_BYTES);
    long readOpCount = readOpValue != null ? Bytes.toLong(readOpValue) : 0;
    byte[] writeOpValue = result.getValue(KEY_META_INFO_FAMILY, WRITE_OP_COUNT_QUAL_BYTES);
    long writeOpCount = writeOpValue != null ? Bytes.toLong(writeOpValue) : 0;
    ManagedKeyData
      dekKeyData = new ManagedKeyData(key_cust, keyNamespace, dek, keyState, dekMetadata,
      refreshedTimestamp, readOpCount, writeOpCount);
    if (dek != null) {
      long dekChecksum = Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY,
        DEK_CHECKSUM_QUAL_BYTES));
      if (dekKeyData.getKeyChecksum() != dekChecksum) {
        LOG.error("Dropping key, current key checksum: {} didn't match the expected checksum: {}"
          + " for key with metadata: {}", dekKeyData.getKeyChecksum(), dekChecksum, dekMetadata);
        return null;
      }
    }
    return dekKeyData;
  }
}
