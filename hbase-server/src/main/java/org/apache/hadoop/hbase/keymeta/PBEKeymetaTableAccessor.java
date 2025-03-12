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
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.security.Key;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.List;

/**
 * Accessor for PBE keymeta table.
 */
@InterfaceAudience.Private
public class PBEKeymetaTableAccessor extends PBEKeyManager {
  private static final String KEY_META_INFO_FAMILY_STR = "info";

  public static final byte[] KEY_META_INFO_FAMILY = Bytes.toBytes(KEY_META_INFO_FAMILY_STR);

  public static final TableName KEY_META_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "keymeta");

  public static final String DEK_METADATA_QUAL_NAME = "dek_metadata";
  public static final byte[] DEK_METADATA_QUAL_BYTES = Bytes.toBytes(DEK_METADATA_QUAL_NAME);

  public static final String DEK_CHECKSUM_QUAL_NAME = "dek_checksum";
  public static final byte[] DEK_CHECKSUM_QUAL_BYTES = Bytes.toBytes(DEK_CHECKSUM_QUAL_NAME);

  public static final String DEK_WRAPPED_BY_STK_QUAL_NAME = "dek_wrapped_by_stk";
  public static final byte[] DEK_WRAPPED_BY_STK_QUAL_BYTES = Bytes.toBytes(DEK_WRAPPED_BY_STK_QUAL_NAME);

  public static final String STK_CHECKSUM_QUAL_NAME = "stk_checksum";
  public static final byte[] STK_CHECKSUM_QUAL_BYTES = Bytes.toBytes(STK_CHECKSUM_QUAL_NAME);

  public static final String REFRESHED_TIMESTAMP_QUAL_NAME = "refreshed_timestamp";
  public static final byte[] REFRESHED_TIMESTAMP_QUAL_BYTES = Bytes.toBytes(REFRESHED_TIMESTAMP_QUAL_NAME);

  public static final String KEY_STATUS_QUAL_NAME = "key_status";
  public static final byte[] KEY_STATUS_QUAL_BYTES = Bytes.toBytes(KEY_STATUS_QUAL_NAME);

  public static final String READ_OP_COUNT_QUAL_NAME = "read_op_count";
  public static final byte[] READ_OP_COUNT_QUAL_BYTES = Bytes.toBytes(READ_OP_COUNT_QUAL_NAME);

  public static final String WRITE_OP_COUNT_QUAL_NAME = "write_op_count";
  public static final byte[] WRITE_OP_COUNT_QUAL_BYTES = Bytes.toBytes(WRITE_OP_COUNT_QUAL_NAME);

  public PBEKeymetaTableAccessor(Server server) {
    super(server);
  }

  /**
   * Add the specified key to the keymeta table.
   * @param keyData The key data.
   * @throws IOException when there is an underlying IOException.
   */
  public void addKey(PBEKeyData keyData) throws IOException {
    final Put putForMetadata = addMutationColumns(new Put(constructRowKeyForMetadata(keyData)),
      keyData);
    Connection connection = server.getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.put(putForMetadata);
    }
  }

  /**
   * Get all the keys for the specified pbe_prefix and key_namespace.
   *
   * @param pbePrefix The prefix
   * @param keyNamespace The namespace
   * @return a list of key data, one for each key, can be empty when none were found.
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  protected List<PBEKeyData> getAllKeys(byte[] pbePrefix, String keyNamespace)
    throws IOException, KeyException {
    Connection connection = server.getConnection();
    byte[] prefixForScan = Bytes.add(Bytes.toBytes(pbePrefix.length), pbePrefix,
      Bytes.toBytes(keyNamespace));
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      PrefixFilter prefixFilter = new PrefixFilter(prefixForScan);
      Scan scan = new Scan();
      scan.setFilter(prefixFilter);
      scan.addFamily(KEY_META_INFO_FAMILY);

      ResultScanner scanner = table.getScanner(scan);
      List<PBEKeyData> allKeys = new ArrayList<>();
      for (Result result : scanner) {
        PBEKeyData keyData = parseFromResult(pbePrefix, keyNamespace, result);
        if (keyData != null) {
          allKeys.add(keyData);
        }
      }
      return allKeys;
    }
  }

  /**
   * Get all the active keys for the specified pbe_prefix and key_namespace.
   *
   * @param pbePrefix The prefix
   * @param keyNamespace The namespace
   * @return a list of key data, one for each active key, can be empty when none were found.
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public List<PBEKeyData> getActiveKeys(byte[] pbePrefix, String keyNamespace)
    throws IOException, KeyException {
    List<PBEKeyData> activeKeys = new ArrayList<>();
    for (PBEKeyData keyData : getAllKeys(pbePrefix, keyNamespace)) {
      if (keyData.getKeyStatus() == PBEKeyStatus.ACTIVE) {
        activeKeys.add(keyData);
      }
    }
    return activeKeys;
  }

  /**
   * Get the specific key identified by pbePrefix, keyNamespace and keyMetadata.
   *
   * @param pbePrefix    The prefix.
   * @param keyNamespace The namespace.
   * @param keyMetadata  The metadata.
   * @return the key or {@code null}
   * @throws IOException when there is an underlying IOException.
   * @throws KeyException when there is an underlying KeyException.
   */
  public PBEKeyData getKey(byte[] pbePrefix, String keyNamespace, String keyMetadata)
    throws IOException, KeyException {
    Connection connection = server.getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      byte[] rowKey = constructRowKeyForMetadata(pbePrefix, keyNamespace,
        PBEKeyData.constructMetadataHash(keyMetadata));
      Result result = table.get(new Get(rowKey));
      return parseFromResult(pbePrefix, keyNamespace, result);
    }
  }

  /**
   * Report read or write operation count on the specific key identified by pbePrefix, keyNamespace
   * and keyMetadata. The reported value is added to the existing operation count using the
   * Increment mutation.
   * @param pbePrefix    The prefix.
   * @param keyNamespace The namespace.
   * @param keyMetadata  The metadata.
   * @throws IOException when there is an underlying IOException.
   */
  public void reportOperation(byte[] pbePrefix, String keyNamespace, String keyMetadata, long count,
      boolean isReadOperation) throws IOException {
    Connection connection = server.getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      byte[] rowKey = constructRowKeyForMetadata(pbePrefix, keyNamespace,
        PBEKeyData.constructMetadataHash(keyMetadata));
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
  private Put addMutationColumns(Put put, PBEKeyData keyData) throws IOException {
    PBEKeyData latestClusterKey = server.getPBEClusterKeyCache().getLatestClusterKey();
    if (keyData.getTheKey() != null) {
      byte[] dekWrappedBySTK = EncryptionUtil.wrapKey(server.getConfiguration(), null,
        keyData.getTheKey(), latestClusterKey.getTheKey());
      put.addColumn(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES,
          Bytes.toBytes(keyData.getKeyChecksum()))
         .addColumn(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES, dekWrappedBySTK)
         ;
    }
    return put.setDurability(Durability.SKIP_WAL)
      .setPriority(HConstants.SYSTEMTABLE_QOS)
      .addColumn(KEY_META_INFO_FAMILY, DEK_METADATA_QUAL_BYTES, keyData.getKeyMetadata().getBytes())
      .addColumn(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES,
        Bytes.toBytes(latestClusterKey.getKeyChecksum()))
      .addColumn(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES,
        Bytes.toBytes(keyData.getRefreshTimestamp()))
      .addColumn(KEY_META_INFO_FAMILY, KEY_STATUS_QUAL_BYTES,
        new byte[] { keyData.getKeyStatus().getVal() })
      ;
  }

  private byte[] constructRowKeyForMetadata(PBEKeyData keyData) {
    return constructRowKeyForMetadata(keyData.getPBEPrefix(), keyData.getKeyNamespace(),
      keyData.getKeyMetadataHash());
  }

  private static byte[] constructRowKeyForMetadata(byte[] pbePrefix, String keyNamespace,
      byte[] keyMetadataHash) {
    int prefixLength = pbePrefix.length;
    return Bytes.add(Bytes.toBytes(prefixLength), pbePrefix, Bytes.toBytesBinary(keyNamespace),
      keyMetadataHash);
  }

  private PBEKeyData parseFromResult(byte[] pbePrefix, String keyNamespace, Result result)
    throws IOException, KeyException {
    if (result == null || result.isEmpty()) {
      return null;
    }
    String dekMetadata = Bytes.toString(result.getValue(KEY_META_INFO_FAMILY,
      DEK_METADATA_QUAL_BYTES));
    byte[] dekWrappedByStk = result.getValue(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES);
    Key dek = null;
    if (dekWrappedByStk != null) {
      long stkChecksum =
        Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES));
      PBEKeyData clusterKey = server.getPBEClusterKeyCache().getClusterKeyByChecksum(stkChecksum);
      if (clusterKey == null) {
        LOG.error("Dropping key with metadata: {} as STK with checksum: {} is unavailable",
          dekMetadata, stkChecksum);
        return null;
      }
      dek = EncryptionUtil.unwrapKey(server.getConfiguration(), null, dekWrappedByStk,
        clusterKey.getTheKey());
    }
    PBEKeyStatus keyStatus = PBEKeyStatus.forValue(
      result.getValue(KEY_META_INFO_FAMILY, KEY_STATUS_QUAL_BYTES)[0]);
    long refreshedTimestamp = Bytes.toLong(result.getValue(KEY_META_INFO_FAMILY,
      REFRESHED_TIMESTAMP_QUAL_BYTES));
    byte[] readOpValue = result.getValue(KEY_META_INFO_FAMILY, READ_OP_COUNT_QUAL_BYTES);
    long readOpCount = readOpValue != null ? Bytes.toLong(readOpValue) : 0;
    byte[] writeOpValue = result.getValue(KEY_META_INFO_FAMILY, WRITE_OP_COUNT_QUAL_BYTES);
    long writeOpCount = writeOpValue != null ? Bytes.toLong(writeOpValue) : 0;
    PBEKeyData dekKeyData = new PBEKeyData(pbePrefix, keyNamespace, dek, keyStatus, dekMetadata,
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
