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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.PBEKeyData;
import org.apache.hadoop.hbase.io.crypto.PBEKeyStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@InterfaceAudience.Private
public class KeyMetaTableAccessor extends PBEKeyManager {
  private static final String KEY_META_INFO_FAMILY_STR = "info";

  public static final byte[] KEY_META_INFO_FAMILY = Bytes.toBytes(KEY_META_INFO_FAMILY_STR);

  public static final TableName KEY_META_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "keymeta");

  public static final String PBE_PREFIX_QUAL_NAME = "pbe_prefix";
  public static final byte[] PBE_PREFIX_QUAL_BYTES = Bytes.toBytes(PBE_PREFIX_QUAL_NAME);

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

  public KeyMetaTableAccessor(Server server) {
    super(server);
  }

  public void addKey(PBEKeyData keyData) throws IOException {
    long refreshTime = EnvironmentEdgeManager.currentTime();
    final Put putForPrefix = addMutationColumns(new Put(keyData.getPbe_prefix()), keyData,
      refreshTime);
    final Put putForMetadata = addMutationColumns(new Put(constructRowKeyForMetadata(keyData)),
      keyData, refreshTime);

    Connection connection = server.getConnection();
    try (Table table = connection.getTable(KEY_META_TABLE_NAME)) {
      table.put(Arrays.asList(putForPrefix, putForMetadata));
    }
  }

  private Put addMutationColumns(Put put, PBEKeyData keyData, long refreshTime) {
    if (keyData.getTheKey() != null) {
      put.addColumn(KEY_META_INFO_FAMILY, DEK_CHECKSUM_QUAL_BYTES,
        Bytes.toBytes(keyData.getKeyChecksum()));
    }
    return put.setDurability(Durability.SKIP_WAL)
      .setPriority(HConstants.SYSTEMTABLE_QOS)
      .addColumn(KEY_META_INFO_FAMILY, DEK_METADATA_QUAL_BYTES, keyData.getKeyMetadata().getBytes())
      //.addColumn(KEY_META_INFO_FAMILY, DEK_WRAPPED_BY_STK_QUAL_BYTES, null)
      //.addColumn(KEY_META_INFO_FAMILY, STK_CHECKSUM_QUAL_BYTES, null)
      .addColumn(KEY_META_INFO_FAMILY, REFRESHED_TIMESTAMP_QUAL_BYTES, Bytes.toBytes(refreshTime))
      .addColumn(KEY_META_INFO_FAMILY, KEY_STATUS_QUAL_BYTES,
        new byte[] { keyData.getKeyStatus().getVal() })
      ;
  }

  private byte[] constructRowKeyForMetadata(PBEKeyData keyData) {
    byte[] pbePrefix = keyData.getPbe_prefix();
    int prefixLength = pbePrefix.length;
    byte[] keyMetadataHash = keyData.getKeyMetadataHash();
    return Bytes.add(Bytes.toBytes(prefixLength), pbePrefix, keyMetadataHash);
  }

  private byte[] extractPBEPrefix(byte[] rowkey) {
    int prefixLength = Bytes.toInt(rowkey);
    return Bytes.copy(rowkey, Bytes.SIZEOF_INT, prefixLength);
  }

  private byte[] extractKeyMetadataHash(byte[] rowkey, byte[] pbePreefix) {
    return Bytes.copy(rowkey, Bytes.SIZEOF_INT + pbePreefix.length, rowkey.length);
  }
}
