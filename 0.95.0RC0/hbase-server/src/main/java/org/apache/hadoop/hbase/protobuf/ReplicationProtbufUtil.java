/**
 *
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

package org.apache.hadoop.hbase.protobuf;


import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

public class ReplicationProtbufUtil {
  /**
   * Get the HLog entries from a list of protocol buffer WALEntry
   *
   * @param protoList the list of protocol buffer WALEntry
   * @return an array of HLog entries
   */
  public static HLog.Entry[]
      toHLogEntries(final List<AdminProtos.WALEntry> protoList) {
    List<HLog.Entry> entries = new ArrayList<HLog.Entry>();
    for (AdminProtos.WALEntry entry: protoList) {
      AdminProtos.WALEntry.WALKey walKey = entry.getKey();
      java.util.UUID clusterId = HConstants.DEFAULT_CLUSTER_ID;
      if (walKey.hasClusterId()) {
        AdminProtos.UUID protoUuid = walKey.getClusterId();
        clusterId = new java.util.UUID(
          protoUuid.getMostSigBits(), protoUuid.getLeastSigBits());
      }
      HLogKey key = new HLogKey(walKey.getEncodedRegionName().toByteArray(),
        walKey.getTableName().toByteArray(), walKey.getLogSequenceNumber(),
        walKey.getWriteTime(), clusterId);
      AdminProtos.WALEntry.WALEdit walEdit = entry.getEdit();
      WALEdit edit = new WALEdit();
      for (ByteString keyValue: walEdit.getKeyValueBytesList()) {
        edit.add(new KeyValue(keyValue.toByteArray()));
      }
      if (walEdit.getFamilyScopeCount() > 0) {
        TreeMap<byte[], Integer> scopes =
          new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
        for (AdminProtos.WALEntry.WALEdit.FamilyScope scope: walEdit.getFamilyScopeList()) {
          scopes.put(scope.getFamily().toByteArray(),
            Integer.valueOf(scope.getScopeType().ordinal()));
        }
        edit.setScopes(scopes);
      }
      entries.add(new HLog.Entry(key, edit));
    }
    return entries.toArray(new HLog.Entry[entries.size()]);
  }

  /**
   * A helper to replicate a list of HLog entries using admin protocol.
   *
   * @param admin
   * @param entries
   * @throws java.io.IOException
   */
  public static void replicateWALEntry(final AdminProtocol admin,
      final HLog.Entry[] entries) throws IOException {
    AdminProtos.ReplicateWALEntryRequest request =
      buildReplicateWALEntryRequest(entries);
    try {
      admin.replicateWALEntry(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Create a new ReplicateWALEntryRequest from a list of HLog entries
   *
   * @param entries the HLog entries to be replicated
   * @return a ReplicateWALEntryRequest
   */
  public static AdminProtos.ReplicateWALEntryRequest
      buildReplicateWALEntryRequest(final HLog.Entry[] entries) {
    AdminProtos.WALEntry.WALEdit.FamilyScope.Builder scopeBuilder = AdminProtos.WALEntry
        .WALEdit
        .FamilyScope
        .newBuilder();
    AdminProtos.WALEntry.Builder entryBuilder = AdminProtos.WALEntry.newBuilder();
    AdminProtos.ReplicateWALEntryRequest.Builder builder =
      AdminProtos.ReplicateWALEntryRequest.newBuilder();
    for (HLog.Entry entry: entries) {
      entryBuilder.clear();
      AdminProtos.WALEntry.WALKey.Builder keyBuilder = entryBuilder.getKeyBuilder();
      HLogKey key = entry.getKey();
      keyBuilder.setEncodedRegionName(
        ByteString.copyFrom(key.getEncodedRegionName()));
      keyBuilder.setTableName(ByteString.copyFrom(key.getTablename()));
      keyBuilder.setLogSequenceNumber(key.getLogSeqNum());
      keyBuilder.setWriteTime(key.getWriteTime());
      UUID clusterId = key.getClusterId();
      if (clusterId != null) {
        AdminProtos.UUID.Builder uuidBuilder = keyBuilder.getClusterIdBuilder();
        uuidBuilder.setLeastSigBits(clusterId.getLeastSignificantBits());
        uuidBuilder.setMostSigBits(clusterId.getMostSignificantBits());
      }
      WALEdit edit = entry.getEdit();
      AdminProtos.WALEntry.WALEdit.Builder editBuilder = entryBuilder.getEditBuilder();
      NavigableMap<byte[], Integer> scopes = edit.getScopes();
      if (scopes != null && !scopes.isEmpty()) {
        for (Map.Entry<byte[], Integer> scope: scopes.entrySet()) {
          scopeBuilder.setFamily(ByteString.copyFrom(scope.getKey()));
          AdminProtos.WALEntry.WALEdit.ScopeType
              scopeType = AdminProtos.WALEntry
              .WALEdit
              .ScopeType
              .valueOf(scope.getValue().intValue());
          scopeBuilder.setScopeType(scopeType);
          editBuilder.addFamilyScope(scopeBuilder.build());
        }
      }
      List<KeyValue> keyValues = edit.getKeyValues();
      for (KeyValue value: keyValues) {
        editBuilder.addKeyValueBytes(ByteString.copyFrom(
          value.getBuffer(), value.getOffset(), value.getLength()));
      }
      builder.addEntry(entryBuilder.build());
    }
    return builder.build();
  }
}
