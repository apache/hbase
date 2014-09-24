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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.SizedCellScanner;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ServiceException;

@InterfaceAudience.Private
public class ReplicationProtbufUtil {
  /**
   * A helper to replicate a list of HLog entries using admin protocol.
   *
   * @param admin
   * @param entries
   * @throws java.io.IOException
   */
  public static void replicateWALEntry(final AdminService.BlockingInterface admin,
      final HLog.Entry[] entries) throws IOException {
    Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> p =
      buildReplicateWALEntryRequest(entries, null);
    PayloadCarryingRpcController controller = new PayloadCarryingRpcController(p.getSecond());
    try {
      admin.replicateWALEntry(controller, p.getFirst());
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Create a new ReplicateWALEntryRequest from a list of HLog entries
   *
   * @param entries the HLog entries to be replicated
   * @return a pair of ReplicateWALEntryRequest and a CellScanner over all the WALEdit values
   * found.
   */
  public static Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner>
      buildReplicateWALEntryRequest(final HLog.Entry[] entries) {
    return buildReplicateWALEntryRequest(entries, null);
  }

  /**
   * Create a new ReplicateWALEntryRequest from a list of HLog entries
   *
   * @param entries the HLog entries to be replicated
   * @param encodedRegionName alternative region name to use if not null
   * @return a pair of ReplicateWALEntryRequest and a CellScanner over all the WALEdit values
   * found.
   */
  public static Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner>
      buildReplicateWALEntryRequest(final HLog.Entry[] entries, byte[] encodedRegionName) {
    // Accumulate all the Cells seen in here.
    List<List<? extends Cell>> allCells = new ArrayList<List<? extends Cell>>(entries.length);
    int size = 0;
    WALProtos.FamilyScope.Builder scopeBuilder = WALProtos.FamilyScope.newBuilder();
    AdminProtos.WALEntry.Builder entryBuilder = AdminProtos.WALEntry.newBuilder();
    AdminProtos.ReplicateWALEntryRequest.Builder builder =
      AdminProtos.ReplicateWALEntryRequest.newBuilder();
    HBaseProtos.UUID.Builder uuidBuilder = HBaseProtos.UUID.newBuilder();
    for (HLog.Entry entry: entries) {
      entryBuilder.clear();
      // TODO: this duplicates a lot in HLogKey#getBuilder
      WALProtos.WALKey.Builder keyBuilder = entryBuilder.getKeyBuilder();
      HLogKey key = entry.getKey();
      keyBuilder.setEncodedRegionName(
        ByteStringer.wrap(encodedRegionName == null
            ? key.getEncodedRegionName()
            : encodedRegionName));
      keyBuilder.setTableName(ByteStringer.wrap(key.getTablename().getName()));
      keyBuilder.setLogSequenceNumber(key.getLogSeqNum());
      keyBuilder.setWriteTime(key.getWriteTime());
      if (key.getNonce() != HConstants.NO_NONCE) {
        keyBuilder.setNonce(key.getNonce());
      }
      if (key.getNonceGroup() != HConstants.NO_NONCE) {
        keyBuilder.setNonceGroup(key.getNonceGroup());
      }
      for(UUID clusterId : key.getClusterIds()) {
        uuidBuilder.setLeastSigBits(clusterId.getLeastSignificantBits());
        uuidBuilder.setMostSigBits(clusterId.getMostSignificantBits());
        keyBuilder.addClusterIds(uuidBuilder.build());
      }
      if(key.getOrigLogSeqNum() > 0) {
        keyBuilder.setOrigSequenceNumber(key.getOrigLogSeqNum());
      }
      WALEdit edit = entry.getEdit();
      NavigableMap<byte[], Integer> scopes = key.getScopes();
      if (scopes != null && !scopes.isEmpty()) {
        for (Map.Entry<byte[], Integer> scope: scopes.entrySet()) {
          scopeBuilder.setFamily(ByteStringer.wrap(scope.getKey()));
          WALProtos.ScopeType scopeType =
              WALProtos.ScopeType.valueOf(scope.getValue().intValue());
          scopeBuilder.setScopeType(scopeType);
          keyBuilder.addScopes(scopeBuilder.build());
        }
      }
      List<Cell> cells = edit.getCells();
      // Add up the size.  It is used later serializing out the kvs.
      for (Cell cell: cells) {
        size += CellUtil.estimatedLengthOf(cell);
      }
      // Collect up the cells
      allCells.add(cells);
      // Write out how many cells associated with this entry.
      entryBuilder.setAssociatedCellCount(cells.size());
      builder.addEntry(entryBuilder.build());
    }
    return new Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner>(builder.build(),
      getCellScanner(allCells, size));
  }

  /**
   * @param cells
   * @return <code>cells</code> packaged as a CellScanner
   */
  static CellScanner getCellScanner(final List<List<? extends Cell>> cells, final int size) {
    return new SizedCellScanner() {
      private final Iterator<List<? extends Cell>> entries = cells.iterator();
      private Iterator<? extends Cell> currentIterator = null;
      private Cell currentCell;

      @Override
      public Cell current() {
        return this.currentCell;
      }

      @Override
      public boolean advance() {
        if (this.currentIterator == null) {
          if (!this.entries.hasNext()) return false;
          this.currentIterator = this.entries.next().iterator();
        }
        if (this.currentIterator.hasNext()) {
          this.currentCell = this.currentIterator.next();
          return true;
        }
        this.currentCell = null;
        this.currentIterator = null;
        return advance();
      }

      @Override
      public long heapSize() {
        return size;
      }
    };
  }
}
