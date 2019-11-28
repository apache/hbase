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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.SizedCellScanner;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

@InterfaceAudience.Private
public class ReplicationProtbufUtil {
  /**
   * A helper to replicate a list of WAL entries using admin protocol.
   * @param admin Admin service
   * @param entries Array of WAL entries to be replicated
   * @param replicationClusterId Id which will uniquely identify source cluster FS client
   *          configurations in the replication configuration directory
   * @param sourceBaseNamespaceDir Path to source cluster base namespace directory
   * @param sourceHFileArchiveDir Path to the source cluster hfile archive directory
   * @throws java.io.IOException
   */
  public static void replicateWALEntry(final AdminService.BlockingInterface admin,
      final Entry[] entries, String replicationClusterId, Path sourceBaseNamespaceDir,
      Path sourceHFileArchiveDir, int timeout) throws IOException {
    Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> p =
        buildReplicateWALEntryRequest(entries, null, replicationClusterId, sourceBaseNamespaceDir,
          sourceHFileArchiveDir);
    HBaseRpcController controller = new HBaseRpcControllerImpl(p.getSecond());
    controller.setCallTimeout(timeout);
    try {
      admin.replicateWALEntry(controller, p.getFirst());
    } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException e) {
      throw ProtobufUtil.getServiceException(e);
    }
  }

  /**
   * Create a new ReplicateWALEntryRequest from a list of WAL entries
   *
   * @param entries the WAL entries to be replicated
   * @return a pair of ReplicateWALEntryRequest and a CellScanner over all the WALEdit values
   * found.
   */
  public static Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner>
      buildReplicateWALEntryRequest(final Entry[] entries) throws IOException {
    return buildReplicateWALEntryRequest(entries, null, null, null, null);
  }

  /**
   * Create a new ReplicateWALEntryRequest from a list of WAL entries
   * @param entries the WAL entries to be replicated
   * @param encodedRegionName alternative region name to use if not null
   * @param replicationClusterId Id which will uniquely identify source cluster FS client
   *          configurations in the replication configuration directory
   * @param sourceBaseNamespaceDir Path to source cluster base namespace directory
   * @param sourceHFileArchiveDir Path to the source cluster hfile archive directory
   * @return a pair of ReplicateWALEntryRequest and a CellScanner over all the WALEdit values found.
   */
  public static Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner>
      buildReplicateWALEntryRequest(final Entry[] entries, byte[] encodedRegionName,
          String replicationClusterId, Path sourceBaseNamespaceDir, Path sourceHFileArchiveDir)
          throws IOException {
    // Accumulate all the Cells seen in here.
    List<List<? extends Cell>> allCells = new ArrayList<>(entries.length);
    int size = 0;
    AdminProtos.WALEntry.Builder entryBuilder = AdminProtos.WALEntry.newBuilder();
    AdminProtos.ReplicateWALEntryRequest.Builder builder =
      AdminProtos.ReplicateWALEntryRequest.newBuilder();

    for (Entry entry: entries) {
      entryBuilder.clear();
      WALProtos.WALKey.Builder keyBuilder;
      try {
        keyBuilder = entry.getKey().getBuilder(WALCellCodec.getNoneCompressor());
      } catch (IOException e) {
        throw new IOException(
            "There should not throw exception since NoneCompressor do not throw any exceptions", e);
      }
      if(encodedRegionName != null){
        keyBuilder.setEncodedRegionName(
            UnsafeByteOperations.unsafeWrap(encodedRegionName));
      }
      entryBuilder.setKey(keyBuilder.build());
      WALEdit edit = entry.getEdit();
      List<Cell> cells = edit.getCells();
      // Add up the size.  It is used later serializing out the kvs.
      for (Cell cell: cells) {
        size += PrivateCellUtil.estimatedSerializedSizeOf(cell);
      }
      // Collect up the cells
      allCells.add(cells);
      // Write out how many cells associated with this entry.
      entryBuilder.setAssociatedCellCount(cells.size());
      builder.addEntry(entryBuilder.build());
    }

    if (replicationClusterId != null) {
      builder.setReplicationClusterId(replicationClusterId);
    }
    if (sourceBaseNamespaceDir != null) {
      builder.setSourceBaseNamespaceDirPath(sourceBaseNamespaceDir.toString());
    }
    if (sourceHFileArchiveDir != null) {
      builder.setSourceHFileArchiveDirPath(sourceHFileArchiveDir.toString());
    }

    return new Pair<>(builder.build(),
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
