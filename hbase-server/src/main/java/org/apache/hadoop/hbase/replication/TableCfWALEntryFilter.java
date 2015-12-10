/**
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

package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.util.Bytes;

public class TableCfWALEntryFilter implements WALEntryFilter {

  private static final Log LOG = LogFactory.getLog(TableCfWALEntryFilter.class);
  private final ReplicationPeer peer;

  public TableCfWALEntryFilter(ReplicationPeer peer) {
    this.peer = peer;
  }

  @Override
  public Entry filter(Entry entry) {
    TableName tabName = entry.getKey().getTablename();
    ArrayList<Cell> cells = entry.getEdit().getCells();
    Map<TableName, List<String>> tableCFs = null;

    try {
      tableCFs = this.peer.getTableCFs();
    } catch (IllegalArgumentException e) {
      LOG.error("should not happen: can't get tableCFs for peer " + peer.getId() +
          ", degenerate as if it's not configured by keeping tableCFs==null");
    }
    int size = cells.size();
    
    // If null means user has explicitly not configured any table CFs so all the tables data are
    // applicable for replication
    if (tableCFs == null) {
      return entry;
    }
    // return null(prevent replicating) if logKey's table isn't in this peer's
    // replicable table list
    if (!tableCFs.containsKey(tabName)) {
      return null;
    } else {
      List<String> cfs = tableCFs.get(tabName);
      for (int i = size - 1; i >= 0; i--) {
        Cell cell = cells.get(i);
        // TODO There is a similar logic in ScopeWALEntryFilter but data structures are different so
        // cannot refactor into one now, can revisit and see if any way to unify them.
        // Filter bulk load entries separately
        if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
          Cell filteredBulkLoadEntryCell = filterBulkLoadEntries(cfs, cell);
          if (filteredBulkLoadEntryCell != null) {
            cells.set(i, filteredBulkLoadEntryCell);
          } else {
            cells.remove(i);
          }
        } else {
          // ignore(remove) kv if its cf isn't in the replicable cf list
          // (empty cfs means all cfs of this table are replicable)
          if ((cfs != null) && !cfs.contains(Bytes.toString(cell.getFamilyArray(),
            cell.getFamilyOffset(), cell.getFamilyLength()))) {
            cells.remove(i);
          }
        }
      }
    }
    if (cells.size() < size/2) {
      cells.trimToSize();
    }
    return entry;
  }

  private Cell filterBulkLoadEntries(List<String> cfs, Cell cell) {
    byte[] fam;
    BulkLoadDescriptor bld = null;
    try {
      bld = WALEdit.getBulkLoadDescriptor(cell);
    } catch (IOException e) {
      LOG.warn("Failed to get bulk load events information from the WAL file.", e);
      return cell;
    }
    List<StoreDescriptor> storesList = bld.getStoresList();
    // Copy the StoreDescriptor list and update it as storesList is a unmodifiableList
    List<StoreDescriptor> copiedStoresList = new ArrayList<StoreDescriptor>(storesList);
    Iterator<StoreDescriptor> copiedStoresListIterator = copiedStoresList.iterator();
    boolean anyStoreRemoved = false;
    while (copiedStoresListIterator.hasNext()) {
      StoreDescriptor sd = copiedStoresListIterator.next();
      fam = sd.getFamilyName().toByteArray();
      if (cfs != null && !cfs.contains(Bytes.toString(fam))) {
        copiedStoresListIterator.remove();
        anyStoreRemoved = true;
      }
    }

    if (!anyStoreRemoved) {
      return cell;
    } else if (copiedStoresList.isEmpty()) {
      return null;
    }
    BulkLoadDescriptor.Builder newDesc =
        BulkLoadDescriptor.newBuilder().setTableName(bld.getTableName())
            .setEncodedRegionName(bld.getEncodedRegionName())
            .setBulkloadSeqNum(bld.getBulkloadSeqNum());
    newDesc.addAllStores(copiedStoresList);
    BulkLoadDescriptor newBulkLoadDescriptor = newDesc.build();
    return CellUtil.createCell(CellUtil.cloneRow(cell), WALEdit.METAFAMILY, WALEdit.BULK_LOAD,
      cell.getTimestamp(), cell.getTypeByte(), newBulkLoadDescriptor.toByteArray());
  }
}
