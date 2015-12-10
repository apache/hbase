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
import java.util.NavigableMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * Keeps KVs that are scoped other than local
 */
@InterfaceAudience.Private
public class ScopeWALEntryFilter implements WALEntryFilter {
  private static final Log LOG = LogFactory.getLog(ScopeWALEntryFilter.class);

  @Override
  public Entry filter(Entry entry) {
    NavigableMap<byte[], Integer> scopes = entry.getKey().getScopes();
    if (scopes == null || scopes.isEmpty()) {
      return null;
    }
    ArrayList<Cell> cells = entry.getEdit().getCells();
    int size = cells.size();
    byte[] fam;
    for (int i = size - 1; i >= 0; i--) {
      Cell cell = cells.get(i);
      // If a bulk load entry has a scope then that means user has enabled replication for
      // bulk load hfiles.
      // TODO There is a similar logic in TableCfWALEntryFilter but data structures are different so
      // cannot refactor into one now, can revisit and see if any way to unify them.
      if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
        Cell filteredBulkLoadEntryCell = filterBulkLoadEntries(scopes, cell);
        if (filteredBulkLoadEntryCell != null) {
          cells.set(i, filteredBulkLoadEntryCell);
        } else {
          cells.remove(i);
        }
      } else {
        // The scope will be null or empty if
        // there's nothing to replicate in that WALEdit
        fam = CellUtil.cloneFamily(cell);
        if (!scopes.containsKey(fam) || scopes.get(fam) == HConstants.REPLICATION_SCOPE_LOCAL) {
          cells.remove(i);
        }
      }
    }
    if (cells.size() < size / 2) {
      cells.trimToSize();
    }
    return entry;
  }

  private Cell filterBulkLoadEntries(NavigableMap<byte[], Integer> scopes, Cell cell) {
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
      if (!scopes.containsKey(fam) || scopes.get(fam) == HConstants.REPLICATION_SCOPE_LOCAL) {
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
