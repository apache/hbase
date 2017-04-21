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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import com.google.common.base.Predicate;

public class BulkLoadCellFilter {
  private static final Log LOG = LogFactory.getLog(BulkLoadCellFilter.class);

  /**
   * Filters the bulk load cell using the supplied predicate.
   * @param cell The WAL cell to filter.
   * @param famPredicate Returns true of given family should be removed.
   * @return The filtered cell.
   */
  public Cell filterCell(Cell cell, Predicate<byte[]> famPredicate) {
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
    List<StoreDescriptor> copiedStoresList = new ArrayList<>(storesList);
    Iterator<StoreDescriptor> copiedStoresListIterator = copiedStoresList.iterator();
    boolean anyStoreRemoved = false;
    while (copiedStoresListIterator.hasNext()) {
      StoreDescriptor sd = copiedStoresListIterator.next();
      fam = sd.getFamilyName().toByteArray();
      if (famPredicate.apply(fam)) {
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
