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

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Predicate;

public class TableCfWALEntryFilter implements WALEntryFilter, WALCellFilter {

  private static final Log LOG = LogFactory.getLog(TableCfWALEntryFilter.class);
  private ReplicationPeer peer;
  private BulkLoadCellFilter bulkLoadFilter = new BulkLoadCellFilter();

  public TableCfWALEntryFilter(ReplicationPeer peer) {
    this.peer = peer;
  }

  @Override
  public Entry filter(Entry entry) {
    TableName tabName = entry.getKey().getTablename();
    Map<TableName, List<String>> tableCFs = getTableCfs();
    // If null means user has explicitly not configured any table CFs so all the tables data are
    // applicable for replication
    if (tableCFs == null) return entry;

    if (!tableCFs.containsKey(tabName)) {
      return null;
    }

    return entry;
  }

  @Override
  public Cell filterCell(final Entry entry, Cell cell) {
    final Map<TableName, List<String>> tableCfs = getTableCfs();
    if (tableCfs == null) return cell;
    TableName tabName = entry.getKey().getTablename();
    List<String> cfs = tableCfs.get(tabName);
    // ignore(remove) kv if its cf isn't in the replicable cf list
    // (empty cfs means all cfs of this table are replicable)
    if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
      cell = bulkLoadFilter.filterCell(cell, new Predicate<byte[]>() {
        @Override
        public boolean apply(byte[] fam) {
          if (tableCfs != null) {
            List<String> cfs = tableCfs.get(entry.getKey().getTablename());
            if (cfs != null && !cfs.contains(Bytes.toString(fam))) {
              return true;
            }
          }
          return false;
        }
      });
    } else {
      if ((cfs != null) && !cfs.contains(
        Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))) {
        return null;
      }
    }
    return cell;
  }

  Map<TableName, List<String>> getTableCfs() {
    Map<TableName, List<String>> tableCFs = null;
    try {
      tableCFs = this.peer.getTableCFs();
    } catch (IllegalArgumentException e) {
      LOG.error("should not happen: can't get tableCFs for peer " + peer.getId() +
          ", degenerate as if it's not configured by keeping tableCFs==null");
    }
    return tableCFs;
  }
}
