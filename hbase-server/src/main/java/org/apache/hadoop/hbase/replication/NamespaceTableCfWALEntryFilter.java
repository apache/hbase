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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Filter a WAL Entry by the peer config: replicate_all flag, namespaces config, table-cfs config,
 * exclude namespaces config, and exclude table-cfs config.
 *
 * If replicate_all flag is true, it means all user tables will be replicated to peer cluster. But
 * you can set exclude namespaces or exclude table-cfs which can't be replicated to peer cluster.
 * Note: set a exclude namespace means that all tables in this namespace can't be replicated.
 *
 * If replicate_all flag is false, it means all user tables can't be replicated to peer cluster.
 * But you can set namespaces or table-cfs which will be replicated to peer cluster.
 * Note: set a namespace means that all tables in this namespace will be replicated.
 */
@InterfaceAudience.Private
public class NamespaceTableCfWALEntryFilter implements WALEntryFilter, WALCellFilter {

  private final ReplicationPeer peer;
  private BulkLoadCellFilter bulkLoadFilter = new BulkLoadCellFilter();

  public NamespaceTableCfWALEntryFilter(ReplicationPeer peer) {
    this.peer = peer;
  }

  @Override
  public Entry filter(Entry entry) {
    if (ReplicationUtils.contains(this.peer.getPeerConfig(), entry.getKey().getTableName())) {
      return entry;
    } else {
      return null;
    }
  }

  @Override
  public Cell filterCell(final Entry entry, Cell cell) {
    ReplicationPeerConfig peerConfig = this.peer.getPeerConfig();
    if (peerConfig.replicateAllUserTables()) {
      // replicate all user tables, but filter by exclude table-cfs config
      final Map<TableName, List<String>> excludeTableCfs = peerConfig.getExcludeTableCFsMap();
      if (excludeTableCfs == null) {
        return cell;
      }

      if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
        cell = bulkLoadFilter.filterCell(cell,
          fam -> filterByExcludeTableCfs(entry.getKey().getTableName(), Bytes.toString(fam),
            excludeTableCfs));
      } else {
        if (filterByExcludeTableCfs(entry.getKey().getTableName(),
          Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()),
          excludeTableCfs)) {
          return null;
        }
      }

      return cell;
    } else {
      // not replicate all user tables, so filter by table-cfs config
      final Map<TableName, List<String>> tableCfs = peerConfig.getTableCFsMap();
      if (tableCfs == null) {
        return cell;
      }

      if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
        cell = bulkLoadFilter.filterCell(cell,
          fam -> filterByTableCfs(entry.getKey().getTableName(), Bytes.toString(fam), tableCfs));
      } else {
        if (filterByTableCfs(entry.getKey().getTableName(),
          Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()),
          tableCfs)) {
          return null;
        }
      }

      return cell;
    }
  }

  private boolean filterByExcludeTableCfs(TableName tableName, String family,
      Map<TableName, List<String>> excludeTableCfs) {
    List<String> excludeCfs = excludeTableCfs.get(tableName);
    if (excludeCfs != null) {
      // empty cfs means all cfs of this table are excluded
      if (excludeCfs.isEmpty()) {
        return true;
      }
      // ignore(remove) kv if its cf is in the exclude cfs list
      if (excludeCfs.contains(family)) {
        return true;
      }
    }
    return false;
  }

  private boolean filterByTableCfs(TableName tableName, String family,
      Map<TableName, List<String>> tableCfs) {
    List<String> cfs = tableCfs.get(tableName);
    // ignore(remove) kv if its cf isn't in the replicable cf list
    // (empty cfs means all cfs of this table are replicable)
    if (cfs != null && !cfs.contains(family)) {
      return true;
    }
    return false;
  }
}
