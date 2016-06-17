package org.apache.hadoop.hbase.replication;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TableCFScopeFilter implements WALEntryFilter {
  private static final Log LOG = LogFactory.getLog(TableCFScopeFilter.class);
  private final ReplicationPeer peer;
  private boolean scopeMatters = true;
  private boolean tableCFMatters = true;

  public TableCFScopeFilter(ReplicationPeer peer) {
    this.peer = peer;
  }

  public void setScopeMatters(boolean scopeMatters) {
    this.scopeMatters = scopeMatters;
  }

  public void setTableCFMatters(boolean tableCFMatters) {
    this.tableCFMatters = tableCFMatters;
  }

  public boolean isAnyFilterSet() {
    return scopeMatters || tableCFMatters;
  }

  @Override
  public WAL.Entry filter(WAL.Entry entry) {
    List<WallEntryCellFilter> filters = Lists.newArrayList();
    NavigableMap<byte[], Integer> scopes = entry.getKey().getReplicationScopes();
    if (scopeMatters) {
      if (scopes != null && !scopes.isEmpty()) {
        filters.add(new ScopeWALEntryCellFilter(scopes));
      } else {
        return null;
      }
    }

    TableName tabName = entry.getKey().getTablename();
    ArrayList<Cell> cells = entry.getEdit().getCells();
    Map<TableName, List<String>> tableCFs = null;

    if (tableCFMatters) {
      try {
        tableCFs = this.peer.getTableCFs();
      } catch (IllegalArgumentException e) {
        LOG.error("should not happen: can't get tableCFs for peer " + peer.getId() +
            ", degenerate as if it's not configured by keeping tableCFs==null");
      }

      // If null means user has explicitly not configured any table CFs so all the tables data are
      // applicable for replication
      if (tableCFs == null) {
        return entry;
      } else {
        if (!tableCFs.containsKey(tabName)) {
          return null;
        }
        filters.add(new TableCfWALEntryCellFilter(tableCFs.get(tabName)));
      }
    }

    int size = cells.size();
    for (int i = size - 1; i >= 0; i--) {
      Cell cell = cells.get(i);
      for (WallEntryCellFilter filter : filters) {
        Cell result = filter.filterCell(cell);
        if (result != null) {
          cells.set(i, result);
        } else {
          cells.remove(i);
        }
      }
    }
    if (cells.size() < size/2) {
      cells.trimToSize();
    }
    return entry;

  }
}
