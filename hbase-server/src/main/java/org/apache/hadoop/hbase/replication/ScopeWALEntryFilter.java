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
package org.apache.hadoop.hbase.replication;

import java.util.NavigableMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Predicate;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * Keeps KVs that are scoped other than local
 */
@InterfaceAudience.Private
public class ScopeWALEntryFilter extends WALEntryFilterBase implements WALCellFilter {

  private final BulkLoadCellFilter bulkLoadFilter = new BulkLoadCellFilter();

  @Override
  public Entry filter(Entry entry) {
    NavigableMap<byte[], Integer> scopes = entry.getKey().getReplicationScopes();
    if (MapUtils.isNotEmpty(scopes)) {
      return entry;
    }
    return clearOrNull(entry);
  }

  private static boolean hasGlobalScope(NavigableMap<byte[], Integer> scopes, byte[] family) {
    Integer scope = scopes.get(family);
    return scope != null && scope.intValue() == HConstants.REPLICATION_SCOPE_GLOBAL;
  }

  @Override
  public Cell filterCell(Entry entry, Cell cell) {
    ExtendedCell extendedCell = PrivateCellUtil.ensureExtendedCell(cell);
    NavigableMap<byte[], Integer> scopes = entry.getKey().getReplicationScopes();
    if (MapUtils.isEmpty(scopes)) {
      return null;
    }
    byte[] family = CellUtil.cloneFamily(cell);
    if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
      return bulkLoadFilter.filterCell(extendedCell, new Predicate<byte[]>() {
        @Override
        public boolean apply(byte[] family) {
          return !hasGlobalScope(scopes, family);
        }
      });
    }
    return hasGlobalScope(scopes, family) ? cell : null;
  }
}
