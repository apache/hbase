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

import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import com.google.common.base.Predicate;

/**
 * Keeps KVs that are scoped other than local
 */
@InterfaceAudience.Private
public class ScopeWALEntryFilter implements WALEntryFilter, WALCellFilter {

  BulkLoadCellFilter bulkLoadFilter = new BulkLoadCellFilter();

  @Override
  public Entry filter(Entry entry) {
    NavigableMap<byte[], Integer> scopes = entry.getKey().getScopes();
    if (scopes == null || scopes.isEmpty()) {
      return null;
    }
    return entry;
  }

  @Override
  public Cell filterCell(Entry entry, Cell cell) {
    final NavigableMap<byte[], Integer> scopes = entry.getKey().getScopes();
      // The scope will be null or empty if
      // there's nothing to replicate in that WALEdit
      byte[] fam = CellUtil.cloneFamily(cell);
      if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
        cell = bulkLoadFilter.filterCell(cell, new Predicate<byte[]>() {
          @Override
          public boolean apply(byte[] fam) {
            return !scopes.containsKey(fam) || scopes.get(fam) == HConstants.REPLICATION_SCOPE_LOCAL;
          }
        });
      } else {
        if (!scopes.containsKey(fam) || scopes.get(fam) == HConstants.REPLICATION_SCOPE_LOCAL) {
          return null;
        }
      }
    return cell;
  }
}
