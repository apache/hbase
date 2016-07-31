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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;

/**
 * Query matcher for compaction.
 */
@InterfaceAudience.Private
public abstract class CompactionScanQueryMatcher extends ScanQueryMatcher {

  /** readPoint over which the KVs are unconditionally included */
  protected final long maxReadPointToTrackVersions;

  /** Keeps track of deletes */
  protected final DeleteTracker deletes;

  /** whether to return deleted rows */
  protected final KeepDeletedCells keepDeletedCells;

  protected CompactionScanQueryMatcher(ScanInfo scanInfo, DeleteTracker deletes,
      long readPointToUse, long oldestUnexpiredTS, long now) {
    super(HConstants.EMPTY_START_ROW, scanInfo,
        new ScanWildcardColumnTracker(scanInfo.getMinVersions(), scanInfo.getMaxVersions(),
            oldestUnexpiredTS),
        oldestUnexpiredTS, now);
    this.maxReadPointToTrackVersions = readPointToUse;
    this.deletes = deletes;
    this.keepDeletedCells = scanInfo.getKeepDeletedCells();
  }

  @Override
  public boolean hasNullColumnInQuery() {
    return true;
  }

  @Override
  public boolean isUserScan() {
    return false;
  }

  @Override
  public boolean moreRowsMayExistAfter(Cell cell) {
    return true;
  }

  @Override
  public Filter getFilter() {
    // no filter when compaction
    return null;
  }

  @Override
  public Cell getNextKeyHint(Cell cell) throws IOException {
    // no filter, so no key hint.
    return null;
  }

  @Override
  protected void reset() {
    deletes.reset();
  }

  protected final void trackDelete(Cell cell) {
    // If keepDeletedCells is true, then we only remove cells by versions or TTL during
    // compaction, so we do not need to track delete here.
    // If keepDeletedCells is TTL and the delete marker is expired, then we can make sure that the
    // minVerions is larger than 0(otherwise we will just return at preCheck). So here we still
    // need to track the delete marker to see if it masks some cells.
    if (keepDeletedCells == KeepDeletedCells.FALSE
        || (keepDeletedCells == KeepDeletedCells.TTL && cell.getTimestamp() < oldestUnexpiredTS)) {
      deletes.add(cell);
    }
  }

  public static CompactionScanQueryMatcher create(ScanInfo scanInfo, ScanType scanType,
      long readPointToUse, long earliestPutTs, long oldestUnexpiredTS, long now,
      byte[] dropDeletesFromRow, byte[] dropDeletesToRow,
      RegionCoprocessorHost regionCoprocessorHost) throws IOException {
    DeleteTracker deleteTracker = instantiateDeleteTracker(regionCoprocessorHost);
    if (dropDeletesFromRow == null) {
      if (scanType == ScanType.COMPACT_RETAIN_DELETES) {
        return new MinorCompactionScanQueryMatcher(scanInfo, deleteTracker, readPointToUse,
            oldestUnexpiredTS, now);
      } else {
        return new MajorCompactionScanQueryMatcher(scanInfo, deleteTracker, readPointToUse,
            earliestPutTs, oldestUnexpiredTS, now);
      }
    } else {
      return new StripeCompactionScanQueryMatcher(scanInfo, deleteTracker, readPointToUse,
          earliestPutTs, oldestUnexpiredTS, now, dropDeletesFromRow, dropDeletesToRow);
    }
  }
}
