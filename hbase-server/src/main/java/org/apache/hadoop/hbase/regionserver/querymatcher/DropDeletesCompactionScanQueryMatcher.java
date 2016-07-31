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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.ScanInfo;

/**
 * A query matcher for compaction which can drop delete markers.
 */
@InterfaceAudience.Private
public abstract class DropDeletesCompactionScanQueryMatcher extends CompactionScanQueryMatcher {

  /**
   * By default, when hbase.hstore.time.to.purge.deletes is 0ms, a delete marker is always removed
   * during a major compaction. If set to non-zero value then major compaction will try to keep a
   * delete marker around for the given number of milliseconds. We want to keep the delete markers
   * around a bit longer because old puts might appear out-of-order. For example, during log
   * replication between two clusters.
   * <p>
   * If the delete marker has lived longer than its column-family's TTL then the delete marker will
   * be removed even if time.to.purge.deletes has not passed. This is because all the Puts that this
   * delete marker can influence would have also expired. (Removing of delete markers on col family
   * TTL will not happen if min-versions is set to non-zero)
   * <p>
   * But, if time.to.purge.deletes has not expired then a delete marker will not be removed just
   * because there are no Puts that it is currently influencing. This is because Puts, that this
   * delete can influence. may appear out of order.
   */
  protected final long timeToPurgeDeletes;

  /**
   * Oldest put in any of the involved store files Used to decide whether it is ok to delete family
   * delete marker of this store keeps deleted KVs.
   */
  protected final long earliestPutTs;

  protected DropDeletesCompactionScanQueryMatcher(ScanInfo scanInfo, DeleteTracker deletes,
      long readPointToUse, long earliestPutTs, long oldestUnexpiredTS, long now) {
    super(scanInfo, deletes, readPointToUse, oldestUnexpiredTS, now);
    this.timeToPurgeDeletes = scanInfo.getTimeToPurgeDeletes();
    this.earliestPutTs = earliestPutTs;
  }

  protected final MatchCode tryDropDelete(Cell cell) {
    long timestamp = cell.getTimestamp();
    // If it is not the time to drop the delete marker, just return
    if (timeToPurgeDeletes > 0 && now - timestamp <= timeToPurgeDeletes) {
      return MatchCode.INCLUDE;
    }
    if (keepDeletedCells == KeepDeletedCells.TRUE
        || (keepDeletedCells == KeepDeletedCells.TTL && timestamp >= oldestUnexpiredTS)) {
      // If keepDeletedCell is true, or the delete marker is not expired yet, we should include it
      // in version counting to see if we can drop it. The only exception is that, we can make
      // sure that no put is older than this delete marker. And under this situation, all later
      // cells of this column(must be delete markers) can be skipped.
      if (timestamp < earliestPutTs) {
        return columns.getNextRowOrNextColumn(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
      } else {
        return null;
      }
    } else {
      return MatchCode.SKIP;
    }
  }
}
