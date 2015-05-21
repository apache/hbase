/**
 *
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Keeps track of the columns for a scan if they are not explicitly specified
 */
@InterfaceAudience.Private
public class ScanWildcardColumnTracker implements ColumnTracker {
  private Cell columnCell = null;
  private int currentCount = 0;
  private int maxVersions;
  private int minVersions;
  /* Keeps track of the latest timestamp and type included for current column.
   * Used to eliminate duplicates. */
  private long latestTSOfCurrentColumn;
  private byte latestTypeOfCurrentColumn;

  private long oldestStamp;

  /**
   * Return maxVersions of every row.
   * @param minVersion Minimum number of versions to keep
   * @param maxVersion Maximum number of versions to return
   * @param oldestUnexpiredTS oldest timestamp that has not expired according
   *          to the TTL.
   */
  public ScanWildcardColumnTracker(int minVersion, int maxVersion,
      long oldestUnexpiredTS) {
    this.maxVersions = maxVersion;
    this.minVersions = minVersion;
    this.oldestStamp = oldestUnexpiredTS;
  }

  /**
   * {@inheritDoc}
   * This receives puts *and* deletes.
   */
  @Override
  public MatchCode checkColumn(Cell cell, byte type) throws IOException {
    return MatchCode.INCLUDE;
  }

  /**
   * {@inheritDoc}
   * This receives puts *and* deletes. Deletes do not count as a version, but rather
   * take the version of the previous put (so eventually all but the last can be reclaimed).
   */
  @Override
  public ScanQueryMatcher.MatchCode checkVersions(Cell cell,
      long timestamp, byte type, boolean ignoreCount) throws IOException {

    if (columnCell == null) {
      // first iteration.
      resetCell(cell);
      if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;
      // do not count a delete marker as another version
      return checkVersion(type, timestamp);
    }
    int cmp = CellComparator.compareQualifiers(cell, this.columnCell);
    if (cmp == 0) {
      if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;

      //If column matches, check if it is a duplicate timestamp
      if (sameAsPreviousTSAndType(timestamp, type)) {
        return ScanQueryMatcher.MatchCode.SKIP;
      }
      return checkVersion(type, timestamp);
    }

    resetTSAndType();

    // new col > old col
    if (cmp > 0) {
      // switched columns, lets do something.x
      resetCell(cell);
      if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;
      return checkVersion(type, timestamp);
    }

    // new col < oldcol
    // WARNING: This means that very likely an edit for some other family
    // was incorrectly stored into the store for this one. Throw an exception,
    // because this might lead to data corruption.
    throw new IOException(
        "ScanWildcardColumnTracker.checkColumn ran into a column actually " +
        "smaller than the previous column: " +
        Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
  }

  private void resetCell(Cell columnCell) {
    this.columnCell = columnCell;
    currentCount = 0;
  }

  /**
   * Check whether this version should be retained.
   * There are 4 variables considered:
   * If this version is past max versions -> skip it
   * If this kv has expired or was deleted, check min versions
   * to decide whther to skip it or not.
   *
   * Increase the version counter unless this is a delete
   */
  private MatchCode checkVersion(byte type, long timestamp) {
    if (!CellUtil.isDelete(type)) {
      currentCount++;
    }
    if (currentCount > maxVersions) {
      return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL; // skip to next col
    }
    // keep the KV if required by minversions or it is not expired, yet
    if (currentCount <= minVersions || !isExpired(timestamp)) {
      setTSAndType(timestamp, type);
      return ScanQueryMatcher.MatchCode.INCLUDE;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }

  }

  @Override
  public void reset() {
    columnCell = null;
    resetTSAndType();
  }

  private void resetTSAndType() {
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
    latestTypeOfCurrentColumn = 0;
  }

  private void setTSAndType(long timestamp, byte type) {
    latestTSOfCurrentColumn = timestamp;
    latestTypeOfCurrentColumn = type;
  }

  private boolean sameAsPreviousTSAndType(long timestamp, byte type) {
    return timestamp == latestTSOfCurrentColumn && type == latestTypeOfCurrentColumn;
  }

  private boolean isExpired(long timestamp) {
    return timestamp < oldestStamp;
  }

  /**
   * Used by matcher and scan/get to get a hint of the next column
   * to seek to after checkColumn() returns SKIP.  Returns the next interesting
   * column we want, or NULL there is none (wildcard scanner).
   *
   * @return The column count.
   */
  public ColumnCount getColumnHint() {
    return null;
  }

  /**
   * We can never know a-priori if we are done, so always return false.
   * @return false
   */
  @Override
  public boolean done() {
    return false;
  }

  @Override
  public MatchCode getNextRowOrNextColumn(Cell cell) {
    return MatchCode.SEEK_NEXT_COL;
  }

  public boolean isDone(long timestamp) {
    return minVersions <= 0 && isExpired(timestamp);
  }
}
