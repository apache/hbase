/*
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
import java.util.NavigableSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;

/**
 * This class is used for the tracking and enforcement of columns and numbers
 * of versions during the course of a Get or Scan operation, when explicit
 * column qualifiers have been asked for in the query.
 *
 * With a little magic (see {@link ScanQueryMatcher}), we can use this matcher
 * for both scans and gets.  The main difference is 'next' and 'done' collapse
 * for the scan case (since we see all columns in order), and we only reset
 * between rows.
 *
 * <p>
 * This class is utilized by {@link ScanQueryMatcher} mainly through two methods:
 * <ul><li>{@link #checkColumn} is called when a Put satisfies all other
 * conditions of the query.</li>
 * <li>{@link #getNextRowOrNextColumn} is called whenever ScanQueryMatcher
 * believes that the current column should be skipped (by timestamp, filter etc.)</li>
 * </ul>
 * <p>
 * These two methods returns a
 * {@link org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode}
 * to define what action should be taken.
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
@InterfaceAudience.Private
public class ExplicitColumnTracker implements ColumnTracker {

  private final int maxVersions;
  private final int minVersions;

 /**
  * Contains the list of columns that the ExplicitColumnTracker is tracking.
  * Each ColumnCount instance also tracks how many versions of the requested
  * column have been returned.
  */
  private final ColumnCount[] columns;
  private int index;
  private ColumnCount column;
  /** Keeps track of the latest timestamp included for current column.
   * Used to eliminate duplicates. */
  private long latestTSOfCurrentColumn;
  private long oldestStamp;

  /**
   * Default constructor.
   * @param columns columns specified user in query
   * @param minVersions minimum number of versions to keep
   * @param maxVersions maximum versions to return per column
   * @param oldestUnexpiredTS the oldest timestamp we are interested in,
   *  based on TTL
   */
  public ExplicitColumnTracker(NavigableSet<byte[]> columns, int minVersions,
      int maxVersions, long oldestUnexpiredTS) {
    this.maxVersions = maxVersions;
    this.minVersions = minVersions;
    this.oldestStamp = oldestUnexpiredTS;
    this.columns = new ColumnCount[columns.size()];
    int i=0;
    for(byte [] column : columns) {
      this.columns[i++] = new ColumnCount(column);
    }
    reset();
  }

    /**
   * Done when there are no more columns to match against.
   */
  public boolean done() {
    return this.index >= columns.length;
  }

  public ColumnCount getColumnHint() {
    return this.column;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ScanQueryMatcher.MatchCode checkColumn(Cell cell, byte type) {
    // delete markers should never be passed to an
    // *Explicit*ColumnTracker
    assert !CellUtil.isDelete(type);
    do {
      // No more columns left, we are done with this query
      if(done()) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // No more columns to match against, done with storefile
      if(this.column == null) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // Compare specific column to current column
      int ret = CellComparator.compareQualifiers(cell, column.getBuffer(), column.getOffset(),
          column.getLength());

      // Column Matches. Return include code. The caller would call checkVersions
      // to limit the number of versions.
      if (ret == 0) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }

      resetTS();

      if (ret < 0) {
        // The current KV is smaller than the column the ExplicitColumnTracker
        // is interested in, so seek to that column of interest.
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL;
      }

      // The current KV is bigger than the column the ExplicitColumnTracker
      // is interested in. That means there is no more data for the column
      // of interest. Advance the ExplicitColumnTracker state to next
      // column of interest, and check again.
      if (ret > 0) {
        ++this.index;
        if (done()) {
          // No more to match, do not include, done with this row.
          return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
        }
        // This is the recursive case.
        this.column = this.columns[this.index];
      }
    } while(true);
  }

  @Override
  public ScanQueryMatcher.MatchCode checkVersions(Cell cell,
      long timestamp, byte type, boolean ignoreCount) throws IOException {
    assert !CellUtil.isDelete(type);
    if (ignoreCount) return ScanQueryMatcher.MatchCode.INCLUDE;
    // Check if it is a duplicate timestamp
    if (sameAsPreviousTS(timestamp)) {
      // If duplicate, skip this Key
      return ScanQueryMatcher.MatchCode.SKIP;
    }
    int count = this.column.increment();
    if (count >= maxVersions || (count >= minVersions && isExpired(timestamp))) {
      // Done with versions for this column
      ++this.index;
      resetTS();
      if (done()) {
        // We have served all the requested columns.
        this.column = null;
        return ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
      }
      // We are done with current column; advance to next column
      // of interest.
      this.column = this.columns[this.index];
      return ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL;
    }
    setTS(timestamp);
    return ScanQueryMatcher.MatchCode.INCLUDE;
  }

  // Called between every row.
  public void reset() {
    this.index = 0;
    this.column = this.columns[this.index];
    for(ColumnCount col : this.columns) {
      col.setCount(0);
    }
    resetTS();
  }

  private void resetTS() {
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
  }

  private void setTS(long timestamp) {
    latestTSOfCurrentColumn = timestamp;
  }

  private boolean sameAsPreviousTS(long timestamp) {
    return timestamp == latestTSOfCurrentColumn;
  }

  private boolean isExpired(long timestamp) {
    return timestamp < oldestStamp;
  }

  /**
   * This method is used to inform the column tracker that we are done with
   * this column. We may get this information from external filters or
   * timestamp range and we then need to indicate this information to
   * tracker. It is required only in case of ExplicitColumnTracker.
   * @param cell
   */
  public void doneWithColumn(Cell cell) {
    while (this.column != null) {
      int compare = CellComparator.compareQualifiers(cell, column.getBuffer(), column.getOffset(),
          column.getLength());
      resetTS();
      if (compare >= 0) {
        ++this.index;
        if (done()) {
          // Will not hit any more columns in this storefile
          this.column = null;
        } else {
          this.column = this.columns[this.index];
        }
        if (compare > 0) continue;
      }
      return;
    }
  }

  @Override
  public MatchCode getNextRowOrNextColumn(Cell cell) {
    doneWithColumn(cell);

    if (getColumnHint() == null) {
      return MatchCode.SEEK_NEXT_ROW;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }
  }

  public boolean isDone(long timestamp) {
    return minVersions <= 0 && isExpired(timestamp);
  }
}
