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
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;

/**
 * Query matcher for user scan.
 * <p>
 * We do not consider mvcc here because
 * {@link org.apache.hadoop.hbase.regionserver.StoreFileScanner} and
 * {@link org.apache.hadoop.hbase.regionserver.MemStore#getScanners(long)} will only return a cell
 * whose mvcc is less than or equal to given read point. For
 * {@link org.apache.hadoop.hbase.client.IsolationLevel#READ_UNCOMMITTED}, we just set the read
 * point to {@link Long#MAX_VALUE}, i.e. still do not need to consider it.
 */
@InterfaceAudience.Private
public abstract class UserScanQueryMatcher extends ScanQueryMatcher {

  protected final boolean hasNullColumn;

  protected final Filter filter;

  protected final byte[] stopRow;

  protected final TimeRange tr;

  protected UserScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
      boolean hasNullColumn, long oldestUnexpiredTS, long now) {
    super(scan.getStartRow(), scanInfo, columns, oldestUnexpiredTS, now);
    this.hasNullColumn = hasNullColumn;
    this.filter = scan.getFilter();
    this.stopRow = scan.getStopRow();
    TimeRange timeRange = scan.getColumnFamilyTimeRange().get(scanInfo.getFamily());
    if (timeRange == null) {
      this.tr = scan.getTimeRange();
    } else {
      this.tr = timeRange;
    }
  }

  @Override
  public boolean hasNullColumnInQuery() {
    return hasNullColumn;
  }

  @Override
  public boolean isUserScan() {
    return true;
  }

  @Override
  public Filter getFilter() {
    return filter;
  }

  @Override
  public Cell getNextKeyHint(Cell cell) throws IOException {
    if (filter == null) {
      return null;
    } else {
      return filter.getNextCellHint(cell);
    }
  }

  protected final MatchCode matchColumn(Cell cell) throws IOException {
    long timestamp = cell.getTimestamp();
    int tsCmp = tr.compare(timestamp);
    if (tsCmp > 0) {
      return MatchCode.SKIP;
    }
    int qualifierOffset = cell.getQualifierOffset();
    int qualifierLength = cell.getQualifierLength();
    if (tsCmp < 0) {
      return columns.getNextRowOrNextColumn(cell.getQualifierArray(), qualifierOffset,
        qualifierLength);
    }
    byte typeByte = cell.getTypeByte();
    // STEP 1: Check if the column is part of the requested columns
    MatchCode colChecker = columns.checkColumn(cell.getQualifierArray(), qualifierOffset,
      qualifierLength, typeByte);
    if (colChecker != MatchCode.INCLUDE) {
      if (colChecker == MatchCode.SEEK_NEXT_ROW) {
        stickyNextRow = true;
      }
      return colChecker;
    }
    ReturnCode filterResponse = ReturnCode.SKIP;
    // STEP 2: Yes, the column is part of the requested columns. Check if filter is present
    if (filter != null) {
      // STEP 3: Filter the key value and return if it filters out
      filterResponse = filter.filterKeyValue(cell);
      switch (filterResponse) {
        case SKIP:
          return MatchCode.SKIP;
        case NEXT_COL:
          return columns.getNextRowOrNextColumn(cell.getQualifierArray(), qualifierOffset,
            qualifierLength);
        case NEXT_ROW:
          stickyNextRow = true;
          return MatchCode.SEEK_NEXT_ROW;
        case SEEK_NEXT_USING_HINT:
          return MatchCode.SEEK_NEXT_USING_HINT;
        default:
          // It means it is either include or include and seek next
          break;
      }
    }
    /*
     * STEP 4: Reaching this step means the column is part of the requested columns and either
     * the filter is null or the filter has returned INCLUDE or INCLUDE_AND_NEXT_COL response.
     * Now check the number of versions needed. This method call returns SKIP, INCLUDE,
     * INCLUDE_AND_SEEK_NEXT_ROW, INCLUDE_AND_SEEK_NEXT_COL.
     *
     * FilterResponse            ColumnChecker               Desired behavior
     * INCLUDE                   SKIP                        row has already been included, SKIP.
     * INCLUDE                   INCLUDE                     INCLUDE
     * INCLUDE                   INCLUDE_AND_SEEK_NEXT_COL   INCLUDE_AND_SEEK_NEXT_COL
     * INCLUDE                   INCLUDE_AND_SEEK_NEXT_ROW   INCLUDE_AND_SEEK_NEXT_ROW
     * INCLUDE_AND_SEEK_NEXT_COL SKIP                        row has already been included, SKIP.
     * INCLUDE_AND_SEEK_NEXT_COL INCLUDE                     INCLUDE_AND_SEEK_NEXT_COL
     * INCLUDE_AND_SEEK_NEXT_COL INCLUDE_AND_SEEK_NEXT_COL   INCLUDE_AND_SEEK_NEXT_COL
     * INCLUDE_AND_SEEK_NEXT_COL INCLUDE_AND_SEEK_NEXT_ROW   INCLUDE_AND_SEEK_NEXT_ROW
     *
     * In all the above scenarios, we return the column checker return value except for
     * FilterResponse (INCLUDE_AND_SEEK_NEXT_COL) and ColumnChecker(INCLUDE)
     */
    colChecker = columns.checkVersions(cell.getQualifierArray(), qualifierOffset, qualifierLength,
      timestamp, typeByte, false);
    // Optimize with stickyNextRow
    stickyNextRow = colChecker == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW ? true : stickyNextRow;
    return (filterResponse == ReturnCode.INCLUDE_AND_NEXT_COL && colChecker == MatchCode.INCLUDE)
        ? MatchCode.INCLUDE_AND_SEEK_NEXT_COL : colChecker;
  }

  protected abstract boolean isGet();

  protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
    return cmpToStopRow < 0;
  }

  @Override
  public boolean moreRowsMayExistAfter(Cell cell) {
    // If a 'get' Scan -- we are doing a Get (every Get is a single-row Scan in implementation) --
    // then we are looking at one row only, the one specified in the Get coordinate..so we know
    // for sure that there are no more rows on this Scan
    if (isGet()) {
      return false;
    }
    // If no stopRow, return that there may be more rows. The tests that follow depend on a
    // non-empty, non-default stopRow so this little test below short-circuits out doing the
    // following compares.
    if (this.stopRow == null || this.stopRow.length == 0) {
      return true;
    }
    return moreRowsMayExistsAfter(rowComparator.compareRows(cell, stopRow, 0, stopRow.length));
  }

  public static UserScanQueryMatcher create(Scan scan, ScanInfo scanInfo,
      NavigableSet<byte[]> columns, long oldestUnexpiredTS, long now,
      RegionCoprocessorHost regionCoprocessorHost) throws IOException {
    int maxVersions = scan.isRaw() ? scan.getMaxVersions()
        : Math.min(scan.getMaxVersions(), scanInfo.getMaxVersions());
    boolean hasNullColumn;
    ColumnTracker columnTracker;
    if (columns == null || columns.size() == 0) {
      // there is always a null column in the wildcard column query.
      hasNullColumn = true;
      // use a specialized scan for wildcard column tracker.
      columnTracker = new ScanWildcardColumnTracker(scanInfo.getMinVersions(), maxVersions,
          oldestUnexpiredTS);
    } else {
      // We can share the ExplicitColumnTracker, diff is we reset
      // between rows, not between storefiles.
      // whether there is null column in the explicit column query
      hasNullColumn = columns.first().length == 0;
      columnTracker = new ExplicitColumnTracker(columns, scanInfo.getMinVersions(), maxVersions,
          oldestUnexpiredTS);
    }
    if (scan.isRaw()) {
      return RawScanQueryMatcher.create(scan, scanInfo, columnTracker, hasNullColumn,
        oldestUnexpiredTS, now);
    } else {
      return NormalUserScanQueryMatcher.create(scan, scanInfo, columnTracker, hasNullColumn,
        oldestUnexpiredTS, now, regionCoprocessorHost);
    }
  }
}
