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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Query matcher for user scan.
 * <p>
 * We do not consider mvcc here because
 * {@link org.apache.hadoop.hbase.regionserver.StoreFileScanner} and
 * {@link org.apache.hadoop.hbase.regionserver.SegmentScanner} will only return a cell whose mvcc is
 * less than or equal to given read point. For
 * {@link org.apache.hadoop.hbase.client.IsolationLevel#READ_UNCOMMITTED}, we just set the read
 * point to {@link Long#MAX_VALUE}, i.e. still do not need to consider it.
 */
@InterfaceAudience.Private
public abstract class UserScanQueryMatcher extends ScanQueryMatcher {

  protected final boolean hasNullColumn;

  protected final Filter filter;

  protected final byte[] stopRow;

  protected final TimeRange tr;

  private final int versionsAfterFilter;

  private int count = 0;

  private Cell curColCell = null;

  private static Cell createStartKey(Scan scan, ScanInfo scanInfo) {
    if (scan.includeStartRow()) {
      return createStartKeyFromRow(scan.getStartRow(), scanInfo);
    } else {
      return PrivateCellUtil.createLastOnRow(scan.getStartRow());
    }
  }

  protected UserScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
      boolean hasNullColumn, long oldestUnexpiredTS, long now) {
    super(createStartKey(scan, scanInfo), scanInfo, columns, oldestUnexpiredTS, now);
    this.hasNullColumn = hasNullColumn;
    this.filter = scan.getFilter();
    if (this.filter != null) {
      this.versionsAfterFilter =
          scan.isRaw() ? scan.getMaxVersions() : Math.min(scan.getMaxVersions(),
            scanInfo.getMaxVersions());
    } else {
      this.versionsAfterFilter = 0;
    }
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

  @Override
  public void beforeShipped() throws IOException {
    super.beforeShipped();
    if (curColCell != null) {
      this.curColCell = KeyValueUtil.toNewKeyCell(this.curColCell);
    }
  }

  protected final MatchCode matchColumn(Cell cell, long timestamp, byte typeByte)
      throws IOException {
    int tsCmp = tr.compare(timestamp);
    if (tsCmp > 0) {
      return MatchCode.SKIP;
    }
    if (tsCmp < 0) {
      return columns.getNextRowOrNextColumn(cell);
    }
    // STEP 1: Check if the column is part of the requested columns
    MatchCode matchCode = columns.checkColumn(cell, typeByte);
    if (matchCode != MatchCode.INCLUDE) {
      return matchCode;
    }
    /*
     * STEP 2: check the number of versions needed. This method call returns SKIP, SEEK_NEXT_COL,
     * INCLUDE, INCLUDE_AND_SEEK_NEXT_COL, or INCLUDE_AND_SEEK_NEXT_ROW.
     */
    matchCode = columns.checkVersions(cell, timestamp, typeByte, false);
    switch (matchCode) {
      case SKIP:
        return MatchCode.SKIP;
      case SEEK_NEXT_COL:
        return MatchCode.SEEK_NEXT_COL;
      default:
        // It means it is INCLUDE, INCLUDE_AND_SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_ROW.
        assert matchCode == MatchCode.INCLUDE || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL
            || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
        break;
    }

    return filter == null ? matchCode : mergeFilterResponse(cell, matchCode,
      filter.filterCell(cell));
  }

  /**
   * Call this when scan has filter. Decide the desired behavior by checkVersions's MatchCode and
   * filterCell's ReturnCode. Cell may be skipped by filter, so the column versions in result may be
   * less than user need. It need to check versions again when filter and columnTracker both include
   * the cell. <br/>
   *
   * <pre>
   * ColumnChecker                FilterResponse               Desired behavior
   * INCLUDE                      SKIP                         SKIP
   * INCLUDE                      NEXT_COL                     SEEK_NEXT_COL or SEEK_NEXT_ROW
   * INCLUDE                      NEXT_ROW                     SEEK_NEXT_ROW
   * INCLUDE                      SEEK_NEXT_USING_HINT         SEEK_NEXT_USING_HINT
   * INCLUDE                      INCLUDE                      INCLUDE
   * INCLUDE                      INCLUDE_AND_NEXT_COL         INCLUDE_AND_SEEK_NEXT_COL
   * INCLUDE                      INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_COL    SKIP                         SEEK_NEXT_COL
   * INCLUDE_AND_SEEK_NEXT_COL    NEXT_COL                     SEEK_NEXT_COL or SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_COL    NEXT_ROW                     SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_COL    SEEK_NEXT_USING_HINT         SEEK_NEXT_USING_HINT
   * INCLUDE_AND_SEEK_NEXT_COL    INCLUDE                      INCLUDE_AND_SEEK_NEXT_COL
   * INCLUDE_AND_SEEK_NEXT_COL    INCLUDE_AND_NEXT_COL         INCLUDE_AND_SEEK_NEXT_COL
   * INCLUDE_AND_SEEK_NEXT_COL    INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    SKIP                         SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    NEXT_COL                     SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    NEXT_ROW                     SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    SEEK_NEXT_USING_HINT         SEEK_NEXT_USING_HINT
   * INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE                      INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_NEXT_COL         INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW
   * </pre>
   */
  private final MatchCode mergeFilterResponse(Cell cell, MatchCode matchCode,
      ReturnCode filterResponse) {
    switch (filterResponse) {
      case SKIP:
        if (matchCode == MatchCode.INCLUDE) {
          return MatchCode.SKIP;
        } else if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
          return MatchCode.SEEK_NEXT_COL;
        } else if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
          return MatchCode.SEEK_NEXT_ROW;
        }
        break;
      case NEXT_COL:
        if (matchCode == MatchCode.INCLUDE || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
          return columns.getNextRowOrNextColumn(cell);
        } else if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
          return MatchCode.SEEK_NEXT_ROW;
        }
        break;
      case NEXT_ROW:
        return MatchCode.SEEK_NEXT_ROW;
      case SEEK_NEXT_USING_HINT:
        return MatchCode.SEEK_NEXT_USING_HINT;
      case INCLUDE:
        break;
      case INCLUDE_AND_NEXT_COL:
        if (matchCode == MatchCode.INCLUDE) {
          matchCode = MatchCode.INCLUDE_AND_SEEK_NEXT_COL;
        }
        break;
      case INCLUDE_AND_SEEK_NEXT_ROW:
        matchCode = MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
        break;
      default:
        throw new RuntimeException("UNEXPECTED");
    }

    // It means it is INCLUDE, INCLUDE_AND_SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_ROW.
    assert matchCode == MatchCode.INCLUDE || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL
        || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;

    // We need to make sure that the number of cells returned will not exceed max version in scan
    // when the match code is INCLUDE* case.
    if (curColCell == null || !CellUtil.matchingRowColumn(cell, curColCell)) {
      count = 0;
      curColCell = cell;
    }
    count += 1;

    if (count > versionsAfterFilter) {
      // when the number of cells exceed max version in scan, we should return SEEK_NEXT_COL match
      // code, but if current code is INCLUDE_AND_SEEK_NEXT_ROW, we can optimize to choose the max
      // step between SEEK_NEXT_COL and INCLUDE_AND_SEEK_NEXT_ROW, which is SEEK_NEXT_ROW.
      if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
        matchCode = MatchCode.SEEK_NEXT_ROW;
      } else {
        matchCode = MatchCode.SEEK_NEXT_COL;
      }
    }
    if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL || matchCode == MatchCode.SEEK_NEXT_COL) {
      // Update column tracker to next column, As we use the column hint from the tracker to seek
      // to next cell (HBASE-19749)
      columns.doneWithColumn(cell);
    }
    return matchCode;
  }

  protected abstract boolean isGet();

  protected abstract boolean moreRowsMayExistsAfter(int cmpToStopRow);

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
    boolean hasNullColumn =
        !(columns != null && columns.size() != 0 && columns.first().length != 0);
    Pair<DeleteTracker, ColumnTracker> trackers = getTrackers(regionCoprocessorHost, columns,
        scanInfo, oldestUnexpiredTS, scan);
    DeleteTracker deleteTracker = trackers.getFirst();
    ColumnTracker columnTracker = trackers.getSecond();
    if (scan.isRaw()) {
      return RawScanQueryMatcher.create(scan, scanInfo, columnTracker, hasNullColumn,
        oldestUnexpiredTS, now);
    } else {
      return NormalUserScanQueryMatcher.create(scan, scanInfo, columnTracker, deleteTracker,
          hasNullColumn, oldestUnexpiredTS, now);
    }
  }
}
