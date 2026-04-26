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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import java.io.IOException;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

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

  private ExtendedCell curColCell = null;

  /**
   * Holds a seek-hint produced by {@link org.apache.hadoop.hbase.filter.Filter#getSkipHint(Cell)}
   * at one of the structural short-circuit points in {@link #matchColumn}. When non-null this is
   * returned by {@link #getNextKeyHint} instead of delegating to
   * {@link org.apache.hadoop.hbase.filter.Filter#getNextCellHint}, because the hint was computed
   * for a cell that never reached {@code filterCell}. Cleared on every {@link #getNextKeyHint} call
   * so it cannot leak across multiple seek-hint cycles.
   */
  private ExtendedCell pendingSkipHint = null;

  private static ExtendedCell createStartKey(Scan scan, ScanInfo scanInfo) {
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
      this.versionsAfterFilter = scan.isRaw()
        ? scan.getMaxVersions()
        : Math.min(scan.getMaxVersions(), scanInfo.getMaxVersions());
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
  public ExtendedCell getNextKeyHint(ExtendedCell cell) throws IOException {
    // A structural short-circuit in matchColumn (time-range, column, or version gate) may have
    // stored a hint via resolveSkipHint() before returning SEEK_NEXT_USING_HINT. Drain and return
    // it first; it takes priority because it was produced for the exact cell that triggered the
    // seek code, without ever calling filterCell.
    if (pendingSkipHint != null) {
      ExtendedCell hint = pendingSkipHint;
      pendingSkipHint = null;
      return hint;
    }
    // Normal path: filterCell returned SEEK_NEXT_USING_HINT — delegate to the filter.
    if (filter == null) {
      return null;
    }
    Cell hint = filter.getNextCellHint(cell);
    if (hint == null || hint instanceof ExtendedCell) {
      return (ExtendedCell) hint;
    } else {
      throw new DoNotRetryIOException("Incorrect filter implementation, "
        + "the Cell returned by getNextKeyHint is not an ExtendedCell. Filter class: "
        + filter.getClass().getName());
    }
  }

  @Override
  public void beforeShipped() throws IOException {
    super.beforeShipped();
    if (curColCell != null) {
      this.curColCell = KeyValueUtil.toNewKeyCell(this.curColCell);
    }
  }

  protected final MatchCode matchColumn(ExtendedCell cell, long timestamp, byte typeByte)
    throws IOException {
    int tsCmp = tr.compare(timestamp);
    if (tsCmp > 0) {
      // Cell is newer than the scan's time-range upper bound. Give the filter one last chance to
      // provide a seek hint before we fall back to a plain cell-level SKIP. This addresses
      // HBASE-29974 Path 2: time-range gate fires before filterCell is reached.
      if (resolveSkipHint(cell)) {
        return MatchCode.SEEK_NEXT_USING_HINT;
      }
      return MatchCode.SKIP;
    }
    if (tsCmp < 0) {
      // Cell is older than the scan's time-range lower bound. Give the filter a chance to provide
      // a seek hint before we defer to the column tracker's next-row/next-column suggestion.
      // Addresses HBASE-29974 Path 2: time-range gate fires before filterCell is reached.
      if (resolveSkipHint(cell)) {
        return MatchCode.SEEK_NEXT_USING_HINT;
      }
      return columns.getNextRowOrNextColumn(cell);
    }
    // STEP 1: Check if the column is part of the requested columns
    MatchCode matchCode = columns.checkColumn(cell, typeByte);
    if (matchCode != MatchCode.INCLUDE) {
      // Column is excluded by the scan's column set. Give the filter a chance to provide a
      // seek hint before the column-tracker's suggestion is used. Addresses HBASE-29974 Path 3.
      if (resolveSkipHint(cell)) {
        return MatchCode.SEEK_NEXT_USING_HINT;
      }
      return matchCode;
    }
    /*
     * STEP 2: check the number of versions needed. This method call returns SKIP, SEEK_NEXT_COL,
     * INCLUDE, INCLUDE_AND_SEEK_NEXT_COL, or INCLUDE_AND_SEEK_NEXT_ROW.
     */
    matchCode = columns.checkVersions(cell, timestamp, typeByte, false);
    switch (matchCode) {
      case SKIP:
        // Version limit reached; skip this cell. Give the filter a hint opportunity before
        // falling back to SKIP. Addresses HBASE-29974 Path 3: version gate fires before filterCell.
        if (resolveSkipHint(cell)) {
          return MatchCode.SEEK_NEXT_USING_HINT;
        }
        return MatchCode.SKIP;
      case SEEK_NEXT_COL:
        // Version limit reached; advance to the next column. Give the filter a hint opportunity
        // before falling back to SEEK_NEXT_COL. Addresses HBASE-29974 Path 3.
        if (resolveSkipHint(cell)) {
          return MatchCode.SEEK_NEXT_USING_HINT;
        }
        return MatchCode.SEEK_NEXT_COL;
      default:
        // It means it is INCLUDE, INCLUDE_AND_SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_ROW.
        assert matchCode == MatchCode.INCLUDE || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL
          || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
        break;
    }

    return filter == null
      ? matchCode
      : mergeFilterResponse(cell, matchCode, filter.filterCell(cell));
  }

  /**
   * Asks the current filter for a seek hint via
   * {@link org.apache.hadoop.hbase.filter.Filter#getSkipHint(Cell)}, validates the returned cell
   * type, and if non-null stores it in {@link #pendingSkipHint} so that {@link #getNextKeyHint} can
   * return it when the scan pipeline asks for the seek target after receiving
   * {@link ScanQueryMatcher.MatchCode#SEEK_NEXT_USING_HINT}.
   * <p>
   * This is only called from the structural short-circuit branches of {@link #matchColumn}, where
   * {@code filterCell} has <em>not</em> been called, in accordance with the stateless contract of
   * {@code Filter#getSkipHint}. The filter-null guard is included here so call-sites need no
   * boilerplate.
   * @param cell the cell that triggered the structural short-circuit
   * @return {@code true} if the filter returned a valid hint (stored in {@link #pendingSkipHint}),
   *         {@code false} if no filter is set or the filter returned {@code null}
   * @throws DoNotRetryIOException if the filter returns a non-{@link ExtendedCell} instance
   * @throws IOException           if the filter signals an I/O failure
   */
  private boolean resolveSkipHint(ExtendedCell cell) throws IOException {
    if (filter == null) {
      return false;
    }
    Cell raw = filter.getSkipHint(cell);
    if (raw == null) {
      return false;
    }
    if (!(raw instanceof ExtendedCell)) {
      throw new DoNotRetryIOException(
        "Incorrect filter implementation: the Cell returned by getSkipHint "
          + "is not an ExtendedCell. Filter class: " + filter.getClass().getName());
    }
    pendingSkipHint = (ExtendedCell) raw;
    return true;
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
  private final MatchCode mergeFilterResponse(ExtendedCell cell, MatchCode matchCode,
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
  public boolean moreRowsMayExistAfter(ExtendedCell cell) {
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
    Pair<DeleteTracker, ColumnTracker> trackers =
      getTrackers(regionCoprocessorHost, columns, scanInfo, oldestUnexpiredTS, scan);
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
