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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * The old query matcher implementation. Used to keep compatibility for coprocessor that could
 * overwrite the StoreScanner before compaction. Should be removed once we find a better way to do
 * filtering during compaction.
 */
@Deprecated
@InterfaceAudience.Private
public class LegacyScanQueryMatcher extends ScanQueryMatcher {

  private final TimeRange tr;

  private final Filter filter;

  /** Keeps track of deletes */
  private final DeleteTracker deletes;

  /**
   * The following three booleans define how we deal with deletes. There are three different
   * aspects:
   * <ol>
   * <li>Whether to keep delete markers. This is used in compactions. Minor compactions always keep
   * delete markers.</li>
   * <li>Whether to keep deleted rows. This is also used in compactions, if the store is set to keep
   * deleted rows. This implies keeping the delete markers as well.</li> In this case deleted rows
   * are subject to the normal max version and TTL/min version rules just like "normal" rows.
   * <li>Whether a scan can do time travel queries even before deleted marker to reach deleted
   * rows.</li>
   * </ol>
   */
  /** whether to retain delete markers */
  private boolean retainDeletesInOutput;

  /** whether to return deleted rows */
  private final KeepDeletedCells keepDeletedCells;

  // By default, when hbase.hstore.time.to.purge.deletes is 0ms, a delete
  // marker is always removed during a major compaction. If set to non-zero
  // value then major compaction will try to keep a delete marker around for
  // the given number of milliseconds. We want to keep the delete markers
  // around a bit longer because old puts might appear out-of-order. For
  // example, during log replication between two clusters.
  //
  // If the delete marker has lived longer than its column-family's TTL then
  // the delete marker will be removed even if time.to.purge.deletes has not
  // passed. This is because all the Puts that this delete marker can influence
  // would have also expired. (Removing of delete markers on col family TTL will
  // not happen if min-versions is set to non-zero)
  //
  // But, if time.to.purge.deletes has not expired then a delete
  // marker will not be removed just because there are no Puts that it is
  // currently influencing. This is because Puts, that this delete can
  // influence. may appear out of order.
  private final long timeToPurgeDeletes;

  /**
   * This variable shows whether there is an null column in the query. There always exists a null
   * column in the wildcard column query. There maybe exists a null column in the explicit column
   * query based on the first column.
   */
  private final boolean hasNullColumn;

  /** readPoint over which the KVs are unconditionally included */
  private final long maxReadPointToTrackVersions;

  /**
   * Oldest put in any of the involved store files Used to decide whether it is ok to delete family
   * delete marker of this store keeps deleted KVs.
   */
  protected final long earliestPutTs;

  private final byte[] stopRow;

  private byte[] dropDeletesFromRow = null, dropDeletesToRow = null;

  private LegacyScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
      boolean hasNullColumn, DeleteTracker deletes, ScanType scanType, long readPointToUse,
      long earliestPutTs, long oldestUnexpiredTS, long now) {
    super(scan.getStartRow(), scanInfo, columns, oldestUnexpiredTS, now);
    TimeRange timeRange = scan.getColumnFamilyTimeRange().get(scanInfo.getFamily());
    if (timeRange == null) {
      this.tr = scan.getTimeRange();
    } else {
      this.tr = timeRange;
    }
    this.hasNullColumn = hasNullColumn;
    this.deletes = deletes;
    this.filter = scan.getFilter();
    this.maxReadPointToTrackVersions = readPointToUse;
    this.timeToPurgeDeletes = scanInfo.getTimeToPurgeDeletes();
    this.earliestPutTs = earliestPutTs;

    /* how to deal with deletes */
    this.keepDeletedCells = scanInfo.getKeepDeletedCells();
    this.retainDeletesInOutput = scanType == ScanType.COMPACT_RETAIN_DELETES;
    this.stopRow = scan.getStopRow();
  }

  private LegacyScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
      boolean hasNullColumn, DeleteTracker deletes, ScanType scanType, long readPointToUse,
      long earliestPutTs, long oldestUnexpiredTS, long now, byte[] dropDeletesFromRow,
      byte[] dropDeletesToRow) {
    this(scan, scanInfo, columns, hasNullColumn, deletes, scanType, readPointToUse, earliestPutTs,
        oldestUnexpiredTS, now);
    this.dropDeletesFromRow = Preconditions.checkNotNull(dropDeletesFromRow);
    this.dropDeletesToRow = Preconditions.checkNotNull(dropDeletesToRow);
  }

  @Override
  public MatchCode match(Cell cell) throws IOException {
    if (filter != null && filter.filterAllRemaining()) {
      return MatchCode.DONE_SCAN;
    }
    MatchCode returnCode = preCheck(cell);
    if (returnCode != null) {
      return returnCode;
    }
    /*
     * The delete logic is pretty complicated now.
     * This is corroborated by the following:
     * 1. The store might be instructed to keep deleted rows around.
     * 2. A scan can optionally see past a delete marker now.
     * 3. If deleted rows are kept, we have to find out when we can
     *    remove the delete markers.
     * 4. Family delete markers are always first (regardless of their TS)
     * 5. Delete markers should not be counted as version
     * 6. Delete markers affect puts of the *same* TS
     * 7. Delete marker need to be version counted together with puts
     *    they affect
     */
    long timestamp = cell.getTimestamp();
    byte typeByte = cell.getTypeByte();
    long mvccVersion = cell.getSequenceId();
    if (CellUtil.isDelete(typeByte)) {
      if (keepDeletedCells == KeepDeletedCells.FALSE
          || (keepDeletedCells == KeepDeletedCells.TTL && timestamp < oldestUnexpiredTS)) {
        // first ignore delete markers if the scanner can do so, and the
        // range does not include the marker
        //
        // during flushes and compactions also ignore delete markers newer
        // than the readpoint of any open scanner, this prevents deleted
        // rows that could still be seen by a scanner from being collected
        boolean includeDeleteMarker = tr.withinOrAfterTimeRange(timestamp);
        if (includeDeleteMarker && mvccVersion <= maxReadPointToTrackVersions) {
          this.deletes.add(cell);
        }
        // Can't early out now, because DelFam come before any other keys
      }

      if (timeToPurgeDeletes > 0
          && (EnvironmentEdgeManager.currentTime() - timestamp) <= timeToPurgeDeletes) {
        return MatchCode.INCLUDE;
      } else if (retainDeletesInOutput || mvccVersion > maxReadPointToTrackVersions) {
        // always include or it is not time yet to check whether it is OK
        // to purge deltes or not
        // if this is not a user scan (compaction), we can filter this deletemarker right here
        // otherwise (i.e. a "raw" scan) we fall through to normal version and timerange checking
        return MatchCode.INCLUDE;
      } else if (keepDeletedCells == KeepDeletedCells.TRUE
          || (keepDeletedCells == KeepDeletedCells.TTL && timestamp >= oldestUnexpiredTS)) {
        if (timestamp < earliestPutTs) {
          // keeping delete rows, but there are no puts older than
          // this delete in the store files.
          return columns.getNextRowOrNextColumn(cell);
        }
        // else: fall through and do version counting on the
        // delete markers
      } else {
        return MatchCode.SKIP;
      }
      // note the following next else if...
      // delete marker are not subject to other delete markers
    } else if (!this.deletes.isEmpty()) {
      DeleteResult deleteResult = deletes.isDeleted(cell);
      switch (deleteResult) {
        case FAMILY_DELETED:
        case COLUMN_DELETED:
          return columns.getNextRowOrNextColumn(cell);
        case VERSION_DELETED:
        case FAMILY_VERSION_DELETED:
          return MatchCode.SKIP;
        case NOT_DELETED:
          break;
        default:
          throw new RuntimeException("UNEXPECTED");
        }
    }

    int timestampComparison = tr.compare(timestamp);
    if (timestampComparison >= 1) {
      return MatchCode.SKIP;
    } else if (timestampComparison <= -1) {
      return columns.getNextRowOrNextColumn(cell);
    }

    // STEP 1: Check if the column is part of the requested columns
    MatchCode colChecker = columns.checkColumn(cell, typeByte);
    if (colChecker == MatchCode.INCLUDE) {
      ReturnCode filterResponse = ReturnCode.SKIP;
      // STEP 2: Yes, the column is part of the requested columns. Check if filter is present
      if (filter != null) {
        // STEP 3: Filter the key value and return if it filters out
        filterResponse = filter.filterKeyValue(cell);
        switch (filterResponse) {
        case SKIP:
          return MatchCode.SKIP;
        case NEXT_COL:
          return columns.getNextRowOrNextColumn(cell);
        case NEXT_ROW:
          stickyNextRow = true;
          return MatchCode.SEEK_NEXT_ROW;
        case SEEK_NEXT_USING_HINT:
          return MatchCode.SEEK_NEXT_USING_HINT;
        default:
          //It means it is either include or include and seek next
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
      colChecker = columns.checkVersions(cell, timestamp, typeByte,
          mvccVersion > maxReadPointToTrackVersions);
      //Optimize with stickyNextRow
      boolean seekNextRowFromEssential = filterResponse == ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW &&
          filter.isFamilyEssential(cell.getFamilyArray());
      if (colChecker == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW || seekNextRowFromEssential) {
        stickyNextRow = true;
      }
      if (filterResponse == ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW) {
        if (colChecker != MatchCode.SKIP) {
          return MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
        }
        return MatchCode.SEEK_NEXT_ROW;
      }
      return (filterResponse == ReturnCode.INCLUDE_AND_NEXT_COL &&
          colChecker == MatchCode.INCLUDE) ? MatchCode.INCLUDE_AND_SEEK_NEXT_COL
          : colChecker;
    }
    stickyNextRow = (colChecker == MatchCode.SEEK_NEXT_ROW) ? true
        : stickyNextRow;
    return colChecker;
  }

  @Override
  public boolean hasNullColumnInQuery() {
    return hasNullColumn;
  }

  /**
   * Handle partial-drop-deletes. As we match keys in order, when we have a range from which we can
   * drop deletes, we can set retainDeletesInOutput to false for the duration of this range only,
   * and maintain consistency.
   */
  private void checkPartialDropDeleteRange(Cell curCell) {
    // If partial-drop-deletes are used, initially, dropDeletesFromRow and dropDeletesToRow
    // are both set, and the matcher is set to retain deletes. We assume ordered keys. When
    // dropDeletesFromRow is leq current kv, we start dropping deletes and reset
    // dropDeletesFromRow; thus the 2nd "if" starts to apply.
    if ((dropDeletesFromRow != null)
        && (Arrays.equals(dropDeletesFromRow, HConstants.EMPTY_START_ROW)
            || (CellComparator.COMPARATOR.compareRows(curCell, dropDeletesFromRow, 0,
              dropDeletesFromRow.length) >= 0))) {
      retainDeletesInOutput = false;
      dropDeletesFromRow = null;
    }
    // If dropDeletesFromRow is null and dropDeletesToRow is set, we are inside the partial-
    // drop-deletes range. When dropDeletesToRow is leq current kv, we stop dropping deletes,
    // and reset dropDeletesToRow so that we don't do any more compares.
    if ((dropDeletesFromRow == null) && (dropDeletesToRow != null)
        && !Arrays.equals(dropDeletesToRow, HConstants.EMPTY_END_ROW) && (CellComparator.COMPARATOR
            .compareRows(curCell, dropDeletesToRow, 0, dropDeletesToRow.length) >= 0)) {
      retainDeletesInOutput = true;
      dropDeletesToRow = null;
    }
  }

  @Override
  protected void reset() {
    checkPartialDropDeleteRange(currentRow);
  }

  @Override
  public boolean isUserScan() {
    return false;
  }

  @Override
  public boolean moreRowsMayExistAfter(Cell cell) {
    if (this.stopRow == null || this.stopRow.length == 0) {
      return true;
    }
    return rowComparator.compareRows(cell, stopRow, 0, stopRow.length) < 0;
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

  public static LegacyScanQueryMatcher create(Scan scan, ScanInfo scanInfo,
      NavigableSet<byte[]> columns, ScanType scanType, long readPointToUse, long earliestPutTs,
      long oldestUnexpiredTS, long now, byte[] dropDeletesFromRow, byte[] dropDeletesToRow,
      RegionCoprocessorHost regionCoprocessorHost) throws IOException {
    int maxVersions = Math.min(scan.getMaxVersions(), scanInfo.getMaxVersions());
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
    DeleteTracker deletes = instantiateDeleteTracker(regionCoprocessorHost);
    if (dropDeletesFromRow == null) {
      return new LegacyScanQueryMatcher(scan, scanInfo, columnTracker, hasNullColumn, deletes,
          scanType, readPointToUse, earliestPutTs, oldestUnexpiredTS, now);
    } else {
      return new LegacyScanQueryMatcher(scan, scanInfo, columnTracker, hasNullColumn, deletes,
          scanType, readPointToUse, earliestPutTs, oldestUnexpiredTS, now, dropDeletesFromRow,
          dropDeletesToRow);
    }
  }
}
