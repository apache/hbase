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
import java.util.NavigableSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * A query matcher that is specifically designed for the scan case.
 */
public class ScanQueryMatcher {
  // Optimization so we can skip lots of compares when we decide to skip
  // to the next row.
  private boolean stickyNextRow;
  private final byte[] stopRow;

  private final TimeRange tr;

  private final Filter filter;

  /** Keeps track of deletes */
  private final DeleteTracker deletes;

  /*
   * The following three booleans define how we deal with deletes.
   * There are three different aspects:
   * 1. Whether to keep delete markers. This is used in compactions.
   *    Minor compactions always keep delete markers.
   * 2. Whether to keep deleted rows. This is also used in compactions,
   *    if the store is set to keep deleted rows. This implies keeping
   *    the delete markers as well.
   *    In this case deleted rows are subject to the normal max version
   *    and TTL/min version rules just like "normal" rows.
   * 3. Whether a scan can do time travel queries even before deleted
   *    marker to reach deleted rows.
   */
  /** whether to retain delete markers */
  private final boolean retainDeletesInOutput;
  /** whether to return deleted rows */
  private final boolean keepDeletedCells;
  /** whether time range queries can see rows "behind" a delete */
  private final boolean seePastDeleteMarkers;


  /** Keeps track of columns and versions */
  private final ColumnTracker columns;

  /** Key to seek to in memstore and StoreFiles */
  private final KeyValue startKey;

  /** Row comparator for the region this query is for */
  private final KeyValue.KeyComparator rowComparator;

  /* row is not private for tests */
  /** Row the query is on */
  byte [] row;
  int rowOffset;
  short rowLength;
  
  /**
   * Oldest put in any of the involved store files
   * Used to decide whether it is ok to delete
   * family delete marker of this store keeps
   * deleted KVs.
   */
  private final long earliestPutTs;

  /** readPoint over which the KVs are unconditionally included */
  protected long maxReadPointToTrackVersions;

  /**
   * This variable shows whether there is an null column in the query. There
   * always exists a null column in the wildcard column query.
   * There maybe exists a null column in the explicit column query based on the
   * first column.
   * */
  private boolean hasNullColumn = true;

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
  // influence.  may appear out of order.
  private final long timeToPurgeDeletes;
  
  private final boolean isUserScan;

  /**
   * Construct a QueryMatcher for a scan
   * @param scan
   * @param scanInfo The store's immutable scan info
   * @param columns
   * @param scanType Type of the scan
   * @param earliestPutTs Earliest put seen in any of the store files.
   * @param oldestUnexpiredTS the oldest timestamp we are interested in,
   *  based on TTL
   */
  public ScanQueryMatcher(Scan scan, Store.ScanInfo scanInfo,
      NavigableSet<byte[]> columns, ScanType scanType,
      long readPointToUse, long earliestPutTs, long oldestUnexpiredTS) {
    this.tr = scan.getTimeRange();
    this.rowComparator = scanInfo.getComparator().getRawComparator();
    this.deletes =  new ScanDeleteTracker();
    this.stopRow = scan.getStopRow();
    this.startKey = KeyValue.createFirstDeleteFamilyOnRow(scan.getStartRow(),
        scanInfo.getFamily());
    this.filter = scan.getFilter();
    this.earliestPutTs = earliestPutTs;
    this.maxReadPointToTrackVersions = readPointToUse;
    this.timeToPurgeDeletes = scanInfo.getTimeToPurgeDeletes();

    /* how to deal with deletes */
    this.isUserScan = scanType == ScanType.USER_SCAN;
    // keep deleted cells: if compaction or raw scan
    this.keepDeletedCells = (scanInfo.getKeepDeletedCells() && !isUserScan) || scan.isRaw();
    // retain deletes: if minor compaction or raw scan
    this.retainDeletesInOutput = scanType == ScanType.MINOR_COMPACT || scan.isRaw();
    // seePastDeleteMarker: user initiated scans
    this.seePastDeleteMarkers = scanInfo.getKeepDeletedCells() && isUserScan;

    int maxVersions =
        scan.isRaw() ? scan.getMaxVersions() : Math.min(scan.getMaxVersions(),
          scanInfo.getMaxVersions());

    // Single branch to deal with two types of reads (columns vs all in family)
    if (columns == null || columns.size() == 0) {
      // there is always a null column in the wildcard column query.
      hasNullColumn = true;

      // use a specialized scan for wildcard column tracker.
      this.columns = new ScanWildcardColumnTracker(
          scanInfo.getMinVersions(), maxVersions, oldestUnexpiredTS);
    } else {
      // whether there is null column in the explicit column query
      hasNullColumn = (columns.first().length == 0);

      // We can share the ExplicitColumnTracker, diff is we reset
      // between rows, not between storefiles.
      byte[] attr = scan.getAttribute(Scan.HINT_LOOKAHEAD);
      this.columns = new ExplicitColumnTracker(columns, scanInfo.getMinVersions(), maxVersions,
          oldestUnexpiredTS, attr == null ? 0 : Bytes.toInt(attr));
    }
  }

  /*
   * Constructor for tests
   */
  ScanQueryMatcher(Scan scan, Store.ScanInfo scanInfo,
      NavigableSet<byte[]> columns, long oldestUnexpiredTS) {
    this(scan, scanInfo, columns, ScanType.USER_SCAN,
          Long.MAX_VALUE, /* max Readpoint to track versions */
        HConstants.LATEST_TIMESTAMP, oldestUnexpiredTS);
  }

  /**
   *
   * @return  whether there is an null column in the query
   */
  public boolean hasNullColumnInQuery() {
    return hasNullColumn;
  }

  /**
   * Determines if the caller should do one of several things:
   * - seek/skip to the next row (MatchCode.SEEK_NEXT_ROW)
   * - seek/skip to the next column (MatchCode.SEEK_NEXT_COL)
   * - include the current KeyValue (MatchCode.INCLUDE)
   * - ignore the current KeyValue (MatchCode.SKIP)
   * - got to the next row (MatchCode.DONE)
   *
   * @param kv KeyValue to check
   * @return The match code instance.
   * @throws IOException in case there is an internal consistency problem
   *      caused by a data corruption.
   */
  public MatchCode match(KeyValue kv) throws IOException {
    if (filter != null && filter.filterAllRemaining()) {
      return MatchCode.DONE_SCAN;
    }

    byte [] bytes = kv.getBuffer();
    int offset = kv.getOffset();

    int keyLength = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
    offset += KeyValue.ROW_OFFSET;

    int initialOffset = offset;

    short rowLength = Bytes.toShort(bytes, offset, Bytes.SIZEOF_SHORT);
    offset += Bytes.SIZEOF_SHORT;

    int ret = this.rowComparator.compareRows(row, this.rowOffset, this.rowLength,
        bytes, offset, rowLength);
    if (ret <= -1) {
      return MatchCode.DONE;
    } else if (ret >= 1) {
      // could optimize this, if necessary?
      // Could also be called SEEK_TO_CURRENT_ROW, but this
      // should be rare/never happens.
      return MatchCode.SEEK_NEXT_ROW;
    }

    // optimize case.
    if (this.stickyNextRow)
        return MatchCode.SEEK_NEXT_ROW;

    if (this.columns.done()) {
      stickyNextRow = true;
      return MatchCode.SEEK_NEXT_ROW;
    }

    //Passing rowLength
    offset += rowLength;

    //Skipping family
    byte familyLength = bytes [offset];
    offset += familyLength + 1;

    int qualLength = keyLength -
      (offset - initialOffset) - KeyValue.TIMESTAMP_TYPE_SIZE;

    long timestamp = Bytes.toLong(bytes, initialOffset + keyLength - KeyValue.TIMESTAMP_TYPE_SIZE);
    // check for early out based on timestamp alone
    if (columns.isDone(timestamp)) {
        return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
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
    byte type = bytes[initialOffset + keyLength - 1];
    if (kv.isDelete()) {
      if (!keepDeletedCells) {
        // first ignore delete markers if the scanner can do so, and the
        // range does not include the marker
        //
        // during flushes and compactions also ignore delete markers newer
        // than the readpoint of any open scanner, this prevents deleted
        // rows that could still be seen by a scanner from being collected
        boolean includeDeleteMarker = seePastDeleteMarkers ?
            tr.withinTimeRange(timestamp) :
            tr.withinOrAfterTimeRange(timestamp);
        if (includeDeleteMarker
            && kv.getMemstoreTS() <= maxReadPointToTrackVersions) {
          this.deletes.add(bytes, offset, qualLength, timestamp, type);
        }
        // Can't early out now, because DelFam come before any other keys
      }
      if ((!isUserScan)
          && timeToPurgeDeletes > 0
          && (EnvironmentEdgeManager.currentTimeMillis() - timestamp) <= timeToPurgeDeletes) {
        return MatchCode.INCLUDE;
      } else if (retainDeletesInOutput || kv.getMemstoreTS() > maxReadPointToTrackVersions) {
        // always include or it is not time yet to check whether it is OK
        // to purge deltes or not
        if (!isUserScan) {
          // if this is not a user scan (compaction), we can filter this deletemarker right here
          // otherwise (i.e. a "raw" scan) we fall through to normal version and timerange checking
          return MatchCode.INCLUDE;
        }
      } else if (keepDeletedCells) {
        if (timestamp < earliestPutTs) {
          // keeping delete rows, but there are no puts older than
          // this delete in the store files.
          return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
        }
        // else: fall through and do version counting on the
        // delete markers
      } else {
        return MatchCode.SKIP;
      }
      // note the following next else if...
      // delete marker are not subject to other delete markers
    } else if (!this.deletes.isEmpty()) {
      DeleteResult deleteResult = deletes.isDeleted(bytes, offset, qualLength,
          timestamp);
      switch (deleteResult) {
        case FAMILY_DELETED:
        case COLUMN_DELETED:
          return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
        case VERSION_DELETED:
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
      return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
    }

    // STEP 1: Check if the column is part of the requested columns
    MatchCode colChecker = columns.checkColumn(bytes, offset, qualLength, type);
    if (colChecker == MatchCode.INCLUDE) {
      ReturnCode filterResponse = ReturnCode.SKIP;
      // STEP 2: Yes, the column is part of the requested columns. Check if filter is present
      if (filter != null) {
        // STEP 3: Filter the key value and return if it filters out
        filterResponse = filter.filterKeyValue(kv);
        switch (filterResponse) {
        case SKIP:
          return MatchCode.SKIP;
        case NEXT_COL:
          return columns.getNextRowOrNextColumn(bytes, offset, qualLength);
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
      colChecker =
          columns.checkVersions(bytes, offset, qualLength, timestamp, type,
            kv.getMemstoreTS() > maxReadPointToTrackVersions);
      //Optimize with stickyNextRow
      stickyNextRow = colChecker == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW ? true : stickyNextRow;
      return (filterResponse == ReturnCode.INCLUDE_AND_NEXT_COL &&
          colChecker == MatchCode.INCLUDE) ? MatchCode.INCLUDE_AND_SEEK_NEXT_COL
          : colChecker;
    }
    stickyNextRow = (colChecker == MatchCode.SEEK_NEXT_ROW) ? true
        : stickyNextRow;
    return colChecker;
  }

  public boolean moreRowsMayExistAfter(KeyValue kv) {
    if (!Bytes.equals(stopRow , HConstants.EMPTY_END_ROW) &&
        rowComparator.compareRows(kv.getBuffer(),kv.getRowOffset(),
            kv.getRowLength(), stopRow, 0, stopRow.length) >= 0) {
      // KV >= STOPROW
      // then NO there is nothing left.
      return false;
    } else {
      return true;
    }
  }

  /**
   * Set current row
   * @param row
   */
  public void setRow(byte [] row, int offset, short length) {
    this.row = row;
    this.rowOffset = offset;
    this.rowLength = length;
    reset();
  }

  public void reset() {
    this.deletes.reset();
    this.columns.reset();

    stickyNextRow = false;
  }

  /**
   *
   * @return the start key
   */
  public KeyValue getStartKey() {
    return this.startKey;
  }

  /**
   *
   * @return the Filter
   */
  Filter getFilter() {
    return this.filter;
  }

  public KeyValue getNextKeyHint(KeyValue kv) {
    if (filter == null) {
      return null;
    } else {
      return filter.getNextKeyHint(kv);
    }
  }

  public KeyValue getKeyForNextColumn(KeyValue kv) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return KeyValue.createLastOnRow(
          kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
          kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
          kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
    } else {
      return KeyValue.createFirstOnRow(
          kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
          kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
          nextColumn.getBuffer(), nextColumn.getOffset(), nextColumn.getLength());
    }
  }

  public KeyValue getKeyForNextRow(KeyValue kv) {
    return KeyValue.createLastOnRow(
        kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
        null, 0, 0,
        null, 0, 0);
  }

  // Used only for testing purposes
  static MatchCode checkColumn(ColumnTracker columnTracker, byte[] bytes, int offset, int length,
      long ttl, byte type, boolean ignoreCount) throws IOException {
    MatchCode matchCode = columnTracker.checkColumn(bytes, offset, length, type);
    if (matchCode == MatchCode.INCLUDE) {
      return columnTracker.checkVersions(bytes, offset, length, ttl, type, ignoreCount);
    }
    return matchCode;
  }

  /**
   * {@link #match} return codes.  These instruct the scanner moving through
   * memstores and StoreFiles what to do with the current KeyValue.
   * <p>
   * Additionally, this contains "early-out" language to tell the scanner to
   * move on to the next File (memstore or Storefile), or to return immediately.
   */
  public static enum MatchCode {
    /**
     * Include KeyValue in the returned result
     */
    INCLUDE,

    /**
     * Do not include KeyValue in the returned result
     */
    SKIP,

    /**
     * Do not include, jump to next StoreFile or memstore (in time order)
     */
    NEXT,

    /**
     * Do not include, return current result
     */
    DONE,

    /**
     * These codes are used by the ScanQueryMatcher
     */

    /**
     * Done with the row, seek there.
     */
    SEEK_NEXT_ROW,
    /**
     * Done with column, seek to next.
     */
    SEEK_NEXT_COL,

    /**
     * Done with scan, thanks to the row filter.
     */
    DONE_SCAN,

    /*
     * Seek to next key which is given as hint.
     */
    SEEK_NEXT_USING_HINT,

    /**
     * Include KeyValue and done with column, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_COL,

    /**
     * Include KeyValue and done with row, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_ROW,
  }
}
