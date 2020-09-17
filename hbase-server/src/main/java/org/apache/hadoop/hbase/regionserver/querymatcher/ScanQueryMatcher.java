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
import java.util.Iterator;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.security.visibility.VisibilityNewVersionBehaivorTracker;
import org.apache.hadoop.hbase.security.visibility.VisibilityScanDeleteTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A query matcher that is specifically designed for the scan case.
 */
@InterfaceAudience.Private
public abstract class ScanQueryMatcher implements ShipperListener {

  /**
   * {@link #match} return codes. These instruct the scanner moving through memstores and StoreFiles
   * what to do with the current KeyValue.
   * <p>
   * Additionally, this contains "early-out" language to tell the scanner to move on to the next
   * File (memstore or Storefile), or to return immediately.
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

    /**
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

  /** Row comparator for the region this query is for */
  protected final CellComparator rowComparator;

  /** Key to seek to in memstore and StoreFiles */
  protected final Cell startKey;

  /** Keeps track of columns and versions */
  protected final ColumnTracker columns;

  /** The oldest timestamp we are interested in, based on TTL */
  protected final long oldestUnexpiredTS;

  protected final long now;

  /** Row the query is on */
  protected Cell currentRow;

  protected ScanQueryMatcher(Cell startKey, ScanInfo scanInfo, ColumnTracker columns,
      long oldestUnexpiredTS, long now) {
    this.rowComparator = scanInfo.getComparator();
    this.startKey = startKey;
    this.oldestUnexpiredTS = oldestUnexpiredTS;
    this.now = now;
    this.columns = columns;
  }

  /**
   * @param cell
   * @param oldestTimestamp
   * @return true if the cell is expired
   */
  private static boolean isCellTTLExpired(final Cell cell, final long oldestTimestamp,
      final long now) {
    // Look for a TTL tag first. Use it instead of the family setting if
    // found. If a cell has multiple TTLs, resolve the conflict by using the
    // first tag encountered.
    Iterator<Tag> i = PrivateCellUtil.tagsIterator(cell);
    while (i.hasNext()) {
      Tag t = i.next();
      if (TagType.TTL_TAG_TYPE == t.getType()) {
        // Unlike in schema cell TTLs are stored in milliseconds, no need
        // to convert
        long ts = cell.getTimestamp();
        assert t.getValueLength() == Bytes.SIZEOF_LONG;
        long ttl = Tag.getValueAsLong(t);
        if (ts + ttl < now) {
          return true;
        }
        // Per cell TTLs cannot extend lifetime beyond family settings, so
        // fall through to check that
        break;
      }
    }
    return false;
  }

  /**
   * Check before the delete logic.
   * @return null means continue.
   */
  protected final MatchCode preCheck(Cell cell) {
    if (currentRow == null) {
      // Since the curCell is null it means we are already sure that we have moved over to the next
      // row
      return MatchCode.DONE;
    }
    // if row key is changed, then we know that we have moved over to the next row
    if (rowComparator.compareRows(currentRow, cell) != 0) {
      return MatchCode.DONE;
    }

    if (this.columns.done()) {
      return MatchCode.SEEK_NEXT_ROW;
    }

    long timestamp = cell.getTimestamp();
    // check if this is a fake cell. The fake cell is an optimization, we should make the scanner
    // seek to next column or next row. See StoreFileScanner.requestSeek for more details.
    // check for early out based on timestamp alone
    if (timestamp == HConstants.OLDEST_TIMESTAMP || columns.isDone(timestamp)) {
      return columns.getNextRowOrNextColumn(cell);
    }
    // check if the cell is expired by cell TTL
    if (isCellTTLExpired(cell, this.oldestUnexpiredTS, this.now)) {
      return MatchCode.SKIP;
    }
    return null;
  }

  protected final MatchCode checkDeleted(DeleteTracker deletes, Cell cell) {
    if (deletes.isEmpty() && !(deletes instanceof NewVersionBehaviorTracker)) {
      return null;
    }
    // MvccSensitiveTracker always need check all cells to save some infos.
    DeleteResult deleteResult = deletes.isDeleted(cell);
    switch (deleteResult) {
      case FAMILY_DELETED:
      case COLUMN_DELETED:
        if (!(deletes instanceof NewVersionBehaviorTracker)) {
          // MvccSensitive can not seek to next because the Put with lower ts may have higher mvcc
          return columns.getNextRowOrNextColumn(cell);
        }
      case VERSION_DELETED:
      case FAMILY_VERSION_DELETED:
      case VERSION_MASKED:
        return MatchCode.SKIP;
      case NOT_DELETED:
        return null;
      default:
        throw new RuntimeException("Unexpected delete result: " + deleteResult);
    }
  }


  /**
   * Determines if the caller should do one of several things:
   * <ul>
   * <li>seek/skip to the next row (MatchCode.SEEK_NEXT_ROW)</li>
   * <li>seek/skip to the next column (MatchCode.SEEK_NEXT_COL)</li>
   * <li>include the current KeyValue (MatchCode.INCLUDE)</li>
   * <li>ignore the current KeyValue (MatchCode.SKIP)</li>
   * <li>got to the next row (MatchCode.DONE)</li>
   * </ul>
   * @param cell KeyValue to check
   * @return The match code instance.
   * @throws IOException in case there is an internal consistency problem caused by a data
   *           corruption.
   */
  public abstract MatchCode match(Cell cell) throws IOException;

  /**
   * @return the start key
   */
  public Cell getStartKey() {
    return startKey;
  }

  /**
   * @return whether there is an null column in the query
   */
  public abstract boolean hasNullColumnInQuery();

  /**
   * @return a cell represent the current row
   */
  public Cell currentRow() {
    return currentRow;
  }

  /**
   * Make {@link #currentRow()} return null.
   */
  public void clearCurrentRow() {
    currentRow = null;
  }

  protected abstract void reset();

  /**
   * Set the row when there is change in row
   * @param currentRow
   */
  public void setToNewRow(Cell currentRow) {
    this.currentRow = currentRow;
    columns.reset();
    reset();
  }

  public abstract boolean isUserScan();

  /**
   * @return Returns false if we know there are no more rows to be scanned (We've reached the
   *         <code>stopRow</code> or we are scanning on row only because this Scan is for a Get,
   *         etc.
   */
  public abstract boolean moreRowsMayExistAfter(Cell cell);

  public Cell getKeyForNextColumn(Cell cell) {
    // We aren't sure whether any DeleteFamily cells exist, so we can't skip to next column.
    // TODO: Current way disable us to seek to next column quickly. Is there any better solution?
    // see HBASE-18471 for more details
    // see TestFromClientSide3#testScanAfterDeletingSpecifiedRow
    // see TestFromClientSide3#testScanAfterDeletingSpecifiedRowV2
    if (cell.getQualifierLength() == 0) {
      Cell nextKey = PrivateCellUtil.createNextOnRowCol(cell);
      if (nextKey != cell) {
        return nextKey;
      }
      // The cell is at the end of row/family/qualifier, so it is impossible to find any DeleteFamily cells.
      // Let us seek to next column.
    }
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return PrivateCellUtil.createLastOnRowCol(cell);
    } else {
      return PrivateCellUtil.createFirstOnRowCol(cell, nextColumn.getBuffer(),
        nextColumn.getOffset(), nextColumn.getLength());
    }
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param currentCell The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextRow(Cell nextIndexed, Cell currentCell) {
    return PrivateCellUtil.compareKeyBasedOnColHint(rowComparator, nextIndexed, currentCell, 0, 0, null, 0,
      0, HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param currentCell The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextColumn(Cell nextIndexed, Cell currentCell) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return PrivateCellUtil.compareKeyBasedOnColHint(rowComparator, nextIndexed, currentCell, 0, 0, null,
        0, 0, HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
    } else {
      return PrivateCellUtil.compareKeyBasedOnColHint(rowComparator, nextIndexed, currentCell,
        currentCell.getFamilyOffset(), currentCell.getFamilyLength(), nextColumn.getBuffer(),
        nextColumn.getOffset(), nextColumn.getLength(), HConstants.LATEST_TIMESTAMP,
        Type.Maximum.getCode());
    }
  }

  /**
   * @return the Filter
   */
  public abstract Filter getFilter();

  /**
   * Delegate to {@link Filter#getNextCellHint(Cell)}. If no filter, return {@code null}.
   */
  public abstract Cell getNextKeyHint(Cell cell) throws IOException;

  @Override
  public void beforeShipped() throws IOException {
    if (this.currentRow != null) {
      this.currentRow = PrivateCellUtil.createFirstOnRow(CellUtil.copyRow(this.currentRow));
    }
    if (columns != null) {
      columns.beforeShipped();
    }
  }

  protected static Cell createStartKeyFromRow(byte[] startRow, ScanInfo scanInfo) {
    return PrivateCellUtil.createFirstDeleteFamilyCellOnRow(startRow, scanInfo.getFamily());
  }

  protected static Pair<DeleteTracker, ColumnTracker> getTrackers(RegionCoprocessorHost host,
      NavigableSet<byte[]> columns, ScanInfo scanInfo, long oldestUnexpiredTS, Scan userScan)
      throws IOException {
    int resultMaxVersion = scanInfo.getMaxVersions();
    int maxVersionToCheck = resultMaxVersion;
    if (userScan != null) {
      if (userScan.isRaw()) {
        resultMaxVersion = userScan.getMaxVersions();
        maxVersionToCheck = userScan.hasFilter() ? Integer.MAX_VALUE : resultMaxVersion;
      } else {
        resultMaxVersion = Math.min(userScan.getMaxVersions(), scanInfo.getMaxVersions());
        maxVersionToCheck = userScan.hasFilter() ? scanInfo.getMaxVersions() : resultMaxVersion;
      }
    }

    DeleteTracker deleteTracker;
    if (scanInfo.isNewVersionBehavior() && (userScan == null || !userScan.isRaw())) {
      deleteTracker = new NewVersionBehaviorTracker(columns, scanInfo.getComparator(),
          scanInfo.getMinVersions(), scanInfo.getMaxVersions(), resultMaxVersion,
          oldestUnexpiredTS);
    } else {
      deleteTracker = new ScanDeleteTracker(scanInfo.getComparator());
    }
    if (host != null) {
      deleteTracker = host.postInstantiateDeleteTracker(deleteTracker);
      if (deleteTracker instanceof VisibilityScanDeleteTracker && scanInfo.isNewVersionBehavior()) {
        deleteTracker = new VisibilityNewVersionBehaivorTracker(columns, scanInfo.getComparator(),
            scanInfo.getMinVersions(), scanInfo.getMaxVersions(), resultMaxVersion,
            oldestUnexpiredTS);
      }
    }

    ColumnTracker columnTracker;

    if (deleteTracker instanceof NewVersionBehaviorTracker) {
      columnTracker = (NewVersionBehaviorTracker) deleteTracker;
    } else if (columns == null || columns.size() == 0) {
      columnTracker = new ScanWildcardColumnTracker(scanInfo.getMinVersions(), maxVersionToCheck,
          oldestUnexpiredTS, scanInfo.getComparator());
    } else {
      columnTracker = new ExplicitColumnTracker(columns, scanInfo.getMinVersions(),
        maxVersionToCheck, oldestUnexpiredTS);
    }
    return new Pair<>(deleteTracker, columnTracker);
  }

  // Used only for testing purposes
  static MatchCode checkColumn(ColumnTracker columnTracker, byte[] bytes, int offset, int length,
      long ttl, byte type, boolean ignoreCount) throws IOException {
    KeyValue kv = KeyValueUtil.createFirstOnRow(HConstants.EMPTY_BYTE_ARRAY, 0, 0,
      HConstants.EMPTY_BYTE_ARRAY, 0, 0, bytes, offset, length);
    MatchCode matchCode = columnTracker.checkColumn(kv, type);
    if (matchCode == MatchCode.INCLUDE) {
      return columnTracker.checkVersions(kv, ttl, type, ignoreCount);
    }
    return matchCode;
  }
}
