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
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker.DeleteResult;

/**
 * A query matcher that is specifically designed for the scan case.
 */
@InterfaceAudience.Private
public abstract class ScanQueryMatcher {

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
  protected final KVComparator rowComparator;

  /** Key to seek to in memstore and StoreFiles */
  protected final Cell startKey;

  /** Keeps track of columns and versions */
  protected final ColumnTracker columns;

  /** The oldest timestamp we are interested in, based on TTL */
  protected final long oldestUnexpiredTS;

  protected final long now;

  /** Row the query is on */
  protected Cell currentRow;

  protected boolean stickyNextRow;

  protected ScanQueryMatcher(byte[] startRow, ScanInfo scanInfo, ColumnTracker columns,
      long oldestUnexpiredTS, long now) {
    this.rowComparator = scanInfo.getComparator();
    this.startKey = KeyValueUtil.createFirstDeleteFamilyOnRow(startRow, scanInfo.getFamily());
    this.oldestUnexpiredTS = oldestUnexpiredTS;
    this.now = now;
    this.columns = columns;
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
    // optimize case.
    if (this.stickyNextRow) {
      return MatchCode.SEEK_NEXT_ROW;
    }

    if (this.columns.done()) {
      stickyNextRow = true;
      return MatchCode.SEEK_NEXT_ROW;
    }

    long timestamp = cell.getTimestamp();
    // check if this is a fake cell. The fake cell is an optimization, we should make the scanner
    // seek to next column or next row. See StoreFileScanner.requestSeek for more details.
    // check for early out based on timestamp alone
    if (timestamp == HConstants.OLDEST_TIMESTAMP || columns.isDone(timestamp)) {
      return columns.getNextRowOrNextColumn(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
    }
    // check if the cell is expired by cell TTL
    if (HStore.isCellTTLExpired(cell, this.oldestUnexpiredTS, this.now)) {
      return MatchCode.SKIP;
    }
    return null;
  }

  protected final MatchCode checkDeleted(DeleteTracker deletes, Cell cell) {
    if (deletes.isEmpty()) {
      return null;
    }
    DeleteResult deleteResult = deletes.isDeleted(cell);
    switch (deleteResult) {
      case FAMILY_DELETED:
      case COLUMN_DELETED:
        return columns.getNextRowOrNextColumn(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
      case VERSION_DELETED:
      case FAMILY_VERSION_DELETED:
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
    stickyNextRow = false;
  }

  public abstract boolean isUserScan();

  /**
   * @return Returns false if we know there are no more rows to be scanned (We've reached the
   *         <code>stopRow</code> or we are scanning on row only because this Scan is for a Get,
   *         etc.
   */
  public abstract boolean moreRowsMayExistAfter(Cell cell);

  public Cell getKeyForNextColumn(Cell cell) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return KeyValueUtil.createLastOnRow(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    } else {
      return KeyValueUtil.createFirstOnRow(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        nextColumn.getBuffer(), nextColumn.getOffset(), nextColumn.getLength());
    }
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param currentCell The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextRow(Cell nextIndexed, Cell currentCell) {
    return rowComparator.compareKey(nextIndexed, currentCell.getRowArray(),
      currentCell.getRowOffset(), currentCell.getRowLength(), null, 0, 0, null, 0, 0,
      HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param currentCell The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextColumn(Cell nextIndexed, Cell currentCell) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return rowComparator.compareKey(nextIndexed, currentCell.getRowArray(),
        currentCell.getRowOffset(), currentCell.getRowLength(), currentCell.getFamilyArray(),
        currentCell.getFamilyOffset(), currentCell.getFamilyLength(),
        currentCell.getQualifierArray(), currentCell.getQualifierOffset(),
        currentCell.getQualifierLength(), HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
    } else {
      return rowComparator.compareKey(nextIndexed, currentCell.getRowArray(),
        currentCell.getRowOffset(), currentCell.getRowLength(), currentCell.getFamilyArray(),
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

  protected static DeleteTracker instantiateDeleteTracker(RegionCoprocessorHost host)
      throws IOException {
    DeleteTracker tracker = new ScanDeleteTracker();
    if (host != null) {
      tracker = host.postInstantiateDeleteTracker(tracker);
    }
    return tracker;
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
}
