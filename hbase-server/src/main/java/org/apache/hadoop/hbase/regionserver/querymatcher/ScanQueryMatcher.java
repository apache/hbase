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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.DeleteTracker;
import org.apache.hadoop.hbase.regionserver.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;

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

  protected ScanQueryMatcher(Cell startKey, ScanInfo scanInfo, ColumnTracker columns,
      long oldestUnexpiredTS, long now) {
    this.rowComparator = scanInfo.getComparator();
    this.startKey = startKey;
    this.oldestUnexpiredTS = oldestUnexpiredTS;
    this.now = now;
    this.columns = columns;
  }

  protected static Cell createStartKeyFromRow(byte[] startRow, ScanInfo scanInfo) {
    return KeyValueUtil.createFirstDeleteFamilyOnRow(startRow, scanInfo.getFamily());
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
      Cell nextKey = createNextOnRowCol(cell);
      if (nextKey != cell) {
        return nextKey;
      }
      // The cell is at the end of row/family/qualifier, so it is impossible to find any DeleteFamily cells.
      // Let us seek to next column.
    }
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

  /**
   * @return An new cell is located following input cell. If both of type and timestamp are
   *         minimum, the input cell will be returned directly.
   */
  private static Cell createNextOnRowCol(Cell cell) {
    long ts = cell.getTimestamp();
    byte type = cell.getTypeByte();
    if (type != Type.Minimum.getCode()) {
      type = KeyValue.Type.values()[KeyValue.Type.codeToType(type).ordinal() - 1].getCode();
    } else if (ts != HConstants.OLDEST_TIMESTAMP) {
      ts = ts - 1;
      type = Type.Maximum.getCode();
    } else {
      return cell;
    }
    return createNextOnRowCol(cell, ts, type);
  }

  private static Cell createNextOnRowCol(final Cell cell, final long ts, final byte type) {
    return new Cell() {
      @Override
      public byte[] getRowArray() { return cell.getRowArray(); }

      @Override
      public int getRowOffset() { return cell.getRowOffset(); }

      @Override
      public short getRowLength() { return cell.getRowLength(); }

      @Override
      public byte[] getFamilyArray() { return cell.getFamilyArray(); }

      @Override
      public int getFamilyOffset() { return cell.getFamilyOffset(); }

      @Override
      public byte getFamilyLength() { return cell.getFamilyLength(); }

      @Override
      public byte[] getQualifierArray() { return cell.getQualifierArray(); }

      @Override
      public int getQualifierOffset() { return cell.getQualifierOffset(); }

      @Override
      public int getQualifierLength() { return cell.getQualifierLength(); }

      @Override
      public long getTimestamp() { return ts; }

      @Override
      public byte getTypeByte() {return type; }

      @Override
      public long getMvccVersion() { return cell.getMvccVersion(); }

      @Override
      public long getSequenceId() { return cell.getSequenceId(); }

      @Override
      public byte[] getValueArray() { return cell.getValueArray(); }

      @Override
      public int getValueOffset() { return cell.getValueOffset(); }

      @Override
      public int getValueLength() { return cell.getValueLength(); }

      @Override
      public byte[] getTagsArray() { return cell.getTagsArray(); }

      @Override
      public int getTagsOffset() { return cell.getTagsOffset(); }

      @Override
      public int getTagsLength() { return cell.getTagsLength(); }

      @Override
      public byte[] getValue() { return cell.getValue(); }

      @Override
      public byte[] getFamily() { return cell.getFamily(); }

      @Override
      public byte[] getQualifier() { return cell.getQualifier(); }

      @Override
      public byte[] getRow() {return cell.getRow(); }
    };
  }
}
