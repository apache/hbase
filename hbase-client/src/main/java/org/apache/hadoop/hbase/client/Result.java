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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Single row result of a {@link Get} or {@link Scan} query.<p>
 *
 * This class is <b>NOT THREAD SAFE</b>.<p>
 *
 * Convenience methods are available that return various {@link Map}
 * structures and values directly.<p>
 *
 * To get a complete mapping of all cells in the Result, which can include
 * multiple families and multiple versions, use {@link #getMap()}.<p>
 *
 * To get a mapping of each family to its columns (qualifiers and values),
 * including only the latest version of each, use {@link #getNoVersionMap()}.
 *
 * To get a mapping of qualifiers to latest values for an individual family use
 * {@link #getFamilyMap(byte[])}.<p>
 *
 * To get the latest value for a specific family and qualifier use
 * {@link #getValue(byte[], byte[])}.
 *
 * A Result is backed by an array of {@link Cell} objects, each representing
 * an HBase cell defined by the row, family, qualifier, timestamp, and value.<p>
 *
 * The underlying {@link Cell} objects can be accessed through the method {@link #listCells()}.
 * This will create a List from the internal Cell []. Better is to exploit the fact that
 * a new Result instance is a primed {@link CellScanner}; just call {@link #advance()} and
 * {@link #current()} to iterate over Cells as you would any {@link CellScanner}.
 * Call {@link #cellScanner()} to reset should you need to iterate the same Result over again
 * ({@link CellScanner}s are one-shot).
 *
 * If you need to overwrite a Result with another Result instance -- as in the old 'mapred'
 * RecordReader next invocations -- then create an empty Result with the null constructor and
 * in then use {@link #copyFrom(Result)}
 */
@InterfaceAudience.Public
public class Result implements CellScannable, CellScanner {
  private Cell[] cells;
  private Boolean exists; // if the query was just to check existence.
  private boolean stale = false;

  /**
   * See {@link #mayHaveMoreCellsInRow()}.
   */
  private boolean mayHaveMoreCellsInRow = false;
  // We're not using java serialization.  Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  private transient byte [] row = null;
  // Ditto for familyMap.  It can be composed on fly from passed in kvs.
  private transient NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      familyMap = null;

  private static ThreadLocal<byte[]> localBuffer = new ThreadLocal<>();
  private static final int PAD_WIDTH = 128;
  public static final Result EMPTY_RESULT = new Result(true);

  private final static int INITIAL_CELLSCANNER_INDEX = -1;

  /**
   * Index for where we are when Result is acting as a {@link CellScanner}.
   */
  private int cellScannerIndex = INITIAL_CELLSCANNER_INDEX;
  private RegionLoadStats stats;

  private final boolean readonly;

  private Cursor cursor = null;

  /**
   * Creates an empty Result w/ no KeyValue payload; returns null if you call {@link #rawCells()}.
   * Use this to represent no results if {@code null} won't do or in old 'mapred' as opposed
   * to 'mapreduce' package MapReduce where you need to overwrite a Result instance with a
   * {@link #copyFrom(Result)} call.
   */
  public Result() {
    this(false);
  }

  /**
   * Allows to construct special purpose immutable Result objects,
   * such as EMPTY_RESULT.
   * @param readonly whether this Result instance is readonly
   */
  private Result(boolean readonly) {
    this.readonly = readonly;
  }

  /**
   * Instantiate a Result with the specified List of KeyValues.
   * <br><strong>Note:</strong> You must ensure that the keyvalues are already sorted.
   * @param cells List of cells
   */
  public static Result create(List<Cell> cells) {
    return create(cells, null);
  }

  public static Result create(List<Cell> cells, Boolean exists) {
    return create(cells, exists, false);
  }

  public static Result create(List<Cell> cells, Boolean exists, boolean stale) {
    return create(cells, exists, stale, false);
  }

  public static Result create(List<Cell> cells, Boolean exists, boolean stale,
      boolean mayHaveMoreCellsInRow) {
    if (exists != null){
      return new Result(null, exists, stale, mayHaveMoreCellsInRow);
    }
    return new Result(cells.toArray(new Cell[cells.size()]), null, stale, mayHaveMoreCellsInRow);
  }

  /**
   * Instantiate a Result with the specified array of KeyValues.
   * <br><strong>Note:</strong> You must ensure that the keyvalues are already sorted.
   * @param cells array of cells
   */
  public static Result create(Cell[] cells) {
    return create(cells, null, false);
  }

  public static Result create(Cell[] cells, Boolean exists, boolean stale) {
    return create(cells, exists, stale, false);
  }

  public static Result create(Cell[] cells, Boolean exists, boolean stale,
      boolean mayHaveMoreCellsInRow) {
    if (exists != null) {
      return new Result(null, exists, stale, mayHaveMoreCellsInRow);
    }
    return new Result(cells, null, stale, mayHaveMoreCellsInRow);
  }

  public static Result createCursorResult(Cursor cursor) {
    return new Result(cursor);
  }

  private Result(Cursor cursor) {
    this.cursor = cursor;
    this.readonly = false;
  }

  /** Private ctor. Use {@link #create(Cell[])}. */
  private Result(Cell[] cells, Boolean exists, boolean stale, boolean mayHaveMoreCellsInRow) {
    this.cells = cells;
    this.exists = exists;
    this.stale = stale;
    this.mayHaveMoreCellsInRow = mayHaveMoreCellsInRow;
    this.readonly = false;
  }

  /**
   * Method for retrieving the row key that corresponds to
   * the row from which this Result was created.
   * @return row
   */
  public byte [] getRow() {
    if (this.row == null) {
      this.row = (this.cells == null || this.cells.length == 0) ?
          null :
          CellUtil.cloneRow(this.cells[0]);
    }
    return this.row;
  }

  /**
   * Return the array of Cells backing this Result instance.
   *
   * The array is sorted from smallest -&gt; largest using the
   * {@link CellComparator}.
   *
   * The array only contains what your Get or Scan specifies and no more.
   * For example if you request column "A" 1 version you will have at most 1
   * Cell in the array. If you request column "A" with 2 version you will
   * have at most 2 Cells, with the first one being the newer timestamp and
   * the second being the older timestamp (this is the sort order defined by
   * {@link CellComparator}).  If columns don't exist, they won't be
   * present in the result. Therefore if you ask for 1 version all columns,
   * it is safe to iterate over this array and expect to see 1 Cell for
   * each column and no more.
   *
   * This API is faster than using getFamilyMap() and getMap()
   *
   * @return array of Cells; can be null if nothing in the result
   */
  public Cell[] rawCells() {
    return cells;
  }

  /**
   * Create a sorted list of the Cell's in this result.
   *
   * Since HBase 0.20.5 this is equivalent to raw().
   *
   * @return sorted List of Cells; can be null if no cells in the result
   */
  public List<Cell> listCells() {
    return isEmpty()? null: Arrays.asList(rawCells());
  }

  /**
   * Return the Cells for the specific column.  The Cells are sorted in
   * the {@link CellComparator} order.  That implies the first entry in
   * the list is the most recent column.  If the query (Scan or Get) only
   * requested 1 version the list will contain at most 1 entry.  If the column
   * did not exist in the result set (either the column does not exist
   * or the column was not selected in the query) the list will be empty.
   *
   * Also see getColumnLatest which returns just a Cell
   *
   * @param family the family
   * @param qualifier
   * @return a list of Cells for this column or empty list if the column
   * did not exist in the result set
   */
  public List<Cell> getColumnCells(byte [] family, byte [] qualifier) {
    List<Cell> result = new ArrayList<>();

    Cell [] kvs = rawCells();

    if (kvs == null || kvs.length == 0) {
      return result;
    }
    int pos = binarySearch(kvs, family, qualifier);
    if (pos == -1) {
      return result; // cant find it
    }

    for (int i = pos; i < kvs.length; i++) {
      if (CellUtil.matchingColumn(kvs[i], family,qualifier)) {
        result.add(kvs[i]);
      } else {
        break;
      }
    }

    return result;
  }

  private byte[] notNullBytes(final byte[] bytes) {
    if (bytes == null) {
      return HConstants.EMPTY_BYTE_ARRAY;
    } else {
      return bytes;
    }
  }

  protected int binarySearch(final Cell [] kvs,
                             final byte [] family,
                             final byte [] qualifier) {
    byte[] familyNotNull = notNullBytes(family);
    byte[] qualifierNotNull = notNullBytes(qualifier);
    Cell searchTerm =
        PrivateCellUtil.createFirstOnRow(kvs[0].getRowArray(),
            kvs[0].getRowOffset(), kvs[0].getRowLength(),
            familyNotNull, 0, (byte)familyNotNull.length,
            qualifierNotNull, 0, qualifierNotNull.length);

    // pos === ( -(insertion point) - 1)
    int pos = Arrays.binarySearch(kvs, searchTerm, CellComparator.getInstance());
    // never will exact match
    if (pos < 0) {
      pos = (pos+1) * -1;
      // pos is now insertion point
    }
    if (pos == kvs.length) {
      return -1; // doesn't exist
    }
    return pos;
  }

  /**
   * Searches for the latest value for the specified column.
   *
   * @param kvs the array to search
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return the index where the value was found, or -1 otherwise
   */
  protected int binarySearch(final Cell [] kvs,
      final byte [] family, final int foffset, final int flength,
      final byte [] qualifier, final int qoffset, final int qlength) {

    double keyValueSize = (double)
        KeyValue.getKeyValueDataStructureSize(kvs[0].getRowLength(), flength, qlength, 0);

    byte[] buffer = localBuffer.get();
    if (buffer == null || keyValueSize > buffer.length) {
      // pad to the smallest multiple of the pad width
      buffer = new byte[(int) Math.ceil(keyValueSize / PAD_WIDTH) * PAD_WIDTH];
      localBuffer.set(buffer);
    }

    Cell searchTerm = KeyValueUtil.createFirstOnRow(buffer, 0,
        kvs[0].getRowArray(), kvs[0].getRowOffset(), kvs[0].getRowLength(),
        family, foffset, flength,
        qualifier, qoffset, qlength);

    // pos === ( -(insertion point) - 1)
    int pos = Arrays.binarySearch(kvs, searchTerm, CellComparator.getInstance());
    // never will exact match
    if (pos < 0) {
      pos = (pos+1) * -1;
      // pos is now insertion point
    }
    if (pos == kvs.length) {
      return -1; // doesn't exist
    }
    return pos;
  }

  /**
   * The Cell for the most recent timestamp for a given column.
   *
   * @param family
   * @param qualifier
   *
   * @return the Cell for the column, or null if no value exists in the row or none have been
   * selected in the query (Get/Scan)
   */
  public Cell getColumnLatestCell(byte [] family, byte [] qualifier) {
    Cell [] kvs = rawCells(); // side effect possibly.
    if (kvs == null || kvs.length == 0) {
      return null;
    }
    int pos = binarySearch(kvs, family, qualifier);
    if (pos == -1) {
      return null;
    }
    if (CellUtil.matchingColumn(kvs[pos], family, qualifier)) {
      return kvs[pos];
    }
    return null;
  }

  /**
   * The Cell for the most recent timestamp for a given column.
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return the Cell for the column, or null if no value exists in the row or none have been
   * selected in the query (Get/Scan)
   */
  public Cell getColumnLatestCell(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    Cell [] kvs = rawCells(); // side effect possibly.
    if (kvs == null || kvs.length == 0) {
      return null;
    }
    int pos = binarySearch(kvs, family, foffset, flength, qualifier, qoffset, qlength);
    if (pos == -1) {
      return null;
    }
    if (PrivateCellUtil.matchingColumn(kvs[pos], family, foffset, flength, qualifier, qoffset,
      qlength)) {
      return kvs[pos];
    }
    return null;
  }

  /**
   * Get the latest version of the specified column.
   * Note: this call clones the value content of the hosting Cell. See
   * {@link #getValueAsByteBuffer(byte[], byte[])}, etc., or {@link #listCells()} if you would
   * avoid the cloning.
   * @param family family name
   * @param qualifier column qualifier
   * @return value of latest version of column, null if none found
   */
  public byte[] getValue(byte [] family, byte [] qualifier) {
    Cell kv = getColumnLatestCell(family, qualifier);
    if (kv == null) {
      return null;
    }
    return CellUtil.cloneValue(kv);
  }

  /**
   * Returns the value wrapped in a new <code>ByteBuffer</code>.
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return the latest version of the column, or <code>null</code> if none found
   */
  public ByteBuffer getValueAsByteBuffer(byte [] family, byte [] qualifier) {

    Cell kv = getColumnLatestCell(family, 0, family.length, qualifier, 0, qualifier.length);

    if (kv == null) {
      return null;
    }
    return ByteBuffer.wrap(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()).
      asReadOnlyBuffer();
  }

  /**
   * Returns the value wrapped in a new <code>ByteBuffer</code>.
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return the latest version of the column, or <code>null</code> if none found
   */
  public ByteBuffer getValueAsByteBuffer(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    Cell kv = getColumnLatestCell(family, foffset, flength, qualifier, qoffset, qlength);

    if (kv == null) {
      return null;
    }
    return ByteBuffer.wrap(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()).
      asReadOnlyBuffer();
  }

  /**
   * Loads the latest version of the specified column into the provided <code>ByteBuffer</code>.
   * <p>
   * Does not clear or flip the buffer.
   *
   * @param family family name
   * @param qualifier column qualifier
   * @param dst the buffer where to write the value
   *
   * @return <code>true</code> if a value was found, <code>false</code> otherwise
   *
   * @throws BufferOverflowException there is insufficient space remaining in the buffer
   */
  public boolean loadValue(byte [] family, byte [] qualifier, ByteBuffer dst)
          throws BufferOverflowException {
    return loadValue(family, 0, family.length, qualifier, 0, qualifier.length, dst);
  }

  /**
   * Loads the latest version of the specified column into the provided <code>ByteBuffer</code>.
   * <p>
   * Does not clear or flip the buffer.
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param dst the buffer where to write the value
   *
   * @return <code>true</code> if a value was found, <code>false</code> otherwise
   *
   * @throws BufferOverflowException there is insufficient space remaining in the buffer
   */
  public boolean loadValue(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength, ByteBuffer dst)
          throws BufferOverflowException {
    Cell kv = getColumnLatestCell(family, foffset, flength, qualifier, qoffset, qlength);

    if (kv == null) {
      return false;
    }
    dst.put(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
    return true;
  }

  /**
   * Checks if the specified column contains a non-empty value (not a zero-length byte array).
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return whether or not a latest value exists and is not empty
   */
  public boolean containsNonEmptyColumn(byte [] family, byte [] qualifier) {

    return containsNonEmptyColumn(family, 0, family.length, qualifier, 0, qualifier.length);
  }

  /**
   * Checks if the specified column contains a non-empty value (not a zero-length byte array).
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return whether or not a latest value exists and is not empty
   */
  public boolean containsNonEmptyColumn(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    Cell kv = getColumnLatestCell(family, foffset, flength, qualifier, qoffset, qlength);

    return (kv != null) && (kv.getValueLength() > 0);
  }

  /**
   * Checks if the specified column contains an empty value (a zero-length byte array).
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return whether or not a latest value exists and is empty
   */
  public boolean containsEmptyColumn(byte [] family, byte [] qualifier) {

    return containsEmptyColumn(family, 0, family.length, qualifier, 0, qualifier.length);
  }

  /**
   * Checks if the specified column contains an empty value (a zero-length byte array).
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return whether or not a latest value exists and is empty
   */
  public boolean containsEmptyColumn(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {
    Cell kv = getColumnLatestCell(family, foffset, flength, qualifier, qoffset, qlength);

    return (kv != null) && (kv.getValueLength() == 0);
  }

  /**
   * Checks for existence of a value for the specified column (empty or not).
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return true if at least one value exists in the result, false if not
   */
  public boolean containsColumn(byte [] family, byte [] qualifier) {
    Cell kv = getColumnLatestCell(family, qualifier);
    return kv != null;
  }

  /**
   * Checks for existence of a value for the specified column (empty or not).
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return true if at least one value exists in the result, false if not
   */
  public boolean containsColumn(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    return getColumnLatestCell(family, foffset, flength, qualifier, qoffset, qlength) != null;
  }

  /**
   * Map of families to all versions of its qualifiers and values.
   * <p>
   * Returns a three level Map of the form:
   * <code>Map&amp;family,Map&lt;qualifier,Map&lt;timestamp,value&gt;&gt;&gt;</code>
   * <p>
   * Note: All other map returning methods make use of this map internally.
   * @return map from families to qualifiers to versions
   */
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    if (this.familyMap != null) {
      return this.familyMap;
    }
    if(isEmpty()) {
      return null;
    }
    this.familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(Cell kv : this.cells) {
      byte [] family = CellUtil.cloneFamily(kv);
      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(family);
      if(columnMap == null) {
        columnMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        familyMap.put(family, columnMap);
      }
      byte [] qualifier = CellUtil.cloneQualifier(kv);
      NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
      if(versionMap == null) {
        versionMap = new TreeMap<>(new Comparator<Long>() {
          @Override
          public int compare(Long l1, Long l2) {
            return l2.compareTo(l1);
          }
        });
        columnMap.put(qualifier, versionMap);
      }
      Long timestamp = kv.getTimestamp();
      byte [] value = CellUtil.cloneValue(kv);

      versionMap.put(timestamp, value);
    }
    return this.familyMap;
  }

  /**
   * Map of families to their most recent qualifiers and values.
   * <p>
   * Returns a two level Map of the form: <code>Map&amp;family,Map&lt;qualifier,value&gt;&gt;</code>
   * <p>
   * The most recent version of each qualifier will be used.
   * @return map from families to qualifiers and value
   */
  public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> returnMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      familyEntry : familyMap.entrySet()) {
      NavigableMap<byte[], byte[]> qualifierMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for(Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry :
        familyEntry.getValue().entrySet()) {
        byte [] value =
          qualifierEntry.getValue().get(qualifierEntry.getValue().firstKey());
        qualifierMap.put(qualifierEntry.getKey(), value);
      }
      returnMap.put(familyEntry.getKey(), qualifierMap);
    }
    return returnMap;
  }

  /**
   * Map of qualifiers to values.
   * <p>
   * Returns a Map of the form: <code>Map&lt;qualifier,value&gt;</code>
   * @param family column family to get
   * @return map of qualifiers to values
   */
  public NavigableMap<byte[], byte[]> getFamilyMap(byte [] family) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], byte[]> returnMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return returnMap;
    }
    for(Map.Entry<byte[], NavigableMap<Long, byte[]>> entry :
      qualifierMap.entrySet()) {
      byte [] value =
        entry.getValue().get(entry.getValue().firstKey());
      returnMap.put(entry.getKey(), value);
    }
    return returnMap;
  }

  /**
   * Returns the value of the first column in the Result.
   * @return value of the first column
   */
  public byte [] value() {
    if (isEmpty()) {
      return null;
    }
    return CellUtil.cloneValue(cells[0]);
  }

  /**
   * Check if the underlying Cell [] is empty or not
   * @return true if empty
   */
  public boolean isEmpty() {
    return this.cells == null || this.cells.length == 0;
  }

  /**
   * @return the size of the underlying Cell []
   */
  public int size() {
    return this.cells == null? 0: this.cells.length;
  }

  /**
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("keyvalues=");
    if(isEmpty()) {
      sb.append("NONE");
      return sb.toString();
    }
    sb.append("{");
    boolean moreThanOne = false;
    for(Cell kv : this.cells) {
      if(moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append(kv.toString());
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Does a deep comparison of two Results, down to the byte arrays.
   * @param res1 first result to compare
   * @param res2 second result to compare
   * @throws Exception Every difference is throwing an exception
   */
  public static void compareResults(Result res1, Result res2)
      throws Exception {
    if (res2 == null) {
      throw new Exception("There wasn't enough rows, we stopped at "
          + Bytes.toStringBinary(res1.getRow()));
    }
    if (res1.size() != res2.size()) {
      throw new Exception("This row doesn't have the same number of KVs: "
          + res1.toString() + " compared to " + res2.toString());
    }
    Cell[] ourKVs = res1.rawCells();
    Cell[] replicatedKVs = res2.rawCells();
    for (int i = 0; i < res1.size(); i++) {
      if (!ourKVs[i].equals(replicatedKVs[i]) ||
          !CellUtil.matchingValue(ourKVs[i], replicatedKVs[i])) {
        throw new Exception("This result was different: "
            + res1.toString() + " compared to " + res2.toString());
      }
    }
  }

  /**
   * Forms a single result from the partial results in the partialResults list. This method is
   * useful for reconstructing partial results on the client side.
   * @param partialResults list of partial results
   * @return The complete result that is formed by combining all of the partial results together
   * @throws IOException A complete result cannot be formed because the results in the partial list
   *           come from different rows
   */
  public static Result createCompleteResult(Iterable<Result> partialResults)
      throws IOException {
    if (partialResults == null) {
      return Result.create(Collections.emptyList(), null, false);
    }
    List<Cell> cells = new ArrayList<>();
    boolean stale = false;
    byte[] prevRow = null;
    byte[] currentRow = null;
    for (Iterator<Result> iter = partialResults.iterator(); iter.hasNext();) {
      Result r = iter.next();
      currentRow = r.getRow();
      if (prevRow != null && !Bytes.equals(prevRow, currentRow)) {
        throw new IOException(
            "Cannot form complete result. Rows of partial results do not match." +
                " Partial Results: " + partialResults);
      }
      // Ensure that all Results except the last one are marked as partials. The last result
      // may not be marked as a partial because Results are only marked as partials when
      // the scan on the server side must be stopped due to reaching the maxResultSize.
      // Visualizing it makes it easier to understand:
      // maxResultSize: 2 cells
      // (-x-) represents cell number x in a row
      // Example: row1: -1- -2- -3- -4- -5- (5 cells total)
      // How row1 will be returned by the server as partial Results:
      // Result1: -1- -2- (2 cells, size limit reached, mark as partial)
      // Result2: -3- -4- (2 cells, size limit reached, mark as partial)
      // Result3: -5- (1 cell, size limit NOT reached, NOT marked as partial)
      if (iter.hasNext() && !r.mayHaveMoreCellsInRow()) {
        throw new IOException("Cannot form complete result. Result is missing partial flag. " +
            "Partial Results: " + partialResults);
      }
      prevRow = currentRow;
      stale = stale || r.isStale();
      for (Cell c : r.rawCells()) {
        cells.add(c);
      }
    }

    return Result.create(cells, null, stale);
  }

  /**
   * Get total size of raw cells
   * @param result
   * @return Total size.
   */
  public static long getTotalSizeOfCells(Result result) {
    long size = 0;
    if (result.isEmpty()) {
      return size;
    }
    for (Cell c : result.rawCells()) {
      size += PrivateCellUtil.estimatedSizeOfCell(c);
    }
    return size;
  }

  /**
   * Copy another Result into this one. Needed for the old Mapred framework
   * @throws UnsupportedOperationException if invoked on instance of EMPTY_RESULT
   * (which is supposed to be immutable).
   * @param other
   */
  public void copyFrom(Result other) {
    checkReadonly();
    this.row = null;
    this.familyMap = null;
    this.cells = other.cells;
  }

  @Override
  public CellScanner cellScanner() {
    // Reset
    this.cellScannerIndex = INITIAL_CELLSCANNER_INDEX;
    return this;
  }

  @Override
  public Cell current() {
    if (cells == null
            || cellScannerIndex == INITIAL_CELLSCANNER_INDEX
            || cellScannerIndex >= cells.length)
      return null;
    return this.cells[cellScannerIndex];
  }

  @Override
  public boolean advance() {
    if (cells == null) return false;
    cellScannerIndex++;
    if (cellScannerIndex < this.cells.length) {
      return true;
    } else if (cellScannerIndex == this.cells.length) {
      return false;
    }
    throw new NoSuchElementException("Cannot advance beyond the last cell");
  }

  public Boolean getExists() {
    return exists;
  }

  public void setExists(Boolean exists) {
    checkReadonly();
    this.exists = exists;
  }

  /**
   * Whether or not the results are coming from possibly stale data. Stale results
   * might be returned if {@link Consistency} is not STRONG for the query.
   * @return Whether or not the results are coming from possibly stale data.
   */
  public boolean isStale() {
    return stale;
  }

  /**
   * @deprecated the word 'partial' ambiguous, use {@link #mayHaveMoreCellsInRow()} instead.
   *             Deprecated since 1.4.0.
   * @see #mayHaveMoreCellsInRow()
   */
  @Deprecated
  public boolean isPartial() {
    return mayHaveMoreCellsInRow;
  }

  /**
   * For scanning large rows, the RS may choose to return the cells chunk by chunk to prevent OOM
   * or timeout. This flag is used to tell you if the current Result is the last one of the current
   * row. False means this Result is the last one. True means there MAY be more cells belonging to
   * the current row.
   * If you don't use {@link Scan#setAllowPartialResults(boolean)} or {@link Scan#setBatch(int)},
   * this method will always return false because the Result must contains all cells in one Row.
   */
  public boolean mayHaveMoreCellsInRow() {
    return mayHaveMoreCellsInRow;
  }

  /**
   * Set load information about the region to the information about the result
   * @param loadStats statistics about the current region from which this was returned
   */
  @InterfaceAudience.Private
  public void setStatistics(RegionLoadStats loadStats) {
    this.stats = loadStats;
  }

  /**
   * @return the associated statistics about the region from which this was returned. Can be
   * <tt>null</tt> if stats are disabled.
   */
  public RegionLoadStats getStats() {
    return stats;
  }

  /**
   * All methods modifying state of Result object must call this method
   * to ensure that special purpose immutable Results can't be accidentally modified.
   */
  private void checkReadonly() {
    if (readonly == true) {
      throw new UnsupportedOperationException("Attempting to modify readonly EMPTY_RESULT!");
    }
  }

  /**
   * Return true if this Result is a cursor to tell users where the server has scanned.
   * In this Result the only meaningful method is {@link #getCursor()}.
   *
   * {@code
   *  while (r = scanner.next() && r != null) {
   *    if(r.isCursor()){
   *    // scanning is not end, it is a cursor, save its row key and close scanner if you want, or
   *    // just continue the loop to call next().
   *    } else {
   *    // just like before
   *    }
   *  }
   *  // scanning is end
   *
   * }
   * {@link Scan#setNeedCursorResult(boolean)}
   * {@link Cursor}
   * {@link #getCursor()}
   */
  public boolean isCursor() {
    return cursor != null ;
  }

  /**
   * Return the cursor if this Result is a cursor result.
   * {@link Scan#setNeedCursorResult(boolean)}
   * {@link Cursor}
   * {@link #isCursor()}
   */
  public Cursor getCursor(){
    return cursor;
  }
}
