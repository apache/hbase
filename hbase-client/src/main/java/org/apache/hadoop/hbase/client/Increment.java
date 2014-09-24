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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Used to perform Increment operations on a single row.
 * <p>
 * This operation does not appear atomic to readers.  Increments are done
 * under a single row lock, so write operations to a row are synchronized, but
 * readers do not take row locks so get and scan operations can see this
 * operation partially completed.
 * <p>
 * To increment columns of a row, instantiate an Increment object with the row
 * to increment.  At least one column to increment must be specified using the
 * {@link #addColumn(byte[], byte[], long)} method.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Increment extends Mutation implements Comparable<Row> {
  private static final long HEAP_OVERHEAD =  ClassSize.REFERENCE + ClassSize.TIMERANGE;

  private TimeRange tr = new TimeRange();

  /**
   * Create a Increment operation for the specified row.
   * <p>
   * At least one column must be incremented.
   * @param row row key (we will make a copy of this).
   */
  public Increment(byte [] row) {
    this(row, 0, row.length);
  }

  /**
   * Create a Increment operation for the specified row.
   * <p>
   * At least one column must be incremented.
   * @param row row key (we will make a copy of this).
   */
  public Increment(final byte [] row, final int offset, final int length) {
    checkRow(row, offset, length);
    this.row = Bytes.copy(row, offset, length);
  }
  /**
   * Copy constructor
   * @param i
   */
  public Increment(Increment i) {
    this.row = i.getRow();
    this.ts = i.getTimeStamp();
    this.tr = i.getTimeRange();
    this.familyMap.putAll(i.getFamilyCellMap());
    for (Map.Entry<String, byte[]> entry : i.getAttributesMap().entrySet()) {
      this.setAttribute(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Add the specified KeyValue to this operation.
   * @param cell individual Cell
   * @return this
   * @throws java.io.IOException e
   */
  @SuppressWarnings("unchecked")
  public Increment add(Cell cell) throws IOException{
    KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
    byte [] family = kv.getFamily();
    List<Cell> list = getCellList(family);
    //Checking that the row of the kv is the same as the put
    int res = Bytes.compareTo(this.row, 0, row.length,
        kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
    if (res != 0) {
      throw new WrongRowIOException("The row in " + kv.toString() +
        " doesn't match the original one " +  Bytes.toStringBinary(this.row));
    }
    list.add(kv);
    familyMap.put(family, list);
    return this;
  }

  /**
   * Increment the column from the specific family with the specified qualifier
   * by the specified amount.
   * <p>
   * Overrides previous calls to addColumn for this family and qualifier.
   * @param family family name
   * @param qualifier column qualifier
   * @param amount amount to increment by
   * @return the Increment object
   */
  @SuppressWarnings("unchecked")
  public Increment addColumn(byte [] family, byte [] qualifier, long amount) {
    if (family == null) {
      throw new IllegalArgumentException("family cannot be null");
    }
    if (qualifier == null) {
      throw new IllegalArgumentException("qualifier cannot be null");
    }
    List<Cell> list = getCellList(family);
    KeyValue kv = createPutKeyValue(family, qualifier, ts, Bytes.toBytes(amount));
    list.add(kv);
    familyMap.put(kv.getFamily(), list);
    return this;
  }

  /**
   * Gets the TimeRange used for this increment.
   * @return TimeRange
   */
  public TimeRange getTimeRange() {
    return this.tr;
  }

  /**
   * Sets the TimeRange to be used on the Get for this increment.
   * <p>
   * This is useful for when you have counters that only last for specific
   * periods of time (ie. counters that are partitioned by time).  By setting
   * the range of valid times for this increment, you can potentially gain
   * some performance with a more optimal Get operation.
   * <p>
   * This range is used as [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range
   * @return this
   */
  public Increment setTimeRange(long minStamp, long maxStamp)
  throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Method for retrieving the number of families to increment from
   * @return number of families
   */
  public int numFamilies() {
    return this.familyMap.size();
  }

  /**
   * Method for checking if any families have been inserted into this Increment
   * @return true if familyMap is non empty false otherwise
   */
  public boolean hasFamilies() {
    return !this.familyMap.isEmpty();
  }

  /**
   * Before 0.95, when you called Increment#getFamilyMap(), you got back
   * a map of families to a list of Longs.  Now, {@link #getFamilyCellMap()} returns
   * families by list of Cells.  This method has been added so you can have the
   * old behavior.
   * @return Map of families to a Map of qualifers and their Long increments.
   * @since 0.95.0
   */
  public Map<byte[], NavigableMap<byte [], Long>> getFamilyMapOfLongs() {
    NavigableMap<byte[], List<Cell>> map = super.getFamilyCellMap();
    Map<byte [], NavigableMap<byte[], Long>> results =
      new TreeMap<byte[], NavigableMap<byte [], Long>>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte [], List<Cell>> entry: map.entrySet()) {
      NavigableMap<byte [], Long> longs = new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR);
      for (Cell cell: entry.getValue()) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        longs.put(kv.getQualifier(),
            Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
      }
      results.put(entry.getKey(), longs);
    }
    return results;
  }

  /**
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("row=");
    sb.append(Bytes.toStringBinary(this.row));
    if(this.familyMap.size() == 0) {
      sb.append(", no columns set to be incremented");
      return sb.toString();
    }
    sb.append(", families=");
    boolean moreThanOne = false;
    for(Map.Entry<byte [], List<Cell>> entry: this.familyMap.entrySet()) {
      if(moreThanOne) {
        sb.append("), ");
      } else {
        moreThanOne = true;
        sb.append("{");
      }
      sb.append("(family=");
      sb.append(Bytes.toString(entry.getKey()));
      sb.append(", columns=");
      if(entry.getValue() == null) {
        sb.append("NONE");
      } else {
        sb.append("{");
        boolean moreThanOneB = false;
        for(Cell cell : entry.getValue()) {
          if(moreThanOneB) {
            sb.append(", ");
          } else {
            moreThanOneB = true;
          }
          KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
          sb.append(Bytes.toStringBinary(kv.getKey()) + "+=" +
              Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
        }
        sb.append("}");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public int compareTo(Row i) {
    // TODO: This is wrong.  Can't have two the same just because on same row.
    return Bytes.compareTo(this.getRow(), i.getRow());
  }

  @Override
  public int hashCode() {
    // TODO: This is wrong.  Can't have two gets the same just because on same row.  But it
    // matches how equals works currently and gets rid of the findbugs warning.
    return Bytes.hashCode(this.getRow());
  }

  @Override
  public boolean equals(Object obj) {
    // TODO: This is wrong.  Can't have two the same just because on same row.
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Row other = (Row) obj;
    return compareTo(other) == 0;
  }

  protected long extraHeapSize(){
    return HEAP_OVERHEAD;
  }
}
