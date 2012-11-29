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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

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
public class Increment implements Row {
  private byte [] row = null;
  private long lockId = -1L;
  private boolean writeToWAL = true;
  private TimeRange tr = new TimeRange();
  private Map<byte [], NavigableMap<byte [], Long>> familyMap =
    new TreeMap<byte [], NavigableMap<byte [], Long>>(Bytes.BYTES_COMPARATOR);

  /** Constructor for Writable.  DO NOT USE */
  public Increment() {}

  /**
   * Create a Increment operation for the specified row.
   * <p>
   * At least one column must be incremented.
   * @param row row key
   */
  public Increment(byte [] row) {
    this(row, null);
  }

  /**
   * Create a Increment operation for the specified row, using an existing row
   * lock.
   * <p>
   * At least one column must be incremented.
   * @param row row key
   * @param rowLock previously acquired row lock, or null
   */
  public Increment(byte [] row, RowLock rowLock) {
    if (row == null) {
      throw new IllegalArgumentException("Cannot increment a null row");
    }
    this.row = row;
    if(rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
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
  public Increment addColumn(byte [] family, byte [] qualifier, long amount) {
    if (family == null) {
      throw new IllegalArgumentException("family cannot be null");
    }
    if (qualifier == null) {
      throw new IllegalArgumentException("qualifier cannot be null");
    }
    NavigableMap<byte [], Long> set = familyMap.get(family);
    if(set == null) {
      set = new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR);
    }
    set.put(qualifier, amount);
    familyMap.put(family, set);
    return this;
  }

  /* Accessors */

  /**
   * Method for retrieving the increment's row
   * @return row
   */
  public byte [] getRow() {
    return this.row;
  }

  /**
   * Method for retrieving the increment's RowLock
   * @return RowLock
   */
  public RowLock getRowLock() {
    return new RowLock(this.row, this.lockId);
  }

  /**
   * Method for retrieving the increment's lockId
   * @return lockId
   */
  public long getLockId() {
    return this.lockId;
  }

  /**
   * Method for retrieving whether WAL will be written to or not
   * @return true if WAL should be used, false if not
   */
  public boolean getWriteToWAL() {
    return this.writeToWAL;
  }

  /**
   * Sets whether this operation should write to the WAL or not.
   * @param writeToWAL true if WAL should be used, false if not
   * @return this increment operation
   */
  public Increment setWriteToWAL(boolean writeToWAL) {
    this.writeToWAL = writeToWAL;
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
   * Method for retrieving the keys in the familyMap
   * @return keys in the current familyMap
   */
  public Set<byte[]> familySet() {
    return this.familyMap.keySet();
  }

  /**
   * Method for retrieving the number of families to increment from
   * @return number of families
   */
  public int numFamilies() {
    return this.familyMap.size();
  }

  /**
   * Method for retrieving the number of columns to increment
   * @return number of columns across all families
   */
  public int numColumns() {
    if (!hasFamilies()) return 0;
    int num = 0;
    for (NavigableMap<byte [], Long> family : familyMap.values()) {
      num += family.size();
    }
    return num;
  }

  /**
   * Method for checking if any families have been inserted into this Increment
   * @return true if familyMap is non empty false otherwise
   */
  public boolean hasFamilies() {
    return !this.familyMap.isEmpty();
  }

  /**
   * Method for retrieving the increment's familyMap
   * @return familyMap
   */
  public Map<byte[],NavigableMap<byte[], Long>> getFamilyMap() {
    return this.familyMap;
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
    for(Map.Entry<byte [], NavigableMap<byte[], Long>> entry :
      this.familyMap.entrySet()) {
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
        for(Map.Entry<byte [], Long> column : entry.getValue().entrySet()) {
          if(moreThanOneB) {
            sb.append(", ");
          } else {
            moreThanOneB = true;
          }
          sb.append(Bytes.toStringBinary(column.getKey()) + "+=" + column.getValue());
        }
        sb.append("}");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public int compareTo(Row i) {
    return Bytes.compareTo(this.getRow(), i.getRow());
  }
}
