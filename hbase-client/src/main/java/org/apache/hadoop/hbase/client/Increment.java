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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to perform Increment operations on a single row.
 * <p>
 * This operation ensures atomicity to readers. Increments are done under a single row lock, so
 * write operations to a row are synchronized, and readers are guaranteed to see this operation
 * fully completed.
 * <p>
 * To increment columns of a row, instantiate an Increment object with the row to increment. At
 * least one column to increment must be specified using the
 * {@link #addColumn(byte[], byte[], long)} method.
 */
@InterfaceAudience.Public
public class Increment extends Mutation {
  private static final int HEAP_OVERHEAD = ClassSize.REFERENCE + ClassSize.TIMERANGE;
  private TimeRange tr = TimeRange.allTime();

  /**
   * Create a Increment operation for the specified row.
   * <p>
   * At least one column must be incremented.
   * @param row row key (we will make a copy of this).
   */
  public Increment(byte[] row) {
    this(row, 0, row.length);
  }

  /**
   * Create a Increment operation for the specified row.
   * <p>
   * At least one column must be incremented.
   * @param row row key (we will make a copy of this).
   */
  public Increment(final byte[] row, final int offset, final int length) {
    checkRow(row, offset, length);
    this.row = Bytes.copy(row, offset, length);
  }

  /**
   * Copy constructor
   * @param incrementToCopy increment to copy
   */
  public Increment(Increment incrementToCopy) {
    super(incrementToCopy);
    this.tr = incrementToCopy.getTimeRange();
  }

  /**
   * Construct the Increment with user defined data. NOTED: 1) all cells in the familyMap must have
   * the Type.Put 2) the row of each cell must be same with passed row.
   * @param row       row. CAN'T be null
   * @param ts        timestamp
   * @param familyMap the map to collect all cells internally. CAN'T be null
   */
  public Increment(byte[] row, long ts, NavigableMap<byte[], List<Cell>> familyMap) {
    super(row, ts, familyMap);
  }

  /**
   * Add the specified KeyValue to this operation.
   * @param cell individual Cell n * @throws java.io.IOException e
   */
  public Increment add(Cell cell) throws IOException {
    super.add(cell);
    return this;
  }

  /**
   * Increment the column from the specific family with the specified qualifier by the specified
   * amount.
   * <p>
   * Overrides previous calls to addColumn for this family and qualifier.
   * @param family    family name
   * @param qualifier column qualifier
   * @param amount    amount to increment by
   * @return the Increment object
   */
  public Increment addColumn(byte[] family, byte[] qualifier, long amount) {
    if (family == null) {
      throw new IllegalArgumentException("family cannot be null");
    }
    List<Cell> list = getCellList(family);
    KeyValue kv = createPutKeyValue(family, qualifier, ts, Bytes.toBytes(amount));
    list.add(kv);
    return this;
  }

  /**
   * Gets the TimeRange used for this increment. n
   */
  public TimeRange getTimeRange() {
    return this.tr;
  }

  /**
   * Sets the TimeRange to be used on the Get for this increment.
   * <p>
   * This is useful for when you have counters that only last for specific periods of time (ie.
   * counters that are partitioned by time). By setting the range of valid times for this increment,
   * you can potentially gain some performance with a more optimal Get operation. Be careful adding
   * the time range to this class as you will update the old cell if the time range doesn't include
   * the latest cells.
   * <p>
   * This range is used as [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range n
   */
  public Increment setTimeRange(long minStamp, long maxStamp) throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  @Override
  public Increment setTimestamp(long timestamp) {
    super.setTimestamp(timestamp);
    return this;
  }

  /**
   * @param returnResults True (default) if the increment operation should return the results. A
   *                      client that is not interested in the result can save network bandwidth
   *                      setting this to false.
   */
  @Override
  public Increment setReturnResults(boolean returnResults) {
    super.setReturnResults(returnResults);
    return this;
  }

  /**
   * @return current setting for returnResults
   */
  // This method makes public the superclasses's protected method.
  @Override
  public boolean isReturnResults() {
    return super.isReturnResults();
  }

  /**
   * Method for retrieving the number of families to increment from
   * @return number of families
   */
  @Override
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
   * Before 0.95, when you called Increment#getFamilyMap(), you got back a map of families to a list
   * of Longs. Now, {@link #getFamilyCellMap()} returns families by list of Cells. This method has
   * been added so you can have the old behavior.
   * @return Map of families to a Map of qualifiers and their Long increments.
   * @since 0.95.0
   */
  public Map<byte[], NavigableMap<byte[], Long>> getFamilyMapOfLongs() {
    NavigableMap<byte[], List<Cell>> map = super.getFamilyCellMap();
    Map<byte[], NavigableMap<byte[], Long>> results = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], List<Cell>> entry : map.entrySet()) {
      NavigableMap<byte[], Long> longs = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (Cell cell : entry.getValue()) {
        longs.put(CellUtil.cloneQualifier(cell),
          Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      }
      results.put(entry.getKey(), longs);
    }
    return results;
  }

  /**
   * n
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("row=");
    sb.append(Bytes.toStringBinary(this.row));
    if (this.familyMap.isEmpty()) {
      sb.append(", no columns set to be incremented");
      return sb.toString();
    }
    sb.append(", families=");
    boolean moreThanOne = false;
    for (Map.Entry<byte[], List<Cell>> entry : this.familyMap.entrySet()) {
      if (moreThanOne) {
        sb.append("), ");
      } else {
        moreThanOne = true;
        sb.append("{");
      }
      sb.append("(family=");
      sb.append(Bytes.toString(entry.getKey()));
      sb.append(", columns=");
      if (entry.getValue() == null) {
        sb.append("NONE");
      } else {
        sb.append("{");
        boolean moreThanOneB = false;
        for (Cell cell : entry.getValue()) {
          if (moreThanOneB) {
            sb.append(", ");
          } else {
            moreThanOneB = true;
          }
          sb.append(CellUtil.getCellKeyAsString(cell) + "+="
            + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        sb.append("}");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. No replacement.
   */
  @Deprecated
  @Override
  public int hashCode() {
    // TODO: This is wrong. Can't have two gets the same just because on same row. But it
    // matches how equals works currently and gets rid of the findbugs warning.
    return Bytes.hashCode(this.getRow());
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link Row#COMPARATOR} instead
   */
  @Deprecated
  @Override
  public boolean equals(Object obj) {
    // TODO: This is wrong. Can't have two the same just because on same row.
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Row other = (Row) obj;
    return compareTo(other) == 0;
  }

  @Override
  protected long extraHeapSize() {
    return HEAP_OVERHEAD;
  }

  @Override
  public Increment setAttribute(String name, byte[] value) {
    return (Increment) super.setAttribute(name, value);
  }

  @Override
  public Increment setId(String id) {
    return (Increment) super.setId(id);
  }

  @Override
  public Increment setDurability(Durability d) {
    return (Increment) super.setDurability(d);
  }

  /**
   * Method for setting the Increment's familyMap
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link Increment#Increment(byte[], long, NavigableMap)} instead
   */
  @Deprecated
  @Override
  public Increment setFamilyCellMap(NavigableMap<byte[], List<Cell>> map) {
    return (Increment) super.setFamilyCellMap(map);
  }

  @Override
  public Increment setClusterIds(List<UUID> clusterIds) {
    return (Increment) super.setClusterIds(clusterIds);
  }

  @Override
  public Increment setCellVisibility(CellVisibility expression) {
    return (Increment) super.setCellVisibility(expression);
  }

  @Override
  public Increment setACL(String user, Permission perms) {
    return (Increment) super.setACL(user, perms);
  }

  @Override
  public Increment setACL(Map<String, Permission> perms) {
    return (Increment) super.setACL(perms);
  }

  @Override
  public Increment setTTL(long ttl) {
    return (Increment) super.setTTL(ttl);
  }

  @Override
  public Increment setPriority(int priority) {
    return (Increment) super.setPriority(priority);
  }
}
