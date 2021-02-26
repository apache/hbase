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
import java.util.UUID;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs Append operations on a single row.
 * <p>
 * This operation ensures atomicty to readers. Appends are done
 * under a single row lock, so write operations to a row are synchronized, and
 * readers are guaranteed to see this operation fully completed.
 * <p>
 * To append to a set of columns of a row, instantiate an Append object with the
 * row to append to. At least one column to append must be specified using the
 * {@link #addColumn(byte[], byte[], byte[])} method.
 */
@InterfaceAudience.Public
public class Append extends Mutation {
  private static final Logger LOG = LoggerFactory.getLogger(Append.class);
  private static final long HEAP_OVERHEAD = ClassSize.REFERENCE + ClassSize.TIMERANGE;
  private TimeRange tr = TimeRange.allTime();

  /**
   * Sets the TimeRange to be used on the Get for this append.
   * <p>
   * This is useful for when you have counters that only last for specific
   * periods of time (ie. counters that are partitioned by time).  By setting
   * the range of valid times for this append, you can potentially gain
   * some performance with a more optimal Get operation.
   * Be careful adding the time range to this class as you will update the old cell if the
   * time range doesn't include the latest cells.
   * <p>
   * This range is used as [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @return this
   */
  public Append setTimeRange(long minStamp, long maxStamp) {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Gets the TimeRange used for this append.
   * @return TimeRange
   */
  public TimeRange getTimeRange() {
    return this.tr;
  }

  @Override
  protected long extraHeapSize(){
    return HEAP_OVERHEAD;
  }

  /**
   * @param returnResults
   *          True (default) if the append operation should return the results.
   *          A client that is not interested in the result can save network
   *          bandwidth setting this to false.
   */
  @Override
  public Append setReturnResults(boolean returnResults) {
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
   * Create a Append operation for the specified row.
   * <p>
   * At least one column must be appended to.
   * @param row row key; makes a local copy of passed in array.
   */
  public Append(byte[] row) {
    this(row, 0, row.length);
  }
  /**
   * Copy constructor
   * @param appendToCopy append to copy
   */
  public Append(Append appendToCopy) {
    super(appendToCopy);
    this.tr = appendToCopy.getTimeRange();
  }

  /** Create a Append operation for the specified row.
   * <p>
   * At least one column must be appended to.
   * @param rowArray Makes a copy out of this buffer.
   * @param rowOffset
   * @param rowLength
   */
  public Append(final byte [] rowArray, final int rowOffset, final int rowLength) {
    checkRow(rowArray, rowOffset, rowLength);
    this.row = Bytes.copy(rowArray, rowOffset, rowLength);
  }

  /**
   * Construct the Append with user defined data. NOTED:
   * 1) all cells in the familyMap must have the Type.Put
   * 2) the row of each cell must be same with passed row.
   * @param row row. CAN'T be null
   * @param ts timestamp
   * @param familyMap the map to collect all cells internally. CAN'T be null
   */
  public Append(byte[] row, long ts, NavigableMap<byte [], List<Cell>> familyMap) {
    super(row, ts, familyMap);
  }

  /**
   * Add the specified column and value to this Append operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value value to append to specified column
   * @return this
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #addColumn(byte[], byte[], byte[])} instead
   */
  @Deprecated
  public Append add(byte [] family, byte [] qualifier, byte [] value) {
    return this.addColumn(family, qualifier, value);
  }

  /**
   * Add the specified column and value to this Append operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value value to append to specified column
   * @return this
   */
  public Append addColumn(byte[] family, byte[] qualifier, byte[] value) {
    KeyValue kv = new KeyValue(this.row, family, qualifier, this.ts, KeyValue.Type.Put, value);
    return add(kv);
  }

  /**
   * Add column and value to this Append operation.
   * @param cell
   * @return This instance
   */
  @SuppressWarnings("unchecked")
  public Append add(final Cell cell) {
    try {
      super.add(cell);
    } catch (IOException e) {
      // we eat the exception of wrong row for BC..
      LOG.error(e.toString(), e);
    }
    return this;
  }

  @Override
  public Append setTimestamp(long timestamp) {
    super.setTimestamp(timestamp);
    return this;
  }

  @Override
  public Append setAttribute(String name, byte[] value) {
    return (Append) super.setAttribute(name, value);
  }

  @Override
  public Append setId(String id) {
    return (Append) super.setId(id);
  }

  @Override
  public Append setDurability(Durability d) {
    return (Append) super.setDurability(d);
  }

  /**
   * Method for setting the Append's familyMap
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link Append#Append(byte[], long, NavigableMap)} instead
   */
  @Deprecated
  @Override
  public Append setFamilyCellMap(NavigableMap<byte[], List<Cell>> map) {
    return (Append) super.setFamilyCellMap(map);
  }

  @Override
  public Append setClusterIds(List<UUID> clusterIds) {
    return (Append) super.setClusterIds(clusterIds);
  }

  @Override
  public Append setCellVisibility(CellVisibility expression) {
    return (Append) super.setCellVisibility(expression);
  }

  @Override
  public Append setACL(String user, Permission perms) {
    return (Append) super.setACL(user, perms);
  }

  @Override
  public Append setACL(Map<String, Permission> perms) {
    return (Append) super.setACL(perms);
  }

  @Override
  public Append setPriority(int priority) {
    return (Append) super.setPriority(priority);
  }

  @Override
  public Append setTTL(long ttl) {
    return (Append) super.setTTL(ttl);
  }
}
