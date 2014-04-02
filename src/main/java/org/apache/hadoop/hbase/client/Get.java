/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;

/**
 * Used to perform Get operations on a single row.
 * <p>
 * To get everything for a row, instantiate a Get object with the row to get.
 * To further define the scope of what to get, perform additional methods as
 * outlined below.
 * <p>
 * To get all columns from specific families, execute {@link #addFamily(byte[]) addFamily}
 * for each family to retrieve.
 * <p>
 * To get specific columns, execute {@link #addColumn(byte[], byte[]) addColumn}
 * for each column to retrieve.
 * <p>
 * To only retrieve columns within a specific range of version timestamps,
 * execute {@link #setTimeRange(long, long) setTimeRange}.
 * <p>
 * To only retrieve columns with a specific timestamp, execute
 * {@link #setTimeStamp(long) setTimestamp}.
 * <p>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p>
 * To limit the number of values of each column family to be returned, execute
 * {@link #setMaxResultsPerColumnFamily(int) setMaxResultsPerColumnFamily}.
 * <p>
 * To add a filter, execute {@link #setFilter(Filter) setFilter}.
 */

@ThriftStruct
public class Get extends OperationWithAttributes
  implements Writable, Row, Comparable<Row> {
  private static final Log LOG = LogFactory.getLog(Get.class);
  private static final byte STORE_LIMIT_VERSION = (byte) 2;
  private static final byte STORE_OFFSET_VERSION = (byte) 3;
  private static final byte FLASHBACK_VERSION = (byte) 4;
  private static final byte GET_VERSION = FLASHBACK_VERSION;

  private byte[] row = null;
  private long lockId = -1L;
  private int maxVersions = 1;
  private int storeLimit = -1;
  private int storeOffset = 0;
  private Filter filter = null;
  private TFilter tFilter = null;
  private TimeRange tr = new TimeRange();
  private Map<byte[], NavigableSet<byte[]>> familyMap =
      new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
  // Operation should be performed as if it was performed at the given ts.
  private long effectiveTS = HConstants.LATEST_TIMESTAMP;

  /** Constructor for Writable.  DO NOT USE */
  public Get() {}

  /**
   * Create a Get operation for the specified row.
   * <p>
   * If no further operations are done, this will get the latest version of
   * all columns in all families of the specified row.
   * @param row row key
   */
  public Get(byte[] row) {
    this(row, null);
  }

  /**
   * Create a Get operation for the specified row, using an existing row lock.
   * <p>
   * If no further operations are done, this will get the latest version of
   * all columns in all families of the specified row.
   * @param row row key
   * @param rowLock previously acquired row lock, or null
   */
  public Get(byte[] row, RowLock rowLock) {
    this.row = row;
    if(rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
  }

  /**
   * Thrift Constructor
   * TODO: add Filter annotations!
   * @param row
   * @param lockId
   * @param maxVersions
   * @param storeLimit
   * @param storeOffset
   * @param tr
   * @param familyMap
   * @param effectiveTS
   */
  @ThriftConstructor
  public Get(
      @ThriftField(1) byte[] row,
      @ThriftField(2) long lockId,
      @ThriftField(3) int maxVersions,
      @ThriftField(4) int storeLimit,
      @ThriftField(5) int storeOffset,
      @ThriftField(6) TimeRange tr,
      @ThriftField(7) Map<byte[], Set<byte[]>> familyMap,
      @ThriftField(8) long effectiveTS,
      @ThriftField(9) TFilter tFilter){
    this.row = row;
    this.lockId = lockId;
    this.maxVersions = maxVersions;
    this.storeLimit = storeLimit;
    this.storeOffset = storeOffset;
    this.effectiveTS = effectiveTS;
    this.tr = tr;
    this.tFilter = tFilter;
    this.filter = tFilter;
    if (familyMap != null) {
      for (Entry<byte[], Set<byte[]>> entry : familyMap.entrySet()) {
        NavigableSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        if (entry.getValue() != null) {
          set.addAll(entry.getValue());
        }
        this.familyMap.put(entry.getKey(), set);
      }
    }
  }


  /**
   * Overrides previous calls to addColumn for this family.
   * @param family family name
   * @return the Get object
   * @deprecated use {@link Builder#addFamily(byte[])} instead.
   */
  @Deprecated
  public Get addFamily(byte[] family) {
    familyMap.put(family, new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));
    return this;
  }

  /**
   * Get the column from the specific family with the specified qualifier.
   * <p>
   * Overrides previous calls to addFamily for this family.
   * @param family family name
   * @param qualifier column qualifier
   * @return the Get objec
   * @deprecated use {@link Builder#addColumn(byte[], byte[])} instead.
   */
  @Deprecated
  public Get addColumn(byte[] family, byte[] qualifier) {
    NavigableSet<byte[]> set = familyMap.get(family);
    if (set == null) {
      set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    }
    if (qualifier == null) {
      set.add(HConstants.EMPTY_BYTE_ARRAY);
    } else {
      set.add(qualifier);
    }
    familyMap.put(family, set);
    return this;
  }

  /**
   * Get versions of columns only within the specified timestamp range,
   * [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range
   * @return this for invocation chaining
   */
  public Get setTimeRange(long minStamp, long maxStamp)
  throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Get versions of columns with the specified timestamp.
   * @param timestamp version timestamp
   * @return this for invocation chaining
   */
  public Get setTimeStamp(long timestamp) {
    try {
      tr = new TimeRange(timestamp, timestamp+1);
    } catch(IOException e) {
      // Will never happen
    }
    return this;
  }

  /**
   * Get all available versions.
   * @return this for invocation chaining
   */
  public Get setMaxVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Set the effective timestamp for this get.
   *
   * @return this for invocation chaining
   */
  public Get setEffectiveTS(long effectiveTS) {
    this.effectiveTS = effectiveTS;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @throws IOException if invalid number of versions
   * @return this for invocation chaining
   */
  public Get setMaxVersions(int maxVersions) throws IOException {
    if(maxVersions <= 0) {
      throw new IOException("maxVersions must be positive");
    }
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Set the maximum number of values to return per row per Column Family
   * @param limit the maximum number of values returned / row / CF
   * @return this for invocation chaining
   */
  public Get setMaxResultsPerColumnFamily(int limit) {
    this.storeLimit = limit;
    return this;
  }

  /**
   * Set offset for the row per Column Family.
   * @param offset is the number of kvs that will be skipped.
   * @return this for invocation chaining
   */
  public Get setRowOffsetPerColumnFamily(int offset) {
    this.storeOffset = offset;
    return this;
  }

  /**
   * Apply the specified server-side filter when performing the Get.
   * Only {@link Filter#filterKeyValue(KeyValue)} is called AFTER all tests
   * for ttl, column match, deletes and max versions have been run.
   * @param filter filter to run on the server
   * @return this for invocation chaining
   */
  public Get setFilter(Filter filter) {
    if (filter == null) return this;
    try {
      this.tFilter = TFilter.getTFilter(filter);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    try {
      this.filter = this.tFilter.getFilter();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  /* Accessors */

  /**
   * @return Filter
   */
  @ThriftField(9)
  public TFilter getTFilter() {
    return this.tFilter;
  }

  public Filter getFilter() {
    try {
      if (tFilter == null) return null;
      return this.tFilter.getFilter();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Method for retrieving the get's row
   * @return row
   */
  @Override
  @ThriftField(1)
  public byte[] getRow() {
    return this.row;
  }

  /**
   * Method for retrieving the get's RowLock
   * @return RowLock
   */
  public RowLock getRowLock() {
    return new RowLock(this.row, this.lockId);
  }

  /**
   * Method for retrieving the get's lockId
   * @return lockId
   */
  @ThriftField(2)
  public long getLockId() {
    return this.lockId;
  }

  /**
   * Method for retrieving the get's maximum number of version
   * @return the maximum number of version to fetch for this get
   */
  @ThriftField(3)
  public int getMaxVersions() {
    return this.maxVersions;
  }

  @ThriftField(4)
  public int getStoreLimit() {
    return this.storeLimit;
  }

  @ThriftField(5)
  public int getStoreOffset() {
    return this.storeOffset;
  }

  /**
   * @return the effective timestamp of this operation.
   */
  public long getEffectiveTS() {
    return this.effectiveTS;
  }

  /**
   * Method for retrieving the get's maximum number of values
   * to return per Column Family
   * @return the maximum number of values to fetch per CF
   */
  public int getMaxResultsPerColumnFamily() {
    return this.storeLimit;
  }

  /**
   * Method for retrieving the get's offset per row per column
   * family (#kvs to be skipped)
   * @return the row offset
   */
  public int getRowOffsetPerColumnFamily() {
    return this.storeOffset;
  }

  /**
   * Method for retrieving the get's TimeRange
   * @return timeRange
   */
  @ThriftField(6)
  public TimeRange getTimeRange() {
    return this.tr;
  }

  /**
   * Method for retrieving the keys in the familyMap
   * @return keys in the current familyMap
   */
  public Set<byte[]> familySet() {
    return this.familyMap.keySet();
  }

  /**
   * Method for retrieving the number of families to get from
   * @return number of families
   */
  public int numFamilies() {
    return this.familyMap.size();
  }

  /**
   * Method for checking if any families have been inserted into this Get
   * @return true if familyMap is non empty false otherwise
   */
  public boolean hasFamilies() {
    return !this.familyMap.isEmpty();
  }

  /**
   * Method for retrieving the get's familyMap
   * @return familyMap
   */
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return familyMap;
  }


  @ThriftField(7)
  public Map<byte[], Set<byte[]>> getSerializableFamilyMap() {
    Map<byte[], Set<byte[]>> serializableMap = new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
    for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
      Set<byte[]> serilizableSet = new HashSet<byte[]>();
      for (byte[] setEntry : entry.getValue()) {
        serilizableSet.add(setEntry);
      }
      serializableMap.put(entry.getKey(), serilizableSet);
    }
    return serializableMap;
  }

  @ThriftField(8)
  public long geEffectiveTS() {
    return this.effectiveTS;
  }

  /**
   * Compile the table and column family (i.e. schema) information
   * into a String. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * @return Map
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<String, Object>();
    List<String> families = new ArrayList<String>(this.familyMap.size());
    map.put("families", families);
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
      families.add(Bytes.toStringBinary(entry.getKey()));
    }
    return map;
  }

  /**
   * Compile the details beyond the scope of getFingerprint (row, columns,
   * timestamps, etc.) into a Map along with the fingerprinted information.
   * Useful for debugging, logging, and administration tools.
   * @param maxCols a limit on the number of columns output prior to truncation
   * @return Map
   */
  @Override
  public Map<String, Object> toMap(int maxCols) {
    // we start with the fingerprint map and build on top of it.
    Map<String, Object> map = getFingerprint();
    // replace the fingerprint's simple list of families with a
    // map from column families to lists of qualifiers and kv details
    Map<String, List<String>> columns = new HashMap<String, List<String>>();
    map.put("families", columns);
    // add scalar information first
    map.put("row", Bytes.toStringBinary(this.row));
    map.put("maxVersions", this.maxVersions);
    map.put("storeLimit", this.storeLimit);
    List<Long> timeRange = new ArrayList<Long>(2);
    timeRange.add(this.tr.getMin());
    timeRange.add(this.tr.getMax());
    map.put("timeRange", timeRange);
    int colCount = 0;
    // iterate through affected families and add details
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
      List<String> familyList = new ArrayList<String>();
      columns.put(Bytes.toStringBinary(entry.getKey()), familyList);
      if(entry.getValue() == null) {
        colCount++;
        --maxCols;
        familyList.add("ALL");
      } else {
        colCount += entry.getValue().size();
        if (maxCols <= 0) {
          continue;
        }
        for (byte[] column : entry.getValue()) {
          if (--maxCols <= 0) {
            continue;
          }
          familyList.add(Bytes.toStringBinary(column));
        }
      }
    }
    map.put("totalColumns", colCount);
    if (this.filter != null) {
      map.put("filter", this.filter.toString());
    }
    return map;
  }

  @Override
  public int compareTo(Row p) {
    return Bytes.compareTo(this.getRow(), p.getRow());
  }

  //Writable
  @Override
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > GET_VERSION) {
      throw new IOException("unsupported version");
    }
    this.row = Bytes.readByteArray(in);
    this.lockId = in.readLong();
    this.maxVersions = in.readInt();
    if (version >= STORE_LIMIT_VERSION) {
      this.storeLimit = in.readInt();
    }
    if (version >= STORE_OFFSET_VERSION) {
      this.storeOffset = in.readInt();
    }
    if (version >= FLASHBACK_VERSION) {
      effectiveTS = in.readLong();
    }
    boolean hasFilter = in.readBoolean();
    if (hasFilter) {
      this.filter = (Filter)createForName(Bytes.toString(Bytes.readByteArray(in)));
      this.filter.readFields(in);
    }
    setFilter(filter);
    this.tr = new TimeRange();
    tr.readFields(in);
    int numFamilies = in.readInt();
    this.familyMap =
      new TreeMap<byte[],NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numFamilies; i++) {
      byte[] family = Bytes.readByteArray(in);
      boolean hasColumns = in.readBoolean();
      NavigableSet<byte[]> set = null;
      if(hasColumns) {
        int numColumns = in.readInt();
        set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for(int j=0; j<numColumns; j++) {
          byte[] qualifier = Bytes.readByteArray(in);
          set.add(qualifier);
        }
      }
      this.familyMap.put(family, set);
    }
  }

  @Override
  public void write(final DataOutput out)
  throws IOException {
    // We try to talk a protocol version as low as possible so that we can be
    // backward compatible as far as possible.
    byte version = (byte) 1;
    if (effectiveTS != HConstants.LATEST_TIMESTAMP) {
      version = FLASHBACK_VERSION;
    } else if (this.storeOffset != 0) {
      version = STORE_OFFSET_VERSION;
    } else if (this.storeLimit != -1) {
      version = STORE_LIMIT_VERSION;
    }
    out.writeByte(version);
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.lockId);
    out.writeInt(this.maxVersions);
    if (version >= STORE_LIMIT_VERSION) {
      out.writeInt(this.storeLimit);
    }
    if (version >= STORE_OFFSET_VERSION) {
      out.writeInt(this.storeOffset);
    }
    if (version >= FLASHBACK_VERSION) {
      out.writeLong(effectiveTS);
    }
    if(this.filter == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      Bytes.writeByteArray(out, Bytes.toBytes(filter.getClass().getName()));
      filter.write(out);
    }
    tr.write(out);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte[], NavigableSet<byte[]>> entry :
      familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      NavigableSet<byte[]> columnSet = entry.getValue();
      if(columnSet == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        out.writeInt(columnSet.size());
        for(byte[] qualifier : columnSet) {
          Bytes.writeByteArray(out, qualifier);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Writable createForName(String className) {
    try {
      Class<? extends Writable> clazz =
        (Class<? extends Writable>) Class.forName(className);
      return WritableFactories.newInstance(clazz, new Configuration());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can't find class " + className);
    }
  }

  /**
   * Adds an array of columns specified the old format, family:qualifier.
   * <p>
   * Overrides previous calls to addFamily for any families in the input.
   * @param columns array of columns, formatted as <pre>family:qualifier</pre>
   * @deprecated issue multiple {@link #addColumn(byte[], byte[])} instead
   * @return this for invocation chaining
   */
  @Deprecated
  public Get addColumns(byte[][] columns) {
    if (columns == null) return this;
    for (byte[] column : columns) {
      try {
        addColumn(column);
      } catch (Exception ignored) {
      }
    }
    return this;
  }

  /**
   *
   * @param column Old format column.
   * @return This.
   * @deprecated use {@link #addColumn(byte[], byte[])} instead
   */
  @Deprecated
  public Get addColumn(final byte[] column) {
    if (column == null) return this;
    byte[][] split = KeyValue.parseColumn(column);
    if (split.length > 1 && split[1] != null && split[1].length > 0) {
      addColumn(split[0], split[1]);
    } else {
      addFamily(split[0]);
    }
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(row, lockId, maxVersions, storeLimit, storeOffset,
        tr, familyMap, effectiveTS);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Get other = (Get) obj;
    if (effectiveTS != other.effectiveTS)
      return false;
    if (familyMap == null) {
      if (other.familyMap != null)
        return false;
    } else if (!familyMap.equals(other.familyMap))
      return false;
    if (filter == null) {
      if (other.filter != null)
        return false;
    } else if (!filter.equals(other.filter))
      return false;
    if (lockId != other.lockId)
      return false;
    if (maxVersions != other.maxVersions)
      return false;
    if (!Arrays.equals(row, other.row))
      return false;
    if (storeLimit != other.storeLimit)
      return false;
    if (storeOffset != other.storeOffset)
      return false;
    if (tr == null) {
      if (other.tr != null)
        return false;
    } else if (!tr.equals(other.tr))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Get [row=" + Arrays.toString(row) + ", lockId=" + lockId
        + ", maxVersions=" + maxVersions + ", storeLimit=" + storeLimit
        + ", storeOffset=" + storeOffset + ", filter=" + filter + ", tr=" + tr
        + ", familyMap=" + familyMap + ", effectiveTS=" + effectiveTS + "]";
  }

  /**
   * Builder class for Get
   */
  public static class Builder {
    private byte[] row;
    private long lockId;
    private int maxVersions;
    private int storeLimit;
    private int storeOffset;
    private TimeRange tr;
    private Map<byte[], Set<byte[]>> familyMap;
    private long effectiveTS;
    private TFilter tFilter;

    public Builder(byte[] row) {
      this.row = row;
      this.lockId = -1L;
      this.maxVersions = 1;
      this.storeLimit = -1;
      this.storeOffset = 0;
      this.tr = new TimeRange();
      this.effectiveTS = HConstants.LATEST_TIMESTAMP;
    }

    public Builder setLockId(long lockId) {
      this.lockId = lockId;
      return this;
    }

    public Builder setMaxVersions(int maxVersions) {
      this.maxVersions = maxVersions;
      return this;
    }

    public Builder setMaxVersions() {
      this.maxVersions = Integer.MAX_VALUE;
      return this;
    }

    public Builder setStoreLimit(int storeLimit) {
      this.storeLimit = storeLimit;
      return this;
    }

    public Builder setStoreOffset(int storeOffset) {
      this.storeOffset = storeOffset;
      return this;
    }

    public Builder setTr(TimeRange tr) {
      this.tr = tr;
      return this;
    }

    public Builder setFamilyMap(Map<byte[], Set<byte[]>> familyMap) {
      this.familyMap = familyMap;
      return this;
    }

    /**
     * Overrides previous calls to addColumn for this family.
     * @param family family name
     * @return the Build object
     */
    public Builder addFamily(byte[] family) {
      if (this.familyMap == null) {
        this.familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      }
      this.familyMap.put(family, new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));
      return this;
    }

    /**
     * Overrides previous calls to addFamily for this family.
     * @param family family name
     * @param qualifier column qualifier
     * @return the Build object
     */
    public Builder addColumn(byte[] family, byte[] qualifier) {
      if (this.familyMap == null) {
        this.familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      }
      Set<byte[]> set = this.familyMap.get(family);
      if (set == null) {
        set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      }
      if (qualifier == null) {
        set.add(HConstants.EMPTY_BYTE_ARRAY);
      } else {
        set.add(qualifier);
      }
      this.familyMap.put(family, set);
      return this;
    }

    public Builder setEffectiveTS(long effectiveTS) {
      this.effectiveTS = effectiveTS;
      return this;
    }

    public Builder setFilter(Filter f) throws IOException {
      this.tFilter = TFilter.getTFilter(f);
      return this;
    }

    public Get create() {
      return new Get(this.row, this.lockId, this.maxVersions, this.storeLimit,
          this.storeOffset, this.tr, this.familyMap, this.effectiveTS, this.tFilter);
    }
  }
}
