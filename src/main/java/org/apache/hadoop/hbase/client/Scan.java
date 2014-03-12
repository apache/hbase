/*
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
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
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
 * Used to perform Scan operations.
 * <p>
 * All operations are identical to {@link Get} with the exception of
 * instantiation.  Rather than specifying a single row, an optional startRow
 * and stopRow may be defined.  If rows are not specified, the Scanner will
 * iterate over all rows.
 * <p>
 * To scan everything for each row, instantiate a Scan object.
 * <p>
 * To modify scanner caching for just this scan, use {@link #setCaching(int) setCaching}.
 * <p>
 * To further define the scope of what to get when scanning, perform additional
 * methods as outlined below.
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
 * To limit the maximum number of values returned for each call to next(),
 * execute {@link #setBatch(int) setBatch}.
 * <p>
 * To limit the maximum number of values returned per row per Column Family,
 * execute {@link #setMaxResultsPerColumnFamily(int) setMaxResultsPerColumnFamily}.
 * <p>
 * To add a filter, execute {@link #setFilter(org.apache.hadoop.hbase.filter.Filter) setFilter}.
 * <p>
 * Expert: To explicitly disable server-side block caching for this scan,
 * execute {@link #setCacheBlocks(boolean)}.
 */
@ThriftStruct
public class Scan extends Operation implements Writable {
  private static final byte STORE_LIMIT_VERSION = (byte)2;
  private static final byte STORE_OFFSET_VERSION = (byte)3;
  private static final byte RESPONSE_SIZE_VERSION = (byte)4;
  private static final byte FLASHBACK_VERSION = (byte) 5;
  private static final byte PREFETCH_VERSION = (byte) 6;
  private static final byte PRELOAD_VERSION = (byte) 7;
  private static final byte SCAN_VERSION = PRELOAD_VERSION;

  private byte [] startRow = HConstants.EMPTY_START_ROW;
  private byte [] stopRow  = HConstants.EMPTY_END_ROW;
  private int maxVersions = 1;
  private int batch = -1;
  private int storeLimit = -1;
  private int storeOffset = 0;
  private int caching = -1;
  private boolean serverPrefetching = false;
  private int maxResponseSize = HConstants.DEFAULT_HBASE_SCANNER_MAX_RESULT_SIZE;
  private int currentPartialResponseSize = 0;
  private boolean partialRow = false;
  private boolean cacheBlocks = true;
  private Filter filter = null;
  private TFilter tFilter = null;
  private TimeRange tr = new TimeRange();
  private Map<byte [], NavigableSet<byte []>> familyMap =
    new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
  private long effectiveTS = HConstants.LATEST_TIMESTAMP;
  /*This tells whether the scanner will preload blocks or not*/
  private boolean preloadBlocks = false;

  /**
   * Create a Scan operation across all rows.
   */
  public Scan() {}

  public Scan(byte [] startRow, Filter filter) {
    this(startRow);
    this.setFilter(filter);
  }

  /**
   * Create a Scan operation starting at the specified row.
   * <p>
   * If the specified row does not exist, the Scanner will start from the
   * next closest row after the specified row.
   * @param startRow row to start scanner at or after
   */
  public Scan(byte [] startRow) {
    this.startRow = startRow;
  }

  /**
   * Create a Scan operation for the range of rows specified.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  public Scan(byte [] startRow, byte [] stopRow) {
    this.startRow = startRow;
    this.stopRow = stopRow;
  }

  /**
   * Creates a new instance of this class while copying all values.
   *
   * @param scan  The scan instance to copy from.
   * @throws IOException When copying the values fails.
   */
  public Scan(Scan scan) throws IOException {
    startRow = scan.getStartRow();
    stopRow  = scan.getStopRow();
    maxVersions = scan.getMaxVersions();
    batch = scan.getBatch();
    storeLimit = scan.getMaxResultsPerColumnFamily();
    storeOffset = scan.getRowOffsetPerColumnFamily();
    caching = scan.getCaching();
    serverPrefetching = scan.getServerPrefetching();
    maxResponseSize = scan.getMaxResponseSize();
    partialRow = scan.isPartialRow();
    cacheBlocks = scan.getCacheBlocks();
    setFilter(scan.getFilter());
    TimeRange ctr = scan.getTimeRange();
    tr = new TimeRange(ctr.getMin(), ctr.getMax());
    Map<byte[], NavigableSet<byte[]>> fams = scan.getFamilyMap();
    for (Map.Entry<byte[],NavigableSet<byte[]>> entry : fams.entrySet()) {
      byte [] fam = entry.getKey();
      NavigableSet<byte[]> cols = entry.getValue();
      if (cols != null && cols.size() > 0) {
        for (byte[] col : cols) {
          addColumn(fam, col);
        }
      } else {
        addFamily(fam);
      }
    }
    effectiveTS = scan.getEffectiveTS();
  }

  /**
   * Builds a scan object with the same specs as get.
   * @param get get to model scan after
   */
  public Scan(Get get) {
    this.startRow = get.getRow();
    this.stopRow = Bytes.nextOf(get.getRow());
    setFilter(get.getFilter());
    this.maxVersions = get.getMaxVersions();
    this.storeLimit = get.getMaxResultsPerColumnFamily();
    this.storeOffset = get.getRowOffsetPerColumnFamily();
    this.tr = get.getTimeRange();
    this.familyMap = get.getFamilyMap();
    this.effectiveTS = get.getEffectiveTS();
  }

  @ThriftConstructor
  public Scan(@ThriftField(1) final byte[] startRow,
              @ThriftField(2) final byte[] stopRow,
              @ThriftField(3) final int maxVersions,
              @ThriftField(4) final int batch,
              @ThriftField(5) final int caching,
              @ThriftField(6) final int storeLimit,
              @ThriftField(7) final int storeOffset,
              @ThriftField(8) final boolean serverPrefetching,
              @ThriftField(9) final int maxResponseSize,
              @ThriftField(10) final boolean partialRow,
              @ThriftField(11) final boolean cacheBlocks,
              @ThriftField(12) final TimeRange tr,
              @ThriftField(13) final Map<byte[], Set<byte[]>> familyMap,
              @ThriftField(14) final long effectiveTS,
              @ThriftField(15) final int currentPartialResponseSize,
              @ThriftField(16) final TFilter tFilter,
              @ThriftField(17) final boolean preloadBlocks) throws IOException {
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.maxVersions = maxVersions;
    this.batch = batch;
    this.storeLimit = storeLimit;
    this.storeOffset = storeOffset;
    this.caching = caching;
    this.serverPrefetching = serverPrefetching;
    this.maxResponseSize = maxResponseSize;
    this.partialRow = partialRow;
    this.cacheBlocks = cacheBlocks;
    this.tr = new TimeRange(tr.getMin(), tr.getMax());
    this.effectiveTS = effectiveTS;
    this.currentPartialResponseSize = currentPartialResponseSize;
    for (Map.Entry<byte[], Set<byte[]>> e : familyMap.entrySet()) {
      NavigableSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      for (byte[] setEntry : e.getValue()) {
        set.add(setEntry);
      }
      this.familyMap.put(e.getKey(), set);
    }
    this.tFilter = tFilter;
    this.filter = tFilter;
    this.preloadBlocks = preloadBlocks;
  }

  public boolean isGetScan() {
    return Bytes.isNext(startRow, stopRow);
  }

  /**
   * Get all columns from the specified family.
   * <p>
   * Overrides previous calls to addColumn for this family.
   * @param family family name
   * @return this
   */
  public Scan addFamily(byte [] family) {
    familyMap.remove(family);
    familyMap.put(family, new TreeSet<byte []>(Bytes.BYTES_COMPARATOR));
    return this;
  }

  /**
   * Get the column from the specified family with the specified qualifier.
   * <p>
   * Overrides previous calls to addFamily for this family.
   * @param family family name
   * @param qualifier column qualifier
   * @return this
   */
  public Scan addColumn(byte [] family, byte [] qualifier) {
    NavigableSet<byte []> set = familyMap.get(family);
    if (set == null) {
      set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
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
   * [minStamp, maxStamp).  Note, default maximum versions to return is 1.  If
   * your time range spans more than one version and you want all versions
   * returned, up the number of versions beyond the defaut.
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   * @return this
   */
  public Scan setTimeRange(long minStamp, long maxStamp)
  throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * This tells whether this scanner will preload data blocks or not.
   * @return status of preload data blocks
   */
  public boolean isPreloadBlocks() {
    return preloadBlocks;
  }

  /**
   * Set whether this scanner should preload data blocks or not.
   * @param value
   */
  public void setPreloadBlocks(boolean value) {
    preloadBlocks = true;
  }

  /**
   * Get versions of columns with the specified timestamp. Note, default maximum
   * versions to return is 1.  If your time range spans more than one version
   * and you want all versions returned, up the number of versions beyond the
   * defaut.
   * @param timestamp version timestamp
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   * @return this
   */
  public Scan setTimeStamp(long timestamp) {
    try {
      tr = new TimeRange(timestamp, timestamp+1);
    } catch(IOException e) {
      // Will never happen
    }
    return this;
  }

  /**
   * Set the start row of the scan.
   * @param startRow row to start scan on, inclusive
   * @return this
   */
  public Scan setStartRow(byte [] startRow) {
    this.startRow = startRow;
    return this;
  }

  /**
   * Set the stop row.
   * @param stopRow row to end at (exclusive)
   * @return this
   */
  public Scan setStopRow(byte [] stopRow) {
    this.stopRow = stopRow;
    return this;
  }

  /**
   * Set the effective timestamp of this operation
   *
   * @return this
   */
  public Scan setEffectiveTS(long effectiveTS) {
    this.effectiveTS = effectiveTS;
    return this;
  }

  /**
   * Get all available versions.
   * @return this
   */
  public Scan setMaxVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @return this
   */
  public Scan setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Set the maximum number of values to return for each call to next()
   * @param batch the maximum number of values
   */
  public void setBatch(int batch) {
    if (this.hasFilter() && this.filter.hasFilterRow()) {
      throw new IncompatibleFilterException(
        "Cannot set batch on a scan using a filter" +
        " that returns true for filter.hasFilterRow");
    }
    this.batch = batch;
  }

  /**
   * Set the maximum number of values to return per row per Column Family
   * @param limit the maximum number of values returned / row / CF
   */
  public void setMaxResultsPerColumnFamily(int limit) {
    this.storeLimit = limit;
  }

  /**
   * Set offset for the row per Column Family.
   * @param offset is the number of kvs that will be skipped.
   */
  public void setRowOffsetPerColumnFamily(int offset) {
    this.storeOffset = offset;
  }

  /**
   * Set the number of rows for caching that will be passed to scanners.
   * If not set, the default setting from {@link HTable#getScannerCaching()} will apply.
   * Higher caching values will enable faster scanners but will use more memory.
   * @param caching the number of rows for caching
   */
  public void setCaching(int caching) {
    this.caching = caching;
    this.partialRow = false;
    this.maxResponseSize = HConstants.DEFAULT_HBASE_SCANNER_MAX_RESULT_SIZE;
  }

  /**
   * This is technically not the max available memory setting, more of a hint.
   * We will add KV's till we exceed this setting if partialRow is true,
   * and add entire rows till we exceed this setting if partialRow is false.
   * !!!NOTE!!!: this call will overwrite the caching setting and set it as
   * int.max_value. If you really want row-based constraint as well, use
   * setCaching(int caching), which will reset maxResponseSize to match your
   * configuration and disable partial row.
   */
  public void setCaching(int responseSize, boolean partialRow) {
    this.maxResponseSize = responseSize;
    this.partialRow = partialRow;
    this.caching = Integer.MAX_VALUE;
  }

  /**
   * Set if pre-fetching is enabled on the region server. If enabled, the
   * region server will try to read the next scan result ahead of time. This
   * improves scan performance if we are doing large scans.
   * @param enablePrefetching if pre-fetching is enabled or not
   */
  public void setServerPrefetching(boolean enablePrefetching) {
    this.serverPrefetching = enablePrefetching;
  }

  @ThriftField(8)
  public boolean getServerPrefetching() {
    return serverPrefetching;
  }

  /**
   * @return maximum response size that client can handle for a single call to next()
   */
  @ThriftField(9)
  public int getMaxResponseSize() {
    return this.maxResponseSize;
  }

  /**
   * @return whether the last row can be partially transferred for a single call to next()
   */
  public boolean isPartialRow() {
    return this.partialRow;
  }

  /**
   * Set currentPartialResponseSize to accumulated response size
   * for all the KeyValue pairs collected so far. This is only used at
   * server side, and not used as a client API.
   * @param responseSize
   */
  public void setCurrentPartialResponseSize(int responseSize) {
    this.currentPartialResponseSize = responseSize;
  }

  /*
   * Get current PartialResponseSize. This is only used at server side,
   * and not used as a client API.
   */
  @ThriftField(15)
  public int getCurrentPartialResponseSize() {
    return this.currentPartialResponseSize;
  }

  @ThriftField(16)
  public TFilter getTFilter() {
    return this.tFilter;
  }

  @ThriftField(17)
  public boolean getPreloadBlocks() {
    return this.preloadBlocks;
  }

  /**
   * Apply the specified server-side filter when performing the Scan.
   * @param filter filter to run on the server
   * @return this
   * @throws IOException
   */
  public Scan setFilter(Filter filter) {
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

  /**
   * Setting the familyMap
   * @param familyMap map of family to qualifier
   * @return this
   */
  public Scan setFamilyMap(Map<byte [], NavigableSet<byte []>> familyMap) {
    this.familyMap = familyMap;
    return this;
  }

  /**
   * Getting the familyMap
   * @return familyMap
   */
  @ThriftField(13)
  public Map<byte [], NavigableSet<byte []>> getFamilyMap() {
    return this.familyMap;
  }

  /**
   * @return the number of families in familyMap
   */
  public int numFamilies() {
    if(hasFamilies()) {
      return this.familyMap.size();
    }
    return 0;
  }

  /**
   * @return true if familyMap is non empty, false otherwise
   */
  public boolean hasFamilies() {
    return !this.familyMap.isEmpty();
  }

  /**
   * @return the keys of the familyMap
   */
  public byte[][] getFamilies() {
    if(hasFamilies()) {
      return this.familyMap.keySet().toArray(new byte[0][0]);
    }
    return null;
  }

  /**
   * @return the startrow
   */
  @ThriftField(1)
  public byte [] getStartRow() {
    return this.startRow;
  }

  /**
   * @return the stoprow
   */
  @ThriftField(2)
  public byte [] getStopRow() {
    return this.stopRow;
  }

  /**
   * @return the max number of versions to fetch
   */
  @ThriftField(3)
  public int getMaxVersions() {
    return this.maxVersions;
  }

  /**
   * @return the effective timestamp for this operation
   */
  @ThriftField(14)
  public long getEffectiveTS() {
    return this.effectiveTS;
  }

  /**
   * @return maximum number of values to return for a single call to next()
   */
  @ThriftField(4)
  public int getBatch() {
    return this.batch;
  }

  /**
   * @return maximum number of values to return per row per CF
   */
  public int getMaxResultsPerColumnFamily() {
    return this.storeLimit;
  }

  /**
   * Method for retrieving the scan's offset per row per column
   * family (#kvs to be skipped)
   * @return row offset
   */
  public int getRowOffsetPerColumnFamily() {
    return this.storeOffset;
  }

  /**
   * @return caching the number of rows fetched when calling next on a scanner
   */
  @ThriftField(5)
  public int getCaching() {
    return this.caching;
  }

  /**
   * @return TimeRange
   */
  @ThriftField(12)
  public TimeRange getTimeRange() {
    return this.tr;
  }

  /**
   * @return RowFilter
   */
  public Filter getFilter() {
    try {
      if (tFilter == null) return null;
      return tFilter.getFilter();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return true is a filter has been specified, false if not
   */
  public boolean hasFilter() {
    return filter != null;
  }

  /**
   * Set whether blocks should be cached for this Scan.
   * <p>
   * This is true by default.  When true, default settings of the table and
   * family are used (this will never override caching blocks if the block
   * cache is disabled for that family or entirely).
   *
   * @param cacheBlocks if false, default settings are overridden and blocks
   * will not be cached
   */
  public void setCacheBlocks(boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
  }

  /**
   * Get whether blocks should be cached for this Scan.
   * @return true if default caching should be used, false if blocks should not
   * be cached
   */
  @ThriftField(11)
  public boolean getCacheBlocks() {
    return cacheBlocks;
  }

  @ThriftField(6)
  public int getStoreLimit() {
    return storeLimit;
  }

  @ThriftField(7)
  public int getStoreOffset() {
    return storeOffset;
  }

  @ThriftField(10)
  public boolean getPartialRow() {
    return this.partialRow;
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
    List<String> families = new ArrayList<String>();
    if (this.familyMap.isEmpty()) {
      map.put("families", "ALL");
      return map;
    } else {
      map.put("families", families);
    }
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
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
    // start with the fingerpring map and build on top of it
    Map<String, Object> map = getFingerprint();
    // map from families to column list replaces fingerprint's list of families
    Map<String, List<String>> familyColumns = new HashMap<String, List<String>>();
    map.put("families", familyColumns);
    // add scalar information first
    map.put("startRow", Bytes.toStringBinary(this.startRow));
    map.put("stopRow", Bytes.toStringBinary(this.stopRow));
    map.put("maxVersions", this.maxVersions);
    map.put("batch", this.batch);
    map.put("caching", this.caching);
    map.put("cacheBlocks", this.cacheBlocks);
    map.put("storeLimit", this.storeLimit);
    map.put("maxResponseSize", this.maxResponseSize);
    map.put("partialRow", this.partialRow);
    List<Long> timeRange = new ArrayList<Long>();
    timeRange.add(this.tr.getMin());
    timeRange.add(this.tr.getMax());
    map.put("timeRange", timeRange);
    int colCount = 0;
    // iterate through affected families and list out up to maxCols columns
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
      List<String> columns = new ArrayList<String>();
      familyColumns.put(Bytes.toStringBinary(entry.getKey()), columns);
      if(entry.getValue() == null) {
        colCount++;
        --maxCols;
        columns.add("ALL");
      } else {
        colCount += entry.getValue().size();
        if (maxCols <= 0) {
          continue;
        }
        for (byte [] column : entry.getValue()) {
          if (--maxCols <= 0) {
            continue;
          }
          columns.add(Bytes.toStringBinary(column));
        }
      }
    }
    map.put("totalColumns", colCount);
    if (this.filter != null) {
      map.put("filter", this.filter.toString());
    }
    return map;
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

  //Writable
  @Override
  public void readFields(final DataInput in)
  throws IOException {
    int version = in.readByte();
    if (version > (int)SCAN_VERSION) {
      throw new IOException("version not supported");
    }
    this.startRow = Bytes.readByteArray(in);
    this.stopRow = Bytes.readByteArray(in);
    this.maxVersions = in.readInt();
    this.batch = in.readInt();
    if (version >= STORE_LIMIT_VERSION) {
      this.storeLimit = in.readInt();
    }
    if (version >= STORE_OFFSET_VERSION) {
      this.storeOffset = in.readInt();
    }
    if (version >= RESPONSE_SIZE_VERSION) {
      this.maxResponseSize = in.readInt();
      this.partialRow = in.readBoolean();
    }
    if (version >= FLASHBACK_VERSION) {
      effectiveTS = in.readLong();
    }
    if (version >= PREFETCH_VERSION) {
      serverPrefetching = in.readBoolean();
    }
    this.caching = in.readInt();
    this.cacheBlocks = in.readBoolean();
    if (version >= PRELOAD_VERSION) {
      this.preloadBlocks = in.readBoolean();
    }
    if (in.readBoolean()) {
      this.filter = (Filter)createForName(Bytes.toString(Bytes.readByteArray(in)));
      this.filter.readFields(in);
    }
    setFilter(filter);

    this.tr = new TimeRange();
    tr.readFields(in);
    int numFamilies = in.readInt();
    this.familyMap =
      new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numFamilies; i++) {
      byte [] family = Bytes.readByteArray(in);
      int numColumns = in.readInt();
      TreeSet<byte []> set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      for(int j=0; j<numColumns; j++) {
        byte [] qualifier = Bytes.readByteArray(in);
        set.add(qualifier);
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
    if (preloadBlocks) {
      version = PRELOAD_VERSION;
    } else if (serverPrefetching) {
      version = PREFETCH_VERSION;
    } else if (effectiveTS != HConstants.LATEST_TIMESTAMP) {
      version = FLASHBACK_VERSION;
    } else if (this.maxResponseSize
        != HConstants.DEFAULT_HBASE_SCANNER_MAX_RESULT_SIZE) {
      version = (byte) RESPONSE_SIZE_VERSION;
    } else if (this.storeOffset != 0) {
      version = STORE_OFFSET_VERSION;
    } else if (this.storeLimit != -1) {
      version = STORE_LIMIT_VERSION;
    }

    out.writeByte(version);
    Bytes.writeByteArray(out, this.startRow);
    Bytes.writeByteArray(out, this.stopRow);
    out.writeInt(this.maxVersions);
    out.writeInt(this.batch);
    if (version >= STORE_LIMIT_VERSION) {
      out.writeInt(this.storeLimit);
    }
    if (version >= STORE_OFFSET_VERSION) {
      out.writeInt(this.storeOffset);
    }
    if (version >= RESPONSE_SIZE_VERSION) {
      out.writeInt(this.maxResponseSize);
      out.writeBoolean(this.partialRow);
    }
    if (version >= FLASHBACK_VERSION) {
      out.writeLong(effectiveTS);
    }
    if (version >= PREFETCH_VERSION) {
      out.writeBoolean(serverPrefetching);
    }
    out.writeInt(this.caching);
    out.writeBoolean(this.cacheBlocks);
    if (version >= PRELOAD_VERSION) {
      out.writeBoolean(this.preloadBlocks);
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
    for(Map.Entry<byte [], NavigableSet<byte []>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      NavigableSet<byte []> columnSet = entry.getValue();
      if(columnSet != null){
        out.writeInt(columnSet.size());
        for(byte [] qualifier : columnSet) {
          Bytes.writeByteArray(out, qualifier);
        }
      } else {
        out.writeInt(0);
      }
    }
  }

   /**
   * Parses a combined family and qualifier and adds either both or just the
   * family in case there is not qualifier. This assumes the older colon
   * divided notation, e.g. "data:contents" or "meta:".
   * <p>
   * Note: It will through an error when the colon is missing.
   *
   * @param familyAndQualifier family and qualifier
   * @return A reference to this instance.
   * @throws IllegalArgumentException When the colon is missing.
   * @deprecated use {@link #addColumn(byte[], byte[])} instead
   */
  @Deprecated
  public Scan addColumn(byte[] familyAndQualifier) {
    byte [][] fq = KeyValue.parseColumn(familyAndQualifier);
    if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
      addColumn(fq[0], fq[1]);
    } else {
      addFamily(fq[0]);
    }
    return this;
  }

  /**
   * Adds an array of columns specified using old format, family:qualifier.
   * <p>
   * Overrides previous calls to addFamily for any families in the input.
   *
   * @param columns array of columns, formatted as <pre>family:qualifier</pre>
   * @deprecated issue multiple {@link #addColumn(byte[], byte[])} instead
   * @return this
   */
  @Deprecated
  public Scan addColumns(byte [][] columns) {
    for (byte[] column : columns) {
      addColumn(column);
    }
    return this;
  }

  /**
   * Convenience method to help parse old style (or rather user entry on the
   * command line) column definitions, e.g. "data:contents mime:". The columns
   * must be space delimited and always have a colon (":") to denote family
   * and qualifier.
   *
   * @param columns  The columns to parse.
   * @return A reference to this instance.
   * @deprecated use {@link #addColumn(byte[], byte[])} instead
   */
  @Deprecated
  public Scan addColumns(String columns) {
    String[] cols = columns.split(" ");
    for (String col : cols) {
      addColumn(Bytes.toBytes(col));
    }
    return this;
  }

  /**
   * Helps to convert the binary column families and qualifiers to a text
   * representation, e.g. "data:mimetype data:contents meta:". Binary values
   * are properly encoded using {@link Bytes#toBytesBinary(String)}.
   *
   * @return The columns in an old style string format.
   * @deprecated
   */
  @Deprecated
  public String getInputColumns() {
    StringBuilder cols = new StringBuilder("");
    for (Map.Entry<byte[], NavigableSet<byte[]>> e :
      familyMap.entrySet()) {
      byte[] fam = e.getKey();
      if (cols.length() > 0) cols.append(" ");
      NavigableSet<byte[]> quals = e.getValue();
      // check if this family has qualifiers
      if (quals != null && quals.size() > 0) {
        StringBuilder cs = new StringBuilder("");
        for (byte[] qual : quals) {
          if (cs.length() > 0) cs.append(" ");
          // encode values to make parsing easier later
          cs.append(Bytes.toStringBinary(fam)).append(":").append(Bytes.toStringBinary(qual));
        }
        cols.append(cs);
      } else {
        // only add the family but with old style delimiter
        cols.append(Bytes.toStringBinary(fam)).append(":");
      }
    }
    return cols.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(batch, cacheBlocks, caching,
      currentPartialResponseSize, effectiveTS, familyMap, filter, tFilter,
      maxResponseSize, maxVersions, partialRow, serverPrefetching,
      startRow, stopRow, storeLimit, storeOffset, tr);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Scan other = (Scan) obj;
    if (batch != other.batch) {
      return false;
    }
    if (cacheBlocks != other.cacheBlocks) {
      return false;
    }
    if (caching != other.caching) {
      return false;
    }
    if (currentPartialResponseSize != other.currentPartialResponseSize) {
      return false;
    }
    if (effectiveTS != other.effectiveTS) {
      return false;
    }
    if (familyMap == null) {
      if (other.familyMap != null) {
        return false;
      }
    } else if (!familyMap.equals(other.familyMap)) {
      return false;
    }
    if (filter == null) {
      if (other.filter != null) {
        return false;
      }
    } else if (!filter.equals(other.filter)) {
      return false;
    }
    if (maxResponseSize != other.maxResponseSize) {
      return false;
    }
    if (maxVersions != other.maxVersions) {
      return false;
    }
    if (partialRow != other.partialRow) {
      return false;
    }
    if (serverPrefetching != other.serverPrefetching) {
      return false;
    }
    if (!Arrays.equals(startRow, other.startRow)) {
      return false;
    }
    if (!Arrays.equals(stopRow, other.stopRow)) {
      return false;
    }
    if (storeLimit != other.storeLimit) {
      return false;
    }
    if (storeOffset != other.storeOffset) {
      return false;
    }
    if (tFilter == null) {
      if (other.tFilter != null) {
        return false;
      }
    } else if (!tFilter.equals(other.tFilter)) {
      return false;
    }
    if (tr == null) {
      if (other.tr != null) {
        return false;
      }
    } else if (!tr.equals(other.tr)) {
      return false;
    }
    return true;
  }

  public static class Builder {
    private byte[] startRow = HConstants.EMPTY_START_ROW;
    private byte[] stopRow = HConstants.EMPTY_END_ROW;
    private int maxVersions = 1;
    private int batch = -1;
    private int storeLimit = -1;
    private int storeOffset = 0;
    private int caching = -1;
    private boolean serverPrefetching = false;
    private int maxResponseSize = HConstants.DEFAULT_HBASE_SCANNER_MAX_RESULT_SIZE;
    private int currentPartialResponseSize = 0;
    private boolean partialRow = false;
    private boolean cacheBlocks = true;
    private Filter filter = null;
    private TimeRange tr = new TimeRange();
    private Map<byte[], Set<byte[]>> familyMap = new TreeMap<byte[], Set<byte[]>>(
        Bytes.BYTES_COMPARATOR);
    private long effectiveTS = HConstants.LATEST_TIMESTAMP;
    private TFilter tFilter = null;
    private boolean preloadBlocks = false;

    public Builder() {
    }

    /**
     * {@link Scan#setBatch(int)}
     * @param batch
     * @return
     */
    public Builder setBatch(int batch) {
      if ((this.filter != null) && this.filter.hasFilterRow()) {
        throw new IncompatibleFilterException(
            "Cannot set batch on a scan using a filter"
                + " that returns true for filter.hasFilterRow");
      }
      this.batch = batch;
      return this;
    }

    /**
     * {@link Scan#setCacheBlocks(boolean)}
     * @param cacheBlocks
     * @return
     */
    public Builder setCacheBlocks(boolean cacheBlocks) {
      this.cacheBlocks = cacheBlocks;
      return this;
    }

    /**
     * {@link Scan#setCaching(int)}
     * @param caching
     * @return
     */
    public Builder setCaching(int caching) {
      this.caching = caching;
      this.partialRow = false;
      this.maxResponseSize = HConstants.DEFAULT_HBASE_SCANNER_MAX_RESULT_SIZE;
      return this;
    }

    /**
     * Builder function to set the Response Size of the next call
     * @param responseSize
     * @return
     */
    public Builder setResponseSize(int responseSize) {
      this.maxResponseSize = responseSize;
      this.caching = Integer.MAX_VALUE;
      return this;
    }

    /**
     * Setter function to set partial rows
     * @param partialRow
     * @return
     */
    public Builder setPartialRow(boolean partialRow) {
      this.partialRow = partialRow;
      this.caching = Integer.MAX_VALUE;
      return this;
    }

    /**
     * {@link Scan#setCurrentPartialResponseSize(int)}
     * @param responseSize
     * @return
     */
    public Builder setCurrentPartialResponseSize(int responseSize) {
      this.currentPartialResponseSize = responseSize;
      return this;
    }

    /**
     * {@link Scan#setEffectiveTS(long)}
     * @param effectiveTS
     * @return
     */
    public Builder setEffectiveTS(long effectiveTS) {
      this.effectiveTS = effectiveTS;
      return this;
    }

    /**
     * {@link Scan#setFilter(Filter)}
     * @param filter
     * @return
     * @throws IOException
     */
    public Builder setFilter(Filter filter) throws IOException {
      this.tFilter = TFilter.getTFilter(filter);
      this.filter = tFilter;
      return this;
    }

    /**
     * {@link Scan#setMaxResultsPerColumnFamily(int)}
     * @param limit
     * @return
     */
    public Builder setMaxResultsPerColumnFamily(int limit) {
      this.storeLimit = limit;
      return this;
    }

    /**
     * {@link Scan#setMaxVersions()}
     * @return
     */
    public Builder setMaxVersions() {
      this.maxVersions = Integer.MAX_VALUE;
      return this;
    }

    /**
     * {@link Scan#setMaxVersions(int)}
     * @param maxVersions
     * @return
     */
    public Builder setMaxVersions(int maxVersions) {
      this.maxVersions = maxVersions;
      return this;
    }

    /**
     * {@link Scan#setRowOffsetPerColumnFamily(int)}
     * @param offset
     * @return
     */
    public Builder setRowOffsetPerColumnFamily(int offset) {
      this.storeOffset = offset;
      return this;
    }

    /**
     * {@link Scan#setServerPrefetching(boolean)}
     * @param enablePrefetching
     * @return
     */
    public Builder setServerPrefetching(boolean enablePrefetching) {
      this.serverPrefetching = enablePrefetching;
      return this;
    }

    /**
     * {@link Scan#setStartRow(byte[])}
     * @param startRow
     * @return
     */
    public Builder setStartRow(byte[] startRow) {
      this.startRow = startRow;
      return this;
    }

    /**
     * {@link Scan#setStopRow(byte[])}
     * @param stopRow
     * @return
     */
    public Builder setStopRow(byte[] stopRow) {
      this.stopRow = stopRow;
      return this;
    }

    /**
     * {@link Scan#setPreloadBlocks(boolean)}
     * @param preloadBlocks
     * @return
     */
    public Builder setPreloadBlocks(boolean preloadBlocks) {
      this.preloadBlocks = preloadBlocks;
      return this;
    }

    /**
     * {@link Scan#setTimeRange(long, long)}
     * @param minStamp
     * @param maxStamp
     * @return
     * @throws IOException
     */
    public Builder setTimeRange(long minStamp, long maxStamp)
        throws IOException {
      tr = new TimeRange(minStamp, maxStamp);
      return this;
    }

    /**
     * {@link Scan#setTimeStamp(long)}
     * @param timestamp
     * @return
     */
    public Builder setTimeStamp(long timestamp) {
      try {
        tr = new TimeRange(timestamp, timestamp + 1);
      } catch (IOException e) {
        // Will never happen
      }
      return this;
    }

    /**
     * {@link Scan#addFamily(byte[])}
     * @param family
     * @return
     */
    public Builder addFamily(byte [] family) {
      familyMap.remove(family);
      familyMap.put(family, new TreeSet<byte []>(Bytes.BYTES_COMPARATOR));
      return this;
    }

    /**
     * {@link Scan#addColumn(byte[], byte[])}
     * @param family
     * @param qualifier
     * @return
     */
    public Builder addColumn(byte[] family, byte[] qualifier) {
      Set<byte[]> set = familyMap.get(family);
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
     * Builder's create method which gives a Scan object that is created from
     * all the optional fields that were set on this Builder class.
     * @return
     * @throws IOException
     */
    public Scan create() throws IOException {
      return new Scan(this.startRow, this.stopRow, this.maxVersions, this.batch,
          this.caching, this.storeLimit, this.storeOffset,
          this.serverPrefetching, this.maxResponseSize, this.partialRow,
          this.cacheBlocks, this.tr, this.familyMap, this.effectiveTS,
          this.currentPartialResponseSize, this.tFilter, this.preloadBlocks);
    }
  }
}
