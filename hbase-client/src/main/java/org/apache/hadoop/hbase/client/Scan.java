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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to perform Scan operations.
 * <p>
 * All operations are identical to {@link Get} with the exception of instantiation. Rather than
 * specifying a single row, an optional startRow and stopRow may be defined. If rows are not
 * specified, the Scanner will iterate over all rows.
 * <p>
 * To get all columns from all rows of a Table, create an instance with no constraints; use the
 * {@link #Scan()} constructor. To constrain the scan to specific column families, call
 * {@link #addFamily(byte[]) addFamily} for each family to retrieve on your Scan instance.
 * <p>
 * To get specific columns, call {@link #addColumn(byte[], byte[]) addColumn} for each column to
 * retrieve.
 * <p>
 * To only retrieve columns within a specific range of version timestamps, call
 * {@link #setTimeRange(long, long) setTimeRange}.
 * <p>
 * To only retrieve columns with a specific timestamp, call {@link #setTimestamp(long) setTimestamp}
 * .
 * <p>
 * To limit the number of versions of each column to be returned, call {@link #readVersions(int)}.
 * <p>
 * To limit the maximum number of values returned for each call to next(), call
 * {@link #setBatch(int) setBatch}.
 * <p>
 * To add a filter, call {@link #setFilter(org.apache.hadoop.hbase.filter.Filter) setFilter}.
 * <p>
 * For small scan, it is deprecated in 2.0.0. Now we have a {@link #setLimit(int)} method in Scan
 * object which is used to tell RS how many rows we want. If the rows return reaches the limit, the
 * RS will close the RegionScanner automatically. And we will also fetch data when openScanner in
 * the new implementation, this means we can also finish a scan operation in one rpc call. And we
 * have also introduced a {@link #setReadType(ReadType)} method. You can use this method to tell RS
 * to use pread explicitly.
 * <p>
 * Expert: To explicitly disable server-side block caching for this scan, execute
 * {@link #setCacheBlocks(boolean)}.
 * <p>
 * <em>Note:</em> Usage alters Scan instances. Internally, attributes are updated as the Scan runs
 * and if enabled, metrics accumulate in the Scan instance. Be aware this is the case when you go to
 * clone a Scan instance or if you go to reuse a created Scan instance; safer is create a Scan
 * instance per usage.
 */
@InterfaceAudience.Public
public class Scan extends Query {
  private static final Logger LOG = LoggerFactory.getLogger(Scan.class);

  private static final String RAW_ATTR = "_raw_";

  private byte[] startRow = HConstants.EMPTY_START_ROW;
  private boolean includeStartRow = true;
  private byte[] stopRow  = HConstants.EMPTY_END_ROW;
  private boolean includeStopRow = false;
  private int maxVersions = 1;
  private int batch = -1;

  /**
   * Partial {@link Result}s are {@link Result}s must be combined to form a complete {@link Result}.
   * The {@link Result}s had to be returned in fragments (i.e. as partials) because the size of the
   * cells in the row exceeded max result size on the server. Typically partial results will be
   * combined client side into complete results before being delivered to the caller. However, if
   * this flag is set, the caller is indicating that they do not mind seeing partial results (i.e.
   * they understand that the results returned from the Scanner may only represent part of a
   * particular row). In such a case, any attempt to combine the partials into a complete result on
   * the client side will be skipped, and the caller will be able to see the exact results returned
   * from the server.
   */
  private boolean allowPartialResults = false;

  private int storeLimit = -1;
  private int storeOffset = 0;

  private static final String SCAN_ATTRIBUTES_METRICS_ENABLE = "scan.attributes.metrics.enable";

  // If an application wants to use multiple scans over different tables each scan must
  // define this attribute with the appropriate table name by calling
  // scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName))
  static public final String SCAN_ATTRIBUTES_TABLE_NAME = "scan.attributes.table.name";

  /**
   * -1 means no caching specified and the value of {@link HConstants#HBASE_CLIENT_SCANNER_CACHING}
   * (default to {@link HConstants#DEFAULT_HBASE_CLIENT_SCANNER_CACHING}) will be used
   */
  private int caching = -1;
  private long maxResultSize = -1;
  private boolean cacheBlocks = true;
  private boolean reversed = false;
  private TimeRange tr = TimeRange.allTime();
  private Map<byte [], NavigableSet<byte []>> familyMap =
    new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
  private Boolean asyncPrefetch = null;

  /**
   * Parameter name for client scanner sync/async prefetch toggle.
   * When using async scanner, prefetching data from the server is done at the background.
   * The parameter currently won't have any effect in the case that the user has set
   * Scan#setSmall or Scan#setReversed
   */
  public static final String HBASE_CLIENT_SCANNER_ASYNC_PREFETCH =
      "hbase.client.scanner.async.prefetch";

  /**
   * Default value of {@link #HBASE_CLIENT_SCANNER_ASYNC_PREFETCH}.
   */
  public static final boolean DEFAULT_HBASE_CLIENT_SCANNER_ASYNC_PREFETCH = false;

  /**
   * The mvcc read point to use when open a scanner. Remember to clear it after switching regions as
   * the mvcc is only valid within region scope.
   */
  private long mvccReadPoint = -1L;

  /**
   * The number of rows we want for this scan. We will terminate the scan if the number of return
   * rows reaches this value.
   */
  private int limit = -1;

  /**
   * Control whether to use pread at server side.
   */
  private ReadType readType = ReadType.DEFAULT;

  private boolean needCursorResult = false;

  /**
   * Create a Scan operation across all rows.
   */
  public Scan() {}

  /**
   * Creates a new instance of this class while copying all values.
   *
   * @param scan  The scan instance to copy from.
   * @throws IOException When copying the values fails.
   */
  public Scan(Scan scan) throws IOException {
    startRow = scan.getStartRow();
    includeStartRow = scan.includeStartRow();
    stopRow  = scan.getStopRow();
    includeStopRow = scan.includeStopRow();
    maxVersions = scan.getMaxVersions();
    batch = scan.getBatch();
    storeLimit = scan.getMaxResultsPerColumnFamily();
    storeOffset = scan.getRowOffsetPerColumnFamily();
    caching = scan.getCaching();
    maxResultSize = scan.getMaxResultSize();
    cacheBlocks = scan.getCacheBlocks();
    filter = scan.getFilter(); // clone?
    loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
    consistency = scan.getConsistency();
    this.setIsolationLevel(scan.getIsolationLevel());
    reversed = scan.isReversed();
    asyncPrefetch = scan.isAsyncPrefetch();
    allowPartialResults = scan.getAllowPartialResults();
    tr = scan.getTimeRange(); // TimeRange is immutable
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
    for (Map.Entry<String, byte[]> attr : scan.getAttributesMap().entrySet()) {
      setAttribute(attr.getKey(), attr.getValue());
    }
    for (Map.Entry<byte[], TimeRange> entry : scan.getColumnFamilyTimeRange().entrySet()) {
      TimeRange tr = entry.getValue();
      setColumnFamilyTimeRange(entry.getKey(), tr.getMin(), tr.getMax());
    }
    this.mvccReadPoint = scan.getMvccReadPoint();
    this.limit = scan.getLimit();
    this.needCursorResult = scan.isNeedCursorResult();
    setPriority(scan.getPriority());
    readType = scan.getReadType();
    super.setReplicaId(scan.getReplicaId());
  }

  /**
   * Builds a scan object with the same specs as get.
   * @param get get to model scan after
   */
  public Scan(Get get) {
    this.startRow = get.getRow();
    this.includeStartRow = true;
    this.stopRow = get.getRow();
    this.includeStopRow = true;
    this.filter = get.getFilter();
    this.cacheBlocks = get.getCacheBlocks();
    this.maxVersions = get.getMaxVersions();
    this.storeLimit = get.getMaxResultsPerColumnFamily();
    this.storeOffset = get.getRowOffsetPerColumnFamily();
    this.tr = get.getTimeRange();
    this.familyMap = get.getFamilyMap();
    this.asyncPrefetch = false;
    this.consistency = get.getConsistency();
    this.setIsolationLevel(get.getIsolationLevel());
    this.loadColumnFamiliesOnDemand = get.getLoadColumnFamiliesOnDemandValue();
    for (Map.Entry<String, byte[]> attr : get.getAttributesMap().entrySet()) {
      setAttribute(attr.getKey(), attr.getValue());
    }
    for (Map.Entry<byte[], TimeRange> entry : get.getColumnFamilyTimeRange().entrySet()) {
      TimeRange tr = entry.getValue();
      setColumnFamilyTimeRange(entry.getKey(), tr.getMin(), tr.getMax());
    }
    this.mvccReadPoint = -1L;
    setPriority(get.getPriority());
    super.setReplicaId(get.getReplicaId());
  }

  public boolean isGetScan() {
    return includeStartRow && includeStopRow
        && ClientUtil.areScanStartRowAndStopRowEqual(this.startRow, this.stopRow);
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
    familyMap.put(family, null);
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
    if(set == null) {
      set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      familyMap.put(family, set);
    }
    if (qualifier == null) {
      qualifier = HConstants.EMPTY_BYTE_ARRAY;
    }
    set.add(qualifier);
    return this;
  }

  /**
   * Get versions of columns only within the specified timestamp range,
   * [minStamp, maxStamp).  Note, default maximum versions to return is 1.  If
   * your time range spans more than one version and you want all versions
   * returned, up the number of versions beyond the default.
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @see #readAllVersions()
   * @see #readVersions(int)
   * @return this
   */
  public Scan setTimeRange(long minStamp, long maxStamp) throws IOException {
    tr = TimeRange.between(minStamp, maxStamp);
    return this;
  }

  /**
   * Get versions of columns with the specified timestamp. Note, default maximum
   * versions to return is 1.  If your time range spans more than one version
   * and you want all versions returned, up the number of versions beyond the
   * defaut.
   * @param timestamp version timestamp
   * @see #readAllVersions()
   * @see #readVersions(int)
   * @return this
   */
  public Scan setTimestamp(long timestamp) {
    try {
      tr = TimeRange.at(timestamp);
    } catch(Exception e) {
      // This should never happen, unless integer overflow or something extremely wrong...
      LOG.error("TimeRange failed, likely caused by integer overflow. ", e);
      throw e;
    }

    return this;
  }

  @Override public Scan setColumnFamilyTimeRange(byte[] cf, long minStamp, long maxStamp) {
    return (Scan) super.setColumnFamilyTimeRange(cf, minStamp, maxStamp);
  }

  /**
   * Set the start row of the scan.
   * <p>
   * If the specified row does not exist, the Scanner will start from the next closest row after the
   * specified row.
   * <p>
   * <b>Note:</b> <strong>Do NOT use this in combination with
   * {@link #setRowPrefixFilter(byte[])} or {@link #setStartStopRowForPrefixScan(byte[])}.</strong>
   * Doing so will make the scan result unexpected or even undefined.
   * </p>
   * @param startRow row to start scanner at or after
   * @return this
   * @throws IllegalArgumentException if startRow does not meet criteria for a row key (when length
   *           exceeds {@link HConstants#MAX_ROW_LENGTH})
   */
  public Scan withStartRow(byte[] startRow) {
    return withStartRow(startRow, true);
  }

  /**
   * Set the start row of the scan.
   * <p>
   * If the specified row does not exist, or the {@code inclusive} is {@code false}, the Scanner
   * will start from the next closest row after the specified row.
   * <p>
   * <b>Note:</b> <strong>Do NOT use this in combination with
   * {@link #setRowPrefixFilter(byte[])} or {@link #setStartStopRowForPrefixScan(byte[])}.</strong>
   * Doing so will make the scan result unexpected or even undefined.
   * </p>
   * @param startRow row to start scanner at or after
   * @param inclusive whether we should include the start row when scan
   * @return this
   * @throws IllegalArgumentException if startRow does not meet criteria for a row key (when length
   *           exceeds {@link HConstants#MAX_ROW_LENGTH})
   */
  public Scan withStartRow(byte[] startRow, boolean inclusive) {
    if (Bytes.len(startRow) > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("startRow's length must be less than or equal to "
          + HConstants.MAX_ROW_LENGTH + " to meet the criteria" + " for a row key.");
    }
    this.startRow = startRow;
    this.includeStartRow = inclusive;
    return this;
  }

  /**
   * Set the stop row of the scan.
   * <p>
   * The scan will include rows that are lexicographically less than the provided stopRow.
   * <p>
   * <b>Note:</b> <strong>Do NOT use this in combination with
   * {@link #setRowPrefixFilter(byte[])} or {@link #setStartStopRowForPrefixScan(byte[])}.</strong>
   * Doing so will make the scan result unexpected or even undefined.
   * </p>
   * @param stopRow row to end at (exclusive)
   * @return this
   * @throws IllegalArgumentException if stopRow does not meet criteria for a row key (when length
   *           exceeds {@link HConstants#MAX_ROW_LENGTH})
   */
  public Scan withStopRow(byte[] stopRow) {
    return withStopRow(stopRow, false);
  }

  /**
   * Set the stop row of the scan.
   * <p>
   * The scan will include rows that are lexicographically less than (or equal to if
   * {@code inclusive} is {@code true}) the provided stopRow.
   * <p>
   * <b>Note:</b> <strong>Do NOT use this in combination with
   * {@link #setRowPrefixFilter(byte[])} or {@link #setStartStopRowForPrefixScan(byte[])}.</strong>
   * Doing so will make the scan result unexpected or even undefined.
   * </p>
   * @param stopRow row to end at
   * @param inclusive whether we should include the stop row when scan
   * @return this
   * @throws IllegalArgumentException if stopRow does not meet criteria for a row key (when length
   *           exceeds {@link HConstants#MAX_ROW_LENGTH})
   */
  public Scan withStopRow(byte[] stopRow, boolean inclusive) {
    if (Bytes.len(stopRow) > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("stopRow's length must be less than or equal to "
          + HConstants.MAX_ROW_LENGTH + " to meet the criteria" + " for a row key.");
    }
    this.stopRow = stopRow;
    this.includeStopRow = inclusive;
    return this;
  }

  /**
   * <p>Set a filter (using stopRow and startRow) so the result set only contains rows where the
   * rowKey starts with the specified prefix.</p>
   * <p>This is a utility method that converts the desired rowPrefix into the appropriate values
   * for the startRow and stopRow to achieve the desired result.</p>
   * <p>This can safely be used in combination with setFilter.</p>
   * <p><strong>This CANNOT be used in combination with withStartRow and/or withStopRow.</strong>
   * Such a combination will yield unexpected and even undefined results.</p>
   * @param rowPrefix the prefix all rows must start with. (Set <i>null</i> to remove the filter.)
   * @return this
   * @deprecated since 2.5.0, will be removed in 4.0.0.
   *       The name of this method is considered to be confusing as it does not
   *       use a {@link Filter} but uses setting the startRow and stopRow instead.
   *       Use {@link #setStartStopRowForPrefixScan(byte[])} instead.
   */
  @Deprecated
  public Scan setRowPrefixFilter(byte[] rowPrefix) {
    return setStartStopRowForPrefixScan(rowPrefix);
  }

  /**
   * <p>Set a filter (using stopRow and startRow) so the result set only contains rows where the
   * rowKey starts with the specified prefix.</p>
   * <p>This is a utility method that converts the desired rowPrefix into the appropriate values
   * for the startRow and stopRow to achieve the desired result.</p>
   * <p>This can safely be used in combination with setFilter.</p>
   * <p><strong>This CANNOT be used in combination with withStartRow and/or withStopRow.</strong>
   * Such a combination will yield unexpected and even undefined results.</p>
   * @param rowPrefix the prefix all rows must start with. (Set <i>null</i> to remove the filter.)
   * @return this
   */
  public Scan setStartStopRowForPrefixScan(byte[] rowPrefix) {
    if (rowPrefix == null) {
      withStartRow(HConstants.EMPTY_START_ROW);
      withStopRow(HConstants.EMPTY_END_ROW);
    } else {
      this.withStartRow(rowPrefix);
      this.withStopRow(ClientUtil.calculateTheClosestNextRowKeyForPrefix(rowPrefix));
    }
    return this;
  }

  /**
   * Get all available versions.
   * @return this
   */
  public Scan readAllVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param versions specified number of versions for each column
   * @return this
   */
  public Scan readVersions(int versions) {
    this.maxVersions = versions;
    return this;
  }

  /**
   * Set the maximum number of cells to return for each call to next(). Callers should be aware
   * that this is not equivalent to calling {@link #setAllowPartialResults(boolean)}.
   * If you don't allow partial results, the number of cells in each Result must equal to your
   * batch setting unless it is the last Result for current row. So this method is helpful in paging
   * queries. If you just want to prevent OOM at client, use setAllowPartialResults(true) is better.
   * @param batch the maximum number of values
   * @see Result#mayHaveMoreCellsInRow()
   */
  public Scan setBatch(int batch) {
    if (this.hasFilter() && this.filter.hasFilterRow()) {
      throw new IncompatibleFilterException(
        "Cannot set batch on a scan using a filter" +
        " that returns true for filter.hasFilterRow");
    }
    this.batch = batch;
    return this;
  }

  /**
   * Set the maximum number of values to return per row per Column Family
   * @param limit the maximum number of values returned / row / CF
   */
  public Scan setMaxResultsPerColumnFamily(int limit) {
    this.storeLimit = limit;
    return this;
  }

  /**
   * Set offset for the row per Column Family.
   * @param offset is the number of kvs that will be skipped.
   */
  public Scan setRowOffsetPerColumnFamily(int offset) {
    this.storeOffset = offset;
    return this;
  }

  /**
   * Set the number of rows for caching that will be passed to scanners.
   * If not set, the Configuration setting {@link HConstants#HBASE_CLIENT_SCANNER_CACHING} will
   * apply.
   * Higher caching values will enable faster scanners but will use more memory.
   * @param caching the number of rows for caching
   */
  public Scan setCaching(int caching) {
    this.caching = caching;
    return this;
  }

  /**
   * @return the maximum result size in bytes. See {@link #setMaxResultSize(long)}
   */
  public long getMaxResultSize() {
    return maxResultSize;
  }

  /**
   * Set the maximum result size. The default is -1; this means that no specific
   * maximum result size will be set for this scan, and the global configured
   * value will be used instead. (Defaults to unlimited).
   *
   * @param maxResultSize The maximum result size in bytes.
   */
  public Scan setMaxResultSize(long maxResultSize) {
    this.maxResultSize = maxResultSize;
    return this;
  }

  @Override
  public Scan setFilter(Filter filter) {
    super.setFilter(filter);
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
  public byte [] getStartRow() {
    return this.startRow;
  }

  /**
   * @return if we should include start row when scan
   */
  public boolean includeStartRow() {
    return includeStartRow;
  }

  /**
   * @return the stoprow
   */
  public byte[] getStopRow() {
    return this.stopRow;
  }

  /**
   * @return if we should include stop row when scan
   */
  public boolean includeStopRow() {
    return includeStopRow;
  }

  /**
   * @return the max number of versions to fetch
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }

  /**
   * @return maximum number of values to return for a single call to next()
   */
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
  public int getCaching() {
    return this.caching;
  }

  /**
   * @return TimeRange
   */
  public TimeRange getTimeRange() {
    return this.tr;
  }

  /**
   * @return RowFilter
   */
  @Override
  public Filter getFilter() {
    return filter;
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
  public Scan setCacheBlocks(boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
    return this;
  }

  /**
   * Get whether blocks should be cached for this Scan.
   * @return true if default caching should be used, false if blocks should not
   * be cached
   */
  public boolean getCacheBlocks() {
    return cacheBlocks;
  }

  /**
   * Set whether this scan is a reversed one
   * <p>
   * This is false by default which means forward(normal) scan.
   *
   * @param reversed if true, scan will be backward order
   * @return this
   */
  public Scan setReversed(boolean reversed) {
    this.reversed = reversed;
    return this;
  }

  /**
   * Get whether this scan is a reversed one.
   * @return true if backward scan, false if forward(default) scan
   */
  public boolean isReversed() {
    return reversed;
  }

  /**
   * Setting whether the caller wants to see the partial results when server returns
   * less-than-expected cells. It is helpful while scanning a huge row to prevent OOM at client.
   * By default this value is false and the complete results will be assembled client side
   * before being delivered to the caller.
   * @param allowPartialResults
   * @return this
   * @see Result#mayHaveMoreCellsInRow()
   * @see #setBatch(int)
   */
  public Scan setAllowPartialResults(final boolean allowPartialResults) {
    this.allowPartialResults = allowPartialResults;
    return this;
  }

  /**
   * @return true when the constructor of this scan understands that the results they will see may
   *         only represent a partial portion of a row. The entire row would be retrieved by
   *         subsequent calls to {@link ResultScanner#next()}
   */
  public boolean getAllowPartialResults() {
    return allowPartialResults;
  }

  @Override
  public Scan setLoadColumnFamiliesOnDemand(boolean value) {
    return (Scan) super.setLoadColumnFamiliesOnDemand(value);
  }

  /**
   * Compile the table and column family (i.e. schema) information
   * into a String. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * @return Map
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<>();
    List<String> families = new ArrayList<>();
    if(this.familyMap.isEmpty()) {
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
    Map<String, List<String>> familyColumns = new HashMap<>();
    map.put("families", familyColumns);
    // add scalar information first
    map.put("startRow", Bytes.toStringBinary(this.startRow));
    map.put("stopRow", Bytes.toStringBinary(this.stopRow));
    map.put("maxVersions", this.maxVersions);
    map.put("batch", this.batch);
    map.put("caching", this.caching);
    map.put("maxResultSize", this.maxResultSize);
    map.put("cacheBlocks", this.cacheBlocks);
    map.put("loadColumnFamiliesOnDemand", this.loadColumnFamiliesOnDemand);
    List<Long> timeRange = new ArrayList<>(2);
    timeRange.add(this.tr.getMin());
    timeRange.add(this.tr.getMax());
    map.put("timeRange", timeRange);
    int colCount = 0;
    // iterate through affected families and list out up to maxCols columns
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
      List<String> columns = new ArrayList<>();
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
    // add the id if set
    if (getId() != null) {
      map.put("id", getId());
    }
    return map;
  }

  /**
   * Enable/disable "raw" mode for this scan.
   * If "raw" is enabled the scan will return all
   * delete marker and deleted rows that have not
   * been collected, yet.
   * This is mostly useful for Scan on column families
   * that have KEEP_DELETED_ROWS enabled.
   * It is an error to specify any column when "raw" is set.
   * @param raw True/False to enable/disable "raw" mode.
   */
  public Scan setRaw(boolean raw) {
    setAttribute(RAW_ATTR, Bytes.toBytes(raw));
    return this;
  }

  /**
   * @return True if this Scan is in "raw" mode.
   */
  public boolean isRaw() {
    byte[] attr = getAttribute(RAW_ATTR);
    return attr == null ? false : Bytes.toBoolean(attr);
  }

  @Override
  public Scan setAttribute(String name, byte[] value) {
    return (Scan) super.setAttribute(name, value);
  }

  @Override
  public Scan setId(String id) {
    return (Scan) super.setId(id);
  }

  @Override
  public Scan setAuthorizations(Authorizations authorizations) {
    return (Scan) super.setAuthorizations(authorizations);
  }

  @Override
  public Scan setACL(Map<String, Permission> perms) {
    return (Scan) super.setACL(perms);
  }

  @Override
  public Scan setACL(String user, Permission perms) {
    return (Scan) super.setACL(user, perms);
  }

  @Override
  public Scan setConsistency(Consistency consistency) {
    return (Scan) super.setConsistency(consistency);
  }

  @Override
  public Scan setReplicaId(int Id) {
    return (Scan) super.setReplicaId(Id);
  }

  @Override
  public Scan setIsolationLevel(IsolationLevel level) {
    return (Scan) super.setIsolationLevel(level);
  }

  @Override
  public Scan setPriority(int priority) {
    return (Scan) super.setPriority(priority);
  }

  /**
   * Enable collection of {@link ScanMetrics}. For advanced users.
   * @param enabled Set to true to enable accumulating scan metrics
   */
  public Scan setScanMetricsEnabled(final boolean enabled) {
    setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.valueOf(enabled)));
    return this;
  }

  /**
   * @return True if collection of scan metrics is enabled. For advanced users.
   */
  public boolean isScanMetricsEnabled() {
    byte[] attr = getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE);
    return attr == null ? false : Bytes.toBoolean(attr);
  }

  public Boolean isAsyncPrefetch() {
    return asyncPrefetch;
  }

  /**
   * @deprecated Since 3.0.0, will be removed in 4.0.0. After building sync client upon async
   *             client, the implementation is always 'async prefetch', so this flag is useless now.
   */
  @Deprecated
  public Scan setAsyncPrefetch(boolean asyncPrefetch) {
    this.asyncPrefetch = asyncPrefetch;
    return this;
  }

  /**
   * @return the limit of rows for this scan
   */
  public int getLimit() {
    return limit;
  }

  /**
   * Set the limit of rows for this scan. We will terminate the scan if the number of returned rows
   * reaches this value.
   * <p>
   * This condition will be tested at last, after all other conditions such as stopRow, filter, etc.
   * @param limit the limit of rows for this scan
   * @return this
   */
  public Scan setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * Call this when you only want to get one row. It will set {@code limit} to {@code 1}, and also
   * set {@code readType} to {@link ReadType#PREAD}.
   * @return this
   */
  public Scan setOneRowLimit() {
    return setLimit(1).setReadType(ReadType.PREAD);
  }

  @InterfaceAudience.Public
  public enum ReadType {
    DEFAULT, STREAM, PREAD
  }

  /**
   * @return the read type for this scan
   */
  public ReadType getReadType() {
    return readType;
  }

  /**
   * Set the read type for this scan.
   * <p>
   * Notice that we may choose to use pread even if you specific {@link ReadType#STREAM} here. For
   * example, we will always use pread if this is a get scan.
   * @return this
   */
  public Scan setReadType(ReadType readType) {
    this.readType = readType;
    return this;
  }

  /**
   * Get the mvcc read point used to open a scanner.
   */
  long getMvccReadPoint() {
    return mvccReadPoint;
  }

  /**
   * Set the mvcc read point used to open a scanner.
   */
  Scan setMvccReadPoint(long mvccReadPoint) {
    this.mvccReadPoint = mvccReadPoint;
    return this;
  }

  /**
   * Set the mvcc read point to -1 which means do not use it.
   */
  Scan resetMvccReadPoint() {
    return setMvccReadPoint(-1L);
  }

  /**
   * When the server is slow or we scan a table with many deleted data or we use a sparse filter,
   * the server will response heartbeat to prevent timeout. However the scanner will return a Result
   * only when client can do it. So if there are many heartbeats, the blocking time on
   * ResultScanner#next() may be very long, which is not friendly to online services.
   *
   * Set this to true then you can get a special Result whose #isCursor() returns true and is not
   * contains any real data. It only tells you where the server has scanned. You can call next
   * to continue scanning or open a new scanner with this row key as start row whenever you want.
   *
   * Users can get a cursor when and only when there is a response from the server but we can not
   * return a Result to users, for example, this response is a heartbeat or there are partial cells
   * but users do not allow partial result.
   *
   * Now the cursor is in row level which means the special Result will only contains a row key.
   * {@link Result#isCursor()}
   * {@link Result#getCursor()}
   * {@link Cursor}
   */
  public Scan setNeedCursorResult(boolean needCursorResult) {
    this.needCursorResult = needCursorResult;
    return this;
  }

  public boolean isNeedCursorResult() {
    return needCursorResult;
  }

  /**
   * Create a new Scan with a cursor. It only set the position information like start row key.
   * The others (like cfs, stop row, limit) should still be filled in by the user.
   * {@link Result#isCursor()}
   * {@link Result#getCursor()}
   * {@link Cursor}
   */
  public static Scan createScanFromCursor(Cursor cursor) {
    return new Scan().withStartRow(cursor.getRow());
  }
}
