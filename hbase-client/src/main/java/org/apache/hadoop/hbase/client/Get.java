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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Used to perform Get operations on a single row.
 * <p>
 * To get everything for a row, instantiate a Get object with the row to get.
 * To further narrow the scope of what to Get, use the methods below.
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
 * {@link #setTimestamp(long) setTimestamp}.
 * <p>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p>
 * To add a filter, call {@link #setFilter(Filter) setFilter}.
 */
@InterfaceAudience.Public
public class Get extends Query implements Row {
  private static final Logger LOG = LoggerFactory.getLogger(Get.class);

  private byte [] row = null;
  private int maxVersions = 1;
  private boolean cacheBlocks = true;
  private int storeLimit = -1;
  private int storeOffset = 0;
  private TimeRange tr = TimeRange.allTime();
  private boolean checkExistenceOnly = false;
  private boolean closestRowBefore = false;
  private Map<byte [], NavigableSet<byte []>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * Create a Get operation for the specified row.
   * <p>
   * If no further operations are done, this will get the latest version of
   * all columns in all families of the specified row.
   * @param row row key
   */
  public Get(byte [] row) {
    Mutation.checkRow(row);
    this.row = row;
  }

  /**
   * Copy-constructor
   *
   * @param get
   */
  public Get(Get get) {
    this(get.getRow());
    // from Query
    this.setFilter(get.getFilter());
    this.setReplicaId(get.getReplicaId());
    this.setConsistency(get.getConsistency());
    // from Get
    this.cacheBlocks = get.getCacheBlocks();
    this.maxVersions = get.getMaxVersions();
    this.storeLimit = get.getMaxResultsPerColumnFamily();
    this.storeOffset = get.getRowOffsetPerColumnFamily();
    this.tr = get.getTimeRange();
    this.checkExistenceOnly = get.isCheckExistenceOnly();
    this.loadColumnFamiliesOnDemand = get.getLoadColumnFamiliesOnDemandValue();
    Map<byte[], NavigableSet<byte[]>> fams = get.getFamilyMap();
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
    for (Map.Entry<String, byte[]> attr : get.getAttributesMap().entrySet()) {
      setAttribute(attr.getKey(), attr.getValue());
    }
    for (Map.Entry<byte[], TimeRange> entry : get.getColumnFamilyTimeRange().entrySet()) {
      TimeRange tr = entry.getValue();
      setColumnFamilyTimeRange(entry.getKey(), tr.getMin(), tr.getMax());
    }
    super.setPriority(get.getPriority());
  }

  /**
   * Create a Get operation for the specified row.
   * @param row
   * @param rowOffset
   * @param rowLength
   */
  public Get(byte[] row, int rowOffset, int rowLength) {
    Mutation.checkRow(row, rowOffset, rowLength);
    this.row = Bytes.copy(row, rowOffset, rowLength);
  }

  /**
   * Create a Get operation for the specified row.
   * @param row
   */
  public Get(ByteBuffer row) {
    Mutation.checkRow(row);
    this.row = new byte[row.remaining()];
    row.get(this.row);
  }

  public boolean isCheckExistenceOnly() {
    return checkExistenceOnly;
  }

  public Get setCheckExistenceOnly(boolean checkExistenceOnly) {
    this.checkExistenceOnly = checkExistenceOnly;
    return this;
  }

  /**
   * This will always return the default value which is false as client cannot set the value to this
   * property any more.
   * @deprecated since 2.0.0 and will be removed in 3.0.0
   */
  @Deprecated
  public boolean isClosestRowBefore() {
    return closestRowBefore;
  }

  /**
   * This is not used any more and does nothing. Use reverse scan instead.
   * @deprecated since 2.0.0 and will be removed in 3.0.0
   */
  @Deprecated
  public Get setClosestRowBefore(boolean closestRowBefore) {
    // do Nothing
    return this;
  }

  /**
   * Get all columns from the specified family.
   * <p>
   * Overrides previous calls to addColumn for this family.
   * @param family family name
   * @return the Get object
   */
  public Get addFamily(byte [] family) {
    familyMap.remove(family);
    familyMap.put(family, null);
    return this;
  }

  /**
   * Get the column from the specific family with the specified qualifier.
   * <p>
   * Overrides previous calls to addFamily for this family.
   * @param family family name
   * @param qualifier column qualifier
   * @return the Get objec
   */
  public Get addColumn(byte [] family, byte [] qualifier) {
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
   * [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException
   * @return this for invocation chaining
   */
  public Get setTimeRange(long minStamp, long maxStamp) throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }

  /**
   * Get versions of columns with the specified timestamp.
   * @param timestamp version timestamp
   * @return this for invocation chaining
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #setTimestamp(long)} instead
   */
  @Deprecated
  public Get setTimeStamp(long timestamp) throws IOException {
    return this.setTimestamp(timestamp);
  }

  /**
   * Get versions of columns with the specified timestamp.
   * @param timestamp version timestamp
   * @return this for invocation chaining
   */
  public Get setTimestamp(long timestamp) {
    try {
      tr = new TimeRange(timestamp, timestamp + 1);
    } catch(Exception e) {
      // This should never happen, unless integer overflow or something extremely wrong...
      LOG.error("TimeRange failed, likely caused by integer overflow. ", e);
      throw e;
    }

    return this;
  }

  @Override public Get setColumnFamilyTimeRange(byte[] cf, long minStamp, long maxStamp) {
    return (Get) super.setColumnFamilyTimeRange(cf, minStamp, maxStamp);
  }

  /**
   * Get all available versions.
   * @return this for invocation chaining
   * @deprecated It is easy to misunderstand with column family's max versions, so use
   *             {@link #readAllVersions()} instead.
   */
  @Deprecated
  public Get setMaxVersions() {
    return readAllVersions();
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @throws IOException if invalid number of versions
   * @return this for invocation chaining
   * @deprecated It is easy to misunderstand with column family's max versions, so use
   *             {@link #readVersions(int)} instead.
   */
  @Deprecated
  public Get setMaxVersions(int maxVersions) throws IOException {
    return readVersions(maxVersions);
  }

  /**
   * Get all available versions.
   * @return this for invocation chaining
   */
  public Get readAllVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param versions specified number of versions for each column
   * @throws IOException if invalid number of versions
   * @return this for invocation chaining
   */
  public Get readVersions(int versions) throws IOException {
    if (versions <= 0) {
      throw new IOException("versions must be positive");
    }
    this.maxVersions = versions;
    return this;
  }

  @Override
  public Get setLoadColumnFamiliesOnDemand(boolean value) {
    return (Get) super.setLoadColumnFamiliesOnDemand(value);
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
   * Set offset for the row per Column Family. This offset is only within a particular row/CF
   * combination. It gets reset back to zero when we move to the next row or CF.
   * @param offset is the number of kvs that will be skipped.
   * @return this for invocation chaining
   */
  public Get setRowOffsetPerColumnFamily(int offset) {
    this.storeOffset = offset;
    return this;
  }

  @Override
  public Get setFilter(Filter filter) {
    super.setFilter(filter);
    return this;
  }

  /* Accessors */

  /**
   * Set whether blocks should be cached for this Get.
   * <p>
   * This is true by default.  When true, default settings of the table and
   * family are used (this will never override caching blocks if the block
   * cache is disabled for that family or entirely).
   *
   * @param cacheBlocks if false, default settings are overridden and blocks
   * will not be cached
   */
  public Get setCacheBlocks(boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
    return this;
  }

  /**
   * Get whether blocks should be cached for this Get.
   * @return true if default caching should be used, false if blocks should not
   * be cached
   */
  public boolean getCacheBlocks() {
    return cacheBlocks;
  }

  /**
   * Method for retrieving the get's row
   * @return row
   */
  @Override
  public byte [] getRow() {
    return this.row;
  }

  /**
   * Method for retrieving the get's maximum number of version
   * @return the maximum number of version to fetch for this get
   */
  public int getMaxVersions() {
    return this.maxVersions;
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
  public Map<byte[],NavigableSet<byte[]>> getFamilyMap() {
    return this.familyMap;
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
    List<String> families = new ArrayList<>(this.familyMap.entrySet().size());
    map.put("families", families);
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
    // we start with the fingerprint map and build on top of it.
    Map<String, Object> map = getFingerprint();
    // replace the fingerprint's simple list of families with a
    // map from column families to lists of qualifiers and kv details
    Map<String, List<String>> columns = new HashMap<>();
    map.put("families", columns);
    // add scalar information first
    map.put("row", Bytes.toStringBinary(this.row));
    map.put("maxVersions", this.maxVersions);
    map.put("cacheBlocks", this.cacheBlocks);
    List<Long> timeRange = new ArrayList<>(2);
    timeRange.add(this.tr.getMin());
    timeRange.add(this.tr.getMax());
    map.put("timeRange", timeRange);
    int colCount = 0;
    // iterate through affected families and add details
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
      this.familyMap.entrySet()) {
      List<String> familyList = new ArrayList<>();
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
        for (byte [] column : entry.getValue()) {
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
    // add the id if set
    if (getId() != null) {
      map.put("id", getId());
    }
    return map;
  }

  //Row
  @Override
  public int compareTo(Row other) {
    // TODO: This is wrong.  Can't have two gets the same just because on same row.
    return Bytes.compareTo(this.getRow(), other.getRow());
  }

  @Override
  public int hashCode() {
    // TODO: This is wrong.  Can't have two gets the same just because on same row.  But it
    // matches how equals works currently and gets rid of the findbugs warning.
    return Bytes.hashCode(this.getRow());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Row other = (Row) obj;
    // TODO: This is wrong.  Can't have two gets the same just because on same row.
    return compareTo(other) == 0;
  }

  @Override
  public Get setAttribute(String name, byte[] value) {
    return (Get) super.setAttribute(name, value);
  }

  @Override
  public Get setId(String id) {
    return (Get) super.setId(id);
  }

  @Override
  public Get setAuthorizations(Authorizations authorizations) {
    return (Get) super.setAuthorizations(authorizations);
  }

  @Override
  public Get setACL(Map<String, Permission> perms) {
    return (Get) super.setACL(perms);
  }

  @Override
  public Get setACL(String user, Permission perms) {
    return (Get) super.setACL(user, perms);
  }

  @Override
  public Get setConsistency(Consistency consistency) {
    return (Get) super.setConsistency(consistency);
  }

  @Override
  public Get setReplicaId(int Id) {
    return (Get) super.setReplicaId(Id);
  }

  @Override
  public Get setIsolationLevel(IsolationLevel level) {
      return (Get) super.setIsolationLevel(level);
  }

  @Override
  public Get setPriority(int priority) {
    return (Get) super.setPriority(priority);
  }
}
