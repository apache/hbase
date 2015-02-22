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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
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
 * {@link #setTimeStamp(long) setTimestamp}.
 * <p>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p>
 * To add a filter, call {@link #setFilter(Filter) setFilter}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Get extends Query
  implements Row, Comparable<Row> {
  private static final Log LOG = LogFactory.getLog(Get.class);

  private byte [] row = null;
  private int maxVersions = 1;
  private boolean cacheBlocks = true;
  private int storeLimit = -1;
  private int storeOffset = 0;
  private TimeRange tr = new TimeRange();
  private boolean checkExistenceOnly = false;
  private boolean closestRowBefore = false;
  private Map<byte [], NavigableSet<byte []>> familyMap =
    new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);

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
    this.filter = get.getFilter();
    this.cacheBlocks = get.getCacheBlocks();
    this.maxVersions = get.getMaxVersions();
    this.storeLimit = get.getMaxResultsPerColumnFamily();
    this.storeOffset = get.getRowOffsetPerColumnFamily();
    this.tr = get.getTimeRange();
    this.checkExistenceOnly = get.isCheckExistenceOnly();
    this.closestRowBefore = get.isClosestRowBefore();
    this.familyMap = get.getFamilyMap();
    for (Map.Entry<String, byte[]> attr : get.getAttributesMap().entrySet()) {
      setAttribute(attr.getKey(), attr.getValue());
    }
  }

  public boolean isCheckExistenceOnly() {
    return checkExistenceOnly;
  }

  public Get setCheckExistenceOnly(boolean checkExistenceOnly) {
    this.checkExistenceOnly = checkExistenceOnly;
    return this;
  }

  public boolean isClosestRowBefore() {
    return closestRowBefore;
  }

  public Get setClosestRowBefore(boolean closestRowBefore) {
    this.closestRowBefore = closestRowBefore;
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
      set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    }
    if (qualifier == null) {
      qualifier = HConstants.EMPTY_BYTE_ARRAY;
    }
    set.add(qualifier);
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
  public Get setTimeStamp(long timestamp)
  throws IOException {
    try {
      tr = new TimeRange(timestamp, timestamp+1);
    } catch(IOException e) {
      // This should never happen, unless integer overflow or something extremely wrong...
      LOG.error("TimeRange failed, likely caused by integer overflow. ", e);
      throw e;
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
    Map<String, Object> map = new HashMap<String, Object>();
    List<String> families = new ArrayList<String>();
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
    Map<String, List<String>> columns = new HashMap<String, List<String>>();
    map.put("families", columns);
    // add scalar information first
    map.put("row", Bytes.toStringBinary(this.row));
    map.put("maxVersions", this.maxVersions);
    map.put("cacheBlocks", this.cacheBlocks);
    List<Long> timeRange = new ArrayList<Long>();
    timeRange.add(this.tr.getMin());
    timeRange.add(this.tr.getMax());
    map.put("timeRange", timeRange);
    int colCount = 0;
    // iterate through affected families and add details
    for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
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

}
