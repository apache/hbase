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
import java.util.Collections;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Immutable version of Scan
 */
@InterfaceAudience.Private
public final class ImmutableScan extends Scan {

  /**
   * Create Immutable instance of Scan from given Scan object
   *
   * @param scan Copy all values from Scan
   * @throws IOException From parent constructor
   */
  public ImmutableScan(Scan scan) throws IOException {
    super(scan);
    super.setIsolationLevel(scan.getIsolationLevel());
    Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
      byte[] family = entry.getKey();
      NavigableSet<byte[]> cols = entry.getValue();
      if (cols != null && cols.size() > 0) {
        for (byte[] col : cols) {
          super.addColumn(family, col);
        }
      } else {
        super.addFamily(family);
      }
    }
    for (Map.Entry<String, byte[]> attr : scan.getAttributesMap().entrySet()) {
      super.setAttribute(attr.getKey(), attr.getValue());
    }
    for (Map.Entry<byte[], TimeRange> entry : scan.getColumnFamilyTimeRange().entrySet()) {
      TimeRange tr = entry.getValue();
      super.setColumnFamilyTimeRange(entry.getKey(), tr.getMin(), tr.getMax());
    }
    super.setPriority(scan.getPriority());
  }

  /**
   * Create Immutable instance of Scan from given Get object
   *
   * @param get Get to model Scan after
   */
  public ImmutableScan(Get get) {
    super(get);
    super.setIsolationLevel(get.getIsolationLevel());
    for (Map.Entry<String, byte[]> attr : get.getAttributesMap().entrySet()) {
      super.setAttribute(attr.getKey(), attr.getValue());
    }
    for (Map.Entry<byte[], TimeRange> entry : get.getColumnFamilyTimeRange().entrySet()) {
      TimeRange tr = entry.getValue();
      super.setColumnFamilyTimeRange(entry.getKey(), tr.getMin(), tr.getMax());
    }
    super.setPriority(get.getPriority());
  }

  /**
   * Create a new Scan with a cursor. It only set the position information like start row key.
   * The others (like cfs, stop row, limit) should still be filled in by the user.
   * {@link Result#isCursor()}
   * {@link Result#getCursor()}
   * {@link Cursor}
   */
  public static Scan createScanFromCursor(Cursor cursor) {
    Scan scan = new Scan().withStartRow(cursor.getRow());
    try {
      return new ImmutableScan(scan);
    } catch (IOException e) {
      throw new RuntimeException("Scan should not throw IOException", e);
    }
  }

  @Override
  public Scan addFamily(byte[] family) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to addFamily");
  }

  @Override
  public Scan addColumn(byte[] family, byte[] qualifier) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to addColumn");
  }

  @Override
  public Scan setTimeRange(long minStamp, long maxStamp) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setTimeRange");
  }

  @Deprecated
  @Override
  public Scan setTimeStamp(long timestamp) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setTimeStamp");
  }

  @Override
  public Scan setTimestamp(long timestamp) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setTimestamp");
  }

  @Override
  public Scan setColumnFamilyTimeRange(byte[] cf, long minStamp, long maxStamp) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setColumnFamilyTimeRange");
  }

  @Override
  public Scan withStartRow(byte[] startRow) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to withStartRow");
  }

  @Override
  public Scan withStartRow(byte[] startRow, boolean inclusive) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to withStartRow");
  }

  @Override
  public Scan withStopRow(byte[] stopRow) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to withStopRow");
  }

  @Override
  public Scan withStopRow(byte[] stopRow, boolean inclusive) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to withStopRow");
  }

  @Override
  public Scan setRowPrefixFilter(byte[] rowPrefix) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setRowPrefixFilter");
  }

  @Override
  public Scan readAllVersions() {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to readAllVersions");
  }

  @Override
  public Scan readVersions(int versions) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to readVersions");
  }

  @Override
  public Scan setBatch(int batch) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setBatch");
  }

  @Override
  public Scan setMaxResultsPerColumnFamily(int limit) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setMaxResultsPerColumnFamily");
  }

  @Override
  public Scan setRowOffsetPerColumnFamily(int offset) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setRowOffsetPerColumnFamily");
  }

  @Override
  public Scan setCaching(int caching) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setCaching");
  }

  @Override
  public Scan setMaxResultSize(long maxResultSize) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setMaxResultSize");
  }

  @Override
  public Scan setFilter(Filter filter) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setFilter");
  }

  @Override
  public Scan setFamilyMap(Map<byte[], NavigableSet<byte[]>> familyMap) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setFamilyMap");
  }

  @Override
  public Scan setCacheBlocks(boolean cacheBlocks) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setCacheBlocks");
  }

  @Override
  public Scan setReversed(boolean reversed) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setReversed");
  }

  @Override
  public Scan setAllowPartialResults(final boolean allowPartialResults) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setAllowPartialResults");
  }

  @Override
  public Scan setLoadColumnFamiliesOnDemand(boolean value) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setLoadColumnFamiliesOnDemand");
  }

  @Override
  public Scan setRaw(boolean raw) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setRaw");
  }

  @Override
  @Deprecated
  public Scan setSmall(boolean small) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setSmall");
  }

  @Override
  public Scan setAttribute(String name, byte[] value) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setAttribute");
  }

  @Override
  public Scan setId(String id) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setId");
  }

  @Override
  public Scan setAuthorizations(Authorizations authorizations) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setAuthorizations");
  }

  @Override
  public Scan setACL(Map<String, Permission> perms) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setACL");
  }

  @Override
  public Scan setACL(String user, Permission perms) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setACL");
  }

  @Override
  public Scan setConsistency(Consistency consistency) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setConsistency");
  }

  @Override
  public Scan setReplicaId(int Id) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setReplicaId");
  }

  @Override
  public Scan setIsolationLevel(IsolationLevel level) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setIsolationLevel");
  }

  @Override
  public Scan setPriority(int priority) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setPriority");
  }

  @Override
  public Scan setScanMetricsEnabled(final boolean enabled) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setScanMetricsEnabled");
  }

  @Override
  @Deprecated
  public Scan setAsyncPrefetch(boolean asyncPrefetch) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setAsyncPrefetch");
  }

  @Override
  public Scan setLimit(int limit) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setLimit");
  }

  @Override
  public Scan setOneRowLimit() {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setOneRowLimit");
  }

  @Override
  public Scan setReadType(ReadType readType) {
    throw new UnsupportedOperationException("ImmutableScan does not allow access to setReadType");
  }

  @Override
  Scan setMvccReadPoint(long mvccReadPoint) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setMvccReadPoint");
  }

  @Override
  Scan resetMvccReadPoint() {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to resetMvccReadPoint");
  }

  @Override
  public Scan setNeedCursorResult(boolean needCursorResult) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setNeedCursorResult");
  }

  @Override
  public long getMaxResultSize() {
    return super.getMaxResultSize();
  }

  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return Collections.unmodifiableMap(super.getFamilyMap());
  }

  @Override
  public int numFamilies() {
    return super.numFamilies();
  }

  @Override
  public boolean hasFamilies() {
    return super.hasFamilies();
  }

  @Override
  public byte[][] getFamilies() {
    return super.getFamilies();
  }

  @Override
  public byte[] getStartRow() {
    return super.getStartRow();
  }

  @Override
  public boolean includeStartRow() {
    return super.includeStartRow();
  }

  @Override
  public byte[] getStopRow() {
    return super.getStopRow();
  }

  @Override
  public boolean includeStopRow() {
    return super.includeStopRow();
  }

  @Override
  public int getMaxVersions() {
    return super.getMaxVersions();
  }

  @Override
  public int getBatch() {
    return super.getBatch();
  }

  @Override
  public int getMaxResultsPerColumnFamily() {
    return super.getMaxResultsPerColumnFamily();
  }

  @Override
  public int getRowOffsetPerColumnFamily() {
    return super.getRowOffsetPerColumnFamily();
  }

  @Override
  public int getCaching() {
    return super.getCaching();
  }

  @Override
  public TimeRange getTimeRange() {
    return super.getTimeRange();
  }

  @Override
  public Filter getFilter() {
    return super.getFilter();
  }

  @Override
  public boolean hasFilter() {
    return super.hasFilter();
  }

  @Override
  public boolean getCacheBlocks() {
    return super.getCacheBlocks();
  }

  @Override
  public boolean isReversed() {
    return super.isReversed();
  }

  @Override
  public boolean getAllowPartialResults() {
    return super.getAllowPartialResults();
  }

  @Override
  public Map<String, Object> getFingerprint() {
    return Collections.unmodifiableMap(super.getFingerprint());
  }

  @Override
  public Map<String, Object> toMap(int maxCols) {
    return Collections.unmodifiableMap(super.toMap(maxCols));
  }

  @Override
  public boolean isRaw() {
    return super.isRaw();
  }

  @Override
  @Deprecated
  public boolean isSmall() {
    return super.isSmall();
  }

  @Override
  public boolean isScanMetricsEnabled() {
    return super.isScanMetricsEnabled();
  }

  @Override
  public Boolean isAsyncPrefetch() {
    return super.isAsyncPrefetch();
  }

  @Override
  public int getLimit() {
    return super.getLimit();
  }

  @Override
  public ReadType getReadType() {
    return super.getReadType();
  }

  @Override
  long getMvccReadPoint() {
    return super.getMvccReadPoint();
  }

  @Override
  public boolean isNeedCursorResult() {
    return super.isNeedCursorResult();
  }

}
