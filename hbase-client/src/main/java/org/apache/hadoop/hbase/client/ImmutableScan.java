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
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Immutable version of Scan
 */
@InterfaceAudience.Private
public final class ImmutableScan extends Scan {

  private final Scan delegateScan;

  /**
   * Create Immutable instance of Scan from given Scan object
   *
   * @param scan Copy all values from Scan
   */
  public ImmutableScan(Scan scan) {
    this.delegateScan = scan;
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
  public Scan setStartStopRowForPrefixScan(byte[] rowPrefix) {
    throw new UnsupportedOperationException(
      "ImmutableScan does not allow access to setStartStopRowForPrefixScan");
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
  public Scan setReplicaId(int id) {
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
    return this.delegateScan.getMaxResultSize();
  }

  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return Collections.unmodifiableMap(this.delegateScan.getFamilyMap());
  }

  @Override
  public int numFamilies() {
    return this.delegateScan.numFamilies();
  }

  @Override
  public boolean hasFamilies() {
    return this.delegateScan.hasFamilies();
  }

  @Override
  public byte[][] getFamilies() {
    final byte[][] families = this.delegateScan.getFamilies();
    byte[][] cloneFamilies = new byte[families.length][];
    for (int i = 0; i < families.length; i++) {
      cloneFamilies[i] = Bytes.copy(families[i]);
    }
    return cloneFamilies;
  }

  @Override
  public byte[] getStartRow() {
    final byte[] startRow = this.delegateScan.getStartRow();
    return Bytes.copy(startRow);
  }

  @Override
  public boolean includeStartRow() {
    return this.delegateScan.includeStartRow();
  }

  @Override
  public byte[] getStopRow() {
    final byte[] stopRow = this.delegateScan.getStopRow();
    return Bytes.copy(stopRow);
  }

  @Override
  public boolean includeStopRow() {
    return this.delegateScan.includeStopRow();
  }

  @Override
  public int getMaxVersions() {
    return this.delegateScan.getMaxVersions();
  }

  @Override
  public int getBatch() {
    return this.delegateScan.getBatch();
  }

  @Override
  public int getMaxResultsPerColumnFamily() {
    return this.delegateScan.getMaxResultsPerColumnFamily();
  }

  @Override
  public int getRowOffsetPerColumnFamily() {
    return this.delegateScan.getRowOffsetPerColumnFamily();
  }

  @Override
  public int getCaching() {
    return this.delegateScan.getCaching();
  }

  @Override
  public TimeRange getTimeRange() {
    return this.delegateScan.getTimeRange();
  }

  @Override
  public Filter getFilter() {
    return this.delegateScan.getFilter();
  }

  @Override
  public boolean hasFilter() {
    return this.delegateScan.hasFilter();
  }

  @Override
  public boolean getCacheBlocks() {
    return this.delegateScan.getCacheBlocks();
  }

  @Override
  public boolean isReversed() {
    return this.delegateScan.isReversed();
  }

  @Override
  public boolean getAllowPartialResults() {
    return this.delegateScan.getAllowPartialResults();
  }

  @Override
  public byte[] getACL() {
    final byte[] acl = this.delegateScan.getACL();
    return Bytes.copy(acl);
  }

  @Override
  public Map<String, Object> getFingerprint() {
    return Collections.unmodifiableMap(this.delegateScan.getFingerprint());
  }

  @Override
  public Map<String, Object> toMap(int maxCols) {
    return Collections.unmodifiableMap(this.delegateScan.toMap(maxCols));
  }

  @Override
  public boolean isRaw() {
    return this.delegateScan.isRaw();
  }

  @Override
  public boolean isScanMetricsEnabled() {
    return this.delegateScan.isScanMetricsEnabled();
  }

  @Override
  public Boolean isAsyncPrefetch() {
    return this.delegateScan.isAsyncPrefetch();
  }

  @Override
  public int getLimit() {
    return this.delegateScan.getLimit();
  }

  @Override
  public ReadType getReadType() {
    return this.delegateScan.getReadType();
  }

  @Override
  long getMvccReadPoint() {
    return this.delegateScan.getMvccReadPoint();
  }

  @Override
  public boolean isNeedCursorResult() {
    return this.delegateScan.isNeedCursorResult();
  }

  @Override
  public byte[] getAttribute(String name) {
    final byte[] attribute = this.delegateScan.getAttribute(name);
    return Bytes.copy(attribute);
  }

  @Override
  public Consistency getConsistency() {
    return this.delegateScan.getConsistency();
  }

  @Override
  public long getAttributeSize() {
    return this.delegateScan.getAttributeSize();
  }

  @Override
  public Map<String, byte[]> getAttributesMap() {
    return Collections.unmodifiableMap(this.delegateScan.getAttributesMap());
  }

  @Override
  public Boolean getLoadColumnFamiliesOnDemandValue() {
    return this.delegateScan.getLoadColumnFamiliesOnDemandValue();
  }

  @Override
  public int getPriority() {
    return this.delegateScan.getPriority();
  }

  @Override
  public Map<byte[], TimeRange> getColumnFamilyTimeRange() {
    return Collections.unmodifiableMap(this.delegateScan.getColumnFamilyTimeRange());
  }

  @Override
  public int getReplicaId() {
    return this.delegateScan.getReplicaId();
  }

  @Override
  public boolean doLoadColumnFamiliesOnDemand() {
    return this.delegateScan.doLoadColumnFamiliesOnDemand();
  }

  @Override
  public String getId() {
    return this.delegateScan.getId();
  }

  @Override
  public boolean isGetScan() {
    return this.delegateScan.isGetScan();
  }

  @Override
  public IsolationLevel getIsolationLevel() {
    return this.delegateScan.getIsolationLevel();
  }

  @Override
  public Authorizations getAuthorizations() throws DeserializationException {
    return this.delegateScan.getAuthorizations();
  }

  @Override
  public String toString(int maxCols) {
    return this.delegateScan.toString(maxCols);
  }

  @Override
  public String toString() {
    return this.delegateScan.toString();
  }

  @Override
  public Map<String, Object> toMap() {
    return Collections.unmodifiableMap(this.delegateScan.toMap());
  }

  @Override
  public String toJSON(int maxCols) throws IOException {
    return this.delegateScan.toJSON(maxCols);
  }

  @Override
  public String toJSON() throws IOException {
    return this.delegateScan.toJSON();
  }

}
