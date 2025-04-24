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
package org.apache.hadoop.hbase.client.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ScanMetricsHolder;
import org.apache.hadoop.hbase.client.ScanMetricsRegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Provides server side metrics related to scan operations.
 */
@InterfaceAudience.Public
@SuppressWarnings("checkstyle:VisibilityModifier") // See HBASE-27757
public class ServerSideScanMetrics {
  /**
   * Hash to hold the String -&gt; Atomic Long mappings for each metric
   */
  private List<ScanMetricsHolder> scanMetricsHolders = new ArrayList<>();
  private ScanMetricsHolder currentScanMetricsHolder;

  public ServerSideScanMetrics() {
    createScanMetricsHolderInternal();
  }

  public void createScanMetricsHolder() {
    createScanMetricsHolderInternal();
  }

  private void createScanMetricsHolderInternal() {
    currentScanMetricsHolder = new ScanMetricsHolder();
    scanMetricsHolders.add(currentScanMetricsHolder);
    countOfRowsScanned = createCounter(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);
    countOfRowsFiltered = createCounter(COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME);
    countOfBlockBytesScanned = createCounter(BLOCK_BYTES_SCANNED_KEY_METRIC_NAME);
    fsReadTime = createCounter(FS_READ_TIME_METRIC_NAME);
  }

  /**
   * Create a new counter with the specified name
   * @return {@link AtomicLong} instance for the counter with counterName
   */
  protected AtomicLong createCounter(String counterName) {
    return currentScanMetricsHolder.createCounter(counterName);
  }

  public static final String COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME = "ROWS_SCANNED";
  public static final String COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME = "ROWS_FILTERED";

  public static final String BLOCK_BYTES_SCANNED_KEY_METRIC_NAME = "BLOCK_BYTES_SCANNED";

  public static final String FS_READ_TIME_METRIC_NAME = "FS_READ_TIME";

  /**
   * number of rows filtered during scan RPC
   */
  public AtomicLong countOfRowsFiltered;

  /**
   * number of rows scanned during scan RPC. Not every row scanned will be returned to the client
   * since rows may be filtered.
   */
  public AtomicLong countOfRowsScanned;

  public AtomicLong countOfBlockBytesScanned;

  public AtomicLong fsReadTime;

  public void setCounter(String counterName, long value) {
    currentScanMetricsHolder.setCounter(counterName, value);
  }

  /** Returns true if a counter exists with the counterName */
  public boolean hasCounter(String counterName) {
    return currentScanMetricsHolder.hasCounter(counterName);
  }

  /** Returns {@link AtomicLong} instance for this counter name, null if counter does not exist. */
  public AtomicLong getCounter(String counterName) {
    return currentScanMetricsHolder.getCounter(counterName);
  }

  public void addToCounter(String counterName, long delta) {
    currentScanMetricsHolder.addToCounter(counterName, delta);
  }

  /**
   * Get all of the values since the last time this function was called. Calling this function will
   * reset all AtomicLongs in the instance back to 0.
   * @return A Map of String -&gt; Long for metrics
   */
  public Map<String, Long> getMetricsMap() {
    return currentScanMetricsHolder.getMetricsMap(true);
  }

  /**
   * Get all of the values. If reset is true, we will reset the all AtomicLongs back to 0.
   * @param reset whether to reset the AtomicLongs to 0.
   * @return A Map of String -&gt; Long for metrics
   */
  public Map<String, Long> getMetricsMap(boolean reset) {
    return currentScanMetricsHolder.getMetricsMap(reset);
  }

  public Map<ScanMetricsRegionInfo, Map<String, Long>> getMetricsMapByRegion() {
    return getMetricsMapByRegion(true);
  }

  public Map<ScanMetricsRegionInfo, Map<String, Long>> getMetricsMapByRegion(boolean reset) {
    // Create a builder
    ImmutableMap.Builder<ScanMetricsRegionInfo, Map<String, Long>> builder = ImmutableMap.builder();
    for (ScanMetricsHolder scanMetricsHolder : scanMetricsHolders) {
      builder.put(scanMetricsHolder.getScanMetricsRegionInfo(),
        scanMetricsHolder.getMetricsMap(reset));
    }
    return builder.build();
  }

  @Override
  public String toString() {
    ToStringBuilder tsb = new ToStringBuilder(this);
    for (ScanMetricsHolder h : scanMetricsHolders) {
      tsb.append(h.toString());
    }
    return tsb.toString();
  }

  public void initScanMetricsRegionInfo(ServerName serverName, String encodedRegionName) {
    currentScanMetricsHolder.initScanMetricsRegionInfo(serverName, encodedRegionName);
  }
}
