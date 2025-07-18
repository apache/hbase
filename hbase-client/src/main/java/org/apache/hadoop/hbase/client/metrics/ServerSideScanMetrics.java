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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Provides server side metrics related to scan operations.
 */
@InterfaceAudience.Public
@SuppressWarnings("checkstyle:VisibilityModifier") // See HBASE-27757
public class ServerSideScanMetrics {
  /**
   * Hash to hold the String -> Atomic Long mappings for each metric
   */
  private final Map<String, AtomicLong> counters = new HashMap<>();
  private final List<RegionScanMetricsData> regionScanMetricsData = new ArrayList<>(0);
  protected RegionScanMetricsData currentRegionScanMetricsData = null;

  /**
   * If region level scan metrics are enabled, must call this method to start collecting metrics for
   * the region before scanning the region.
   */
  public void moveToNextRegion() {
    currentRegionScanMetricsData = new RegionScanMetricsData();
    regionScanMetricsData.add(currentRegionScanMetricsData);
    currentRegionScanMetricsData.createCounter(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);
    currentRegionScanMetricsData.createCounter(COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME);
    currentRegionScanMetricsData.createCounter(BLOCK_BYTES_SCANNED_KEY_METRIC_NAME);
    currentRegionScanMetricsData.createCounter(FS_READ_TIME_METRIC_NAME);
    currentRegionScanMetricsData.createCounter(BYTES_READ_FROM_FS_METRIC_NAME);
    currentRegionScanMetricsData.createCounter(BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME);
    currentRegionScanMetricsData.createCounter(BYTES_READ_FROM_MEMSTORE_METRIC_NAME);
    currentRegionScanMetricsData.createCounter(BLOCK_READ_OPS_COUNT_METRIC_NAME);
  }

  /**
   * Create a new counter with the specified name.
   * @return {@link AtomicLong} instance for the counter with counterName
   */
  protected AtomicLong createCounter(String counterName) {
    return ScanMetricsUtil.createCounter(counters, counterName);
  }

  public static final String COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME = "ROWS_SCANNED";
  public static final String COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME = "ROWS_FILTERED";

  public static final String BLOCK_BYTES_SCANNED_KEY_METRIC_NAME = "BLOCK_BYTES_SCANNED";

  public static final String FS_READ_TIME_METRIC_NAME = "FS_READ_TIME";
  public static final String BYTES_READ_FROM_FS_METRIC_NAME = "BYTES_READ_FROM_FS";
  public static final String BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME =
    "BYTES_READ_FROM_BLOCK_CACHE";
  public static final String BYTES_READ_FROM_MEMSTORE_METRIC_NAME = "BYTES_READ_FROM_MEMSTORE";
  public static final String BLOCK_READ_OPS_COUNT_METRIC_NAME = "BLOCK_READ_OPS_COUNT";

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-17886">HBASE-17886</a>). Use
   *             {@link #COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME}.
   */
  @Deprecated
  public static final String COUNT_OF_ROWS_SCANNED_KEY = COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME;

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-17886">HBASE-17886</a>). Use
   *             {@link #COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME}.
   */
  @Deprecated
  public static final String COUNT_OF_ROWS_FILTERED_KEY = COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME;

  /**
   * number of rows filtered during scan RPC
   */
  public final AtomicLong countOfRowsFiltered =
    createCounter(COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME);

  /**
   * number of rows scanned during scan RPC. Not every row scanned will be returned to the client
   * since rows may be filtered.
   */
  public final AtomicLong countOfRowsScanned = createCounter(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);

  public final AtomicLong countOfBlockBytesScanned =
    createCounter(BLOCK_BYTES_SCANNED_KEY_METRIC_NAME);

  public final AtomicLong fsReadTime = createCounter(FS_READ_TIME_METRIC_NAME);

  public final AtomicLong bytesReadFromFs = createCounter(BYTES_READ_FROM_FS_METRIC_NAME);

  public final AtomicLong bytesReadFromBlockCache =
    createCounter(BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME);

  public final AtomicLong bytesReadFromMemstore =
    createCounter(BYTES_READ_FROM_MEMSTORE_METRIC_NAME);

  public final AtomicLong blockReadOpsCount = createCounter(BLOCK_READ_OPS_COUNT_METRIC_NAME);

  /**
   * Sets counter with counterName to passed in value, does nothing if counter does not exist. If
   * region level scan metrics are enabled then sets the value of counter for the current region
   * being scanned.
   */
  public void setCounter(String counterName, long value) {
    ScanMetricsUtil.setCounter(counters, counterName, value);
    if (this.currentRegionScanMetricsData != null) {
      this.currentRegionScanMetricsData.setCounter(counterName, value);
    }
  }

  /**
   * Returns true if a counter exists with the counterName.
   */
  public boolean hasCounter(String counterName) {
    return ScanMetricsUtil.hasCounter(counters, counterName);
  }

  /**
   * Returns {@link AtomicLong} instance for this counter name, null if counter does not exist.
   */
  public AtomicLong getCounter(String counterName) {
    return ScanMetricsUtil.getCounter(counters, counterName);
  }

  /**
   * Increments the counter with counterName by delta, does nothing if counter does not exist. If
   * region level scan metrics are enabled then increments the counter corresponding to the current
   * region being scanned. Please see {@link #moveToNextRegion()}.
   */
  public void addToCounter(String counterName, long delta) {
    ScanMetricsUtil.addToCounter(counters, counterName, delta);
    if (this.currentRegionScanMetricsData != null) {
      this.currentRegionScanMetricsData.addToCounter(counterName, delta);
    }
  }

  /**
   * Get all the values combined for all the regions since the last time this function was called.
   * Calling this function will reset all AtomicLongs in the instance back to 0.
   * @return A Map of String -> Long for metrics
   */
  public Map<String, Long> getMetricsMap() {
    return getMetricsMap(true);
  }

  /**
   * Get all the values combined for all the regions. If reset is true, we will reset all the
   * AtomicLongs back to 0.
   * @param reset whether to reset the AtomicLongs to 0.
   * @return A Map of String -> Long for metrics
   */
  public Map<String, Long> getMetricsMap(boolean reset) {
    return ImmutableMap.copyOf(ScanMetricsUtil.collectMetrics(counters, reset));
  }

  /**
   * Get values grouped by each region scanned since the last time this was called. Calling this
   * function will reset all region level scan metrics counters back to 0.
   * @return A Map of region -> (Map of metric name -> Long) for metrics
   */
  public Map<ScanMetricsRegionInfo, Map<String, Long>> collectMetricsByRegion() {
    return collectMetricsByRegion(true);
  }

  /**
   * Get values grouped by each region scanned. If reset is true, will reset all the region level
   * scan metrics counters back to 0.
   * @param reset whether to reset region level scan metric counters to 0.
   * @return A Map of region -> (Map of metric name -> Long) for metrics
   */
  public Map<ScanMetricsRegionInfo, Map<String, Long>> collectMetricsByRegion(boolean reset) {
    // Create a builder
    ImmutableMap.Builder<ScanMetricsRegionInfo, Map<String, Long>> builder = ImmutableMap.builder();
    for (RegionScanMetricsData regionScanMetricsData : this.regionScanMetricsData) {
      if (
        regionScanMetricsData.getScanMetricsRegionInfo()
            == ScanMetricsRegionInfo.EMPTY_SCAN_METRICS_REGION_INFO
      ) {
        continue;
      }
      builder.put(regionScanMetricsData.getScanMetricsRegionInfo(),
        regionScanMetricsData.collectMetrics(reset));
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return counters + "," + regionScanMetricsData.stream().map(RegionScanMetricsData::toString)
      .collect(Collectors.joining(","));
  }

  /**
   * Call this method after calling {@link #moveToNextRegion()} to populate server name and encoded
   * region name details for the region being scanned and for which metrics are being collected at
   * the moment.
   */
  public void initScanMetricsRegionInfo(String encodedRegionName, ServerName serverName) {
    currentRegionScanMetricsData.initScanMetricsRegionInfo(encodedRegionName, serverName);
  }
}
