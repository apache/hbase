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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Captures region level scan metrics as a map of metric name ({@link String}) -> Value
 * ({@link AtomicLong}). <br/>
 * <br/>
 * One instance stores scan metrics for a single region only.
 */
@InterfaceAudience.Private
public class RegionScanMetricsData {
  private final Map<String, AtomicLong> counters = new HashMap<>();
  private ScanMetricsRegionInfo scanMetricsRegionInfo =
    ScanMetricsRegionInfo.EMPTY_SCAN_METRICS_REGION_INFO;

  AtomicLong createCounter(String counterName) {
    return ScanMetricsUtil.createCounter(counters, counterName);
  }

  void setCounter(String counterName, long value) {
    ScanMetricsUtil.setCounter(counters, counterName, value);
  }

  void addToCounter(String counterName, long delta) {
    ScanMetricsUtil.addToCounter(counters, counterName, delta);
  }

  Map<String, Long> getMetricsMap(boolean reset) {
    return ScanMetricsUtil.getMetricsMap(counters, reset);
  }

  @Override
  public String toString() {
    return "EncodedRegionName=" + scanMetricsRegionInfo.getEncodedRegionName() + "," + "ServerName="
      + scanMetricsRegionInfo.getServerName() + "," + counters;
  }

  /**
   * Populate encoded region name and server name details if not already populated. If details are
   * already populated and a re-attempt is done then {@link UnsupportedOperationException} is
   * thrown.
   */
  void initScanMetricsRegionInfo(String encodedRegionName, ServerName serverName) {
    // Check by reference
    if (scanMetricsRegionInfo == ScanMetricsRegionInfo.EMPTY_SCAN_METRICS_REGION_INFO) {
      scanMetricsRegionInfo = new ScanMetricsRegionInfo(encodedRegionName, serverName);
    } else {
      throw new UnsupportedOperationException("ScanMetricsRegionInfo has already been initialized");
    }
  }

  ScanMetricsRegionInfo getScanMetricsRegionInfo() {
    return scanMetricsRegionInfo;
  }
}
