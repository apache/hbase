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
 * Generic holder class for capturing Scan Metrics as a map of
 * metric name ({@link String}) -&gt Value ({@link AtomicLong}).
 */
@InterfaceAudience.Private
public class ScanMetricsHolder {
  private final Map<String, AtomicLong> counters = new HashMap<>();
  private ScanMetricsRegionInfo scanMetricsRegionInfo =
    ScanMetricsRegionInfo.EMPTY_SCAN_METRICS_REGION_INFO;

  /**
   * Create a new counter with the specified name
   * @return {@link AtomicLong} instance for the counter with counterName
   */
  AtomicLong createCounter(String counterName) {
    AtomicLong c = new AtomicLong(0);
    counters.put(counterName, c);
    return c;
  }

  /** Sets counter with counterName to passed in value, does nothing if counter does not exist. */
  void setCounter(String counterName, long value) {
    AtomicLong c = this.counters.get(counterName);
    if (c != null) {
      c.set(value);
    }
  }

  /** Returns true if a counter exists with the counterName */
  boolean hasCounter(String counterName) {
    return this.counters.containsKey(counterName);
  }

  /** Returns {@link AtomicLong} instance for this counter name, null if counter does not exist. */
  AtomicLong getCounter(String counterName) {
    return this.counters.get(counterName);
  }

  /** Increments the counter with counterName by delta, does nothing if counter does not exist. */
  void addToCounter(String counterName, long delta) {
    AtomicLong c = this.counters.get(counterName);
    if (c != null) {
      c.addAndGet(delta);
    }
  }

  /**
   * Get all of the values. If reset is true, we will reset the all AtomicLongs back to 0.
   * @param reset whether to reset the AtomicLongs to 0.
   * @return A Map of String -&gt; Long for metrics
   */
  Map<String, Long> getMetricsMap(boolean reset) {
    Map<String, Long> metricsSnapshot = new HashMap<>();
    for (Map.Entry<String, AtomicLong> e : this.counters.entrySet()) {
      long value = reset ? e.getValue().getAndSet(0) : e.getValue().get();
      metricsSnapshot.put(e.getKey(), value);
    }
    return metricsSnapshot;
  }

  @Override
  public String toString() {
    return "ServerName=" + scanMetricsRegionInfo.getServerName() + "," + "EncodedRegionName="
      + scanMetricsRegionInfo.getEncodedRegionName() + "," + counters;
  }

  /**
   * Populate server name and encoded region name details if not already populated. If details are
   * already populated and a re-attempt is done then {@link UnsupportedOperationException} is
   * thrown.
   */
  void initScanMetricsRegionInfo(
    ServerName serverName, String encodedRegionName) {
    // Check by reference
    if (scanMetricsRegionInfo == ScanMetricsRegionInfo.EMPTY_SCAN_METRICS_REGION_INFO) {
      scanMetricsRegionInfo = new ScanMetricsRegionInfo(encodedRegionName, serverName);
    }
    else {
      throw new UnsupportedOperationException("ScanMetricsRegionInfo has already been initialized");
    }
  }

  ScanMetricsRegionInfo getScanMetricsRegionInfo() {
    return scanMetricsRegionInfo;
  }
}
