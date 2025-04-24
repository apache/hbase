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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.ScanMetricsRegionInfo.EMPTY_SCAN_METRICS_REGION_INFO;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@InterfaceAudience.Private
public class ScanMetricsHolder {
  private final Map<String, AtomicLong> counters = new HashMap<>();
  private ScanMetricsRegionInfo scanMetricsRegionInfo = EMPTY_SCAN_METRICS_REGION_INFO;

  /**
   * Create a new counter with the specified name
   * @return {@link AtomicLong} instance for the counter with counterName
   */
  public AtomicLong createCounter(String counterName) {
    AtomicLong c = new AtomicLong(0);
    counters.put(counterName, c);
    return c;
  }

  public void setCounter(String counterName, long value) {
    AtomicLong c = this.counters.get(counterName);
    if (c != null) {
      c.set(value);
    }
  }

  /** Returns true if a counter exists with the counterName */
  public boolean hasCounter(String counterName) {
    return this.counters.containsKey(counterName);
  }

  /** Returns {@link AtomicLong} instance for this counter name, null if counter does not exist. */
  public AtomicLong getCounter(String counterName) {
    return this.counters.get(counterName);
  }

  public void addToCounter(String counterName, long delta) {
    AtomicLong c = this.counters.get(counterName);
    if (c != null) {
      c.addAndGet(delta);
    }
  }

  /**
   * Get all of the values since the last time this function was called. Calling this function will
   * reset all AtomicLongs in the instance back to 0.
   * @return A Map of String -&gt; Long for metrics
   */
  public Map<String, Long> getMetricsMap() {
    return getMetricsMap(true);
  }

  /**
   * Get all of the values. If reset is true, we will reset the all AtomicLongs back to 0.
   * @param reset whether to reset the AtomicLongs to 0.
   * @return A Map of String -&gt; Long for metrics
   */
  public Map<String, Long> getMetricsMap(boolean reset) {
    // Create a builder
    ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
    for (Map.Entry<String, AtomicLong> e : this.counters.entrySet()) {
      long value = reset ? e.getValue().getAndSet(0) : e.getValue().get();
      builder.put(e.getKey(), value);
    }
    // Build the immutable map so that people can't mess around with it.
    return builder.build();
  }

  @Override
  public String toString() {
    ToStringBuilder tsb = new ToStringBuilder(this).
      append("ServerName", scanMetricsRegionInfo.getServerName()).
      append("EncodedRegionName", scanMetricsRegionInfo.getEncodedRegionName());
    for (Map.Entry<String, AtomicLong> e : counters.entrySet()) {
      tsb.append(e.getKey()).append(e.getValue().get());
    }
    return tsb.toString();
  }

  public void initScanMetricsRegionInfo(
    ServerName serverName, String encodedRegionName) {
    // Check by reference
    if (scanMetricsRegionInfo == EMPTY_SCAN_METRICS_REGION_INFO) {
      scanMetricsRegionInfo = new ScanMetricsRegionInfo(encodedRegionName, serverName);
    }
    else {
      throw new UnsupportedOperationException("ScanMetricsRegionInfo has already been initialized");
    }
  }

  public ScanMetricsRegionInfo getScanMetricsRegionInfo() {
    return scanMetricsRegionInfo;
  }
}
