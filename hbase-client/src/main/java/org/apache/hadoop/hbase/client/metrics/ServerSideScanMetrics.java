/**
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

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Provides server side metrics related to scan operations.
 */
@InterfaceAudience.Public
public class ServerSideScanMetrics {
  /**
   * Hash to hold the String -&gt; Atomic Long mappings for each metric
   */
  private final Map<String, AtomicLong> counters = new HashMap<>();

  /**
   * Create a new counter with the specified name
   * @param counterName
   * @return {@link AtomicLong} instance for the counter with counterName
   */
  protected AtomicLong createCounter(String counterName) {
    AtomicLong c = new AtomicLong(0);
    counters.put(counterName, c);
    return c;
  }

  public static final String COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME = "ROWS_SCANNED";
  public static final String COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME = "ROWS_FILTERED";

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-17886">HBASE-17886</a>).
   *             Use {@link #COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME}.
   */
  @Deprecated
  public static final String COUNT_OF_ROWS_SCANNED_KEY = COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME;

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-17886">HBASE-17886</a>).
   *             Use {@link #COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME}.
   */
  @Deprecated
  public static final String COUNT_OF_ROWS_FILTERED_KEY = COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME;

  /**
   * number of rows filtered during scan RPC
   */
  public final AtomicLong countOfRowsFiltered = createCounter(COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME);

  /**
   * number of rows scanned during scan RPC. Not every row scanned will be returned to the client
   * since rows may be filtered.
   */
  public final AtomicLong countOfRowsScanned = createCounter(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);

  /**
   * @param counterName
   * @param value
   */
  public void setCounter(String counterName, long value) {
    AtomicLong c = this.counters.get(counterName);
    if (c != null) {
      c.set(value);
    }
  }

  /**
   * @param counterName
   * @return true if a counter exists with the counterName
   */
  public boolean hasCounter(String counterName) {
    return this.counters.containsKey(counterName);
  }

  /**
   * @param counterName
   * @return {@link AtomicLong} instance for this counter name, null if counter does not exist.
   */
  public AtomicLong getCounter(String counterName) {
    return this.counters.get(counterName);
  }

  /**
   * @param counterName
   * @param delta
   */
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
}
