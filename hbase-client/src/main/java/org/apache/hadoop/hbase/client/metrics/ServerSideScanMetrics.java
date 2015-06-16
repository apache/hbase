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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import com.google.common.collect.ImmutableMap;

/**
 * Provides server side metrics related to scan operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServerSideScanMetrics {
  /**
   * Hash to hold the String -> Atomic Long mappings for each metric
   */
  private final Map<String, AtomicLong> counters = new HashMap<String, AtomicLong>();

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

  public static final String COUNT_OF_ROWS_SCANNED_KEY = "ROWS_SCANNED";
  public static final String COUNT_OF_ROWS_FILTERED_KEY = "ROWS_FILTERED";

  /**
   * number of rows filtered during scan RPC
   */
  public final AtomicLong countOfRowsFiltered = createCounter(COUNT_OF_ROWS_FILTERED_KEY);

  /**
   * number of rows scanned during scan RPC. Not every row scanned will be returned to the client
   * since rows may be filtered.
   */
  public final AtomicLong countOfRowsScanned = createCounter(COUNT_OF_ROWS_SCANNED_KEY);

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
   * @return A Map of String -> Long for metrics
   */
  public Map<String, Long> getMetricsMap() {
    // Create a builder
    ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
    // For every entry add the value and reset the AtomicLong back to zero
    for (Map.Entry<String, AtomicLong> e : this.counters.entrySet()) {
      builder.put(e.getKey(), e.getValue().getAndSet(0));
    }
    // Build the immutable map so that people can't mess around with it.
    return builder.build();
  }
}
