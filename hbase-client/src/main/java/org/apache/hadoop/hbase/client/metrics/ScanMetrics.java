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
 * Provides client-side metrics related to scan operations.
 * The data can be passed to mapreduce framework or other systems.
 * We use atomic longs so that one thread can increment,
 * while another atomically resets to zero after the values are reported
 * to hadoop's counters.
 *
 * Some of these metrics are general for any client operation such as put
 * However, there is no need for this. So they are defined under scan operation
 * for now.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ScanMetrics {

  /**
   * Hash to hold the String -> Atomic Long mappings.
   */
  private final Map<String, AtomicLong> counters = new HashMap<String, AtomicLong>();

  // AtomicLongs to hold the metrics values.  These are all updated through ClientScanner and
  // ScannerCallable.  They are atomic longs so that atomic getAndSet can be used to reset the
  // values after progress is passed to hadoop's counters.


  /**
   * number of RPC calls
   */
  public final AtomicLong countOfRPCcalls = createCounter("RPC_CALLS");

  /**
   * number of remote RPC calls
   */
  public final AtomicLong countOfRemoteRPCcalls = createCounter("REMOTE_RPC_CALLS");

  /**
   * sum of milliseconds between sequential next calls
   */
  public final AtomicLong sumOfMillisSecBetweenNexts = createCounter("MILLIS_BETWEEN_NEXTS");

  /**
   * number of NotServingRegionException caught
   */
  public final AtomicLong countOfNSRE = createCounter("NOT_SERVING_REGION_EXCEPTION");

  /**
   * number of bytes in Result objects from region servers
   */
  public final AtomicLong countOfBytesInResults = createCounter("BYTES_IN_RESULTS");

  /**
   * number of bytes in Result objects from remote region servers
   */
  public final AtomicLong countOfBytesInRemoteResults = createCounter("BYTES_IN_REMOTE_RESULTS");

  /**
   * number of regions
   */
  public final AtomicLong countOfRegions = createCounter("REGIONS_SCANNED");

  /**
   * number of RPC retries
   */
  public final AtomicLong countOfRPCRetries = createCounter("RPC_RETRIES");

  /**
   * number of remote RPC retries
   */
  public final AtomicLong countOfRemoteRPCRetries = createCounter("REMOTE_RPC_RETRIES");

  /**
   * constructor
   */
  public ScanMetrics() {
  }

  private AtomicLong createCounter(String counterName) {
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

  /**
   * Get all of the values since the last time this function was called.
   *
   * Calling this function will reset all AtomicLongs in the instance back to 0.
   *
   * @return A Map of String -> Long for metrics
   */
  public Map<String, Long> getMetricsMap() {
    //Create a builder
    ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
    //For every entry add the value and reset the AtomicLong back to zero
    for (Map.Entry<String, AtomicLong> e : this.counters.entrySet()) {
      builder.put(e.getKey(), e.getValue().getAndSet(0));
    }
    //Build the immutable map so that people can't mess around with it.
    return builder.build();
  }

}
