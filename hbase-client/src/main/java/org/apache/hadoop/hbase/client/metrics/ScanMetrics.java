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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.yetus.audience.InterfaceAudience;


/**
 * Provides metrics related to scan operations (both server side and client side metrics).
 * <p>
 * The data can be passed to mapreduce framework or other systems.
 * We use atomic longs so that one thread can increment,
 * while another atomically resets to zero after the values are reported
 * to hadoop's counters.
 * <p>
 * Some of these metrics are general for any client operation such as put
 * However, there is no need for this. So they are defined under scan operation
 * for now.
 */
@InterfaceAudience.Public
public class ScanMetrics extends ServerSideScanMetrics {

  // AtomicLongs to hold the metrics values. These are all updated through ClientScanner and
  // ScannerCallable. They are atomic longs so that atomic getAndSet can be used to reset the
  // values after progress is passed to hadoop's counters.

  public static final String RPC_CALLS_METRIC_NAME = "RPC_CALLS";
  public static final String REMOTE_RPC_CALLS_METRIC_NAME = "REMOTE_RPC_CALLS";
  public static final String MILLIS_BETWEEN_NEXTS_METRIC_NAME = "MILLIS_BETWEEN_NEXTS";
  public static final String NOT_SERVING_REGION_EXCEPTION_METRIC_NAME = "NOT_SERVING_REGION_EXCEPTION";
  public static final String BYTES_IN_RESULTS_METRIC_NAME = "BYTES_IN_RESULTS";
  public static final String BYTES_IN_REMOTE_RESULTS_METRIC_NAME = "BYTES_IN_REMOTE_RESULTS";
  public static final String REGIONS_SCANNED_METRIC_NAME = "REGIONS_SCANNED";
  public static final String RPC_RETRIES_METRIC_NAME = "RPC_RETRIES";
  public static final String REMOTE_RPC_RETRIES_METRIC_NAME = "REMOTE_RPC_RETRIES";
 
  /**
   * number of RPC calls
   */
  public final AtomicLong countOfRPCcalls = createCounter(RPC_CALLS_METRIC_NAME);

  /**
   * number of remote RPC calls
   */
  public final AtomicLong countOfRemoteRPCcalls = createCounter(REMOTE_RPC_CALLS_METRIC_NAME);

  /**
   * sum of milliseconds between sequential next calls
   */
  public final AtomicLong sumOfMillisSecBetweenNexts = createCounter(MILLIS_BETWEEN_NEXTS_METRIC_NAME);

  /**
   * number of NotServingRegionException caught
   */
  public final AtomicLong countOfNSRE = createCounter(NOT_SERVING_REGION_EXCEPTION_METRIC_NAME);

  /**
   * number of bytes in Result objects from region servers
   */
  public final AtomicLong countOfBytesInResults = createCounter(BYTES_IN_RESULTS_METRIC_NAME);

  /**
   * number of bytes in Result objects from remote region servers
   */
  public final AtomicLong countOfBytesInRemoteResults = createCounter(BYTES_IN_REMOTE_RESULTS_METRIC_NAME);

  /**
   * number of regions
   */
  public final AtomicLong countOfRegions = createCounter(REGIONS_SCANNED_METRIC_NAME);

  /**
   * number of RPC retries
   */
  public final AtomicLong countOfRPCRetries = createCounter(RPC_RETRIES_METRIC_NAME);

  /**
   * number of remote RPC retries
   */
  public final AtomicLong countOfRemoteRPCRetries = createCounter(REMOTE_RPC_RETRIES_METRIC_NAME);

  /**
   * constructor
   */
  public ScanMetrics() {
  }
}
