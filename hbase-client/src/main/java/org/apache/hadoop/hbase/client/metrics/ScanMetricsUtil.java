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
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class ScanMetricsUtil {

  private ScanMetricsUtil() {}

  /**
   * Creates a new counter with the specified name and stores it in the counters map.
   * @return {@link AtomicLong} instance for the counter with counterName
   */
  static AtomicLong createCounter(Map<String, AtomicLong> counters, String counterName) {
    AtomicLong c = new AtomicLong(0);
    counters.put(counterName, c);
    return c;
  }

  /** Sets counter with counterName to passed in value, does nothing if counter does not exist. */
  static void setCounter(Map<String, AtomicLong> counters, String counterName, long value) {
    AtomicLong c = counters.get(counterName);
    if (c != null) {
      c.set(value);
    }
  }

  /** Increments the counter with counterName by delta, does nothing if counter does not exist. */
  static void addToCounter(Map<String, AtomicLong> counters, String counterName, long delta) {
    AtomicLong c = counters.get(counterName);
    if (c != null) {
      c.addAndGet(delta);
    }
  }

  /** Returns true if a counter exists with the counterName */
  static boolean hasCounter(Map<String, AtomicLong> counters, String counterName) {
    return counters.containsKey(counterName);
  }

  /** Returns {@link AtomicLong} instance for this counter name, null if counter does not exist. */
  static AtomicLong getCounter(Map<String, AtomicLong> counters, String counterName) {
    return counters.get(counterName);
  }

  /**
   * Get all of the values. If reset is true, we will reset the all AtomicLongs back to 0.
   * @param reset whether to reset the AtomicLongs to 0.
   * @return A Map of String -> Long for metrics
   */
  static Map<String, Long> getMetricsMap(Map<String, AtomicLong> counters, boolean reset) {
    Map<String, Long> metricsSnapshot = new HashMap<>();
    for (Map.Entry<String, AtomicLong> e : counters.entrySet()) {
      long value = reset ? e.getValue().getAndSet(0) : e.getValue().get();
      metricsSnapshot.put(e.getKey(), value);
    }
    return metricsSnapshot;
  }
}
