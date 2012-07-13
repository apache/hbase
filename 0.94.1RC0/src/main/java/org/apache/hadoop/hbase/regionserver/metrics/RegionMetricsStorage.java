/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.regionserver.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Pair;

/**
 * This class if for maintaining the maps used to power metrics for hfiles,
 * regions, and regionservers. It has methods to mutate and get state of metrics
 * numbers. These numbers are exposed to Hadoop metrics through
 * RegionServerDynamicMetrics.
 */
public class RegionMetricsStorage {

  // for simple numeric metrics (# of blocks read from block cache)
  private static final ConcurrentMap<String, AtomicLong> numericMetrics =
      new ConcurrentHashMap<String, AtomicLong>();

  // for simple numeric metrics (current block cache size)
  // These ones are not reset to zero when queried, unlike the previous.
  private static final ConcurrentMap<String, AtomicLong> numericPersistentMetrics =
      new ConcurrentHashMap<String, AtomicLong>();

  /**
   * Used for metrics where we want track a metrics (such as latency) over a
   * number of operations.
   */
  private static final ConcurrentMap<String, Pair<AtomicLong, AtomicInteger>> timeVaryingMetrics =
      new ConcurrentHashMap<String, Pair<AtomicLong, AtomicInteger>>();

  public static Map<String, AtomicLong> getNumericMetrics() {
    return numericMetrics;
  }

  public static Map<String, AtomicLong> getNumericPersistentMetrics() {
    return numericPersistentMetrics;
  }

  public static Map<String, Pair<AtomicLong, AtomicInteger>> getTimeVaryingMetrics() {
    return timeVaryingMetrics;
  }

  public static void incrNumericMetric(String key, long amount) {
    AtomicLong oldVal = numericMetrics.get(key);
    if (oldVal == null) {
      oldVal = numericMetrics.putIfAbsent(key, new AtomicLong(amount));
      if (oldVal == null)
        return;
    }
    oldVal.addAndGet(amount);
  }

  public static void incrTimeVaryingMetric(String key, long amount) {
    Pair<AtomicLong, AtomicInteger> oldVal = timeVaryingMetrics.get(key);
    if (oldVal == null) {
      oldVal =
          timeVaryingMetrics.putIfAbsent(key, 
              new Pair<AtomicLong, AtomicInteger>(
                  new AtomicLong(amount), 
                  new AtomicInteger(1)));
      if (oldVal == null)
        return;
    }
    oldVal.getFirst().addAndGet(amount); // total time
    oldVal.getSecond().incrementAndGet(); // increment ops by 1
  }

  public static void incrNumericPersistentMetric(String key, long amount) {
    AtomicLong oldVal = numericPersistentMetrics.get(key);
    if (oldVal == null) {
      oldVal = numericPersistentMetrics.putIfAbsent(key, new AtomicLong(amount));
      if (oldVal == null)
        return;
    }
    oldVal.addAndGet(amount);
  }

  public static void setNumericMetric(String key, long amount) {
    numericMetrics.put(key, new AtomicLong(amount));
  }

  public static long getNumericMetric(String key) {
    AtomicLong m = numericMetrics.get(key);
    if (m == null)
      return 0;
    return m.get();
  }

  public static Pair<Long, Integer> getTimeVaryingMetric(String key) {
    Pair<AtomicLong, AtomicInteger> pair = timeVaryingMetrics.get(key);
    if (pair == null) {
      return new Pair<Long, Integer>(0L, 0);
    }

    return new Pair<Long, Integer>(pair.getFirst().get(), pair.getSecond().get());
  }

  public static long getNumericPersistentMetric(String key) {
    AtomicLong m = numericPersistentMetrics.get(key);
    if (m == null)
      return 0;
    return m.get();
  }

  /**
   * Clear all copies of the metrics this stores.
   */
  public static void clear() {
    timeVaryingMetrics.clear();
    numericMetrics.clear();
    numericPersistentMetrics.clear();
  }
}
