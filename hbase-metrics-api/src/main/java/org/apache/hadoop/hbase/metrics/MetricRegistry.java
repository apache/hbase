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
package org.apache.hadoop.hbase.metrics;

import java.util.Optional;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * General purpose factory for creating various metrics.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface MetricRegistry extends MetricSet {

  /**
   * Get or construct a {@link Timer} used to measure durations and report rates.
   *
   * @param name the name of the timer.
   * @return An instance of {@link Timer}.
   */
  Timer timer(String name);

  /**
   * Get or construct a {@link Histogram} used to measure a distribution of values.
   *
   * @param name The name of the Histogram.
   * @return An instance of {@link Histogram}.
   */
  Histogram histogram(String name);

  /**
   * Get or construct a {@link Meter} used to measure durations and report distributions (a
   * combination of a {@link Timer} and a {@link Histogram}.
   *
   * @param name The name of the Meter.
   * @return An instance of {@link Meter}.
   */
  Meter meter(String name);

  /**
   * Get or construct a {@link Counter} used to track a mutable number.
   *
   * @param name The name of the Counter
   * @return An instance of {@link Counter}.
   */
  Counter counter(String name);

  /**
   * Register a {@link Gauge}. The Gauge will be invoked at a period defined by the implementation
   * of {@link MetricRegistry}.
   * @param name The name of the Gauge.
   * @param gauge A callback to compute the current value.
   * @return the registered gauge, or the existing gauge
   */
  <T> Gauge<T> register(String name, Gauge<T> gauge);

  /**
   * Registers the {@link Metric} with the given name if there does not exist one with the same
   * name. Returns the newly registered or existing Metric.
   * @param name The name of the Metric.
   * @param metric the metric to register
   * @return the registered metric, or the existing metrid
   */
  Metric register(String name, Metric metric);

  /**
   * Registers the {@link Metric}s in the given MetricSet.
   * @param metricSet set of metrics to register.
   */
  void registerAll(MetricSet metricSet);

  /**
   * Returns previously registered metric with the name if any.
   * @param name the name of the metric
   * @return previously registered metric
   */
  Optional<Metric> get(String name);

  /**
   * Removes the metric with the given name.
   *
   * @param name the name of the metric
   * @return true if the metric is removed.
   */
  boolean remove(String name);

  /**
   * Return the MetricRegistryInfo object for this registry.
   * @return MetricRegistryInfo describing the registry.
   */
  @InterfaceAudience.Private
  MetricRegistryInfo getMetricRegistryInfo();
}
