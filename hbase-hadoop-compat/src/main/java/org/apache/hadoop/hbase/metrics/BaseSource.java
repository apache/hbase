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

/**
 *   BaseSource for dynamic metrics to announce to Metrics2.
 *   In hbase-hadoop{1|2}-compat there is an implementation of this interface.
 */
public interface BaseSource {

  String HBASE_METRICS_SYSTEM_NAME = "HBase";

  /**
   * Clear out the metrics and re-prepare the source.
   */
  void init();

  /**
   * Set a gauge to a specific value.
   *
   * @param gaugeName the name of the gauge
   * @param value     the value
   */
  void setGauge(String gaugeName, long value);

  /**
   * Add some amount to a gauge.
   *
   * @param gaugeName the name of the gauge
   * @param delta     the amount to change the gauge by.
   */
  void incGauge(String gaugeName, long delta);

  /**
   * Subtract some amount from a gauge.
   *
   * @param gaugeName the name of the gauge
   * @param delta     the amount to change the gauge by.
   */
  void decGauge(String gaugeName, long delta);

  /**
   * Remove a metric and no longer announce it.
   *
   * @param key Name of the gauge to remove.
   */
  void removeMetric(String key);

  /**
   * Add some amount to a counter.
   *
   * @param counterName the name of the counter
   * @param delta       the amount to change the counter by.
   */
  void incCounters(String counterName, long delta);

  /**
   * Add some value to a histogram.
   *
   * @param name the name of the histogram
   * @param value the value to add to the histogram
   */
  void updateHistogram(String name, long value);


  /**
   * Add some value to a Quantile (An accurate histogram).
   *
   * @param name the name of the quantile
   * @param value the value to add to the quantile
   */
  void updateQuantile(String name, long value);

  /**
   * Get the metrics context.  For hadoop metrics2 system this is usually an all lowercased string.
   * eg. regionserver, master, thriftserver
   *
   * @return The string context used to register this source to hadoop's metrics2 system.
   */
  String getMetricsContext();

  /**
   * Get the description of what this source exposes.
   */
  String getMetricsDescription();

  /**
   * Get the name of the context in JMX that this source will be exposed through.
   * This is in ObjectName format. With the default context being Hadoop -&gt; HBase
   */
  String getMetricsJmxContext();

  /**
   * Get the name of the metrics that are being exported by this source.
   * Eg. IPC, GC, WAL
   */
  String getMetricsName();

}
