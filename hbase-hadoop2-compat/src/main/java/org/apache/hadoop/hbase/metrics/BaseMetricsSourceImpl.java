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

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * Hadoop 2 implementation of BaseMetricsSource (using metrics2 framework)
 */
public class BaseMetricsSourceImpl implements BaseMetricsSource, MetricsSource {

  private static boolean defaultMetricsSystemInited = false;
  public static final String HBASE_METRICS_SYSTEM_NAME = "hbase";

  final DynamicMetricsRegistry metricsRegistry;

  private JvmMetrics jvmMetricsSource;

  public BaseMetricsSourceImpl(String metricsName,
                               String metricsDescription,
                               String metricsContext) {
    metricsRegistry = new DynamicMetricsRegistry(metricsName).setContext(metricsContext);

    if (!defaultMetricsSystemInited) {
      //Not too worried about mutlithread here as all it does is spam the logs.
      defaultMetricsSystemInited = true;
      DefaultMetricsSystem.initialize(HBASE_METRICS_SYSTEM_NAME);
      jvmMetricsSource = JvmMetrics.create(metricsName, "", DefaultMetricsSystem.instance());
    }
    DefaultMetricsSystem.instance().register(metricsContext, metricsDescription, this);

  }

  /**
   * Set a single gauge to a value.
   *
   * @param gaugeName gauge name
   * @param value     the new value of the gauge.
   */
  public void setGauge(String gaugeName, long value) {
    MutableGaugeLong gaugeInt = getLongGauge(gaugeName, value);
    gaugeInt.set(value);
  }

  /**
   * Add some amount to a gauge.
   *
   * @param gaugeName The name of the gauge to increment.
   * @param delta     The amount to increment the gauge by.
   */
  public void incGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = getLongGauge(gaugeName, 0l);
    gaugeInt.incr(delta);
  }

  /**
   * Decrease the value of a named gauge.
   *
   * @param gaugeName The name of the gauge.
   * @param delta     the ammount to subtract from a gauge value.
   */
  public void decGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = getLongGauge(gaugeName, 0l);
    gaugeInt.decr(delta);
  }

  /**
   * Increment a named counter by some value.
   *
   * @param key   the name of the counter
   * @param delta the ammount to increment
   */
  public void incCounters(String key, long delta) {
    MutableCounterLong counter = getLongCounter(key, 0l);
    counter.incr(delta);

  }

  /**
   * Remove a named gauge.
   *
   * @param key
   */
  public void removeGauge(String key) {
    metricsRegistry.removeMetric(key);
  }

  /**
   * Remove a named counter.
   *
   * @param key
   */
  public void removeCounter(String key) {
    metricsRegistry.removeMetric(key);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    metricsRegistry.snapshot(metricsCollector.addRecord(metricsRegistry.info()), all);
  }

  /**
   * Get a MetricMutableGaugeLong from the storage.  If it is not there atomically put it.
   *
   * @param gaugeName              name of the gauge to create or get.
   * @param potentialStartingValue value of the new counter if we have to create it.
   * @return
   */
  protected MutableGaugeLong getLongGauge(String gaugeName, long potentialStartingValue) {
    return metricsRegistry.getLongGauge(gaugeName, potentialStartingValue);
  }

  /**
   * Get a MetricMutableCounterLong from the storage.  If it is not there atomically put it.
   *
   * @param counterName            Name of the counter to get
   * @param potentialStartingValue starting value if we have to create a new counter
   * @return
   */
  protected MutableCounterLong getLongCounter(String counterName, long potentialStartingValue) {
    return metricsRegistry.getLongCounter(counterName, potentialStartingValue);
  }
}
