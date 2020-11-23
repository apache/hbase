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

import org.apache.hadoop.hbase.metrics.impl.GlobalMetricRegistriesAdapter;
import org.apache.hadoop.hbase.metrics.impl.HBaseMetrics2HadoopMetricsAdapter;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceImpl;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop 2 implementation of BaseSource (using metrics2 framework).  It handles registration to
 * DefaultMetricsSystem and creation of the metrics registry.
 *
 * All MetricsSource's in hbase-hadoop2-compat should derive from this class.
 */
@InterfaceAudience.Private
public class BaseSourceImpl implements BaseSource, MetricsSource {

  private static enum DefaultMetricsSystemInitializer {
    INSTANCE;
    private boolean inited = false;

    synchronized void init(String name) {
      if (inited) {
        return;
      }

      inited = true;
      DefaultMetricsSystem.initialize(HBASE_METRICS_SYSTEM_NAME);
      JvmMetrics.initSingleton(name, "");
      // initialize hbase-metrics module based metric system as well. GlobalMetricRegistriesSource
      // initialization depends on the metric system being already initialized, that is why we are
      // doing it here. Once BaseSourceSourceImpl is removed, we should do the initialization of
      // these elsewhere.
      GlobalMetricRegistriesAdapter.init();
    }
  }

  /**
   * @deprecated Use hbase-metrics/hbase-metrics-api module interfaces for new metrics.
   *             Defining BaseSources for new metric groups (WAL, RPC, etc) is not needed anymore,
   *             however, for existing {@link BaseSource} implementations, please use the field
   *             named "registry" which is a {@link MetricRegistry} instance together with the
   *             {@link HBaseMetrics2HadoopMetricsAdapter}.
   */
  @Deprecated
  protected final DynamicMetricsRegistry metricsRegistry;
  protected final String metricsName;
  protected final String metricsDescription;
  protected final String metricsContext;
  protected final String metricsJmxContext;

  /**
   * Note that there are at least 4 MetricRegistry definitions in the source code. The first one is
   * Hadoop Metrics2 MetricRegistry, second one is DynamicMetricsRegistry which is HBase's fork
   * of the Hadoop metrics2 class. The third one is the dropwizard metrics implementation of
   * MetricRegistry, and finally a new API abstraction in HBase that is the
   * o.a.h.h.metrics.MetricRegistry class. This last one is the new way to use metrics within the
   * HBase code. However, the others are in play because of existing metrics2 based code still
   * needs to coexists until we get rid of all of our BaseSource and convert them to the new
   * framework. Until that happens, new metrics can use the new API, but will be collected
   * through the HBaseMetrics2HadoopMetricsAdapter class.
   *
   * BaseSourceImpl has two MetricRegistries. metricRegistry is for hadoop Metrics2 based
   * metrics, while the registry is for hbase-metrics based metrics.
   */
  protected final MetricRegistry registry;

  /**
   * The adapter from hbase-metrics module to metrics2. This adepter is the connection between the
   * Metrics in the MetricRegistry and the Hadoop Metrics2 system. Using this adapter, existing
   * BaseSource implementations can define new metrics using the hbase-metrics/hbase-metrics-api
   * module interfaces and still be able to make use of metrics2 sinks (including JMX). Existing
   * BaseSources should call metricsAdapter.snapshotAllMetrics() in getMetrics() method. See
   * {@link MetricsRegionServerSourceImpl}.
   */
  protected final HBaseMetrics2HadoopMetricsAdapter metricsAdapter;

  public BaseSourceImpl(
      String metricsName,
      String metricsDescription,
      String metricsContext,
      String metricsJmxContext) {

    this.metricsName = metricsName;
    this.metricsDescription = metricsDescription;
    this.metricsContext = metricsContext;
    this.metricsJmxContext = metricsJmxContext;

    metricsRegistry = new DynamicMetricsRegistry(metricsName).setContext(metricsContext);
    DefaultMetricsSystemInitializer.INSTANCE.init(metricsName);

    //Register this instance.
    DefaultMetricsSystem.instance().register(metricsJmxContext, metricsDescription, this);

    // hbase-metrics module based metrics are registered in the hbase MetricsRegistry.
    registry = MetricRegistries.global().create(this.getMetricRegistryInfo());
    metricsAdapter = new HBaseMetrics2HadoopMetricsAdapter();

    init();

  }

  public void init() {
    this.metricsRegistry.clearMetrics();
  }

  /**
   * Set a single gauge to a value.
   *
   * @param gaugeName gauge name
   * @param value     the new value of the gauge.
   */
  public void setGauge(String gaugeName, long value) {
    MutableGaugeLong gaugeInt = metricsRegistry.getGauge(gaugeName, value);
    gaugeInt.set(value);
  }

  /**
   * Add some amount to a gauge.
   *
   * @param gaugeName The name of the gauge to increment.
   * @param delta     The amount to increment the gauge by.
   */
  public void incGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = metricsRegistry.getGauge(gaugeName, 0L);
    gaugeInt.incr(delta);
  }

  /**
   * Decrease the value of a named gauge.
   *
   * @param gaugeName The name of the gauge.
   * @param delta     the ammount to subtract from a gauge value.
   */
  public void decGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = metricsRegistry.getGauge(gaugeName, 0L);
    gaugeInt.decr(delta);
  }

  /**
   * Increment a named counter by some value.
   *
   * @param key   the name of the counter
   * @param delta the ammount to increment
   */
  public void incCounters(String key, long delta) {
    MutableFastCounter counter = metricsRegistry.getCounter(key, 0L);
    counter.incr(delta);

  }

  @Override
  public void updateHistogram(String name, long value) {
    MutableHistogram histo = metricsRegistry.getHistogram(name);
    histo.add(value);
  }

  /**
   * Remove a named gauge.
   *
   * @param key the key of the gauge to remove
   */
  public void removeMetric(String key) {
    metricsRegistry.removeMetric(key);
    JmxCacheBuster.clearJmxCache();
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    metricsRegistry.snapshot(metricsCollector.addRecord(metricsRegistry.info()), all);
  }

  public DynamicMetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }

  public String getMetricsContext() {
    return metricsContext;
  }

  public String getMetricsDescription() {
    return metricsDescription;
  }

  public String getMetricsJmxContext() {
    return metricsJmxContext;
  }

  public String getMetricsName() {
    return metricsName;
  }

}
