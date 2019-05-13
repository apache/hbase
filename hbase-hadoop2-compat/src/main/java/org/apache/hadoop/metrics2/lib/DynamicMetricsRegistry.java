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

package org.apache.hadoop.metrics2.lib;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * An optional metrics registry class for creating and maintaining a
 * collection of MetricsMutables, making writing metrics source easier.
 * NOTE: this is a copy of org.apache.hadoop.metrics2.lib.MetricsRegistry with added one
 *       feature: metrics can be removed. When HADOOP-8313 is fixed, usages of this class
 *       should be substituted with org.apache.hadoop.metrics2.lib.MetricsRegistry.
 *       This implementation also provides handy methods for creating metrics
 *       dynamically.
 *       Another difference is that metricsMap implementation is substituted with
 *       thread-safe map, as we allow dynamic metrics additions/removals.
 */
@InterfaceAudience.Private
public class DynamicMetricsRegistry extends MetricsFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicMetricsRegistry.class);

  private final ConcurrentMap<String, MutableMetric> metricsMap =
      Maps.newConcurrentMap();
  private final ConcurrentMap<String, ConcurrentMap<String, MutableMetric>> scopedMetricsMap =
      Maps.newConcurrentMap();
  private final ConcurrentMap<String, MetricsTag> tagsMap =
      Maps.newConcurrentMap();
  private final MetricsInfo metricsInfo;
  private final DefaultMetricsSystemHelper helper = new DefaultMetricsSystemHelper();
  private final static String[] histogramSuffixes = new String[]{
    "_num_ops",
    "_min",
    "_max",
    "_median",
    "_75th_percentile",
    "_90th_percentile",
    "_95th_percentile",
    "_99th_percentile"};

  /**
   * Construct the registry with a record name
   * @param name  of the record of the metrics
   */
  public DynamicMetricsRegistry(String name) {
    this(Interns.info(name,name));
  }

  /**
   * Construct the registry with a metadata object
   * @param info  the info object for the metrics record/group
   */
  public DynamicMetricsRegistry(MetricsInfo info) {
    metricsInfo = info;
  }

  /**
   * @return the info object of the metrics registry
   */
  public MetricsInfo info() {
    return metricsInfo;
  }

  /**
   * Get a metric by name
   * @param name  of the metric
   * @return the metric object
   */
  public MutableMetric get(String name) {
    return metricsMap.get(name);
  }

  /**
   * Get a tag by name
   * @param name  of the tag
   * @return the tag object
   */
  public MetricsTag getTag(String name) {
    return tagsMap.get(name);
  }

  /**
   * Create a mutable long integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  @Override
  public MutableFastCounter newCounter(String name, String desc, long iVal) {
    return newCounter(new MetricsInfoImpl(name, desc), iVal);
  }

  public MutableFastCounter newScopedCounter(String scope, String name, String desc, long iVal) {
    MutableFastCounter ret = super.newCounter(new MetricsInfoImpl(name, desc), iVal);
    return addNewScopedMetricIfAbsent(scope, name, ret, MutableFastCounter.class);
  }

  public MutableHistogram newScopedHistogram(String scope, String name, String desc) {
    MutableHistogram ret = super.newHistogram(name, desc);
    return addNewScopedMetricIfAbsent(scope, name, ret, MutableHistogram.class);
  }

  public MutableTimeHistogram newScopedTimeHistogram(String scope, String name, String desc) {
    MutableTimeHistogram ret = super.newTimeHistogram(name, desc);
    return addNewScopedMetricIfAbsent(scope, name, ret, MutableTimeHistogram.class);
  }

  public MutableSizeHistogram newScopedSizeHistogram(String scope, String name, String desc) {
    MutableSizeHistogram ret = super.newSizeHistogram(name, desc);
    return addNewScopedMetricIfAbsent(scope, name, ret, MutableSizeHistogram.class);
  }

  public void removeScopedMetric(String scope, String name) {
    // Note: don't call helper.removeObjectName on scoped metrics, they all have the same name.
    ConcurrentMap<String, MutableMetric> map = scopedMetricsMap.getOrDefault(scope, null);
    if (map == null) {
      LOG.warn("Cannot remove metric {} - scope {} not found", name, scope);
      return;
    }
    map.remove(name);
    if (map.isEmpty()) {
      scopedMetricsMap.computeIfPresent(scope, (k, v) -> v.isEmpty() ? null : v);
    }
  }

  public void removeScopedHistogramMetrics(String scope, String baseName) {
    // Note: don't call helper.removeObjectName on scoped metrics, they all have the same name.
    ConcurrentMap<String, MutableMetric> map = scopedMetricsMap.getOrDefault(scope, null);
    if (map == null) {
      LOG.warn("Cannot remove metric {} - scope {} not found", baseName, scope);
      return;
    }
    for (String suffix : histogramSuffixes) {
      map.remove(baseName + suffix);
    }
    if (map.isEmpty()) {
      scopedMetricsMap.computeIfPresent(scope, (k, v) -> v.isEmpty() ? null : v);
    }
  }

  /**
   * Create a mutable long integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public MutableFastCounter newCounter(MetricsInfo info, long iVal) {
    MutableFastCounter ret = super.newCounter(info, iVal);
    return addNewMetricIfAbsent(info.name(), ret, MutableFastCounter.class);
  }

  /**
   * Create a mutable long integer gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public MutableGaugeLong newGauge(String name, String desc, long iVal) {
    return newGauge(new MetricsInfoImpl(name, desc), iVal);
  }

  /**
   * Create a mutable long integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public MutableGaugeLong newGauge(MetricsInfo info, long iVal) {
    MutableGaugeLong ret = super.newGauge(info, iVal);
    return addNewMetricIfAbsent(info.name(), ret, MutableGaugeLong.class);
  }

  /**
   * Create a new histogram.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableHistogram
   */
  public MutableHistogram newHistogram(String name, String desc) {
    MutableHistogram histo = super.newHistogram(name, desc);
    return addNewMetricIfAbsent(name, histo, MutableHistogram.class);
  }
  
  /**
   * Create a new histogram with time range counts.
   * @param name Name of the histogram.
   * @return A new MutableTimeHistogram
   */
  public MutableTimeHistogram newTimeHistogram(String name) {
    return newTimeHistogram(name, "");
  }

  /**
   * Create a new histogram with time range counts.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableTimeHistogram
   */
  public MutableTimeHistogram newTimeHistogram(String name, String desc) {
    MutableTimeHistogram histo = super.newTimeHistogram(name, desc);
    return addNewMetricIfAbsent(name, histo, MutableTimeHistogram.class);
  }

  /**
   * Create a new histogram with size range counts.
   * @param name Name of the histogram.
   * @return A new MutableSizeHistogram
   */
  public MutableSizeHistogram newSizeHistogram(String name) {
    return newSizeHistogram(name, "");
  }

  /**
   * Create a new histogram with size range counts.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableSizeHistogram
   */
  public MutableSizeHistogram newSizeHistogram(String name, String desc) {
    MutableSizeHistogram histo = super.newSizeHistogram(name, desc);
    return addNewMetricIfAbsent(name, histo, MutableSizeHistogram.class);
  }

  /**
   * Set the metrics context tag
   * @param name of the context
   * @return the registry itself as a convenience
   */
  public DynamicMetricsRegistry setContext(String name) {
    return tag(MsInfo.Context, name, true);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return the registry (for keep adding tags)
   */
  public DynamicMetricsRegistry tag(String name, String description, String value) {
    return tag(name, description, value, false);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @param override  existing tag if true
   * @return the registry (for keep adding tags)
   */
  public DynamicMetricsRegistry tag(String name, String description, String value,
                             boolean override) {
    return tag(new MetricsInfoImpl(name, description), value, override);
  }

  /**
   * Add a tag to the metrics
   * @param info  metadata of the tag
   * @param value of the tag
   * @param override existing tag if true
   * @return the registry (for keep adding tags etc.)
   */
  public DynamicMetricsRegistry tag(MetricsInfo info, String value, boolean override) {
    MetricsTag tag = Interns.tag(info, value);

    if (!override) {
      MetricsTag existing = tagsMap.putIfAbsent(info.name(), tag);
      if (existing != null) {
        throw new MetricsException("Tag "+ info.name() +" already exists!");
      }
      return this;
    }

    tagsMap.put(info.name(), tag);

    return this;
  }

  public DynamicMetricsRegistry tag(MetricsInfo info, String value) {
    return tag(info, value, false);
  }

  Collection<MetricsTag> tags() {
    return tagsMap.values();
  }

  Collection<MutableMetric> metrics() {
    return metricsMap.values();
  }

  /**
   * Sample all the mutable metrics and put the snapshot in the builder
   * @param builder to contain the metrics snapshot
   * @param all get all the metrics even if the values are not changed.
   */
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    for (MetricsTag tag : tags()) {
      builder.add(tag);
    }
    for (MutableMetric metric : metrics()) {
      metric.snapshot(builder, all);
    }
  }

  /**
   * Sample all the scoped metrics for a given scope and put the snapshot in the builder.
   * @param scope scope to snapshot
   * @param builder to contain the metrics snapshot
   * @param all get all the metrics even if the values are not changed.
   */
  public void snapshotScoped(String scope, MetricsRecordBuilder builder, boolean all) {
    for (MetricsTag tag : tags()) {
      builder.add(tag);
    }
    ConcurrentMap<String, MutableMetric> map = scopedMetricsMap.getOrDefault(scope, null);
    if (map == null) {
      return;
    }
    for (MutableMetric metric : map.values()) {
      metric.snapshot(builder, all);
    }
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", metricsInfo).add("tags", tags()).add("metrics", metrics())
        .toString();
  }

  /**
   * Removes metric by name
   * @param name name of the metric to remove
   */
  public void removeMetric(String name) {
    helper.removeObjectName(name);
    metricsMap.remove(name);
  }

  public void removeHistogramMetrics(String baseName) {
    for (String suffix:histogramSuffixes) {
      removeMetric(baseName+suffix);
    }
  }

  /**
   * Get a MetricMutableGaugeLong from the storage.  If it is not there atomically put it.
   *
   * @param gaugeName              name of the gauge to create or get.
   * @param potentialStartingValue value of the new gauge if we have to create it.
   */
  public MutableGaugeLong getGauge(String gaugeName, long potentialStartingValue) {
    //Try and get the guage.
    MutableMetric metric = metricsMap.get(gaugeName);

    //If it's not there then try and put a new one in the storage.
    if (metric == null) {

      //Create the potential new gauge.
      MutableGaugeLong newGauge = new MutableGaugeLong(new MetricsInfoImpl(gaugeName, ""),
              potentialStartingValue);

      // Try and put the gauge in.  This is atomic.
      metric = metricsMap.putIfAbsent(gaugeName, newGauge);

      //If the value we get back is null then the put was successful and we will return that.
      //otherwise gaugeLong should contain the thing that was in before the put could be completed.
      if (metric == null) {
        return newGauge;
      }
    }

    if (!(metric instanceof MutableGaugeLong)) {
      throw new MetricsException("Metric already exists in registry for metric name: " + gaugeName +
              " and not of type MetricMutableGaugeLong");
    }

    return (MutableGaugeLong) metric;
  }

  /**
   * Get a MetricMutableCounterLong from the storage.  If it is not there atomically put it.
   *
   * @param counterName            Name of the counter to get
   * @param potentialStartingValue starting value if we have to create a new counter
   */
  public MutableFastCounter getCounter(String counterName, long potentialStartingValue) {
    //See getGauge for description on how this works.
    MutableMetric counter = metricsMap.get(counterName);
    if (counter == null) {
      MutableFastCounter newCounter =
              new MutableFastCounter(new MetricsInfoImpl(counterName, ""), potentialStartingValue);
      counter = metricsMap.putIfAbsent(counterName, newCounter);
      if (counter == null) {
        return newCounter;
      }
    }


    if (!(counter instanceof MutableCounter)) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
              counterName + " and not of type MutableCounter");
    }

    return (MutableFastCounter) counter;
  }

  public MutableHistogram getHistogram(String histoName) {
    //See getGauge for description on how this works.
    MutableMetric histo = metricsMap.get(histoName);
    if (histo == null) {
      MutableHistogram newCounter =
          new MutableHistogram(new MetricsInfoImpl(histoName, ""));
      histo = metricsMap.putIfAbsent(histoName, newCounter);
      if (histo == null) {
        return newCounter;
      }
    }


    if (!(histo instanceof MutableHistogram)) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
          histoName + " and not of type MutableHistogram");
    }

    return (MutableHistogram) histo;
  }

  private<T extends MutableMetric> T addNewMetricIfAbsent(String name, T ret,
      Class<T> metricClass) {
    return addNewMetricToMapIfAbsent(metricsMap, name, ret, metricClass);
  }

  private static <T extends MutableMetric> T addNewMetricToMapIfAbsent(
      ConcurrentMap<String, MutableMetric> map, String name, T ret, Class<T> metricClass) {
    //If the value we get back is null then the put was successful and we will
    // return that. Otherwise metric should contain the thing that was in
    // before the put could be completed.
    MutableMetric metric = map.putIfAbsent(name, ret);
    if (metric == null) {
      return ret;
    }

    return returnExistingWithCast(metric, metricClass, name);
  }


  private<T extends MutableMetric> T addNewScopedMetricIfAbsent(
      String scope, String name, T ret, Class<T> metricClass) {
    ConcurrentMap<String, MutableMetric> map = scopedMetricsMap.computeIfAbsent(
        scope, k -> Maps.newConcurrentMap());
    return addNewMetricToMapIfAbsent(map, name, ret, metricClass);
  }

  @SuppressWarnings("unchecked")
  private static <T> T returnExistingWithCast(MutableMetric metric,
                                      Class<T> metricClass, String name) {
    if (!metricClass.isAssignableFrom(metric.getClass())) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
              name + " and not of type " + metricClass +
              " but instead of type " + metric.getClass());
    }

    return (T) metric;
  }

  public void clearMetrics() {
    for (String name:metricsMap.keySet()) {
      helper.removeObjectName(name);
    }
    metricsMap.clear();
  }
}
