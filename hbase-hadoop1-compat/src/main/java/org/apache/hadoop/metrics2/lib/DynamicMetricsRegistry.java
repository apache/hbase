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

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * An optional metrics registry class for creating and maintaining a
 * collection of MetricsMutables, making writing metrics source easier.
 * NOTE: this is a copy of org.apache.hadoop.metrics2.lib.MetricsRegistry with added one
 *       feature: metrics can be removed. When HADOOP-8313 is fixed, usages of this class
 *       should be substituted with org.apache.hadoop.metrics2.lib.MetricsRegistry.
 *       This implementation also provides handy methods for creating metrics dynamically.
 *       Another difference is that metricsMap & tagsMap implementation is substituted with
 *       concurrent map, as we allow dynamic metrics additions/removals.
 */
public class DynamicMetricsRegistry {

  private final Log LOG = LogFactory.getLog(this.getClass());

  /** key for the context tag */
  public static final String CONTEXT_KEY = "context";
  /** description for the context tag */
  public static final String CONTEXT_DESC = "Metrics context";

  private final ConcurrentMap<String, MetricMutable> metricsMap =
      new ConcurrentHashMap<String, MetricMutable>();
  private final ConcurrentMap<String, MetricsTag> tagsMap =
      new ConcurrentHashMap<String, MetricsTag>();
  private final String name;
  private final MetricMutableFactory mf;

  /**
   * Construct the registry with a record name
   * @param name  of the record of the metrics
   */
  public DynamicMetricsRegistry(String name) {
    this.name = name;
    this.mf = new MetricMutableFactory();
  }

  /**
   * Construct the registry with a name and a metric factory
   * @param name  of the record of the metrics
   * @param factory for creating new mutable metrics
   */
  public DynamicMetricsRegistry(String name, MetricMutableFactory factory) {
    this.name = name;
    this.mf = factory;
  }

  /**
   * @return  the name of the metrics registry
   */
  public String name() {
    return name;
  }

  /**
   * Get a metric by name
   * @param name  of the metric
   * @return  the metric object
   */
  public MetricMutable get(String name) {
    return metricsMap.get(name);
  }

  /**
   * Create a mutable integer counter
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new counter object
   */
  public MetricMutableCounterInt
  newCounter(String name, String description, int initValue) {
    MetricMutableCounterInt ret = mf.newCounter(name, description, initValue);
    return addNewMetricIfAbsent(name, ret, MetricMutableCounterInt.class);
  }

  /**
   * Create a mutable long integer counter
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new counter object
   */
  public MetricMutableCounterLong
  newCounter(String name, String description, long initValue) {
    MetricMutableCounterLong ret = mf.newCounter(name, description, initValue);
    return addNewMetricIfAbsent(name, ret, MetricMutableCounterLong.class);
  }

  /**
   * Create a mutable integer gauge
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new gauge object
   */
  public MetricMutableGaugeInt
  newGauge(String name, String description, int initValue) {
    MetricMutableGaugeInt ret = mf.newGauge(name, description, initValue);
    return addNewMetricIfAbsent(name, ret, MetricMutableGaugeInt.class);
  }

  /**
   * Create a mutable long integer gauge
   * @param name  of the metric
   * @param description of the metric
   * @param initValue of the metric
   * @return  a new gauge object
   */
  public MetricMutableGaugeLong
  newGauge(String name, String description, long initValue) {
    MetricMutableGaugeLong ret = mf.newGauge(name, description, initValue);
    return addNewMetricIfAbsent(name, ret, MetricMutableGaugeLong.class);
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g., "ops")
   * @param valueName   of the metric (e.g., "time" or "latency")
   * @param extended    produce extended stat (stdev, min/max etc.) if true.
   * @return  a new metric object
   */
  public MetricMutableStat newStat(String name, String description,
                                   String sampleName, String valueName,
                                   boolean extended) {
    MetricMutableStat ret =
        mf.newStat(name, description, sampleName, valueName, extended);
    return addNewMetricIfAbsent(name, ret, MetricMutableStat.class);
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g., "ops")
   * @param valueName   of the metric (e.g., "time" or "latency")
   * @return  a new metric object
   */
  public MetricMutableStat newStat(String name, String description,
                                   String sampleName, String valueName) {
    return newStat(name, description, sampleName, valueName, false);
  }

  /**
   * Create a mutable metric with stats using the name only
   * @param name  of the metric
   * @return a new metric object
   */
  public MetricMutableStat newStat(String name) {
    return newStat(name, "", "ops", "time", false);
  }

  /**
   * Create a new histogram.
   * @param name Name of the histogram.
   * @return A new MutableHistogram
   */
  public MetricMutableHistogram newHistogram(String name) {
    return newHistogram(name, "");
  }

  /**
   * Create a new histogram.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableHistogram
   */
  public MetricMutableHistogram newHistogram(String name, String desc) {
    MetricMutableHistogram histo = new MetricMutableHistogram(name, desc);
    return addNewMetricIfAbsent(name, histo, MetricMutableHistogram.class);
  }

  /**
   * Create a new histogram with time range counts.
   * @param name Name of the histogram.
   * @return A new MetricMutableTimeHistogram
   */
  public MetricMutableTimeHistogram newTimeHistogram(String name) {
     return newTimeHistogram(name, "");
  }

  /**
   * Create a new histogram with time range counts.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MetricMutableTimeHistogram
   */
  public MetricMutableTimeHistogram newTimeHistogram(String name, String desc) {
    MetricMutableTimeHistogram histo = new MetricMutableTimeHistogram(name, desc);
    return addNewMetricIfAbsent(name, histo, MetricMutableTimeHistogram.class);
  }
  
  /**
   * Create a new histogram with size range counts.
   * @param name Name of the histogram.
   * @return A new MetricMutableSizeHistogram
   */
  public MetricMutableSizeHistogram newSizeHistogram(String name) {
     return newSizeHistogram(name, "");
  }

  /**
   * Create a new histogram with size range counts.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MetricMutableSizeHistogram
   */
  public MetricMutableSizeHistogram newSizeHistogram(String name, String desc) {
    MetricMutableSizeHistogram histo = new MetricMutableSizeHistogram(name, desc);
    return addNewMetricIfAbsent(name, histo, MetricMutableSizeHistogram.class);
  }

  /**
   * Create a new MutableQuantile(A more accurate histogram).
   * @param name The name of the histogram
   * @return a new MutableQuantile
   */
  public MetricMutableQuantiles newQuantile(String name) {
    return newQuantile(name, "");
  }

  /**
   * Create a new MutableQuantile(A more accurate histogram).
   * @param name The name of the histogram
   * @param desc Description of the data.
   * @return a new MutableQuantile
   */
  public MetricMutableQuantiles newQuantile(String name, String desc) {
    MetricMutableQuantiles histo = new MetricMutableQuantiles(name, desc);
    return addNewMetricIfAbsent(name, histo, MetricMutableQuantiles.class);
  }

  /**
   * Set the metrics context tag
   * @param name of the context
   * @return the registry itself as a convenience
   */
  public DynamicMetricsRegistry setContext(String name) {
    return tag(CONTEXT_KEY, CONTEXT_DESC, name);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return  the registry (for keep adding tags)
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
   * @return  the registry (for keep adding tags)
   */
  public DynamicMetricsRegistry tag(String name, String description, String value,
                             boolean override) {
    MetricsTag tag = new MetricsTag(name, description, value);

    if (!override) {
      MetricsTag existing = tagsMap.putIfAbsent(name, tag);
      if (existing != null) {
        throw new MetricsException("Tag "+ name +" already exists!");
      }
      return this;
    }

    tagsMap.put(name, tag);

    return this;
  }

  /**
   * Get the tags
   * @return  the tags set
   */
  public Set<Entry<String, MetricsTag>> tags() {
    return tagsMap.entrySet();
  }

  /**
   * Get the metrics
   * @return  the metrics set
   */
  public Set<Entry<String, MetricMutable>> metrics() {
    return metricsMap.entrySet();
  }

  /**
   * Sample all the mutable metrics and put the snapshot in the builder
   * @param builder to contain the metrics snapshot
   * @param all get all the metrics even if the values are not changed.
   */
  public void snapshot(MetricsRecordBuilder builder, boolean all) {

    for (Entry<String, MetricsTag> entry : tags()) {
      builder.add(entry.getValue());
    }
    for (Entry<String, MetricMutable> entry : metrics()) {
      entry.getValue().snapshot(builder, all);
    }
  }

  /**
   * Removes metric by name
   * @param name name of the metric to remove
   */
  public void removeMetric(String name) {
    metricsMap.remove(name);
  }

  /**
   * Get a MetricMutableGaugeLong from the storage.  If it is not there
   * atomically put it.
   *
   * @param gaugeName              name of the gauge to create or get.
   * @param potentialStartingValue value of the new counter if we have to create it.
   * @return a metric object
   */
  public MetricMutableGaugeLong getLongGauge(String gaugeName,
                                             long potentialStartingValue) {
    //Try and get the guage.
    MetricMutable metric = metricsMap.get(gaugeName);

    //If it's not there then try and put a new one in the storage.
    if (metric == null) {

      //Create the potential new gauge.
      MetricMutableGaugeLong newGauge = mf.newGauge(gaugeName, "",
              potentialStartingValue);

        // Try and put the gauge in.  This is atomic.
      metric = metricsMap.putIfAbsent(gaugeName, newGauge);

      //If the value we get back is null then the put was successful and we will
      // return that. Otherwise gaugeLong should contain the thing that was in
      // before the put could be completed.
      if (metric == null) {
        return newGauge;
      }
    }

    if (!(metric instanceof MetricMutableGaugeLong)) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
              name + " and not of type MetricMutableGaugeLong");
    }

    return (MetricMutableGaugeLong) metric;
  }

  /**
   * Get a MetricMutableCounterLong from the storage.  If it is not there
   * atomically put it.
   *
   * @param counterName            Name of the counter to get
   * @param potentialStartingValue starting value if we have to create a new counter
   * @return a metric object
   */
  public MetricMutableCounterLong getLongCounter(String counterName,
                                                 long potentialStartingValue) {
    //See getLongGauge for description on how this works.
    MetricMutable counter = metricsMap.get(counterName);
    if (counter == null) {
      MetricMutableCounterLong newCounter =
              mf.newCounter(counterName, "", potentialStartingValue);
      counter = metricsMap.putIfAbsent(counterName, newCounter);
      if (counter == null) {
        return newCounter;
      }
    }

    if (!(counter instanceof MetricMutableCounterLong)) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
              name + "and not of type MetricMutableCounterLong");
    }

    return (MetricMutableCounterLong) counter;
  }

  public MetricMutableHistogram getHistogram(String histoName) {
    //See getLongGauge for description on how this works.
    MetricMutable histo = metricsMap.get(histoName);
    if (histo == null) {
      MetricMutableHistogram newHisto =
          new MetricMutableHistogram(histoName, "");
      histo = metricsMap.putIfAbsent(histoName, newHisto);
      if (histo == null) {
        return newHisto;
      }
    }

    if (!(histo instanceof MetricMutableHistogram)) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
          name + "and not of type MetricMutableHistogram");
    }

    return (MetricMutableHistogram) histo;
  }

  public MetricMutableQuantiles getQuantile(String histoName) {
    //See getLongGauge for description on how this works.
    MetricMutable histo = metricsMap.get(histoName);
    if (histo == null) {
      MetricMutableQuantiles newHisto =
          new MetricMutableQuantiles(histoName, "");
      histo = metricsMap.putIfAbsent(histoName, newHisto);
      if (histo == null) {
        return newHisto;
      }
    }

    if (!(histo instanceof MetricMutableQuantiles)) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
          name + "and not of type MetricMutableQuantiles");
    }

    return (MetricMutableQuantiles) histo;
  }

  private<T extends MetricMutable> T
  addNewMetricIfAbsent(String name,
                       T ret,
                       Class<T> metricClass) {
    //If the value we get back is null then the put was successful and we will
    // return that. Otherwise metric should contain the thing that was in
    // before the put could be completed.
    MetricMutable metric = metricsMap.putIfAbsent(name, ret);
    if (metric == null) {
      return ret;
    }

    return returnExistingWithCast(metric, metricClass, name);
  }

  private<T> T returnExistingWithCast(MetricMutable metric,
                                      Class<T> metricClass, String name) {
    if (!metricClass.isAssignableFrom(metric.getClass())) {
      throw new MetricsException("Metric already exists in registry for metric name: " +
              name + " and not of type " + metricClass);
    }

    return (T) metric;
  }

  public void clearMetrics() {
    metricsMap.clear();
  }
}
