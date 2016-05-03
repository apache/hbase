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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

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
public class DynamicMetricsRegistry {
  private static final Log LOG = LogFactory.getLog(DynamicMetricsRegistry.class);

  private final ConcurrentMap<String, MutableMetric> metricsMap =
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
  public MutableFastCounter newCounter(String name, String desc, long iVal) {
    return newCounter(new MetricsInfoImpl(name, desc), iVal);
  }

  /**
   * Create a mutable long integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public MutableFastCounter newCounter(MetricsInfo info, long iVal) {
    MutableFastCounter ret = new MutableFastCounter(info, iVal);
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
    MutableGaugeLong ret = new MutableGaugeLong(info, iVal);
    return addNewMetricIfAbsent(info.name(), ret, MutableGaugeLong.class);
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @param extended    produce extended stat (stdev, min/max etc.) if true.
   * @return a new mutable stat metric object
   */
  public MutableStat newStat(String name, String desc,
      String sampleName, String valueName, boolean extended) {
    MutableStat ret =
        new MutableStat(name, desc, sampleName, valueName, extended);
    return addNewMetricIfAbsent(name, ret, MutableStat.class);
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @return a new mutable metric object
   */
  public MutableStat newStat(String name, String desc,
                             String sampleName, String valueName) {
    return newStat(name, desc, sampleName, valueName, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @return a new mutable metric object
   */
  public MutableRate newRate(String name) {
    return newRate(name, name, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @param description of the metric
   * @return a new mutable rate metric object
   */
  public MutableRate newRate(String name, String description) {
    return newRate(name, description, false);
  }

  /**
   * Create a mutable rate metric (for throughput measurement)
   * @param name  of the metric
   * @param desc  description
   * @param extended  produce extended stat (stdev/min/max etc.) if true
   * @return a new mutable rate metric object
   */
  public MutableRate newRate(String name, String desc, boolean extended) {
    return newRate(name, desc, extended, true);
  }

  @InterfaceAudience.Private
  public MutableRate newRate(String name, String desc,
      boolean extended, boolean returnExisting) {
    if (returnExisting) {
      MutableMetric rate = metricsMap.get(name);
      if (rate != null) {
        if (rate instanceof MutableRate) return (MutableRate) rate;
        throw new MetricsException("Unexpected metrics type "+ rate.getClass()
                                   +" for "+ name);
      }
    }
    MutableRate ret = new MutableRate(name, desc, extended);
    return addNewMetricIfAbsent(name, ret, MutableRate.class);
  }

  /**
   * Create a new histogram.
   * @param name Name of the histogram.
   * @return A new MutableHistogram
   */
  public MutableHistogram newHistogram(String name) {
     return newHistogram(name, "");
  }

  /**
   * Create a new histogram.
   * @param name The name of the histogram
   * @param desc The description of the data in the histogram.
   * @return A new MutableHistogram
   */
  public MutableHistogram newHistogram(String name, String desc) {
    MutableHistogram histo = new MutableHistogram(name, desc);
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
    MutableTimeHistogram histo = new MutableTimeHistogram(name, desc);
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
    MutableSizeHistogram histo = new MutableSizeHistogram(name, desc);
    return addNewMetricIfAbsent(name, histo, MutableSizeHistogram.class);
  }


  synchronized void add(String name, MutableMetric metric) {
    addNewMetricIfAbsent(name, metric, MutableMetric.class);
  }

  /**
   * Add sample to a stat metric by name.
   * @param name  of the metric
   * @param value of the snapshot to add
   */
  public void add(String name, long value) {
    MutableMetric m = metricsMap.get(name);

    if (m != null) {
      if (m instanceof MutableStat) {
        ((MutableStat) m).add(value);
      }
      else {
        throw new MetricsException("Unsupported add(value) for metric "+ name);
      }
    }
    else {
      metricsMap.put(name, newRate(name)); // default is a rate metric
      add(name, value);
    }
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

  @Override public String toString() {
    return Objects.toStringHelper(this)
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

  private<T extends MutableMetric> T
  addNewMetricIfAbsent(String name,
                       T ret,
                       Class<T> metricClass) {
    //If the value we get back is null then the put was successful and we will
    // return that. Otherwise metric should contain the thing that was in
    // before the put could be completed.
    MutableMetric metric = metricsMap.putIfAbsent(name, ret);
    if (metric == null) {
      return ret;
    }

    return returnExistingWithCast(metric, metricClass, name);
  }

  @SuppressWarnings("unchecked")
  private<T> T returnExistingWithCast(MutableMetric metric,
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
