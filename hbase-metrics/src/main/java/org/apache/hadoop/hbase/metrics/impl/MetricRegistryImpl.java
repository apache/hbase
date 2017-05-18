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
package org.apache.hadoop.hbase.metrics.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.MetricSet;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.hbase.util.CollectionUtils;

import com.google.common.base.Optional;

/**
 * Custom implementation of {@link MetricRegistry}.
 */
@InterfaceAudience.Private
public class MetricRegistryImpl implements MetricRegistry {

  private final MetricRegistryInfo info;

  private final ConcurrentMap<String, Metric> metrics;

  public MetricRegistryImpl(MetricRegistryInfo info) {
    this.info = info;
    this.metrics = new ConcurrentHashMap<>();
  }

  @Override
  public Timer timer(String name) {
    Timer metric = (Timer) metrics.get(name);
    if (metric != null) {
      return metric;
    }

    Timer newTimer = createTimer();
    metric = (Timer) metrics.putIfAbsent(name, newTimer);
    if (metric != null) {
      return metric;
    }

    return newTimer;
  }

  protected Timer createTimer() {
    return new TimerImpl();
  }

  @Override
  public Histogram histogram(String name) {
    Histogram metric = (Histogram) metrics.get(name);
    if (metric != null) {
      return metric;
    }

    Histogram newHistogram = createHistogram();
    metric = (Histogram) metrics.putIfAbsent(name, newHistogram);
    if (metric != null) {
      return metric;
    }

    return newHistogram;
  }

  protected Histogram createHistogram() {
    return new HistogramImpl();
  }

  @Override
  public Meter meter(String name) {
    Meter metric = (Meter) metrics.get(name);
    if (metric != null) {
      return metric;
    }

    Meter newmeter = createMeter();
    metric = (Meter) metrics.putIfAbsent(name, newmeter);
    if (metric != null) {
      return metric;
    }

    return newmeter;
  }

  protected Meter createMeter() {
    return new DropwizardMeter();
  }

  @Override
  public Counter counter(String name) {
    Counter metric = (Counter) metrics.get(name);
    if (metric != null) {
      return metric;
    }

    Counter newCounter = createCounter();
    metric = (Counter) metrics.putIfAbsent(name, newCounter);
    if (metric != null) {
      return metric;
    }

    return newCounter;
  }

  protected Counter createCounter() {
    return new CounterImpl();
  }

  @Override
  public Optional<Metric> get(String name) {

    return Optional.fromNullable(metrics.get(name));
  }

  @Override
  public Metric register(String name, Metric metric) {
    Metric m = metrics.get(name);
    if (m != null) {
      return m;
    }

    Metric oldMetric = metrics.putIfAbsent(name, metric);
    if (oldMetric != null) {
      return oldMetric;
    }

    return metric;
  }

  @Override
  public <T> Gauge<T> register(String name, Gauge<T> gauge) {
    return (Gauge) register(name, (Metric)gauge);
  }

  @Override
  public void registerAll(MetricSet metricSet) {
    Set<Entry<String,Metric>> entrySet = metricSet.getMetrics().entrySet();
    for (Entry<String, Metric> entry : entrySet) {
      register(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return metrics;
  }

  @Override
  public boolean remove(String name) {
    return metrics.remove(name) != null;
  }

  @Override
  public MetricRegistryInfo getMetricRegistryInfo() {
    return info;
  }
}