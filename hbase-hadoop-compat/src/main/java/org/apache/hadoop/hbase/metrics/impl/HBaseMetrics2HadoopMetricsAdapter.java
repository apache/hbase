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
/*
 * Copyright 2016 Josh Elser
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the adapter from "HBase Metrics Framework", implemented in hbase-metrics-api and
 * hbase-metrics modules to the Hadoop Metrics2 framework. This adapter is not a metric source,
 * but a helper to be able to collect all of the Metric's in the MetricRegistry using the
 * MetricsCollector and MetricsRecordBuilder.
 *
 * Some of the code is forked from https://github.com/joshelser/dropwizard-hadoop-metrics2.
 */
@InterfaceAudience.Private
public class HBaseMetrics2HadoopMetricsAdapter {
  private static final Logger LOG
      = LoggerFactory.getLogger(HBaseMetrics2HadoopMetricsAdapter.class);
  private static final String EMPTY_STRING = "";

  public HBaseMetrics2HadoopMetricsAdapter() {
  }

  /**
   * Iterates over the MetricRegistry and adds them to the {@code collector}.
   *
   * @param collector A metrics collector
   */
  public void snapshotAllMetrics(MetricRegistry metricRegistry,
                                 MetricsCollector collector) {
    MetricRegistryInfo info = metricRegistry.getMetricRegistryInfo();
    MetricsRecordBuilder builder = collector.addRecord(Interns.info(info.getMetricsName(),
        info.getMetricsDescription()));
    builder.setContext(info.getMetricsContext());

    snapshotAllMetrics(metricRegistry, builder);
  }

  /**
   * Iterates over the MetricRegistry and adds them to the {@code builder}.
   *
   * @param builder A record builder
   */
  public void snapshotAllMetrics(MetricRegistry metricRegistry, MetricsRecordBuilder builder) {
    Map<String, Metric> metrics = metricRegistry.getMetrics();

    for (Map.Entry<String, Metric> e: metrics.entrySet()) {
      // Always capitalize the name
      String name = StringUtils.capitalize(e.getKey());
      Metric metric = e.getValue();

      if (metric instanceof Gauge) {
        addGauge(name, (Gauge<?>) metric, builder);
      } else if (metric instanceof Counter) {
        addCounter(name, (Counter)metric, builder);
      } else if (metric instanceof Histogram) {
        addHistogram(name, (Histogram)metric, builder);
      } else if (metric instanceof Meter) {
        addMeter(name, (Meter)metric, builder);
      } else if (metric instanceof Timer) {
        addTimer(name, (Timer)metric, builder);
      } else {
        LOG.info("Ignoring unknown Metric class " + metric.getClass().getName());
      }
    }
  }

  private void addGauge(String name, Gauge<?> gauge, MetricsRecordBuilder builder) {
    final MetricsInfo info = Interns.info(name, EMPTY_STRING);
    final Object o = gauge.getValue();

    // Figure out which gauge types metrics2 supports and call the right method
    if (o instanceof Integer) {
      builder.addGauge(info, (int) o);
    } else if (o instanceof Long) {
      builder.addGauge(info, (long) o);
    } else if (o instanceof Float) {
      builder.addGauge(info, (float) o);
    } else if (o instanceof Double) {
      builder.addGauge(info, (double) o);
    } else {
      LOG.warn("Ignoring Gauge (" + name + ") with unhandled type: " + o.getClass());
    }
  }

  private void addCounter(String name, Counter counter, MetricsRecordBuilder builder) {
    MetricsInfo info = Interns.info(name, EMPTY_STRING);
    builder.addCounter(info, counter.getCount());
  }

  /**
   * Add Histogram value-distribution data to a Hadoop-Metrics2 record building.
   *
   * @param name A base name for this record.
   * @param histogram A histogram to measure distribution of values.
   * @param builder A Hadoop-Metrics2 record builder.
   */
  private void addHistogram(String name, Histogram histogram, MetricsRecordBuilder builder) {
    MutableHistogram.snapshot(name, EMPTY_STRING, histogram, builder, true);
  }

  /**
   * Add Dropwizard-Metrics rate information to a Hadoop-Metrics2 record builder, converting the
   * rates to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param name A base name for this record.
   */
  private void addMeter(String name, Meter meter, MetricsRecordBuilder builder) {
    builder.addGauge(Interns.info(name + "_count", EMPTY_STRING), meter.getCount());
    builder.addGauge(Interns.info(name + "_mean_rate", EMPTY_STRING), meter.getMeanRate());
    builder.addGauge(Interns.info(name + "_1min_rate", EMPTY_STRING), meter.getOneMinuteRate());
    builder.addGauge(Interns.info(name + "_5min_rate", EMPTY_STRING), meter.getFiveMinuteRate());
    builder.addGauge(Interns.info(name + "_15min_rate", EMPTY_STRING),
        meter.getFifteenMinuteRate());
  }

  private void addTimer(String name, Timer timer, MetricsRecordBuilder builder) {
    addMeter(name, timer.getMeter(), builder);
    addHistogram(name, timer.getHistogram(), builder);
  }
}
