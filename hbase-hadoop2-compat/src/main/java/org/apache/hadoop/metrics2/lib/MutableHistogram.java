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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.HistogramImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A histogram implementation that runs in constant space, and exports to hadoop2's metrics2 system.
 */
@InterfaceAudience.Private
public class MutableHistogram extends MutableMetric implements MetricHistogram {
  protected HistogramImpl histogram;
  protected final String name;
  protected final String desc;

  public MutableHistogram(MetricsInfo info) {
    this(info.name(), info.description());
  }

  public MutableHistogram(String name, String description) {
    this.name = StringUtils.capitalize(name);
    this.desc = StringUtils.uncapitalize(description);
    this.histogram = new HistogramImpl();
  }

  public void add(final long val) {
    histogram.update(val);
  }

  @Override public long getCount() {
    return histogram.getCount();
  }

  public long getMax() {
    return histogram.getMax();
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder metricsRecordBuilder, boolean all) {
    snapshot(name, desc, histogram, metricsRecordBuilder, all);
  }

  public static void snapshot(String name, String desc, Histogram histogram,
                              MetricsRecordBuilder metricsRecordBuilder, boolean all) {
    // Get a reference to the old histogram.
    Snapshot snapshot = histogram.snapshot();
    if (snapshot != null) {
      updateSnapshotMetrics(name, desc, histogram, snapshot, metricsRecordBuilder);
    }
  }

  protected static void updateSnapshotMetrics(String name, String desc, Histogram histogram,
      Snapshot snapshot, MetricsRecordBuilder metricsRecordBuilder) {
    metricsRecordBuilder.addCounter(Interns.info(name + NUM_OPS_METRIC_NAME, desc),
        histogram.getCount());
    metricsRecordBuilder.addGauge(Interns.info(name + MIN_METRIC_NAME, desc), snapshot.getMin());
    metricsRecordBuilder.addGauge(Interns.info(name + MAX_METRIC_NAME, desc), snapshot.getMax());
    metricsRecordBuilder.addGauge(Interns.info(name + MEAN_METRIC_NAME, desc), snapshot.getMean());

    metricsRecordBuilder.addGauge(Interns.info(name + TWENTY_FIFTH_PERCENTILE_METRIC_NAME, desc),
        snapshot.get25thPercentile());
    metricsRecordBuilder.addGauge(Interns.info(name + MEDIAN_METRIC_NAME, desc),
        snapshot.getMedian());
    metricsRecordBuilder.addGauge(Interns.info(name + SEVENTY_FIFTH_PERCENTILE_METRIC_NAME, desc),
        snapshot.get75thPercentile());
    metricsRecordBuilder.addGauge(Interns.info(name + NINETIETH_PERCENTILE_METRIC_NAME, desc),
        snapshot.get90thPercentile());
    metricsRecordBuilder.addGauge(Interns.info(name + NINETY_FIFTH_PERCENTILE_METRIC_NAME, desc),
        snapshot.get95thPercentile());
    metricsRecordBuilder.addGauge(Interns.info(name + NINETY_EIGHTH_PERCENTILE_METRIC_NAME, desc),
        snapshot.get98thPercentile());
    metricsRecordBuilder.addGauge(Interns.info(name + NINETY_NINETH_PERCENTILE_METRIC_NAME, desc),
        snapshot.get99thPercentile());
    metricsRecordBuilder.addGauge(
        Interns.info(name + NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC_NAME, desc),
        snapshot.get999thPercentile());
  }
}
