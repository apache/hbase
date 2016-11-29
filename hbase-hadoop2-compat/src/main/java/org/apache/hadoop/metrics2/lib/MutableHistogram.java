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

import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.hbase.util.FastLongHistogram;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * A histogram implementation that runs in constant space, and exports to hadoop2's metrics2 system.
 */
@InterfaceAudience.Private
public class MutableHistogram extends MutableMetric implements MetricHistogram {
  // Double buffer the two FastLongHistograms.
  // As they are reset they learn how the buckets should be spaced
  // So keep two around and use them
  protected final FastLongHistogram histogram;

  protected final String name;
  protected final String desc;
  protected final LongAdder counter = new LongAdder();

  private boolean metricsInfoStringInited = false;
  private String NUM_OPS_METRIC;
  private String MIN_METRIC;
  private String MAX_METRIC;
  private String MEAN_METRIC;
  private String MEDIAN_METRIC;
  private String TWENTY_FIFTH_PERCENTILE_METRIC;
  private String SEVENTY_FIFTH_PERCENTILE_METRIC;
  private String NINETIETH_PERCENTILE_METRIC;
  private String NINETY_FIFTH_PERCENTILE_METRIC;
  private String NINETY_EIGHTH_PERCENTILE_METRIC;
  private String NINETY_NINETH_PERCENTILE_METRIC;
  private String NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC;

  public MutableHistogram(MetricsInfo info) {
    this(info.name(), info.description());
  }

  public MutableHistogram(String name, String description) {
    this(name, description, Integer.MAX_VALUE << 2);
  }

  protected MutableHistogram(String name, String description, long maxExpected) {
    this.name = StringUtils.capitalize(name);
    this.desc = StringUtils.uncapitalize(description);
    this.histogram = new FastLongHistogram(FastLongHistogram.DEFAULT_NBINS, 1, maxExpected);
  }

  public void add(final long val) {
    counter.increment();
    histogram.add(val, 1);
  }

  public long getMax() {
    return histogram.getMax();
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder metricsRecordBuilder, boolean all) {
    // Get a reference to the old histogram.
    FastLongHistogram histo = histogram.reset();
    if (histo != null) {
      updateSnapshotMetrics(metricsRecordBuilder, histo);
    }
  }

  protected void updateSnapshotMetrics(MetricsRecordBuilder metricsRecordBuilder,
                                       FastLongHistogram histo) {
    if (!metricsInfoStringInited) {
      NUM_OPS_METRIC = name + NUM_OPS_METRIC_NAME;
      MIN_METRIC = name + MIN_METRIC_NAME;
      MAX_METRIC = name + MAX_METRIC_NAME;
      MEAN_METRIC = name + MEAN_METRIC_NAME;
      MEDIAN_METRIC = name + MEDIAN_METRIC_NAME;
      TWENTY_FIFTH_PERCENTILE_METRIC = name + TWENTY_FIFTH_PERCENTILE_METRIC_NAME;
      SEVENTY_FIFTH_PERCENTILE_METRIC = name + SEVENTY_FIFTH_PERCENTILE_METRIC_NAME;
      NINETIETH_PERCENTILE_METRIC = name + NINETIETH_PERCENTILE_METRIC_NAME;
      NINETY_FIFTH_PERCENTILE_METRIC = name + NINETY_FIFTH_PERCENTILE_METRIC_NAME;
      NINETY_EIGHTH_PERCENTILE_METRIC = name + NINETY_EIGHTH_PERCENTILE_METRIC_NAME;
      NINETY_NINETH_PERCENTILE_METRIC = name + NINETY_NINETH_PERCENTILE_METRIC_NAME;
      NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC = name +
          NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC_NAME;

      metricsInfoStringInited = true;
    }

    metricsRecordBuilder.addCounter(Interns.info(NUM_OPS_METRIC, desc), counter.sum());
    metricsRecordBuilder.addGauge(Interns.info(MIN_METRIC, desc), histo.getMin());
    metricsRecordBuilder.addGauge(Interns.info(MAX_METRIC, desc), histo.getMax());
    metricsRecordBuilder.addGauge(Interns.info(MEAN_METRIC, desc), histo.getMean());

    long[] percentiles = histo.getQuantiles();

    metricsRecordBuilder.addGauge(Interns.info(TWENTY_FIFTH_PERCENTILE_METRIC, desc),
        percentiles[0]);
    metricsRecordBuilder.addGauge(Interns.info(MEDIAN_METRIC, desc),
        percentiles[1]);
    metricsRecordBuilder.addGauge(Interns.info(SEVENTY_FIFTH_PERCENTILE_METRIC, desc),
        percentiles[2]);
    metricsRecordBuilder.addGauge(Interns.info(NINETIETH_PERCENTILE_METRIC, desc),
        percentiles[3]);
    metricsRecordBuilder.addGauge(Interns.info(NINETY_FIFTH_PERCENTILE_METRIC, desc),
        percentiles[4]);
    metricsRecordBuilder.addGauge(Interns.info(NINETY_EIGHTH_PERCENTILE_METRIC, desc),
        percentiles[5]);
    metricsRecordBuilder.addGauge(Interns.info(NINETY_NINETH_PERCENTILE_METRIC, desc),
        percentiles[6]);
    metricsRecordBuilder.addGauge(
        Interns.info(NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC, desc),
        percentiles[7]);
  }
}
