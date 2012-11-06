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

import com.yammer.metrics.stats.ExponentiallyDecayingSample;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import java.util.concurrent.atomic.AtomicLong;

/**
 *  A histogram implementation that runs in constant space, and exports to hadoop's metrics2 system.
 */
public class MetricMutableHistogram extends MetricMutable implements MetricHistogram {

  private static final int DEFAULT_SAMPLE_SIZE = 2046;
  // the bias towards sampling from more recent data.
  // Per Cormode et al. an alpha of 0.015 strongly biases to the last 5 minutes
  private static final double DEFAULT_ALPHA = 0.015;

  private final Sample sample;
  private final AtomicLong min;
  private final AtomicLong max;
  private final AtomicLong sum;
  private final AtomicLong count;


  public MetricMutableHistogram(String name, String description) {
    super(name, description);
    sample = new ExponentiallyDecayingSample(DEFAULT_SAMPLE_SIZE, DEFAULT_ALPHA);
    count = new AtomicLong();
    min = new AtomicLong(Long.MAX_VALUE);
    max = new AtomicLong(Long.MIN_VALUE);
    sum = new AtomicLong();
  }

  public void add(final long val) {
    setChanged();
    count.incrementAndGet();
    sample.update(val);
    setMax(val);
    setMin(val);
    sum.getAndAdd(val);
  }

  private void setMax(final long potentialMax) {
    boolean done = false;
    while (!done) {
      final long currentMax = max.get();
      done = currentMax >= potentialMax
          || max.compareAndSet(currentMax, potentialMax);
    }
  }

  private void setMin(long potentialMin) {
    boolean done = false;
    while (!done) {
      final long currentMin = min.get();
      done = currentMin <= potentialMin
          || min.compareAndSet(currentMin, potentialMin);
    }
  }

  public long getMax() {
    if (count.get() > 0) {
      return max.get();
    }
    return 0L;
  }

  public long getMin() {
    if (count.get() > 0) {
      return min.get();
    }
    return 0L;
  }

  public double getMean() {
    long cCount = count.get();
    if (cCount > 0) {
      return sum.get() / (double) cCount;
    }
    return 0.0;
  }


  @Override
  public void snapshot(MetricsRecordBuilder metricsRecordBuilder, boolean all) {
    if (all || changed()) {
      clearChanged();
      final Snapshot s = sample.getSnapshot();
      metricsRecordBuilder.addCounter(name + NUM_OPS_METRIC_NAME, "", count.get());
      metricsRecordBuilder.addGauge(name + MIN_METRIC_NAME, "", getMin());
      metricsRecordBuilder.addGauge(name + MAX_METRIC_NAME,  "", getMax());
      metricsRecordBuilder.addGauge(name + MEAN_METRIC_NAME, "", getMean());

      metricsRecordBuilder.addGauge(name + MEDIAN_METRIC_NAME, "", s.getMedian());
      metricsRecordBuilder.addGauge(name + SEVENTY_FIFTH_PERCENTILE_METRIC_NAME, "", s.get75thPercentile());
      metricsRecordBuilder.addGauge(name + NINETY_FIFTH_PERCENTILE_METRIC_NAME, "", s.get95thPercentile());
      metricsRecordBuilder.addGauge(name + NINETY_NINETH_PERCENTILE_METRIC_NAME, "", s.get99thPercentile());
    }
  }
}
