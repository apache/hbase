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

package org.apache.hadoop.hbase.metrics.histogram;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;

public class MetricsHistogram extends MetricsBase {
  
  // 1028 items implies 99.9% CI w/ 5% margin of error 
  // (assuming a normal distribution on the underlying data)
  private static final int DEFAULT_SAMPLE_SIZE = 1028;

  // the bias towards sampling from more recent data. 
  // Per Cormode et al. an alpha of 0.015 strongly biases to the last 5 minutes
  private static final double DEFAULT_ALPHA = 0.015;

  /**
   * Constructor to create a new histogram metric
   * @param nam           the name to publish the metric under
   * @param registry      where the metrics object will be registered
   * @param description   the metric's description
   * @param forwardBiased true if you want this histogram to give more 
   *                      weight to recent data, 
   *                      false if you want all data to have uniform weight
   */
  public MetricsHistogram(final String nam, final MetricsRegistry registry, 
      final String description, boolean forwardBiased) {
    super(nam, description);

    this.min = new AtomicLong();
    this.max = new AtomicLong();
    this.sum = new AtomicLong();
    this.sample = forwardBiased ? 
        new ExponentiallyDecayingSample(DEFAULT_SAMPLE_SIZE, DEFAULT_ALPHA) 
    : new UniformSample(DEFAULT_SAMPLE_SIZE);

    this.variance =  new AtomicReference<double[]>(new double[]{-1, 0});
    this.count = new AtomicLong();

    this.clear();

    if (registry != null) {
      registry.add(nam, this);      
    }
  }

  /**
   * Constructor create a new (forward biased) histogram metric
   * @param nam         the name to publish the metric under
   * @param registry    where the metrics object will be registered
   * @param description the metric's description
   */
  public MetricsHistogram(final String nam, MetricsRegistry registry, 
      final String description) {
    this(nam, registry, NO_DESCRIPTION, true);
  }
    
  /**
   * Constructor - create a new (forward biased) histogram metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   */
  public MetricsHistogram(final String nam, MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION);
  }

  private final Sample sample;
  private final AtomicLong min;
  private final AtomicLong max;
  private final AtomicLong sum;

  // these are for computing a running-variance, 
  // without letting floating point errors accumulate via Welford's algorithm
  private final AtomicReference<double[]> variance;
  private final AtomicLong count;

  /**
   * Clears all recorded values.
   */
  public void clear() {
    this.sample.clear();
    this.count.set(0);
    this.max.set(Long.MIN_VALUE);
    this.min.set(Long.MAX_VALUE);
    this.sum.set(0);
    variance.set(new double[]{-1, 0});
  }

  public void update(int val) {
    update((long) val);
  }

  public void update(final long val) {
    count.incrementAndGet();
    sample.update(val);
    setMax(val);
    setMin(val);
    sum.getAndAdd(val);
    updateVariance(val);
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

  private void updateVariance(long value) {
    boolean done = false;
    while (!done) {
      final double[] oldValues = variance.get();
      final double[] newValues = new double[2];
      if (oldValues[0] == -1) {
        newValues[0] = value;
        newValues[1] = 0;
      } else {
        final double oldM = oldValues[0];
        final double oldS = oldValues[1];

        final double newM = oldM + ((value - oldM) / getCount());
        final double newS = oldS + ((value - oldM) * (value - newM));

        newValues[0] = newM;
        newValues[1] = newS;
      }
      done = variance.compareAndSet(oldValues, newValues);
    }
  }


  public long getCount() {
    return count.get();
  }

  public long getMax() {
    if (getCount() > 0) {
      return max.get();
    }
    return 0L;
  }

  public long getMin() {
    if (getCount() > 0) {
      return min.get();
    }
    return 0L;
  }

  public double getMean() {
    if (getCount() > 0) {
      return sum.get() / (double) getCount();
    }
    return 0.0;
  }

  public double getStdDev() {
    if (getCount() > 0) {
      return Math.sqrt(getVariance());
    }
    return 0.0;
  }

  public Snapshot getSnapshot() {
    return sample.getSnapshot();
  }

  private double getVariance() {
    if (getCount() <= 1) {
      return 0.0;
    }
    return variance.get()[1] / (getCount() - 1);
  }

  @Override
  public void pushMetric(MetricsRecord mr) {
    final Snapshot s = this.getSnapshot();
    mr.setMetric(getName() + "_num_ops", this.getCount());
    mr.setMetric(getName() + "_min", this.getMin());
    mr.setMetric(getName() + "_max", this.getMax());
    
    mr.setMetric(getName() + "_mean", (float) this.getMean());
    mr.setMetric(getName() + "_std_dev", (float) this.getStdDev());
    
    mr.setMetric(getName() + "_median", (float) s.getMedian());
    mr.setMetric(getName() + "_75th_percentile", 
        (float) s.get75thPercentile());
    mr.setMetric(getName() + "_95th_percentile", 
        (float) s.get95thPercentile());
    mr.setMetric(getName() + "_99th_percentile", 
        (float) s.get99thPercentile());
  }
}
