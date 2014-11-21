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
package org.apache.hadoop.hbase.regionserver.metrics;
import org.apache.hadoop.hbase.util.Histogram;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.MetricsRecord;

/**
 * Used the org.apache.hadoop.hbase.util.Histogram to maintain time varying
 * metrics. The histogram class can provide various approximate details about
 * a stream of data supplied to the PercentileMetric without actually
 * storing the data.
 */
public class PercentileMetric extends MetricsLongValue {
  public static final int HISTOGRAM_NUM_BUCKETS_DEFAULT = 20;
  public static final double HISTOGRAM_MINVALUE_DEFAULT = 0.0;
  public static final double HISTOGRAM_MAXVALUE_DEFAULT = 1000000000.0;
  public static final double DEFAULT_PERCENTILE = 99.0;
  public static final long DEFAULT_SAMPLE_WINDOW = 60;
  public static final double P99 = 99.0;
  public static final double P95 = 95.0;
  public static final double P75 = 75.0;
  public static final double P50 = 50.0;

  private int numBuckets;
  private double percentile;
  private Histogram underlyingHistogram;

  /**
   * This constructor provides a way to create a HistogramMetric which uses a
   * Histogram to maintain the statistics of a metric stream.
   */
  public PercentileMetric(final String nam, final MetricsRegistry registry,
    Histogram histogram) {
    super(nam, registry);
    underlyingHistogram = histogram;
  }

  public PercentileMetric(String nam, MetricsRegistry registry,
      Histogram histogram, double percentile, int numBuckets) {
    super(nam, registry);
    this.underlyingHistogram = histogram;
    this.percentile = percentile;
    this.numBuckets = numBuckets;
  }

  /**
   * The histogram which has the values updated.
   */
  public void setHistogram(final Histogram hist) {
    this.underlyingHistogram = hist;
  }

  /**
   * numBuckets : This denotes the number of buckets used to sample the data.
   * the updateMetric and refresh calls will run in O(numBuckets).
   */
  public void setNumBuckets(final int numBuckets) {
    this.numBuckets = numBuckets;
  }

  /**
   * percentile : The percentile estimate of the metric that will be seeked
   * using this metric. The value should be between 0 and 100,
   * else it will throw and exception.
   */
  public void setPercentile(final double prcntyl) {
    this.percentile = prcntyl;
  }

  public double getValue() {
    return this.get();
  }

  public void updateMetric() {
    this.set((long)underlyingHistogram.getPercentileEstimate(percentile));
  }

  public void refresh() {
    underlyingHistogram.refresh(this.numBuckets);
  }

  /**
   * Add a value in the underlying histogram.
   * @param value The value to be added.
   */
  public void addValueInHistogram(long value) {
    underlyingHistogram.addValue(value);
  }

  /**
   * Push the metric value to the <code>MetricsRecord</code> object
   * @param mr
   */
  public void pushMetric(final MetricsRecord mr) {
    this.updateMetric();
    mr.setMetric(getName(), (long)getValue());
  }
}
