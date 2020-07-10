/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.metrics.impl;

import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Custom histogram implementation based on FastLongHistogram. Dropwizard-based histograms are
 * slow compared to this implementation, so we are using our implementation here.
 * See HBASE-15222.
 */
@InterfaceAudience.Private
public class HistogramImpl implements Histogram {
  // Double buffer the two FastLongHistograms.
  // As they are reset they learn how the buckets should be spaced
  // So keep two around and use them
  protected final FastLongHistogram histogram;
  private final CounterImpl counter;

  public HistogramImpl() {
    this((long) Integer.MAX_VALUE << 2);
  }

  public HistogramImpl(long maxExpected) {
    this(FastLongHistogram.DEFAULT_NBINS, 1, maxExpected);
  }

  public HistogramImpl(int numBins, long min, long maxExpected) {
    this.counter = new CounterImpl();
    this.histogram = new FastLongHistogram(numBins, min, maxExpected);
  }

  protected HistogramImpl(CounterImpl counter, FastLongHistogram histogram) {
    this.counter = counter;
    this.histogram = histogram;
  }

  @Override
  public void update(int value) {
    counter.increment();
    histogram.add(value, 1);
  }

  @Override
  public void update(long value) {
    counter.increment();
    histogram.add(value, 1);
  }

  @Override
  public long getCount() {
    return counter.getCount();
  }

  public long getMax() {
    return this.histogram.getMax();
  }

  public long getMin() {
    return this.histogram.getMin();
  }

  @Override
  public Snapshot snapshot() {
    return histogram.snapshotAndReset();
  }

  public long[] getQuantiles(double[] quantiles) {
    return histogram.getQuantiles(quantiles);
  }
}
