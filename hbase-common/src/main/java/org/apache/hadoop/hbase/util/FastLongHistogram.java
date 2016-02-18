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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * FastLongHistogram is a thread-safe class that estimate distribution of data and computes the
 * quantiles.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FastLongHistogram {

  /**
   * Default number of bins.
   */
  public static final int DEFAULT_NBINS = 255;

  public static final double[] DEFAULT_QUANTILES =
      new double[]{0.25, 0.5, 0.75, 0.90, 0.95, 0.98, 0.99, 0.999};

  /**
   * Bins is a class containing a list of buckets(or bins) for estimation histogram of some data.
   */
  private static class Bins {
    private final Counter[] counts;
    // inclusive
    private final long binsMin;
    // exclusive
    private final long binsMax;
    private final long bins10XMax;
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(0L);

    private final Counter count = new Counter(0);
    private final Counter total = new Counter(0);

    // set to true when any of data has been inserted to the Bins. It is set after the counts are
    // updated.
    private final AtomicBoolean hasData = new AtomicBoolean(false);
    
    /**
     * The constructor for creating a Bins without any prior data.
     */
    public Bins(int numBins) {
      counts = createCounters(numBins + 3);
      this.binsMin = 1L;

      // These two numbers are total guesses
      // and should be treated as highly suspect.
      this.binsMax = 1000;
      this.bins10XMax = binsMax * 10;
    }

    /**
     * The constructor for creating a Bins with last Bins.
     */
    public Bins(Bins last, int numOfBins, double minQ, double maxQ) {
      long[] values = last.getQuantiles(new double[] { minQ, maxQ });
      long wd = values[1] - values[0] + 1;
      // expand minQ and maxQ in two ends back assuming uniform distribution
      this.binsMin = Math.max(0L, (long) (values[0] - wd * minQ));
      long binsMax = (long) (values[1] + wd * (1 - maxQ)) + 1;
      // make sure each of bins is at least of width 1
      this.binsMax = Math.max(binsMax, this.binsMin + numOfBins);
      this.bins10XMax = Math.max((long) (values[1] + (binsMax - 1) * 9), this.binsMax + 1);

      this.counts = createCounters(numOfBins + 3);
    }

    private Counter[] createCounters(int num) {
      Counter[] counters = new Counter[num];
      for (int i = 0; i < num; i++) {
        counters[i] = new Counter();
      }
      return counters;
    }

    private int getIndex(long value) {
      if (value < this.binsMin) {
        return 0;
      } else if (value > this.bins10XMax) {
        return this.counts.length - 1;
      } else if (value >= this.binsMax) {
        return this.counts.length - 2;
      }
      // compute the position
      return 1 + (int) ((value - this.binsMin) * (this.counts.length - 3) /
          (this.binsMax - this.binsMin));

    }

    /**
     * Adds a value to the histogram.
     */
    public void add(long value, long count) {
      if (value < 0) {
        // The whole computation is completely thrown off if there are negative numbers
        //
        // Normally we would throw an IllegalArgumentException however this is the metrics
        // system and it should be completely safe at all times.
        // So silently throw it away.
        return;
      }
      AtomicUtils.updateMin(min, value);
      AtomicUtils.updateMax(max, value);

      this.count.add(count);
      this.total.add(value * count);

      int pos = getIndex(value);
      this.counts[pos].add(count);

      // hasData needs to be updated as last
      this.hasData.set(true);
    }
    
    /**
     * Computes the quantiles give the ratios.
     */
    public long[] getQuantiles(double[] quantiles) {
      if (!this.hasData.get()) {
        // No data yet.
        return new long[quantiles.length];
      }

      // Make a snapshot of lowerCounter, higherCounter and bins.counts to counts.
      // This is not synchronized, but since the counter are accumulating, the result is a good
      // estimation of a snapshot.
      long[] counts = new long[this.counts.length];
      long total = 0L;
      for (int i = 0; i < this.counts.length; i++) {
        counts[i] = this.counts[i].get();
        total += counts[i];
      }

      int rIndex = 0;
      double qCount = total * quantiles[0];
      long cum = 0L;
      
      long[] res = new long[quantiles.length];
      countsLoop: for (int i = 0; i < counts.length; i++) {
        // mn and mx define a value range
        long mn, mx;
        if (i == 0) {
          mn = this.min.get();
          mx = this.binsMin;
        } else if (i == counts.length - 1) {
          mn = this.bins10XMax;
          mx = this.max.get();
        } else if (i == counts.length - 2) {
          mn = this.binsMax;
          mx = this.bins10XMax;
        } else {
          mn = this.binsMin + (i - 1) * (this.binsMax - this.binsMin) / (this.counts.length - 3);
          mx = this.binsMin + i * (this.binsMax - this.binsMin) / (this.counts.length - 3);
        }

        if (mx < this.min.get()) {
          continue;
        }
        if (mn > this.max.get()) {
          break;
        }
        mn = Math.max(mn, this.min.get());
        mx = Math.min(mx, this.max.get());

        // lastCum/cum are the corresponding counts to mn/mx
        double lastCum = cum;
        cum += counts[i];

        // fill the results for qCount is within current range.
        while (qCount <= cum) {
          if (cum == lastCum) {
            res[rIndex] = mn;
          } else {
            res[rIndex] = (long) ((qCount - lastCum) * (mx - mn) / (cum - lastCum) + mn);
          }

          // move to next quantile
          rIndex++;
          if (rIndex >= quantiles.length) {
            break countsLoop;
          }
          qCount = total * quantiles[rIndex];
        }
      }
      // In case quantiles contains values >= 100%
      for (; rIndex < quantiles.length; rIndex++) {
        res[rIndex] = this.max.get();
      }

      return res;
    }


    long getNumAtOrBelow(long val) {
      final int targetIndex = getIndex(val);
      long totalToCurrentIndex = 0;
      for (int i = 0; i <= targetIndex; i++) {
        totalToCurrentIndex += this.counts[i].get();
      }
      return  totalToCurrentIndex;
    }
  }

  // The bins counting values. It is replaced with a new one in calling of reset().
  private volatile Bins bins;

  /**
   * Constructor.
   */
  public FastLongHistogram() {
    this(DEFAULT_NBINS);
  }

  /**
   * Constructor.
   * @param numOfBins the number of bins for the histogram. A larger value results in more precise
   *          results but with lower efficiency, and vice versus.
   */
  public FastLongHistogram(int numOfBins) {
    this.bins = new Bins(numOfBins);
  }

  /**
   * Constructor setting the bins assuming a uniform distribution within a range.
   * @param numOfBins the number of bins for the histogram. A larger value results in more precise
   *          results but with lower efficiency, and vice versus.
   * @param min lower bound of the region, inclusive.
   * @param max higher bound of the region, inclusive.
   */
  public FastLongHistogram(int numOfBins, long min, long max) {
    this(numOfBins);
    Bins bins = new Bins(numOfBins);
    bins.add(min, 1);
    bins.add(max, 1);
    this.bins = new Bins(bins, numOfBins, 0.01, 0.999);
  }

  private FastLongHistogram(Bins bins) {
    this.bins = bins;
  }

  /**
   * Adds a value to the histogram.
   */
  public void add(long value, long count) {
    this.bins.add(value, count);
  }

  /**
   * Computes the quantiles give the ratios.
   */
  public long[] getQuantiles(double[] quantiles) {
    return this.bins.getQuantiles(quantiles);
  }

  public long[] getQuantiles() {
    return this.bins.getQuantiles(DEFAULT_QUANTILES);
  }

  public long getMin() {
    return this.bins.min.get();
  }

  public long getMax() {
    return this.bins.max.get();
  }

  public long getCount() {
    return this.bins.count.get();
  }

  public long getMean() {
    Bins bins = this.bins;
    long count = bins.count.get();
    long total = bins.total.get();
    if (count == 0) {
      return 0;
    }
    return total / count;
  }

  public long getNumAtOrBelow(long value) {
    return this.bins.getNumAtOrBelow(value);
  }

  /**
   * Resets the histogram for new counting.
   */
  public FastLongHistogram reset() {
    if (this.bins.hasData.get()) {
      Bins oldBins = this.bins;
      this.bins = new Bins(this.bins, this.bins.counts.length - 3, 0.01, 0.99);
      return new FastLongHistogram(oldBins);
    }

    return null;
  }
}
