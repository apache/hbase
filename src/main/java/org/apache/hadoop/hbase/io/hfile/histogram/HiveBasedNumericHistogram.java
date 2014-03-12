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
package org.apache.hadoop.hbase.io.hfile.histogram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Adapted from NumericHistogram from hive
 * Reference : hive/trunk/ql/src/java/org/apache/hadoop/hive/ql/udf/
 *             generic/NumericHistogram.java
 *
 */

/**
 * A generic, re-usable histogram class that supports partial aggregations. The
 * algorithm is a heuristic adapted from the following paper: Yael Ben-Haim and
 * Elad Tom-Tov, "A streaming parallel decision tree algorithm", J. Machine
 * Learning Research 11 (2010), pp. 849--872. Although there are no
 * approximation guarantees, it appears to work well with adequate data and a
 * large (e.g., 20-80) number of histogram bins.
 */
public class HiveBasedNumericHistogram implements NumericHistogram {
  /**
   * The Coord class defines a histogram bin, which is just an (x,y) pair.
   */
  protected static class Coord implements Comparable<Coord> {
    double x;
    double y;
    Map<Enum<?>, Double> stats;

    @Override
    public int compareTo(Coord o) {
      if (x < o.x) {
        return -1;
      }
      if (x > o.x) {
        return 1;
      }
      return 0;
    }
  };

  // Class variables
  private int nbins;
  private int nusedbins;
  private ArrayList<Coord> bins;
  private Random prng;
  private final double minusInfinity;
  private final double infinity;

  /**
   * Creates a new histogram object. Note that the allocate() or merge() method
   * must be called before the histogram can be used.
   */
  public HiveBasedNumericHistogram(double minusInfinity, double infinity) {
    nbins = 0;
    nusedbins = 0;
    bins = null;
    this.minusInfinity = minusInfinity;
    this.infinity = infinity;

    // init the RNG for breaking ties in histogram merging. A fixed seed is
    // specified here
    // to aid testing, but can be eliminated to use a time-based seed (which
    // would
    // make the algorithm non-deterministic).
    prng = new Random(31183);
  }

  protected HiveBasedNumericHistogram(double minusInfinity2, double infinity2,
      List<Bucket> nbuckets) {
    this.minusInfinity = minusInfinity2;
    this.infinity = infinity2;

  }

  /**
   * Resets a histogram object to its initial state. allocate() or merge() must
   * be called again before use.
   */
  public void reset() {
    bins = null;
    nbins = nusedbins = 0;
  }

  /**
   * Returns the number of bins currently being used by the histogram.
   */
  public int getUsedBins() {
    return nusedbins;
  }

  /**
   * Returns true if this histogram object has been initialized by calling
   * merge() or allocate().
   */
  public boolean isReady() {
    return nbins != 0;
  }

  /**
   * Returns a particular histogram bin.
   */
  public Coord getBin(int b) {
    return bins.get(b);
  }

  /**
   * Sets the number of histogram bins to use for approximating data.
   *
   * @param num_bins
   *          Number of non-uniform-width histogram bins to use
   */
  @Override
  public void allocate(int numBins) {
    nbins = numBins;
    bins = new ArrayList<Coord>();
    nusedbins = 0;
  }

  /**
   * Takes a serialized histogram created by the serialize() method and merges
   * it with the current histogram object.
   *
   * @param other
   *          A serialized histogram created by the serialize() method
   * @see #merge
   */
  @Override
  public NumericHistogram merge(NumericHistogram hist) {
    Preconditions.checkNotNull(hist);
    Preconditions.checkArgument(hist instanceof HiveBasedNumericHistogram);
    HiveBasedNumericHistogram other = (HiveBasedNumericHistogram)hist;

    if (nbins == 0 || nusedbins == 0) {
      // Our aggregation buffer has nothing in it, so just copy over 'other'
      // by deserializing the ArrayList of (x,y) pairs into an array of Coord
      // objects
      nbins = other.nbins;
      nusedbins = other.nusedbins;
      bins = new ArrayList<Coord>(nusedbins);
      for (int i = 0; i < other.nusedbins; i++) {
        Coord bin = new Coord();
        bin.x = other.bins.get(i).x;
        bin.y = other.bins.get(i).y;
        bins.add(bin);
      }
    } else {
      // The aggregation buffer already contains a partial histogram. Therefore,
      // we need
      // to merge histograms using Algorithm #2 from the Ben-Haim and Tom-Tov
      // paper.

      ArrayList<Coord> tmp_bins = new ArrayList<Coord>(nusedbins
          + other.nusedbins);
      // Copy all the histogram bins from us and 'other' into an overstuffed
      // histogram
      for (int i = 0; i < nusedbins; i++) {
        Coord bin = new Coord();
        bin.x = bins.get(i).x;
        bin.y = bins.get(i).y;
        tmp_bins.add(bin);
      }
      for (int j = 0; j < other.nusedbins; j++) {
        Coord bin = new Coord();
        bin.x = other.bins.get(j).x;
        bin.y = other.bins.get(j).y;
        tmp_bins.add(bin);
      }
      Collections.sort(tmp_bins);

      // Now trim the overstuffed histogram down to the correct number of bins
      bins = tmp_bins;
      nusedbins += other.nusedbins;
      trim();
    }
    return this;
  }

  /**
   * Adds a new data point to the histogram approximation. Make sure you have
   * called either allocate() or merge() first. This method implements Algorithm
   * #1 from Ben-Haim and Tom-Tov,
   * "A Streaming Parallel Decision Tree Algorithm", JMLR 2010.
   *
   * @param v
   *          The data point to add to the histogram approximation.
   */
  @Override
  public void add(double v) {
    // Binary search to find the closest bucket that v should go into.
    // 'bin' should be interpreted as the bin to shift right in order to
    // accomodate
    // v. As a result, bin is in the range [0,N], where N means that the value v
    // is
    // greater than all the N bins currently in the histogram. It is also
    // possible that
    // a bucket centered at 'v' already exists, so this must be checked in the
    // next step.
    int bin = 0;
    for (int l = 0, r = nusedbins; l < r;) {
      bin = (l + r) / 2;
      if (bins.get(bin).x > v) {
        r = bin;
      } else {
        if (bins.get(bin).x < v) {
          l = ++bin;
        } else {
          break; // break loop on equal comparator
        }
      }
    }

    // If we found an exact bin match for value v, then just increment that
    // bin's count.
    // Otherwise, we need to insert a new bin and trim the resulting histogram
    // back to size.
    // A possible optimization here might be to set some threshold under which
    // 'v' is just
    // assumed to be equal to the closest bin -- if fabs(v-bins[bin].x) <
    // THRESHOLD, then
    // just increment 'bin'. This is not done now because we don't want to make
    // any
    // assumptions about the range of numeric data being analyzed.
    if (bin < nusedbins && bins.get(bin).x == v) {
      bins.get(bin).y++;
    } else {
      Coord newBin = new Coord();
      newBin.x = v;
      newBin.y = 1;
      bins.add(bin, newBin);

      // Trim the bins down to the correct number of bins.
      if (++nusedbins > nbins) {
        trim();
      }
    }

  }

  /**
   * Trims a histogram down to 'nbins' bins by iteratively merging the closest
   * bins. If two pairs of bins are equally close to each other, decide
   * uniformly at random which pair to merge, based on a PRNG.
   */
  private void trim() {
    while (nusedbins > nbins) {
      // Find the closest pair of bins in terms of x coordinates.
      // Break ties randomly.
      double smallestdiff = bins.get(1).x - bins.get(0).x;
      int smallestdiffloc = 0, smallestdiffcount = 1;
      for (int i = 1; i < nusedbins - 1; i++) {
        double diff = bins.get(i + 1).x - bins.get(i).x;
        if (diff < smallestdiff) {
          smallestdiff = diff;
          smallestdiffloc = i;
          smallestdiffcount = 1;
        } else {
          if (diff == smallestdiff
              && prng.nextDouble() <= (1.0 / ++smallestdiffcount)) {
            smallestdiffloc = i;
          }
        }
      }

      // Merge the two closest bins into their average x location, weighted by
      // their heights.
      // The height of the new bin is the sum of the heights of the old bins.
      // double d = bins[smallestdiffloc].y + bins[smallestdiffloc+1].y;
      // bins[smallestdiffloc].x *= bins[smallestdiffloc].y / d;
      // bins[smallestdiffloc].x += bins[smallestdiffloc+1].x / d *
      // bins[smallestdiffloc+1].y;
      // bins[smallestdiffloc].y = d;

      double d = bins.get(smallestdiffloc).y + bins.get(smallestdiffloc + 1).y;
      Coord smallestdiffbin = bins.get(smallestdiffloc);
      smallestdiffbin.x *= smallestdiffbin.y / d;
      smallestdiffbin.x += bins.get(smallestdiffloc + 1).x / d
          * bins.get(smallestdiffloc + 1).y;
      smallestdiffbin.y = d;
      // Shift the remaining bins left one position
      bins.remove(smallestdiffloc + 1);
      nusedbins--;
    }
  }

  /**
   * Gets an approximate quantile value from the current histogram. Some popular
   * quantiles are 0.5 (median), 0.95, and 0.98.
   *
   * @param q
   *          The requested quantile, must be strictly within the range (0,1).
   * @return The quantile value.
   */
  public double quantile(double q) {
    assert (bins != null && nusedbins > 0 && nbins > 0);
    double sum = 0, csum = 0;
    int b;
    for (b = 0; b < nusedbins; b++) {
      sum += bins.get(b).y;
    }
    for (b = 0; b < nusedbins; b++) {
      csum += bins.get(b).y;
      if (csum / sum >= q) {
        if (b == 0) {
          return bins.get(b).x;
        }

        csum -= bins.get(b).y;
        double r = bins.get(b - 1).x + (q * sum - csum)
            * (bins.get(b).x - bins.get(b - 1).x) / (bins.get(b).y);
        return r;
      }
    }
    return -1; // for Xlint, code will never reach here
  }

  public int getNumBins() {
    return bins == null ? 0 : bins.size();
  }

  /**
   * Gives the sum for points from [-infinity, pi]
   *
   * @param i
   * @return
   */
  public double sum(int i) {
    double sum = 0;
    for (int j = i - 1; j >= 0; j--) {
      sum += this.bins.get(j).y;
    }
    return sum + (bins.get(i).y / 2);
  }

  /**
   * Following the Algorithm 4 : Uniform Procedure in the paper. Returns a
   * HiveBasedNumericHistogram which has B - 1 points such that each range
   * [-infinity, p1], [p1, p2] .. [pB, infinity] have the same number of
   * elements.
   *
   * @return
   */
  public HiveBasedNumericHistogram uniform(int B) {
    double sum = 0.0;
    HiveBasedNumericHistogram hist = new HiveBasedNumericHistogram(
        this.minusInfinity, this.infinity);
    hist.allocate(this.nusedbins);
    for (int j = 0; j < this.nusedbins; j++) {
      sum += bins.get(j).y;
    }
    if (this.nusedbins == 0)
      return hist;
    double[] partialSums = new double[this.nusedbins];
    partialSums[0] = this.bins.get(0).y / 2;
    for (int j = 1; j < this.nusedbins; j++) {
      partialSums[j] = partialSums[j - 1]
          + (this.bins.get(j - 1).y + this.bins.get(j).y) / 2;
    }
    for (int j = 0; j < (B - 1); j++) {
      double s = sum * (j + 1) / B;
      int i = 0;
      double d = s - partialSums[this.nusedbins - 1];
      for (; i < (this.nusedbins - 1); i++) {
        if (s >= partialSums[i] && s < partialSums[i + 1]) {
          d = s - partialSums[i];
          break;
        }
      }
      double endVal = 0;
      double endKey = this.infinity;
      if (i < (this.nusedbins - 1)) {
        endVal = this.bins.get(i + 1).y;
        endKey = this.bins.get(i + 1).x;
      }
      double a = endVal - this.bins.get(i).y;
      a = (a == 0) ? 1 : a;
      double b = 2 * this.bins.get(i).y;
      double c = -2 * d;
      double det = b * b - 2 * a * c;
      double z = (-b + Math.sqrt(det)) / (2 * a);
      Coord newBin = new Coord();
      newBin.x = this.bins.get(i).x + (endKey - this.bins.get(i).x) * z;
      newBin.y = sum / this.nusedbins;
      hist.bins.add(newBin);
    }
    hist.nusedbins = hist.bins.size();
    hist.nbins = hist.nusedbins;
    return hist;
  }

  private List<Bucket> getBuckets(HiveBasedNumericHistogram hist) {
    List<Bucket> buckets = Lists.newArrayList();
    Preconditions.checkArgument(hist != null);
    Preconditions.checkArgument(hist.bins != null);
    if (hist.bins.isEmpty()) {
      return buckets;
    }
    buckets.add(new Bucket(this.minusInfinity, hist.bins.get(0).x, hist.bins
        .get(0).y / 2, hist.bins.get(0).stats));
    if (hist.bins.size() != 1) {
      for (int i = 1; i < hist.bins.size(); i++) {
        buckets.add(new Bucket(hist.bins.get(i - 1).x, hist.bins.get(i).x,
            (hist.bins.get(i - 1).y + hist.bins.get(i).y) / 2,
            hist.bins.get(i).stats));
      }
    }
    buckets.add(new Bucket(hist.bins.get(hist.bins.size() - 1).x,
        this.infinity, hist.bins.get(hist.bins.size() - 1).y / 2, hist.bins
            .get(hist.bins.size() - 1).stats));
    return buckets;
  }

  public static HiveBasedNumericHistogram getHistogram(List<Bucket> buckets) {
    if (buckets.size() <= 1)
      return null;
    double min = buckets.get(0).getStart();
    double max = buckets.get(buckets.size() - 1).getEnd();
    HiveBasedNumericHistogram ret = new HiveBasedNumericHistogram(min, max);
    ret.allocate(buckets.size() - 1);
    for (int i = 1; i < buckets.size(); i++) {
      Coord c = new Coord();
      Bucket b = buckets.get(i);
      Bucket prevB = buckets.get(i - 1);
      c.x = b.getStart();
      c.y = (prevB.getCount() + b.getCount()) / 2;
      ret.bins.add(c);
    }
    ret.nusedbins = ret.bins.size();
    return ret;
  }

  /**
   * Constructs a uniform histogram and returns the list of buckets.
   */
  @Override
  public List<Bucket> getUniformBuckets() {
    return getBuckets(uniform(this.getUsedBins()));
  }

  public List<Bucket> getOriginalBuckets() {
    return getBuckets(this);
  }

  @Override
  public int getBinCount() {
    return this.nbins;
  }
}
