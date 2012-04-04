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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

/**
 * A snapshot of all the information seen in a Sample.
 */
public class Snapshot {

  private static final double MEDIAN_Q = 0.5;
  private static final double P75_Q = 0.75;
  private static final double P95_Q = 0.95;
  private static final double P98_Q = 0.98;
  private static final double P99_Q = 0.99;
  private static final double P999_Q = 0.999;

  private final double[] values;

  /**
   * Create a new {@link Snapshot} with the given values.
   *
   * @param values    an unordered set of values in the sample
   */
  public Snapshot(Collection<Long> values) {
    final Object[] copy = values.toArray();
    this.values = new double[copy.length];
    for (int i = 0; i < copy.length; i++) {
      this.values[i] = (Long) copy[i];
    }
    Arrays.sort(this.values);
  }

  /**
   * Create a new {@link Snapshot} with the given values.
   *
   * @param values    an unordered set of values in the sample
   */
  public Snapshot(double[] values) {
    this.values = new double[values.length];
    System.arraycopy(values, 0, this.values, 0, values.length);
    Arrays.sort(this.values);
  }

  /**
   * Returns the value at the given quantile.
   *
   * @param quantile    a given quantile, in [0..1]
   * @return the value in the distribution at quantile
   */
  public double getValue(double quantile) {
    if (quantile < 0.0 || quantile > 1.0) {
      throw new IllegalArgumentException(quantile + " is not in [0..1]");
    }

    if (values.length == 0) {
      return 0.0;
    }

    final double pos = quantile * (values.length + 1);

    if (pos < 1) {
      return values[0];
    }

    if (pos >= values.length) {
      return values[values.length - 1];
    }

    final double lower = values[(int) pos - 1];
    final double upper = values[(int) pos];
    return lower + (pos - Math.floor(pos)) * (upper - lower);
  }

  /**
   * Returns the number of values in the snapshot.
   *
   * @return the number of values in the snapshot
   */
  public int size() {
    return values.length;
  }

  /**
   * Returns the median value in the distribution.
   *
   * @return the median value in the distribution
   */
  public double getMedian() {
    return getValue(MEDIAN_Q);
  }

  /**
   * Returns the value at the 75th percentile in the distribution.
   *
   * @return the value at the 75th percentile in the distribution
   */
  public double get75thPercentile() {
    return getValue(P75_Q);
  }

  /**
   * Returns the value at the 95th percentile in the distribution.
   *
   * @return the value at the 95th percentile in the distribution
   */
  public double get95thPercentile() {
    return getValue(P95_Q);
  }

  /**
   * Returns the value at the 98th percentile in the distribution.
   *
   * @return the value at the 98th percentile in the distribution
   */
  public double get98thPercentile() {
    return getValue(P98_Q);
  }

  /**
   * Returns the value at the 99th percentile in the distribution.
   *
   * @return the value at the 99th percentile in the distribution
   */
  public double get99thPercentile() {
    return getValue(P99_Q);
  }

  /**
   * Returns the value at the 99.9th percentile in the distribution.
   *
   * @return the value at the 99.9th percentile in the distribution
   */
  public double get999thPercentile() {
    return getValue(P999_Q);
  }

  /**
   * Returns the entire set of values in the snapshot.
   *
   * @return the entire set of values in the snapshot
   */
  public double[] getValues() {
    return Arrays.copyOf(values, values.length);
  }
}
