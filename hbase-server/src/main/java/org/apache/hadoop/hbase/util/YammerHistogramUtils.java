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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;

import java.lang.reflect.Constructor;
import java.text.DecimalFormat;

/** Utility functions for working with Yammer Metrics. */
public final class YammerHistogramUtils {

  // not for public consumption
  private YammerHistogramUtils() {}

  /**
   * Used formatting doubles so only two places after decimal point.
   */
  private static DecimalFormat DOUBLE_FORMAT = new DecimalFormat("#0.00");

  /**
   * Create a new {@link com.codahale.metrics.Histogram} instance. These constructors are
   * not public in 2.2.0, so we use reflection to find them.
   */
  public static Histogram newHistogram(Reservoir sample) {
    try {
      Constructor<?> ctor =
          Histogram.class.getDeclaredConstructor(Reservoir.class);
      ctor.setAccessible(true);
      return (Histogram) ctor.newInstance(sample);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** @return an abbreviated summary of {@code hist}. */
  public static String getShortHistogramReport(final Histogram hist) {
    Snapshot sn = hist.getSnapshot();
    return "mean=" + DOUBLE_FORMAT.format(sn.getMean()) +
        ", min=" + DOUBLE_FORMAT.format(sn.getMin()) +
        ", max=" + DOUBLE_FORMAT.format(sn.getMax()) +
        ", stdDev=" + DOUBLE_FORMAT.format(sn.getStdDev()) +
        ", 95th=" + DOUBLE_FORMAT.format(sn.get95thPercentile()) +
        ", 99th=" + DOUBLE_FORMAT.format(sn.get99thPercentile());
  }

  /** @return a summary of {@code hist}. */
  public static String getHistogramReport(final Histogram hist) {
    Snapshot sn = hist.getSnapshot();
    return "mean=" + DOUBLE_FORMAT.format(sn.getMean()) +
        ", min=" + DOUBLE_FORMAT.format(sn.getMin()) +
        ", max=" + DOUBLE_FORMAT.format(sn.getMax()) +
        ", stdDev=" + DOUBLE_FORMAT.format(sn.getStdDev()) +
        ", 50th=" + DOUBLE_FORMAT.format(sn.getMedian()) +
        ", 75th=" + DOUBLE_FORMAT.format(sn.get75thPercentile()) +
        ", 95th=" + DOUBLE_FORMAT.format(sn.get95thPercentile()) +
        ", 99th=" + DOUBLE_FORMAT.format(sn.get99thPercentile()) +
        ", 99.9th=" + DOUBLE_FORMAT.format(sn.get999thPercentile()) +
        ", 99.99th=" + DOUBLE_FORMAT.format(sn.getValue(0.9999)) +
        ", 99.999th=" + DOUBLE_FORMAT.format(sn.getValue(0.99999));
  }

  /** @return pretty summary of {@code hist}. */
  public static String getPrettyHistogramReport(final Histogram h) {
    Snapshot sn = h.getSnapshot();
    return
        "Mean      = " + DOUBLE_FORMAT.format(sn.getMean()) + "\n" +
        "Min       = " + DOUBLE_FORMAT.format(sn.getMin()) + "\n" +
        "Max       = " + DOUBLE_FORMAT.format(sn.getMax()) + "\n" +
        "StdDev    = " + DOUBLE_FORMAT.format(sn.getStdDev()) + "\n" +
        "50th      = " + DOUBLE_FORMAT.format(sn.getMedian()) + "\n" +
        "75th      = " + DOUBLE_FORMAT.format(sn.get75thPercentile()) + "\n" +
        "95th      = " + DOUBLE_FORMAT.format(sn.get95thPercentile()) + "\n" +
        "99th      = " + DOUBLE_FORMAT.format(sn.get99thPercentile()) + "\n" +
        "99.9th    = " + DOUBLE_FORMAT.format(sn.get999thPercentile()) + "\n" +
        "99.99th   = " + DOUBLE_FORMAT.format(sn.getValue(0.9999)) + "\n" +
        "99.999th  = " + DOUBLE_FORMAT.format(sn.getValue(0.99999));
  }
}
