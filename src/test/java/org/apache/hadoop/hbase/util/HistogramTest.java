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

import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;
import org.junit.Test;

import junit.framework.TestCase;

public class HistogramTest extends TestCase{

  @Test
  public void testAboveMaxValue() {
    Double hi = 10000.0;
    Histogram hist = new Histogram(1000, 100.0, hi);
    for (int i=0; i<1000; i++) {
      Double tmp = hi + i * 1.0;
      hist.addValue(tmp);
    }
    Double prcntyl = hist.getPercentileEstimate(95.0);
    assertTrue(prcntyl >= (hi + 949) && prcntyl <= (hi + 950));
  }
  @Test
  public void testBelowMinValue() {
    Histogram hist = new Histogram(1000, 1000.0, 10000.0);
    for (int i=0; i<1000; i++) {
      Double tmp = i * 1.0;
      hist.addValue(tmp);
    }
    Double prcntyl = hist.getPercentileEstimate(95.0);
    assertTrue(prcntyl >= 949 && prcntyl <= 950);
  }

  @Test
  public void testHistogram() {
    Histogram hist = new Histogram(1000, 0.0, 10000.0);
    for (int i=1; i<10000; i++) {
      Double tmp = i * 1.0;
      hist.addValue(tmp);
    }
    Double prcntyl = hist.getPercentileEstimate(99.0);
    assertTrue(prcntyl >= 9890 && prcntyl <= 9910);

    prcntyl = hist.getPercentileEstimate(95.0);
    assertTrue(prcntyl >= 9490 && prcntyl <= 9510);
  }

  @Test
  public void testHistogramExtremeValues() {
    Histogram hist = new Histogram(100, 0.0, 1000.0);
    for (int i=1; i<1000; i++) {
      Double tmp = i - 0.1;
      hist.addValue(tmp);
    }
    hist.addValue(1000.1);
    hist.addValue(1000.2);
    Double prcntyl = hist.getPercentileEstimate(99.9999999999);
    assertTrue(prcntyl <= 1000.2);

    hist = new Histogram(100, 10.0, 100.0);
    for (int i=10; i<1100; i++) {
      Double tmp = i - 0.1;
      hist.addValue(tmp);
    }
    hist.addValue(5.1);
    hist.addValue(9.1);
    prcntyl = hist.getPercentileEstimate(0.0);
    assertTrue(prcntyl >= 5.1);
  }

  public void testFewDataPoints() {
    Histogram hist = new Histogram(100, 0.0, 100.0);
    for (int i=10; i>=1; i--) {
      hist.addValue((double)i);
    }
    Double prcntyl = hist.getPercentileEstimate(99.0);
    assertTrue(prcntyl >= 9 && prcntyl <= 10);
    prcntyl = hist.getPercentileEstimate(0.0);
    assertTrue(prcntyl >= 1 && prcntyl <= 2);
  }

  /**
   * This failure scenario was identified when a histogram has 100 entries and
   * the last bucket gets 1 elements, p99 gives a Double.MaxValue because
   * of a bug. This case fails before the fix.
   */
  public void testLargeP99ErrorCase() {
    Histogram hist = new Histogram(100, 0.0, 100.0);
    double d;
    for (d = 0.5; d <= 101; d += 1.0) {
      hist.addValue(d);
    }
    Double prcntyl = PercentileMetric.P99;
    Double actualValue = hist.getPercentileEstimate(prcntyl);
    assertTrue(actualValue <= d);
  }
}
