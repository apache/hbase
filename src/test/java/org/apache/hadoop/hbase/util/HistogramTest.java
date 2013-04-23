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

import org.junit.Test;

import junit.framework.TestCase;

public class HistogramTest extends TestCase{

  @Test
  public void testAboveMaxValue() {
    Double hi = 10000.0;
    Histogram hist = new Histogram(1000, 100.0, hi);
    for (int i=0; i<100; i++) {
      Double tmp = hi + i * 1.0;
      hist.addValue(tmp);
    }
    Double prcntyl = hist.getPercentileEstimate(95.0);
    assertTrue(prcntyl >= (hi + 94) && prcntyl <= (hi + 96));
  }
  @Test
  public void testBelowMinValue() {
    Histogram hist = new Histogram(1000, 100.0, 10000.0);
    for (int i=0; i<100; i++) {
      Double tmp = i * 1.0;
      hist.addValue(tmp);
    }
    Double prcntyl = hist.getPercentileEstimate(95.0);
    assertTrue(prcntyl >= 94 && prcntyl <= 96);
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
    Histogram hist = new Histogram(100, 0.0, 100.0);
    for (int i=1; i<100; i++) {
      Double tmp = i - 0.1;
      hist.addValue(tmp);
    }
    hist.addValue(100.1);
    hist.addValue(100.2);
    Double prcntyl = hist.getPercentileEstimate(99.9999999999);
    assertTrue(prcntyl <= 100.2);

    hist = new Histogram(100, 10.0, 100.0);
    for (int i=11; i<100; i++) {
      Double tmp = i - 0.1;
      hist.addValue(tmp);
    }
    hist.addValue(5.1);
    hist.addValue(9.1);
    prcntyl = hist.getPercentileEstimate(0.0);
    assertTrue(prcntyl >= 5.1);
  }
}
