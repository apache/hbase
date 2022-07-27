/*
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
package org.apache.hadoop.hbase.metrics.impl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcases for FastLongHistogram.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestFastLongHistogram {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFastLongHistogram.class);

  private static void doTestUniform(FastLongHistogram hist) {
    long[] VALUES = { 0, 10, 20, 30, 40, 50 };
    double[] qs = new double[VALUES.length];
    for (int i = 0; i < qs.length; i++) {
      qs[i] = (double) VALUES[i] / VALUES[VALUES.length - 1];
    }

    for (int i = 0; i < 10; i++) {
      for (long v : VALUES) {
        hist.add(v, 1);
      }
      long[] vals = hist.getQuantiles(qs);
      System.out.println(Arrays.toString(vals));
      for (int j = 0; j < qs.length; j++) {
        Assert.assertTrue(j + "-th element org: " + VALUES[j] + ", act: " + vals[j],
          Math.abs(vals[j] - VALUES[j]) <= 10);
      }
      hist.snapshotAndReset();
    }
  }

  @Test
  public void testUniform() {
    FastLongHistogram hist = new FastLongHistogram(100, 0, 50);
    doTestUniform(hist);
  }

  @Test
  public void testAdaptionOfChange() {
    // assumes the uniform distribution
    FastLongHistogram hist = new FastLongHistogram(100, 0, 100);

    Random rand = ThreadLocalRandom.current();

    for (int n = 0; n < 10; n++) {
      for (int i = 0; i < 900; i++) {
        hist.add(rand.nextInt(100), 1);
      }

      // add 10% outliers, this breaks the assumption, hope bin10xMax works
      for (int i = 0; i < 100; i++) {
        hist.add(1000 + rand.nextInt(100), 1);
      }

      long[] vals = hist.getQuantiles(new double[] { 0.25, 0.75, 0.95 });
      System.out.println(Arrays.toString(vals));
      if (n == 0) {
        Assert.assertTrue("Out of possible value", vals[0] >= 0 && vals[0] <= 50);
        Assert.assertTrue("Out of possible value", vals[1] >= 50 && vals[1] <= 100);
        Assert.assertTrue("Out of possible value", vals[2] >= 900 && vals[2] <= 1100);
      }

      hist.snapshotAndReset();
    }
  }

  @Test
  public void testGetNumAtOrBelow() {
    long[] VALUES = { 1, 10, 20, 30, 40, 50 };

    FastLongHistogram h = new FastLongHistogram();
    for (long v : VALUES) {
      for (int i = 0; i < 100; i++) {
        h.add(v, 1);
      }
    }

    h.add(Integer.MAX_VALUE, 1);

    h.snapshotAndReset();

    for (long v : VALUES) {
      for (int i = 0; i < 100; i++) {
        h.add(v, 1);
      }
    }
    // Add something way out there to make sure it doesn't throw off the counts.
    h.add(Integer.MAX_VALUE, 1);

    assertEquals(100, h.getNumAtOrBelow(1));
    assertEquals(200, h.getNumAtOrBelow(11));
    assertEquals(601, h.getNumAtOrBelow(Long.MAX_VALUE));
  }

  @Test
  public void testSameValues() {
    FastLongHistogram hist = new FastLongHistogram(100);

    hist.add(50, 100);

    hist.snapshotAndReset();
    doTestUniform(hist);
  }
}
