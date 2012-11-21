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

package org.apache.hadoop.hbase.metrics;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;
import org.apache.hadoop.hbase.SmallTests;
import com.yammer.metrics.stats.Snapshot;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMetricsHistogram {

  @Test
  public void testBasicUniform() {
    MetricsHistogram h = new MetricsHistogram("testHistogram", null);

    for (int i = 0; i < 100; i++) {
      h.update(i);
    }

    Assert.assertEquals(100, h.getCount());
    Assert.assertEquals(0, h.getMin());
    Assert.assertEquals(99, h.getMax());
  }

  private static int safeIndex(int i, int len) {
    if (i < len && i>= 0) {
      return i;
    } else if (i >= len) {
      return len - 1; 
    } else {
      return 0;
    }    
  }
  
  @Test
  public void testRandom() {
    final Random r = new Random();
    final MetricsHistogram h = new MetricsHistogram("testHistogram", null);

    final long[] data = new long[1000];

    for (int i = 0; i < data.length; i++) {
      data[i] = (long) (r.nextGaussian() * 10000.0);
      h.update(data[i]);
    }

    final Snapshot s = h.getSnapshot();
    Arrays.sort(data);

    // as long as the histogram chooses an item with index N+/-slop, accept it
    final int slop = 20;

    // make sure the median, 75th percentile and 95th percentile are good
    final int medianIndex = data.length / 2;
    final long minAcceptableMedian = data[safeIndex(medianIndex - slop, 
        data.length)];
    final long maxAcceptableMedian = data[safeIndex(medianIndex + slop, 
        data.length)];
    Assert.assertTrue(s.getMedian() >= minAcceptableMedian 
        && s.getMedian() <= maxAcceptableMedian);

    final int seventyFifthIndex = (int) (data.length * 0.75);
    final long minAcceptableseventyFifth = data[safeIndex(seventyFifthIndex 
        - slop, data.length)];
    final long maxAcceptableseventyFifth = data[safeIndex(seventyFifthIndex 
        + slop, data.length)];
    Assert.assertTrue(s.get75thPercentile() >= minAcceptableseventyFifth 
        && s.get75thPercentile() <= maxAcceptableseventyFifth);

    final int ninetyFifthIndex = (int) (data.length * 0.95);
    final long minAcceptableninetyFifth = data[safeIndex(ninetyFifthIndex 
        - slop, data.length)];
    final long maxAcceptableninetyFifth = data[safeIndex(ninetyFifthIndex 
        + slop, data.length)];
    Assert.assertTrue(s.get95thPercentile() >= minAcceptableninetyFifth 
        && s.get95thPercentile() <= maxAcceptableninetyFifth);

  }
}
