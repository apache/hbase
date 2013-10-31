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

import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Random;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;
import org.apache.hadoop.metrics.MetricsRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.yammer.metrics.stats.Snapshot;

@SuppressWarnings("deprecation")
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
    Assert.assertEquals(49.5d, h.getMean(), 0.01);
  }

  @Test
  public void testSnapshotPercentiles() {
    final MetricsHistogram h = new MetricsHistogram("testHistogram", null);
    final long[] data = genRandomData(h);

    final Snapshot s = h.getSnapshot();

    assertPercentile(data, 50, s.getMedian());
    assertPercentile(data, 75, s.get75thPercentile());
    assertPercentile(data, 95, s.get95thPercentile());
    assertPercentile(data, 98, s.get98thPercentile());
    assertPercentile(data, 99, s.get99thPercentile());
    assertPercentile(data, 99.9, s.get999thPercentile());
  }

  @Test
  public void testPushMetric() {
    final MetricsHistogram h = new MetricsHistogram("testHistogram", null);
    genRandomData(h);

    MetricsRecord mr = mock(MetricsRecord.class);
    h.pushMetric(mr);
    
    verify(mr).setMetric("testHistogram_num_ops", 10000L);
    verify(mr).setMetric(eq("testHistogram_min"), anyLong());
    verify(mr).setMetric(eq("testHistogram_max"), anyLong());
    verify(mr).setMetric(eq("testHistogram_mean"), anyFloat());
    verify(mr).setMetric(eq("testHistogram_std_dev"), anyFloat());
    verify(mr).setMetric(eq("testHistogram_median"), anyFloat());
    verify(mr).setMetric(eq("testHistogram_75th_percentile"), anyFloat());
    verify(mr).setMetric(eq("testHistogram_95th_percentile"), anyFloat());
    verify(mr).setMetric(eq("testHistogram_99th_percentile"), anyFloat());    
  }

  private void assertPercentile(long[] data, double percentile, double value) {
    int count = 0;
    for (long v : data) {
      if (v < value) {
        count++;
      }
    }
    Assert.assertEquals("Wrong " + percentile + " percentile", 
        (int)(percentile / 100), count / data.length);
  }
  
  private long[] genRandomData(final MetricsHistogram h) {
    final Random r = new Random();
    final long[] data = new long[10000];

    for (int i = 0; i < data.length; i++) {
      data[i] = (long) (r.nextGaussian() * 10000);
      h.update(data[i]);
    }
    
    return data;
  }
  
}
