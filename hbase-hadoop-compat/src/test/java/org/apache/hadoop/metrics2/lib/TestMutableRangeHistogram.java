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

package org.apache.hadoop.metrics2.lib;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

@Category({ MetricsTests.class, SmallTests.class })
public class TestMutableRangeHistogram {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMutableRangeHistogram.class);

  private static final String RECORD_NAME = "test";
  private static final String SIZE_HISTOGRAM_NAME = "TestSize";

  /**
   * calculate the distribution for last bucket, see HBASE-24615 for detail.
   */
  @Test
  public void testLastBucketWithSizeHistogram() {
    // create and init histogram minValue and maxValue
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    MutableSizeHistogram histogram = new MutableSizeHistogram(SIZE_HISTOGRAM_NAME, "");
    long[] ranges = histogram.getRanges();
    int len = ranges.length;
    histogram.add(0L);
    histogram.add(ranges[len - 1]);
    histogram.snapshot(collector.addRecord(RECORD_NAME), true);
    collector.clear();

    // fill up values and snapshot
    histogram.add(ranges[len - 2] * 2);
    histogram.add(ranges[len - 1] * 2);
    histogram.snapshot(collector.addRecord(RECORD_NAME), true);
    List<? extends MetricsRecord> records = collector.getRecords();
    assertEquals(1, records.size());
    MetricsRecord record = records.iterator().next();
    assertEquals(RECORD_NAME, record.name());

    // get size range metrics
    String histogramMetricPrefix = SIZE_HISTOGRAM_NAME + "_" + histogram.getRangeType();
    List<AbstractMetric> metrics = new ArrayList<>();
    for (AbstractMetric metric : record.metrics()) {
      if (metric.name().startsWith(histogramMetricPrefix)) {
        metrics.add(metric);
      }
    }
    assertEquals(2, metrics.size());

    // check range [10000000,100000000]
    String metricName = histogramMetricPrefix + "_" + ranges[len - 2] + "-" + ranges[len - 1];
    assertEquals(metricName, metrics.get(0).name());
    assertEquals(1, metrics.get(0).value().longValue());

    // check range [100000000, inf]
    metricName = histogramMetricPrefix + "_" + ranges[len - 1] + "-inf";
    assertEquals(metricName, metrics.get(1).name(), metricName);
    assertEquals(1, metrics.get(1).value().longValue());
  }
}
