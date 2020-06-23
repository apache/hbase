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

package org.apache.hadoop.hbase.http.prometheus;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class, MiscTests.class })
public class TestPrometheusServlet {

  @ClassRule
  public static final HBaseClassTestRule CLASS_TEST_RULE =
    HBaseClassTestRule.forClass(TestPrometheusServlet.class);

  PrometheusServlet ps;
  MetricRegistry mr;

  @Before
  public void setup() {
    ps = new PrometheusServlet();
    MetricRegistryInfo mri = new MetricRegistryInfo(
      "metricGroup",
      "dummy metric for tests",
      "",
      "mctxt",
      false);
    mr = MetricRegistries.global().create(mri);
  }

  @Test
  public void testPrometheusServlet() {

    //Counters
    Counter c1 = mr.counter("c1");
    c1.increment();
    test(ps, "c1", "metric_group_c1 1\n");
    mr.remove("c1");

    //Meters
    Meter m1 = mr.meter("m1");
    m1.mark(0);
    test(ps,
      "m1",
      "metric_group_m1_count 0\n" + "metric_group_m1_mean_rate 0.0\n"
      + "metric_group_m1_1min_rate 0.0\n" + "metric_group_m1_5min_rate 0.0\n"
      + "metric_group_m1_15min_rate 0.0\n");
    mr.remove("m1");

    //Timers (don't update the timer)
    mr.timer("t1");
    test(ps,
      "t1",
      "metric_group_t1_count 0\n"
      + "metric_group_t1_mean_rate 0.0\n"
      + "metric_group_t1_1min_rate 0.0\n"
      + "metric_group_t1_5min_rate 0.0\n"
      + "metric_group_t1_15min_rate 0.0\n"
      + "metric_group_t1_num_ops 0\n"
      + "metric_group_t1_min 0\n"
      + "metric_group_t1_max 0\n"
      + "metric_group_t1_mean 0\n"
      + "metric_group_t1_25th_percentile 0\n"
      + "metric_group_t1_median 0\n"
      + "metric_group_t1_75th_percentile 0\n"
      + "metric_group_t1_90th_percentile 0\n"
      + "metric_group_t1_95th_percentile 0\n"
      + "metric_group_t1_98th_percentile 0\n"
      + "metric_group_t1_99th_percentile 0\n"
      + "metric_group_t1_99.9th_percentile 0\n");
    mr.remove("t1");

    //Histograms
    Histogram h1 = mr.histogram("h1");
    h1.update(0);
    test(ps,
      "h1",
      "metric_group_h1_num_ops 1\n"
      + "metric_group_h1_min 0\n"
      + "metric_group_h1_max 0\n"
      + "metric_group_h1_mean 0\n"
      + "metric_group_h1_25th_percentile 0\n"
      + "metric_group_h1_median 0\n"
      + "metric_group_h1_75th_percentile 0\n"
      + "metric_group_h1_90th_percentile 0\n"
      + "metric_group_h1_95th_percentile 0\n"
      + "metric_group_h1_98th_percentile 0\n"
      + "metric_group_h1_99th_percentile 0\n"
      + "metric_group_h1_99.9th_percentile 0\n");
    mr.remove("h1");

  }

  private void test(PrometheusServlet ps, String metricName, String expected) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(bos);
    ps.writeMetrics(MetricRegistries.global().getMetricRegistries(), pw);
    String out = bos.toString();
    assert out.equals(expected) : String.format("Expected [%s] but result is [%s]. input was [%s]", expected, out, metricName);
  }

}
