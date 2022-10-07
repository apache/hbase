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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test prometheus Sink.
 */
@Category({ SmallTests.class, MiscTests.class })
public class TestPrometheusServlet {

  @ClassRule
  public static final HBaseClassTestRule CLASS_TEST_RULE =
    HBaseClassTestRule.forClass(TestPrometheusServlet.class);

  @Test
  public void testPublish() throws IOException {
    // GIVEN
    MetricsSystem metrics = DefaultMetricsSystem.instance();
    metrics.init("test");
    TestMetrics testMetrics = metrics.register("TestMetrics", "Testing metrics", new TestMetrics());
    metrics.start();

    testMetrics.numBucketCreateFails.incr();
    metrics.publishMetricsNow();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    // WHEN
    PrometheusHadoopServlet prom2Servlet = new PrometheusHadoopServlet();
    // Test with no description
    prom2Servlet.writeMetrics(writer, false);

    // THEN
    String writtenMetrics = stream.toString(UTF_8.name());
    System.out.println(writtenMetrics);
    Assert.assertTrue("The expected metric line is missing from prometheus metrics output",
      writtenMetrics.contains("test_metrics_num_bucket_create_fails{context=\"dfs\""));

    metrics.stop();
    metrics.shutdown();
  }

  /**
   * Example metric pojo.
   */
  @Metrics(about = "Test Metrics", context = "dfs")
  private static class TestMetrics {

    @Metric
    private MutableCounterLong numBucketCreateFails;
  }
}
