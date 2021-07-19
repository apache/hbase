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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.experimental.categories.Category;
import static org.apache.hadoop.hbase.http.prometheus.PrometheusUtils.toPrometheusName;

@Category({ SmallTests.class, MiscTests.class })
public class TestPrometheusUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_TEST_RULE =
    HBaseClassTestRule.forClass(TestPrometheusUtils.class);

  @Test
  public void testConversionForNonAlNumChars() {

    test("metric_group_my_counter", "metricGroup", "my.counter");

    test("metric_group_my_counter", "metricGroup", "my-counter");

  }

  @Test
  public void testConversionForCamelCase() {

    test("rpc_time_some_metrics", "RpcTime", "SomeMetrics");

    test("rpc_time_small", "RpcTime", "small");

    test("metric_group_my_counter", "metricGroup", "myCounter");

  }

  @Test
  public void testConversionForMultiUnderscores() {

    test("metric_group_my_counter", "metricGroup", "my_Counter");

    test("metric_group_my_counter", "metricGroup", "my__Counter");
  }

  @Test
  public void testConversionWithAbbreviations() {

    test("om_rpc_time_om_info_keys", "OMRpcTime", "OMInfoKeys");

    test(
      "scm_pipeline_metrics_num_blocks_allocated_ratis_three_47659e3d_40c9_43b3_9792_4982fc279aba",
      "SCMPipelineMetrics",
      "NumBlocksAllocated-RATIS-THREE-47659e3d-40c9-43b3-9792-4982fc279aba"
    );
  }

  private void test(String expected, String group, String metric) {
    String actual = toPrometheusName(group, metric);
    assertEquals(
      String.format(
        "conversion failed for [group: %s, metric: %s] expected [%s], actual [%s]",
        group, metric, expected, actual
      ),
      expected,
      actual
    );
  }
}
