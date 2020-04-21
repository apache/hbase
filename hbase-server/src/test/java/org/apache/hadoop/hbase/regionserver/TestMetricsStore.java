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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestMetricsStore {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsStore.class);


  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);


  @Test
  public void testStoreWrapperMetrics() {
    MetricsStore ms = new MetricsStore(new MetricsStoreWrapperStub(), new Configuration());
    MetricsStoreAggregateSource agg = ms.getSource().getAggregateSource();
    // increment the metrics based on the new registry
    ms.updateFileGet();
    ms.updateMemstoreGet();
    HELPER.assertGauge(
      "namespace_ns_table_table1_region_region1_store_store1_metric_maxStoreFileAge",
      50, agg);
    HELPER.assertGauge(
      "namespace_ns_table_table1_region_region1_store_store1_metric_minStoreFileAge",
      20, agg);
    HELPER.assertGauge(
      "namespace_ns_table_table1_region_region1_store_store1_metric_avgStoreFileAge",
      30, agg);
    HELPER.assertGauge(
      "namespace_ns_table_table1_region_region1_store_store1_metric_numReferenceFiles",
      11, agg);
    HELPER.assertGauge(
      "namespace_ns_table_table1_region_region1_store_store1_metric_memstoreSize",
      1000, agg);
    HELPER.assertCounter(
      "namespace_ns_table_table1_region_region1_store_store1_metric_" +
        "readRequestCount",
      1000, agg);
    HELPER.assertCounter(
      "namespace_ns_table_table1_region_region1_store_store1_metric_" +
        "getRequestCountOnMemstore",
      500, agg);
    HELPER.assertCounter(
      "namespace_ns_table_table1_region_region1_store_store1_metric_" +
        "getRequestCountOnFiles",
      500, agg);
    HELPER.assertCounter(
      "namespace_ns_table_table1_region_region1_store_store1_metric_" +
        "getsOnMemstoreCount",
      1, agg);
    HELPER.assertCounter(
      "namespace_ns_table_table1_region_region1_store_store1_metric_" +
        "getsOnFileCount",
      1, agg);
    ms.close();
  }

}
