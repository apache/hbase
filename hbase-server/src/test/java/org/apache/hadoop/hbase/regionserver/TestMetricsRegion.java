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

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMetricsRegion {


  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testRegionWrapperMetrics() {
    MetricsRegion mr = new MetricsRegion(new MetricsRegionWrapperStub());
    MetricsRegionAggregateSource agg = mr.getSource().getAggregateSource();

    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_storeCount", 
      101, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_storeFileCount",
      102, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_maxStoreFileAge",
      2, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_minStoreFileAge",
      2, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_avgStoreFileAge",
      2, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_numReferenceFiles",
      2, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_memstoreSize",
      103, agg);
    HELPER.assertCounter(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_replicaid", 
      0, agg);
    mr.close();

    // test region with replica id > 0
    mr = new MetricsRegion(new MetricsRegionWrapperStub(1));
    agg = mr.getSource().getAggregateSource();
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_storeCount", 
      101, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_storeFileCount",
      102, agg);
    HELPER.assertGauge(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_memstoreSize",
      103, agg);
    HELPER.assertCounter(
      "namespace_TestNS_table_MetricsRegionWrapperStub_region_DEADBEEF001_metric_replicaid", 
      1, agg);
    mr.close();
  }
}
