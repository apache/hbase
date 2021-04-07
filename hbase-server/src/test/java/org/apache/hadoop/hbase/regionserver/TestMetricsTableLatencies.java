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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestMetricsTableLatencies {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsTableLatencies.class);

  public static MetricsAssertHelper HELPER =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testTableWrapperAggregateMetrics() throws IOException {
    TableName tn1 = TableName.valueOf("table1");
    TableName tn2 = TableName.valueOf("table2");
    MetricsTableLatencies latencies = CompatibilitySingletonFactory.getInstance(
        MetricsTableLatencies.class);
    assertTrue("'latencies' is actually " + latencies.getClass(),
        latencies instanceof MetricsTableLatenciesImpl);
    MetricsTableLatenciesImpl latenciesImpl = (MetricsTableLatenciesImpl) latencies;
    RegionServerTableMetrics tableMetrics = new RegionServerTableMetrics(false);

    // Metrics to each table should be disjoint
    // N.B. each call to assertGauge removes all previously acquired metrics so we have to
    //   make the metrics call and then immediately verify it. Trying to do multiple metrics
    //   updates followed by multiple verifications will fail on the 2nd verification (as the
    //   first verification cleaned the data structures in MetricsAssertHelperImpl).
    tableMetrics.updateGet(tn1, 500L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(
        tn1, MetricsTableLatencies.GET_TIME + "_" + "999th_percentile"), 500L, latenciesImpl);
    tableMetrics.updatePut(tn1, 50L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(
        tn1, MetricsTableLatencies.PUT_TIME + "_" + "99th_percentile"), 50L, latenciesImpl);

    tableMetrics.updateGet(tn2, 300L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(
        tn2, MetricsTableLatencies.GET_TIME + "_" + "999th_percentile"), 300L, latenciesImpl);
    tableMetrics.updatePut(tn2, 75L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(
        tn2, MetricsTableLatencies.PUT_TIME + "_" + "99th_percentile"), 75L, latenciesImpl);
  }

  @Test
  public void testTableQueryMeterSwitch() {
    TableName tn1 = TableName.valueOf("table1");
    MetricsTableLatencies latencies = CompatibilitySingletonFactory.getInstance(
      MetricsTableLatencies.class);
    assertTrue("'latencies' is actually " + latencies.getClass(),
      latencies instanceof MetricsTableLatenciesImpl);
    MetricsTableLatenciesImpl latenciesImpl = (MetricsTableLatenciesImpl) latencies;

    Configuration conf = new Configuration();
    conf.setBoolean(MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY, false);
    boolean enableTableQueryMeter = conf.getBoolean(
      MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY,
      MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    // disable
    assertFalse(enableTableQueryMeter);
    RegionServerTableMetrics tableMetrics = new RegionServerTableMetrics(enableTableQueryMeter);
    tableMetrics.updateTableReadQueryMeter(tn1, 500L);
    assertFalse(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(
      tn1, MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND + "_" + "count"),
      latenciesImpl));
    tableMetrics.updateTableWriteQueryMeter(tn1, 500L);
    assertFalse(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(
      tn1, MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND + "_" + "count"),
      latenciesImpl));

    // enable
    conf.setBoolean(MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY, true);
    enableTableQueryMeter = conf.getBoolean(
      MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY,
      MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    assertTrue(enableTableQueryMeter);
    tableMetrics = new RegionServerTableMetrics(true);
    tableMetrics.updateTableReadQueryMeter(tn1, 500L);
    assertTrue(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(
      tn1, MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND + "_" + "count"),
      latenciesImpl));
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(
      tn1, MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND + "_" + "count"),
      500L, latenciesImpl);
    tableMetrics.updateTableWriteQueryMeter(tn1, 500L);
    assertTrue(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(
      tn1, MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND + "_" + "count"),
      latenciesImpl));
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(
      tn1, MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND + "_" + "count"),
      500L, latenciesImpl);
  }
}
