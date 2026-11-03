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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestMetricsTableLatencies {

  public static MetricsAssertHelper HELPER =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Test
  public void testTableWrapperAggregateMetrics() throws IOException {
    TableName tn1 = TableName.valueOf("table1");
    TableName tn2 = TableName.valueOf("table2");
    MetricsTableLatencies latencies =
      CompatibilitySingletonFactory.getInstance(MetricsTableLatencies.class);
    assertTrue(latencies instanceof MetricsTableLatenciesImpl,
      "'latencies' is actually " + latencies.getClass());
    MetricsTableLatenciesImpl latenciesImpl = (MetricsTableLatenciesImpl) latencies;
    RegionServerTableMetrics tableMetrics = new RegionServerTableMetrics(false);

    // Metrics to each table should be disjoint
    // N.B. each call to assertGauge removes all previously acquired metrics so we have to
    // make the metrics call and then immediately verify it. Trying to do multiple metrics
    // updates followed by multiple verifications will fail on the 2nd verification (as the
    // first verification cleaned the data structures in MetricsAssertHelperImpl).
    tableMetrics.updateGet(tn1, 500L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
      MetricsTableLatencies.GET_TIME + "_" + "999th_percentile"), 500L, latenciesImpl);
    tableMetrics.updatePut(tn1, 50L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
      MetricsTableLatencies.PUT_TIME + "_" + "99th_percentile"), 50L, latenciesImpl);

    tableMetrics.updateGet(tn2, 300L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(tn2,
      MetricsTableLatencies.GET_TIME + "_" + "999th_percentile"), 300L, latenciesImpl);
    tableMetrics.updatePut(tn2, 75L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(tn2,
      MetricsTableLatencies.PUT_TIME + "_" + "99th_percentile"), 75L, latenciesImpl);
  }

  @Test
  public void testTableQueryMeterSwitch() {
    TableName tn1 = TableName.valueOf("table1");
    MetricsTableLatencies latencies =
      CompatibilitySingletonFactory.getInstance(MetricsTableLatencies.class);
    assertTrue(latencies instanceof MetricsTableLatenciesImpl,
      "'latencies' is actually " + latencies.getClass());
    MetricsTableLatenciesImpl latenciesImpl = (MetricsTableLatenciesImpl) latencies;

    Configuration conf = new Configuration();
    conf.setBoolean(MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY, false);
    boolean enableTableQueryMeter =
      conf.getBoolean(MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY,
        MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    // disable
    assertFalse(enableTableQueryMeter);
    RegionServerTableMetrics tableMetrics = new RegionServerTableMetrics(enableTableQueryMeter);
    tableMetrics.updateTableReadQueryMeter(tn1, 500L);
    assertFalse(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
      MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND + "_" + "count"), latenciesImpl));
    tableMetrics.updateTableWriteQueryMeter(tn1, 500L);
    assertFalse(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
      MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND + "_" + "count"), latenciesImpl));

    // enable
    conf.setBoolean(MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY, true);
    enableTableQueryMeter =
      conf.getBoolean(MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY,
        MetricsRegionServer.RS_ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    assertTrue(enableTableQueryMeter);
    tableMetrics = new RegionServerTableMetrics(true);
    tableMetrics.updateTableReadQueryMeter(tn1, 500L);
    assertTrue(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
      MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND + "_" + "count"), latenciesImpl));
    HELPER.assertGauge(
      MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
        MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND + "_" + "count"),
      500L, latenciesImpl);
    tableMetrics.updateTableWriteQueryMeter(tn1, 500L);
    assertTrue(HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
      MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND + "_" + "count"), latenciesImpl));
    HELPER.assertGauge(
      MetricsTableLatenciesImpl.qualifyMetricsName(tn1,
        MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND + "_" + "count"),
      500L, latenciesImpl);
  }
  /**
   * Verifies that {@link MetricsTableLatencies#deleteTable(String)} removes every histogram
   * family (across all metric suffixes) previously registered for the given table on
   * {@link MetricsTableLatenciesImpl}, while leaving other tables untouched. Also verifies that
   * re-writing samples for the dropped table lazily re-registers its histograms and that
   * deleting an unknown table is a no-op (HBASE-27486).
   */
  @Test
  public void testDeleteTableRemovesAllLatencyHistograms() throws IOException {
    TableName tnKeep = TableName.valueOf("keep_table");
    TableName tnDrop = TableName.valueOf("drop_table");
    MetricsTableLatencies latencies =
      CompatibilitySingletonFactory.getInstance(MetricsTableLatencies.class);
    assertTrue(latencies instanceof MetricsTableLatenciesImpl,
      "'latencies' is actually " + latencies.getClass());
    MetricsTableLatenciesImpl latenciesImpl = (MetricsTableLatenciesImpl) latencies;
    RegionServerTableMetrics tableMetrics = new RegionServerTableMetrics(false);

    // Every metric family registerd by MetricsTableLatenciesImpl.TableHistograms for a table.
    String[] families = new String[] {
      MetricsTableLatencies.GET_TIME,
      MetricsTableLatencies.PUT_TIME,
      MetricsTableLatencies.PUT_BATCH_TIME,
      MetricsTableLatencies.DELETE_TIME,
      MetricsTableLatencies.DELETE_BATCH_TIME,
      MetricsTableLatencies.INCREMENT_TIME,
      MetricsTableLatencies.APPEND_TIME,
      MetricsTableLatencies.SCAN_TIME,
      MetricsTableLatencies.SCAN_SIZE,
      MetricsTableLatencies.CHECK_AND_DELETE_TIME,
      MetricsTableLatencies.CHECK_AND_PUT_TIME,
      MetricsTableLatencies.CHECK_AND_MUTATE_TIME };

    // Populate both tables so every histogram family is registered in the underlying
    // DynamicMetricsRegistry.
    tableMetrics.updateGet(tnKeep, 100L);
    tableMetrics.updatePut(tnKeep, 20L);
    tableMetrics.updatePutBatch(tnKeep, 21L);
    tableMetrics.updateDelete(tnKeep, 22L);
    tableMetrics.updateDeleteBatch(tnKeep, 23L);
    tableMetrics.updateIncrement(tnKeep, 24L);
    tableMetrics.updateAppend(tnKeep, 25L);
    tableMetrics.updateScanTime(tnKeep, 26L);
    tableMetrics.updateScanSize(tnKeep, 27L);
    tableMetrics.updateCheckAndDelete(tnKeep, 28L);
    tableMetrics.updateCheckAndPut(tnKeep, 29L);
    tableMetrics.updateCheckAndMutate(tnKeep, 30L);

    tableMetrics.updateGet(tnDrop, 200L);
    tableMetrics.updatePut(tnDrop, 40L);
    tableMetrics.updatePutBatch(tnDrop, 41L);
    tableMetrics.updateDelete(tnDrop, 42L);
    tableMetrics.updateDeleteBatch(tnDrop, 43L);
    tableMetrics.updateIncrement(tnDrop, 44L);
    tableMetrics.updateAppend(tnDrop, 45L);
    tableMetrics.updateScanTime(tnDrop, 46L);
    tableMetrics.updateScanSize(tnDrop, 47L);
    tableMetrics.updateCheckAndDelete(tnDrop, 48L);
    tableMetrics.updateCheckAndPut(tnDrop, 49L);
    tableMetrics.updateCheckAndMutate(tnDrop, 50L);

    // Sanity: every family exists for both tables before deletion.
    for (String f : families) {
      assertTrue(
        HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tnKeep, f)
          + "_999th_percentile", latenciesImpl),
        "keep_table." + f + " should exist before deleteTable");
      assertTrue(
        HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tnDrop, f)
          + "_999th_percentile", latenciesImpl),
        "drop_table." + f + " should exist before deleteTable");
    }

    // Act: drop only tnDrop.
    latencies.deleteTable(tnDrop.getNameAsString());

    // Assert: all histogram families of tnDrop are gone from the registry, tnKeep is intact.
    for (String f : families) {
      assertFalse(
        HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tnDrop, f)
          + "_999th_percentile", latenciesImpl),
        "drop_table." + f + " should have been removed by deleteTable");
      assertTrue(
        HELPER.checkGaugeExists(MetricsTableLatenciesImpl.qualifyMetricsName(tnKeep, f)
          + "_999th_percentile", latenciesImpl),
        "keep_table." + f + " must not be affected by deleteTable(drop_table)");
    }

    // Re-adding samples for the dropped table should lazily re-register its histograms.
    tableMetrics.updateGet(tnDrop, 999L);
    HELPER.assertGauge(MetricsTableLatenciesImpl.qualifyMetricsName(tnDrop,
      MetricsTableLatencies.GET_TIME) + "_999th_percentile", 999L, latenciesImpl);

    // Deleting an unknown table must be a no-op.
    latencies.deleteTable(TableName.valueOf("never_seen").getNameAsString());
  }
}
