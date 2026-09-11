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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * End-to-end verification of the per-table metric cleanup path added in HBASE-27486.
 *
 * <p>Drives two successive rounds of
 * {@link MetricsTableWrapperAggregateImpl.TableMetricsWrapperRunnable#run()} against a mocked
 * {@link HRegionServer} whose online-region set transitions from
 * <code>{keep_table, drop_table}</code> to <code>{keep_table}</code>, then asserts that every
 * per-table latency histogram and every per-table query-meter belonging to
 * <code>drop_table</code> has been removed from the shared metric registry, while everything
 * belonging to <code>keep_table</code> is preserved.
 */
@Tag(SmallTests.TAG)
@Tag(RegionServerTests.TAG)
public class TestMetricsTableWrapperCleanup {

  private static final MetricsAssertHelper HELPER =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private static final TableName KEEP = TableName.valueOf("keep_table");
  private static final TableName DROP = TableName.valueOf("drop_table");

  @Test
  public void testMetricsAreRemovedWhenTableLeavesRegionServer() throws IOException {
    // A real RegionServerTableMetrics so we exercise both the latency and query-meter cleanup
    // paths through RegionServerTableMetrics.deleteTable(TableName).
    RegionServerTableMetrics tableMetrics = new RegionServerTableMetrics(true);
    MetricsTableLatencies latencies =
      CompatibilitySingletonFactory.getInstance(MetricsTableLatencies.class);
    assertTrue(latencies instanceof MetricsTableLatenciesImpl,
      "expected MetricsTableLatenciesImpl, got " + latencies.getClass());
    // Both the latency histograms and the query meters live under this BaseSource, so we can
    // use it as the target for HELPER.checkGaugeExists() for both metric bean families.
    MetricsTableLatenciesImpl source = (MetricsTableLatenciesImpl) latencies;

    // Populate metrics for both tables via the public RegionServerTableMetrics API so every
    // histogram / meter family is registered in the underlying DynamicMetricsRegistry.
    updateAllLatencies(tableMetrics, KEEP);
    updateAllLatencies(tableMetrics, DROP);
    tableMetrics.updateTableReadQueryMeter(KEEP, 3L);
    tableMetrics.updateTableWriteQueryMeter(KEEP, 4L);
    tableMetrics.updateTableReadQueryMeter(DROP, 5L);
    tableMetrics.updateTableWriteQueryMeter(DROP, 6L);

    // Sanity: every family exists for both tables before we run the wrapper.
    assertLatencyGaugesExist(source, KEEP);
    assertLatencyGaugesExist(source, DROP);
    assertQueryMeterGaugesExist(source, KEEP);
    assertQueryMeterGaugesExist(source, DROP);

    // Wire up a mocked HRegionServer whose online regions transition from {KEEP, DROP} on round
    // 1 to {KEEP} on round 2. Long metrics-period so the internal ScheduledFuture never fires
    // on its own; we drive runnable.run() by hand for determinism.
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.REGIONSERVER_METRICS_PERIOD, 600 * 1000);

    HRegionServer rs = mock(HRegionServer.class);
    when(rs.getConfiguration()).thenReturn(conf);
    MetricsRegionServer mrs = mock(MetricsRegionServer.class);
    when(mrs.getRegionServerTableMetrics()).thenReturn(tableMetrics);
    when(rs.getMetrics()).thenReturn(mrs);
    List<HRegion> roundOneRegions = Lists.newArrayList(regionOf(KEEP), regionOf(DROP));
    List<HRegion> roundTwoRegions = Collections.singletonList(regionOf(KEEP));
    when(rs.getOnlineRegionsLocalContext()).thenReturn(roundOneRegions).thenReturn(roundTwoRegions);

    MetricsTableWrapperAggregateImpl wrapper = new MetricsTableWrapperAggregateImpl(rs);
    try {
      MetricsTableWrapperAggregateImpl.TableMetricsWrapperRunnable runnable =
        wrapper.new TableMetricsWrapperRunnable();

      // Round 1: both tables online -> lastSeenTables becomes {KEEP, DROP}, no cleanup fires.
      runnable.run();
      assertLatencyGaugesExist(source, KEEP);
      assertLatencyGaugesExist(source, DROP);
      assertQueryMeterGaugesExist(source, KEEP);
      assertQueryMeterGaugesExist(source, DROP);

      // Round 2: DROP has left the RS -> lastSeenTables diff detects it and
      // RegionServerTableMetrics.deleteTable(DROP) fires.
      runnable.run();
      assertLatencyGaugesExist(source, KEEP);
      assertQueryMeterGaugesExist(source, KEEP);
      assertLatencyGaugesAbsent(source, DROP);
      assertQueryMeterGaugesAbsent(source, DROP);
    } finally {
      wrapper.close();
    }
  }

  // ------------------------------------------------------------------- helpers ----

  private static HRegion regionOf(TableName table) {
    TableDescriptor descriptor = mock(TableDescriptor.class);
    when(descriptor.getTableName()).thenReturn(table);
    HRegion region = mock(HRegion.class);
    when(region.getTableDescriptor()).thenReturn(descriptor);
    // No stores so the per-store aggregation loop in TableMetricsWrapperRunnable is a no-op;
    // we only care about the cleanup path here.
    when(region.getStores()).thenReturn(Collections.emptyList());
    return region;
  }

  private static void updateAllLatencies(RegionServerTableMetrics tableMetrics, TableName t) {
    tableMetrics.updateGet(t, 1L);
    tableMetrics.updatePut(t, 1L);
    tableMetrics.updatePutBatch(t, 1L);
    tableMetrics.updateDelete(t, 1L);
    tableMetrics.updateDeleteBatch(t, 1L);
    tableMetrics.updateIncrement(t, 1L);
    tableMetrics.updateAppend(t, 1L);
    tableMetrics.updateScanTime(t, 1L);
    tableMetrics.updateScanSize(t, 1L);
    tableMetrics.updateCheckAndDelete(t, 1L);
    tableMetrics.updateCheckAndPut(t, 1L);
    tableMetrics.updateCheckAndMutate(t, 1L);
  }

  private static final String[] LATENCY_FAMILIES = new String[] {
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

  private static void assertLatencyGaugesExist(MetricsTableLatenciesImpl source, TableName t) {
    for (String f : LATENCY_FAMILIES) {
      assertTrue(
        HELPER.checkGaugeExists(
          MetricsTableLatenciesImpl.qualifyMetricsName(t, f) + "_999th_percentile", source),
        t + "." + f + " should exist");
    }
  }

  private static void assertLatencyGaugesAbsent(MetricsTableLatenciesImpl source, TableName t) {
    for (String f : LATENCY_FAMILIES) {
      assertFalse(
        HELPER.checkGaugeExists(
          MetricsTableLatenciesImpl.qualifyMetricsName(t, f) + "_999th_percentile", source),
        t + "." + f + " should have been removed by deleteTable");
    }
  }

  private static void assertQueryMeterGaugesExist(MetricsTableLatenciesImpl source, TableName t) {
    assertTrue(
      HELPER.checkGaugeExists(
        MetricsTableLatenciesImpl.qualifyMetricsName(t,
          MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND) + "_count",
        source),
      t + " read query meter should exist");
    assertTrue(
      HELPER.checkGaugeExists(
        MetricsTableLatenciesImpl.qualifyMetricsName(t,
          MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND) + "_count",
        source),
      t + " write query meter should exist");
  }

  private static void assertQueryMeterGaugesAbsent(MetricsTableLatenciesImpl source, TableName t) {
    assertFalse(
      HELPER.checkGaugeExists(
        MetricsTableLatenciesImpl.qualifyMetricsName(t,
          MetricsTableQueryMeterImpl.TABLE_READ_QUERY_PER_SECOND) + "_count",
        source),
      t + " read query meter should have been removed by deleteTable");
    assertFalse(
      HELPER.checkGaugeExists(
        MetricsTableLatenciesImpl.qualifyMetricsName(t,
          MetricsTableQueryMeterImpl.TABLE_WRITE_QUERY_PER_SECOND) + "_count",
        source),
      t + " write query meter should have been removed by deleteTable");
  }
}
