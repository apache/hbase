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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.BYTES_IN_RESULTS_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.REGIONS_SCANNED_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.RPC_CALLS_METRIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableScanMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableScanMetrics.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("ScanMetrics");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection CONN;

  private static int NUM_REGIONS;

  @FunctionalInterface
  private interface ScanWithMetrics {
    Pair<List<Result>, ScanMetrics> scan(Scan scan) throws Exception;
  }

  @Parameter(0)
  public String methodName;

  @Parameter(1)
  public ScanWithMetrics method;

  @Parameters(name = "{index}: scan={0}")
  public static List<Object[]> params() {
    ScanWithMetrics doScanWithRawAsyncTable = TestAsyncTableScanMetrics::doScanWithRawAsyncTable;
    ScanWithMetrics doScanWithAsyncTableScan = TestAsyncTableScanMetrics::doScanWithAsyncTableScan;
    ScanWithMetrics doScanWithAsyncTableScanner =
      TestAsyncTableScanMetrics::doScanWithAsyncTableScanner;
    return Arrays.asList(new Object[] { "doScanWithRawAsyncTable", doScanWithRawAsyncTable },
      new Object[] { "doScanWithAsyncTableScan", doScanWithAsyncTableScan },
      new Object[] { "doScanWithAsyncTableScanner", doScanWithAsyncTableScanner });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    // Create 3 rows in the table, with rowkeys starting with "xxx*", "yyy*" and "zzz*" so that
    // scan hits all the region and not all rows lie in a single region
    try (Table table = UTIL.createMultiRegionTable(TABLE_NAME, CF)) {
      table.put(Arrays.asList(new Put(Bytes.toBytes("xxx1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("yyy1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz1")).addColumn(CF, CQ, VALUE)));
    }
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    NUM_REGIONS = UTIL.getHBaseCluster().getRegions(TABLE_NAME).size();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  private static Pair<List<Result>, ScanMetrics> doScanWithRawAsyncTable(Scan scan)
    throws IOException, InterruptedException {
    BufferingScanResultConsumer consumer = new BufferingScanResultConsumer();
    CONN.getTable(TABLE_NAME).scan(scan, consumer);
    List<Result> results = new ArrayList<>();
    for (Result result; (result = consumer.take()) != null;) {
      results.add(result);
    }
    return Pair.newPair(results, consumer.getScanMetrics());
  }

  private static Pair<List<Result>, ScanMetrics> doScanWithAsyncTableScan(Scan scan)
    throws Exception {
    SimpleScanResultConsumerImpl consumer = new SimpleScanResultConsumerImpl();
    CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool()).scan(scan, consumer);
    return Pair.newPair(consumer.getAll(), consumer.getScanMetrics());
  }

  private static Pair<List<Result>, ScanMetrics> doScanWithAsyncTableScanner(Scan scan)
    throws IOException {
    try (ResultScanner scanner =
      CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool()).getScanner(scan)) {
      List<Result> results = new ArrayList<>();
      for (Result result; (result = scanner.next()) != null;) {
        results.add(result);
      }
      return Pair.newPair(results, scanner.getScanMetrics());
    }
  }

  @Test
  public void testScanMetricsDisabled() throws Exception {
    Pair<List<Result>, ScanMetrics> pair = method.scan(new Scan());
    assertEquals(3, pair.getFirst().size());
    // Assert no scan metrics
    assertNull(pair.getSecond());
  }

  @Test
  public void testScanMetricsWithScanMetricsByRegionDisabled() throws Exception {
    Scan scan = new Scan();
    scan.setScanMetricsEnabled(true);
    Pair<List<Result>, ScanMetrics> pair = method.scan(scan);
    List<Result> results = pair.getFirst();
    assertEquals(3, results.size());
    long bytes = getBytesOfResults(results);
    ScanMetrics scanMetrics = pair.getSecond();
    assertEquals(NUM_REGIONS, scanMetrics.countOfRegions.get());
    assertEquals(bytes, scanMetrics.countOfBytesInResults.get());
    assertEquals(NUM_REGIONS, scanMetrics.countOfRPCcalls.get());
    // Assert scan metrics have not been collected by region
    assertTrue(scanMetrics.collectMetricsByRegion().isEmpty());
  }

  @Test
  public void testScanMetricsByRegionForSingleRegionScan() throws Exception {
    Scan scan = new Scan();
    scan.withStartRow(Bytes.toBytes("zzz1"), true);
    scan.withStopRow(Bytes.toBytes("zzz1"), true);
    scan.setEnableScanMetricsByRegion(true);
    Pair<List<Result>, ScanMetrics> pair = method.scan(scan);
    List<Result> results = pair.getFirst();
    assertEquals(1, results.size());
    long bytes = getBytesOfResults(results);
    ScanMetrics scanMetrics = pair.getSecond();
    assertEquals(1, scanMetrics.countOfRegions.get());
    assertEquals(bytes, scanMetrics.countOfBytesInResults.get());
    assertEquals(1, scanMetrics.countOfRPCcalls.get());
    // Assert scan metrics by region were collected for the region scanned
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.collectMetricsByRegion(false);
    assertEquals(1, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
      .entrySet()) {
      ScanMetricsRegionInfo smri = entry.getKey();
      Map<String, Long> metrics = entry.getValue();
      assertNotNull(smri.getServerName());
      assertNotNull(smri.getEncodedRegionName());
      // Assert overall scan metrics and scan metrics by region should be equal as only 1 region
      // was scanned.
      assertEquals(scanMetrics.getMetricsMap(false), metrics);
    }
  }

  @Test
  public void testScanMetricsByRegionForMultiRegionScan() throws Exception {
    Scan scan = new Scan();
    scan.setEnableScanMetricsByRegion(true);
    Pair<List<Result>, ScanMetrics> pair = method.scan(scan);
    List<Result> results = pair.getFirst();
    assertEquals(3, results.size());
    long bytes = getBytesOfResults(results);
    ScanMetrics scanMetrics = pair.getSecond();
    Map<String, Long> overallMetrics = scanMetrics.getMetricsMap(false);
    assertEquals(NUM_REGIONS, (long) overallMetrics.get(REGIONS_SCANNED_METRIC_NAME));
    assertEquals(NUM_REGIONS, scanMetrics.countOfRegions.get());
    assertEquals(bytes, (long) overallMetrics.get(BYTES_IN_RESULTS_METRIC_NAME));
    assertEquals(bytes, scanMetrics.countOfBytesInResults.get());
    assertEquals(NUM_REGIONS, (long) overallMetrics.get(RPC_CALLS_METRIC_NAME));
    assertEquals(NUM_REGIONS, scanMetrics.countOfRPCcalls.get());
    // Assert scan metrics by region were collected for the region scanned
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.collectMetricsByRegion(false);
    assertEquals(NUM_REGIONS, scanMetricsByRegion.size());
    int rowsScannedAcrossAllRegions = 0;
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
      .entrySet()) {
      ScanMetricsRegionInfo smri = entry.getKey();
      Map<String, Long> perRegionMetrics = entry.getValue();
      assertNotNull(smri.getServerName());
      assertNotNull(smri.getEncodedRegionName());
      assertEquals(1, (long) perRegionMetrics.get(REGIONS_SCANNED_METRIC_NAME));
      if (perRegionMetrics.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME) == 1) {
        bytes = getBytesOfResults(Collections.singletonList(results.get(0)));
        assertEquals(bytes, (long) perRegionMetrics.get(BYTES_IN_RESULTS_METRIC_NAME));
        rowsScannedAcrossAllRegions++;
      } else {
        assertEquals(0, (long) perRegionMetrics.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
        assertEquals(0, (long) perRegionMetrics.get(BYTES_IN_RESULTS_METRIC_NAME));
      }
    }
    assertEquals(3, rowsScannedAcrossAllRegions);
  }

  static long getBytesOfResults(List<Result> results) {
    return results.stream().flatMap(r -> Arrays.asList(r.rawCells()).stream())
      .mapToLong(c -> PrivateCellUtil.estimatedSerializedSizeOf(c)).sum();
  }
}
