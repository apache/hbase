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

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.REGIONS_SCANNED_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.RPC_RETRIES_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.RPC_QUEUE_WAIT_TIME_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.RPC_SCAN_TIME_METRIC_NAME;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category({ ClientTests.class, LargeTests.class })
public class TestTableScanMetrics extends FromClientSideBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableScanMetrics.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME =
    TableName.valueOf(TestTableScanMetrics.class.getSimpleName());

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static final Random RAND = new Random(11);

  private static int NUM_REGIONS;

  private static Connection CONN;

  @Parameters(name = "{index}: scanner={0}")
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { "ForwardScanner", new Scan() },
      new Object[] { "ReverseScanner", new Scan().setReversed(true) });
  }

  @Parameter(0)
  public String scannerName;

  @Parameter(1)
  public Scan originalScan;

  @BeforeClass
  public static void setUp() throws Exception {
    // Start the minicluster
    TEST_UTIL.startMiniCluster(2);
    // Create 3 rows in the table, with rowkeys starting with "xxx*", "yyy*" and "zzz*" so that
    // scan hits all the region and not all rows lie in a single region
    try (Table table = TEST_UTIL.createMultiRegionTable(TABLE_NAME, CF)) {
      table.put(Arrays.asList(new Put(Bytes.toBytes("xxx1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("yyy1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz1")).addColumn(CF, CQ, VALUE)));
    }
    CONN = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    NUM_REGIONS = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).size();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Scan generateScan(byte[] smallerRow, byte[] largerRow) throws IOException {
    Scan scan = new Scan(originalScan);
    if (originalScan.isReversed()) {
      scan.withStartRow(largerRow, true);
      scan.withStopRow(smallerRow, true);
    } else {
      scan.withStartRow(smallerRow, true);
      scan.withStopRow(largerRow, true);
    }
    return scan;
  }

  private ScanMetrics assertScannedRowsAndGetScanMetrics(Scan scan, int expectedCount)
    throws IOException {
    int countOfRows = 0;
    ScanMetrics scanMetrics;
    try (Table table = CONN.getTable(TABLE_NAME); ResultScanner scanner = table.getScanner(scan)) {
      for (Result result : scanner) {
        Assert.assertFalse(result.isEmpty());
        countOfRows++;
      }
      scanMetrics = scanner.getScanMetrics();
    }
    Assert.assertEquals(expectedCount, countOfRows);
    return scanMetrics;
  }

  @Test
  public void testScanMetricsDisabled() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("xxx1"), Bytes.toBytes("zzz1"));
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, 3);
    Assert.assertNull(scanMetrics);
  }

  @Test
  public void testScanMetricsWithScanMetricByRegionDisabled() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("xxx1"), Bytes.toBytes("zzz1"));
    scan.setScanMetricsEnabled(true);
    int expectedRowsScanned = 3;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);
    // The test setup is such that we have 1 row per region in the scan range
    Assert.assertEquals(expectedRowsScanned, scanMetrics.countOfRegions.get());
    Assert.assertEquals(expectedRowsScanned,
      (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    Assert.assertTrue(scanMetrics.collectMetricsByRegion().isEmpty());
  }

  @Test
  public void testScanMetricsResetWithScanMetricsByRegionDisabled() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("xxx1"), Bytes.toBytes("zzz1"));
    scan.setScanMetricsEnabled(true);
    int expectedRowsScanned = 3;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);
    // By default counters are collected with reset as true
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap();
    Assert.assertEquals(expectedRowsScanned, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
    Assert.assertEquals(expectedRowsScanned,
      (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    // Subsequent call to get scan metrics map should show all counters as 0
    Assert.assertEquals(0, scanMetrics.countOfRegions.get());
    Assert.assertEquals(0, scanMetrics.countOfRowsScanned.get());
  }

  @Test
  public void testScanMetricsByRegionForSingleRegionScan() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("xxx1"), Bytes.toBytes("xxx1"));
    scan.setEnableScanMetricsByRegion(true);
    int expectedRowsScanned = 1;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);
    Assert.assertEquals(expectedRowsScanned, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
    Assert.assertEquals(expectedRowsScanned,
      (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.collectMetricsByRegion(false);
    Assert.assertEquals(expectedRowsScanned, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
      .entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      metricsMap = entry.getValue();
      Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
      Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
      // As we are scanning single row so, overall scan metrics will match per region scan metrics
      Assert.assertEquals(expectedRowsScanned, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
      Assert.assertEquals(expectedRowsScanned,
        (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    }
  }

  @Test
  public void testScanMetricsByRegionForMultiRegionScan() throws Exception {
    Scan scan = generateScan(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
    scan.setEnableScanMetricsByRegion(true);
    int expectedRowsScanned = 3;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);
    Assert.assertEquals(NUM_REGIONS, scanMetrics.countOfRegions.get());
    Assert.assertEquals(expectedRowsScanned, scanMetrics.countOfRowsScanned.get());
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.collectMetricsByRegion(false);
    Assert.assertEquals(NUM_REGIONS, scanMetricsByRegion.size());
    int rowsScannedAcrossAllRegions = 0;
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
      .entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      Map<String, Long> metricsMap = entry.getValue();
      Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
      Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
      Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
      if (metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME) == 1) {
        rowsScannedAcrossAllRegions++;
      } else {
        assertEquals(0, (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
      }
    }
    Assert.assertEquals(expectedRowsScanned, rowsScannedAcrossAllRegions);
  }

  @Test
  public void testScanMetricsByRegionReset() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("xxx1"), Bytes.toBytes("zzz1"));
    scan.setEnableScanMetricsByRegion(true);
    int expectedRowsScanned = 3;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);

    // Retrieve scan metrics by region as a map and reset
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.collectMetricsByRegion();
    // We scan 1 row per region
    Assert.assertEquals(expectedRowsScanned, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
      .entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      Map<String, Long> metricsMap = entry.getValue();
      Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
      Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
      Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
      Assert.assertEquals(1, (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    }

    // Scan metrics have already been reset and now all counters should be 0
    scanMetricsByRegion = scanMetrics.collectMetricsByRegion(false);
    // Size of map should be same as earlier
    Assert.assertEquals(expectedRowsScanned, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
      .entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      Map<String, Long> metricsMap = entry.getValue();
      Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
      Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
      // Counters should have been reset to 0
      Assert.assertEquals(0, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
      Assert.assertEquals(0, (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    }
  }

  @Test
  public void testConcurrentUpdatesAndResetOfScanMetricsByRegion() throws Exception {
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    TableName tableName = TableName.valueOf(TestTableScanMetrics.class.getSimpleName()
      + "_testConcurrentUpdatesAndResetToScanMetricsByRegion");
    try (Table table = TEST_UTIL.createMultiRegionTable(tableName, CF)) {
      TEST_UTIL.loadTable(table, CF);

      Map<ScanMetricsRegionInfo, Map<String, Long>> concurrentScanMetricsByRegion = new HashMap<>();

      // Trigger two concurrent threads one of which scans the table and other periodically
      // collects the scan metrics (along with resetting the counters to 0).
      Scan scan = generateScan(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
      scan.setEnableScanMetricsByRegion(true);
      scan.setCaching(2);
      try (ResultScanner rs = table.getScanner(scan)) {
        ScanMetrics scanMetrics = rs.getScanMetrics();
        AtomicInteger rowsScanned = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        Runnable tableScanner = new Runnable() {
          public void run() {
            for (Result r : rs) {
              Assert.assertFalse(r.isEmpty());
              rowsScanned.incrementAndGet();
            }
            latch.countDown();
          }
        };
        Runnable metricsCollector =
          getPeriodicScanMetricsCollector(scanMetrics, concurrentScanMetricsByRegion, latch);
        executor.execute(tableScanner);
        executor.execute(metricsCollector);
        latch.await();
        // Merge leftover scan metrics
        mergeScanMetricsByRegion(scanMetrics.collectMetricsByRegion(),
          concurrentScanMetricsByRegion);
        Assert.assertEquals(HBaseTestingUtil.ROWS.length, rowsScanned.get());
      }

      Map<ScanMetricsRegionInfo, Map<String, Long>> expectedScanMetricsByRegion;

      // Collect scan metrics by region from single thread. Assert that concurrent scan
      // and metrics collection works as expected.
      scan = generateScan(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
      scan.setEnableScanMetricsByRegion(true);
      scan.setCaching(2);
      try (ResultScanner rs = table.getScanner(scan)) {
        ScanMetrics scanMetrics = rs.getScanMetrics();
        int rowsScanned = 0;
        for (Result r : rs) {
          Assert.assertFalse(r.isEmpty());
          rowsScanned++;
        }
        Assert.assertEquals(HBaseTestingUtil.ROWS.length, rowsScanned);
        expectedScanMetricsByRegion = scanMetrics.collectMetricsByRegion();
        for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : expectedScanMetricsByRegion
          .entrySet()) {
          ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
          Map<String, Long> metricsMap = entry.getValue();
          metricsMap.remove(RPC_SCAN_TIME_METRIC_NAME);
          metricsMap.remove(RPC_QUEUE_WAIT_TIME_METRIC_NAME);
          Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
          Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
          Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
          // Each region will have 26 * 26 + 26 + 1 rows except last region which will have 1 row
          long rowsScannedFromMetrics = metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);
          Assert.assertTrue(
            rowsScannedFromMetrics == 1 || rowsScannedFromMetrics == (26 * 26 + 26 + 1));
        }
      }

      // Assert on scan metrics by region
      Assert.assertEquals(expectedScanMetricsByRegion, concurrentScanMetricsByRegion);
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testRPCCallProcessingAndQueueWaitTimeMetrics() throws Exception {
    final int numThreads = 20;
    Configuration conf = TEST_UTIL.getConfiguration();
    // Handler count is 3 by default.
    int handlerCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
      HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    // Keep the number of threads to be high enough for RPC calls to queue up. For now going with 6
    // times the handler count.
    Assert.assertTrue(numThreads > 6 * handlerCount);
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
    TableName tableName = TableName.valueOf(
      TestTableScanMetrics.class.getSimpleName() + "_testRPCCallProcessingAndQueueWaitTimeMetrics");
    AtomicLong totalScanRpcTime = new AtomicLong(0);
    AtomicLong totalQueueWaitTime = new AtomicLong(0);
    CountDownLatch latch = new CountDownLatch(numThreads);
    try (Table table = TEST_UTIL.createMultiRegionTable(tableName, CF)) {
      TEST_UTIL.loadTable(table, CF);
      for (int i = 0; i < numThreads; i++) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              Scan scan = generateScan(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
              scan.setEnableScanMetricsByRegion(true);
              scan.setCaching(2);
              try (ResultScanner rs = table.getScanner(scan)) {
                Result r;
                while ((r = rs.next()) != null) {
                  Assert.assertFalse(r.isEmpty());
                }
                ScanMetrics scanMetrics = rs.getScanMetrics();
                Map<String, Long> metricsMap = scanMetrics.getMetricsMap();
                totalScanRpcTime.addAndGet(metricsMap.get(RPC_SCAN_TIME_METRIC_NAME));
                totalQueueWaitTime.addAndGet(metricsMap.get(RPC_QUEUE_WAIT_TIME_METRIC_NAME));
              }
              latch.countDown();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
      latch.await();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
      Assert.assertTrue(totalScanRpcTime.get() > 0);
      Assert.assertTrue(totalQueueWaitTime.get() > 0);
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testScanMetricsByRegionWithRegionMove() throws Exception {
    TableName tableName = TableName.valueOf(
      TestTableScanMetrics.class.getSimpleName() + "testScanMetricsByRegionWithRegionMove");
    try (Table table = TEST_UTIL.createMultiRegionTable(tableName, CF)) {
      TEST_UTIL.loadTable(table, CF);

      // Scan 2 regions with start row keys: bbb and ccc
      byte[] bbb = Bytes.toBytes("bbb");
      byte[] ccc = Bytes.toBytes("ccc");
      byte[] ddc = Bytes.toBytes("ddc");
      long expectedCountOfRowsScannedInMovedRegion = 0;
      // ROWS is the data loaded by loadTable()
      for (byte[] row : HBaseTestingUtil.ROWS) {
        if (Bytes.compareTo(row, bbb) >= 0 && Bytes.compareTo(row, ccc) < 0) {
          expectedCountOfRowsScannedInMovedRegion++;
        }
      }
      byte[] movedRegion = null;
      ScanMetrics scanMetrics;

      // Initialize scan with maxResultSize as size of 50 rows.
      Scan scan = generateScan(bbb, ddc);
      scan.setEnableScanMetricsByRegion(true);
      scan.setMaxResultSize(8000);

      try (ResultScanner rs = table.getScanner(scan)) {
        boolean isFirstScanOfRegion = true;
        for (Result r : rs) {
          byte[] row = r.getRow();
          if (isFirstScanOfRegion) {
            movedRegion = moveRegion(tableName, row);
            isFirstScanOfRegion = false;
          }
        }
        Assert.assertNotNull(movedRegion);

        scanMetrics = rs.getScanMetrics();
        Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
          scanMetrics.collectMetricsByRegion();
        long actualCountOfRowsScannedInMovedRegion = 0;
        Set<ServerName> serversForMovedRegion = new HashSet<>();

        // 2 regions scanned with two entries for first region as it moved in b/w scan
        Assert.assertEquals(3, scanMetricsByRegion.size());
        for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
          .entrySet()) {
          ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
          Map<String, Long> metricsMap = entry.getValue();
          if (scanMetricsRegionInfo.getEncodedRegionName().equals(Bytes.toString(movedRegion))) {
            long rowsScanned = metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);
            actualCountOfRowsScannedInMovedRegion += rowsScanned;
            serversForMovedRegion.add(scanMetricsRegionInfo.getServerName());

            Assert.assertEquals(1, (long) metricsMap.get(RPC_RETRIES_METRIC_NAME));
          }
          Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
        }
        Assert.assertEquals(expectedCountOfRowsScannedInMovedRegion,
          actualCountOfRowsScannedInMovedRegion);
        Assert.assertEquals(2, serversForMovedRegion.size());
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testScanMetricsByRegionWithRegionSplit() throws Exception {
    TableName tableName = TableName.valueOf(
      TestTableScanMetrics.class.getSimpleName() + "testScanMetricsByRegionWithRegionSplit");
    try (Table table = TEST_UTIL.createMultiRegionTable(tableName, CF)) {
      TEST_UTIL.loadTable(table, CF);

      // Scan 1 region with start row key: bbb
      byte[] bbb = Bytes.toBytes("bbb");
      byte[] bmw = Bytes.toBytes("bmw");
      byte[] ccb = Bytes.toBytes("ccb");
      long expectedCountOfRowsScannedInRegion = 0;
      // ROWS is the data loaded by loadTable()
      for (byte[] row : HBaseTestingUtil.ROWS) {
        if (Bytes.compareTo(row, bbb) >= 0 && Bytes.compareTo(row, ccb) <= 0) {
          expectedCountOfRowsScannedInRegion++;
        }
      }
      ScanMetrics scanMetrics;
      Set<String> expectedSplitRegionRes = new HashSet<>();

      // Initialize scan
      Scan scan = generateScan(bbb, ccb);
      scan.setEnableScanMetricsByRegion(true);
      scan.setMaxResultSize(8000);

      try (ResultScanner rs = table.getScanner(scan)) {
        boolean isFirstScanOfRegion = true;
        for (Result r : rs) {
          if (isFirstScanOfRegion) {
            splitRegion(tableName, bbb, bmw)
              .forEach(region -> expectedSplitRegionRes.add(Bytes.toString(region)));
            isFirstScanOfRegion = false;
          }
        }

        scanMetrics = rs.getScanMetrics();
        Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
          scanMetrics.collectMetricsByRegion();

        long actualCountOfRowsScannedInRegion = 0;
        long rpcRetiesCount = 0;
        Set<String> splitRegionRes = new HashSet<>();

        // 1 entry each for parent and two child regions
        Assert.assertEquals(3, scanMetricsByRegion.size());
        for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
          .entrySet()) {
          ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
          Map<String, Long> metricsMap = entry.getValue();
          long rowsScanned = metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);
          actualCountOfRowsScannedInRegion += rowsScanned;
          splitRegionRes.add(scanMetricsRegionInfo.getEncodedRegionName());

          if (metricsMap.get(RPC_RETRIES_METRIC_NAME) == 1) {
            rpcRetiesCount++;
          }

          Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
        }
        Assert.assertEquals(expectedCountOfRowsScannedInRegion, actualCountOfRowsScannedInRegion);
        Assert.assertEquals(2, rpcRetiesCount);
        Assert.assertEquals(expectedSplitRegionRes, splitRegionRes);
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testScanMetricsByRegionWithRegionMerge() throws Exception {
    TableName tableName = TableName.valueOf(
      TestTableScanMetrics.class.getSimpleName() + "testScanMetricsByRegionWithRegionMerge");
    try (Table table = TEST_UTIL.createMultiRegionTable(tableName, CF)) {
      TEST_UTIL.loadTable(table, CF);

      // Scan 2 regions with start row keys: bbb and ccc
      byte[] bbb = Bytes.toBytes("bbb");
      byte[] ccc = Bytes.toBytes("ccc");
      byte[] ddc = Bytes.toBytes("ddc");
      long expectedCountOfRowsScannedInRegions = 0;
      // ROWS is the data loaded by loadTable()
      for (byte[] row : HBaseTestingUtil.ROWS) {
        if (Bytes.compareTo(row, bbb) >= 0 && Bytes.compareTo(row, ddc) <= 0) {
          expectedCountOfRowsScannedInRegions++;
        }
      }
      ScanMetrics scanMetrics;
      Set<String> expectedMergeRegionsRes = new HashSet<>();
      String mergedRegionEncodedName = null;

      // Initialize scan
      Scan scan = generateScan(bbb, ddc);
      scan.setEnableScanMetricsByRegion(true);
      scan.setMaxResultSize(8000);

      try (ResultScanner rs = table.getScanner(scan)) {
        boolean isFirstScanOfRegion = true;
        for (Result r : rs) {
          if (isFirstScanOfRegion) {
            List<byte[]> out = mergeRegions(tableName, bbb, ccc);
            // Entry with index 2 is the encoded region name of merged region
            mergedRegionEncodedName = Bytes.toString(out.get(2));
            out.forEach(region -> expectedMergeRegionsRes.add(Bytes.toString(region)));
            isFirstScanOfRegion = false;
          }
        }

        scanMetrics = rs.getScanMetrics();
        Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
          scanMetrics.collectMetricsByRegion();
        long actualCountOfRowsScannedInRegions = 0;
        Set<String> mergeRegionsRes = new HashSet<>();
        boolean containsMergedRegionInScanMetrics = false;

        // 1 entry each for old region from which first row was scanned and new merged region
        Assert.assertEquals(2, scanMetricsByRegion.size());
        for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
          .entrySet()) {
          ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
          Map<String, Long> metricsMap = entry.getValue();
          long rowsScanned = metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);
          actualCountOfRowsScannedInRegions += rowsScanned;
          mergeRegionsRes.add(scanMetricsRegionInfo.getEncodedRegionName());
          if (scanMetricsRegionInfo.getEncodedRegionName().equals(mergedRegionEncodedName)) {
            containsMergedRegionInScanMetrics = true;
          }

          Assert.assertEquals(1, (long) metricsMap.get(RPC_RETRIES_METRIC_NAME));
          Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
        }
        Assert.assertEquals(expectedCountOfRowsScannedInRegions, actualCountOfRowsScannedInRegions);
        Assert.assertTrue(expectedMergeRegionsRes.containsAll(mergeRegionsRes));
        Assert.assertTrue(containsMergedRegionInScanMetrics);
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private Runnable getPeriodicScanMetricsCollector(ScanMetrics scanMetrics,
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegionCollection,
    CountDownLatch latch) {
    return new Runnable() {
      public void run() {
        try {
          while (latch.getCount() > 0) {
            Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
              scanMetrics.collectMetricsByRegion();
            mergeScanMetricsByRegion(scanMetricsByRegion, scanMetricsByRegionCollection);
            Thread.sleep(RAND.nextInt(10));
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private void mergeScanMetricsByRegion(Map<ScanMetricsRegionInfo, Map<String, Long>> srcMap,
    Map<ScanMetricsRegionInfo, Map<String, Long>> dstMap) {
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : srcMap.entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      Map<String, Long> metricsMap = entry.getValue();
      metricsMap.remove(RPC_SCAN_TIME_METRIC_NAME);
      metricsMap.remove(RPC_QUEUE_WAIT_TIME_METRIC_NAME);
      if (dstMap.containsKey(scanMetricsRegionInfo)) {
        Map<String, Long> dstMetricsMap = dstMap.get(scanMetricsRegionInfo);
        for (Map.Entry<String, Long> metricEntry : metricsMap.entrySet()) {
          String metricName = metricEntry.getKey();
          Long existingValue = dstMetricsMap.get(metricName);
          Long newValue = metricEntry.getValue();
          dstMetricsMap.put(metricName, existingValue + newValue);
        }
      } else {
        dstMap.put(scanMetricsRegionInfo, metricsMap);
      }
    }
  }

  /**
   * Moves the region with start row key from its original region server to some other region
   * server. This is a synchronous method.
   * @param tableName Table name of region to be moved belongs.
   * @param startRow  Start row key of the region to be moved.
   * @return Encoded region name of the region which was moved.
   */
  private byte[] moveRegion(TableName tableName, byte[] startRow) throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    RegionLocator regionLocator = CONN.getRegionLocator(tableName);
    HRegionLocation loc = regionLocator.getRegionLocation(startRow, true);
    byte[] encodedRegionName = loc.getRegion().getEncodedNameAsBytes();
    ServerName initialServerName = loc.getServerName();

    admin.move(encodedRegionName);

    ServerName finalServerName = regionLocator.getRegionLocation(startRow, true).getServerName();

    // Assert that region actually moved
    Assert.assertNotEquals(initialServerName, finalServerName);
    return encodedRegionName;
  }

  /**
   * Splits the region with start row key at the split key provided. This is a synchronous method.
   * @param tableName Table name of region to be split.
   * @param startRow  Start row key of the region to be split.
   * @param splitKey  Split key for splitting the region.
   * @return List of encoded region names with first element being parent region followed by two
   *         child regions.
   */
  private List<byte[]> splitRegion(TableName tableName, byte[] startRow, byte[] splitKey)
    throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    RegionLocator regionLocator = CONN.getRegionLocator(tableName);
    HRegionLocation topLoc = regionLocator.getRegionLocation(startRow, true);
    byte[] initialEncodedTopRegionName = topLoc.getRegion().getEncodedNameAsBytes();
    ServerName initialTopServerName = topLoc.getServerName();
    HRegionLocation bottomLoc = regionLocator.getRegionLocation(splitKey, true);
    byte[] initialEncodedBottomRegionName = bottomLoc.getRegion().getEncodedNameAsBytes();
    ServerName initialBottomServerName = bottomLoc.getServerName();

    // Assert region is ready for split
    Assert.assertEquals(initialTopServerName, initialBottomServerName);
    Assert.assertEquals(initialEncodedTopRegionName, initialEncodedBottomRegionName);

    FutureUtils.get(admin.splitRegionAsync(initialEncodedTopRegionName, splitKey));

    topLoc = regionLocator.getRegionLocation(startRow, true);
    byte[] finalEncodedTopRegionName = topLoc.getRegion().getEncodedNameAsBytes();
    bottomLoc = regionLocator.getRegionLocation(splitKey, true);
    byte[] finalEncodedBottomRegionName = bottomLoc.getRegion().getEncodedNameAsBytes();

    // Assert that region split is complete
    Assert.assertNotEquals(finalEncodedTopRegionName, finalEncodedBottomRegionName);
    Assert.assertNotEquals(initialEncodedTopRegionName, finalEncodedBottomRegionName);
    Assert.assertNotEquals(initialEncodedBottomRegionName, finalEncodedTopRegionName);

    return Arrays.asList(initialEncodedTopRegionName, finalEncodedTopRegionName,
      finalEncodedBottomRegionName);
  }

  /**
   * Merges two regions with the start row key as topRegion and bottomRegion. Ensures that the
   * regions to be merged are adjacent regions. This is a synchronous method.
   * @param tableName    Table name of regions to be merged.
   * @param topRegion    Start row key of first region for merging.
   * @param bottomRegion Start row key of second region for merging.
   * @return List of encoded region names with first two elements being original regions followed by
   *         the merged region.
   */
  private List<byte[]> mergeRegions(TableName tableName, byte[] topRegion, byte[] bottomRegion)
    throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    RegionLocator regionLocator = CONN.getRegionLocator(tableName);
    HRegionLocation topLoc = regionLocator.getRegionLocation(topRegion, true);
    byte[] initialEncodedTopRegionName = topLoc.getRegion().getEncodedNameAsBytes();
    String initialTopRegionEndKey = Bytes.toString(topLoc.getRegion().getEndKey());
    HRegionLocation bottomLoc = regionLocator.getRegionLocation(bottomRegion, true);
    byte[] initialEncodedBottomRegionName = bottomLoc.getRegion().getEncodedNameAsBytes();
    String initialBottomRegionStartKey = Bytes.toString(bottomLoc.getRegion().getStartKey());

    // Assert that regions are ready to be merged
    Assert.assertNotEquals(initialEncodedTopRegionName, initialEncodedBottomRegionName);
    Assert.assertEquals(initialBottomRegionStartKey, initialTopRegionEndKey);

    FutureUtils.get(admin.mergeRegionsAsync(
      new byte[][] { initialEncodedTopRegionName, initialEncodedBottomRegionName }, false));

    topLoc = regionLocator.getRegionLocation(topRegion, true);
    byte[] finalEncodedTopRegionName = topLoc.getRegion().getEncodedNameAsBytes();
    bottomLoc = regionLocator.getRegionLocation(bottomRegion, true);
    byte[] finalEncodedBottomRegionName = bottomLoc.getRegion().getEncodedNameAsBytes();

    // Assert regions have been merges successfully
    Assert.assertEquals(finalEncodedTopRegionName, finalEncodedBottomRegionName);
    Assert.assertNotEquals(initialEncodedTopRegionName, finalEncodedTopRegionName);
    Assert.assertNotEquals(initialEncodedBottomRegionName, finalEncodedTopRegionName);

    return Arrays.asList(initialEncodedTopRegionName, initialEncodedBottomRegionName,
      finalEncodedTopRegionName);
  }
}
