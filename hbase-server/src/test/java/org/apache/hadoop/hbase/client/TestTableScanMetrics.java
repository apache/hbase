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

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.REGIONS_SCANNED_METRIC_NAME;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.
  COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category({ ClientTests.class, MediumTests.class })
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
    }
    else {
      scan.withStartRow(smallerRow, true);
      scan.withStopRow(largerRow, true);
    }
    return scan;
  }

  private ScanMetrics assertScannedRowsAndGetScanMetrics(Scan scan, int expectedCount)
    throws IOException {
    int countOfRows = 0;
    ScanMetrics scanMetrics;
    try (Table table = CONN.getTable(TABLE_NAME);
      ResultScanner scanner = table.getScanner(scan)) {
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
    Assert.assertEquals(expectedRowsScanned, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
    Assert.assertEquals(expectedRowsScanned,
      (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    Assert.assertTrue(scanMetrics.getMetricsMapByRegion().isEmpty());
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
    metricsMap = scanMetrics.getMetricsMap(false);
    Assert.assertEquals(0, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
    Assert.assertEquals(0, (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
  }

  @Test
  public void testScanMetricsByRegionForSingleRegionScan() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("xxx1"), Bytes.toBytes("xxx1"));
    scan.setScanMetricsEnabled(true);
    scan.setEnableScanMetricsByRegion(true);
    int expectedRowsScanned = 1;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);
    Assert.assertEquals(expectedRowsScanned, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
    Assert.assertEquals(expectedRowsScanned,
      (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.getMetricsMapByRegion(false);
    Assert.assertEquals(expectedRowsScanned, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry :
      scanMetricsByRegion.entrySet()) {
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
    scan.setScanMetricsEnabled(true);
    scan.setEnableScanMetricsByRegion(true);
    int expectedRowsScanned = 3;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);
    Assert.assertEquals(NUM_REGIONS, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
    Assert.assertEquals(expectedRowsScanned,
      (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.getMetricsMapByRegion(false);
    Assert.assertEquals(NUM_REGIONS, scanMetricsByRegion.size());
    int rowsScannedAcrossAllRegions = 0;
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry :
      scanMetricsByRegion.entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      metricsMap = entry.getValue();
      Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
      Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
      Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
      if (metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME) == 1) {
        rowsScannedAcrossAllRegions++;
      }
      else {
        assertEquals(0, (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
      }
    }
    Assert.assertEquals(expectedRowsScanned, rowsScannedAcrossAllRegions);
  }

  @Test
  public void testScanMetricsByRegionReset() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("xxx1"), Bytes.toBytes("zzz1"));
    scan.setScanMetricsEnabled(true);
    scan.setEnableScanMetricsByRegion(true);
    int expectedRowsScanned = 3;
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, expectedRowsScanned);
    Assert.assertNotNull(scanMetrics);

    // Retrieve scan metrics by region as a map and reset
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.getMetricsMapByRegion();
    // We scan 1 row per region
    Assert.assertEquals(expectedRowsScanned, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry :
    scanMetricsByRegion.entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      Map<String, Long> metricsMap = entry.getValue();
      Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
      Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
      Assert.assertEquals(1, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
      Assert.assertEquals(1, (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    }

    // Scan metrics have already been reset and now all counters should be 0
    scanMetricsByRegion = scanMetrics.getMetricsMapByRegion(false);
    // Size of map should be same as earlier
    Assert.assertEquals(expectedRowsScanned, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry :
      scanMetricsByRegion.entrySet()) {
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
  public void testConcurrentUpdatesAndResetToScanMetricsByRegion() throws Exception {
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    TableName tableName = TableName.valueOf(TestTableScanMetrics.class.getSimpleName()
      + "_testConcurrentUpdatesAndResetToScanMetricsByRegion");
    try (Table table = TEST_UTIL.createMultiRegionTable(tableName, CF)) {
      TEST_UTIL.loadTable(table, CF);

      Map<ScanMetricsRegionInfo, Map<String, Long>> concurrentScanMetricsByRegion = new HashMap<>();

      // Trigger two concurrent threads one of which scans the table and other periodically
      // collects the scan metrics.
      Scan scan = generateScan(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
      scan.setScanMetricsEnabled(true);
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
        Runnable metricsCollector = getPeriodicScanMetricsCollector(scanMetrics,
          concurrentScanMetricsByRegion, latch);
        executor.execute(tableScanner);
        executor.execute(metricsCollector);
        latch.await();
        // Merge leftover scan metrics
        mergeMaps(scanMetrics.getMetricsMapByRegion(), concurrentScanMetricsByRegion);
        Assert.assertEquals(HBaseTestingUtil.ROWS.length, rowsScanned.get());
      }

      Map<ScanMetricsRegionInfo, Map<String, Long>> expectedScanMetricsByRegion;

      // Collect scan metrics by region from single thread and assert that concurrent scan
      // and metrics collection works as expected
      scan = generateScan(EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
      scan.setScanMetricsEnabled(true);
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
        expectedScanMetricsByRegion = scanMetrics.getMetricsMapByRegion();
        for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry :
          expectedScanMetricsByRegion.entrySet()) {
          ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
          Map<String, Long> metricsMap = entry.getValue();
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
    }
    finally {
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
              scanMetrics.getMetricsMapByRegion();
            mergeMaps(scanMetricsByRegion, scanMetricsByRegionCollection);
            Thread.sleep(RAND.nextInt(10));
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private void mergeMaps(Map<ScanMetricsRegionInfo, Map<String, Long>> srcMap,
    Map<ScanMetricsRegionInfo, Map<String, Long>> dstMap) {
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : srcMap.entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      Map<String, Long> metricsMap = entry.getValue();
      if (dstMap.containsKey(scanMetricsRegionInfo)) {
        Map<String, Long> dstMetricsMap = dstMap.get(scanMetricsRegionInfo);
        for (Map.Entry<String, Long> metricEntry : metricsMap.entrySet()) {
          String metricName = metricEntry.getKey();
          Long existingValue = dstMetricsMap.get(metricName);
          Long newValue = metricEntry.getValue();
          dstMetricsMap.put(metricName, existingValue + newValue);
        }
      }
      else {
        dstMap.put(scanMetricsRegionInfo, metricsMap);
      }
    }
  }
}
