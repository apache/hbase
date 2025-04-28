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
    // Subsequent call to get scan metrics map should show all counters as 0 (reset)
    metricsMap = scanMetrics.getMetricsMap();
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
    Assert.assertEquals(1, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry :
      scanMetricsByRegion.entrySet()) {
      ScanMetricsRegionInfo scanMetricsRegionInfo = entry.getKey();
      Map<String, Long> metrics = entry.getValue();
      Assert.assertNotNull(scanMetricsRegionInfo.getEncodedRegionName());
      Assert.assertNotNull(scanMetricsRegionInfo.getServerName());
      // As we are scanning single row so, overall scan metrics will match per region scan metrics
      Assert.assertEquals(expectedRowsScanned, (long) metricsMap.get(REGIONS_SCANNED_METRIC_NAME));
      Assert.assertEquals(expectedRowsScanned,
        (long) metricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
    }
  }
}
