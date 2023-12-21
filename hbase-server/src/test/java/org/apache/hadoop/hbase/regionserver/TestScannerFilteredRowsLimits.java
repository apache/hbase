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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class })
public class TestScannerFilteredRowsLimits {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScannerFilteredRowsLimits.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static int NUM_ROWS = 10;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  private static int NUM_FAMILIES = 1;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 1;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 10;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  private static Table TABLE = null;
  private static TableName TABLE_NAME = TableName.valueOf("testTable");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TABLE = createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  private static Table createTestTable(TableName name, byte[][] rows, byte[][] families,
    byte[][] qualifiers, byte[] cellValue) throws IOException {
    Table ht = TEST_UTIL.createTable(name, families);
    List<Put> puts = createPuts(rows, families, qualifiers, cellValue);
    ht.put(puts);

    return ht;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
    byte[] value) throws IOException {
    Put put;
    ArrayList<Put> puts = new ArrayList<>();

    for (int row = 0; row < rows.length; row++) {
      put = new Put(rows[row]);
      for (int fam = 0; fam < families.length; fam++) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(rows[row], families[fam], qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }

    return puts;
  }

  @Test
  public void testRowsFilteredMetricLimiter() throws Exception {
    // Base scan configuration
    Scan baseScan;
    baseScan = new Scan();
    baseScan.setScanMetricsEnabled(true);

    // No matching column value should exist in any row. Filter all rows
    Filter filter =
      new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS[0], CompareOperator.NOT_EQUAL, VALUE);
    testRowsFilteredMetric(baseScan, filter, ROWS.length);
    long scanRpcCalls =
      getScanMetricValue(new Scan(baseScan).setFilter(filter), ScanMetrics.RPC_CALLS_METRIC_NAME);

    HRegionServer rs1;
    // Update server side max count of rows filtered config.
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(TABLE.getName())) {
      RegionInfo firstHRI = locator.getAllRegionLocations().get(0).getRegion();
      rs1 = TEST_UTIL.getHBaseCluster()
        .getRegionServer(TEST_UTIL.getHBaseCluster().getServerWith(firstHRI.getRegionName()));
    }

    Configuration conf = TEST_UTIL.getConfiguration();
    // Set max rows filtered limitation.
    conf.setLong(RegionScannerLimiter.HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY, 7);
    conf.setBoolean(
      RegionScannerLimiter.HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_REACHED_REQUEST_KILLED_KEY, true);
    rs1.getConfigurationManager().notifyAllObservers(conf);

    assertThrows("Should throw a DoNotRetryIOException when too many rows have been filtered.",
      DoNotRetryIOException.class, () -> testRowsFilteredMetric(baseScan, filter, ROWS.length));

    conf.setBoolean(
      RegionScannerLimiter.HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_REACHED_REQUEST_KILLED_KEY,
      false);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    testRowsFilteredMetric(baseScan, filter, ROWS.length);

    // Test scan rpc calls.
    long scanRpcCalls2 =
      getScanMetricValue(new Scan(baseScan).setFilter(filter), ScanMetrics.RPC_CALLS_METRIC_NAME);
    assertEquals(scanRpcCalls + 1, scanRpcCalls2);

    conf.setLong(RegionScannerLimiter.HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY, 5);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    long scanRpcCalls3 =
      getScanMetricValue(new Scan(baseScan).setFilter(filter), ScanMetrics.RPC_CALLS_METRIC_NAME);
    assertEquals(scanRpcCalls + 2, scanRpcCalls3);

    conf.setLong(RegionScannerLimiter.HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY, 3);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    long scanRpcCalls4 =
      getScanMetricValue(new Scan(baseScan).setFilter(filter), ScanMetrics.RPC_CALLS_METRIC_NAME);
    assertEquals(scanRpcCalls + 3, scanRpcCalls4);

    // no max rows filtered limitation.
    conf.setLong(RegionScannerLimiter.HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY, 0);
    rs1.getConfigurationManager().notifyAllObservers(conf);
    testRowsFilteredMetric(baseScan, filter, ROWS.length);

    assertEquals(0, rs1.getRegionScannerLimiter().getScanners().size());
  }

  private void testRowsFilteredMetric(Scan baseScan, Filter filter, int expectedNumFiltered)
    throws Exception {
    Scan scan = new Scan(baseScan);
    if (filter != null) {
      scan.setFilter(filter);
    }
    testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME,
      expectedNumFiltered);
  }

  private void testMetric(Scan scan, String metricKey, long expectedValue) throws Exception {
    final long actualMetricValue = getScanMetricValue(scan, metricKey);
    assertEquals(
      "Metric: " + metricKey + " Expected: " + expectedValue + " Actual: " + actualMetricValue,
      expectedValue, actualMetricValue);
  }

  private static long getScanMetricValue(Scan scan, String metricKey) throws IOException {
    assertTrue("Scan should be configured to record metrics", scan.isScanMetricsEnabled());
    ResultScanner scanner = TABLE.getScanner(scan);
    // Iterate through all the results
    while (scanner.next() != null) {
      continue;
    }
    scanner.close();
    ScanMetrics metrics = scanner.getScanMetrics();
    assertNotNull("Metrics are null", metrics);
    assertTrue("Metric : " + metricKey + " does not exist", metrics.hasCounter(metricKey));
    return metrics.getCounter(metricKey).get();
  }
}
