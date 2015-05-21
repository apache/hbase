/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestServerSideScanMetricsFromClientSide {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Table TABLE = null;

  /**
   * Table configuration
   */
  private static TableName TABLE_NAME = TableName.valueOf("testTable");

  private static int NUM_ROWS = 10;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  // Should keep this value below 10 to keep generation of expected kv's simple. If above 10 then
  // table/row/cf1/... will be followed by table/row/cf10/... instead of table/row/cf2/... which
  // breaks the simple generation of expected kv's
  private static int NUM_FAMILIES = 1;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 1;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 10;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  private static int NUM_COLS = NUM_FAMILIES * NUM_QUALIFIERS;

  // Approximation of how large the heap size of cells in our table. Should be accessed through
  // getCellHeapSize().
  private static long CELL_HEAP_SIZE = -1;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TABLE = createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  static Table createTestTable(TableName name, byte[][] rows, byte[][] families,
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

  /**
   * Make puts to put the input value into each combination of row, family, and qualifier
   * @param rows
   * @param families
   * @param qualifiers
   * @param value
   * @return
   * @throws IOException
   */
  static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
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

  /**
   * @return The approximate heap size of a cell in the test table. All cells should have
   *         approximately the same heap size, so the value is cached to avoid repeating the
   *         calculation
   * @throws Exception
   */
  private long getCellHeapSize() throws Exception {
    if (CELL_HEAP_SIZE == -1) {
      // Do a partial scan that will return a single result with a single cell
      Scan scan = new Scan();
      scan.setMaxResultSize(1);
      scan.setAllowPartialResults(true);
      ResultScanner scanner = TABLE.getScanner(scan);

      Result result = scanner.next();

      assertTrue(result != null);
      assertTrue(result.rawCells() != null);
      assertTrue(result.rawCells().length == 1);

      CELL_HEAP_SIZE = CellUtil.estimatedHeapSizeOf(result.rawCells()[0]);
      scanner.close();
    }

    return CELL_HEAP_SIZE;
  }

  @Test
  public void testRowsSeenMetric() throws Exception {
    // Base scan configuration
    Scan baseScan;
    baseScan = new Scan();
    baseScan.setScanMetricsEnabled(true);
    testRowsSeenMetric(baseScan);

    // Test case that only a single result will be returned per RPC to the serer
    baseScan.setCaching(1);
    testRowsSeenMetric(baseScan);

    // Test case that partial results are returned from the server. At most one cell will be
    // contained in each response
    baseScan.setMaxResultSize(1);
    testRowsSeenMetric(baseScan);

    // Test case that size limit is set such that a few cells are returned per partial result from
    // the server
    baseScan.setCaching(NUM_ROWS);
    baseScan.setMaxResultSize(getCellHeapSize() * (NUM_COLS - 1));
    testRowsSeenMetric(baseScan);
  }

  public void testRowsSeenMetric(Scan baseScan) throws Exception {
    Scan scan;
    scan = new Scan(baseScan);
    testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY, NUM_ROWS);

    for (int i = 0; i < ROWS.length - 1; i++) {
      scan = new Scan(baseScan);
      scan.setStartRow(ROWS[0]);
      scan.setStopRow(ROWS[i + 1]);
      testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY, i + 1);
    }

    for (int i = ROWS.length - 1; i > 0; i--) {
      scan = new Scan(baseScan);
      scan.setStartRow(ROWS[i - 1]);
      scan.setStopRow(ROWS[ROWS.length - 1]);
      testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY, ROWS.length - i);
    }

    // The filter should filter out all rows, but we still expect to see every row.
    Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator("xyz".getBytes()));
    scan = new Scan(baseScan);
    scan.setFilter(filter);
    testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY, ROWS.length);

    // Filter should pass on all rows
    SingleColumnValueFilter singleColumnValueFilter =
        new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS[0], CompareOp.EQUAL, VALUE);
    scan = new Scan(baseScan);
    scan.setFilter(singleColumnValueFilter);
    testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY, ROWS.length);

    // Filter should filter out all rows
    singleColumnValueFilter =
        new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS[0], CompareOp.NOT_EQUAL, VALUE);
    scan = new Scan(baseScan);
    scan.setFilter(singleColumnValueFilter);
    testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY, ROWS.length);
  }

  @Test
  public void testRowsFilteredMetric() throws Exception {
    // Base scan configuration
    Scan baseScan;
    baseScan = new Scan();
    baseScan.setScanMetricsEnabled(true);

    // Test case where scan uses default values
    testRowsFilteredMetric(baseScan);

    // Test case where at most one Result is retrieved per RPC
    baseScan.setCaching(1);
    testRowsFilteredMetric(baseScan);

    // Test case where size limit is very restrictive and partial results will be returned from
    // server
    baseScan.setMaxResultSize(1);
    testRowsFilteredMetric(baseScan);

    // Test a case where max result size limits response from server to only a few cells (not all
    // cells from the row)
    baseScan.setCaching(NUM_ROWS);
    baseScan.setMaxResultSize(getCellHeapSize() * (NUM_COLS - 1));
    testRowsSeenMetric(baseScan);
  }

  public void testRowsFilteredMetric(Scan baseScan) throws Exception {
    testRowsFilteredMetric(baseScan, null, 0);

    // Row filter doesn't match any row key. All rows should be filtered
    Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator("xyz".getBytes()));
    testRowsFilteredMetric(baseScan, filter, ROWS.length);

    // Filter will return results containing only the first key. Number of entire rows filtered
    // should be 0.
    filter = new FirstKeyOnlyFilter();
    testRowsFilteredMetric(baseScan, filter, 0);

    // Column prefix will find some matching qualifier on each row. Number of entire rows filtered
    // should be 0
    filter = new ColumnPrefixFilter(QUALIFIERS[0]);
    testRowsFilteredMetric(baseScan, filter, 0);

    // Column prefix will NOT find any matching qualifier on any row. All rows should be filtered
    filter = new ColumnPrefixFilter("xyz".getBytes());
    testRowsFilteredMetric(baseScan, filter, ROWS.length);

    // Matching column value should exist in each row. No rows should be filtered.
    filter = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS[0], CompareOp.EQUAL, VALUE);
    testRowsFilteredMetric(baseScan, filter, 0);

    // No matching column value should exist in any row. Filter all rows
    filter = new SingleColumnValueFilter(FAMILIES[0], QUALIFIERS[0], CompareOp.NOT_EQUAL, VALUE);
    testRowsFilteredMetric(baseScan, filter, ROWS.length);

    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new RowFilter(CompareOp.EQUAL, new BinaryComparator(ROWS[0])));
    filters.add(new RowFilter(CompareOp.EQUAL, new BinaryComparator(ROWS[3])));
    int numberOfMatchingRowFilters = filters.size();
    filter = new FilterList(Operator.MUST_PASS_ONE, filters);
    testRowsFilteredMetric(baseScan, filter, ROWS.length - numberOfMatchingRowFilters);
    filters.clear();

    // Add a single column value exclude filter for each column... The net effect is that all
    // columns will be excluded when scanning on the server side. This will result in an empty cell
    // array in RegionScanner#nextInternal which should be interpreted as a row being filtered.
    for (int family = 0; family < FAMILIES.length; family++) {
      for (int qualifier = 0; qualifier < QUALIFIERS.length; qualifier++) {
        filters.add(new SingleColumnValueExcludeFilter(FAMILIES[family], QUALIFIERS[qualifier],
            CompareOp.EQUAL, VALUE));
      }
    }
    filter = new FilterList(Operator.MUST_PASS_ONE, filters);
    testRowsFilteredMetric(baseScan, filter, ROWS.length);
  }

  public void testRowsFilteredMetric(Scan baseScan, Filter filter, int expectedNumFiltered)
      throws Exception {
    Scan scan = new Scan(baseScan);
    if (filter != null) scan.setFilter(filter);
    testMetric(scan, ServerSideScanMetrics.COUNT_OF_ROWS_FILTERED_KEY, expectedNumFiltered);
  }

  /**
   * Run the scan to completetion and check the metric against the specified value
   * @param scan
   * @param metricKey
   * @param expectedValue
   * @throws Exception
   */
  public void testMetric(Scan scan, String metricKey, long expectedValue) throws Exception {
    assertTrue("Scan should be configured to record metrics", scan.isScanMetricsEnabled());
    ResultScanner scanner = TABLE.getScanner(scan);

    // Iterate through all the results
    for (Result r : scanner) {
    }
    scanner.close();
    ScanMetrics metrics = scan.getScanMetrics();
    assertTrue("Metrics are null", metrics != null);
    assertTrue("Metric : " + metricKey + " does not exist", metrics.hasCounter(metricKey));
    final long actualMetricValue = metrics.getCounter(metricKey).get();
    assertEquals("Metric: " + metricKey + " Expected: " + expectedValue + " Actual: "
        + actualMetricValue, expectedValue, actualMetricValue);

  }
}
