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

import static org.apache.hadoop.hbase.HConstants.RPC_CODEC_CONF_KEY;
import static org.apache.hadoop.hbase.client.FromClientSideTest3.generateHugeValue;
import static org.apache.hadoop.hbase.ipc.RpcClient.DEFAULT_CODEC_CLASS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client-side test, mostly testing scanners with various parameters. Parameterized on different
 * registry implementations.
 */
@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: registryImpl={0}, numHedgedReqs={1}")
public class TestScannersFromClientSide extends FromClientSideTestBase {

  public TestScannersFromClientSide(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestScannersFromClientSide.class);

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, 10 * 1024 * 1024);
    SLAVES = 3;
    initialize();
  }

  /**
   * Test from client side for batch of scan
   */
  @TestTemplate
  public void testScanBatch() throws Exception {
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 8);

    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put;
      Scan scan;
      Delete delete;
      Result result;
      ResultScanner scanner;
      boolean toLog = true;
      List<Cell> kvListExp;

      // table: row, family, c0:0, c1:1, ... , c7:7
      put = new Put(ROW);
      for (int i = 0; i < QUALIFIERS.length; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[i], i, VALUE);
        put.add(kv);
      }
      ht.put(put);

      // table: row, family, c0:0, c1:1, ..., c6:2, c6:6 , c7:7
      put = new Put(ROW);
      KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[6], 2, VALUE);
      put.add(kv);
      ht.put(put);

      // delete upto ts: 3
      delete = new Delete(ROW);
      delete.addFamily(FAMILY, 3);
      ht.delete(delete);

      // without batch
      scan = new Scan().withStartRow(ROW);
      scan.readAllVersions();
      scanner = ht.getScanner(scan);

      // c4:4, c5:5, c6:6, c7:7
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
      result = scanner.next();
      verifyResult(result, kvListExp, toLog, "Testing first batch of scan");

      // with batch
      scan = new Scan().withStartRow(ROW);
      scan.readAllVersions();
      scan.setBatch(2);
      scanner = ht.getScanner(scan);

      // First batch: c4:4, c5:5
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
      result = scanner.next();
      verifyResult(result, kvListExp, toLog, "Testing first batch of scan");

      // Second batch: c6:6, c7:7
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
      result = scanner.next();
      verifyResult(result, kvListExp, toLog, "Testing second batch of scan");
    }
  }

  @TestTemplate
  public void testMaxResultSizeIsSetToDefault() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // The max result size we expect the scan to use by default.
      long expectedMaxResultSize =
        TEST_UTIL.getConfiguration().getLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
          HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);

      int numRows = 5;
      byte[][] ROWS = HTestConst.makeNAscii(ROW, numRows);

      int numQualifiers = 10;
      byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, numQualifiers);

      // Specify the cell size such that a single row will be larger than the default
      // value of maxResultSize. This means that Scan RPCs should return at most a single
      // result back to the client.
      int cellSize = (int) (expectedMaxResultSize / (numQualifiers - 1));
      byte[] cellValue = Bytes.createMaxByteArray(cellSize);

      Put put;
      List<Put> puts = new ArrayList<>();
      for (int row = 0; row < ROWS.length; row++) {
        put = new Put(ROWS[row]);
        for (int qual = 0; qual < QUALIFIERS.length; qual++) {
          KeyValue kv = new KeyValue(ROWS[row], FAMILY, QUALIFIERS[qual], cellValue);
          put.add(kv);
        }
        puts.add(put);
      }
      ht.put(puts);

      // Create a scan with the default configuration.
      Scan scan = new Scan();

      try (ResultScanner scanner = ht.getScanner(scan)) {
        assertThat(scanner, instanceOf(AsyncTableResultScanner.class));
        scanner.next();
        AsyncTableResultScanner s = (AsyncTableResultScanner) scanner;
        // The scanner should have, at most, a single result in its cache. If there more results
        // exists in the cache it means that more than the expected max result size was fetched.
        assertTrue(s.getCacheSize() <= 1, "The cache contains: " + s.getCacheSize() + " results");
      }
    }
  }

  /**
   * Scan on not existing table should throw the exception with correct message
   */
  @TestTemplate
  public void testScannerForNotExistingTable() throws Exception {
    String[] tableNames = { "A", "Z", "A:A", "Z:Z" };
    for (String tableName : tableNames) {
      try (Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName))) {
        TableNotFoundException e = assertThrows(TableNotFoundException.class,
          () -> testSmallScan(table, true, 1, 5), "TableNotFoundException was not thrown");
        // We expect that the message for TableNotFoundException would have only the table name only
        // Otherwise that would mean that localeRegionInMeta doesn't work properly
        assertEquals(e.getMessage(), tableName);
      }
    }
  }

  @TestTemplate
  public void testSmallScan() throws Exception {
    int numRows = 10;
    byte[][] ROWS = HTestConst.makeNAscii(ROW, numRows);

    int numQualifiers = 10;
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, numQualifiers);

    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put;
      List<Put> puts = new ArrayList<>();
      for (int row = 0; row < ROWS.length; row++) {
        put = new Put(ROWS[row]);
        for (int qual = 0; qual < QUALIFIERS.length; qual++) {
          KeyValue kv = new KeyValue(ROWS[row], FAMILY, QUALIFIERS[qual], VALUE);
          put.add(kv);
        }
        puts.add(put);
      }
      ht.put(puts);

      int expectedRows = numRows;
      int expectedCols = numRows * numQualifiers;

      // Test normal and reversed
      testSmallScan(ht, true, expectedRows, expectedCols);
      testSmallScan(ht, false, expectedRows, expectedCols);
    }
  }

  /**
   * Run through a variety of test configurations with a small scan
   */
  private void testSmallScan(Table table, boolean reversed, int rows, int columns)
    throws Exception {
    Scan baseScan = new Scan();
    baseScan.setReversed(reversed);
    baseScan.setReadType(ReadType.PREAD);

    Scan scan = new Scan(baseScan);
    verifyExpectedCounts(table, scan, rows, columns);

    scan = new Scan(baseScan);
    scan.setMaxResultSize(1);
    verifyExpectedCounts(table, scan, rows, columns);

    scan = new Scan(baseScan);
    scan.setMaxResultSize(1);
    scan.setCaching(Integer.MAX_VALUE);
    verifyExpectedCounts(table, scan, rows, columns);
  }

  private void verifyExpectedCounts(Table table, Scan scan, int expectedRowCount,
    int expectedCellCount) throws Exception {
    try (ResultScanner scanner = table.getScanner(scan)) {

      int rowCount = 0;
      int cellCount = 0;
      Result r = null;
      while ((r = scanner.next()) != null) {
        rowCount++;
        cellCount += r.rawCells().length;
      }

      assertEquals(expectedRowCount, rowCount,
        "Expected row count: " + expectedRowCount + " Actual row count: " + rowCount);
      assertEquals(expectedCellCount, cellCount,
        "Expected cell count: " + expectedCellCount + " Actual cell count: " + cellCount);
    }
  }

  /**
   * Test from client side for get with maxResultPerCF set
   */
  @TestTemplate
  public void testGetMaxResults() throws Exception {
    byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    TEST_UTIL.createTable(tableName, FAMILIES);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Get get;
      Put put;
      Result result;
      boolean toLog = true;
      List<Cell> kvListExp;

      kvListExp = new ArrayList<>();
      // Insert one CF for row[0]
      put = new Put(ROW);
      for (int i = 0; i < 10; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
        put.add(kv);
        kvListExp.add(kv);
      }
      ht.put(put);

      get = new Get(ROW);
      result = ht.get(get);
      verifyResult(result, kvListExp, toLog, "Testing without setting maxResults");

      get = new Get(ROW);
      get.setMaxResultsPerColumnFamily(2);
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[0], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
      verifyResult(result, kvListExp, toLog, "Testing basic setMaxResults");

      // Filters: ColumnRangeFilter
      get = new Get(ROW);
      get.setMaxResultsPerColumnFamily(5);
      get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5], true));
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[2], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
      verifyResult(result, kvListExp, toLog, "Testing single CF with CRF");

      // Insert two more CF for row[0]
      // 20 columns for CF2, 10 columns for CF1
      put = new Put(ROW);
      for (int i = 0; i < QUALIFIERS.length; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE);
        put.add(kv);
      }
      ht.put(put);

      put = new Put(ROW);
      for (int i = 0; i < 10; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE);
        put.add(kv);
      }
      ht.put(put);

      get = new Get(ROW);
      get.setMaxResultsPerColumnFamily(12);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      // Exp: CF1:q0, ..., q9, CF2: q0, q1, q10, q11, ..., q19
      for (int i = 0; i < 10; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
      }
      for (int i = 0; i < 2; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
      }
      for (int i = 10; i < 20; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
      }
      verifyResult(result, kvListExp, toLog, "Testing multiple CFs");

      // Filters: ColumnRangeFilter and ColumnPrefixFilter
      get = new Get(ROW);
      get.setMaxResultsPerColumnFamily(3);
      get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, null, true));
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      for (int i = 2; i < 5; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
      }
      for (int i = 2; i < 5; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
      }
      for (int i = 2; i < 5; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
      }
      verifyResult(result, kvListExp, toLog, "Testing multiple CFs + CRF");

      get = new Get(ROW);
      get.setMaxResultsPerColumnFamily(7);
      get.setFilter(new ColumnPrefixFilter(QUALIFIERS[1]));
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[1], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[1], 1, VALUE));
      for (int i = 10; i < 16; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
      }
      verifyResult(result, kvListExp, toLog, "Testing multiple CFs + PFF");
    }

  }

  /**
   * Test from client side for scan with maxResultPerCF set
   */
  @TestTemplate
  public void testScanMaxResults() throws Exception {
    byte[][] ROWS = HTestConst.makeNAscii(ROW, 2);
    byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 10);

    TEST_UTIL.createTable(tableName, FAMILIES);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put;
      Scan scan;
      Result result;
      boolean toLog = true;
      List<Cell> kvListExp, kvListScan;

      kvListExp = new ArrayList<>();

      for (int r = 0; r < ROWS.length; r++) {
        put = new Put(ROWS[r]);
        for (int c = 0; c < FAMILIES.length; c++) {
          for (int q = 0; q < QUALIFIERS.length; q++) {
            KeyValue kv = new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 1, VALUE);
            put.add(kv);
            if (q < 4) {
              kvListExp.add(kv);
            }
          }
        }
        ht.put(put);
      }

      scan = new Scan();
      scan.setMaxResultsPerColumnFamily(4);
      ResultScanner scanner = ht.getScanner(scan);
      kvListScan = new ArrayList<>();
      while ((result = scanner.next()) != null) {
        for (Cell kv : result.listCells()) {
          kvListScan.add(kv);
        }
      }
      result = Result.create(kvListScan);
      verifyResult(result, kvListExp, toLog, "Testing scan with maxResults");
    }
  }

  /**
   * Test from client side for get with rowOffset
   */
  @TestTemplate
  public void testGetRowOffset() throws Exception {
    byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    TEST_UTIL.createTable(tableName, FAMILIES);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Get get;
      Put put;
      Result result;
      boolean toLog = true;
      List<Cell> kvListExp;

      // Insert one CF for row
      kvListExp = new ArrayList<>();
      put = new Put(ROW);
      for (int i = 0; i < 10; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
        put.add(kv);
        // skipping first two kvs
        if (i < 2) {
          continue;
        }
        kvListExp.add(kv);
      }
      ht.put(put);

      // setting offset to 2
      get = new Get(ROW);
      get.setRowOffsetPerColumnFamily(2);
      result = ht.get(get);
      verifyResult(result, kvListExp, toLog, "Testing basic setRowOffset");

      // setting offset to 20
      get = new Get(ROW);
      get.setRowOffsetPerColumnFamily(20);
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      verifyResult(result, kvListExp, toLog, "Testing offset > #kvs");

      // offset + maxResultPerCF
      get = new Get(ROW);
      get.setRowOffsetPerColumnFamily(4);
      get.setMaxResultsPerColumnFamily(5);
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      for (int i = 4; i < 9; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
      }
      verifyResult(result, kvListExp, toLog, "Testing offset + setMaxResultsPerCF");

      // Filters: ColumnRangeFilter
      get = new Get(ROW);
      get.setRowOffsetPerColumnFamily(1);
      get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5], true));
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
      verifyResult(result, kvListExp, toLog, "Testing offset with CRF");

      // Insert into two more CFs for row
      // 10 columns for CF2, 10 columns for CF1
      for (int j = 2; j > 0; j--) {
        put = new Put(ROW);
        for (int i = 0; i < 10; i++) {
          KeyValue kv = new KeyValue(ROW, FAMILIES[j], QUALIFIERS[i], 1, VALUE);
          put.add(kv);
        }
        ht.put(put);
      }

      get = new Get(ROW);
      get.setRowOffsetPerColumnFamily(4);
      get.setMaxResultsPerColumnFamily(2);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      result = ht.get(get);
      kvListExp = new ArrayList<>();
      // Exp: CF1:q4, q5, CF2: q4, q5
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[4], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[5], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[4], 1, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[5], 1, VALUE));
      verifyResult(result, kvListExp, toLog, "Testing offset + multiple CFs + maxResults");
    }
  }

  @TestTemplate
  public void testRawScanExpiredCell() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      final Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      put.setTTL(0);
      table.put(put);
      final Scan scan = new Scan().setRaw(true);
      try (final ResultScanner scanner = table.getScanner(scan)) {
        final Result result = scanner.next();
        assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
        assertNull(scanner.next());
      }
    }
  }

  @TestTemplate
  public void testScanRawDeleteFamilyVersion() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    Configuration conf = getClientConf();
    conf.set(RPC_CODEC_CONF_KEY, "");
    conf.set(DEFAULT_CODEC_CLASS, "");
    try (Connection connection = ConnectionFactory.createConnection(conf);
      Table table = connection.getTable(tableName)) {
      Delete delete = new Delete(ROW);
      delete.addFamilyVersion(FAMILY, 0L);
      table.delete(delete);
      Scan scan = new Scan().withStartRow(ROW).setRaw(true);
      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      while (scanner.next() != null) {
        count++;
      }
      assertEquals(1, count);
    }
  }

  /**
   * Test from client side for scan while the region is reopened on the same region server.
   */
  @TestTemplate
  public void testScanOnReopenedRegion() throws Exception {
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 2);

    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put;
      Scan scan;
      Result result;
      ResultScanner scanner;
      boolean toLog = false;
      List<Cell> kvListExp;

      // table: row, family, c0:0, c1:1
      put = new Put(ROW);
      for (int i = 0; i < QUALIFIERS.length; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[i], i, VALUE);
        put.add(kv);
      }
      ht.put(put);

      scan = new Scan().withStartRow(ROW);
      scanner = ht.getScanner(scan);

      HRegionLocation loc;

      try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        loc = locator.getRegionLocation(ROW);
      }
      RegionInfo hri = loc.getRegion();
      SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
      byte[] regionName = hri.getRegionName();
      int i = cluster.getServerWith(regionName);
      HRegionServer rs = cluster.getRegionServer(i);
      LOG.info("Unassigning " + hri);
      TEST_UTIL.getAdmin().unassign(hri.getRegionName(), true);
      long startTime = EnvironmentEdgeManager.currentTime();
      long timeOut = 10000;
      boolean offline = false;
      while (true) {
        if (rs.getOnlineRegion(regionName) == null) {
          offline = true;
          break;
        }
        assertTrue(EnvironmentEdgeManager.currentTime() < startTime + timeOut,
          "Timed out in closing the testing region");
      }
      assertTrue(offline);
      LOG.info("Assigning " + hri);
      TEST_UTIL.getAdmin().assign(hri.getRegionName());
      startTime = EnvironmentEdgeManager.currentTime();
      while (true) {
        rs = cluster.getRegionServer(cluster.getServerWith(regionName));
        if (rs != null && rs.getOnlineRegion(regionName) != null) {
          offline = false;
          break;
        }
        assertTrue(EnvironmentEdgeManager.currentTime() < startTime + timeOut,
          "Timed out in open the testing region");
      }
      assertFalse(offline);

      // c0:0, c1:1
      kvListExp = new ArrayList<>();
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[0], 0, VALUE));
      kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[1], 1, VALUE));
      result = scanner.next();
      verifyResult(result, kvListExp, toLog, "Testing scan on re-opened region");
    }
  }

  static void verifyResult(Result result, List<Cell> expKvList, boolean toLog, String msg) {
    LOG.info(msg);
    LOG.info("Expected count: " + expKvList.size());
    LOG.info("Actual count: " + result.size());
    if (expKvList.isEmpty()) {
      return;
    }

    int i = 0;
    for (Cell kv : result.rawCells()) {
      if (i >= expKvList.size()) {
        break; // we will check the size later
      }

      Cell kvExp = expKvList.get(i++);
      if (toLog) {
        LOG.info("get kv is: " + kv.toString());
        LOG.info("exp kv is: " + kvExp.toString());
      }
      assertTrue(kvExp.equals(kv), "Not equal");
    }

    assertEquals(expKvList.size(), result.size());
  }

  @TestTemplate
  public void testReadExpiredDataForRawScan() throws IOException {
    long ts = EnvironmentEdgeManager.currentTime() - 10000;
    byte[] value = Bytes.toBytes("expired");
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, ts, value));
      assertArrayEquals(value, table.get(new Get(ROW)).getValue(FAMILY, QUALIFIER));
      TEST_UTIL.getAdmin().modifyColumnFamily(tableName,
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setTimeToLive(5).build());
      try (ResultScanner scanner = table.getScanner(FAMILY)) {
        assertNull(scanner.next());
      }
      try (ResultScanner scanner = table.getScanner(new Scan().setRaw(true))) {
        assertArrayEquals(value, scanner.next().getValue(FAMILY, QUALIFIER));
        assertNull(scanner.next());
      }
    }
  }

  @TestTemplate
  public void testScanWithColumnsAndFilterAndVersion() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    TEST_UTIL.createTable(tableName, FAMILY, 4);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      for (int i = 0; i < 4; i++) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, now + i, VALUE);
        table.put(put);
      }

      Scan scan = new Scan();
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setFilter(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(QUALIFIER)));
      scan.readVersions(3);

      try (ResultScanner scanner = table.getScanner(scan)) {
        Result result = scanner.next();
        assertEquals(3, result.size());
      }
    }
  }

  @TestTemplate
  public void testScanWithSameStartRowStopRow() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE));

      Scan scan = new Scan().withStartRow(ROW).withStopRow(ROW);
      try (ResultScanner scanner = table.getScanner(scan)) {
        assertNull(scanner.next());
      }

      scan = new Scan().withStartRow(ROW, true).withStopRow(ROW, true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result result = scanner.next();
        assertNotNull(result);
        assertArrayEquals(ROW, result.getRow());
        assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
        assertNull(scanner.next());
      }

      scan = new Scan().withStartRow(ROW, true).withStopRow(ROW, false);
      try (ResultScanner scanner = table.getScanner(scan)) {
        assertNull(scanner.next());
      }

      scan = new Scan().withStartRow(ROW, false).withStopRow(ROW, false);
      try (ResultScanner scanner = table.getScanner(scan)) {
        assertNull(scanner.next());
      }

      scan = new Scan().withStartRow(ROW, false).withStopRow(ROW, true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        assertNull(scanner.next());
      }
    }
  }

  @TestTemplate
  public void testReverseScanWithFlush() throws Exception {
    final int BATCH_SIZE = 10;
    final int ROWS_TO_INSERT = 100;
    final byte[] LARGE_VALUE = generateHugeValue(128 * 1024);
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName);
      Admin admin = TEST_UTIL.getAdmin()) {
      List<Put> putList = new ArrayList<>();
      for (long i = 0; i < ROWS_TO_INSERT; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(FAMILY, QUALIFIER, LARGE_VALUE);
        putList.add(put);

        if (putList.size() >= BATCH_SIZE) {
          table.put(putList);
          admin.flush(tableName);
          putList.clear();
        }
      }

      if (!putList.isEmpty()) {
        table.put(putList);
        admin.flush(tableName);
        putList.clear();
      }

      Scan scan = new Scan();
      scan.setReversed(true);
      int count = 0;

      try (ResultScanner results = table.getScanner(scan)) {
        while (results.next() != null) {
          count++;
        }
      }
      assertEquals(ROWS_TO_INSERT, count,
        "Expected " + ROWS_TO_INSERT + " rows in the table but it is " + count);
    }
  }

  @TestTemplate
  public void testScannerWithPartialResults() throws Exception {
    TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("c"), 4);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      List<Put> puts = new ArrayList<>();
      byte[] largeArray = new byte[10000];
      Put put = new Put(Bytes.toBytes("aaaa0"));
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("1"), Bytes.toBytes("1"));
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("2"), Bytes.toBytes("2"));
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("3"), Bytes.toBytes("3"));
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("4"), Bytes.toBytes("4"));
      puts.add(put);
      put = new Put(Bytes.toBytes("aaaa1"));
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("1"), Bytes.toBytes("1"));
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("2"), largeArray);
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("3"), largeArray);
      puts.add(put);
      table.put(puts);
      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("c"));
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getName());
      scan.setMaxResultSize(10001);
      scan.withStopRow(Bytes.toBytes("bbbb"));
      scan.setFilter(new LimitKVsReturnFilter());
      try (ResultScanner rs = table.getScanner(scan)) {
        Result result;
        int expectedKvNumber = 6;
        int returnedKvNumber = 0;
        while ((result = rs.next()) != null) {
          returnedKvNumber += result.listCells().size();
        }
        assertEquals(expectedKvNumber, returnedKvNumber);
      }
    }
  }

  public static class LimitKVsReturnFilter extends FilterBase {

    private int cellCount = 0;

    @Override
    public ReturnCode filterCell(Cell v) throws IOException {
      if (cellCount >= 6) {
        cellCount++;
        return ReturnCode.SKIP;
      }
      cellCount++;
      return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterAllRemaining() throws IOException {
      if (cellCount < 7) {
        return false;
      }
      cellCount++;
      return true;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }

    public static LimitKVsReturnFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
      return new LimitKVsReturnFilter();
    }
  }
}
