/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A client-side test, mostly testing scanners with various parameters.
 */
@Category(MediumTests.class)
public class TestScannersFromClientSide {
  private static final Log LOG = LogFactory.getLog(TestScannersFromClientSide.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, 10 * 1024 * 1024);
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  /**
   * Test from client side for batch of scan
   *
   * @throws Exception
   */
  @Test
  public void testScanBatch() throws Exception {
    TableName TABLE = TableName.valueOf("testScanBatch");
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 8);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);

    Put put;
    Scan scan;
    Delete delete;
    Result result;
    ResultScanner scanner;
    boolean toLog = true;
    List<Cell> kvListExp;

    // table: row, family, c0:0, c1:1, ... , c7:7
    put = new Put(ROW);
    for (int i=0; i < QUALIFIERS.length; i++) {
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
    delete.deleteFamily(FAMILY, 3);
    ht.delete(delete);

    // without batch
    scan = new Scan().withStartRow(ROW);
    scan.setMaxVersions();
    scanner = ht.getScanner(scan);

    // c4:4, c5:5, c6:6, c7:7
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing first batch of scan");

    // with batch
    scan =  new Scan().withStartRow(ROW);
    scan.setMaxVersions();
    scan.setBatch(2);
    scanner = ht.getScanner(scan);

    // First batch: c4:4, c5:5
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing first batch of scan");

    // Second batch: c6:6, c7:7
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing second batch of scan");

  }

  @Test
  public void testMaxResultSizeIsSetToDefault() throws Exception {
    TableName TABLE = TableName.valueOf("testMaxResultSizeIsSetToDefault");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);

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
    List<Put> puts = new ArrayList<Put>();
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

    ResultScanner scanner = ht.getScanner(scan);
    assertTrue(scanner instanceof ClientScanner);
    ClientScanner clientScanner = (ClientScanner) scanner;

    // Call next to issue a single RPC to the server
    scanner.next();

    // The scanner should have, at most, a single result in its cache. If there more results exists
    // in the cache it means that more than the expected max result size was fetched.
    assertTrue("The cache contains: " + clientScanner.getCacheSize() + " results",
      clientScanner.getCacheSize() <= 1);
  }

  @Test
  public void testSmallScan() throws Exception {
    TableName TABLE = TableName.valueOf("testSmallScan");

    int numRows = 10;
    byte[][] ROWS = HTestConst.makeNAscii(ROW, numRows);

    int numQualifiers = 10;
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, numQualifiers);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);

    Put put;
    List<Put> puts = new ArrayList<Put>();
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

  /**
   * Run through a variety of test configurations with a small scan
   * @param table
   * @param reversed
   * @param rows
   * @param columns
   * @throws Exception
   */
  private void testSmallScan(Table table, boolean reversed, int rows, int columns) throws Exception {
    Scan baseScan = new Scan();
    baseScan.setReversed(reversed);
    baseScan.setSmall(true);

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
    ResultScanner scanner = table.getScanner(scan);

    int rowCount = 0;
    int cellCount = 0;
    Result r = null;
    while ((r = scanner.next()) != null) {
      rowCount++;
      cellCount += r.rawCells().length;
    }

    assertTrue("Expected row count: " + expectedRowCount + " Actual row count: " + rowCount,
      expectedRowCount == rowCount);
    assertTrue("Expected cell count: " + expectedCellCount + " Actual cell count: " + cellCount,
      expectedCellCount == cellCount);
    scanner.close();
  }

  /**
   * Test from client side for get with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testGetMaxResults() throws Exception {
    byte [] TABLE = Bytes.toBytes("testGetMaxResults");
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<Cell> kvListExp;

    kvListExp = new ArrayList<Cell>();
    // Insert one CF for row[0]
    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
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
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[0], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing basic setMaxResults");

    // Filters: ColumnRangeFilter
    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(5);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5],
                                        true));
    result = ht.get(get);
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[2], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing single CF with CRF");

    // Insert two more CF for row[0]
    // 20 columns for CF2, 10 columns for CF1
    put = new Put(ROW);
    for (int i=0; i < QUALIFIERS.length; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
    }
    ht.put(put);

    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
    }
    ht.put(put);

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(12);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    result = ht.get(get);
    kvListExp = new ArrayList<Cell>();
    //Exp: CF1:q0, ..., q9, CF2: q0, q1, q10, q11, ..., q19
    for (int i=0; i < 10; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=0; i < 2; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
      }
    for (int i=10; i < 20; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs");

    // Filters: ColumnRangeFilter and ColumnPrefixFilter
    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(3);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, null, true));
    result = ht.get(get);
    kvListExp = new ArrayList<Cell>();
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs + CRF");

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(7);
    get.setFilter(new ColumnPrefixFilter(QUALIFIERS[1]));
    result = ht.get(get);
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[1], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[1], 1, VALUE));
    for (int i=10; i < 16; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs + PFF");

  }

  /**
   * Test from client side for scan with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testScanMaxResults() throws Exception {
    byte [] TABLE = Bytes.toBytes("testScanLimit");
    byte [][] ROWS = HTestConst.makeNAscii(ROW, 2);
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 10);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Put put;
    Scan scan;
    Result result;
    boolean toLog = true;
    List<Cell> kvListExp, kvListScan;

    kvListExp = new ArrayList<Cell>();

    for (int r=0; r < ROWS.length; r++) {
      put = new Put(ROWS[r]);
      for (int c=0; c < FAMILIES.length; c++) {
        for (int q=0; q < QUALIFIERS.length; q++) {
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
    kvListScan = new ArrayList<Cell>();
    while ((result = scanner.next()) != null) {
      for (Cell kv : result.listCells()) {
        kvListScan.add(kv);
      }
    }
    result = Result.create(kvListScan);
    verifyResult(result, kvListExp, toLog, "Testing scan with maxResults");

  }

  /**
   * Test from client side for get with rowOffset
   *
   * @throws Exception
   */
  @Test
  public void testGetRowOffset() throws Exception {
    byte [] TABLE = Bytes.toBytes("testGetRowOffset");
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<Cell> kvListExp;

    // Insert one CF for row
    kvListExp = new ArrayList<Cell>();
    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
      // skipping first two kvs
      if (i < 2) continue;
      kvListExp.add(kv);
    }
    ht.put(put);

    //setting offset to 2
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(2);
    result = ht.get(get);
    verifyResult(result, kvListExp, toLog, "Testing basic setRowOffset");

    //setting offset to 20
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(20);
    result = ht.get(get);
    kvListExp = new ArrayList<Cell>();
    verifyResult(result, kvListExp, toLog, "Testing offset > #kvs");

    //offset + maxResultPerCF
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(4);
    get.setMaxResultsPerColumnFamily(5);
    result = ht.get(get);
    kvListExp = new ArrayList<Cell>();
    for (int i=4; i < 9; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog,
      "Testing offset + setMaxResultsPerCF");

    // Filters: ColumnRangeFilter
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(1);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5],
                                        true));
    result = ht.get(get);
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing offset with CRF");

    // Insert into two more CFs for row
    // 10 columns for CF2, 10 columns for CF1
    for(int j=2; j > 0; j--) {
      put = new Put(ROW);
      for (int i=0; i < 10; i++) {
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
    kvListExp = new ArrayList<Cell>();
    //Exp: CF1:q4, q5, CF2: q4, q5
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[5], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog,
       "Testing offset + multiple CFs + maxResults");
  }

  /**
   * Test from client side for scan while the region is reopened
   * on the same region server.
   *
   * @throws Exception
   */
  @Test
  public void testScanOnReopenedRegion() throws Exception {
    TableName TABLE = TableName.valueOf("testScanOnReopenedRegion");
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 2);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);

    Put put;
    Scan scan;
    Result result;
    ResultScanner scanner;
    boolean toLog = false;
    List<Cell> kvListExp;

    // table: row, family, c0:0, c1:1
    put = new Put(ROW);
    for (int i=0; i < QUALIFIERS.length; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[i], i, VALUE);
      put.add(kv);
    }
    ht.put(put);

    scan = new Scan().withStartRow(ROW);
    scanner = ht.getScanner(scan);

    HRegionLocation loc = ht.getRegionLocation(ROW);
    HRegionInfo hri = loc.getRegionInfo();
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    byte[] regionName = hri.getRegionName();
    int i = cluster.getServerWith(regionName);
    HRegionServer rs = cluster.getRegionServer(i);
    ProtobufUtil.closeRegion(null,
      rs.getRSRpcServices(), rs.getServerName(), regionName, false);
    long startTime = EnvironmentEdgeManager.currentTime();
    long timeOut = 300000;
    while (true) {
      if (rs.getOnlineRegion(regionName) == null) {
        break;
      }
      assertTrue("Timed out in closing the testing region",
        EnvironmentEdgeManager.currentTime() < startTime + timeOut);
      Thread.sleep(500);
    }

    // Now open the region again.
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    try {
      HMaster master = cluster.getMaster();
      RegionStates states = master.getAssignmentManager().getRegionStates();
      states.regionOffline(hri);
      states.updateRegionState(hri, State.OPENING);
      if (ConfigUtil.useZKForAssignment(TEST_UTIL.getConfiguration())) {
        ZKAssign.createNodeOffline(zkw, hri, loc.getServerName());
      }
      ProtobufUtil.openRegion(null, rs.getRSRpcServices(), rs.getServerName(), hri);
      startTime = EnvironmentEdgeManager.currentTime();
      while (true) {
        if (rs.getOnlineRegion(regionName) != null) {
          break;
        }
        assertTrue("Timed out in open the testing region",
          EnvironmentEdgeManager.currentTime() < startTime + timeOut);
        Thread.sleep(500);
      }
    } finally {
      ZKAssign.deleteNodeFailSilent(zkw, hri);
    }

    // c0:0, c1:1
    kvListExp = new ArrayList<Cell>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[0], 0, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[1], 1, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing scan on re-opened region");
  }

  static void verifyResult(Result result, List<Cell> expKvList, boolean toLog,
      String msg) {

    LOG.info(msg);
    LOG.info("Expected count: " + expKvList.size());
    LOG.info("Actual count: " + result.size());
    if (expKvList.size() == 0)
      return;

    int i = 0;
    for (Cell kv : result.rawCells()) {
      if (i >= expKvList.size()) {
        break;  // we will check the size later
      }

      Cell kvExp = expKvList.get(i++);
      if (toLog) {
        LOG.info("get kv is: " + kv.toString());
        LOG.info("exp kv is: " + kvExp.toString());
      }
      assertTrue("Not equal", kvExp.equals(kv));
    }

    assertEquals(expKvList.size(), result.size());
  }

  @Test
  public void testReadExpiredDataForRawScan() throws IOException {
    TableName tableName = TableName.valueOf("testReadExpiredDataForRawScan");
    long ts = System.currentTimeMillis() - 10000;
    byte[] value = Bytes.toBytes("expired");
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, ts, value));
      assertArrayEquals(value, table.get(new Get(ROW)).getValue(FAMILY, QUALIFIER));
      TEST_UTIL.getHBaseAdmin().modifyColumn(tableName,
        new HColumnDescriptor(FAMILY).setTimeToLive(5));
      try (ResultScanner scanner = table.getScanner(FAMILY)) {
        assertNull(scanner.next());
      }
      try (ResultScanner scanner = table.getScanner(new Scan().setRaw(true))) {
        assertArrayEquals(value, scanner.next().getValue(FAMILY, QUALIFIER));
        assertNull(scanner.next());
      }
    }
  }

  @Test
  public void testScanWithSameStartRowStopRow() throws IOException {
    TableName tableName = TableName.valueOf("testScanWithSameStartRowStopRow");
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
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

  @Test
  public void testScannerWithPartialResults() throws Exception {
    TableName tableName = TableName.valueOf("testScannerWithPartialResults");
    try (Table table = TEST_UTIL.createMultiRegionTable(tableName,
      Bytes.toBytes("c"), 4)) {
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
      scan.setStopRow(Bytes.toBytes("bbbb"));
      scan.setFilter(new LimitKVsReturnFilter());
      ResultScanner rs = table.getScanner(scan);
      Result result;
      int expectedKvNumber = 6;
      int returnedKvNumber = 0;
      while((result = rs.next()) != null) {
        returnedKvNumber += result.listCells().size();
      }
      rs.close();
      assertEquals(expectedKvNumber, returnedKvNumber);
    }
  }

  public static class LimitKVsReturnFilter extends FilterBase {

    private int cellCount = 0;

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
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

    public static LimitKVsReturnFilter parseFrom(final byte [] pbBytes)
        throws DeserializationException {
      return new LimitKVsReturnFilter();
    }
  }
}
