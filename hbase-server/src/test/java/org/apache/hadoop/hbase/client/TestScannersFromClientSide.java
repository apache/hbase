/**
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
import static org.apache.hadoop.hbase.client.TestFromClientSide3.generateHugeValue;
import static org.apache.hadoop.hbase.ipc.RpcClient.DEFAULT_CODEC_CLASS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A client-side test, mostly testing scanners with various parameters. Parameterized on different
 * registry implementations.
 */
@Category({MediumTests.class, ClientTests.class})
@RunWith(Parameterized.class)
public class TestScannersFromClientSide {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestScannersFromClientSide.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestScannersFromClientSide.class);

  private static HBaseTestingUtility TEST_UTIL;
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  @Rule public TableNameTestRule name = new TableNameTestRule();

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
        { MasterRegistry.class, 1},
        { MasterRegistry.class, 2},
        { ZKConnectionRegistry.class, 1}
    });
  }

  /**
   * JUnit does not provide an easy way to run a hook after each parameterized run. Without that
   * there is no easy way to restart the test cluster after each parameterized run. Annotation
   * BeforeParam does not work either because it runs before parameterization and hence does not
   * have access to the test parameters (which is weird).
   *
   * This *hack* checks if the current instance of test cluster configuration has the passed
   * parameterized configs. In such a case, we can just reuse the cluster for test and do not need
   * to initialize from scratch. While this is a hack, it saves a ton of time for the full
   * test and de-flakes it.
   */
  private static boolean isSameParameterizedCluster(Class<?> registryImpl, int numHedgedReqs) {
    // initialize() is called for every unit test, however we only want to reset the cluster state
    // at the end of every parameterized run.
    if (TEST_UTIL == null) {
      return false;
    }
    Configuration conf = TEST_UTIL.getConfiguration();
    Class<?> confClass = conf.getClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      ZKConnectionRegistry.class);
    int hedgedReqConfig = conf.getInt(MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY,
      MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_DEFAULT);
    return confClass.getName().equals(registryImpl.getName()) && numHedgedReqs == hedgedReqConfig;
  }

  public TestScannersFromClientSide(Class<?> registryImpl, int numHedgedReqs) throws Exception {
    if (isSameParameterizedCluster(registryImpl, numHedgedReqs)) {
      return;
    }
    if (TEST_UTIL != null) {
      // We reached the end of a parameterized run, clean up the cluster.
      TEST_UTIL.shutdownMiniCluster();
    }
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, 10 * 1024 * 1024);
    conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, registryImpl,
        ConnectionRegistry.class);
    Preconditions.checkArgument(numHedgedReqs > 0);
    conf.setInt(MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY, numHedgedReqs);
    StartMiniClusterOption.Builder builder = StartMiniClusterOption.builder();
    // Multiple masters needed only when hedged reads for master registry are enabled.
    builder.numMasters(numHedgedReqs > 1 ? 3 : 1).numRegionServers(3);
    TEST_UTIL.startMiniCluster(builder.build());
  }

  /**
   * Test from client side for batch of scan
   *
   * @throws Exception
   */
  @Test
  public void testScanBatch() throws Exception {
    final TableName tableName = name.getTableName();
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 8);

    Table ht = TEST_UTIL.createTable(tableName, FAMILY);

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
    delete.addFamily(FAMILY, 3);
    ht.delete(delete);

    // without batch
    scan = new Scan().withStartRow(ROW);
    scan.setMaxVersions();
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
    scan =  new Scan().withStartRow(ROW);
    scan.setMaxVersions();
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

  @Test
  public void testMaxResultSizeIsSetToDefault() throws Exception {
    final TableName tableName = name.getTableName();
    Table ht = TEST_UTIL.createTable(tableName, FAMILY);

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

  /**
   * Scan on not existing table should throw the exception with correct message
   */
  @Test
  public void testScannerForNotExistingTable() {
    String[] tableNames = {"A", "Z", "A:A", "Z:Z"};
    for(String tableName : tableNames) {
      try {
        Table table = TEST_UTIL.getConnection().getTable(TableName.valueOf(tableName));
        testSmallScan(table, true, 1, 5);
        fail("TableNotFoundException was not thrown");
      } catch (TableNotFoundException e) {
        // We expect that the message for TableNotFoundException would have only the table name only
        // Otherwise that would mean that localeRegionInMeta doesn't work properly
        assertEquals(e.getMessage(), tableName);
      } catch (Exception e) {
        fail("Unexpected exception " + e.getMessage());
      }
    }
  }

  @Test
  public void testSmallScan() throws Exception {
    final TableName tableName = name.getTableName();

    int numRows = 10;
    byte[][] ROWS = HTestConst.makeNAscii(ROW, numRows);

    int numQualifiers = 10;
    byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, numQualifiers);

    Table ht = TEST_UTIL.createTable(tableName, FAMILY);

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

  /**
   * Run through a variety of test configurations with a small scan
   * @param table
   * @param reversed
   * @param rows
   * @param columns
   * @throws Exception
   */
  private void testSmallScan(
      Table table, boolean reversed, int rows, int columns) throws Exception {
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
    final TableName tableName = name.getTableName();
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    Table ht = TEST_UTIL.createTable(tableName, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<Cell> kvListExp;

    kvListExp = new ArrayList<>();
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
    kvListExp = new ArrayList<>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[0], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing basic setMaxResults");

    // Filters: ColumnRangeFilter
    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(5);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5],
                                        true));
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
    kvListExp = new ArrayList<>();
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
    kvListExp = new ArrayList<>();
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
    kvListExp = new ArrayList<>();
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
    final TableName tableName = name.getTableName();
    byte [][] ROWS = HTestConst.makeNAscii(ROW, 2);
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 10);

    Table ht = TEST_UTIL.createTable(tableName, FAMILIES);

    Put put;
    Scan scan;
    Result result;
    boolean toLog = true;
    List<Cell> kvListExp, kvListScan;

    kvListExp = new ArrayList<>();

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
    kvListScan = new ArrayList<>();
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
    final TableName tableName = name.getTableName();
    byte [][] FAMILIES = HTestConst.makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 20);

    Table ht = TEST_UTIL.createTable(tableName, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<Cell> kvListExp;

    // Insert one CF for row
    kvListExp = new ArrayList<>();
    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
      // skipping first two kvs
      if (i < 2) {
        continue;
      }
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
    kvListExp = new ArrayList<>();
    verifyResult(result, kvListExp, toLog, "Testing offset > #kvs");

    //offset + maxResultPerCF
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(4);
    get.setMaxResultsPerColumnFamily(5);
    result = ht.get(get);
    kvListExp = new ArrayList<>();
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
    kvListExp = new ArrayList<>();
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
    kvListExp = new ArrayList<>();
    //Exp: CF1:q4, q5, CF2: q4, q5
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[5], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog,
       "Testing offset + multiple CFs + maxResults");
  }

  @Test
  public void testScanRawDeleteFamilyVersion() throws Exception {
    TableName tableName = name.getTableName();
    TEST_UTIL.createTable(tableName, FAMILY);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(RPC_CODEC_CONF_KEY, "");
    conf.set(DEFAULT_CODEC_CLASS, "");
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(tableName)) {
      Delete delete = new Delete(ROW);
      delete.addFamilyVersion(FAMILY, 0L);
      table.delete(delete);
      Scan scan = new Scan(ROW).setRaw(true);
      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      while (scanner.next() != null) {
        count++;
      }
      assertEquals(1, count);
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  /**
   * Test from client side for scan while the region is reopened
   * on the same region server.
   *
   * @throws Exception
   */
  @Test
  public void testScanOnReopenedRegion() throws Exception {
    final TableName tableName = name.getTableName();
    byte [][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, 2);

    Table ht = TEST_UTIL.createTable(tableName, FAMILY);

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

    HRegionLocation loc;

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      loc = locator.getRegionLocation(ROW);
    }
    HRegionInfo hri = loc.getRegionInfo();
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
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
      assertTrue("Timed out in closing the testing region",
        EnvironmentEdgeManager.currentTime() < startTime + timeOut);
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
      assertTrue("Timed out in open the testing region",
        EnvironmentEdgeManager.currentTime() < startTime + timeOut);
    }
    assertFalse(offline);

    // c0:0, c1:1
    kvListExp = new ArrayList<>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[0], 0, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[1], 1, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing scan on re-opened region");
  }

  @Test
  public void testAsyncScannerWithSmallData() throws Exception {
    testAsyncScanner(name.getTableName(),
      2,
      3,
      10,
      -1,
      null);
  }

  @Test
  public void testAsyncScannerWithManyRows() throws Exception {
    testAsyncScanner(name.getTableName(),
      30000,
      1,
      1,
      -1,
      null);
  }

  @Test
  public void testAsyncScannerWithoutCaching() throws Exception {
    testAsyncScanner(name.getTableName(),
      5,
      1,
      1,
      1,
      (b) -> {
        try {
          TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException ex) {
        }
      });
  }

  private void testAsyncScanner(TableName table, int rowNumber, int familyNumber,
      int qualifierNumber, int caching, Consumer<Boolean> listener) throws Exception {
    assert rowNumber > 0;
    assert familyNumber > 0;
    assert qualifierNumber > 0;
    byte[] row = Bytes.toBytes("r");
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    byte[][] rows = makeNAsciiWithZeroPrefix(row, rowNumber);
    byte[][] families = makeNAsciiWithZeroPrefix(family, familyNumber);
    byte[][] qualifiers = makeNAsciiWithZeroPrefix(qualifier, qualifierNumber);

    Table ht = TEST_UTIL.createTable(table, families);

    boolean toLog = true;
    List<Cell> kvListExp = new ArrayList<>();

    List<Put> puts = new ArrayList<>();
    for (byte[] r : rows) {
      Put put = new Put(r);
      for (byte[] f : families) {
        for (byte[] q : qualifiers) {
          KeyValue kv = new KeyValue(r, f, q, 1, VALUE);
          put.add(kv);
          kvListExp.add(kv);
        }
      }
      puts.add(put);
      if (puts.size() > 1000) {
        ht.put(puts);
        puts.clear();
      }
    }
    if (!puts.isEmpty()) {
      ht.put(puts);
      puts.clear();
    }

    Scan scan = new Scan();
    scan.setAsyncPrefetch(true);
    if (caching > 0) {
      scan.setCaching(caching);
    }
    try (ResultScanner scanner = ht.getScanner(scan)) {
      assertTrue("Not instance of async scanner",scanner instanceof ClientAsyncPrefetchScanner);
      ((ClientAsyncPrefetchScanner) scanner).setPrefetchListener(listener);
      List<Cell> kvListScan = new ArrayList<>();
      Result result;
      boolean first = true;
      int actualRows = 0;
      while ((result = scanner.next()) != null) {
        ++actualRows;
        // waiting for cache. see HBASE-17376
        if (first) {
          TimeUnit.SECONDS.sleep(1);
          first = false;
        }
        for (Cell kv : result.listCells()) {
          kvListScan.add(kv);
        }
      }
      assertEquals(rowNumber, actualRows);
      // These cells may have different rows but it is ok. The Result#getRow
      // isn't used in the verifyResult()
      result = Result.create(kvListScan);
      verifyResult(result, kvListExp, toLog, "Testing async scan");
    }

    TEST_UTIL.deleteTable(table);
  }

  private static byte[][] makeNAsciiWithZeroPrefix(byte[] base, int n) {
    int maxLength = Integer.toString(n).length();
    byte [][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      int length = Integer.toString(i).length();
      StringBuilder buf = new StringBuilder(Integer.toString(i));
      IntStream.range(0, maxLength - length).forEach(v -> buf.insert(0, "0"));
      byte[] tail = Bytes.toBytes(buf.toString());
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

  static void verifyResult(Result result, List<Cell> expKvList, boolean toLog,
      String msg) {

    LOG.info(msg);
    LOG.info("Expected count: " + expKvList.size());
    LOG.info("Actual count: " + result.size());
    if (expKvList.isEmpty()) {
      return;
    }

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
    TableName tableName = name.getTableName();
    long ts = System.currentTimeMillis() - 10000;
    byte[] value = Bytes.toBytes("expired");
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, ts, value));
      assertArrayEquals(value, table.get(new Get(ROW)).getValue(FAMILY, QUALIFIER));
      TEST_UTIL.getAdmin().modifyColumnFamily(tableName,
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
  public void testScanWithColumnsAndFilterAndVersion() throws IOException {
    TableName tableName = name.getTableName();
    long now = System.currentTimeMillis();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY, 4)) {
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

  @Test
  public void testScanWithSameStartRowStopRow() throws IOException {
    TableName tableName = name.getTableName();
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
  public void testReverseScanWithFlush() throws Exception {
    TableName tableName = name.getTableName();
    final int BATCH_SIZE = 10;
    final int ROWS_TO_INSERT = 100;
    final byte[] LARGE_VALUE = generateHugeValue(128 * 1024);

    try (Table table = TEST_UTIL.createTable(tableName, FAMILY);
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
        for (Result result : results) {
          count++;
        }
      }
      assertEquals("Expected " + ROWS_TO_INSERT + " rows in the table but it is " + count,
        ROWS_TO_INSERT, count);
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

    public static LimitKVsReturnFilter parseFrom(final byte [] pbBytes)
        throws DeserializationException {
      return new LimitKVsReturnFilter();
    }
  }
}
