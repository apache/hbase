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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run tests that use the HBase clients; {@link Table}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 *
 * Parameterized to run with different registry implementations.
 *
 * This class was split in three because it got too big when parameterized. Other classes
 * are below.
 *
 * @see TestFromClientSide4
 * @see TestFromClientSide5
 */
// NOTE: Increment tests were moved to their own class, TestIncrementsFromClientSide.
@Category({LargeTests.class, ClientTests.class})
@SuppressWarnings ("deprecation")
@RunWith(Parameterized.class)
public class TestFromClientSide extends FromClientSideBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestFromClientSide.class);

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFromClientSide.class);
  @Rule public TableNameTestRule name = new TableNameTestRule();

  // To keep the child classes happy.
  TestFromClientSide() {
  }

  public TestFromClientSide(Class registry, int numHedgedReqs) throws Exception {
    initialize(registry, numHedgedReqs, MultiRowMutationEndpoint.class);
  }

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { MasterRegistry.class, 1 }, { MasterRegistry.class, 2 },
      { ZKConnectionRegistry.class, 1 } });
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    afterClass();
  }

  /**
   * Test append result when there are duplicate rpc request.
   */
  @Test public void testDuplicateAppend() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor(name.getTableName());
    Map<String, String> kvs = new HashMap<>();
    kvs.put(HConnectionTestingUtility.SleepAtFirstRpcCall.SLEEP_TIME_CONF_KEY, "2000");
    hdt.addCoprocessor(HConnectionTestingUtility.SleepAtFirstRpcCall.class.getName(), null, 1, kvs);
    TEST_UTIL.createTable(hdt, new byte[][] { ROW }).close();

    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_PAUSE, 50);
    // Client will retry because rpc timeout is small than the sleep time of first rpc call
    c.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 1500);

    try (Connection connection = ConnectionFactory.createConnection(c)) {
      try (Table t = connection.getTable(name.getTableName())) {
        if (t instanceof HTable) {
          HTable table = (HTable) t;
          table.setOperationTimeout(3 * 1000);

          Append append = new Append(ROW);
          append.addColumn(HBaseTestingUtility.fam1, QUALIFIER, VALUE);
          Result result = table.append(append);

          // Verify expected result
          Cell[] cells = result.rawCells();
          assertEquals(1, cells.length);
          assertKey(cells[0], ROW, HBaseTestingUtility.fam1, QUALIFIER, VALUE);

          // Verify expected result again
          Result readResult = table.get(new Get(ROW));
          cells = readResult.rawCells();
          assertEquals(1, cells.length);
          assertKey(cells[0], ROW, HBaseTestingUtility.fam1, QUALIFIER, VALUE);
        }
      }
    }
  }

  /**
   * Test batch append result when there are duplicate rpc request.
   */
  @Test
  public void testDuplicateBatchAppend() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor(name.getTableName());
    Map<String, String> kvs = new HashMap<>();
    kvs.put(HConnectionTestingUtility.SleepAtFirstRpcCall.SLEEP_TIME_CONF_KEY, "2000");
    hdt.addCoprocessor(HConnectionTestingUtility.SleepAtFirstRpcCall.class.getName(), null, 1,
      kvs);
    TEST_UTIL.createTable(hdt, new byte[][] { ROW }).close();

    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_PAUSE, 50);
    // Client will retry because rpc timeout is small than the sleep time of first rpc call
    c.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 1500);

    try (Connection connection = ConnectionFactory.createConnection(c);
      Table table = connection.getTableBuilder(name.getTableName(), null).
        setOperationTimeout(3 * 1000).build()) {
      Append append = new Append(ROW);
      append.addColumn(HBaseTestingUtility.fam1, QUALIFIER, VALUE);

      // Batch append
      Object[] results = new Object[1];
      table.batch(Collections.singletonList(append), results);

      // Verify expected result
      Cell[] cells = ((Result) results[0]).rawCells();
      assertEquals(1, cells.length);
      assertKey(cells[0], ROW, HBaseTestingUtility.fam1, QUALIFIER, VALUE);

      // Verify expected result again
      Result readResult = table.get(new Get(ROW));
      cells = readResult.rawCells();
      assertEquals(1, cells.length);
      assertKey(cells[0], ROW, HBaseTestingUtility.fam1, QUALIFIER, VALUE);
    }
  }

  /**
   * Basic client side validation of HBASE-4536
   */
  @Test public void testKeepDeletedCells() throws Exception {
    final TableName tableName = name.getTableName();
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] C0 = Bytes.toBytes("c0");

    final byte[] T1 = Bytes.toBytes("T1");
    final byte[] T2 = Bytes.toBytes("T2");
    final byte[] T3 = Bytes.toBytes("T3");
    HColumnDescriptor hcd =
      new HColumnDescriptor(FAMILY).setKeepDeletedCells(KeepDeletedCells.TRUE).setMaxVersions(3);

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(hcd);
    TEST_UTIL.getAdmin().createTable(desc);
    try (Table h = TEST_UTIL.getConnection().getTable(tableName)) {
      long ts = System.currentTimeMillis();
      Put p = new Put(T1, ts);
      p.addColumn(FAMILY, C0, T1);
      h.put(p);
      p = new Put(T1, ts + 2);
      p.addColumn(FAMILY, C0, T2);
      h.put(p);
      p = new Put(T1, ts + 4);
      p.addColumn(FAMILY, C0, T3);
      h.put(p);

      Delete d = new Delete(T1, ts + 3);
      h.delete(d);

      d = new Delete(T1, ts + 3);
      d.addColumns(FAMILY, C0, ts + 3);
      h.delete(d);

      Get g = new Get(T1);
      // does *not* include the delete
      g.setTimeRange(0, ts + 3);
      Result r = h.get(g);
      assertArrayEquals(T2, r.getValue(FAMILY, C0));

      Scan s = new Scan(T1);
      s.setTimeRange(0, ts + 3);
      s.setMaxVersions();
      try (ResultScanner scanner = h.getScanner(s)) {
        Cell[] kvs = scanner.next().rawCells();
        assertArrayEquals(T2, CellUtil.cloneValue(kvs[0]));
        assertArrayEquals(T1, CellUtil.cloneValue(kvs[1]));
      }

      s = new Scan(T1);
      s.setRaw(true);
      s.setMaxVersions();
      try (ResultScanner scanner = h.getScanner(s)) {
        Cell[] kvs = scanner.next().rawCells();
        assertTrue(PrivateCellUtil.isDeleteFamily(kvs[0]));
        assertArrayEquals(T3, CellUtil.cloneValue(kvs[1]));
        assertTrue(CellUtil.isDelete(kvs[2]));
        assertArrayEquals(T2, CellUtil.cloneValue(kvs[3]));
        assertArrayEquals(T1, CellUtil.cloneValue(kvs[4]));
      }
    }
  }

  /**
   * Basic client side validation of HBASE-10118
   */
  @Test public void testPurgeFutureDeletes() throws Exception {
    final TableName tableName = name.getTableName();
    final byte[] ROW = Bytes.toBytes("row");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN = Bytes.toBytes("column");
    final byte[] VALUE = Bytes.toBytes("value");

    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      // future timestamp
      long ts = System.currentTimeMillis() * 2;
      Put put = new Put(ROW, ts);
      put.addColumn(FAMILY, COLUMN, VALUE);
      table.put(put);

      Get get = new Get(ROW);
      Result result = table.get(get);
      assertArrayEquals(VALUE, result.getValue(FAMILY, COLUMN));

      Delete del = new Delete(ROW);
      del.addColumn(FAMILY, COLUMN, ts);
      table.delete(del);

      get = new Get(ROW);
      result = table.get(get);
      assertNull(result.getValue(FAMILY, COLUMN));

      // major compaction, purged future deletes
      TEST_UTIL.getAdmin().flush(tableName);
      TEST_UTIL.getAdmin().majorCompact(tableName);

      // waiting for the major compaction to complete
      TEST_UTIL.waitFor(6000,
        () -> TEST_UTIL.getAdmin().getCompactionState(tableName) == CompactionState.NONE);

      put = new Put(ROW, ts);
      put.addColumn(FAMILY, COLUMN, VALUE);
      table.put(put);

      get = new Get(ROW);
      result = table.get(get);
      assertArrayEquals(VALUE, result.getValue(FAMILY, COLUMN));
    }
  }

  /**
   * Verifies that getConfiguration returns the same Configuration object used
   * to create the HTable instance.
   */
  @Test public void testGetConfiguration() throws Exception {
    final TableName tableName = name.getTableName();
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    Configuration conf = TEST_UTIL.getConfiguration();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILIES)) {
      assertSame(conf, table.getConfiguration());
    }
  }

  /**
   * Test from client side of an involved filter against a multi family that
   * involves deletes.
   */
  @Test public void testWeirdCacheBehaviour() throws Exception {
    final TableName tableName = name.getTableName();
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("trans-blob"), Bytes.toBytes("trans-type"),
      Bytes.toBytes("trans-date"), Bytes.toBytes("trans-tags"), Bytes.toBytes("trans-group") };
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES)) {
      String value = "this is the value";
      String value2 = "this is some other value";
      String keyPrefix1 = HBaseTestingUtility.getRandomUUID().toString();
      String keyPrefix2 = HBaseTestingUtility.getRandomUUID().toString();
      String keyPrefix3 = HBaseTestingUtility.getRandomUUID().toString();
      putRows(ht, 3, value, keyPrefix1);
      putRows(ht, 3, value, keyPrefix2);
      putRows(ht, 3, value, keyPrefix3);
      putRows(ht, 3, value2, keyPrefix1);
      putRows(ht, 3, value2, keyPrefix2);
      putRows(ht, 3, value2, keyPrefix3);
      try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
        System.out.println("Checking values for key: " + keyPrefix1);
        assertEquals("Got back incorrect number of rows from scan", 3,
          getNumberOfRows(keyPrefix1, value2, table));
        System.out.println("Checking values for key: " + keyPrefix2);
        assertEquals("Got back incorrect number of rows from scan", 3,
          getNumberOfRows(keyPrefix2, value2, table));
        System.out.println("Checking values for key: " + keyPrefix3);
        assertEquals("Got back incorrect number of rows from scan", 3,
          getNumberOfRows(keyPrefix3, value2, table));
        deleteColumns(ht, value2, keyPrefix1);
        deleteColumns(ht, value2, keyPrefix2);
        deleteColumns(ht, value2, keyPrefix3);
        System.out.println("Starting important checks.....");
        assertEquals("Got back incorrect number of rows from scan: " + keyPrefix1, 0,
          getNumberOfRows(keyPrefix1, value2, table));
        assertEquals("Got back incorrect number of rows from scan: " + keyPrefix2, 0,
          getNumberOfRows(keyPrefix2, value2, table));
        assertEquals("Got back incorrect number of rows from scan: " + keyPrefix3, 0,
          getNumberOfRows(keyPrefix3, value2, table));
      }
    }
  }

  /**
   * Test filters when multiple regions.  It does counts.  Needs eye-balling of
   * logs to ensure that we're not scanning more regions that we're supposed to.
   * Related to the TestFilterAcrossRegions over in the o.a.h.h.filter package.
   */
  @Test public void testFilterAcrossMultipleRegions() throws IOException {
    final TableName tableName = name.getTableName();
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      int rowCount = TEST_UTIL.loadTable(t, FAMILY, false);
      assertRowCount(t, rowCount);
      // Split the table.  Should split on a reasonable key; 'lqj'
      List<HRegionLocation> regions = splitTable(t);
      assertRowCount(t, rowCount);
      // Get end key of first region.
      byte[] endKey = regions.get(0).getRegionInfo().getEndKey();
      // Count rows with a filter that stops us before passed 'endKey'.
      // Should be count of rows in first region.
      int endKeyCount = TEST_UTIL.countRows(t, createScanWithRowFilter(endKey));
      assertTrue(endKeyCount < rowCount);

      // How do I know I did not got to second region?  Thats tough.  Can't really
      // do that in client-side region test.  I verified by tracing in debugger.
      // I changed the messages that come out when set to DEBUG so should see
      // when scanner is done. Says "Finished with scanning..." with region name.
      // Check that its finished in right region.

      // New test.  Make it so scan goes into next region by one and then two.
      // Make sure count comes out right.
      byte[] key = new byte[] { endKey[0], endKey[1], (byte) (endKey[2] + 1) };
      int plusOneCount = TEST_UTIL.countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount + 1, plusOneCount);
      key = new byte[] { endKey[0], endKey[1], (byte) (endKey[2] + 2) };
      int plusTwoCount = TEST_UTIL.countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount + 2, plusTwoCount);

      // New test.  Make it so I scan one less than endkey.
      key = new byte[] { endKey[0], endKey[1], (byte) (endKey[2] - 1) };
      int minusOneCount = TEST_UTIL.countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount - 1, minusOneCount);
      // For above test... study logs.  Make sure we do "Finished with scanning.."
      // in first region and that we do not fall into the next region.

      key = new byte[] { 'a', 'a', 'a' };
      int countBBB = TEST_UTIL.countRows(t, createScanWithRowFilter(key, null,
        CompareOperator.EQUAL));
      assertEquals(1, countBBB);

      int countGreater = TEST_UTIL.countRows(t, createScanWithRowFilter(endKey, null,
        CompareOperator.GREATER_OR_EQUAL));
      // Because started at start of table.
      assertEquals(0, countGreater);
      countGreater = TEST_UTIL.countRows(t, createScanWithRowFilter(endKey, endKey,
        CompareOperator.GREATER_OR_EQUAL));
      assertEquals(rowCount - endKeyCount, countGreater);
    }
  }

  @Test public void testSuperSimple() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      Scan scan = new Scan();
      scan.addColumn(FAMILY, tableName.toBytes());
      try (ResultScanner scanner = ht.getScanner(scan)) {
        Result result = scanner.next();
        assertNull("Expected null result", result);
      }
    }
  }

  @Test public void testMaxKeyValueSize() throws Exception {
    final TableName tableName = name.getTableName();
    Configuration conf = TEST_UTIL.getConfiguration();
    String oldMaxSize = conf.get(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY);
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[] value = new byte[4 * 1024 * 1024];
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, value);
      ht.put(put);

      try {
        TEST_UTIL.getConfiguration()
          .setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, 2 * 1024 * 1024);
        // Create new table so we pick up the change in Configuration.
        try (Connection connection =
            ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
          try (Table t = connection.getTable(TableName.valueOf(FAMILY))) {
            put = new Put(ROW);
            put.addColumn(FAMILY, QUALIFIER, value);
            t.put(put);
          }
        }
        fail("Inserting a too large KeyValue worked, should throw exception");
      } catch (Exception ignored) {
      }
    }
    conf.set(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, oldMaxSize);
  }

  @Test public void testFilters() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS =
        { Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
          Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
          Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
          Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
          Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>") };
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.addFamily(FAMILY);
      Filter filter = new QualifierFilter(CompareOperator.EQUAL,
        new RegexStringComparator("col[1-5]"));
      scan.setFilter(filter);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int expectedIndex = 1;
        for (Result result : scanner) {
          assertEquals(1, result.size());
          assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[expectedIndex]));
          assertTrue(Bytes.equals(CellUtil.cloneQualifier(result.rawCells()[0]),
            QUALIFIERS[expectedIndex]));
          expectedIndex++;
        }
        assertEquals(6, expectedIndex);
      }
    }
  }

  @Test public void testFilterWithLongCompartor() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] values = new byte[10][];
      for (int i = 0; i < 10; i++) {
        values[i] = Bytes.toBytes(100L * i);
      }
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(FAMILY, QUALIFIER, values[i]);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.addFamily(FAMILY);
      Filter filter = new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.GREATER,
        new LongComparator(500));
      scan.setFilter(filter);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int expectedIndex = 0;
        for (Result result : scanner) {
          assertEquals(1, result.size());
          assertTrue(Bytes.toLong(result.getValue(FAMILY, QUALIFIER)) > 500);
          expectedIndex++;
        }
        assertEquals(4, expectedIndex);
      }
    }
  }

  @Test public void testKeyOnlyFilter() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS =
        { Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
          Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
          Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
          Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
          Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>") };
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.addFamily(FAMILY);
      Filter filter = new KeyOnlyFilter(true);
      scan.setFilter(filter);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int count = 0;
        for (Result result : scanner) {
          assertEquals(1, result.size());
          assertEquals(Bytes.SIZEOF_INT, result.rawCells()[0].getValueLength());
          assertEquals(VALUE.length, Bytes.toInt(CellUtil.cloneValue(result.rawCells()[0])));
          count++;
        }
        assertEquals(10, count);
      }
    }
  }

  /**
   * Test simple table and non-existent row cases.
   */
  @Test public void testSimpleMissing() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 4);

      // Try to get a row on an empty table
      Get get = new Get(ROWS[0]);
      Result result = ht.get(get);
      assertEmptyResult(result);

      get = new Get(ROWS[0]);
      get.addFamily(FAMILY);
      result = ht.get(get);
      assertEmptyResult(result);

      get = new Get(ROWS[0]);
      get.addColumn(FAMILY, QUALIFIER);
      result = ht.get(get);
      assertEmptyResult(result);

      Scan scan = new Scan();
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan(ROWS[0]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan(ROWS[0], ROWS[1]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.addFamily(FAMILY);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.addColumn(FAMILY, QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Insert a row

      Put put = new Put(ROWS[2]);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);

      // Try to get empty rows around it

      get = new Get(ROWS[1]);
      result = ht.get(get);
      assertEmptyResult(result);

      get = new Get(ROWS[0]);
      get.addFamily(FAMILY);
      result = ht.get(get);
      assertEmptyResult(result);

      get = new Get(ROWS[3]);
      get.addColumn(FAMILY, QUALIFIER);
      result = ht.get(get);
      assertEmptyResult(result);

      // Try to scan empty rows around it

      scan = new Scan(ROWS[3]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan(ROWS[0], ROWS[2]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Make sure we can actually get the row

      get = new Get(ROWS[2]);
      result = ht.get(get);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      get = new Get(ROWS[2]);
      get.addFamily(FAMILY);
      result = ht.get(get);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      get = new Get(ROWS[2]);
      get.addColumn(FAMILY, QUALIFIER);
      result = ht.get(get);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      // Make sure we can scan the row

      scan = new Scan();
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan(ROWS[0], ROWS[3]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan(ROWS[2], ROWS[3]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
    }
  }

  /**
   * Test basic puts, gets, scans, and deletes for a single row
   * in a multiple family table.
   */
  @SuppressWarnings("checkstyle:MethodLength") @Test public void testSingleRowMultipleFamily()
    throws Exception {
    final TableName tableName = name.getTableName();
    byte[][] ROWS = makeN(ROW, 3);
    byte[][] FAMILIES = makeNAscii(FAMILY, 10);
    byte[][] QUALIFIERS = makeN(QUALIFIER, 10);
    byte[][] VALUES = makeN(VALUE, 10);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES)) {
      ////////////////////////////////////////////////////////////////////////////
      // Insert one column to one family
      ////////////////////////////////////////////////////////////////////////////

      Put put = new Put(ROWS[0]);
      put.addColumn(FAMILIES[4], QUALIFIERS[0], VALUES[0]);
      ht.put(put);

      // Get the single column
      getVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);

      // Scan the single column
      scanVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);

      // Get empty results around inserted column
      getVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);

      // Scan empty results around inserted column
      scanVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);

      ////////////////////////////////////////////////////////////////////////////
      // Flush memstore and run same tests from storefiles
      ////////////////////////////////////////////////////////////////////////////

      TEST_UTIL.flush();

      // Redo get and scan tests from storefile
      getVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
      scanVerifySingleColumn(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0, VALUES, 0);
      getVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);
      scanVerifySingleEmpty(ht, ROWS, 0, FAMILIES, 4, QUALIFIERS, 0);

      ////////////////////////////////////////////////////////////////////////////
      // Now, Test reading from memstore and storefiles at once
      ////////////////////////////////////////////////////////////////////////////

      // Insert multiple columns to two other families
      put = new Put(ROWS[0]);
      put.addColumn(FAMILIES[2], QUALIFIERS[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIERS[4], VALUES[4]);
      put.addColumn(FAMILIES[4], QUALIFIERS[4], VALUES[4]);
      put.addColumn(FAMILIES[6], QUALIFIERS[6], VALUES[6]);
      put.addColumn(FAMILIES[6], QUALIFIERS[7], VALUES[7]);
      put.addColumn(FAMILIES[7], QUALIFIERS[7], VALUES[7]);
      put.addColumn(FAMILIES[9], QUALIFIERS[0], VALUES[0]);
      ht.put(put);

      // Get multiple columns across multiple families and get empties around it
      singleRowGetTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);

      // Scan multiple columns across multiple families and scan empties around it
      singleRowScanTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);

      ////////////////////////////////////////////////////////////////////////////
      // Flush the table again
      ////////////////////////////////////////////////////////////////////////////

      TEST_UTIL.flush();

      // Redo tests again
      singleRowGetTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);
      singleRowScanTest(ht, ROWS, FAMILIES, QUALIFIERS, VALUES);

      // Insert more data to memstore
      put = new Put(ROWS[0]);
      put.addColumn(FAMILIES[6], QUALIFIERS[5], VALUES[5]);
      put.addColumn(FAMILIES[6], QUALIFIERS[8], VALUES[8]);
      put.addColumn(FAMILIES[6], QUALIFIERS[9], VALUES[9]);
      put.addColumn(FAMILIES[4], QUALIFIERS[3], VALUES[3]);
      ht.put(put);

      ////////////////////////////////////////////////////////////////////////////
      // Delete a storefile column
      ////////////////////////////////////////////////////////////////////////////
      Delete delete = new Delete(ROWS[0]);
      delete.addColumns(FAMILIES[6], QUALIFIERS[7]);
      ht.delete(delete);

      // Try to get deleted column
      Get get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[7]);
      Result result = ht.get(get);
      assertEmptyResult(result);

      // Try to scan deleted column
      Scan scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[7]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Make sure we can still get a column before it and after it
      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[6]);
      result = ht.get(get);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[8]);
      result = ht.get(get);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[8], VALUES[8]);

      // Make sure we can still scan a column before it and after it
      scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

      scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[8]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[8], VALUES[8]);

      ////////////////////////////////////////////////////////////////////////////
      // Delete a memstore column
      ////////////////////////////////////////////////////////////////////////////
      delete = new Delete(ROWS[0]);
      delete.addColumns(FAMILIES[6], QUALIFIERS[8]);
      ht.delete(delete);

      // Try to get deleted column
      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[8]);
      result = ht.get(get);
      assertEmptyResult(result);

      // Try to scan deleted column
      scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[8]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Make sure we can still get a column before it and after it
      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[6]);
      result = ht.get(get);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[9]);
      result = ht.get(get);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

      // Make sure we can still scan a column before it and after it
      scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

      scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

      ////////////////////////////////////////////////////////////////////////////
      // Delete joint storefile/memstore family
      ////////////////////////////////////////////////////////////////////////////

      delete = new Delete(ROWS[0]);
      delete.addFamily(FAMILIES[4]);
      ht.delete(delete);

      // Try to get storefile column in deleted family
      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[4], QUALIFIERS[4]);
      result = ht.get(get);
      assertEmptyResult(result);

      // Try to get memstore column in deleted family
      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[4], QUALIFIERS[3]);
      result = ht.get(get);
      assertEmptyResult(result);

      // Try to get deleted family
      get = new Get(ROWS[0]);
      get.addFamily(FAMILIES[4]);
      result = ht.get(get);
      assertEmptyResult(result);

      // Try to scan storefile column in deleted family
      scan = new Scan();
      scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Try to scan memstore column in deleted family
      scan = new Scan();
      scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Try to scan deleted family
      scan = new Scan();
      scan.addFamily(FAMILIES[4]);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Make sure we can still get another family
      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[2], QUALIFIERS[2]);
      result = ht.get(get);
      assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[9]);
      result = ht.get(get);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

      // Make sure we can still scan another family
      scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

      scan = new Scan();
      scan.addColumn(FAMILIES[6], QUALIFIERS[9]);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);

      ////////////////////////////////////////////////////////////////////////////
      // Flush everything and rerun delete tests
      ////////////////////////////////////////////////////////////////////////////

      TEST_UTIL.flush();

      // Try to get storefile column in deleted family
      assertEmptyResult(ht.get(new Get(ROWS[0]).addColumn(FAMILIES[4], QUALIFIERS[4])));

      // Try to get memstore column in deleted family
      assertEmptyResult(ht.get(new Get(ROWS[0]).addColumn(FAMILIES[4], QUALIFIERS[3])));

      // Try to get deleted family
      assertEmptyResult(ht.get(new Get(ROWS[0]).addFamily(FAMILIES[4])));

      // Try to scan storefile column in deleted family
      assertNullResult(getSingleScanResult(ht, new Scan().addColumn(FAMILIES[4], QUALIFIERS[4])));

      // Try to scan memstore column in deleted family
      assertNullResult(getSingleScanResult(ht, new Scan().addColumn(FAMILIES[4], QUALIFIERS[3])));

      // Try to scan deleted family
      assertNullResult(getSingleScanResult(ht, new Scan().addFamily(FAMILIES[4])));

      // Make sure we can still get another family
      assertSingleResult(ht.get(new Get(ROWS[0]).addColumn(FAMILIES[2], QUALIFIERS[2])), ROWS[0],
        FAMILIES[2], QUALIFIERS[2], VALUES[2]);

      assertSingleResult(ht.get(new Get(ROWS[0]).addColumn(FAMILIES[6], QUALIFIERS[9])), ROWS[0],
        FAMILIES[6], QUALIFIERS[9], VALUES[9]);

      // Make sure we can still scan another family
      assertSingleResult(getSingleScanResult(ht, new Scan().addColumn(FAMILIES[6], QUALIFIERS[6])),
        ROWS[0], FAMILIES[6], QUALIFIERS[6], VALUES[6]);

      assertSingleResult(getSingleScanResult(ht, new Scan().addColumn(FAMILIES[6], QUALIFIERS[9])),
        ROWS[0], FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    }
  }

  @Test(expected = NullPointerException.class) public void testNullTableName() throws IOException {
    // Null table name (should NOT work)
    TEST_UTIL.createTable(null, FAMILY);
    fail("Creating a table with null name passed, should have failed");
  }

  @Test(expected = IllegalArgumentException.class) public void testNullFamilyName()
    throws IOException {
    final TableName tableName = name.getTableName();

    // Null family (should NOT work)
    TEST_UTIL.createTable(tableName, new byte[][] { null });
    fail("Creating a table with a null family passed, should fail");
  }

  @Test public void testNullRowAndQualifier() throws Exception {
    final TableName tableName = name.getTableName();

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Null row (should NOT work)
      try {
        Put put = new Put((byte[]) null);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        ht.put(put);
        fail("Inserting a null row worked, should throw exception");
      } catch (Exception ignored) {
      }

      // Null qualifier (should work)
      {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, null, VALUE);
        ht.put(put);

        getTestNull(ht, ROW, FAMILY, VALUE);

        scanTestNull(ht, ROW, FAMILY, VALUE);

        Delete delete = new Delete(ROW);
        delete.addColumns(FAMILY, null);
        ht.delete(delete);

        Get get = new Get(ROW);
        Result result = ht.get(get);
        assertEmptyResult(result);
      }
    }
  }

  @Test public void testNullEmptyQualifier() throws Exception {
    final TableName tableName = name.getTableName();

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Empty qualifier, byte[0] instead of null (should work)
      try {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, HConstants.EMPTY_BYTE_ARRAY, VALUE);
        ht.put(put);

        getTestNull(ht, ROW, FAMILY, VALUE);

        scanTestNull(ht, ROW, FAMILY, VALUE);

        // Flush and try again

        TEST_UTIL.flush();

        getTestNull(ht, ROW, FAMILY, VALUE);

        scanTestNull(ht, ROW, FAMILY, VALUE);

        Delete delete = new Delete(ROW);
        delete.addColumns(FAMILY, HConstants.EMPTY_BYTE_ARRAY);
        ht.delete(delete);

        Get get = new Get(ROW);
        Result result = ht.get(get);
        assertEmptyResult(result);

      } catch (Exception e) {
        throw new IOException("Using a row with null qualifier should not throw exception");
      }
    }
  }

  @Test public void testNullValue() throws IOException {
    final TableName tableName = name.getTableName();

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Null value
      try {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, null);
        ht.put(put);

        Get get = new Get(ROW);
        get.addColumn(FAMILY, QUALIFIER);
        Result result = ht.get(get);
        assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);

        Scan scan = new Scan();
        scan.addColumn(FAMILY, QUALIFIER);
        result = getSingleScanResult(ht, scan);
        assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);

        Delete delete = new Delete(ROW);
        delete.addColumns(FAMILY, QUALIFIER);
        ht.delete(delete);

        get = new Get(ROW);
        result = ht.get(get);
        assertEmptyResult(result);

      } catch (Exception e) {
        throw new IOException("Null values should be allowed, but threw exception");
      }
    }
  }

  @Test public void testNullQualifier() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Work for Put
      Put put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      table.put(put);

      // Work for Get, Scan
      getTestNull(table, ROW, FAMILY, VALUE);
      scanTestNull(table, ROW, FAMILY, VALUE);

      // Work for Delete
      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, null);
      table.delete(delete);

      Get get = new Get(ROW);
      Result result = table.get(get);
      assertEmptyResult(result);

      // Work for Increment/Append
      Increment increment = new Increment(ROW);
      increment.addColumn(FAMILY, null, 1L);
      table.increment(increment);
      getTestNull(table, ROW, FAMILY, 1L);

      table.incrementColumnValue(ROW, FAMILY, null, 1L);
      getTestNull(table, ROW, FAMILY, 2L);

      delete = new Delete(ROW);
      delete.addColumns(FAMILY, null);
      table.delete(delete);

      Append append = new Append(ROW);
      append.addColumn(FAMILY, null, VALUE);
      table.append(append);
      getTestNull(table, ROW, FAMILY, VALUE);

      // Work for checkAndMutate using thenPut, thenMutate and thenDelete
      put = new Put(ROW);
      put.addColumn(FAMILY, null, Bytes.toBytes("checkAndPut"));
      table.put(put);
      table.checkAndMutate(ROW, FAMILY).ifEquals(VALUE).thenPut(put);

      RowMutations mutate = new RowMutations(ROW);
      mutate.add(new Put(ROW).addColumn(FAMILY, null, Bytes.toBytes("checkAndMutate")));
      table.checkAndMutate(ROW, FAMILY).ifEquals(Bytes.toBytes("checkAndPut")).thenMutate(mutate);

      delete = new Delete(ROW);
      delete.addColumns(FAMILY, null);
      table.checkAndMutate(ROW, FAMILY).
        ifEquals(Bytes.toBytes("checkAndMutate")).thenDelete(delete);
    }
  }

  @Test @SuppressWarnings("checkstyle:MethodLength") public void testVersions() throws Exception {
    final TableName tableName = name.getTableName();

    long[] STAMPS = makeStamps(20);
    byte[][] VALUES = makeNAscii(VALUE, 20);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 10)) {

      // Insert 4 versions of same column
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      // Verify we can get each one properly
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

      // Verify we don't accidentally get others
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

      // Ensure maxVersions in query is respected
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions(2);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      Scan scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      // Flush and redo

      TEST_UTIL.flush();

      // Verify we can get each one properly
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);

      // Verify we don't accidentally get others
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[3]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[6]);

      // Ensure maxVersions in query is respected
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions(2);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[] { STAMPS[4], STAMPS[5] },
        new byte[][] { VALUES[4], VALUES[5] }, 0, 1);

      // Add some memstore and retest

      // Insert 4 more versions of same column and a dupe
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
      ht.put(put);

      // Ensure maxVersions in query is respected
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions();
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
          STAMPS[8] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] }, 0, 7);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions();
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
          STAMPS[8] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] }, 0, 7);

      get = new Get(ROW);
      get.setMaxVersions();
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
          STAMPS[8] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] }, 0, 7);

      scan = new Scan(ROW);
      scan.setMaxVersions();
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
          STAMPS[8] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
          VALUES[8] }, 0, 7);

      // Verify we can get each one properly
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);

      // Verify we don't accidentally get others
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[0]);
      scanVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[9]);

      // Ensure maxVersions of table is respected

      TEST_UTIL.flush();

      // Insert 4 more versions of same column and a dupe
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
      ht.put(put);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
          STAMPS[11], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9],
          VALUES[11], VALUES[13], VALUES[15] }, 0, 9);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
          STAMPS[11], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9],
          VALUES[11], VALUES[13], VALUES[15] }, 0, 9);

      // Delete a version in the memstore and a version in a storefile
      Delete delete = new Delete(ROW);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[11]);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[7]);
      ht.delete(delete);

      // Test that it's gone
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
          STAMPS[9], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8],
          VALUES[9], VALUES[13], VALUES[15] }, 0, 9);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
          STAMPS[9], STAMPS[13], STAMPS[15] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8],
          VALUES[9], VALUES[13], VALUES[15] }, 0, 9);
    }
  }

  @Test @SuppressWarnings("checkstyle:MethodLength") public void testVersionLimits()
    throws Exception {
    final TableName tableName = name.getTableName();
    byte[][] FAMILIES = makeNAscii(FAMILY, 3);
    int[] LIMITS = { 1, 3, 5 };
    long[] STAMPS = makeStamps(10);
    byte[][] VALUES = makeNAscii(VALUE, 10);
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES, LIMITS)) {

      // Insert limit + 1 on each family
      Put put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
      put.addColumn(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
      ht.put(put);

      // Verify we only get the right number out of each

      // Family0

      Get get = new Get(ROW);
      get.addColumn(FAMILIES[0], QUALIFIER);
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { STAMPS[1] },
        new byte[][] { VALUES[1] }, 0, 0);

      get = new Get(ROW);
      get.addFamily(FAMILIES[0]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { STAMPS[1] },
        new byte[][] { VALUES[1] }, 0, 0);

      Scan scan = new Scan(ROW);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { STAMPS[1] },
        new byte[][] { VALUES[1] }, 0, 0);

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { STAMPS[1] },
        new byte[][] { VALUES[1] }, 0, 0);

      // Family1

      get = new Get(ROW);
      get.addColumn(FAMILIES[1], QUALIFIER);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      get = new Get(ROW);
      get.addFamily(FAMILIES[1]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      scan = new Scan(ROW);
      scan.addColumn(FAMILIES[1], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[1]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long[] { STAMPS[1], STAMPS[2], STAMPS[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      // Family2

      get = new Get(ROW);
      get.addColumn(FAMILIES[2], QUALIFIER);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6] }, 0, 4);

      get = new Get(ROW);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6] }, 0, 4);

      scan = new Scan(ROW);
      scan.addColumn(FAMILIES[2], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6] }, 0, 4);

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long[] { STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6] },
        new byte[][] { VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6] }, 0, 4);

      // Try all families

      get = new Get(ROW);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 9 keys but received " + result.size(), 9, result.size());

      get = new Get(ROW);
      get.addFamily(FAMILIES[0]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 9 keys but received " + result.size(), 9, result.size());

      get = new Get(ROW);
      get.addColumn(FAMILIES[0], QUALIFIER);
      get.addColumn(FAMILIES[1], QUALIFIER);
      get.addColumn(FAMILIES[2], QUALIFIER);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 9 keys but received " + result.size(), 9, result.size());

      scan = new Scan(ROW);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals("Expected 9 keys but received " + result.size(), 9, result.size());

      scan = new Scan(ROW);
      scan.setMaxVersions(Integer.MAX_VALUE);
      scan.addFamily(FAMILIES[0]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      result = getSingleScanResult(ht, scan);
      assertEquals("Expected 9 keys but received " + result.size(), 9, result.size());

      scan = new Scan(ROW);
      scan.setMaxVersions(Integer.MAX_VALUE);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.addColumn(FAMILIES[1], QUALIFIER);
      scan.addColumn(FAMILIES[2], QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertEquals("Expected 9 keys but received " + result.size(), 9, result.size());
    }
  }

  @Test public void testDeleteFamilyVersion() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      final TableName tableName = name.getTableName();

      byte[][] QUALIFIERS = makeNAscii(QUALIFIER, 1);
      byte[][] VALUES = makeN(VALUE, 5);
      long[] ts = { 1000, 2000, 3000, 4000, 5000 };

      try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 5)) {

        Put put = new Put(ROW);
        for (int q = 0; q < 1; q++) {
          for (int t = 0; t < 5; t++) {
            put.addColumn(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
          }
        }
        ht.put(put);
        admin.flush(tableName);

        Delete delete = new Delete(ROW);
        delete.addFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
        delete.addFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
        ht.delete(delete);
        admin.flush(tableName);

        for (int i = 0; i < 1; i++) {
          Get get = new Get(ROW);
          get.addColumn(FAMILY, QUALIFIERS[i]);
          get.setMaxVersions(Integer.MAX_VALUE);
          Result result = ht.get(get);
          // verify version '1000'/'3000'/'5000' remains for all columns
          assertNResult(result, ROW, FAMILY, QUALIFIERS[i], new long[] { ts[0], ts[2], ts[4] },
            new byte[][] { VALUES[0], VALUES[2], VALUES[4] }, 0, 2);
        }
      }
    }
  }

  @Test public void testDeleteFamilyVersionWithOtherDeletes() throws Exception {
    final TableName tableName = name.getTableName();

    byte[][] QUALIFIERS = makeNAscii(QUALIFIER, 5);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };

    try (Admin admin = TEST_UTIL.getAdmin(); Table ht = TEST_UTIL.createTable(tableName, FAMILY,
      5)) {
      Put put;
      Result result = null;
      Get get = null;
      Delete delete = null;

      // 1. put on ROW
      put = new Put(ROW);
      for (int q = 0; q < 5; q++) {
        for (int t = 0; t < 5; t++) {
          put.addColumn(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
        }
      }
      ht.put(put);
      admin.flush(tableName);

      // 2. put on ROWS[0]
      byte[] ROW2 = Bytes.toBytes("myRowForTest");
      put = new Put(ROW2);
      for (int q = 0; q < 5; q++) {
        for (int t = 0; t < 5; t++) {
          put.addColumn(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
        }
      }
      ht.put(put);
      admin.flush(tableName);

      // 3. delete on ROW
      delete = new Delete(ROW);
      // delete version <= 2000 of all columns
      // note: addFamily must be the first since it will mask
      // the subsequent other type deletes!
      delete.addFamily(FAMILY, ts[1]);
      // delete version '4000' of all columns
      delete.addFamilyVersion(FAMILY, ts[3]);
      // delete version <= 3000 of column 0
      delete.addColumns(FAMILY, QUALIFIERS[0], ts[2]);
      // delete version <= 5000 of column 2
      delete.addColumns(FAMILY, QUALIFIERS[2], ts[4]);
      // delete version 5000 of column 4
      delete.addColumn(FAMILY, QUALIFIERS[4], ts[4]);
      ht.delete(delete);
      admin.flush(tableName);

      // 4. delete on ROWS[0]
      delete = new Delete(ROW2);
      delete.addFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
      delete.addFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
      ht.delete(delete);
      admin.flush(tableName);

      // 5. check ROW
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[0]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[0], new long[] { ts[4] },
        new byte[][] { VALUES[4] }, 0, 0);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[1]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[1], new long[] { ts[2], ts[4] },
        new byte[][] { VALUES[2], VALUES[4] }, 0, 1);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals(0, result.size());

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[3]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[3], new long[] { ts[2], ts[4] },
        new byte[][] { VALUES[2], VALUES[4] }, 0, 1);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[4]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[4], new long[] { ts[2] },
        new byte[][] { VALUES[2] }, 0, 0);

      // 6. check ROWS[0]
      for (int i = 0; i < 5; i++) {
        get = new Get(ROW2);
        get.addColumn(FAMILY, QUALIFIERS[i]);
        get.readVersions(Integer.MAX_VALUE);
        result = ht.get(get);
        // verify version '1000'/'3000'/'5000' remains for all columns
        assertNResult(result, ROW2, FAMILY, QUALIFIERS[i], new long[] { ts[0], ts[2], ts[4] },
          new byte[][] { VALUES[0], VALUES[2], VALUES[4] }, 0, 2);
      }
    }
  }

  @Test public void testDeleteWithFailed() throws Exception {
    final TableName tableName = name.getTableName();

    byte[][] FAMILIES = makeNAscii(FAMILY, 3);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES, 3)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
      ht.put(put);

      // delete wrong family
      Delete delete = new Delete(ROW);
      delete.addFamily(FAMILIES[1], ts[0]);
      ht.delete(delete);

      Get get = new Get(ROW);
      get.addFamily(FAMILIES[0]);
      get.readAllVersions();
      Result result = ht.get(get);
      assertTrue(Bytes.equals(result.getValue(FAMILIES[0], QUALIFIER), VALUES[0]));
    }
  }

  @Test @SuppressWarnings("checkstyle:MethodLength") public void testDeletes() throws Exception {
    final TableName tableName = name.getTableName();

    byte[][] ROWS = makeNAscii(ROW, 6);
    byte[][] FAMILIES = makeNAscii(FAMILY, 3);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES, 3)) {

      Put put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
      ht.put(put);

      Delete delete = new Delete(ROW);
      delete.addFamily(FAMILIES[0], ts[0]);
      ht.delete(delete);

      Get get = new Get(ROW);
      get.addFamily(FAMILIES[0]);
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1] },
        new byte[][] { VALUES[1] }, 0, 0);

      Scan scan = new Scan(ROW);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1] },
        new byte[][] { VALUES[1] }, 0, 0);

      // Test delete latest version
      put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
      put.addColumn(FAMILIES[0], null, ts[4], VALUES[4]);
      put.addColumn(FAMILIES[0], null, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[0], null, ts[3], VALUES[3]);
      ht.put(put);

      delete = new Delete(ROW);
      delete.addColumn(FAMILIES[0], QUALIFIER); // ts[4]
      ht.delete(delete);

      get = new Get(ROW);
      get.addColumn(FAMILIES[0], QUALIFIER);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1], ts[2], ts[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      scan = new Scan(ROW);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1], ts[2], ts[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      // Test for HBASE-1847
      delete = new Delete(ROW);
      delete.addColumn(FAMILIES[0], null);
      ht.delete(delete);

      // Cleanup null qualifier
      delete = new Delete(ROW);
      delete.addColumns(FAMILIES[0], null);
      ht.delete(delete);

      // Expected client behavior might be that you can re-put deleted values
      // But alas, this is not to be.  We can't put them back in either case.

      put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
      put.addColumn(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
      ht.put(put);

      // It used to be due to the internal implementation of Get, that
      // the Get() call would return ts[4] UNLIKE the Scan below. With
      // the switch to using Scan for Get this is no longer the case.
      get = new Get(ROW);
      get.addFamily(FAMILIES[0]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1], ts[2], ts[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      // The Scanner returns the previous values, the expected-naive-unexpected behavior

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1], ts[2], ts[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      // Test deleting an entire family from one row but not the other various ways

      put = new Put(ROWS[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      put = new Put(ROWS[1]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      put = new Put(ROWS[2]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      // Assert that above went in.
      get = new Get(ROWS[2]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 4 key but received " + result.size() + ": " + result, 4,
        result.size());

      delete = new Delete(ROWS[0]);
      delete.addFamily(FAMILIES[2]);
      ht.delete(delete);

      delete = new Delete(ROWS[1]);
      delete.addColumns(FAMILIES[1], QUALIFIER);
      ht.delete(delete);

      delete = new Delete(ROWS[2]);
      delete.addColumn(FAMILIES[1], QUALIFIER);
      delete.addColumn(FAMILIES[1], QUALIFIER);
      delete.addColumn(FAMILIES[2], QUALIFIER);
      ht.delete(delete);

      get = new Get(ROWS[0]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());
      assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, new long[] { ts[0], ts[1] },
        new byte[][] { VALUES[0], VALUES[1] }, 0, 1);

      scan = new Scan(ROWS[0]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());
      assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, new long[] { ts[0], ts[1] },
        new byte[][] { VALUES[0], VALUES[1] }, 0, 1);

      get = new Get(ROWS[1]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());

      scan = new Scan(ROWS[1]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());

      get = new Get(ROWS[2]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals(1, result.size());
      assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER, new long[] { ts[2] },
        new byte[][] { VALUES[2] }, 0, 0);

      scan = new Scan(ROWS[2]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals(1, result.size());
      assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER, new long[] { ts[2] },
        new byte[][] { VALUES[2] }, 0, 0);

      // Test if we delete the family first in one row (HBASE-1541)

      delete = new Delete(ROWS[3]);
      delete.addFamily(FAMILIES[1]);
      ht.delete(delete);

      put = new Put(ROWS[3]);
      put.addColumn(FAMILIES[2], QUALIFIER, VALUES[0]);
      ht.put(put);

      put = new Put(ROWS[4]);
      put.addColumn(FAMILIES[1], QUALIFIER, VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, VALUES[2]);
      ht.put(put);

      get = new Get(ROWS[3]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 1 key but received " + result.size(), 1, result.size());

      get = new Get(ROWS[4]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());

      scan = new Scan(ROWS[3]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        result = scanner.next();
        assertEquals("Expected 1 key but received " + result.size(), 1, result.size());
        assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
        assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
        result = scanner.next();
        assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());
        assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
        assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
        assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
        assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
      }

      // Add test of bulk deleting.
      for (int i = 0; i < 10; i++) {
        byte[] bytes = Bytes.toBytes(i);
        put = new Put(bytes);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(FAMILIES[0], QUALIFIER, bytes);
        ht.put(put);
      }
      for (int i = 0; i < 10; i++) {
        byte[] bytes = Bytes.toBytes(i);
        get = new Get(bytes);
        get.addFamily(FAMILIES[0]);
        result = ht.get(get);
        assertEquals(1, result.size());
      }
      ArrayList<Delete> deletes = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        byte[] bytes = Bytes.toBytes(i);
        delete = new Delete(bytes);
        delete.addFamily(FAMILIES[0]);
        deletes.add(delete);
      }
      ht.delete(deletes);
      for (int i = 0; i < 10; i++) {
        byte[] bytes = Bytes.toBytes(i);
        get = new Get(bytes);
        get.addFamily(FAMILIES[0]);
        result = ht.get(get);
        assertTrue(result.isEmpty());
      }
    }
  }
}

