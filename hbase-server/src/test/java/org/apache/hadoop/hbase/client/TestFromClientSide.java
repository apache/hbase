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

import static org.apache.hadoop.hbase.HBaseTestingUtility.countRows;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.NonRepeatedEnvironmentEdge;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run tests that use the HBase clients; {@link Table}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 */
@Category({LargeTests.class, ClientTests.class})
@SuppressWarnings ("deprecation")
public class TestFromClientSide {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFromClientSide.class);

  // NOTE: Increment tests were moved to their own class, TestIncrementsFromClientSide.
  private static final Logger LOG = LoggerFactory.getLogger(TestFromClientSide.class);
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] INVALID_FAMILY = Bytes.toBytes("invalidTestFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  protected static int SLAVES = 3;

  @Rule
  public TestName name = new TestName();

  protected static final void initialize(Class<?>... cps) throws Exception {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    // ((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    // make sure that we do not get the same ts twice, see HBASE-19731 for more details.
    EnvironmentEdgeManager.injectEdge(new NonRepeatedEnvironmentEdge());
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      Arrays.stream(cps).map(Class::getName).toArray(String[]::new));
    conf.setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, true); // enable for below tests
    // We need more than one region server in this test
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initialize(MultiRowMutationEndpoint.class);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test append result when there are duplicate rpc request.
   */
  @Test
  public void testDuplicateAppend() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor(name.getMethodName());
    Map<String, String> kvs = new HashMap<>();
    kvs.put(SleepAtFirstRpcCall.SLEEP_TIME_CONF_KEY, "2000");
    hdt.addCoprocessor(SleepAtFirstRpcCall.class.getName(), null, 1, kvs);
    TEST_UTIL.createTable(hdt, new byte[][] { ROW }).close();

    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_PAUSE, 50);
    // Client will retry beacuse rpc timeout is small than the sleep time of first rpc call
    c.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 1500);

    try (Connection connection = ConnectionFactory.createConnection(c);
        Table table = connection.getTableBuilder(TableName.valueOf(name.getMethodName()), null)
          .setOperationTimeout(3 * 1000).build()) {
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

  /**
   * Basic client side validation of HBASE-4536
   */
  @Test
  public void testKeepDeletedCells() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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
      ResultScanner scanner = h.getScanner(s);
      Cell[] kvs = scanner.next().rawCells();
      assertArrayEquals(T2, CellUtil.cloneValue(kvs[0]));
      assertArrayEquals(T1, CellUtil.cloneValue(kvs[1]));
      scanner.close();

      s = new Scan(T1);
      s.setRaw(true);
      s.setMaxVersions();
      scanner = h.getScanner(s);
      kvs = scanner.next().rawCells();
      assertTrue(PrivateCellUtil.isDeleteFamily(kvs[0]));
      assertArrayEquals(T3, CellUtil.cloneValue(kvs[1]));
      assertTrue(CellUtil.isDelete(kvs[2]));
      assertArrayEquals(T2, CellUtil.cloneValue(kvs[3]));
      assertArrayEquals(T1, CellUtil.cloneValue(kvs[4]));
      scanner.close();
    }
  }

  /**
   * Basic client side validation of HBASE-10118
   */
  @Test
  public void testPurgeFutureDeletes() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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
      TEST_UTIL.waitFor(6000, new Waiter.Predicate<IOException>() {
        @Override
        public boolean evaluate() throws IOException {
          return TEST_UTIL.getAdmin().getCompactionState(tableName) == CompactionState.NONE;
        }
      });

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
  @Test
  public void testGetConfiguration() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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
  @Test
  public void testWeirdCacheBehaviour() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] FAMILIES = new byte[][] { Bytes.toBytes("trans-blob"),
        Bytes.toBytes("trans-type"), Bytes.toBytes("trans-date"),
        Bytes.toBytes("trans-tags"), Bytes.toBytes("trans-group") };
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES)) {
      String value = "this is the value";
      String value2 = "this is some other value";
      String keyPrefix1 = TEST_UTIL.getRandomUUID().toString();
      String keyPrefix2 = TEST_UTIL.getRandomUUID().toString();
      String keyPrefix3 = TEST_UTIL.getRandomUUID().toString();
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
        assertEquals("Got back incorrect number of rows from scan: " + keyPrefix1,
                0, getNumberOfRows(keyPrefix1, value2, table));
        assertEquals("Got back incorrect number of rows from scan: " + keyPrefix2,
                0, getNumberOfRows(keyPrefix2, value2, table));
        assertEquals("Got back incorrect number of rows from scan: " + keyPrefix3,
                0, getNumberOfRows(keyPrefix3, value2, table));
      }
    }
  }

  private void deleteColumns(Table ht, String value, String keyPrefix)
  throws IOException {
    ResultScanner scanner = buildScanner(keyPrefix, value, ht);
    Iterator<Result> it = scanner.iterator();
    int count = 0;
    while (it.hasNext()) {
      Result result = it.next();
      Delete delete = new Delete(result.getRow());
      delete.addColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"));
      ht.delete(delete);
      count++;
    }
    assertEquals("Did not perform correct number of deletes", 3, count);
  }

  private int getNumberOfRows(String keyPrefix, String value, Table ht)
      throws Exception {
    ResultScanner resultScanner = buildScanner(keyPrefix, value, ht);
    Iterator<Result> scanner = resultScanner.iterator();
    int numberOfResults = 0;
    while (scanner.hasNext()) {
      Result result = scanner.next();
      System.out.println("Got back key: " + Bytes.toString(result.getRow()));
      for (Cell kv : result.rawCells()) {
        System.out.println("kv=" + kv.toString() + ", "
            + Bytes.toString(CellUtil.cloneValue(kv)));
      }
      numberOfResults++;
    }
    return numberOfResults;
  }

  private ResultScanner buildScanner(String keyPrefix, String value, Table ht)
      throws IOException {
    // OurFilterList allFilters = new OurFilterList();
    FilterList allFilters = new FilterList(/* FilterList.Operator.MUST_PASS_ALL */);
    allFilters.addFilter(new PrefixFilter(Bytes.toBytes(keyPrefix)));
    SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes
        .toBytes("trans-tags"), Bytes.toBytes("qual2"), CompareOperator.EQUAL, Bytes
        .toBytes(value));
    filter.setFilterIfMissing(true);
    allFilters.addFilter(filter);

    // allFilters.addFilter(new
    // RowExcludingSingleColumnValueFilter(Bytes.toBytes("trans-tags"),
    // Bytes.toBytes("qual2"), CompareOp.EQUAL, Bytes.toBytes(value)));

    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("trans-blob"));
    scan.addFamily(Bytes.toBytes("trans-type"));
    scan.addFamily(Bytes.toBytes("trans-date"));
    scan.addFamily(Bytes.toBytes("trans-tags"));
    scan.addFamily(Bytes.toBytes("trans-group"));
    scan.setFilter(allFilters);

    return ht.getScanner(scan);
  }

  private void putRows(Table ht, int numRows, String value, String key)
      throws IOException {
    for (int i = 0; i < numRows; i++) {
      String row = key + "_" + TEST_UTIL.getRandomUUID().toString();
      System.out.println(String.format("Saving row: %s, with value %s", row,
          value));
      Put put = new Put(Bytes.toBytes(row));
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(Bytes.toBytes("trans-blob"), null, Bytes
          .toBytes("value for blob"));
      put.addColumn(Bytes.toBytes("trans-type"), null, Bytes.toBytes("statement"));
      put.addColumn(Bytes.toBytes("trans-date"), null, Bytes
          .toBytes("20090921010101999"));
      put.addColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"), Bytes
          .toBytes(value));
      put.addColumn(Bytes.toBytes("trans-group"), null, Bytes
          .toBytes("adhocTransactionGroupId"));
      ht.put(put);
    }
  }

  /**
   * Test filters when multiple regions.  It does counts.  Needs eye-balling of
   * logs to ensure that we're not scanning more regions that we're supposed to.
   * Related to the TestFilterAcrossRegions over in the o.a.h.h.filter package.
   */
  @Test
  public void testFilterAcrossMultipleRegions()
  throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      int rowCount = TEST_UTIL.loadTable(t, FAMILY, false);
      assertRowCount(t, rowCount);
      // Split the table.  Should split on a reasonable key; 'lqj'
      List<HRegionLocation> regions = splitTable(t);
      assertRowCount(t, rowCount);
      // Get end key of first region.
      byte[] endKey = regions.get(0).getRegion().getEndKey();
      // Count rows with a filter that stops us before passed 'endKey'.
      // Should be count of rows in first region.
      int endKeyCount = countRows(t, createScanWithRowFilter(endKey));
      assertTrue(endKeyCount < rowCount);

      // How do I know I did not got to second region?  Thats tough.  Can't really
      // do that in client-side region test.  I verified by tracing in debugger.
      // I changed the messages that come out when set to DEBUG so should see
      // when scanner is done. Says "Finished with scanning..." with region name.
      // Check that its finished in right region.

      // New test.  Make it so scan goes into next region by one and then two.
      // Make sure count comes out right.
      byte[] key = new byte[]{endKey[0], endKey[1], (byte) (endKey[2] + 1)};
      int plusOneCount = countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount + 1, plusOneCount);
      key = new byte[]{endKey[0], endKey[1], (byte) (endKey[2] + 2)};
      int plusTwoCount = countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount + 2, plusTwoCount);

      // New test.  Make it so I scan one less than endkey.
      key = new byte[]{endKey[0], endKey[1], (byte) (endKey[2] - 1)};
      int minusOneCount = countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount - 1, minusOneCount);
      // For above test... study logs.  Make sure we do "Finished with scanning.."
      // in first region and that we do not fall into the next region.

      key = new byte[] { 'a', 'a', 'a' };
      int countBBB = countRows(t, createScanWithRowFilter(key, null, CompareOperator.EQUAL));
      assertEquals(1, countBBB);

      int countGreater =
        countRows(t, createScanWithRowFilter(endKey, null, CompareOperator.GREATER_OR_EQUAL));
      // Because started at start of table.
      assertEquals(0, countGreater);
      countGreater =
        countRows(t, createScanWithRowFilter(endKey, endKey, CompareOperator.GREATER_OR_EQUAL));
      assertEquals(rowCount - endKeyCount, countGreater);
    }
  }

  /*
   * @param key
   * @return Scan with RowFilter that does LESS than passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key) {
    return createScanWithRowFilter(key, null, CompareOperator.LESS);
  }

  /*
   * @param key
   * @param op
   * @param startRow
   * @return Scan with RowFilter that does CompareOp op on passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key,
      final byte [] startRow, CompareOperator op) {
    // Make sure key is of some substance... non-null and > than first key.
    assertTrue(key != null && key.length > 0 &&
      Bytes.BYTES_COMPARATOR.compare(key, new byte [] {'a', 'a', 'a'}) >= 0);
    LOG.info("Key=" + Bytes.toString(key));
    Scan s = startRow == null? new Scan(): new Scan(startRow);
    Filter f = new RowFilter(op, new BinaryComparator(key));
    f = new WhileMatchFilter(f);
    s.setFilter(f);
    return s;
  }

  private void assertRowCount(final Table t, final int expected) throws IOException {
    assertEquals(expected, countRows(t, new Scan()));
  }

  /**
   * Split table into multiple regions.
   * @param t Table to split.
   * @return Map of regions to servers.
   */
  private List<HRegionLocation> splitTable(final Table t)
  throws IOException, InterruptedException {
    // Split this table in two.
    Admin admin = TEST_UTIL.getAdmin();
    admin.split(t.getName());
    admin.close();
    List<HRegionLocation> regions = waitOnSplit(t);
    assertTrue(regions.size() > 1);
    return regions;
  }

  /*
   * Wait on table split.  May return because we waited long enough on the split
   * and it didn't happen.  Caller should check.
   * @param t
   * @return Map of table regions; caller needs to check table actually split.
   */
  private List<HRegionLocation> waitOnSplit(final Table t)
  throws IOException {
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(t.getName())) {
      List<HRegionLocation> regions = locator.getAllRegionLocations();
      int originalCount = regions.size();
      for (int i = 0; i < TEST_UTIL.getConfiguration().getInt("hbase.test.retries", 30); i++) {
        Thread.currentThread();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        regions = locator.getAllRegionLocations();
        if (regions.size() > originalCount)
          break;
      }
      return regions;
    }
  }

  @Test
  public void testSuperSimple() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      Scan scan = new Scan();
      scan.addColumn(FAMILY, tableName.toBytes());
      ResultScanner scanner = ht.getScanner(scan);
      Result result = scanner.next();
      assertTrue("Expected null result", result == null);
      scanner.close();
    }
  }

  @Test
  public void testMaxKeyValueSize() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Configuration conf = TEST_UTIL.getConfiguration();
    String oldMaxSize = conf.get(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY);
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[] value = new byte[4 * 1024 * 1024];
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, value);
      ht.put(put);

      try {
        TEST_UTIL.getConfiguration().setInt(
                ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, 2 * 1024 * 1024);
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
      } catch (Exception e) {
      }
    }
    conf.set(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, oldMaxSize);
  }

  @Test
  public void testFilters() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS = {
              Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
              Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
              Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
              Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
              Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")
      };
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

  @Test
  public void testFilterWithLongCompartor() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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

  @Test
  public void testKeyOnlyFilter() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS = {
              Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
              Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
              Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
              Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
              Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")
      };
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
  @Test
  public void testSimpleMissing() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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
  @Test
  public void testSingleRowMultipleFamily() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] ROWS = makeN(ROW, 3);
    byte [][] FAMILIES = makeNAscii(FAMILY, 10);
    byte [][] QUALIFIERS = makeN(QUALIFIER, 10);
    byte [][] VALUES = makeN(VALUE, 10);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES)) {

      Get get;
      Scan scan;
      Delete delete;
      Put put;
      Result result;

      ////////////////////////////////////////////////////////////////////////////
      // Insert one column to one family
      ////////////////////////////////////////////////////////////////////////////

      put = new Put(ROWS[0]);
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
      delete = new Delete(ROWS[0]);
      delete.addColumns(FAMILIES[6], QUALIFIERS[7]);
      ht.delete(delete);

      // Try to get deleted column
      get = new Get(ROWS[0]);
      get.addColumn(FAMILIES[6], QUALIFIERS[7]);
      result = ht.get(get);
      assertEmptyResult(result);

      // Try to scan deleted column
      scan = new Scan();
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
    }
  }

  @Test(expected = NullPointerException.class)
  public void testNullTableName() throws IOException {
    // Null table name (should NOT work)
    TEST_UTIL.createTable((TableName)null, FAMILY);
    fail("Creating a table with null name passed, should have failed");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullFamilyName() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    // Null family (should NOT work)
    TEST_UTIL.createTable(tableName, new byte[][]{null});
    fail("Creating a table with a null family passed, should fail");
  }

  @Test
  public void testNullRowAndQualifier() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Null row (should NOT work)
      try {
        Put put = new Put((byte[]) null);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        ht.put(put);
        fail("Inserting a null row worked, should throw exception");
      } catch (Exception e) {
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

  @Test
  public void testNullEmptyQualifier() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

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

  @Test
  public void testNullValue() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());

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

  @Test
  public void testNullQualifier() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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
      table.checkAndMutate(ROW, FAMILY).ifEquals(Bytes.toBytes("checkAndMutate"))
              .thenDelete(delete);
    }
  }

  @Test
  public void testVersions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

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
      get.readVersions(2);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[4], STAMPS[5]},
          new byte[][] {VALUES[4], VALUES[5]},
          0, 1);

      Scan scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
              new long[]{STAMPS[4], STAMPS[5]},
              new byte[][]{VALUES[4], VALUES[5]},
              0, 1);

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
      get.readVersions(2);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[4], STAMPS[5]},
          new byte[][] {VALUES[4], VALUES[5]},
          0, 1);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
              new long[]{STAMPS[4], STAMPS[5]},
              new byte[][]{VALUES[4], VALUES[5]},
              0, 1);


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
      get.readAllVersions();
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
                  STAMPS[8]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
                  VALUES[8]},
          0, 7);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions();
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long[]{STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
                  STAMPS[8]},
          new byte[][]{VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
                  VALUES[8]},0, 7);

      get = new Get(ROW);
      get.readAllVersions();
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
                  STAMPS[8]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
                  VALUES[8]},
          0, 7);

      scan = new Scan(ROW);
      scan.setMaxVersions();
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long[]{STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7],
                  STAMPS[8]},
          new byte[][]{VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7],
                  VALUES[8]},0, 7);

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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
                  STAMPS[11], STAMPS[13], STAMPS[15]},
          new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9],
                  VALUES[11], VALUES[13], VALUES[15]},
          0, 9);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long[]{STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
                  STAMPS[11], STAMPS[13], STAMPS[15]},
          new byte[][]{VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9],
                  VALUES[11], VALUES[13], VALUES[15]},0, 9);

      // Delete a version in the memstore and a version in a storefile
      Delete delete = new Delete(ROW);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[11]);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[7]);
      ht.delete(delete);

      // Test that it's gone
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
                  STAMPS[9], STAMPS[13], STAMPS[15]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8],
                  VALUES[9], VALUES[13], VALUES[15]},
          0, 9);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long[]{STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
                  STAMPS[9], STAMPS[13], STAMPS[15]},
          new byte[][]{VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8],
                  VALUES[9], VALUES[13], VALUES[15]},0, 9);
    }
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  public void testVersionLimits() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
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
      get.readVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
          new long [] {STAMPS[1]},
          new byte[][] {VALUES[1]},
          0, 0);

      get = new Get(ROW);
      get.addFamily(FAMILIES[0]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
          new long [] {STAMPS[1]},
          new byte[][] {VALUES[1]},
          0, 0);

      Scan scan = new Scan(ROW);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
              new long[]{STAMPS[1]},
              new byte[][]{VALUES[1]},
              0, 0);

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
              new long[]{STAMPS[1]},
              new byte[][]{VALUES[1]},
              0, 0);

      // Family1

      get = new Get(ROW);
      get.addColumn(FAMILIES[1], QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
          new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
          0, 2);

      get = new Get(ROW);
      get.addFamily(FAMILIES[1]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
          new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
          0, 2);

      scan = new Scan(ROW);
      scan.addColumn(FAMILIES[1], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
              new long[]{STAMPS[1], STAMPS[2], STAMPS[3]},
              new byte[][]{VALUES[1], VALUES[2], VALUES[3]},
              0, 2);

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[1]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
              new long[]{STAMPS[1], STAMPS[2], STAMPS[3]},
              new byte[][]{VALUES[1], VALUES[2], VALUES[3]},
              0, 2);

      // Family2

      get = new Get(ROW);
      get.addColumn(FAMILIES[2], QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
          new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
          new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
          0, 4);

      get = new Get(ROW);
      get.addFamily(FAMILIES[2]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
          new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
          new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
          0, 4);

      scan = new Scan(ROW);
      scan.addColumn(FAMILIES[2], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
              new long[]{STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
              new byte[][]{VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
              0, 4);

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
              new long[]{STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
              new byte[][]{VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
              0, 4);

      // Try all families

      get = new Get(ROW);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 9 keys but received " + result.size(),
          result.size() == 9);

      get = new Get(ROW);
      get.addFamily(FAMILIES[0]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 9 keys but received " + result.size(),
          result.size() == 9);

      get = new Get(ROW);
      get.addColumn(FAMILIES[0], QUALIFIER);
      get.addColumn(FAMILIES[1], QUALIFIER);
      get.addColumn(FAMILIES[2], QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 9 keys but received " + result.size(),
          result.size() == 9);

      scan = new Scan(ROW);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertTrue("Expected 9 keys but received " + result.size(),
              result.size() == 9);

      scan = new Scan(ROW);
      scan.setMaxVersions(Integer.MAX_VALUE);
      scan.addFamily(FAMILIES[0]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      result = getSingleScanResult(ht, scan);
      assertTrue("Expected 9 keys but received " + result.size(),
              result.size() == 9);

      scan = new Scan(ROW);
      scan.setMaxVersions(Integer.MAX_VALUE);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.addColumn(FAMILIES[1], QUALIFIER);
      scan.addColumn(FAMILIES[2], QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertTrue("Expected 9 keys but received " + result.size(),
              result.size() == 9);
    }
  }

  @Test
  public void testDeleteFamilyVersion() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      final TableName tableName = TableName.valueOf(name.getMethodName());

      byte[][] QUALIFIERS = makeNAscii(QUALIFIER, 1);
      byte[][] VALUES = makeN(VALUE, 5);
      long[] ts = {1000, 2000, 3000, 4000, 5000};

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
          get.readVersions(Integer.MAX_VALUE);
          Result result = ht.get(get);
          // verify version '1000'/'3000'/'5000' remains for all columns
          assertNResult(result, ROW, FAMILY, QUALIFIERS[i],
                  new long[]{ts[0], ts[2], ts[4]},
                  new byte[][]{VALUES[0], VALUES[2], VALUES[4]},
                  0, 2);
        }
      }
    }
  }

  @Test
  public void testDeleteFamilyVersionWithOtherDeletes() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 5);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    try (Admin admin = TEST_UTIL.getAdmin();
        Table ht = TEST_UTIL.createTable(tableName, FAMILY, 5)) {
      Put put = null;
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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[0],
              new long[]{ts[4]},
              new byte[][]{VALUES[4]},
              0, 0);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[1]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[1],
              new long[]{ts[2], ts[4]},
              new byte[][]{VALUES[2], VALUES[4]},
              0, 1);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[2]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals(0, result.size());

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[3]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[3],
              new long[]{ts[2], ts[4]},
              new byte[][]{VALUES[2], VALUES[4]},
              0, 1);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[4]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIERS[4],
              new long[]{ts[2]},
              new byte[][]{VALUES[2]},
              0, 0);

      // 6. check ROWS[0]
      for (int i = 0; i < 5; i++) {
        get = new Get(ROW2);
        get.addColumn(FAMILY, QUALIFIERS[i]);
        get.readVersions(Integer.MAX_VALUE);
        result = ht.get(get);
        // verify version '1000'/'3000'/'5000' remains for all columns
        assertNResult(result, ROW2, FAMILY, QUALIFIERS[i],
                new long[]{ts[0], ts[2], ts[4]},
                new byte[][]{VALUES[0], VALUES[2], VALUES[4]},
                0, 2);
      }
    }
  }

  @Test
  public void testDeleteWithFailed() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

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

  @Test
  public void testDeletes() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

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
      get.readVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
          new long [] {ts[1]},
          new byte[][] {VALUES[1]},
          0, 0);

      Scan scan = new Scan(ROW);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
              new long[]{ts[1]},
              new byte[][]{VALUES[1]},
              0, 0);

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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
          new long [] {ts[1], ts[2], ts[3]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
          0, 2);

      scan = new Scan(ROW);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
              new long[]{ts[1], ts[2], ts[3]},
              new byte[][]{VALUES[1], VALUES[2], VALUES[3]},
              0, 2);

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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
          new long [] {ts[1], ts[2], ts[3]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
          0, 2);

      // The Scanner returns the previous values, the expected-naive-unexpected behavior

      scan = new Scan(ROW);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
              new long[]{ts[1], ts[2], ts[3]},
              new byte[][]{VALUES[1], VALUES[2], VALUES[3]},
              0, 2);

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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 4 key but received " + result.size() + ": " + result,
          result.size() == 4);

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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 2 keys but received " + result.size(),
          result.size() == 2);
      assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
          new long [] {ts[0], ts[1]},
          new byte[][] {VALUES[0], VALUES[1]},
          0, 1);

      scan = new Scan(ROWS[0]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertTrue("Expected 2 keys but received " + result.size(),
              result.size() == 2);
      assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER,
              new long[]{ts[0], ts[1]},
              new byte[][]{VALUES[0], VALUES[1]},
              0, 1);

      get = new Get(ROWS[1]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 2 keys but received " + result.size(),
          result.size() == 2);

      scan = new Scan(ROWS[1]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertTrue("Expected 2 keys but received " + result.size(),
              result.size() == 2);

      get = new Get(ROWS[2]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertEquals(1, result.size());
      assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
          new long [] {ts[2]},
          new byte[][] {VALUES[2]},
          0, 0);

      scan = new Scan(ROWS[2]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals(1, result.size());
      assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
              new long[]{ts[2]},
              new byte[][]{VALUES[2]},
              0, 0);

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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 1 key but received " + result.size(),
          result.size() == 1);

      get = new Get(ROWS[4]);
      get.addFamily(FAMILIES[1]);
      get.addFamily(FAMILIES[2]);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertTrue("Expected 2 keys but received " + result.size(),
          result.size() == 2);

      scan = new Scan(ROWS[3]);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      ResultScanner scanner = ht.getScanner(scan);
      result = scanner.next();
      assertTrue("Expected 1 key but received " + result.size(),
              result.size() == 1);
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
      result = scanner.next();
      assertTrue("Expected 2 keys but received " + result.size(),
              result.size() == 2);
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
      scanner.close();

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
        assertTrue(result.size() == 1);
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

  /**
   * Test batch operations with combination of valid and invalid args
   */
  @Test
  public void testBatchOperationsWithErrors() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table foo = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 10)) {

      int NUM_OPS = 100;

      // 1.1 Put with no column families (local validation, runtime exception)
      List<Put> puts = new ArrayList<Put>(NUM_OPS);
      for (int i = 0; i != NUM_OPS; i++) {
        Put put = new Put(Bytes.toBytes(i));
        puts.add(put);
      }

      try {
        foo.put(puts);
        fail();
      } catch (IllegalArgumentException e) {
        // expected
        assertEquals(NUM_OPS, puts.size());
      }

      // 1.2 Put with invalid column family
      puts.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn((i % 2) == 0 ? FAMILY : INVALID_FAMILY, FAMILY, Bytes.toBytes(i));
        puts.add(put);
      }

      try {
        foo.put(puts);
        fail();
      } catch (RetriesExhaustedException e) {
        // expected
        assertThat(e.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }

      // 2.1 Get non-existent rows
      List<Get> gets = new ArrayList<>(NUM_OPS);
      for (int i = 0; i < NUM_OPS; i++) {
        Get get = new Get(Bytes.toBytes(i));
        gets.add(get);
      }
      Result[] getsResult = foo.get(gets);
      assertNotNull(getsResult);
      assertEquals(NUM_OPS, getsResult.length);
      for (int i = 0; i < NUM_OPS; i++) {
        Result getResult = getsResult[i];
        if (i % 2 == 0) {
          assertFalse(getResult.isEmpty());
        } else {
          assertTrue(getResult.isEmpty());
        }
      }

      // 2.2 Get with invalid column family
      gets.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Get get = new Get(Bytes.toBytes(i));
        get.addColumn((i % 2) == 0 ? FAMILY : INVALID_FAMILY, FAMILY);
        gets.add(get);
      }
      try {
        foo.get(gets);
        fail();
      } catch (RetriesExhaustedException e) {
        // expected
        assertThat(e.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }

      // 3.1 Delete with invalid column family
      List<Delete> deletes = new ArrayList<>(NUM_OPS);
      for (int i = 0; i < NUM_OPS; i++) {
        Delete delete = new Delete(Bytes.toBytes(i));
        delete.addColumn((i % 2) == 0 ? FAMILY : INVALID_FAMILY, FAMILY);
        deletes.add(delete);
      }
      try {
        foo.delete(deletes);
        fail();
      } catch (RetriesExhaustedException e) {
        // expected
        assertThat(e.getCause(), instanceOf(NoSuchColumnFamilyException.class));
      }

      // all valid rows should have been deleted
      gets.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Get get = new Get(Bytes.toBytes(i));
        gets.add(get);
      }
      getsResult = foo.get(gets);
      assertNotNull(getsResult);
      assertEquals(NUM_OPS, getsResult.length);
      for (Result getResult : getsResult) {
        assertTrue(getResult.isEmpty());
      }

      // 3.2 Delete non-existent rows
      deletes.clear();
      for (int i = 0; i < NUM_OPS; i++) {
        Delete delete = new Delete(Bytes.toBytes(i));
        deletes.add(delete);
      }
      foo.delete(deletes);
    }
  }

  //
  // JIRA Testers
  //

  /**
   * HBASE-867
   *    If millions of columns in a column family, hbase scanner won't come up
   *
   *    Test will create numRows rows, each with numColsPerRow columns
   *    (1 version each), and attempt to scan them all.
   *
   *    To test at scale, up numColsPerRow to the millions
   *    (have not gotten that to work running as junit though)
   */
  @Test
  public void testJiraTest867() throws Exception {
    int numRows = 10;
    int numColsPerRow = 2000;

    final TableName tableName = TableName.valueOf(name.getMethodName());

    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Insert rows

      for (int i = 0; i < numRows; i++) {
        Put put = new Put(ROWS[i]);
        put.setDurability(Durability.SKIP_WAL);
        for (int j = 0; j < numColsPerRow; j++) {
          put.addColumn(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
        }
        assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
                "only contains " + put.size(), put.size() == numColsPerRow);
        ht.put(put);
      }

      // Get a row
      Get get = new Get(ROWS[numRows - 1]);
      Result result = ht.get(get);
      assertNumKeys(result, numColsPerRow);
      Cell[] keys = result.rawCells();
      for (int i = 0; i < result.size(); i++) {
        assertKey(keys[i], ROWS[numRows - 1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }

      // Scan the rows
      Scan scan = new Scan();
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int rowCount = 0;
        while ((result = scanner.next()) != null) {
          assertNumKeys(result, numColsPerRow);
          Cell[] kvs = result.rawCells();
          for (int i = 0; i < numColsPerRow; i++) {
            assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
          }
          rowCount++;
        }
        assertTrue("Expected to scan " + numRows + " rows but actually scanned "
                + rowCount + " rows", rowCount == numRows);
      }

      // flush and try again

      TEST_UTIL.flush();

      // Get a row
      get = new Get(ROWS[numRows - 1]);
      result = ht.get(get);
      assertNumKeys(result, numColsPerRow);
      keys = result.rawCells();
      for (int i = 0; i < result.size(); i++) {
        assertKey(keys[i], ROWS[numRows - 1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }

      // Scan the rows
      scan = new Scan();
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int rowCount = 0;
        while ((result = scanner.next()) != null) {
          assertNumKeys(result, numColsPerRow);
          Cell[] kvs = result.rawCells();
          for (int i = 0; i < numColsPerRow; i++) {
            assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
          }
          rowCount++;
        }
        assertTrue("Expected to scan " + numRows + " rows but actually scanned "
                + rowCount + " rows", rowCount == numRows);
      }
    }
  }

  /**
   * HBASE-861
   *    get with timestamp will return a value if there is a version with an
   *    earlier timestamp
   */
  @Test
  public void testJiraTest861() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 10)) {

      // Insert three versions

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      ht.put(put);

      // Get the middle value
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);

      // Try to get one version before (expect fail)
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);

      // Try to get one version after (expect fail)
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

      // Try same from storefile
      TEST_UTIL.flush();
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);

      // Insert two more versions surrounding others, into memstore
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
      ht.put(put);

      // Check we can get everything we should and can't get what we shouldn't
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);

      // Try same from two storefiles
      TEST_UTIL.flush();
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, STAMPS[5]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    }
  }

  /**
   * HBASE-33
   *    Add a HTable get/obtainScanner method that retrieves all versions of a
   *    particular column and row between two timestamps
   */
  @Test
  public void testJiraTest33() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 10)) {

      // Insert lots versions

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

      // Try same from storefile
      TEST_UTIL.flush();

      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      getVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);

      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 2);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
      scanVersionRangeAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 3);
    }
  }

  /**
   * HBASE-1014
   *    commit(BatchUpdate) method should return timestamp
   */
  @Test
  public void testJiraTest1014() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 10)) {

      long manualStamp = 12345;

      // Insert lots versions

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, manualStamp, VALUE);
      ht.put(put);

      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp - 1);
      getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp + 1);
    }
  }

  /**
   * HBASE-1182
   *    Scan for columns > some timestamp
   */
  @Test
  public void testJiraTest1182() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 10)) {

      // Insert lots versions

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

      // Try same from storefile
      TEST_UTIL.flush();

      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      getVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);

      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 2, 5);
      scanVersionRangeAndVerifyGreaterThan(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 4, 5);
    }
  }

  /**
   * HBASE-52
   *    Add a means of scanning over all versions
   */
  @Test
  public void testJiraTest52() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 10)) {

      // Insert lots versions

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
      ht.put(put);

      getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

      scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

      // Try same from storefile
      TEST_UTIL.flush();

      getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

      scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
    }
  }

  //
  // Bulk Testers
  //

  private void getVersionRangeAndVerifyGreaterThan(Table ht, byte [] row,
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
      int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.readVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }

  private void getVersionRangeAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.readVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start], stamps[end]+1);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void getAllVersionsAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.readVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void scanVersionRangeAndVerifyGreaterThan(Table ht, byte [] row,
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
      int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }

  private void scanVersionRangeAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start], stamps[end]+1);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void scanAllVersionsAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void getVersionAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp, byte [] value)
  throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimestamp(stamp);
    get.readVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  private void getVersionAndVerifyMissing(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp)
  throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimestamp(stamp);
    get.readVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertEmptyResult(result);
  }

  private void scanVersionAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp, byte [] value)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimestamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  private void scanVersionAndVerifyMissing(Table ht, byte [] row,
      byte [] family, byte [] qualifier, long stamp)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimestamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }

  private void getTestNull(Table ht, byte [] row, byte [] family,
      byte [] value)
  throws Exception {

    Get get = new Get(row);
    get.addColumn(family, null);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, null, value);

    get = new Get(row);
    get.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    get.addFamily(family);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

  }

  private void getTestNull(Table ht, byte[] row, byte[] family, long value) throws Exception {
    Get get = new Get(row);
    get.addColumn(family, null);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, null, value);

    get = new Get(row);
    get.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    get.addFamily(family);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);
  }

  private void scanTestNull(Table ht, byte[] row, byte[] family, byte[] value)
      throws Exception {
    scanTestNull(ht, row, family, value, false);
  }

  private void scanTestNull(Table ht, byte[] row, byte[] family, byte[] value,
      boolean isReversedScan) throws Exception {

    Scan scan = new Scan();
    scan.setReversed(isReversedScan);
    scan.addColumn(family, null);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.setReversed(isReversedScan);
    scan.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.setReversed(isReversedScan);
    scan.addFamily(family);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.setReversed(isReversedScan);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

  }

  private void singleRowGetTest(Table ht, byte [][] ROWS, byte [][] FAMILIES,
      byte [][] QUALIFIERS, byte [][] VALUES)
  throws Exception {

    // Single column from memstore
    Get get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);

    // Single column from storefile
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    // Single column from storefile, family match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);

    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });

    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[2]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[6]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    get.addColumn(FAMILIES[2], QUALIFIERS[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    get.addColumn(FAMILIES[6], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Everything
    get = new Get(ROWS[0]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
    });

    // Get around inserted columns

    get = new Get(ROWS[1]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    get.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);

  }

  private void singleRowScanTest(Table ht, byte [][] ROWS, byte [][] FAMILIES,
      byte [][] QUALIFIERS, byte [][] VALUES)
  throws Exception {

    // Single column from memstore
    Scan scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);

    // Single column from storefile
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    // Single column from storefile, family match
    scan = new Scan();
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);

    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
        FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });

    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addFamily(FAMILIES[2]);
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[6]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
    });

    // Everything
    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
        new int [][] {
          {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
    });

    // Scan around inserted columns

    scan = new Scan(ROWS[1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }

  /**
   * Verify a single column using gets.
   * Expects family and qualifier arrays to be valid for at least
   * the range:  idx-2 < idx < idx+2
   */
  private void getVerifySingleColumn(Table ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX,
      byte [][] VALUES, int VALUEIDX)
  throws Exception {

    Get get = new Get(ROWS[ROWIDX]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX-2]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[0]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addColumn(FAMILIES[FAMILYIDX+1], QUALIFIERS[1]);
    get.addColumn(FAMILIES[FAMILYIDX-2], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX-1]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

  }


  /**
   * Verify a single column using scanners.
   * Expects family and qualifier arrays to be valid for at least
   * the range:  idx-2 to idx+2
   * Expects row array to be valid for at least idx to idx+2
   */
  private void scanVerifySingleColumn(Table ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX,
      byte [][] VALUES, int VALUEIDX)
  throws Exception {

    Scan scan = new Scan();
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(ROWS[ROWIDX], ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX-1], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    scan.addFamily(FAMILIES[FAMILYIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
        QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

  }

  /**
   * Verify we do not read any values by accident around a single column
   * Same requirements as getVerifySingleColumn
   */
  private void getVerifySingleEmpty(Table ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX)
  throws Exception {

    Get get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[1]);
    Result result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[3]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    get.addFamily(FAMILIES[5]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX+1]);
    result = ht.get(get);
    assertEmptyResult(result);

  }

  private void scanVerifySingleEmpty(Table ht,
      byte [][] ROWS, int ROWIDX,
      byte [][] FAMILIES, int FAMILYIDX,
      byte [][] QUALIFIERS, int QUALIFIERIDX)
  throws Exception {

    Scan scan = new Scan(ROWS[ROWIDX+1]);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(ROWS[ROWIDX+1],ROWS[ROWIDX+2]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX-1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

  }

  //
  // Verifiers
  //

  private void assertKey(Cell key, byte [] row, byte [] family,
      byte [] qualifier, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(CellUtil.cloneRow(key)) +"]",
        equals(row, CellUtil.cloneRow(key)));
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(key)) + "]",
        equals(family, CellUtil.cloneFamily(key)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(key)) + "]",
        equals(qualifier, CellUtil.cloneQualifier(key)));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(key)) + "]",
        equals(value, CellUtil.cloneValue(key)));
  }

  static void assertIncrementKey(Cell key, byte [] row, byte [] family,
      byte [] qualifier, long value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(CellUtil.cloneRow(key)) +"]",
        equals(row, CellUtil.cloneRow(key)));
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(key)) + "]",
        equals(family, CellUtil.cloneFamily(key)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(key)) + "]",
        equals(qualifier, CellUtil.cloneQualifier(key)));
    assertTrue("Expected value [" + value + "] " +
        "Got value [" + Bytes.toLong(CellUtil.cloneValue(key)) + "]",
        Bytes.toLong(CellUtil.cloneValue(key)) == value);
  }

  private void assertNumKeys(Result result, int n) throws Exception {
    assertTrue("Expected " + n + " keys but got " + result.size(),
        result.size() == n);
  }

  private void assertNResult(Result result, byte [] row,
      byte [][] families, byte [][] qualifiers, byte [][] values,
      int [][] idxs)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected " + idxs.length + " keys but result contains "
        + result.size(), result.size() == idxs.length);

    Cell [] keys = result.rawCells();

    for(int i=0;i<keys.length;i++) {
      byte [] family = families[idxs[i][0]];
      byte [] qualifier = qualifiers[idxs[i][1]];
      byte [] value = values[idxs[i][2]];
      Cell key = keys[i];

      byte[] famb = CellUtil.cloneFamily(key);
      byte[] qualb = CellUtil.cloneQualifier(key);
      byte[] valb = CellUtil.cloneValue(key);
      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(famb) + "]",
          equals(family, famb));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier)
          + "] " + "Got qualifier [" + Bytes.toString(qualb) + "]",
          equals(qualifier, qualb));
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(valb) + "]",
          equals(value, valb));
    }
  }

  private void assertNResult(Result result, byte [] row,
      byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
      int start, int end)
  throws IOException {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    int expectedResults = end - start + 1;
    assertEquals(expectedResults, result.size());

    Cell[] keys = result.rawCells();

    for (int i=0; i<keys.length; i++) {
      byte [] value = values[end-i];
      long ts = stamps[end-i];
      Cell key = keys[i];

      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(CellUtil.cloneFamily(key)) + "]",
          CellUtil.matchingFamily(key, family));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier)
          + "] " + "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(key))+ "]",
          CellUtil.matchingQualifier(key, qualifier));
      assertTrue("Expected ts [" + ts + "] " +
          "Got ts [" + key.getTimestamp() + "]", ts == key.getTimestamp());
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(CellUtil.cloneValue(key)) + "]",
          CellUtil.matchingValue(key,  value));
    }
  }

  /**
   * Validate that result contains two specified keys, exactly.
   * It is assumed key A sorts before key B.
   */
  private void assertDoubleResult(Result result, byte [] row,
      byte [] familyA, byte [] qualifierA, byte [] valueA,
      byte [] familyB, byte [] qualifierB, byte [] valueB)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected two keys but result contains " + result.size(),
        result.size() == 2);
    Cell [] kv = result.rawCells();
    Cell kvA = kv[0];
    assertTrue("(A) Expected family [" + Bytes.toString(familyA) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kvA)) + "]",
        equals(familyA, CellUtil.cloneFamily(kvA)));
    assertTrue("(A) Expected qualifier [" + Bytes.toString(qualifierA) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kvA)) + "]",
        equals(qualifierA, CellUtil.cloneQualifier(kvA)));
    assertTrue("(A) Expected value [" + Bytes.toString(valueA) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kvA)) + "]",
        equals(valueA, CellUtil.cloneValue(kvA)));
    Cell kvB = kv[1];
    assertTrue("(B) Expected family [" + Bytes.toString(familyB) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kvB)) + "]",
        equals(familyB, CellUtil.cloneFamily(kvB)));
    assertTrue("(B) Expected qualifier [" + Bytes.toString(qualifierB) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kvB)) + "]",
        equals(qualifierB, CellUtil.cloneQualifier(kvB)));
    assertTrue("(B) Expected value [" + Bytes.toString(valueB) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kvB)) + "]",
        equals(valueB, CellUtil.cloneValue(kvB)));
  }

  private void assertSingleResult(Result result, byte [] row, byte [] family,
      byte [] qualifier, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected a single key but result contains " + result.size(),
        result.size() == 1);
    Cell kv = result.rawCells()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kv)) + "]",
        equals(family, CellUtil.cloneFamily(kv)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kv)) + "]",
        equals(qualifier, CellUtil.cloneQualifier(kv)));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kv)) + "]",
        equals(value, CellUtil.cloneValue(kv)));
  }

  private void assertSingleResult(Result result, byte[] row, byte[] family, byte[] qualifier,
      long value) throws Exception {
    assertTrue(
      "Expected row [" + Bytes.toString(row) + "] " + "Got row [" + Bytes.toString(result.getRow())
          + "]", equals(row, result.getRow()));
    assertTrue("Expected a single key but result contains " + result.size(), result.size() == 1);
    Cell kv = result.rawCells()[0];
    assertTrue(
      "Expected family [" + Bytes.toString(family) + "] " + "Got family ["
          + Bytes.toString(CellUtil.cloneFamily(kv)) + "]",
      equals(family, CellUtil.cloneFamily(kv)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " + "Got qualifier ["
        + Bytes.toString(CellUtil.cloneQualifier(kv)) + "]",
      equals(qualifier, CellUtil.cloneQualifier(kv)));
    assertTrue(
      "Expected value [" + value + "] " + "Got value [" + Bytes.toLong(CellUtil.cloneValue(kv))
          + "]", value == Bytes.toLong(CellUtil.cloneValue(kv)));
  }

  private void assertSingleResult(Result result, byte [] row, byte [] family,
      byte [] qualifier, long ts, byte [] value)
  throws Exception {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
        equals(row, result.getRow()));
    assertTrue("Expected a single key but result contains " + result.size(),
        result.size() == 1);
    Cell kv = result.rawCells()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kv)) + "]",
        equals(family, CellUtil.cloneFamily(kv)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kv)) + "]",
        equals(qualifier, CellUtil.cloneQualifier(kv)));
    assertTrue("Expected ts [" + ts + "] " +
        "Got ts [" + kv.getTimestamp() + "]", ts == kv.getTimestamp());
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kv)) + "]",
        equals(value, CellUtil.cloneValue(kv)));
  }

  private void assertEmptyResult(Result result) throws Exception {
    assertTrue("expected an empty result but result contains " +
        result.size() + " keys", result.isEmpty());
  }

  private void assertNullResult(Result result) throws Exception {
    assertTrue("expected null result but received a non-null result",
        result == null);
  }

  //
  // Helpers
  //

  private Result getSingleScanResult(Table ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    scanner.close();
    return result;
  }

  private byte [][] makeNAscii(byte [] base, int n) {
    if(n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      byte [] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

  private byte [][] makeN(byte [] base, int n) {
    if (n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, new byte[]{(byte)i});
    }
    return ret;
  }

  private byte [][] makeNBig(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      int byteA = (i % 256);
      int byteB = (i >> 8);
      ret[i] = Bytes.add(base, new byte[]{(byte)byteB,(byte)byteA});
    }
    return ret;
  }

  private long [] makeStamps(int n) {
    long [] stamps = new long[n];
    for (int i = 0; i < n; i++) {
      stamps[i] = i+1L;
    }
    return stamps;
  }

  static boolean equals(byte [] left, byte [] right) {
    if (left == null && right == null) return true;
    if (left == null && right.length == 0) return true;
    if (right == null && left.length == 0) return true;
    return Bytes.equals(left, right);
  }

  @Test
  public void testDuplicateVersions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

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
      get.readVersions(2);
      Result result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[4], STAMPS[5]},
          new byte[][] {VALUES[4], VALUES[5]},
          0, 1);

      Scan scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
              new long[]{STAMPS[4], STAMPS[5]},
              new byte[][]{VALUES[4], VALUES[5]},
              0, 1);

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
      get.readVersions(2);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[4], STAMPS[5]},
          new byte[][] {VALUES[4], VALUES[5]},
          0, 1);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(2);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
              new long[]{STAMPS[4], STAMPS[5]},
              new byte[][]{VALUES[4], VALUES[5]},
              0, 1);


      // Add some memstore and retest

      // Insert 4 more versions of same column and a dupe
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
      put.addColumn(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
      ht.put(put);

      // Ensure maxVersions in query is respected
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(7);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
          new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7],
                  VALUES[8]},
          0, 6);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(7);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[]{STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][]{VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

      get = new Get(ROW);
      get.readVersions(7);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
          new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7],
                  VALUES[8]},
          0, 6);

      scan = new Scan(ROW);
      scan.setMaxVersions(7);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long[]{STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][]{VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

      // Verify we can get each one properly
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
      getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
      scanVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
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
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
                  STAMPS[11], STAMPS[13], STAMPS[15]},
          new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8],
                  VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
          0, 9);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long[]{STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9],
                  STAMPS[11], STAMPS[13], STAMPS[15]},
          new byte[][]{VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9],
                  VALUES[11], VALUES[13], VALUES[15]},0, 9);

      // Delete a version in the memstore and a version in a storefile
      Delete delete = new Delete(ROW);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[11]);
      delete.addColumn(FAMILY, QUALIFIER, STAMPS[7]);
      ht.delete(delete);

      // Test that it's gone
      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
                  STAMPS[9], STAMPS[13], STAMPS[15]},
          new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6],
                  VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
          0, 9);

      scan = new Scan(ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILY, QUALIFIER,
          new long[]{STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8],
                  STAMPS[9], STAMPS[13], STAMPS[15]},
          new byte[][]{VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8],
                  VALUES[9], VALUES[13], VALUES[15]},0,9);
    }
  }

  @Test
  public void testUpdates() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table hTable = TEST_UTIL.createTable(tableName, FAMILY, 10)) {

      // Write a column with values at timestamp 1, 2 and 3
      byte[] row = Bytes.toBytes("row1");
      byte[] qualifier = Bytes.toBytes("myCol");
      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
      hTable.put(put);

      Get get = new Get(row);
      get.addColumn(FAMILY, qualifier);
      get.readAllVersions();

      // Check that the column indeed has the right values at timestamps 1 and
      // 2
      Result result = hTable.get(get);
      NavigableMap<Long, byte[]> navigableMap =
              result.getMap().get(FAMILY).get(qualifier);
      assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
      assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

      // Update the value at timestamp 1
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
      hTable.put(put);

      // Update the value at timestamp 2
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
      hTable.put(put);

      // Check that the values at timestamp 2 and 1 got updated
      result = hTable.get(get);
      navigableMap = result.getMap().get(FAMILY).get(qualifier);
      assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
      assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
    }
  }

  @Test
  public void testUpdatesWithMajorCompaction() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table hTable = TEST_UTIL.createTable(tableName, FAMILY, 10);
        Admin admin = TEST_UTIL.getAdmin()) {

      // Write a column with values at timestamp 1, 2 and 3
      byte[] row = Bytes.toBytes("row2");
      byte[] qualifier = Bytes.toBytes("myCol");
      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
      hTable.put(put);

      Get get = new Get(row);
      get.addColumn(FAMILY, qualifier);
      get.readAllVersions();

      // Check that the column indeed has the right values at timestamps 1 and
      // 2
      Result result = hTable.get(get);
      NavigableMap<Long, byte[]> navigableMap =
              result.getMap().get(FAMILY).get(qualifier);
      assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
      assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Update the value at timestamp 1
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
      hTable.put(put);

      // Update the value at timestamp 2
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
      hTable.put(put);

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Check that the values at timestamp 2 and 1 got updated
      result = hTable.get(get);
      navigableMap = result.getMap().get(FAMILY).get(qualifier);
      assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
      assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
    }
  }

  @Test
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table hTable = TEST_UTIL.createTable(tableName, FAMILY, 10);
        Admin admin = TEST_UTIL.getAdmin()) {

      // Write a column with values at timestamp 1, 2 and 3
      byte[] row = Bytes.toBytes("row3");
      byte[] qualifier = Bytes.toBytes("myCol");
      Put put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
      hTable.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
      hTable.put(put);

      Get get = new Get(row);
      get.addColumn(FAMILY, qualifier);
      get.readAllVersions();

      // Check that the column indeed has the right values at timestamps 1 and
      // 2
      Result result = hTable.get(get);
      NavigableMap<Long, byte[]> navigableMap =
              result.getMap().get(FAMILY).get(qualifier);
      assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
      assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Update the value at timestamp 1
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
      hTable.put(put);

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Update the value at timestamp 2
      put = new Put(row);
      put.addColumn(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
      hTable.put(put);

      // Trigger a major compaction
      admin.flush(tableName);
      admin.majorCompact(tableName);
      Thread.sleep(6000);

      // Check that the values at timestamp 2 and 1 got updated
      result = hTable.get(get);
      navigableMap = result.getMap().get(FAMILY).get(qualifier);

      assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
      assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
    }
  }

  @Test
  public void testGet_EmptyTable() throws IOException {
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {
      Get get = new Get(ROW);
      get.addFamily(FAMILY);
      Result r = table.get(get);
      assertTrue(r.isEmpty());
    }
  }

  @Test
  public void testGet_NullQualifier() throws IOException {
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      table.put(put);
      LOG.info("Row put");

      Get get = new Get(ROW);
      get.addColumn(FAMILY, null);
      Result r = table.get(get);
      assertEquals(1, r.size());

      get = new Get(ROW);
      get.addFamily(FAMILY);
      r = table.get(get);
      assertEquals(2, r.size());
    }
  }

  @Test
  public void testGet_NonExistentRow() throws IOException {
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      LOG.info("Row put");

      Get get = new Get(ROW);
      get.addFamily(FAMILY);
      Result r = table.get(get);
      assertFalse(r.isEmpty());
      System.out.println("Row retrieved successfully");

      byte[] missingrow = Bytes.toBytes("missingrow");
      get = new Get(missingrow);
      get.addFamily(FAMILY);
      r = table.get(get);
      assertTrue(r.isEmpty());
      LOG.info("Row missing as it should be");
    }
  }

  @Test
  public void testPut() throws IOException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] row1 = Bytes.toBytes("row1");
    final byte [] row2 = Bytes.toBytes("row2");
    final byte [] value = Bytes.toBytes("abcd");
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
        new byte[][] { CONTENTS_FAMILY, SMALL_FAMILY })) {
      Put put = new Put(row1);
      put.addColumn(CONTENTS_FAMILY, null, value);
      table.put(put);

      put = new Put(row2);
      put.addColumn(CONTENTS_FAMILY, null, value);

      assertEquals(1, put.size());
      assertEquals(1, put.getFamilyCellMap().get(CONTENTS_FAMILY).size());

      // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
      KeyValue kv = (KeyValue) put.getFamilyCellMap().get(CONTENTS_FAMILY).get(0);

      assertTrue(Bytes.equals(CellUtil.cloneFamily(kv), CONTENTS_FAMILY));
      // will it return null or an empty byte array?
      assertTrue(Bytes.equals(CellUtil.cloneQualifier(kv), new byte[0]));

      assertTrue(Bytes.equals(CellUtil.cloneValue(kv), value));

      table.put(put);

      Scan scan = new Scan();
      scan.addColumn(CONTENTS_FAMILY, null);
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          for (Cell key : r.rawCells()) {
            System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
          }
        }
      }
    }
  }

  @Test
  public void testPutNoCF() throws IOException {
    final byte[] BAD_FAM = Bytes.toBytes("BAD_CF");
    final byte[] VAL = Bytes.toBytes(100);
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {
      boolean caughtNSCFE = false;

      try {
        Put p = new Put(ROW);
        p.addColumn(BAD_FAM, QUALIFIER, VAL);
        table.put(p);
      } catch (Exception e) {
        caughtNSCFE = e instanceof NoSuchColumnFamilyException;
      }
      assertTrue("Should throw NoSuchColumnFamilyException", caughtNSCFE);
    }
  }

  @Test
  public void testRowsPut() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY })) {
      ArrayList<Put> rowsUpdate = new ArrayList<Put>();
      for (int i = 0; i < NB_BATCH_ROWS; i++) {
        byte[] row = Bytes.toBytes("row" + i);
        Put put = new Put(row);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(CONTENTS_FAMILY, null, value);
        rowsUpdate.add(put);
      }
      table.put(rowsUpdate);
      Scan scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int nbRows = 0;
        for (@SuppressWarnings("unused")
                Result row : scanner) {
          nbRows++;
        }
        assertEquals(NB_BATCH_ROWS, nbRows);
      }
    }
  }

  @Test
  public void testRowsPutBufferedManyManyFlushes() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
        new byte[][] { CONTENTS_FAMILY, SMALL_FAMILY })) {
      ArrayList<Put> rowsUpdate = new ArrayList<Put>();
      for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
        byte[] row = Bytes.toBytes("row" + i);
        Put put = new Put(row);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(CONTENTS_FAMILY, null, value);
        rowsUpdate.add(put);
      }
      table.put(rowsUpdate);

      Scan scan = new Scan();
      scan.addFamily(CONTENTS_FAMILY);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int nbRows = 0;
        for (@SuppressWarnings("unused")
                Result row : scanner) {
          nbRows++;
        }
        assertEquals(NB_BATCH_ROWS * 10, nbRows);
      }
    }
  }

  @Test
  public void testAddKeyValue() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);

    // Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = false;
    }
    assertEquals(true, ok);

    // Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = true;
    }
    assertEquals(true, ok);
  }

  /**
   * test for HBASE-737
   */
  @Test
  public void testHBase737 () throws IOException {
    final byte [] FAM1 = Bytes.toBytes("fam1");
    final byte [] FAM2 = Bytes.toBytes("fam2");
    // Open table
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
      new byte [][] {FAM1, FAM2})) {
      // Insert some values
      Put put = new Put(ROW);
      put.addColumn(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
      table.put(put);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException i) {
        //ignore
      }

      put = new Put(ROW);
      put.addColumn(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
      table.put(put);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException i) {
        //ignore
      }

      put = new Put(ROW);
      put.addColumn(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
      table.put(put);

      long[] times = new long[3];

      // First scan the memstore

      Scan scan = new Scan();
      scan.addFamily(FAM1);
      scan.addFamily(FAM2);
      try (ResultScanner s = table.getScanner(scan)) {
        int index = 0;
        Result r = null;
        while ((r = s.next()) != null) {
          for (Cell key : r.rawCells()) {
            times[index++] = key.getTimestamp();
          }
        }
      }
      for (int i = 0; i < times.length - 1; i++) {
        for (int j = i + 1; j < times.length; j++) {
          assertTrue(times[j] > times[i]);
        }
      }

      // Flush data to disk and try again
      TEST_UTIL.flush();

      // Reset times
      for (int i = 0; i < times.length; i++) {
        times[i] = 0;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException i) {
        //ignore
      }
      scan = new Scan();
      scan.addFamily(FAM1);
      scan.addFamily(FAM2);
      try (ResultScanner s = table.getScanner(scan)) {
        int index = 0;
        Result r = null;
        while ((r = s.next()) != null) {
          for (Cell key : r.rawCells()) {
            times[index++] = key.getTimestamp();
          }
        }
        for (int i = 0; i < times.length - 1; i++) {
          for (int j = i + 1; j < times.length; j++) {
            assertTrue(times[j] > times[i]);
          }
        }
      }
    }
  }

  @Test
  public void testListTables() throws IOException, InterruptedException {
    final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    final TableName tableName3 = TableName.valueOf(name.getMethodName() + "3");
    TableName [] tables = new TableName[] { tableName1, tableName2, tableName3 };
    for (int i = 0; i < tables.length; i++) {
      TEST_UTIL.createTable(tables[i], FAMILY);
    }
    try (Admin admin = TEST_UTIL.getAdmin()) {
      List<TableDescriptor> ts = admin.listTableDescriptors();
      HashSet<TableDescriptor> result = new HashSet<>(ts);
      int size = result.size();
      assertTrue(size >= tables.length);
      for (int i = 0; i < tables.length && i < size; i++) {
        boolean found = false;
        for (int j = 0; j < ts.size(); j++) {
          if (ts.get(j).getTableName().equals(tables[i])) {
            found = true;
            break;
          }
        }
        assertTrue("Not found: " + tables[i], found);
      }
    }
  }

  /**
   * simple test that just executes parts of the client
   * API that accept a pre-created Connection instance
   */
  @Test
  public void testUnmanagedHConnection() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Table t = conn.getTable(tableName);
        Admin admin = conn.getAdmin()) {
      assertTrue(admin.tableExists(tableName));
      assertTrue(t.get(new Get(ROW)).isEmpty());
    }
  }

  /**
   * test of that unmanaged HConnections are able to reconnect
   * properly (see HBASE-5058)
   */
  @Test
  public void testUnmanagedHConnectionReconnect() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      try (Table t = conn.getTable(tableName); Admin admin = conn.getAdmin()) {
        assertTrue(admin.tableExists(tableName));
        assertTrue(t.get(new Get(ROW)).isEmpty());
      }

      // stop the master
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.stopMaster(0, false);
      cluster.waitOnMaster(0);

      // start up a new master
      cluster.startMaster();
      assertTrue(cluster.waitForActiveAndReadyMaster());

      // test that the same unmanaged connection works with a new
      // Admin and can connect to the new master;
      boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration());
      try (Admin admin = conn.getAdmin()) {
        assertTrue(admin.tableExists(tableName));
        assertTrue(admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics()
          .size() == SLAVES + (tablesOnMaster ? 1 : 0));
      }
    }
  }

  @Test
  public void testMiscHTableStuff() throws IOException {
    final TableName tableAname = TableName.valueOf(name.getMethodName() + "A");
    final TableName tableBname = TableName.valueOf(name.getMethodName() + "B");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");

    try (Table a = TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
        Table b = TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(HConstants.CATALOG_FAMILY, null, value);
      a.put(put);

      // open a new connection to A and a connection to b
      try (Table newA = TEST_UTIL.getConnection().getTable(tableAname)) {

        // copy data from A to B
        Scan scan = new Scan();
        scan.addFamily(HConstants.CATALOG_FAMILY);
        try (ResultScanner s = newA.getScanner(scan)) {
          for (Result r : s) {
            put = new Put(r.getRow());
            put.setDurability(Durability.SKIP_WAL);
            for (Cell kv : r.rawCells()) {
              put.add(kv);
            }
            b.put(put);
          }
        }
      }

      // Opening a new connection to A will cause the tables to be reloaded
      try (Table anotherA = TEST_UTIL.getConnection().getTable(tableAname)) {
        Get get = new Get(ROW);
        get.addFamily(HConstants.CATALOG_FAMILY);
        anotherA.get(get);
      }

      // We can still access A through newA because it has the table information
      // cached. And if it needs to recalibrate, that will cause the information
      // to be reloaded.

      // Test user metadata
      Admin admin = TEST_UTIL.getAdmin();
      // make a modifiable descriptor
      HTableDescriptor desc = new HTableDescriptor(a.getDescriptor());
      // offline the table
      admin.disableTable(tableAname);
      // add a user attribute to HTD
      desc.setValue(attrName, attrValue);
      // add a user attribute to HCD
      for (HColumnDescriptor c : desc.getFamilies())
        c.setValue(attrName, attrValue);
      // update metadata for all regions of this table
      admin.modifyTable(desc);
      // enable the table
      admin.enableTable(tableAname);

      // Test that attribute changes were applied
      desc = new HTableDescriptor(a.getDescriptor());
      assertEquals("wrong table descriptor returned", desc.getTableName(), tableAname);
      // check HTD attribute
      value = desc.getValue(attrName);
      assertFalse("missing HTD attribute value", value == null);
      assertFalse("HTD attribute value is incorrect", Bytes.compareTo(value, attrValue) != 0);
      // check HCD attribute
      for (HColumnDescriptor c : desc.getFamilies()) {
        value = c.getValue(attrName);
        assertFalse("missing HCD attribute value", value == null);
        assertFalse("HCD attribute value is incorrect", Bytes.compareTo(value, attrValue) != 0);
      }
    }
  }

  @Test
  public void testGetClosestRowBefore() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    try (Table table =
        TEST_UTIL.createTable(tableName,
          new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1, 1024);
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {

      // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
      // in Store.rowAtOrBeforeFromStoreFile
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      Put put1 = new Put(firstRow);
      Put put2 = new Put(secondRow);
      Put put3 = new Put(thirdRow);
      Put put4 = new Put(forthRow);
      byte[] one = new byte[] { 1 };
      byte[] two = new byte[] { 2 };
      byte[] three = new byte[] { 3 };
      byte[] four = new byte[] { 4 };

      put1.addColumn(HConstants.CATALOG_FAMILY, null, one);
      put2.addColumn(HConstants.CATALOG_FAMILY, null, two);
      put3.addColumn(HConstants.CATALOG_FAMILY, null, three);
      put4.addColumn(HConstants.CATALOG_FAMILY, null, four);
      table.put(put1);
      table.put(put2);
      table.put(put3);
      table.put(put4);
      region.flush(true);

      Result result;

      // Test before first that null is returned
      result = getReverseScanResult(table, beforeFirstRow,
        HConstants.CATALOG_FAMILY);
      assertNull(result);

      // Test at first that first is returned
      result = getReverseScanResult(table, firstRow, HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), firstRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

      // Test in between first and second that first is returned
      result = getReverseScanResult(table, beforeSecondRow, HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), firstRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

      // Test at second make sure second is returned
      result = getReverseScanResult(table, secondRow, HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), secondRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

      // Test in second and third, make sure second is returned
      result = getReverseScanResult(table, beforeThirdRow, HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), secondRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

      // Test at third make sure third is returned
      result = getReverseScanResult(table, thirdRow, HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), thirdRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

      // Test in third and forth, make sure third is returned
      result = getReverseScanResult(table, beforeForthRow, HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), thirdRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

      // Test at forth make sure forth is returned
      result = getReverseScanResult(table, forthRow, HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), forthRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

      // Test after forth make sure forth is returned
      result = getReverseScanResult(table, Bytes.add(forthRow, one), HConstants.CATALOG_FAMILY);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), forthRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));
    }
  }

  private Result getReverseScanResult(Table table, byte[] row, byte[] fam) throws IOException {
    Scan scan = new Scan(row);
    scan.setSmall(true);
    scan.setReversed(true);
    scan.setCaching(1);
    scan.addFamily(fam);
    try (ResultScanner scanner = table.getScanner(scan)) {
      return scanner.next();
    }
  }

  /**
   * For HBASE-2156
   */
  @Test
  public void testScanVariableReuse() throws Exception {
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    scan.addColumn(FAMILY, ROW);

    assertTrue(scan.getFamilyMap().get(FAMILY).size() == 1);

    scan = new Scan();
    scan.addFamily(FAMILY);

    assertTrue(scan.getFamilyMap().get(FAMILY) == null);
    assertTrue(scan.getFamilyMap().containsKey(FAMILY));
  }

  @Test
  public void testMultiRowMutation() throws Exception {
    LOG.info("Starting testMultiRowMutation");
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte [] ROW1 = Bytes.toBytes("testRow1");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      Put p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);

      p = new Put(ROW1);
      p.addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, p);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      MutateRowsRequest mrm = mrmBuilder.build();
      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
              MultiRowMutationService.newBlockingStub(channel);
      service.mutateRows(null, mrm);
      Get g = new Get(ROW);
      Result r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
      g = new Get(ROW1);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @Test
  public void testRowMutation() throws Exception {
    LOG.info("Starting testRowMutation");
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] QUALIFIERS = new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b") };
      RowMutations arm = new RowMutations(ROW);
      Put p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[0], VALUE);
      arm.add(p);
      t.mutateRow(arm);

      Get g = new Get(ROW);
      Result r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[1], VALUE);
      arm.add(p);
      Delete d = new Delete(ROW);
      d.addColumns(FAMILY, QUALIFIERS[0]);
      arm.add(d);
      // TODO: Trying mutateRow again. The batch was failing with a one try only.
      t.mutateRow(arm);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

      // Test that we get a region level exception
      try {
        arm = new RowMutations(ROW);
        p = new Put(ROW);
        p.addColumn(new byte[] { 'b', 'o', 'g', 'u', 's' }, QUALIFIERS[0], VALUE);
        arm.add(p);
        t.mutateRow(arm);
        fail("Expected NoSuchColumnFamilyException");
      } catch (NoSuchColumnFamilyException e) {
        return;
      } catch (RetriesExhaustedWithDetailsException e) {
        for (Throwable rootCause : e.getCauses()) {
          if (rootCause instanceof NoSuchColumnFamilyException) {
            return;
          }
        }
        throw e;
      }
    }
  }

  @Test
  public void testBatchAppendWithReturnResultFalse() throws Exception {
    LOG.info("Starting testBatchAppendWithReturnResultFalse");
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      Append append1 = new Append(Bytes.toBytes("row1"));
      append1.setReturnResults(false);
      append1.addColumn(FAMILY, Bytes.toBytes("f1"), Bytes.toBytes("value1"));
      Append append2 = new Append(Bytes.toBytes("row1"));
      append2.setReturnResults(false);
      append2.addColumn(FAMILY, Bytes.toBytes("f1"), Bytes.toBytes("value2"));
      List<Append> appends = new ArrayList<>();
      appends.add(append1);
      appends.add(append2);
      Object[] results = new Object[2];
      table.batch(appends, results);
      assertTrue(results.length == 2);
      for (Object r : results) {
        Result result = (Result) r;
        assertTrue(result.isEmpty());
      }
    }
  }

  @Test
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[] v1 = Bytes.toBytes("42");
      byte[] v2 = Bytes.toBytes("23");
      byte[][] QUALIFIERS = new byte[][]{
              Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c")
      };
      Append a = new Append(ROW);
      a.addColumn(FAMILY, QUALIFIERS[0], v1);
      a.addColumn(FAMILY, QUALIFIERS[1], v2);
      a.setReturnResults(false);
      assertEmptyResult(t.append(a));

      a = new Append(ROW);
      a.addColumn(FAMILY, QUALIFIERS[0], v2);
      a.addColumn(FAMILY, QUALIFIERS[1], v1);
      a.addColumn(FAMILY, QUALIFIERS[2], v2);
      Result r = t.append(a);
      assertEquals(0, Bytes.compareTo(Bytes.add(v1, v2), r.getValue(FAMILY, QUALIFIERS[0])));
      assertEquals(0, Bytes.compareTo(Bytes.add(v2, v1), r.getValue(FAMILY, QUALIFIERS[1])));
      // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
      assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
      assertEquals(r.getColumnLatestCell(FAMILY, QUALIFIERS[0]).getTimestamp(),
              r.getColumnLatestCell(FAMILY, QUALIFIERS[2]).getTimestamp());
    }
  }
  private List<Result> doAppend(final boolean walUsed) throws IOException {
    LOG.info("Starting testAppend, walUsed is " + walUsed);
    final TableName TABLENAME =
            TableName.valueOf(walUsed ? "testAppendWithWAL" : "testAppendWithoutWAL");
    try (Table t = TEST_UTIL.createTable(TABLENAME, FAMILY)) {
      final byte[] row1 = Bytes.toBytes("c");
      final byte[] row2 = Bytes.toBytes("b");
      final byte[] row3 = Bytes.toBytes("a");
      final byte[] qual = Bytes.toBytes("qual");
      Put put_0 = new Put(row2);
      put_0.addColumn(FAMILY, qual, Bytes.toBytes("put"));
      Put put_1 = new Put(row3);
      put_1.addColumn(FAMILY, qual, Bytes.toBytes("put"));
      Append append_0 = new Append(row1);
      append_0.addColumn(FAMILY, qual, Bytes.toBytes("i"));
      Append append_1 = new Append(row1);
      append_1.addColumn(FAMILY, qual, Bytes.toBytes("k"));
      Append append_2 = new Append(row1);
      append_2.addColumn(FAMILY, qual, Bytes.toBytes("e"));
      if (!walUsed) {
        append_2.setDurability(Durability.SKIP_WAL);
      }
      Append append_3 = new Append(row1);
      append_3.addColumn(FAMILY, qual, Bytes.toBytes("a"));
      Scan s = new Scan();
      s.setCaching(1);
      t.append(append_0);
      t.put(put_0);
      t.put(put_1);
      List<Result> results = new LinkedList<>();
      try (ResultScanner scanner = t.getScanner(s)) {
        t.append(append_1);
        t.append(append_2);
        t.append(append_3);
        for (Result r : scanner) {
          results.add(r);
        }
      }
      TEST_UTIL.deleteTable(TABLENAME);
      return results;
    }
  }

  @Test
  public void testAppendWithoutWAL() throws Exception {
    List<Result> resultsWithWal = doAppend(true);
    List<Result> resultsWithoutWal = doAppend(false);
    assertEquals(resultsWithWal.size(), resultsWithoutWal.size());
    for (int i = 0; i != resultsWithWal.size(); ++i) {
      Result resultWithWal = resultsWithWal.get(i);
      Result resultWithoutWal = resultsWithoutWal.get(i);
      assertEquals(resultWithWal.rawCells().length, resultWithoutWal.rawCells().length);
      for (int j = 0; j != resultWithWal.rawCells().length; ++j) {
        Cell cellWithWal = resultWithWal.rawCells()[j];
        Cell cellWithoutWal = resultWithoutWal.rawCells()[j];
        assertArrayEquals(CellUtil.cloneRow(cellWithWal), CellUtil.cloneRow(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneFamily(cellWithWal), CellUtil.cloneFamily(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneQualifier(cellWithWal),
          CellUtil.cloneQualifier(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneValue(cellWithWal), CellUtil.cloneValue(cellWithoutWal));
      }
    }
  }

  @Test
  public void testClientPoolRoundRobin() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    try (Table table =
                 TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, Integer.MAX_VALUE)) {

      final long ts = EnvironmentEdgeManager.currentTime();
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readAllVersions();

      for (int versions = 1; versions <= numVersions; versions++) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, ts + versions, VALUE);
        table.put(put);

        Result result = table.get(get);
        NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
                .get(QUALIFIER);

        assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
        for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
          assertTrue("The value at time " + entry.getKey()
                          + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
        }
      }
    }
  }

  @Ignore ("Flakey: HBASE-8989") @Test
  public void testClientPoolThreadLocal() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    try (final Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY },  3)) {

      final long ts = EnvironmentEdgeManager.currentTime();
      final Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readAllVersions();

      for (int versions = 1; versions <= numVersions; versions++) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, ts + versions, VALUE);
        table.put(put);

        Result result = table.get(get);
        NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
                .get(QUALIFIER);

        assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
        for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
          assertTrue("The value at time " + entry.getKey()
                          + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
        }
      }

      final Object waitLock = new Object();
      ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
      final AtomicReference<AssertionError> error = new AtomicReference<>(null);
      for (int versions = numVersions; versions < numVersions * 2; versions++) {
        final int versionsCopy = versions;
        executorService.submit(new Callable<Void>() {
          @Override
          public Void call() {
            try {
              Put put = new Put(ROW);
              put.addColumn(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
              table.put(put);

              Result result = table.get(get);
              NavigableMap<Long, byte[]> navigableMap = result.getMap()
                      .get(FAMILY).get(QUALIFIER);

              assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                      + Bytes.toString(QUALIFIER) + " did not match " + versionsCopy, versionsCopy,
                      navigableMap.size());
              for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
                assertTrue("The value at time " + entry.getKey()
                                + " did not match what was put",
                        Bytes.equals(VALUE, entry.getValue()));
              }
              synchronized (waitLock) {
                waitLock.wait();
              }
            } catch (Exception e) {
            } catch (AssertionError e) {
              // the error happens in a thread, it won't fail the test,
              // need to pass it to the caller for proper handling.
              error.set(e);
              LOG.error(e.toString(), e);
            }

            return null;
          }
        });
      }
      synchronized (waitLock) {
        waitLock.notifyAll();
      }
      executorService.shutdownNow();
      assertNull(error.get());
    }
  }

  @Test
  public void testCheckAndPut() throws IOException {
    final byte [] anotherrow = Bytes.toBytes("anotherrow");
    final byte [] value2 = Bytes.toBytes("abcd");

    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {
      Put put1 = new Put(ROW);
      put1.addColumn(FAMILY, QUALIFIER, VALUE);

      // row doesn't exist, so using non-null value should be considered "not match".
      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifEquals(VALUE).thenPut(put1);
      assertFalse(ok);

      // row doesn't exist, so using "ifNotExists" should be considered "match".
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put1);
      assertTrue(ok);

      // row now exists, so using "ifNotExists" should be considered "not match".
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put1);
      assertFalse(ok);

      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);

      // row now exists, use the matching value to check
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(VALUE).thenPut(put2);
      assertTrue(ok);

      Put put3 = new Put(anotherrow);
      put3.addColumn(FAMILY, QUALIFIER, VALUE);

      // try to do CheckAndPut on different rows
      try {
        table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(value2).thenPut(put3);
        fail("trying to check and modify different rows should have failed.");
      } catch (Exception e) {
      }
    }
  }

  @Test
  public void testCheckAndMutateWithTimeRange() throws IOException {
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {
      final long ts = System.currentTimeMillis() / 2;
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, ts, VALUE);

      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifNotExists()
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts + 10000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.from(ts + 10000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.between(ts + 10000, ts + 20000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.until(ts))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.from(ts))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.between(ts, ts + 20000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.until(ts + 10000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      RowMutations rm = new RowMutations(ROW)
              .add((Mutation) put);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts + 10000))
              .ifEquals(VALUE)
              .thenMutate(rm);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts))
              .ifEquals(VALUE)
              .thenMutate(rm);
      assertTrue(ok);

      Delete delete = new Delete(ROW)
              .addColumn(FAMILY, QUALIFIER);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts + 10000))
              .ifEquals(VALUE)
              .thenDelete(delete);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts))
              .ifEquals(VALUE)
              .thenDelete(delete);
      assertTrue(ok);
    }
  }

  @Test
  public void testCheckAndPutWithCompareOp() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {

      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);

      Put put3 = new Put(ROW);
      put3.addColumn(FAMILY, QUALIFIER, value3);

      // row doesn't exist, so using "ifNotExists" should be considered "match".
      boolean ok =
              table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put2);
      assertTrue(ok);

      // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value1).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value1).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value1).thenPut(put3);
      assertTrue(ok);

      // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value4).thenPut(put3);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value4).thenPut(put3);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value4).thenPut(put2);
      assertTrue(ok);

      // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value2).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value2).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value2).thenPut(put3);
      assertTrue(ok);
    }
  }

  @Test
  public void testCheckAndDelete() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");

    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
        FAMILY)) {

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, value1);
      table.put(put);

      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, QUALIFIER);

      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifEquals(value1).thenDelete(delete);
      assertTrue(ok);
    }
  }

  @Test
  public void testCheckAndDeleteWithCompareOp() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
        FAMILY)) {

      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);
      table.put(put2);

      Put put3 = new Put(ROW);
      put3.addColumn(FAMILY, QUALIFIER, value3);

      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, QUALIFIER);

      // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value1).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value1).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value1).thenDelete(delete);
      assertTrue(ok);

      // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value4).thenDelete(delete);
      assertTrue(ok);
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value4).thenDelete(delete);
      assertTrue(ok);
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value4).thenDelete(delete);
      assertTrue(ok);

      // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
      // turns out "match"
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
    }
  }

  /**
  * Test ScanMetrics
  */
  @Test
  @SuppressWarnings ("unused")
  public void testScanMetrics() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    // Set up test table:
    // Create table:
    try (Table ht = TEST_UTIL.createMultiRegionTable(tableName, FAMILY)) {
      int numOfRegions = -1;
      try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        numOfRegions = r.getStartKeys().length;
      }
      // Create 3 rows in the table, with rowkeys starting with "zzz*" so that
      // scan are forced to hit all the regions.
      Put put1 = new Put(Bytes.toBytes("zzz1"));
      put1.addColumn(FAMILY, QUALIFIER, VALUE);
      Put put2 = new Put(Bytes.toBytes("zzz2"));
      put2.addColumn(FAMILY, QUALIFIER, VALUE);
      Put put3 = new Put(Bytes.toBytes("zzz3"));
      put3.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(Arrays.asList(put1, put2, put3));

      Scan scan1 = new Scan();
      int numRecords = 0;
      try (ResultScanner scanner = ht.getScanner(scan1)) {
        for (Result result : scanner) {
          numRecords++;
        }

        LOG.info("test data has " + numRecords + " records.");

        // by default, scan metrics collection is turned off
        assertEquals(null, scanner.getScanMetrics());
      }

      // turn on scan metrics
      Scan scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(numRecords + 1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        for (Result result : scanner.next(numRecords - 1)) {
        }
        scanner.close();
        // closing the scanner will set the metrics.
        assertNotNull(scanner.getScanMetrics());
      }

      // set caching to 1, because metrics are collected in each roundtrip only
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        // per HBASE-5717, this should still collect even if you don't run all the way to
        // the end of the scanner. So this is asking for 2 of the 3 rows we inserted.
        for (Result result : scanner.next(numRecords - 1)) {
        }

        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals("Did not access all the regions in the table", numOfRegions,
                scanMetrics.countOfRegions.get());
      }

      // check byte counters
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        int numBytes = 0;
        for (Result result : scanner.next(1)) {
          for (Cell cell : result.listCells()) {
            numBytes += PrivateCellUtil.estimatedSerializedSizeOf(cell);
          }
        }
        scanner.close();
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals("Did not count the result bytes", numBytes,
                scanMetrics.countOfBytesInResults.get());
      }

      // check byte counters on a small scan
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      scan2.setSmall(true);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        int numBytes = 0;
        for (Result result : scanner.next(1)) {
          for (Cell cell : result.listCells()) {
            numBytes += PrivateCellUtil.estimatedSerializedSizeOf(cell);
          }
        }
        scanner.close();
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals("Did not count the result bytes", numBytes,
                scanMetrics.countOfBytesInResults.get());
      }

      // now, test that the metrics are still collected even if you don't call close, but do
      // run past the end of all the records
      /** There seems to be a timing issue here.  Comment out for now. Fix when time.
       Scan scanWithoutClose = new Scan();
       scanWithoutClose.setCaching(1);
       scanWithoutClose.setScanMetricsEnabled(true);
       ResultScanner scannerWithoutClose = ht.getScanner(scanWithoutClose);
       for (Result result : scannerWithoutClose.next(numRecords + 1)) {
       }
       ScanMetrics scanMetricsWithoutClose = getScanMetrics(scanWithoutClose);
       assertEquals("Did not access all the regions in the table", numOfRegions,
       scanMetricsWithoutClose.countOfRegions.get());
       */

      // finally,
      // test that the metrics are collected correctly if you both run past all the records,
      // AND close the scanner
      Scan scanWithClose = new Scan();
      // make sure we can set caching up to the number of a scanned values
      scanWithClose.setCaching(numRecords);
      scanWithClose.setScanMetricsEnabled(true);
      try (ResultScanner scannerWithClose = ht.getScanner(scanWithClose)) {
        for (Result result : scannerWithClose.next(numRecords + 1)) {
        }
        scannerWithClose.close();
        ScanMetrics scanMetricsWithClose = scannerWithClose.getScanMetrics();
        assertEquals("Did not access all the regions in the table", numOfRegions,
                scanMetricsWithClose.countOfRegions.get());
      }
    }
  }

  /**
   * Tests that cache on write works all the way up from the client-side.
   *
   * Performs inserts, flushes, and compactions, verifying changes in the block
   * cache along the way.
   */
  @Test
  public void testCacheOnWriteEvictOnClose() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [] data = Bytes.toBytes("data");
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        // get the block cache and region
        String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();

        HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName)
                .getRegion(regionName);
        HStore store = region.getStores().iterator().next();
        CacheConfig cacheConf = store.getCacheConfig();
        cacheConf.setCacheDataOnWrite(true);
        cacheConf.setEvictOnClose(true);
        BlockCache cache = cacheConf.getBlockCache().get();

        // establish baseline stats
        long startBlockCount = cache.getBlockCount();
        long startBlockHits = cache.getStats().getHitCount();
        long startBlockMiss = cache.getStats().getMissCount();

        // wait till baseline is stable, (minimal 500 ms)
        for (int i = 0; i < 5; i++) {
          Thread.sleep(100);
          if (startBlockCount != cache.getBlockCount()
                  || startBlockHits != cache.getStats().getHitCount()
                  || startBlockMiss != cache.getStats().getMissCount()) {
            startBlockCount = cache.getBlockCount();
            startBlockHits = cache.getStats().getHitCount();
            startBlockMiss = cache.getStats().getMissCount();
            i = -1;
          }
        }

        // insert data
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, data);
        table.put(put);
        assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));

        // data was in memstore so don't expect any changes
        assertEquals(startBlockCount, cache.getBlockCount());
        assertEquals(startBlockHits, cache.getStats().getHitCount());
        assertEquals(startBlockMiss, cache.getStats().getMissCount());

        // flush the data
        LOG.debug("Flushing cache");
        region.flush(true);

        // expect two more blocks in cache - DATA and ROOT_INDEX
        // , no change in hits/misses
        long expectedBlockCount = startBlockCount + 2;
        long expectedBlockHits = startBlockHits;
        long expectedBlockMiss = startBlockMiss;
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // read the data and expect same blocks, one new hit, no misses
        assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // insert a second column, read the row, no new blocks, one new hit
        byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
        byte[] data2 = Bytes.add(data, data);
        put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER2, data2);
        table.put(put);
        Result r = table.get(new Get(ROW));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // flush, one new block
        System.out.println("Flushing cache");
        region.flush(true);

        // + 1 for Index Block, +1 for data block
        expectedBlockCount += 2;
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // compact, net minus two blocks, two hits, no misses
        System.out.println("Compacting");
        assertEquals(2, store.getStorefilesCount());
        store.triggerMajorCompaction();
        region.compact(true);
        store.closeAndArchiveCompactedFiles();
        waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
        assertEquals(1, store.getStorefilesCount());
        // evicted two data blocks and two index blocks and compaction does not cache new blocks
        expectedBlockCount = 0;
        assertEquals(expectedBlockCount, cache.getBlockCount());
        expectedBlockHits += 2;
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        // read the row, this should be a cache miss because we don't cache data
        // blocks on compaction
        r = table.get(new Get(ROW));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
        expectedBlockCount += 1; // cached one data block
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(++expectedBlockMiss, cache.getStats().getMissCount());
      }
    }
  }

  private void waitForStoreFileCount(HStore store, int count, int timeout)
      throws InterruptedException {
    long start = System.currentTimeMillis();
    while (start + timeout > System.currentTimeMillis() && store.getStorefilesCount() != count) {
      Thread.sleep(100);
    }
    System.out.println("start=" + start + ", now=" + System.currentTimeMillis() + ", cur=" +
        store.getStorefilesCount());
    assertEquals(count, store.getStorefilesCount());
  }

  @Test
  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte [] family1 = Bytes.toBytes("f1");
    byte [] family2 = Bytes.toBytes("f2");
    try (Table table = TEST_UTIL.createTable(tableName, new byte[][] {family1, family2}, 10);
        Admin admin = TEST_UTIL.getAdmin();
        RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      List<HRegionLocation> allRegionLocations = locator.getAllRegionLocations();
      assertEquals(1, allRegionLocations.size());
      RegionInfo regionInfo = allRegionLocations.get(0).getRegion();
      ServerName addrBefore = allRegionLocations.get(0).getServerName();
      // Verify region location before move.
      HRegionLocation addrCache = locator.getRegionLocation(regionInfo.getStartKey(), false);
      HRegionLocation addrNoCache = locator.getRegionLocation(regionInfo.getStartKey(),  true);

      assertEquals(addrBefore.getPort(), addrCache.getPort());
      assertEquals(addrBefore.getPort(), addrNoCache.getPort());

      ServerName addrAfter = null;
      // Now move the region to a different server.
      for (int i = 0; i < SLAVES; i++) {
        HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        ServerName addr = regionServer.getServerName();
        if (addr.getPort() != addrBefore.getPort()) {
          admin.move(regionInfo.getEncodedNameAsBytes(), addr);
          // Wait for the region to move.
          Thread.sleep(5000);
          addrAfter = addr;
          break;
        }
      }

      // Verify the region was moved.
      addrCache = locator.getRegionLocation(regionInfo.getStartKey(), false);
      addrNoCache = locator.getRegionLocation(regionInfo.getStartKey(), true);
      assertNotNull(addrAfter);
      assertTrue(addrAfter.getPort() != addrCache.getPort());
      assertEquals(addrAfter.getPort(), addrNoCache.getPort());
    }
  }

  @Test
  /**
   * Tests getRegionsInRange by creating some regions over which a range of
   * keys spans; then changing the key range.
   */
  public void testGetRegionsInRange() throws Exception {
    // Test Initialization.
    byte [] startKey = Bytes.toBytes("ddc");
    byte [] endKey = Bytes.toBytes("mmm");
    TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tableName, new byte[][] { FAMILY }, 10);

    int numOfRegions = -1;
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      numOfRegions = r.getStartKeys().length;
    }
    assertEquals(26, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = getRegionsInRange(tableName, HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = getRegionsInRange(tableName, startKey, HConstants.EMPTY_END_ROW);
    assertEquals(21, regionsList.size());

    // Both start and end keys empty
    regionsList = getRegionsInRange(tableName, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW);
    assertEquals(26, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("zzz1");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(21, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(26, regionsList.size());

    // Make start and end key the same
    startKey = endKey = Bytes.toBytes("ccc");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(1, regionsList.size());
  }

  private List<HRegionLocation> getRegionsInRange(TableName tableName, byte[] startKey,
      byte[] endKey) throws IOException {
    List<HRegionLocation> regionsInRange = new ArrayList<>();
    byte[] currentKey = startKey;
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      do {
        HRegionLocation regionLocation = r.getRegionLocation(currentKey);
        regionsInRange.add(regionLocation);
        currentKey = regionLocation.getRegion().getEndKey();
      } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
          && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
      return regionsInRange;
    }
  }

  @Test
  public void testJira6912() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table foo = TEST_UTIL.createTable(tableName, new byte[][] {FAMILY}, 10)) {

      List<Put> puts = new ArrayList<Put>();
      for (int i = 0; i != 100; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(FAMILY, FAMILY, Bytes.toBytes(i));
        puts.add(put);
      }
      foo.put(puts);
      // If i comment this out it works
      TEST_UTIL.flush();

      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes(1));
      scan.setStopRow(Bytes.toBytes(3));
      scan.addColumn(FAMILY, FAMILY);
      scan.setFilter(new RowFilter(CompareOperator.NOT_EQUAL,
              new BinaryComparator(Bytes.toBytes(1))));

      try (ResultScanner scanner = foo.getScanner(scan)) {
        Result[] bar = scanner.next(100);
        assertEquals(1, bar.length);
      }
    }
  }

  @Test
  public void testScan_NullQualifier() throws IOException {
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      table.put(put);
      LOG.info("Row put");

      Scan scan = new Scan();
      scan.addColumn(FAMILY, null);

      ResultScanner scanner = table.getScanner(scan);
      Result[] bar = scanner.next(100);
      assertEquals(1, bar.length);
      assertEquals(1, bar[0].size());

      scan = new Scan();
      scan.addFamily(FAMILY);

      scanner = table.getScanner(scan);
      bar = scanner.next(100);
      assertEquals(1, bar.length);
      assertEquals(2, bar[0].size());
    }
  }

  @Test
  public void testNegativeTimestamp() throws IOException {
    try (Table table = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY)) {

      try {
        Put put = new Put(ROW, -1);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Put put = new Put(ROW);
        long ts = -1;
        put.addColumn(FAMILY, QUALIFIER, ts, VALUE);
        table.put(put);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Delete delete = new Delete(ROW, -1);
        table.delete(delete);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Delete delete = new Delete(ROW);
        delete.addFamily(FAMILY, -1);
        table.delete(delete);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Scan scan = new Scan();
        scan.setTimeRange(-1, 1);
        table.getScanner(scan);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      // KeyValue should allow negative timestamps for backwards compat. Otherwise, if the user
      // already has negative timestamps in cluster data, HBase won't be able to handle that
      try {
        new KeyValue(Bytes.toBytes(42), Bytes.toBytes(42), Bytes.toBytes(42), -1,
                Bytes.toBytes(42));
      } catch (IllegalArgumentException ex) {
        fail("KeyValue SHOULD allow negative timestamps");
      }

    }
  }

  @Test
  public void testRawScanRespectsVersions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[] row = Bytes.toBytes("row");

      // put the same row 4 times, with different values
      Put p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 10, VALUE);
      table.put(p);
      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 11, ArrayUtils.add(VALUE, (byte) 2));
      table.put(p);

      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 12, ArrayUtils.add(VALUE, (byte) 3));
      table.put(p);

      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 13, ArrayUtils.add(VALUE, (byte) 4));
      table.put(p);

      int versions = 4;
      Scan s = new Scan(row);
      // get all the possible versions
      s.setMaxVersions();
      s.setRaw(true);

      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals("Found an unexpected number of results for the row!", versions,
                  r.listCells().size());
          count++;
        }
        assertEquals("Found more than a single row when raw scanning the table with a single row!",
                1, count);
      }

      // then if we decrease the number of versions, but keep the scan raw, we should see exactly
      // that number of versions
      versions = 2;
      s.setMaxVersions(versions);
      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals("Found an unexpected number of results for the row!", versions,
                  r.listCells().size());
          count++;
        }
        assertEquals("Found more than a single row when raw scanning the table with a single row!",
                1, count);
      }

      // finally, if we turn off raw scanning, but max out the number of versions, we should go back
      // to seeing just three
      versions = 3;
      s.setMaxVersions(versions);
      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals("Found an unexpected number of results for the row!", versions,
                  r.listCells().size());
          count++;
        }
        assertEquals("Found more than a single row when raw scanning the table with a single row!",
                1, count);
      }

    }
    TEST_UTIL.deleteTable(tableName);
  }

  @Test
  public void testEmptyFilterList() throws Exception {
    // Test Initialization.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Insert one row each region
      Put put = new Put(Bytes.toBytes("row"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      List<Result> scanResults = new LinkedList<>();
      Scan scan = new Scan();
      scan.setFilter(new FilterList());
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          scanResults.add(r);
        }
      }
      assertEquals(1, scanResults.size());
      Get g = new Get(Bytes.toBytes("row"));
      g.setFilter(new FilterList());
      Result getResult = table.get(g);
      Result scanResult = scanResults.get(0);
      assertEquals(scanResult.rawCells().length, getResult.rawCells().length);
      for (int i = 0; i != scanResult.rawCells().length; ++i) {
        Cell scanCell = scanResult.rawCells()[i];
        Cell getCell = getResult.rawCells()[i];
        assertEquals(0, Bytes.compareTo(CellUtil.cloneRow(scanCell),
                CellUtil.cloneRow(getCell)));
        assertEquals(0, Bytes.compareTo(CellUtil.cloneFamily(scanCell),
                CellUtil.cloneFamily(getCell)));
        assertEquals(0, Bytes.compareTo(CellUtil.cloneQualifier(scanCell),
                CellUtil.cloneQualifier(getCell)));
        assertEquals(0, Bytes.compareTo(CellUtil.cloneValue(scanCell),
                CellUtil.cloneValue(getCell)));
      }
    }
  }

  @Test
  public void testSmallScan() throws Exception {
    // Test Initialization.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Insert one row each region
      int insertNum = 10;
      for (int i = 0; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("row" + String.format("%03d", i)));
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // normal scan
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertTrue(!r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }

      // small scan
      Scan scan = new Scan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      scan.setSmall(true);
      scan.setCaching(2);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int count = 0;
        for (Result r : scanner) {
          assertTrue(!r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }
    }
  }

  @Test
  public void testSuperSimpleWithReverseScan() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      Put put = new Put(Bytes.toBytes("0-b11111-0000000000000000000"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000002"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000004"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000006"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000008"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000001"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000003"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000005"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000007"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000009"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      Scan scan = new Scan(Bytes.toBytes("0-b11111-9223372036854775807"),
              Bytes.toBytes("0-b11111-0000000000000000000"));
      scan.setReversed(true);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        Result result = scanner.next();
        assertTrue(Bytes.equals(result.getRow(),
                Bytes.toBytes("0-b11111-0000000000000000008")));
      }
    }
  }

  @Test
  public void testFiltersWithReverseScan() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS = {Bytes.toBytes("col0-<d2v1>-<d3v2>"),
              Bytes.toBytes("col1-<d2v1>-<d3v2>"),
              Bytes.toBytes("col2-<d2v1>-<d3v2>"),
              Bytes.toBytes("col3-<d2v1>-<d3v2>"),
              Bytes.toBytes("col4-<d2v1>-<d3v2>"),
              Bytes.toBytes("col5-<d2v1>-<d3v2>"),
              Bytes.toBytes("col6-<d2v1>-<d3v2>"),
              Bytes.toBytes("col7-<d2v1>-<d3v2>"),
              Bytes.toBytes("col8-<d2v1>-<d3v2>"),
              Bytes.toBytes("col9-<d2v1>-<d3v2>")};
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      Filter filter = new QualifierFilter(CompareOperator.EQUAL,
              new RegexStringComparator("col[1-5]"));
      scan.setFilter(filter);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int expectedIndex = 5;
        for (Result result : scanner) {
          assertEquals(1, result.size());
          Cell c = result.rawCells()[0];
          assertTrue(Bytes.equals(c.getRowArray(), c.getRowOffset(), c.getRowLength(),
                  ROWS[expectedIndex], 0, ROWS[expectedIndex].length));
          assertTrue(Bytes.equals(c.getQualifierArray(), c.getQualifierOffset(),
                  c.getQualifierLength(), QUALIFIERS[expectedIndex], 0,
                  QUALIFIERS[expectedIndex].length));
          expectedIndex--;
        }
        assertEquals(0, expectedIndex);
      }
    }
  }

  @Test
  public void testKeyOnlyFilterWithReverseScan() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS = {Bytes.toBytes("col0-<d2v1>-<d3v2>"),
              Bytes.toBytes("col1-<d2v1>-<d3v2>"),
              Bytes.toBytes("col2-<d2v1>-<d3v2>"),
              Bytes.toBytes("col3-<d2v1>-<d3v2>"),
              Bytes.toBytes("col4-<d2v1>-<d3v2>"),
              Bytes.toBytes("col5-<d2v1>-<d3v2>"),
              Bytes.toBytes("col6-<d2v1>-<d3v2>"),
              Bytes.toBytes("col7-<d2v1>-<d3v2>"),
              Bytes.toBytes("col8-<d2v1>-<d3v2>"),
              Bytes.toBytes("col9-<d2v1>-<d3v2>")};
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      Filter filter = new KeyOnlyFilter(true);
      scan.setFilter(filter);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int count = 0;
        for (Result result : ht.getScanner(scan)) {
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
  @Test
  public void testSimpleMissingWithReverseScan() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 4);

      // Try to get a row on an empty table
      Scan scan = new Scan();
      scan.setReversed(true);
      Result result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan(ROWS[0]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan(ROWS[0], ROWS[1]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.setReversed(true);
      scan.addColumn(FAMILY, QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Insert a row

      Put put = new Put(ROWS[2]);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);

      // Make sure we can scan the row
      scan = new Scan();
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan(ROWS[3], ROWS[0]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan(ROWS[2], ROWS[1]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      // Try to scan empty rows around it
      // Introduced MemStore#shouldSeekForReverseScan to fix the following
      scan = new Scan(ROWS[1]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);
    }
  }

  @Test
  public void testNullWithReverseScan() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Null qualifier (should work)
      Put put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      ht.put(put);
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, null);
      ht.delete(delete);
    }

    // Use a new table
    try (Table ht = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName() + "2"), FAMILY)) {
      // Empty qualifier, byte[0] instead of null (should work)
      Put put = new Put(ROW);
      put.addColumn(FAMILY, HConstants.EMPTY_BYTE_ARRAY, VALUE);
      ht.put(put);
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      TEST_UTIL.flush();
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, HConstants.EMPTY_BYTE_ARRAY);
      ht.delete(delete);
      // Null value
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, null);
      ht.put(put);
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addColumn(FAMILY, QUALIFIER);
      Result result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);
    }
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  public void testDeletesWithReverseScan() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
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

      Scan scan = new Scan(ROW);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      Result result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[]{ts[1]},
              new byte[][]{VALUES[1]}, 0, 0);

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

      scan = new Scan(ROW);
      scan.setReversed(true);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[]{ts[1],
              ts[2], ts[3]}, new byte[][]{VALUES[1], VALUES[2], VALUES[3]}, 0, 2);

      // Test for HBASE-1847
      delete = new Delete(ROW);
      delete.addColumn(FAMILIES[0], null);
      ht.delete(delete);

      // Cleanup null qualifier
      delete = new Delete(ROW);
      delete.addColumns(FAMILIES[0], null);
      ht.delete(delete);

      // Expected client behavior might be that you can re-put deleted values
      // But alas, this is not to be. We can't put them back in either case.

      put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
      ht.put(put);

      // The Scanner returns the previous values, the expected-naive-unexpected
      // behavior

      scan = new Scan(ROW);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[]{ts[1],
              ts[2], ts[3]}, new byte[][]{VALUES[1], VALUES[2], VALUES[3]}, 0, 2);

      // Test deleting an entire family from one row but not the other various
      // ways

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

      scan = new Scan(ROWS[0]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertTrue("Expected 2 keys but received " + result.size(),
              result.size() == 2);
      assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, new long[]{ts[0],
              ts[1]}, new byte[][]{VALUES[0], VALUES[1]}, 0, 1);

      scan = new Scan(ROWS[1]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertTrue("Expected 2 keys but received " + result.size(),
              result.size() == 2);

      scan = new Scan(ROWS[2]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals(1, result.size());
      assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
              new long[]{ts[2]}, new byte[][]{VALUES[2]}, 0, 0);

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

      scan = new Scan(ROWS[4]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      ResultScanner scanner = ht.getScanner(scan);
      result = scanner.next();
      assertTrue("Expected 2 keys but received " + result.size(),
              result.size() == 2);
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
      result = scanner.next();
      assertTrue("Expected 1 key but received " + result.size(),
              result.size() == 1);
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
      scanner.close();
    }
  }

  /**
   * Tests reversed scan under multi regions
   */
  @Test
  public void testReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] maxByteArray = ConnectionUtils.MAX_BYTE_ARRAY;
    byte[][] splitRows = new byte[][] { Bytes.toBytes("005"),
        Bytes.add(Bytes.toBytes("005"), Bytes.multiple(maxByteArray, 16)),
        Bytes.toBytes("006"),
        Bytes.add(Bytes.toBytes("006"), Bytes.multiple(maxByteArray, 8)),
        Bytes.toBytes("007"),
        Bytes.add(Bytes.toBytes("007"), Bytes.multiple(maxByteArray, 4)),
        Bytes.toBytes("008"), Bytes.multiple(maxByteArray, 2) };
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY, splitRows)) {
      TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());

      try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        assertEquals(splitRows.length + 1, l.getAllRegionLocations().size());
      }
      // Insert one row each region
      int insertNum = splitRows.length;
      for (int i = 0; i < insertNum; i++) {
        Put put = new Put(splitRows[i]);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // scan forward
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertTrue(!r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }

      // scan backward
      Scan scan = new Scan();
      scan.setReversed(true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int count = 0;
        byte[] lastRow = null;
        for (Result r : scanner) {
          assertTrue(!r.isEmpty());
          count++;
          byte[] thisRow = r.getRow();
          if (lastRow != null) {
            assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                            + ",this row=" + Bytes.toString(thisRow),
                    Bytes.compareTo(thisRow, lastRow) < 0);
          }
          lastRow = thisRow;
        }
        assertEquals(insertNum, count);
      }
    }
  }

  /**
   * Tests reversed scan under multi regions
   */
  @Test
  public void testSmallReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[][] splitRows = new byte[][]{
        Bytes.toBytes("000"), Bytes.toBytes("002"), Bytes.toBytes("004"),
        Bytes.toBytes("006"), Bytes.toBytes("008"), Bytes.toBytes("010")};
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY, splitRows)) {
      TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());

      try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        assertEquals(splitRows.length + 1, l.getAllRegionLocations().size());
      }
      for (byte[] splitRow : splitRows) {
        Put put = new Put(splitRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);

        byte[] nextRow = Bytes.copy(splitRow);
        nextRow[nextRow.length - 1]++;

        put = new Put(nextRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // scan forward
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertTrue(!r.isEmpty());
          count++;
        }
        assertEquals(12, count);
      }

      reverseScanTest(table, false);
      reverseScanTest(table, true);
    }
  }

  private void reverseScanTest(Table table, boolean small) throws IOException {
    // scan backward
    Scan scan = new Scan();
    scan.setReversed(true);
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(12, count);
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("002"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(3, count); // 000 001 002
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("002"));
    scan.setStopRow(Bytes.toBytes("000"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(2, count); // 001 002
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("001"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(2, count); // 000 001
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("000"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(1, count); // 000
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("006"));
    scan.setStopRow(Bytes.toBytes("002"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(4, count); // 003 004 005 006
    }
  }

  @Test
  public void testFilterAllRecords() throws IOException {
    Scan scan = new Scan();
    scan.setBatch(1);
    scan.setCaching(1);
    // Filter out any records
    scan.setFilter(new FilterList(new FirstKeyOnlyFilter(), new InclusiveStopFilter(new byte[0])));
    try (Table table = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      try (ResultScanner s = table.getScanner(scan)) {
        assertNull(s.next());
      }
    }
  }

  @Test
  public void testCellSizeLimit() throws IOException {
    final TableName tableName = TableName.valueOf("testCellSizeLimit");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setConfiguration(HRegion.HBASE_MAX_CELL_SIZE_KEY, Integer.toString(10 * 1024)); // 10K
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    htd.addFamily(fam);
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.createTable(htd);
    }
    // Will succeed
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(0L)));
      t.increment(new Increment(ROW).addColumn(FAMILY, QUALIFIER, 1L));
    }
    // Will succeed
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, new byte[9*1024]));
    }
    // Will fail
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      try {
        t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, new byte[10 * 1024]));
        fail("Oversize cell failed to trigger exception");
      } catch (IOException e) {
        // expected
      }
      try {
        t.append(new Append(ROW).addColumn(FAMILY, QUALIFIER, new byte[2 * 1024]));
        fail("Oversize cell failed to trigger exception");
      } catch (IOException e) {
        // expected
      }
    }
  }

  @Test
  public void testDeleteSpecifiedVersionOfSpecifiedColumn() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      final TableName tableName = TableName.valueOf(name.getMethodName());

      byte[][] VALUES = makeN(VALUE, 5);
      long[] ts = {1000, 2000, 3000, 4000, 5000};

      try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 5)) {

        Put put = new Put(ROW);
        // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
        for (int t = 0; t < 4; t++) {
          put.addColumn(FAMILY, QUALIFIER, ts[t], VALUES[t]);
        }
        ht.put(put);

        Delete delete = new Delete(ROW);
        // Delete version 3000 of column FAMILY:QUALIFIER
        delete.addColumn(FAMILY, QUALIFIER, ts[2]);
        ht.delete(delete);

        Get get = new Get(ROW);
        get.addColumn(FAMILY, QUALIFIER);
        get.readVersions(Integer.MAX_VALUE);
        Result result = ht.get(get);
        // verify version 1000,2000,4000 remains for column FAMILY:QUALIFIER
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[1], ts[3]}, new byte[][]{
                VALUES[0], VALUES[1], VALUES[3]}, 0, 2);

        delete = new Delete(ROW);
        // Delete a version 5000 of column FAMILY:QUALIFIER which didn't exist
        delete.addColumn(FAMILY, QUALIFIER, ts[4]);
        ht.delete(delete);

        get = new Get(ROW);
        get.addColumn(FAMILY, QUALIFIER);
        get.readVersions(Integer.MAX_VALUE);
        result = ht.get(get);
        // verify version 1000,2000,4000 remains for column FAMILY:QUALIFIER
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[1], ts[3]}, new byte[][]{
                VALUES[0], VALUES[1], VALUES[3]}, 0, 2);
      }
    }
  }

  @Test
  public void testDeleteLatestVersionOfSpecifiedColumn() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      final TableName tableName = TableName.valueOf(name.getMethodName());

      byte[][] VALUES = makeN(VALUE, 5);
      long[] ts = {1000, 2000, 3000, 4000, 5000};

      try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 5)) {

        Put put = new Put(ROW);
        // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
        for (int t = 0; t < 4; t++) {
          put.addColumn(FAMILY, QUALIFIER, ts[t], VALUES[t]);
        }
        ht.put(put);

        Delete delete = new Delete(ROW);
        // Delete latest version of column FAMILY:QUALIFIER
        delete.addColumn(FAMILY, QUALIFIER);
        ht.delete(delete);

        Get get = new Get(ROW);
        get.addColumn(FAMILY, QUALIFIER);
        get.readVersions(Integer.MAX_VALUE);
        Result result = ht.get(get);
        // verify version 1000,2000,3000 remains for column FAMILY:QUALIFIER
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[1], ts[2]}, new byte[][]{
                VALUES[0], VALUES[1], VALUES[2]}, 0, 2);

        delete = new Delete(ROW);
        // Delete two latest version of column FAMILY:QUALIFIER
        delete.addColumn(FAMILY, QUALIFIER);
        delete.addColumn(FAMILY, QUALIFIER);
        ht.delete(delete);

        get = new Get(ROW);
        get.addColumn(FAMILY, QUALIFIER);
        get.readVersions(Integer.MAX_VALUE);
        result = ht.get(get);
        // verify version 1000 remains for column FAMILY:QUALIFIER
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0]}, new byte[][]{VALUES[0]},
                0, 0);

        put = new Put(ROW);
        // Put a version 5000 of column FAMILY:QUALIFIER
        put.addColumn(FAMILY, QUALIFIER, ts[4], VALUES[4]);
        ht.put(put);

        get = new Get(ROW);
        get.addColumn(FAMILY, QUALIFIER);
        get.readVersions(Integer.MAX_VALUE);
        result = ht.get(get);
        // verify version 1000,5000 remains for column FAMILY:QUALIFIER
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[4]}, new byte[][]{
                VALUES[0], VALUES[4]}, 0, 1);
      }
    }
  }

  /**
   * Test for HBASE-17125
   */
  @Test
  public void testReadWithFilter() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      final TableName tableName = TableName.valueOf(name.getMethodName());
      try (Table table = TEST_UTIL.createTable(tableName, FAMILY, 3)) {

        byte[] VALUEA = Bytes.toBytes("value-a");
        byte[] VALUEB = Bytes.toBytes("value-b");
        long[] ts = {1000, 2000, 3000, 4000};

        Put put = new Put(ROW);
        // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
        for (int t = 0; t <= 3; t++) {
          if (t <= 1) {
            put.addColumn(FAMILY, QUALIFIER, ts[t], VALUEA);
          } else {
            put.addColumn(FAMILY, QUALIFIER, ts[t], VALUEB);
          }
        }
        table.put(put);

        Scan scan =
                new Scan().setFilter(new ValueFilter(CompareOperator.EQUAL,
                        new SubstringComparator("value-a")))
                        .setMaxVersions(3);
        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
                0);

        Get get =
                new Get(ROW)
                        .setFilter(new ValueFilter(CompareOperator.EQUAL,
                                new SubstringComparator("value-a")))
                        .readVersions(3);
        result = table.get(get);
        // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
                0);

        // Test with max versions 1, it should still read ts[1]
        scan =
                new Scan().setFilter(new ValueFilter(CompareOperator.EQUAL,
                        new SubstringComparator("value-a")))
                        .setMaxVersions(1);
        scanner = table.getScanner(scan);
        result = scanner.next();
        // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
                0);

        // Test with max versions 1, it should still read ts[1]
        get =
                new Get(ROW)
                        .setFilter(new ValueFilter(CompareOperator.EQUAL,
                                new SubstringComparator("value-a")))
                        .readVersions(1);
        result = table.get(get);
        // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
                0);

        // Test with max versions 5, it should still read ts[1]
        scan =
                new Scan().setFilter(new ValueFilter(CompareOperator.EQUAL,
                        new SubstringComparator("value-a")))
                        .setMaxVersions(5);
        scanner = table.getScanner(scan);
        result = scanner.next();
        // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
                0);

        // Test with max versions 5, it should still read ts[1]
        get =
                new Get(ROW)
                        .setFilter(new ValueFilter(CompareOperator.EQUAL,
                                new SubstringComparator("value-a")))
                        .readVersions(5);
        result = table.get(get);
        // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
        assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
                0);
      }
    }
  }

  @Test
  public void testCellUtilTypeMethods() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {

      final byte[] row = Bytes.toBytes("p");
      Put p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(p);

      try (ResultScanner scanner = table.getScanner(new Scan())) {
        Result result = scanner.next();
        assertNotNull(result);
        CellScanner cs = result.cellScanner();
        assertTrue(cs.advance());
        Cell c = cs.current();
        assertTrue(CellUtil.isPut(c));
        assertFalse(CellUtil.isDelete(c));
        assertFalse(cs.advance());
        assertNull(scanner.next());
      }

      Delete d = new Delete(row);
      d.addColumn(FAMILY, QUALIFIER);
      table.delete(d);

      Scan scan = new Scan();
      scan.setRaw(true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result result = scanner.next();
        assertNotNull(result);
        CellScanner cs = result.cellScanner();
        assertTrue(cs.advance());

        // First cell should be the delete (masking the Put)
        Cell c = cs.current();
        assertTrue("Cell should be a Delete: " + c, CellUtil.isDelete(c));
        assertFalse("Cell should not be a Put: " + c, CellUtil.isPut(c));

        // Second cell should be the original Put
        assertTrue(cs.advance());
        c = cs.current();
        assertFalse("Cell should not be a Delete: " + c, CellUtil.isDelete(c));
        assertTrue("Cell should be a Put: " + c, CellUtil.isPut(c));

        // No more cells in this row
        assertFalse(cs.advance());

        // No more results in this scan
        assertNull(scanner.next());
      }
    }
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testCreateTableWithZeroRegionReplicas() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf")))
        .setRegionReplication(0)
        .build();

    TEST_UTIL.getAdmin().createTable(desc);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyTableWithZeroRegionReplicas() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf")))
        .build();

    TEST_UTIL.getAdmin().createTable(desc);
    TableDescriptor newDesc = TableDescriptorBuilder.newBuilder(desc)
        .setRegionReplication(0)
        .build();

    TEST_UTIL.getAdmin().modifyTable(newDesc);
  }

  @Test(timeout = 60000)
  public void testModifyTableWithMemstoreData() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    createTableAndValidateTableSchemaModification(tableName, true);
  }

  @Test(timeout = 60000)
  public void testDeleteCFWithMemstoreData() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    createTableAndValidateTableSchemaModification(tableName, false);
  }

  /**
   * Create table and validate online schema modification
   * @param tableName Table name
   * @param modifyTable Modify table if true otherwise delete column family
   * @throws IOException in case of failures
   */
  private void createTableAndValidateTableSchemaModification(TableName tableName,
      boolean modifyTable) throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    // Create table with two Cfs
    byte[] cf1 = Bytes.toBytes("cf1");
    byte[] cf2 = Bytes.toBytes("cf2");
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf1))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf2)).build();
    admin.createTable(tableDesc);

    Table t = TEST_UTIL.getConnection().getTable(tableName);
    // Insert few records and flush the table
    t.put(new Put(ROW).addColumn(cf1, QUALIFIER, Bytes.toBytes("val1")));
    t.put(new Put(ROW).addColumn(cf2, QUALIFIER, Bytes.toBytes("val2")));
    admin.flush(tableName);
    Path tableDir = FSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), tableName);
    List<Path> regionDirs = FSUtils.getRegionDirs(TEST_UTIL.getTestFileSystem(), tableDir);
    assertTrue(regionDirs.size() == 1);
    List<Path> familyDirs = FSUtils.getFamilyDirs(TEST_UTIL.getTestFileSystem(), regionDirs.get(0));
    assertTrue(familyDirs.size() == 2);

    // Insert record but dont flush the table
    t.put(new Put(ROW).addColumn(cf1, QUALIFIER, Bytes.toBytes("val2")));
    t.put(new Put(ROW).addColumn(cf2, QUALIFIER, Bytes.toBytes("val2")));

    if (modifyTable) {
      tableDesc = TableDescriptorBuilder.newBuilder(tableDesc).removeColumnFamily(cf2).build();
      admin.modifyTable(tableDesc);
    } else {
      admin.deleteColumnFamily(tableName, cf2);
    }
    // After table modification or delete family there should be only one CF in FS
    familyDirs = FSUtils.getFamilyDirs(TEST_UTIL.getTestFileSystem(), regionDirs.get(0));
    assertTrue("CF dir count should be 1, but was " + familyDirs.size(), familyDirs.size() == 1);
  }
}
