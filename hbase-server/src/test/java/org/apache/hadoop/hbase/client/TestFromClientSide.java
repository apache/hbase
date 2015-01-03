/**
 *
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Run tests that use the HBase clients; {@link HTable}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 */
@Category(LargeTests.class)
@SuppressWarnings ("deprecation")
public class TestFromClientSide {
  final Log LOG = LogFactory.getLog(getClass());
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  protected static int SLAVES = 3;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MultiRowMutationEndpoint.class.getName());
    conf.setBoolean("hbase.table.sanity.checks", true); // enable for below tests
    // We need more than one region server in this test
    TEST_UTIL.startMiniCluster(SLAVES);
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
   * Basic client side validation of HBASE-4536
   */
   @Test
   public void testKeepDeletedCells() throws Exception {
     final TableName TABLENAME = TableName.valueOf("testKeepDeletesCells");
     final byte[] FAMILY = Bytes.toBytes("family");
     final byte[] C0 = Bytes.toBytes("c0");

     final byte[] T1 = Bytes.toBytes("T1");
     final byte[] T2 = Bytes.toBytes("T2");
     final byte[] T3 = Bytes.toBytes("T3");
     HColumnDescriptor hcd = new HColumnDescriptor(FAMILY)
         .setKeepDeletedCells(true).setMaxVersions(3);

     HTableDescriptor desc = new HTableDescriptor(TABLENAME);
     desc.addFamily(hcd);
     TEST_UTIL.getHBaseAdmin().createTable(desc);
     Configuration c = TEST_UTIL.getConfiguration();
     Table h = new HTable(c, TABLENAME);

     long ts = System.currentTimeMillis();
     Put p = new Put(T1, ts);
     p.add(FAMILY, C0, T1);
     h.put(p);
     p = new Put(T1, ts+2);
     p.add(FAMILY, C0, T2);
     h.put(p);
     p = new Put(T1, ts+4);
     p.add(FAMILY, C0, T3);
     h.put(p);

     Delete d = new Delete(T1, ts+3);
     h.delete(d);

     d = new Delete(T1, ts+3);
     d.deleteColumns(FAMILY, C0, ts+3);
     h.delete(d);

     Get g = new Get(T1);
     // does *not* include the delete
     g.setTimeRange(0, ts+3);
     Result r = h.get(g);
     assertArrayEquals(T2, r.getValue(FAMILY, C0));

     Scan s = new Scan(T1);
     s.setTimeRange(0, ts+3);
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
     assertTrue(CellUtil.isDeleteFamily(kvs[0]));
     assertArrayEquals(T3, CellUtil.cloneValue(kvs[1]));
     assertTrue(CellUtil.isDelete(kvs[2]));
     assertArrayEquals(T2, CellUtil.cloneValue(kvs[3]));
     assertArrayEquals(T1, CellUtil.cloneValue(kvs[4]));
     scanner.close();
     h.close();
   }

    /**
    * Basic client side validation of HBASE-10118
    */
   @Test
   public void testPurgeFutureDeletes() throws Exception {
     final TableName TABLENAME = TableName.valueOf("testPurgeFutureDeletes");
     final byte[] ROW = Bytes.toBytes("row");
     final byte[] FAMILY = Bytes.toBytes("family");
     final byte[] COLUMN = Bytes.toBytes("column");
     final byte[] VALUE = Bytes.toBytes("value");

     Table table = TEST_UTIL.createTable(TABLENAME, FAMILY);

     // future timestamp
     long ts = System.currentTimeMillis() * 2;
     Put put = new Put(ROW, ts);
     put.add(FAMILY, COLUMN, VALUE);
     table.put(put);

     Get get = new Get(ROW);
     Result result = table.get(get);
     assertArrayEquals(VALUE, result.getValue(FAMILY, COLUMN));

     Delete del = new Delete(ROW);
     del.deleteColumn(FAMILY, COLUMN, ts);
     table.delete(del);

     get = new Get(ROW);
     result = table.get(get);
     assertNull(result.getValue(FAMILY, COLUMN));

     // major compaction, purged future deletes
     TEST_UTIL.getHBaseAdmin().flush(TABLENAME);
     TEST_UTIL.getHBaseAdmin().majorCompact(TABLENAME);

     // waiting for the major compaction to complete
     TEST_UTIL.waitFor(6000, new Waiter.Predicate<IOException>() {
       @Override
       public boolean evaluate() throws IOException {
         return TEST_UTIL.getHBaseAdmin().getCompactionState(TABLENAME) ==
             AdminProtos.GetRegionInfoResponse.CompactionState.NONE;
       }
     });

     put = new Put(ROW, ts);
     put.add(FAMILY, COLUMN, VALUE);
     table.put(put);

     get = new Get(ROW);
     result = table.get(get);
     assertArrayEquals(VALUE, result.getValue(FAMILY, COLUMN));

     table.close();
   }

   /**
    * @deprecated Tests deprecated functionality. Remove when we are past 1.0.
    * @throws Exception
    */
   @Deprecated
   @Test
   public void testSharedZooKeeper() throws Exception {
     Configuration newConfig = new Configuration(TEST_UTIL.getConfiguration());
     newConfig.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "12345");

     // First with a simple ZKW
     ZooKeeperWatcher z0 = new ZooKeeperWatcher(
       newConfig, "hconnection", new Abortable() {
       @Override public void abort(String why, Throwable e) {}
       @Override public boolean isAborted() {return false;}
     });
     z0.getRecoverableZooKeeper().getZooKeeper().exists("/oldZooKeeperWatcher", false);
     z0.close();

     // Then a ZooKeeperKeepAliveConnection
     ConnectionManager.HConnectionImplementation connection1 =
       (ConnectionManager.HConnectionImplementation)
         HConnectionManager.getConnection(newConfig);

     ZooKeeperKeepAliveConnection z1 = connection1.getKeepAliveZooKeeperWatcher();
     z1.getRecoverableZooKeeper().getZooKeeper().exists("/z1", false);

     z1.close();

     // will still work, because the real connection is not closed yet
     // Not do be done in real code
     z1.getRecoverableZooKeeper().getZooKeeper().exists("/z1afterclose", false);


     ZooKeeperKeepAliveConnection z2 = connection1.getKeepAliveZooKeeperWatcher();
     assertTrue(
       "ZooKeeperKeepAliveConnection equals on same connection", z1 == z2);



     Configuration newConfig2 = new Configuration(TEST_UTIL.getConfiguration());
     newConfig2.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "6789");
     ConnectionManager.HConnectionImplementation connection2 =
       (ConnectionManager.HConnectionImplementation)
         HConnectionManager.getConnection(newConfig2);

     assertTrue("connections should be different ", connection1 != connection2);

     ZooKeeperKeepAliveConnection z3 = connection2.getKeepAliveZooKeeperWatcher();
     assertTrue(
       "ZooKeeperKeepAliveConnection should be different" +
         " on different connections", z1 != z3);

     // Bypass the private access
     Method m = ConnectionManager.HConnectionImplementation.class.
       getDeclaredMethod("closeZooKeeperWatcher");
     m.setAccessible(true);
     m.invoke(connection2);

     ZooKeeperKeepAliveConnection z4 = connection2.getKeepAliveZooKeeperWatcher();
     assertTrue(
       "ZooKeeperKeepAliveConnection should be recreated" +
         " when previous connections was closed"
       , z3 != z4);


     z2.getRecoverableZooKeeper().getZooKeeper().exists("/z2", false);
     z4.getRecoverableZooKeeper().getZooKeeper().exists("/z4", false);


     HConnectionManager.deleteConnection(newConfig);
     try {
       z2.getRecoverableZooKeeper().getZooKeeper().exists("/z2", false);
       assertTrue("We should not have a valid connection for z2", false);
     } catch (Exception e){
     }

     z4.getRecoverableZooKeeper().getZooKeeper().exists("/z4", false);
     // We expect success here.


     HConnectionManager.deleteConnection(newConfig2);
     try {
       z4.getRecoverableZooKeeper().getZooKeeper().exists("/z4", false);
       assertTrue("We should not have a valid connection for z4", false);
     } catch (Exception e){
     }
   }


  /**
   * Verifies that getConfiguration returns the same Configuration object used
   * to create the HTable instance.
   */
  @Test
  public void testGetConfiguration() throws Exception {
    TableName TABLE = TableName.valueOf("testGetConfiguration");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    Configuration conf = TEST_UTIL.getConfiguration();
    Table table = TEST_UTIL.createTable(TABLE, FAMILIES, conf);
    assertSame(conf, table.getConfiguration());
  }

  /**
   * Test from client side of an involved filter against a multi family that
   * involves deletes.
   *
   * @throws Exception
   */
  @Test
  public void testWeirdCacheBehaviour() throws Exception {
    TableName TABLE = TableName.valueOf("testWeirdCacheBehaviour");
    byte [][] FAMILIES = new byte[][] { Bytes.toBytes("trans-blob"),
        Bytes.toBytes("trans-type"), Bytes.toBytes("trans-date"),
        Bytes.toBytes("trans-tags"), Bytes.toBytes("trans-group") };
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    String value = "this is the value";
    String value2 = "this is some other value";
    String keyPrefix1 = UUID.randomUUID().toString();
    String keyPrefix2 = UUID.randomUUID().toString();
    String keyPrefix3 = UUID.randomUUID().toString();
    putRows(ht, 3, value, keyPrefix1);
    putRows(ht, 3, value, keyPrefix2);
    putRows(ht, 3, value, keyPrefix3);
    ht.flushCommits();
    putRows(ht, 3, value2, keyPrefix1);
    putRows(ht, 3, value2, keyPrefix2);
    putRows(ht, 3, value2, keyPrefix3);
    Table table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
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
    ht.setScannerCaching(0);
    assertEquals("Got back incorrect number of rows from scan", 0,
      getNumberOfRows(keyPrefix1, value2, table)); ht.setScannerCaching(100);
    assertEquals("Got back incorrect number of rows from scan", 0,
      getNumberOfRows(keyPrefix2, value2, table));
  }

  private void deleteColumns(Table ht, String value, String keyPrefix)
  throws IOException {
    ResultScanner scanner = buildScanner(keyPrefix, value, ht);
    Iterator<Result> it = scanner.iterator();
    int count = 0;
    while (it.hasNext()) {
      Result result = it.next();
      Delete delete = new Delete(result.getRow());
      delete.deleteColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"));
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
        .toBytes("trans-tags"), Bytes.toBytes("qual2"), CompareOp.EQUAL, Bytes
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
      String row = key + "_" + UUID.randomUUID().toString();
      System.out.println(String.format("Saving row: %s, with value %s", row,
          value));
      Put put = new Put(Bytes.toBytes(row));
      put.setDurability(Durability.SKIP_WAL);
      put.add(Bytes.toBytes("trans-blob"), null, Bytes
          .toBytes("value for blob"));
      put.add(Bytes.toBytes("trans-type"), null, Bytes.toBytes("statement"));
      put.add(Bytes.toBytes("trans-date"), null, Bytes
          .toBytes("20090921010101999"));
      put.add(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"), Bytes
          .toBytes(value));
      put.add(Bytes.toBytes("trans-group"), null, Bytes
          .toBytes("adhocTransactionGroupId"));
      ht.put(put);
    }
  }

  /**
   * Test filters when multiple regions.  It does counts.  Needs eye-balling of
   * logs to ensure that we're not scanning more regions that we're supposed to.
   * Related to the TestFilterAcrossRegions over in the o.a.h.h.filter package.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testFilterAcrossMultipleRegions()
  throws IOException, InterruptedException {
    TableName name = TableName.valueOf("testFilterAcrossMutlipleRegions");
    HTable t = TEST_UTIL.createTable(name, FAMILY);
    int rowCount = TEST_UTIL.loadTable(t, FAMILY, false);
    assertRowCount(t, rowCount);
    // Split the table.  Should split on a reasonable key; 'lqj'
    Map<HRegionInfo, ServerName> regions  = splitTable(t);
    assertRowCount(t, rowCount);
    // Get end key of first region.
    byte [] endKey = regions.keySet().iterator().next().getEndKey();
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
    byte [] key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] + 1)};
    int plusOneCount = countRows(t, createScanWithRowFilter(key));
    assertEquals(endKeyCount + 1, plusOneCount);
    key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] + 2)};
    int plusTwoCount = countRows(t, createScanWithRowFilter(key));
    assertEquals(endKeyCount + 2, plusTwoCount);

    // New test.  Make it so I scan one less than endkey.
    key = new byte [] {endKey[0], endKey[1], (byte)(endKey[2] - 1)};
    int minusOneCount = countRows(t, createScanWithRowFilter(key));
    assertEquals(endKeyCount - 1, minusOneCount);
    // For above test... study logs.  Make sure we do "Finished with scanning.."
    // in first region and that we do not fall into the next region.

    key = new byte [] {'a', 'a', 'a'};
    int countBBB = countRows(t,
      createScanWithRowFilter(key, null, CompareFilter.CompareOp.EQUAL));
    assertEquals(1, countBBB);

    int countGreater = countRows(t, createScanWithRowFilter(endKey, null,
      CompareFilter.CompareOp.GREATER_OR_EQUAL));
    // Because started at start of table.
    assertEquals(0, countGreater);
    countGreater = countRows(t, createScanWithRowFilter(endKey, endKey,
      CompareFilter.CompareOp.GREATER_OR_EQUAL));
    assertEquals(rowCount - endKeyCount, countGreater);
  }

  /*
   * @param key
   * @return Scan with RowFilter that does LESS than passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key) {
    return createScanWithRowFilter(key, null, CompareFilter.CompareOp.LESS);
  }

  /*
   * @param key
   * @param op
   * @param startRow
   * @return Scan with RowFilter that does CompareOp op on passed key.
   */
  private Scan createScanWithRowFilter(final byte [] key,
      final byte [] startRow, CompareFilter.CompareOp op) {
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

  /*
   * @param t
   * @param s
   * @return Count of rows in table.
   * @throws IOException
   */
  private int countRows(final Table t, final Scan s)
  throws IOException {
    // Assert all rows in table.
    ResultScanner scanner = t.getScanner(s);
    int count = 0;
    for (Result result: scanner) {
      count++;
      assertTrue(result.size() > 0);
      // LOG.info("Count=" + count + ", row=" + Bytes.toString(result.getRow()));
    }
    return count;
  }

  private void assertRowCount(final Table t, final int expected)
  throws IOException {
    assertEquals(expected, countRows(t, new Scan()));
  }

  /*
   * Split table into multiple regions.
   * @param t Table to split.
   * @return Map of regions to servers.
   * @throws IOException
   */
  private Map<HRegionInfo, ServerName> splitTable(final HTable t)
  throws IOException, InterruptedException {
    // Split this table in two.
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.split(t.getTableName());
    admin.close();
    Map<HRegionInfo, ServerName> regions = waitOnSplit(t);
    assertTrue(regions.size() > 1);
    return regions;
  }

  /*
   * Wait on table split.  May return because we waited long enough on the split
   * and it didn't happen.  Caller should check.
   * @param t
   * @return Map of table regions; caller needs to check table actually split.
   */
  private Map<HRegionInfo, ServerName> waitOnSplit(final HTable t)
  throws IOException {
    Map<HRegionInfo, ServerName> regions = t.getRegionLocations();
    int originalCount = regions.size();
    for (int i = 0; i < TEST_UTIL.getConfiguration().getInt("hbase.test.retries", 30); i++) {
      Thread.currentThread();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      regions = t.getRegionLocations();
      if (regions.size() > originalCount) break;
    }
    return regions;
  }

  @Test
  public void testSuperSimple() throws Exception {
    byte [] TABLE = Bytes.toBytes("testSuperSimple");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    Scan scan = new Scan();
    scan.addColumn(FAMILY, TABLE);
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    assertTrue("Expected null result", result == null);
    scanner.close();
  }

  @Test
  public void testMaxKeyValueSize() throws Exception {
    byte [] TABLE = Bytes.toBytes("testMaxKeyValueSize");
    Configuration conf = TEST_UTIL.getConfiguration();
    String oldMaxSize = conf.get("hbase.client.keyvalue.maxsize");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte[] value = new byte[4 * 1024 * 1024];
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, value);
    ht.put(put);
    try {
      TEST_UTIL.getConfiguration().setInt("hbase.client.keyvalue.maxsize", 2 * 1024 * 1024);
      TABLE = Bytes.toBytes("testMaxKeyValueSize2");
      // Create new table so we pick up the change in Configuration.
      try (Connection connection =
          ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
        try (Table t = connection.getTable(TableName.valueOf(FAMILY))) {
          put = new Put(ROW);
          put.add(FAMILY, QUALIFIER, value);
          t.put(put);
        }
      }
      fail("Inserting a too large KeyValue worked, should throw exception");
    } catch(Exception e) {}
    conf.set("hbase.client.keyvalue.maxsize", oldMaxSize);
  }

  @Test
  public void testFilters() throws Exception {
    byte [] TABLE = Bytes.toBytes("testFilters");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 10);
    byte [][] QUALIFIERS = {
        Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
        Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
        Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
        Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
        Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")
    };
    for(int i=0;i<10;i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILY, QUALIFIERS[i], VALUE);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    Filter filter = new QualifierFilter(CompareOp.EQUAL,
      new RegexStringComparator("col[1-5]"));
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int expectedIndex = 1;
    for(Result result : ht.getScanner(scan)) {
      assertEquals(result.size(), 1);
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[expectedIndex]));
      assertTrue(Bytes.equals(CellUtil.cloneQualifier(result.rawCells()[0]),
          QUALIFIERS[expectedIndex]));
      expectedIndex++;
    }
    assertEquals(expectedIndex, 6);
    scanner.close();
  }

  @Test
  public void testFilterWithLongCompartor() throws Exception {
    byte [] TABLE = Bytes.toBytes("testFilterWithLongCompartor");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 10);
    byte [][] values = new byte[10][];
    for (int i = 0; i < 10; i ++) {
        values[i] = Bytes.toBytes(100L * i);
    }
    for(int i = 0; i < 10; i ++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILY, QUALIFIER, values[i]);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    Filter filter = new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.GREATER,
      new LongComparator(500));
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int expectedIndex = 0;
    for(Result result : ht.getScanner(scan)) {
      assertEquals(result.size(), 1);
      assertTrue(Bytes.toLong(result.getValue(FAMILY, QUALIFIER)) > 500);
      expectedIndex++;
    }
    assertEquals(expectedIndex, 4);
    scanner.close();
}

  @Test
  public void testKeyOnlyFilter() throws Exception {
    byte [] TABLE = Bytes.toBytes("testKeyOnlyFilter");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 10);
    byte [][] QUALIFIERS = {
        Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
        Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
        Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
        Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
        Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>")
    };
    for(int i=0;i<10;i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILY, QUALIFIERS[i], VALUE);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    Filter filter = new KeyOnlyFilter(true);
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int count = 0;
    for(Result result : ht.getScanner(scan)) {
      assertEquals(result.size(), 1);
      assertEquals(result.rawCells()[0].getValueLength(), Bytes.SIZEOF_INT);
      assertEquals(Bytes.toInt(CellUtil.cloneValue(result.rawCells()[0])), VALUE.length);
      count++;
    }
    assertEquals(count, 10);
    scanner.close();
  }

  /**
   * Test simple table and non-existent row cases.
   */
  @Test
  public void testSimpleMissing() throws Exception {
    byte [] TABLE = Bytes.toBytes("testSimpleMissing");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte [][] ROWS = makeN(ROW, 4);

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

    scan = new Scan(ROWS[0],ROWS[1]);
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
    put.add(FAMILY, QUALIFIER, VALUE);
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

    scan = new Scan(ROWS[0],ROWS[2]);
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

    scan = new Scan(ROWS[0],ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

    scan = new Scan(ROWS[2],ROWS[3]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);
  }

  /**
   * Test basic puts, gets, scans, and deletes for a single row
   * in a multiple family table.
   */
  @Test
  public void testSingleRowMultipleFamily() throws Exception {
    byte [] TABLE = Bytes.toBytes("testSingleRowMultipleFamily");
    byte [][] ROWS = makeN(ROW, 3);
    byte [][] FAMILIES = makeNAscii(FAMILY, 10);
    byte [][] QUALIFIERS = makeN(QUALIFIER, 10);
    byte [][] VALUES = makeN(VALUE, 10);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Scan scan;
    Delete delete;
    Put put;
    Result result;

    ////////////////////////////////////////////////////////////////////////////
    // Insert one column to one family
    ////////////////////////////////////////////////////////////////////////////

    put = new Put(ROWS[0]);
    put.add(FAMILIES[4], QUALIFIERS[0], VALUES[0]);
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
    put.add(FAMILIES[2], QUALIFIERS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIERS[4], VALUES[4]);
    put.add(FAMILIES[4], QUALIFIERS[4], VALUES[4]);
    put.add(FAMILIES[6], QUALIFIERS[6], VALUES[6]);
    put.add(FAMILIES[6], QUALIFIERS[7], VALUES[7]);
    put.add(FAMILIES[7], QUALIFIERS[7], VALUES[7]);
    put.add(FAMILIES[9], QUALIFIERS[0], VALUES[0]);
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
    put.add(FAMILIES[6], QUALIFIERS[5], VALUES[5]);
    put.add(FAMILIES[6], QUALIFIERS[8], VALUES[8]);
    put.add(FAMILIES[6], QUALIFIERS[9], VALUES[9]);
    put.add(FAMILIES[4], QUALIFIERS[3], VALUES[3]);
    ht.put(put);

    ////////////////////////////////////////////////////////////////////////////
    // Delete a storefile column
    ////////////////////////////////////////////////////////////////////////////
    delete = new Delete(ROWS[0]);
    delete.deleteColumns(FAMILIES[6], QUALIFIERS[7]);
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
    delete.deleteColumns(FAMILIES[6], QUALIFIERS[8]);
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
    delete.deleteFamily(FAMILIES[4]);
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

  @Test
  public void testNull() throws Exception {
    byte [] TABLE = Bytes.toBytes("testNull");

    // Null table name (should NOT work)
    try {
      TEST_UTIL.createTable((TableName)null, FAMILY);
      fail("Creating a table with null name passed, should have failed");
    } catch(Exception e) {}

    // Null family (should NOT work)
    try {
      TEST_UTIL.createTable(TABLE, new byte[][]{(byte[])null});
      fail("Creating a table with a null family passed, should fail");
    } catch(Exception e) {}

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);

    // Null row (should NOT work)
    try {
      Put put = new Put((byte[])null);
      put.add(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      fail("Inserting a null row worked, should throw exception");
    } catch(Exception e) {}

    // Null qualifier (should work)
    {
      Put put = new Put(ROW);
      put.add(FAMILY, null, VALUE);
      ht.put(put);

      getTestNull(ht, ROW, FAMILY, VALUE);

      scanTestNull(ht, ROW, FAMILY, VALUE);

      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, null);
      ht.delete(delete);

      Get get = new Get(ROW);
      Result result = ht.get(get);
      assertEmptyResult(result);
    }

    // Use a new table
    byte [] TABLE2 = Bytes.toBytes("testNull2");
    ht = TEST_UTIL.createTable(TableName.valueOf(TABLE2), FAMILY);

    // Empty qualifier, byte[0] instead of null (should work)
    try {
      Put put = new Put(ROW);
      put.add(FAMILY, HConstants.EMPTY_BYTE_ARRAY, VALUE);
      ht.put(put);

      getTestNull(ht, ROW, FAMILY, VALUE);

      scanTestNull(ht, ROW, FAMILY, VALUE);

      // Flush and try again

      TEST_UTIL.flush();

      getTestNull(ht, ROW, FAMILY, VALUE);

      scanTestNull(ht, ROW, FAMILY, VALUE);

      Delete delete = new Delete(ROW);
      delete.deleteColumns(FAMILY, HConstants.EMPTY_BYTE_ARRAY);
      ht.delete(delete);

      Get get = new Get(ROW);
      Result result = ht.get(get);
      assertEmptyResult(result);

    } catch(Exception e) {
      throw new IOException("Using a row with null qualifier threw exception, should ");
    }

    // Null value
    try {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, null);
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
      delete.deleteColumns(FAMILY, QUALIFIER);
      ht.delete(delete);

      get = new Get(ROW);
      result = ht.get(get);
      assertEmptyResult(result);

    } catch(Exception e) {
      throw new IOException("Null values should be allowed, but threw exception");
    }
  }

  @Test
  public void testVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
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
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
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
    get.setMaxVersions(2);
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
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    get = new Get(ROW);
    get.setMaxVersions();
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

    scan = new Scan(ROW);
    scan.setMaxVersions();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 7);

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
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

  }

  @Test
  public void testVersionLimits() throws Exception {
    byte [] TABLE = Bytes.toBytes("testVersionLimits");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    int [] LIMITS = {1,3,5};
    long [] STAMPS = makeStamps(10);
    byte [][] VALUES = makeNAscii(VALUE, 10);
    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, LIMITS);

    // Insert limit + 1 on each family
    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[1], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[5], VALUES[5]);
    put.add(FAMILIES[2], QUALIFIER, STAMPS[6], VALUES[6]);
    ht.put(put);

    // Verify we only get the right number out of each

    // Family0

    Get get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER,
        new long [] {STAMPS[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Family1

    get = new Get(ROW);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    get = new Get(ROW);
    get.addFamily(FAMILIES[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[1]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[1], QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Family2

    get = new Get(ROW);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    get = new Get(ROW);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    scan = new Scan(ROW);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[2], QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[4], VALUES[5], VALUES[6]},
        0, 4);

    // Try all families

    get = new Get(ROW);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 9 keys but received " + result.size(),
        result.size() == 9);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.addColumn(FAMILIES[1], QUALIFIER);
    get.addColumn(FAMILIES[2], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
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

  @Test
  public void testDeleteFamilyVersion() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersion");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 1);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);

    Put put = new Put(ROW);
    for (int q = 0; q < 1; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    Delete delete = new Delete(ROW);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    for (int i = 0; i < 1; i++) {
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }

  @Test
  public void testDeleteFamilyVersionWithOtherDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeleteFamilyVersionWithOtherDeletes");

    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 5);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 5);
    Put put = null;
    Result result = null;
    Get get = null;
    Delete delete = null;

    // 1. put on ROW
    put = new Put(ROW);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 2. put on ROWS[0]
    byte [] ROW2 = Bytes.toBytes("myRowForTest");
    put = new Put(ROW2);
    for (int q = 0; q < 5; q++)
      for (int t = 0; t < 5; t++)
        put.add(FAMILY, QUALIFIERS[q], ts[t], VALUES[t]);
    ht.put(put);
    admin.flush(TABLE);

    // 3. delete on ROW
    delete = new Delete(ROW);
    // delete version <= 2000 of all columns
    // note: deleteFamily must be the first since it will mask
    // the subsequent other type deletes!
    delete.deleteFamily(FAMILY, ts[1]);
    // delete version '4000' of all columns
    delete.deleteFamilyVersion(FAMILY, ts[3]);
   // delete version <= 3000 of column 0
    delete.deleteColumns(FAMILY, QUALIFIERS[0], ts[2]);
    // delete version <= 5000 of column 2
    delete.deleteColumns(FAMILY, QUALIFIERS[2], ts[4]);
    // delete version 5000 of column 4
    delete.deleteColumn(FAMILY, QUALIFIERS[4], ts[4]);
    ht.delete(delete);
    admin.flush(TABLE);

     // 4. delete on ROWS[0]
    delete = new Delete(ROW2);
    delete.deleteFamilyVersion(FAMILY, ts[1]);  // delete version '2000'
    delete.deleteFamilyVersion(FAMILY, ts[3]);  // delete version '4000'
    ht.delete(delete);
    admin.flush(TABLE);

    // 5. check ROW
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[0],
        new long [] {ts[4]},
        new byte[][] {VALUES[4]},
        0, 0);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[1]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[1],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertEquals(0, result.size());

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[3]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[3],
        new long [] {ts[2], ts[4]},
        new byte[][] {VALUES[2], VALUES[4]},
        0, 1);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIERS[4]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIERS[4],
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // 6. check ROWS[0]
    for (int i = 0; i < 5; i++) {
      get = new Get(ROW2);
      get.addColumn(FAMILY, QUALIFIERS[i]);
      get.setMaxVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version '1000'/'3000'/'5000' remains for all columns
      assertNResult(result, ROW2, FAMILY, QUALIFIERS[i],
          new long [] {ts[0], ts[2], ts[4]},
          new byte[][] {VALUES[0], VALUES[2], VALUES[4]},
          0, 2);
    }
    ht.close();
    admin.close();
  }

  @Test
  public void testDeletes() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDeletes");

    byte [][] ROWS = makeNAscii(ROW, 6);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] VALUES = makeN(VALUE, 5);
    long [] ts = {1000, 2000, 3000, 4000, 5000};

    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, 3);

    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);

    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);

    Get get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {ts[1]},
        new byte[][] {VALUES[1]},
        0, 0);

    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    put.add(FAMILIES[0], null, ts[4], VALUES[4]);
    put.add(FAMILIES[0], null, ts[2], VALUES[2]);
    put.add(FAMILIES[0], null, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER); // ts[4]
    ht.delete(delete);

    get = new Get(ROW);
    get.addColumn(FAMILIES[0], QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test for HBASE-1847
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], null);
    ht.delete(delete);

    // Cleanup null qualifier
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILIES[0], null);
    ht.delete(delete);

    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be.  We can't put them back in either case.

    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
    ht.put(put);


    // It used to be due to the internal implementation of Get, that
    // the Get() call would return ts[4] UNLIKE the Scan below. With
    // the switch to using Scan for Get this is no longer the case.
    get = new Get(ROW);
    get.addFamily(FAMILIES[0]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {ts[1], ts[2], ts[3]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3]},
        0, 2);

    // Test deleting an entire family from one row but not the other various ways

    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    // Assert that above went in.
    get = new Get(ROWS[2]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 4 key but received " + result.size() + ": " + result,
        result.size() == 4);

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);

    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);

    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);

    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {ts[0], ts[1]},
        new byte[][] {VALUES[0], VALUES[1]},
        0, 1);

    get = new Get(ROWS[1]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
    get.setMaxVersions(Integer.MAX_VALUE);
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
        new long [] {ts[2]},
        new byte[][] {VALUES[2]},
        0, 0);

    // Test if we delete the family first in one row (HBASE-1541)

    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);

    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);

    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
    ht.put(put);

    get = new Get(ROWS[3]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);

    get = new Get(ROWS[4]);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    get.setMaxVersions(Integer.MAX_VALUE);
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
      byte [] bytes = Bytes.toBytes(i);
      put = new Put(bytes);
      put.setDurability(Durability.SKIP_WAL);
      put.add(FAMILIES[0], QUALIFIER, bytes);
      ht.put(put);
    }
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 1);
    }
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      delete = new Delete(bytes);
      delete.deleteFamily(FAMILIES[0]);
      deletes.add(delete);
    }
    ht.delete(deletes);
    for (int i = 0; i < 10; i++) {
      byte [] bytes = Bytes.toBytes(i);
      get = new Get(bytes);
      get.addFamily(FAMILIES[0]);
      result = ht.get(get);
      assertTrue(result.size() == 0);
    }
  }

  /*
   * Baseline "scalability" test.
   *
   * Tests one hundred families, one million columns, one million versions
   */
  @Ignore @Test
  public void testMillions() throws Exception {

    // 100 families

    // millions of columns

    // millions of versions

  }

  @Ignore @Test
  public void testMultipleRegionsAndBatchPuts() throws Exception {
    // Two family table

    // Insert lots of rows

    // Insert to the same row with batched puts

    // Insert to multiple rows with batched puts

    // Split the table

    // Get row from first region

    // Get row from second region

    // Scan all rows

    // Insert to multiple regions with batched puts

    // Get row from first region

    // Get row from second region

    // Scan all rows


  }

  @Ignore @Test
  public void testMultipleRowMultipleFamily() throws Exception {

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

    byte [] TABLE = Bytes.toBytes("testJiraTest867");

    byte [][] ROWS = makeN(ROW, numRows);
    byte [][] QUALIFIERS = makeN(QUALIFIER, numColsPerRow);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);

    // Insert rows

    for(int i=0;i<numRows;i++) {
      Put put = new Put(ROWS[i]);
      put.setDurability(Durability.SKIP_WAL);
      for(int j=0;j<numColsPerRow;j++) {
        put.add(FAMILY, QUALIFIERS[j], QUALIFIERS[j]);
      }
      assertTrue("Put expected to contain " + numColsPerRow + " columns but " +
          "only contains " + put.size(), put.size() == numColsPerRow);
      ht.put(put);
    }

    // Get a row
    Get get = new Get(ROWS[numRows-1]);
    Result result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    Cell [] keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    int rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

    // flush and try again

    TEST_UTIL.flush();

    // Get a row
    get = new Get(ROWS[numRows-1]);
    result = ht.get(get);
    assertNumKeys(result, numColsPerRow);
    keys = result.rawCells();
    for(int i=0;i<result.size();i++) {
      assertKey(keys[i], ROWS[numRows-1], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
    }

    // Scan the rows
    scan = new Scan();
    scanner = ht.getScanner(scan);
    rowCount = 0;
    while((result = scanner.next()) != null) {
      assertNumKeys(result, numColsPerRow);
      Cell [] kvs = result.rawCells();
      for(int i=0;i<numColsPerRow;i++) {
        assertKey(kvs[i], ROWS[rowCount], FAMILY, QUALIFIERS[i], QUALIFIERS[i]);
      }
      rowCount++;
    }
    scanner.close();
    assertTrue("Expected to scan " + numRows + " rows but actually scanned "
        + rowCount + " rows", rowCount == numRows);

  }

  /**
   * HBASE-861
   *    get with timestamp will return a value if there is a version with an
   *    earlier timestamp
   */
  @Test
  public void testJiraTest861() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest861");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert three versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
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
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
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

  /**
   * HBASE-33
   *    Add a HTable get/obtainScanner method that retrieves all versions of a
   *    particular column and row between two timestamps
   */
  @Test
  public void testJiraTest33() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest33");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
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

  /**
   * HBASE-1014
   *    commit(BatchUpdate) method should return timestamp
   */
  @Test
  public void testJiraTest1014() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1014");

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    long manualStamp = 12345;

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, manualStamp, VALUE);
    ht.put(put);

    getVersionAndVerify(ht, ROW, FAMILY, QUALIFIER, manualStamp, VALUE);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp-1);
    getVersionAndVerifyMissing(ht, ROW, FAMILY, QUALIFIER, manualStamp+1);

  }

  /**
   * HBASE-1182
   *    Scan for columns > some timestamp
   */
  @Test
  public void testJiraTest1182() throws Exception {

    byte [] TABLE = Bytes.toBytes("testJiraTest1182");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
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

  /**
   * HBASE-52
   *    Add a means of scanning over all versions
   */
  @Test
  public void testJiraTest52() throws Exception {
    byte [] TABLE = Bytes.toBytes("testJiraTest52");
    byte [][] VALUES = makeNAscii(VALUE, 7);
    long [] STAMPS = makeStamps(7);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert lots versions

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[0], VALUES[0]);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
    ht.put(put);

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    // Try same from storefile
    TEST_UTIL.flush();

    getAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);

    scanAllVersionsAndVerify(ht, ROW, FAMILY, QUALIFIER, STAMPS, VALUES, 0, 5);
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
    get.setMaxVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }

  private void getVersionRangeAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start], stamps[end]+1);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  private void getAllVersionsAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long [] stamps, byte [][] values, int start, int end)
  throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions(Integer.MAX_VALUE);
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
    get.setTimeStamp(stamp);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  private void getVersionAndVerifyMissing(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp)
  throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimeStamp(stamp);
    get.setMaxVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertEmptyResult(result);
  }

  private void scanVersionAndVerify(Table ht, byte [] row, byte [] family,
      byte [] qualifier, long stamp, byte [] value)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimeStamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  private void scanVersionAndVerifyMissing(Table ht, byte [] row,
      byte [] family, byte [] qualifier, long stamp)
  throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimeStamp(stamp);
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

  private void assertIncrementKey(Cell key, byte [] row, byte [] family,
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
    for(int i=0;i<n;i++) stamps[i] = i+1;
    return stamps;
  }

  private boolean equals(byte [] left, byte [] right) {
    if (left == null && right == null) return true;
    if (left == null && right.length == 0) return true;
    if (right == null && left.length == 0) return true;
    return Bytes.equals(left, right);
  }

  @Test
  public void testDuplicateVersions() throws Exception {
    byte [] TABLE = Bytes.toBytes("testDuplicateVersions");

    long [] STAMPS = makeStamps(20);
    byte [][] VALUES = makeNAscii(VALUE, 20);

    Table ht = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Insert 4 versions of same column
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[1], VALUES[1]);
    put.add(FAMILY, QUALIFIER, STAMPS[2], VALUES[2]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[4]);
    put.add(FAMILY, QUALIFIER, STAMPS[5], VALUES[5]);
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
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);

    Scan scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(2);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
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
    get.setMaxVersions(2);
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
        new long [] {STAMPS[4], STAMPS[5]},
        new byte[][] {VALUES[4], VALUES[5]},
        0, 1);


    // Add some memstore and retest

    // Insert 4 more versions of same column and a dupe
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, STAMPS[3], VALUES[3]);
    put.add(FAMILY, QUALIFIER, STAMPS[4], VALUES[14]);
    put.add(FAMILY, QUALIFIER, STAMPS[6], VALUES[6]);
    put.add(FAMILY, QUALIFIER, STAMPS[7], VALUES[7]);
    put.add(FAMILY, QUALIFIER, STAMPS[8], VALUES[8]);
    ht.put(put);

    // Ensure maxVersions in query is respected
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    get = new Get(ROW);
    get.setMaxVersions(7);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
        0, 6);

    scan = new Scan(ROW);
    scan.setMaxVersions(7);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8]},
        new byte[][] {VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8]},
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
    put.add(FAMILY, QUALIFIER, STAMPS[9], VALUES[9]);
    put.add(FAMILY, QUALIFIER, STAMPS[11], VALUES[11]);
    put.add(FAMILY, QUALIFIER, STAMPS[13], VALUES[13]);
    put.add(FAMILY, QUALIFIER, STAMPS[15], VALUES[15]);
    ht.put(put);

    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[7], STAMPS[8], STAMPS[9], STAMPS[11], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[7], VALUES[8], VALUES[9], VALUES[11], VALUES[13], VALUES[15]},
        0, 9);

    // Delete a version in the memstore and a version in a storefile
    Delete delete = new Delete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[11]);
    delete.deleteColumn(FAMILY, QUALIFIER, STAMPS[7]);
    ht.delete(delete);

    // Test that it's gone
    get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions(Integer.MAX_VALUE);
    result = ht.get(get);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);

    scan = new Scan(ROW);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILY, QUALIFIER,
        new long [] {STAMPS[1], STAMPS[2], STAMPS[3], STAMPS[4], STAMPS[5], STAMPS[6], STAMPS[8], STAMPS[9], STAMPS[13], STAMPS[15]},
        new byte[][] {VALUES[1], VALUES[2], VALUES[3], VALUES[14], VALUES[5], VALUES[6], VALUES[8], VALUES[9], VALUES[13], VALUES[15]},
        0, 9);
  }

  @Test
  public void testUpdates() throws Exception {

    byte [] TABLE = Bytes.toBytes("testUpdates");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row1");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }

  @Test
  public void testUpdatesWithMajorCompaction() throws Exception {

    TableName TABLE = TableName.valueOf("testUpdatesWithMajorCompaction");
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

    // Check that the column indeed has the right values at timestamps 1 and
    // 2
    Result result = hTable.get(get);
    NavigableMap<Long, byte[]> navigableMap =
        result.getMap().get(FAMILY).get(qualifier);
    assertEquals("AAA", Bytes.toString(navigableMap.get(1L)));
    assertEquals("BBB", Bytes.toString(navigableMap.get(2L)));

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Update the value at timestamp 1
    put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(TABLE);
    admin.majorCompact(TABLE);
    Thread.sleep(6000);

    // Check that the values at timestamp 2 and 1 got updated
    result = hTable.get(get);
    navigableMap = result.getMap().get(FAMILY).get(qualifier);
    assertEquals("CCC", Bytes.toString(navigableMap.get(1L)));
    assertEquals("DDD", Bytes.toString(navigableMap.get(2L)));
  }

  @Test
  public void testMajorCompactionBetweenTwoUpdates() throws Exception {

    String tableName = "testMajorCompactionBetweenTwoUpdates";
    byte [] TABLE = Bytes.toBytes(tableName);
    Table hTable = TEST_UTIL.createTable(TABLE, FAMILY, 10);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    // Write a column with values at timestamp 1, 2 and 3
    byte[] row = Bytes.toBytes("row3");
    byte[] qualifier = Bytes.toBytes("myCol");
    Put put = new Put(row);
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("AAA"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("BBB"));
    hTable.put(put);

    put = new Put(row);
    put.add(FAMILY, qualifier, 3L, Bytes.toBytes("EEE"));
    hTable.put(put);

    Get get = new Get(row);
    get.addColumn(FAMILY, qualifier);
    get.setMaxVersions();

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
    put.add(FAMILY, qualifier, 1L, Bytes.toBytes("CCC"));
    hTable.put(put);

    // Trigger a major compaction
    admin.flush(tableName);
    admin.majorCompact(tableName);
    Thread.sleep(6000);

    // Update the value at timestamp 2
    put = new Put(row);
    put.add(FAMILY, qualifier, 2L, Bytes.toBytes("DDD"));
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

  @Test
  public void testGet_EmptyTable() throws IOException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_EmptyTable"), FAMILY);
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
  }

  @Test
  public void testGet_NullQualifier() throws IOException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NullQualifier"), FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
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

  @Test
  public void testGet_NonExistentRow() throws IOException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testGet_NonExistentRow"), FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    LOG.info("Row put");

    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    System.out.println("Row retrieved successfully");

    byte [] missingrow = Bytes.toBytes("missingrow");
    get = new Get(missingrow);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    LOG.info("Row missing as it should be");
  }

  @Test
  public void testPut() throws IOException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] row1 = Bytes.toBytes("row1");
    final byte [] row2 = Bytes.toBytes("row2");
    final byte [] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPut"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    Put put = new Put(row1);
    put.add(CONTENTS_FAMILY, null, value);
    table.put(put);

    put = new Put(row2);
    put.add(CONTENTS_FAMILY, null, value);

    assertEquals(put.size(), 1);
    assertEquals(put.getFamilyCellMap().get(CONTENTS_FAMILY).size(), 1);

    // KeyValue v1 expectation.  Cast for now until we go all Cell all the time. TODO
    KeyValue kv = (KeyValue)put.getFamilyCellMap().get(CONTENTS_FAMILY).get(0);

    assertTrue(Bytes.equals(kv.getFamily(), CONTENTS_FAMILY));
    // will it return null or an empty byte array?
    assertTrue(Bytes.equals(kv.getQualifier(), new byte[0]));

    assertTrue(Bytes.equals(kv.getValue(), value));

    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(CONTENTS_FAMILY, null);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r : scanner) {
      for(Cell key : r.rawCells()) {
        System.out.println(Bytes.toString(r.getRow()) + ": " + key.toString());
      }
    }
  }

  @Test
  public void testPutNoCF() throws IOException {
    final byte[] BAD_FAM = Bytes.toBytes("BAD_CF");
    final byte[] VAL = Bytes.toBytes(100);
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testPutNoCF"), FAMILY);

    boolean caughtNSCFE = false;

    try {
      Put p = new Put(ROW);
      p.add(BAD_FAM, QUALIFIER, VAL);
      table.put(p);
    } catch (RetriesExhaustedWithDetailsException e) {
      caughtNSCFE = e.getCause(0) instanceof NoSuchColumnFamilyException;
    }
    assertTrue("Should throw NoSuchColumnFamilyException", caughtNSCFE);

  }

  @Test
  public void testRowsPut() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final int NB_BATCH_ROWS = 10;
    final byte[] value = Bytes.toBytes("abcd");
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPut"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);
    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS, nbRows);
  }

  @Test
  public void testRowsPutBufferedOneFlush() throws IOException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte [] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedOneFlush"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    table.setAutoFlushTo(false);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(0, nbRows);
    scanner.close();

    table.flushCommits();

    scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    scanner = table.getScanner(scan);
    nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
  }

  @Test
  public void testRowsPutBufferedManyManyFlushes() throws IOException {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] SMALL_FAMILY = Bytes.toBytes("smallfam");
    final byte[] value = Bytes.toBytes("abcd");
    final int NB_BATCH_ROWS = 10;
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testRowsPutBufferedManyManyFlushes"),
      new byte[][] {CONTENTS_FAMILY, SMALL_FAMILY });
    table.setAutoFlushTo(false);
    table.setWriteBufferSize(10);
    ArrayList<Put> rowsUpdate = new ArrayList<Put>();
    for (int i = 0; i < NB_BATCH_ROWS * 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      put.add(CONTENTS_FAMILY, null, value);
      rowsUpdate.add(put);
    }
    table.put(rowsUpdate);

    table.flushCommits();

    Scan scan = new Scan();
    scan.addFamily(CONTENTS_FAMILY);
    ResultScanner scanner = table.getScanner(scan);
    int nbRows = 0;
    for (@SuppressWarnings("unused")
    Result row : scanner)
      nbRows++;
    assertEquals(NB_BATCH_ROWS * 10, nbRows);
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
   * @throws IOException
   */
  @Test
  public void testHBase737 () throws IOException {
    final byte [] FAM1 = Bytes.toBytes("fam1");
    final byte [] FAM2 = Bytes.toBytes("fam2");
    // Open table
    Table table = TEST_UTIL.createTable(Bytes.toBytes("testHBase737"),
      new byte [][] {FAM1, FAM2});
    // Insert some values
    Put put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("letters"), Bytes.toBytes("abcdefg"));
    table.put(put);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM1, Bytes.toBytes("numbers"), Bytes.toBytes("123456"));
    table.put(put);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException i) {
      //ignore
    }

    put = new Put(ROW);
    put.add(FAM2, Bytes.toBytes("letters"), Bytes.toBytes("hijklmnop"));
    table.put(put);

    long times[] = new long[3];

    // First scan the memstore

    Scan scan = new Scan();
    scan.addFamily(FAM1);
    scan.addFamily(FAM2);
    ResultScanner s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }

    // Flush data to disk and try again
    TEST_UTIL.flush();

    // Reset times
    for(int i=0;i<times.length;i++) {
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
    s = table.getScanner(scan);
    try {
      int index = 0;
      Result r = null;
      while ((r = s.next()) != null) {
        for(Cell key : r.rawCells()) {
          times[index++] = key.getTimestamp();
        }
      }
    } finally {
      s.close();
    }
    for (int i = 0; i < times.length - 1; i++) {
      for (int j = i + 1; j < times.length; j++) {
        assertTrue(times[j] > times[i]);
      }
    }
  }

  @Test
  public void testListTables() throws IOException, InterruptedException {
    TableName t1 = TableName.valueOf("testListTables1");
    TableName t2 = TableName.valueOf("testListTables2");
    TableName t3 = TableName.valueOf("testListTables3");
    TableName [] tables = new TableName[] { t1, t2, t3 };
    for (int i = 0; i < tables.length; i++) {
      TEST_UTIL.createTable(tables[i], FAMILY);
    }
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] ts = admin.listTables();
    HashSet<HTableDescriptor> result = new HashSet<HTableDescriptor>(ts.length);
    Collections.addAll(result, ts);
    int size = result.size();
    assertTrue(size >= tables.length);
    for (int i = 0; i < tables.length && i < size; i++) {
      boolean found = false;
      for (int j = 0; j < ts.length; j++) {
        if (ts[j].getTableName().equals(tables[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Not found: " + tables[i], found);
    }
  }

  /**
   * creates an HTable for tableName using an unmanaged HConnection.
   *
   * @param tableName - table to create
   * @return the created HTable object
   * @throws IOException
   */
  HTable createUnmangedHConnectionHTable(final TableName tableName) throws IOException {
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    HConnection conn = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
    return (HTable)conn.getTable(tableName);
  }

  /**
   * simple test that just executes parts of the client
   * API that accept a pre-created HConnection instance
   *
   * @throws IOException
   */
  @Test
  public void testUnmanagedHConnection() throws IOException {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnection");
    HTable t = createUnmangedHConnectionHTable(tableName);
    HBaseAdmin ha = new HBaseAdmin(t.getConnection());
    assertTrue(ha.tableExists(tableName));
    assertTrue(t.get(new Get(ROW)).isEmpty());
  }

  /**
   * test of that unmanaged HConnections are able to reconnect
   * properly (see HBASE-5058)
   *
   * @throws Exception
   */
  @Test
  public void testUnmanagedHConnectionReconnect() throws Exception {
    final TableName tableName = TableName.valueOf("testUnmanagedHConnectionReconnect");
    HTable t = createUnmangedHConnectionHTable(tableName);
    Connection conn = t.getConnection();
    HBaseAdmin ha = new HBaseAdmin(conn);
    assertTrue(ha.tableExists(tableName));
    assertTrue(t.get(new Get(ROW)).isEmpty());

    // stop the master
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.stopMaster(0, false);
    cluster.waitOnMaster(0);

    // start up a new master
    cluster.startMaster();
    assertTrue(cluster.waitForActiveAndReadyMaster());

    // test that the same unmanaged connection works with a new
    // HBaseAdmin and can connect to the new master;
    HBaseAdmin newAdmin = new HBaseAdmin(conn);
    assertTrue(newAdmin.tableExists(tableName));
    assertTrue(newAdmin.getClusterStatus().getServersSize() == SLAVES);
  }

  @Test
  public void testMiscHTableStuff() throws IOException {
    final TableName tableAname = TableName.valueOf("testMiscHTableStuffA");
    final TableName tableBname = TableName.valueOf("testMiscHTableStuffB");
    final byte[] attrName = Bytes.toBytes("TESTATTR");
    final byte[] attrValue = Bytes.toBytes("somevalue");
    byte[] value = Bytes.toBytes("value");

    Table a = TEST_UTIL.createTable(tableAname, HConstants.CATALOG_FAMILY);
    Table b = TEST_UTIL.createTable(tableBname, HConstants.CATALOG_FAMILY);
    Put put = new Put(ROW);
    put.add(HConstants.CATALOG_FAMILY, null, value);
    a.put(put);

    // open a new connection to A and a connection to b
    Table newA = new HTable(TEST_UTIL.getConfiguration(), tableAname);

    // copy data from A to B
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = newA.getScanner(scan);
    try {
      for (Result r : s) {
        put = new Put(r.getRow());
        put.setDurability(Durability.SKIP_WAL);
        for (Cell kv : r.rawCells()) {
          put.add(kv);
        }
        b.put(put);
      }
    } finally {
      s.close();
    }

    // Opening a new connection to A will cause the tables to be reloaded
    Table anotherA = new HTable(TEST_UTIL.getConfiguration(), tableAname);
    Get get = new Get(ROW);
    get.addFamily(HConstants.CATALOG_FAMILY);
    anotherA.get(get);

    // We can still access A through newA because it has the table information
    // cached. And if it needs to recalibrate, that will cause the information
    // to be reloaded.

    // Test user metadata
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    // make a modifiable descriptor
    HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
    // offline the table
    admin.disableTable(tableAname);
    // add a user attribute to HTD
    desc.setValue(attrName, attrValue);
    // add a user attribute to HCD
    for (HColumnDescriptor c : desc.getFamilies())
      c.setValue(attrName, attrValue);
    // update metadata for all regions of this table
    admin.modifyTable(tableAname, desc);
    // enable the table
    admin.enableTable(tableAname);

    // Test that attribute changes were applied
    desc = a.getTableDescriptor();
    assertEquals("wrong table descriptor returned", desc.getTableName(), tableAname);
    // check HTD attribute
    value = desc.getValue(attrName);
    assertFalse("missing HTD attribute value", value == null);
    assertFalse("HTD attribute value is incorrect",
      Bytes.compareTo(value, attrValue) != 0);
    // check HCD attribute
    for (HColumnDescriptor c : desc.getFamilies()) {
      value = c.getValue(attrName);
      assertFalse("missing HCD attribute value", value == null);
      assertFalse("HCD attribute value is incorrect",
        Bytes.compareTo(value, attrValue) != 0);
    }
  }

  @Test
  public void testGetClosestRowBefore() throws IOException, InterruptedException {
    final TableName tableAname = TableName.valueOf("testGetClosestRowBefore");
    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    HTable table =
        TEST_UTIL.createTable(tableAname,
          new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1, 1024);
    // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
    // in Store.rowAtOrBeforeFromStoreFile
    table.setAutoFlush(true);
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    HRegion region =
        TEST_UTIL.getRSForFirstRegionInTable(tableAname).getFromOnlineRegions(regionName);
    Put put1 = new Put(firstRow);
    Put put2 = new Put(secondRow);
    Put put3 = new Put(thirdRow);
    Put put4 = new Put(forthRow);
    byte[] one = new byte[] { 1 };
    byte[] two = new byte[] { 2 };
    byte[] three = new byte[] { 3 };
    byte[] four = new byte[] { 4 };

    put1.add(HConstants.CATALOG_FAMILY, null, one);
    put2.add(HConstants.CATALOG_FAMILY, null, two);
    put3.add(HConstants.CATALOG_FAMILY, null, three);
    put4.add(HConstants.CATALOG_FAMILY, null, four);
    table.put(put1);
    table.put(put2);
    table.put(put3);
    table.put(put4);
    region.flushcache();
    Result result = null;

    // Test before first that null is returned
    result = table.getRowOrBefore(beforeFirstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result == null);

    // Test at first that first is returned
    result = table.getRowOrBefore(firstRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test in between first and second that first is returned
    result = table.getRowOrBefore(beforeSecondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), firstRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

    // Test at second make sure second is returned
    result = table.getRowOrBefore(secondRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test in second and third, make sure second is returned
    result = table.getRowOrBefore(beforeThirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), secondRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

    // Test at third make sure third is returned
    result = table.getRowOrBefore(thirdRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test in third and forth, make sure third is returned
    result = table.getRowOrBefore(beforeForthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), thirdRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

    // Test at forth make sure forth is returned
    result = table.getRowOrBefore(forthRow, HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

    // Test after forth make sure forth is returned
    result = table.getRowOrBefore(Bytes.add(forthRow, one), HConstants.CATALOG_FAMILY);
    assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
    assertTrue(Bytes.equals(result.getRow(), forthRow));
    assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));
  }

  /**
   * For HBASE-2156
   * @throws Exception
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
    final TableName TABLENAME = TableName.valueOf("testMultiRowMutation");
    final byte [] ROW1 = Bytes.toBytes("testRow1");

    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIER, VALUE);
    MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, p);

    p = new Put(ROW1);
    p.add(FAMILY, QUALIFIER, VALUE);
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

  @Test
  public void testRowMutation() throws Exception {
    LOG.info("Starting testRowMutation");
    final TableName TABLENAME = TableName.valueOf("testRowMutation");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b")
    };
    RowMutations arm = new RowMutations(ROW);
    Put p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[0], VALUE);
    arm.add(p);
    t.mutateRow(arm);

    Get g = new Get(ROW);
    Result r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

    arm = new RowMutations(ROW);
    p = new Put(ROW);
    p.add(FAMILY, QUALIFIERS[1], VALUE);
    arm.add(p);
    Delete d = new Delete(ROW);
    d.deleteColumns(FAMILY, QUALIFIERS[0]);
    arm.add(d);
    // TODO: Trying mutateRow again.  The batch was failing with a one try only.
    t.mutateRow(arm);
    r = t.get(g);
    assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
    assertNull(r.getValue(FAMILY, QUALIFIERS[0]));
  }

  @Test
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    final TableName TABLENAME = TableName.valueOf("testAppend");
    Table t = TEST_UTIL.createTable(TABLENAME, FAMILY);
    byte[] v1 = Bytes.toBytes("42");
    byte[] v2 = Bytes.toBytes("23");
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c")
    };
    Append a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v1);
    a.add(FAMILY, QUALIFIERS[1], v2);
    a.setReturnResults(false);
    assertNullResult(t.append(a));

    a = new Append(ROW);
    a.add(FAMILY, QUALIFIERS[0], v2);
    a.add(FAMILY, QUALIFIERS[1], v1);
    a.add(FAMILY, QUALIFIERS[2], v2);
    Result r = t.append(a);
    assertEquals(0, Bytes.compareTo(Bytes.add(v1,v2), r.getValue(FAMILY, QUALIFIERS[0])));
    assertEquals(0, Bytes.compareTo(Bytes.add(v2,v1), r.getValue(FAMILY, QUALIFIERS[1])));
    // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
    assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
    assertEquals(r.getColumnLatest(FAMILY, QUALIFIERS[0]).getTimestamp(),
        r.getColumnLatest(FAMILY, QUALIFIERS[2]).getTimestamp());
  }

  @Test
  public void testIncrementWithDeletes() throws Exception {
    LOG.info("Starting testIncrementWithDeletes");
    final TableName TABLENAME =
        TableName.valueOf("testIncrementWithDeletes");
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);
    final byte[] COLUMN = Bytes.toBytes("column");

    ht.incrementColumnValue(ROW, FAMILY, COLUMN, 5);
    TEST_UTIL.flush(TABLENAME);

    Delete del = new Delete(ROW);
    ht.delete(del);

    ht.incrementColumnValue(ROW, FAMILY, COLUMN, 5);

    Get get = new Get(ROW);
    Result r = ht.get(get);
    assertEquals(1, r.size());
    assertEquals(5, Bytes.toLong(r.getValue(FAMILY, COLUMN)));
  }

  @Test
  public void testIncrementingInvalidValue() throws Exception {
    LOG.info("Starting testIncrementingInvalidValue");
    final TableName TABLENAME = TableName.valueOf("testIncrementingInvalidValue");
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);
    final byte[] COLUMN = Bytes.toBytes("column");
    Put p = new Put(ROW);
    // write an integer here (not a Long)
    p.add(FAMILY, COLUMN, Bytes.toBytes(5));
    ht.put(p);
    try {
      ht.incrementColumnValue(ROW, FAMILY, COLUMN, 5);
      fail("Should have thrown DoNotRetryIOException");
    } catch (DoNotRetryIOException iox) {
      // success
    }
    Increment inc = new Increment(ROW);
    inc.addColumn(FAMILY, COLUMN, 5);
    try {
      ht.increment(inc);
      fail("Should have thrown DoNotRetryIOException");
    } catch (DoNotRetryIOException iox) {
      // success
    }
  }

  @Test
  public void testIncrementInvalidArguments() throws Exception {
    LOG.info("Starting testIncrementInvalidArguments");
    final TableName TABLENAME = TableName.valueOf("testIncrementInvalidArguments");
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);
    final byte[] COLUMN = Bytes.toBytes("column");
    try {
      // try null row
      ht.incrementColumnValue(null, FAMILY, COLUMN, 5);
      fail("Should have thrown IOException");
    } catch (IOException iox) {
      // success
    }
    try {
      // try null family
      ht.incrementColumnValue(ROW, null, COLUMN, 5);
      fail("Should have thrown IOException");
    } catch (IOException iox) {
      // success
    }
    try {
      // try null qualifier
      ht.incrementColumnValue(ROW, FAMILY, null, 5);
      fail("Should have thrown IOException");
    } catch (IOException iox) {
      // success
    }
    // try null row
    try {
      Increment incNoRow = new Increment((byte [])null);
      incNoRow.addColumn(FAMILY, COLUMN, 5);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iax) {
      // success
    } catch (NullPointerException npe) {
      // success
    }
    // try null family
    try {
      Increment incNoFamily = new Increment(ROW);
      incNoFamily.addColumn(null, COLUMN, 5);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iax) {
      // success
    }
    // try null qualifier
    try {
      Increment incNoQualifier = new Increment(ROW);
      incNoQualifier.addColumn(FAMILY, null, 5);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iax) {
      // success
    }
  }

  @Test
  public void testIncrementOutOfOrder() throws Exception {
    LOG.info("Starting testIncrementOutOfOrder");
    final TableName TABLENAME = TableName.valueOf("testIncrementOutOfOrder");
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);

    byte [][] QUALIFIERS = new byte [][] {
      Bytes.toBytes("B"), Bytes.toBytes("A"), Bytes.toBytes("C")
    };

    Increment inc = new Increment(ROW);
    for (int i=0; i<QUALIFIERS.length; i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
    }
    ht.increment(inc);

    // Verify expected results
    Result r = ht.get(new Get(ROW));
    Cell [] kvs = r.rawCells();
    assertEquals(3, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[1], 1);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[0], 1);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 1);

    // Now try multiple columns again
    inc = new Increment(ROW);
    for (int i=0; i<QUALIFIERS.length; i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
    }
    ht.increment(inc);

    // Verify
    r = ht.get(new Get(ROW));
    kvs = r.rawCells();
    assertEquals(3, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[1], 2);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[0], 2);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 2);
  }

  @Test
  public void testIncrement() throws Exception {
    LOG.info("Starting testIncrement");
    final TableName TABLENAME = TableName.valueOf("testIncrement");
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);

    byte [][] ROWS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"),
        Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
        Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i")
    };
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"),
        Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
        Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i")
    };

    // Do some simple single-column increments

    // First with old API
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[0], 1);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[1], 2);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[2], 3);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[3], 4);

    // Now increment things incremented with old and do some new
    Increment inc = new Increment(ROW);
    inc.addColumn(FAMILY, QUALIFIERS[1], 1);
    inc.addColumn(FAMILY, QUALIFIERS[3], 1);
    inc.addColumn(FAMILY, QUALIFIERS[4], 1);
    ht.increment(inc);

    // Verify expected results
    Result r = ht.get(new Get(ROW));
    Cell [] kvs = r.rawCells();
    assertEquals(5, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[0], 1);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[1], 3);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 3);
    assertIncrementKey(kvs[3], ROW, FAMILY, QUALIFIERS[3], 5);
    assertIncrementKey(kvs[4], ROW, FAMILY, QUALIFIERS[4], 1);

    // Now try multiple columns by different amounts
    inc = new Increment(ROWS[0]);
    for (int i=0;i<QUALIFIERS.length;i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], i+1);
    }
    ht.increment(inc);
    // Verify
    r = ht.get(new Get(ROWS[0]));
    kvs = r.rawCells();
    assertEquals(QUALIFIERS.length, kvs.length);
    for (int i=0;i<QUALIFIERS.length;i++) {
      assertIncrementKey(kvs[i], ROWS[0], FAMILY, QUALIFIERS[i], i+1);
    }

    // Re-increment them
    inc = new Increment(ROWS[0]);
    for (int i=0;i<QUALIFIERS.length;i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], i+1);
    }
    ht.increment(inc);
    // Verify
    r = ht.get(new Get(ROWS[0]));
    kvs = r.rawCells();
    assertEquals(QUALIFIERS.length, kvs.length);
    for (int i=0;i<QUALIFIERS.length;i++) {
      assertIncrementKey(kvs[i], ROWS[0], FAMILY, QUALIFIERS[i], 2*(i+1));
    }
  }


  @Test
  public void testClientPoolRoundRobin() throws IOException {
    final TableName tableName = TableName.valueOf("testClientPoolRoundRobin");

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, conf, Integer.MAX_VALUE);

    final long ts = EnvironmentEdgeManager.currentTime();
    Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + FAMILY + ":" + QUALIFIER
          + " did not match " + versions, versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }
  }

  @Ignore ("Flakey: HBASE-8989") @Test
  public void testClientPoolThreadLocal() throws IOException {
    final TableName tableName = TableName.valueOf("testClientPoolThreadLocal");

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    final Table table = TEST_UTIL.createTable(tableName,
        new byte[][] { FAMILY }, conf, 3);

    final long ts = EnvironmentEdgeManager.currentTime();
    final Get get = new Get(ROW);
    get.addColumn(FAMILY, QUALIFIER);
    get.setMaxVersions();

    for (int versions = 1; versions <= numVersions; versions++) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, ts + versions, VALUE);
      table.put(put);

      Result result = table.get(get);
      NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
          .get(QUALIFIER);

      assertEquals("The number of versions of '" + FAMILY + ":" + QUALIFIER + " did not match " +
        versions + "; " + put.toString() + ", " + get.toString(), versions, navigableMap.size());
      for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
        assertTrue("The value at time " + entry.getKey()
            + " did not match what was put",
            Bytes.equals(VALUE, entry.getValue()));
      }
    }

    final Object waitLock = new Object();
    ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
    final AtomicReference<AssertionError> error = new AtomicReference<AssertionError>(null);
    for (int versions = numVersions; versions < numVersions * 2; versions++) {
      final int versionsCopy = versions;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          try {
            Put put = new Put(ROW);
            put.add(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
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
            LOG.error(e);
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

  @Test
  public void testCheckAndPut() throws IOException {
    final byte [] anotherrow = Bytes.toBytes("anotherrow");
    final byte [] value2 = Bytes.toBytes("abcd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPut"), FAMILY);
    Put put1 = new Put(ROW);
    put1.add(FAMILY, QUALIFIER, VALUE);

    // row doesn't exist, so using non-null value should be considered "not match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put1);
    assertEquals(ok, false);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, true);

    // row now exists, so using "null" to check for existence should be considered "not match".
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
    assertEquals(ok, false);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    // row now exists, use the matching value to check
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put2);
    assertEquals(ok, true);

    Put put3 = new Put(anotherrow);
    put3.add(FAMILY, QUALIFIER, VALUE);

    // try to do CheckAndPut on different rows
    try {
        ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, value2, put3);
        fail("trying to check and modify different rows should have failed.");
    } catch(Exception e) {}

  }

  @Test
  public void testCheckAndPutWithCompareOp() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndPutWithCompareOp"), FAMILY);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    // row doesn't exist, so using "null" to check for existence should be considered "match".
    boolean ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value1, put3);
    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, put3);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, put3);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, put2);
    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, put2);
    assertEquals(ok, false);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, put2);
    assertEquals(ok, true);
    ok = table.checkAndPut(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, put3);
    assertEquals(ok, true);
  }

  @Test
  public void testCheckAndDeleteWithCompareOp() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    Table table = TEST_UTIL.createTable(TableName.valueOf("testCheckAndDeleteWithCompareOp"),
        FAMILY);

    Put put2 = new Put(ROW);
    put2.add(FAMILY, QUALIFIER, value2);
    table.put(put2);

    Put put3 = new Put(ROW);
    put3.add(FAMILY, QUALIFIER, value3);

    Delete delete = new Delete(ROW);
    delete.deleteColumns(FAMILY, QUALIFIER);

    // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    boolean ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value1, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value1, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value1, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value1, delete);
    assertEquals(ok, true);

    // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
    // turns out "match"
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value4, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value4, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value4, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value4, delete);
    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value4, delete);
    assertEquals(ok, true);
    table.put(put3);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value4, delete);
    assertEquals(ok, true);

    // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
    // turns out "match"
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS, value2, delete);
    assertEquals(ok, false);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.GREATER_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.LESS_OR_EQUAL, value2, delete);
    assertEquals(ok, true);
    table.put(put2);
    ok = table.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, value2, delete);
    assertEquals(ok, true);
  }

  /**
  * Test ScanMetrics
  * @throws Exception
  */
  @Test
  @SuppressWarnings ("unused")
  public void testScanMetrics() throws Exception {
    TableName TABLENAME = TableName.valueOf("testScanMetrics");

    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.createTable(TABLENAME, FAMILY);

    // Set up test table:
    // Create table:
    HTable ht = new HTable(conf, TABLENAME);

    // Create multiple regions for this table
    int numOfRegions = TEST_UTIL.createMultiRegions(ht, FAMILY);
    // Create 3 rows in the table, with rowkeys starting with "z*" so that
    // scan are forced to hit all the regions.
    Put put1 = new Put(Bytes.toBytes("z1"));
    put1.add(FAMILY, QUALIFIER, VALUE);
    Put put2 = new Put(Bytes.toBytes("z2"));
    put2.add(FAMILY, QUALIFIER, VALUE);
    Put put3 = new Put(Bytes.toBytes("z3"));
    put3.add(FAMILY, QUALIFIER, VALUE);
    ht.put(Arrays.asList(put1, put2, put3));

    Scan scan1 = new Scan();
    int numRecords = 0;
    for(Result result : ht.getScanner(scan1)) {
      numRecords++;
    }
    LOG.info("test data has " + numRecords + " records.");

    // by default, scan metrics collection is turned off
    assertEquals(null, scan1.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA));

    // turn on scan metrics
    Scan scan = new Scan();
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));
    scan.setCaching(numRecords+1);
    ResultScanner scanner = ht.getScanner(scan);
    for (Result result : scanner.next(numRecords - 1)) {
    }
    scanner.close();
    // closing the scanner will set the metrics.
    assertNotNull(scan.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA));

    // set caching to 1, becasue metrics are collected in each roundtrip only
    scan = new Scan();
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));
    scan.setCaching(1);
    scanner = ht.getScanner(scan);
    // per HBASE-5717, this should still collect even if you don't run all the way to
    // the end of the scanner. So this is asking for 2 of the 3 rows we inserted.
    for (Result result : scanner.next(numRecords - 1)) {
    }
    scanner.close();

    ScanMetrics scanMetrics = getScanMetrics(scan);
    assertEquals("Did not access all the regions in the table", numOfRegions,
        scanMetrics.countOfRegions.get());

    // now, test that the metrics are still collected even if you don't call close, but do
    // run past the end of all the records
    Scan scanWithoutClose = new Scan();
    scanWithoutClose.setCaching(1);
    scanWithoutClose.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));
    ResultScanner scannerWithoutClose = ht.getScanner(scanWithoutClose);
    for (Result result : scannerWithoutClose.next(numRecords + 1)) {
    }
    ScanMetrics scanMetricsWithoutClose = getScanMetrics(scanWithoutClose);
    assertEquals("Did not access all the regions in the table", numOfRegions,
        scanMetricsWithoutClose.countOfRegions.get());

    // finally, test that the metrics are collected correctly if you both run past all the records,
    // AND close the scanner
    Scan scanWithClose = new Scan();
    // make sure we can set caching up to the number of a scanned values
    scanWithClose.setCaching(numRecords);
    scanWithClose.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));
    ResultScanner scannerWithClose = ht.getScanner(scanWithClose);
    for (Result result : scannerWithClose.next(numRecords + 1)) {
    }
    scannerWithClose.close();
    ScanMetrics scanMetricsWithClose = getScanMetrics(scanWithClose);
    assertEquals("Did not access all the regions in the table", numOfRegions,
        scanMetricsWithClose.countOfRegions.get());
  }

  private ScanMetrics getScanMetrics(Scan scan) throws Exception {
    byte[] serializedMetrics = scan.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA);
    assertTrue("Serialized metrics were not found.", serializedMetrics != null);


    ScanMetrics scanMetrics = ProtobufUtil.toScanMetrics(serializedMetrics);

    return scanMetrics;
  }

  /**
   * Tests that cache on write works all the way up from the client-side.
   *
   * Performs inserts, flushes, and compactions, verifying changes in the block
   * cache along the way.
   *
   * @throws Exception
   */
  @Test
  public void testCacheOnWriteEvictOnClose() throws Exception {
    TableName tableName = TableName.valueOf("testCOWEOCfromClient");
    byte [] data = Bytes.toBytes("data");
    HTable table = TEST_UTIL.createTable(tableName, FAMILY);
    // get the block cache and region
    String regionName = table.getRegionLocations().firstKey().getEncodedName();
    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getFromOnlineRegions(regionName);
    Store store = region.getStores().values().iterator().next();
    CacheConfig cacheConf = store.getCacheConfig();
    cacheConf.setCacheDataOnWrite(true);
    cacheConf.setEvictOnClose(true);
    BlockCache cache = cacheConf.getBlockCache();

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
    put.add(FAMILY, QUALIFIER, data);
    table.put(put);
    assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
    // data was in memstore so don't expect any changes
    assertEquals(startBlockCount, cache.getBlockCount());
    assertEquals(startBlockHits, cache.getStats().getHitCount());
    assertEquals(startBlockMiss, cache.getStats().getMissCount());
    // flush the data
    System.out.println("Flushing cache");
    region.flushcache();
    // expect one more block in cache, no change in hits/misses
    long expectedBlockCount = startBlockCount + 1;
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
    byte [] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
    byte [] data2 = Bytes.add(data, data);
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER2, data2);
    table.put(put);
    Result r = table.get(new Get(ROW));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
    assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
    assertEquals(expectedBlockCount, cache.getBlockCount());
    assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // flush, one new block
    System.out.println("Flushing cache");
    region.flushcache();
    assertEquals(++expectedBlockCount, cache.getBlockCount());
    assertEquals(expectedBlockHits, cache.getStats().getHitCount());
    assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
    // compact, net minus two blocks, two hits, no misses
    System.out.println("Compacting");
    assertEquals(2, store.getStorefilesCount());
    store.triggerMajorCompaction();
    region.compactStores();
    waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
    assertEquals(1, store.getStorefilesCount());
    expectedBlockCount -= 2; // evicted two blocks, cached none
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

  private void waitForStoreFileCount(Store store, int count, int timeout)
  throws InterruptedException {
    long start = System.currentTimeMillis();
    while (start + timeout > System.currentTimeMillis() &&
        store.getStorefilesCount() != count) {
      Thread.sleep(100);
    }
    System.out.println("start=" + start + ", now=" +
        System.currentTimeMillis() + ", cur=" + store.getStorefilesCount());
    assertEquals(count, store.getStorefilesCount());
  }

  @Test
  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testNonCachedGetRegionLocation");
    byte [] family1 = Bytes.toBytes("f1");
    byte [] family2 = Bytes.toBytes("f2");
    HTable table = TEST_UTIL.createTable(TABLE, new byte[][] {family1, family2}, 10);
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    Map <HRegionInfo, ServerName> regionsMap = table.getRegionLocations();
    assertEquals(1, regionsMap.size());
    HRegionInfo regionInfo = regionsMap.keySet().iterator().next();
    ServerName addrBefore = regionsMap.get(regionInfo);
    // Verify region location before move.
    HRegionLocation addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
    HRegionLocation addrNoCache = table.getRegionLocation(regionInfo.getStartKey(),  true);

    assertEquals(addrBefore.getPort(), addrCache.getPort());
    assertEquals(addrBefore.getPort(), addrNoCache.getPort());

    ServerName addrAfter = null;
    // Now move the region to a different server.
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
      ServerName addr = regionServer.getServerName();
      if (addr.getPort() != addrBefore.getPort()) {
        admin.move(regionInfo.getEncodedNameAsBytes(),
            Bytes.toBytes(addr.toString()));
        // Wait for the region to move.
        Thread.sleep(5000);
        addrAfter = addr;
        break;
      }
    }

    // Verify the region was moved.
    addrCache = table.getRegionLocation(regionInfo.getStartKey(), false);
    addrNoCache = table.getRegionLocation(regionInfo.getStartKey(), true);
    assertNotNull(addrAfter);
    assertTrue(addrAfter.getPort() != addrCache.getPort());
    assertEquals(addrAfter.getPort(), addrNoCache.getPort());
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
    TableName TABLE = TableName.valueOf("testGetRegionsInRange");
    HTable table = TEST_UTIL.createTable(TABLE, new byte[][] {FAMILY}, 10);
    int numOfRegions = TEST_UTIL.createMultiRegions(table, FAMILY);
    assertEquals(25, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = table.getRegionsInRange(startKey,
      endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = table.getRegionsInRange(startKey, HConstants.EMPTY_END_ROW);
    assertEquals(20, regionsList.size());

    // Both start and end keys empty
    regionsList = table.getRegionsInRange(HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
    assertEquals(25, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("yyz");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(20, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(25, regionsList.size());

    // Make start and end key the same
    startKey = endKey = Bytes.toBytes("ccc");
    regionsList = table.getRegionsInRange(startKey, endKey);
    assertEquals(1, regionsList.size());
  }

  @Test
  public void testJira6912() throws Exception {
    TableName TABLE = TableName.valueOf("testJira6912");
    Table foo = TEST_UTIL.createTable(TABLE, new byte[][] {FAMILY}, 10);

    List<Put> puts = new ArrayList<Put>();
    for (int i=0;i !=100; i++){
      Put put = new Put(Bytes.toBytes(i));
      put.add(FAMILY, FAMILY, Bytes.toBytes(i));
      puts.add(put);
    }
    foo.put(puts);
    // If i comment this out it works
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(1));
    scan.setStopRow(Bytes.toBytes(3));
    scan.addColumn(FAMILY, FAMILY);
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(1))));

    ResultScanner scanner = foo.getScanner(scan);
    Result[] bar = scanner.next(100);
    assertEquals(1, bar.length);
  }

  @Test
  public void testScan_NullQualifier() throws IOException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testScan_NullQualifier"), FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);

    put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
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

  @Test
  public void testNegativeTimestamp() throws IOException {
    Table table = TEST_UTIL.createTable(TableName.valueOf("testNegativeTimestamp"), FAMILY);

    try {
      Put put = new Put(ROW, -1);
      put.add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      fail("Negative timestamps should not have been allowed");
    } catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains("negative"));
    }

    try {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, -1, VALUE);
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
      delete.deleteFamily(FAMILY, -1);
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
      new KeyValue(Bytes.toBytes(42), Bytes.toBytes(42), Bytes.toBytes(42), -1, Bytes.toBytes(42));
    } catch (IllegalArgumentException ex) {
      fail("KeyValue SHOULD allow negative timestamps");
    }

    table.close();
  }

  @Test
  public void testIllegalTableDescriptor() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testIllegalTableDescriptor"));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);

    // create table with 0 families
    checkTableIsIllegal(htd);
    htd.addFamily(hcd);
    checkTableIsLegal(htd);

    htd.setMaxFileSize(1024); // 1K
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(0);
    checkTableIsIllegal(htd);
    htd.setMaxFileSize(1024 * 1024 * 1024); // 1G
    checkTableIsLegal(htd);

    htd.setMemStoreFlushSize(1024);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(0);
    checkTableIsIllegal(htd);
    htd.setMemStoreFlushSize(128 * 1024 * 1024); // 128M
    checkTableIsLegal(htd);

    htd.setRegionSplitPolicyClassName("nonexisting.foo.class");
    checkTableIsIllegal(htd);
    htd.setRegionSplitPolicyClassName(null);
    checkTableIsLegal(htd);

    hcd.setBlocksize(0);
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024 * 1024 * 128); // 128M
    checkTableIsIllegal(htd);
    hcd.setBlocksize(1024);
    checkTableIsLegal(htd);

    hcd.setTimeToLive(0);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(-1);
    checkTableIsIllegal(htd);
    hcd.setTimeToLive(1);
    checkTableIsLegal(htd);

    hcd.setMinVersions(-1);
    checkTableIsIllegal(htd);
    hcd.setMinVersions(3);
    try {
      hcd.setMaxVersions(2);
      fail();
    } catch (IllegalArgumentException ex) {
      // expected
      hcd.setMaxVersions(10);
    }
    checkTableIsLegal(htd);

    hcd.setScope(-1);
    checkTableIsIllegal(htd);
    hcd.setScope(0);
    checkTableIsLegal(htd);

    // check the conf settings to disable sanity checks
    htd.setMemStoreFlushSize(0);
    htd.setConfiguration("hbase.table.sanity.checks", Boolean.FALSE.toString());
    checkTableIsLegal(htd);
  }

  private void checkTableIsLegal(HTableDescriptor htd) throws IOException {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(htd);
    assertTrue(admin.tableExists(htd.getTableName()));
    admin.disableTable(htd.getTableName());
    admin.deleteTable(htd.getTableName());
  }

  private void checkTableIsIllegal(HTableDescriptor htd) throws IOException {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    try {
      admin.createTable(htd);
      fail();
    } catch(Exception ex) {
      // should throw ex
    }
    assertFalse(admin.tableExists(htd.getTableName()));
  }

  @Test
  public void testRawScanRespectsVersions() throws Exception {
    TableName TABLE = TableName.valueOf("testRawScan");
    Table table = TEST_UTIL.createTable(TABLE, FAMILY);
    byte[] row = Bytes.toBytes("row");

    // put the same row 4 times, with different values
    Put p = new Put(row);
    p.add(FAMILY, QUALIFIER, 10, VALUE);
    table.put(p);
    table.flushCommits();

    p = new Put(row);
    p.add(FAMILY, QUALIFIER, 11, ArrayUtils.add(VALUE, (byte) 2));
    table.put(p);
    table.flushCommits();

    p = new Put(row);
    p.add(FAMILY, QUALIFIER, 12, ArrayUtils.add(VALUE, (byte) 3));
    table.put(p);
    table.flushCommits();

    p = new Put(row);
    p.add(FAMILY, QUALIFIER, 13, ArrayUtils.add(VALUE, (byte) 4));
    table.put(p);
    table.flushCommits();

    int versions = 4;
    Scan s = new Scan(row);
    // get all the possible versions
    s.setMaxVersions();
    s.setRaw(true);

    ResultScanner scanner = table.getScanner(s);
    int count = 0;
    for (Result r : scanner) {
      assertEquals("Found an unexpected number of results for the row!", versions, r.listCells().size());
      count++;
    }
    assertEquals("Found more than a single row when raw scanning the table with a single row!", 1,
      count);
    scanner.close();

    // then if we decrease the number of versions, but keep the scan raw, we should see exactly that
    // number of versions
    versions = 2;
    s.setMaxVersions(versions);
    scanner = table.getScanner(s);
    count = 0;
    for (Result r : scanner) {
      assertEquals("Found an unexpected number of results for the row!", versions, r.listCells().size());
      count++;
    }
    assertEquals("Found more than a single row when raw scanning the table with a single row!", 1,
      count);
    scanner.close();

    // finally, if we turn off raw scanning, but max out the number of versions, we should go back
    // to seeing just three
    versions = 3;
    s.setMaxVersions(versions);
    scanner = table.getScanner(s);
    count = 0;
    for (Result r : scanner) {
      assertEquals("Found an unexpected number of results for the row!", versions, r.listCells().size());
      count++;
    }
    assertEquals("Found more than a single row when raw scanning the table with a single row!", 1,
      count);
    scanner.close();

    table.close();
    TEST_UTIL.deleteTable(TABLE);
  }

  @Test
  public void testSmallScan() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testSmallScan");
    Table table = TEST_UTIL.createTable(TABLE, FAMILY);

    // Insert one row each region
    int insertNum = 10;
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + String.format("%03d", i)));
      put.add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
    }

    // nomal scan
    ResultScanner scanner = table.getScanner(new Scan());
    int count = 0;
    for (Result r : scanner) {
      assertTrue(!r.isEmpty());
      count++;
    }
    assertEquals(insertNum, count);

    // small scan
    Scan scan = new Scan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    scan.setSmall(true);
    scan.setCaching(2);
    scanner = table.getScanner(scan);
    count = 0;
    for (Result r : scanner) {
      assertTrue(!r.isEmpty());
      count++;
    }
    assertEquals(insertNum, count);

  }

  @Test
  public void testSuperSimpleWithReverseScan() throws Exception {
    TableName TABLE = TableName.valueOf("testSuperSimpleWithReverseScan");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    Put put = new Put(Bytes.toBytes("0-b11111-0000000000000000000"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b11111-0000000000000000002"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b11111-0000000000000000004"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b11111-0000000000000000006"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b11111-0000000000000000008"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b22222-0000000000000000001"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b22222-0000000000000000003"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b22222-0000000000000000005"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b22222-0000000000000000007"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    put = new Put(Bytes.toBytes("0-b22222-0000000000000000009"));
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    ht.flushCommits();
    Scan scan = new Scan(Bytes.toBytes("0-b11111-9223372036854775807"),
        Bytes.toBytes("0-b11111-0000000000000000000"));
    scan.setReversed(true);
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    assertTrue(Bytes.equals(result.getRow(),
        Bytes.toBytes("0-b11111-0000000000000000008")));
    scanner.close();
    ht.close();
  }

  @Test
  public void testFiltersWithReverseScan() throws Exception {
    TableName TABLE = TableName.valueOf("testFiltersWithReverseScan");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte[][] ROWS = makeN(ROW, 10);
    byte[][] QUALIFIERS = { Bytes.toBytes("col0-<d2v1>-<d3v2>"),
        Bytes.toBytes("col1-<d2v1>-<d3v2>"),
        Bytes.toBytes("col2-<d2v1>-<d3v2>"),
        Bytes.toBytes("col3-<d2v1>-<d3v2>"),
        Bytes.toBytes("col4-<d2v1>-<d3v2>"),
        Bytes.toBytes("col5-<d2v1>-<d3v2>"),
        Bytes.toBytes("col6-<d2v1>-<d3v2>"),
        Bytes.toBytes("col7-<d2v1>-<d3v2>"),
        Bytes.toBytes("col8-<d2v1>-<d3v2>"),
        Bytes.toBytes("col9-<d2v1>-<d3v2>") };
    for (int i = 0; i < 10; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIERS[i], VALUE);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.setReversed(true);
    scan.addFamily(FAMILY);
    Filter filter = new QualifierFilter(CompareOp.EQUAL,
        new RegexStringComparator("col[1-5]"));
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int expectedIndex = 5;
    for (Result result : scanner) {
      assertEquals(result.size(), 1);
      assertTrue(Bytes.equals(result.raw()[0].getRow(), ROWS[expectedIndex]));
      assertTrue(Bytes.equals(result.raw()[0].getQualifier(),
          QUALIFIERS[expectedIndex]));
      expectedIndex--;
    }
    assertEquals(expectedIndex, 0);
    scanner.close();
    ht.close();
  }

  @Test
  public void testKeyOnlyFilterWithReverseScan() throws Exception {
    TableName TABLE = TableName.valueOf("testKeyOnlyFilterWithReverseScan");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    byte[][] ROWS = makeN(ROW, 10);
    byte[][] QUALIFIERS = { Bytes.toBytes("col0-<d2v1>-<d3v2>"),
        Bytes.toBytes("col1-<d2v1>-<d3v2>"),
        Bytes.toBytes("col2-<d2v1>-<d3v2>"),
        Bytes.toBytes("col3-<d2v1>-<d3v2>"),
        Bytes.toBytes("col4-<d2v1>-<d3v2>"),
        Bytes.toBytes("col5-<d2v1>-<d3v2>"),
        Bytes.toBytes("col6-<d2v1>-<d3v2>"),
        Bytes.toBytes("col7-<d2v1>-<d3v2>"),
        Bytes.toBytes("col8-<d2v1>-<d3v2>"),
        Bytes.toBytes("col9-<d2v1>-<d3v2>") };
    for (int i = 0; i < 10; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIERS[i], VALUE);
      ht.put(put);
    }
    Scan scan = new Scan();
    scan.setReversed(true);
    scan.addFamily(FAMILY);
    Filter filter = new KeyOnlyFilter(true);
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    int count = 0;
    for (Result result : ht.getScanner(scan)) {
      assertEquals(result.size(), 1);
      assertEquals(result.raw()[0].getValueLength(), Bytes.SIZEOF_INT);
      assertEquals(Bytes.toInt(result.raw()[0].getValue()), VALUE.length);
      count++;
    }
    assertEquals(count, 10);
    scanner.close();
    ht.close();
  }

  /**
   * Test simple table and non-existent row cases.
   */
  @Test
  public void testSimpleMissingWithReverseScan() throws Exception {
    TableName TABLE = TableName.valueOf("testSimpleMissingWithReverseScan");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
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
    put.add(FAMILY, QUALIFIER, VALUE);
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
    ht.close();
  }

  @Test
  public void testNullWithReverseScan() throws Exception {
    TableName TABLE = TableName.valueOf("testNullWithReverseScan");
    Table ht = TEST_UTIL.createTable(TABLE, FAMILY);
    // Null qualifier (should work)
    Put put = new Put(ROW);
    put.add(FAMILY, null, VALUE);
    ht.put(put);
    scanTestNull(ht, ROW, FAMILY, VALUE, true);
    Delete delete = new Delete(ROW);
    delete.deleteColumns(FAMILY, null);
    ht.delete(delete);
    // Use a new table
    byte[] TABLE2 = Bytes.toBytes("testNull2WithReverseScan");
    ht = TEST_UTIL.createTable(TableName.valueOf(TABLE2), FAMILY);
    // Empty qualifier, byte[0] instead of null (should work)
    put = new Put(ROW);
    put.add(FAMILY, HConstants.EMPTY_BYTE_ARRAY, VALUE);
    ht.put(put);
    scanTestNull(ht, ROW, FAMILY, VALUE, true);
    TEST_UTIL.flush();
    scanTestNull(ht, ROW, FAMILY, VALUE, true);
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILY, HConstants.EMPTY_BYTE_ARRAY);
    ht.delete(delete);
    // Null value
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, null);
    ht.put(put);
    Scan scan = new Scan();
    scan.setReversed(true);
    scan.addColumn(FAMILY, QUALIFIER);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);
    ht.close();
  }

  @Test
  public void testDeletesWithReverseScan() throws Exception {
    TableName TABLE = TableName.valueOf("testDeletesWithReverseScan");
    byte[][] ROWS = makeNAscii(ROW, 6);
    byte[][] FAMILIES = makeNAscii(FAMILY, 3);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };
    Table ht = TEST_UTIL.createTable(TABLE, FAMILIES, TEST_UTIL.getConfiguration(), 3);

    Put put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
    ht.put(put);

    Delete delete = new Delete(ROW);
    delete.deleteFamily(FAMILIES[0], ts[0]);
    ht.delete(delete);

    Scan scan = new Scan(ROW);
    scan.setReversed(true);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1] },
        new byte[][] { VALUES[1] }, 0, 0);

    // Test delete latest version
    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
    put.add(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
    put.add(FAMILIES[0], null, ts[4], VALUES[4]);
    put.add(FAMILIES[0], null, ts[2], VALUES[2]);
    put.add(FAMILIES[0], null, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], QUALIFIER); // ts[4]
    ht.delete(delete);

    scan = new Scan(ROW);
    scan.setReversed(true);
    scan.addColumn(FAMILIES[0], QUALIFIER);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1],
        ts[2], ts[3] }, new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

    // Test for HBASE-1847
    delete = new Delete(ROW);
    delete.deleteColumn(FAMILIES[0], null);
    ht.delete(delete);

    // Cleanup null qualifier
    delete = new Delete(ROW);
    delete.deleteColumns(FAMILIES[0], null);
    ht.delete(delete);

    // Expected client behavior might be that you can re-put deleted values
    // But alas, this is not to be. We can't put them back in either case.

    put = new Put(ROW);
    put.add(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]); // 1000
    put.add(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]); // 5000
    ht.put(put);

    // The Scanner returns the previous values, the expected-naive-unexpected
    // behavior

    scan = new Scan(ROW);
    scan.setReversed(true);
    scan.addFamily(FAMILIES[0]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1],
        ts[2], ts[3] }, new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

    // Test deleting an entire family from one row but not the other various
    // ways

    put = new Put(ROWS[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[1]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    put = new Put(ROWS[2]);
    put.add(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
    put.add(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
    put.add(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
    ht.put(put);

    delete = new Delete(ROWS[0]);
    delete.deleteFamily(FAMILIES[2]);
    ht.delete(delete);

    delete = new Delete(ROWS[1]);
    delete.deleteColumns(FAMILIES[1], QUALIFIER);
    ht.delete(delete);

    delete = new Delete(ROWS[2]);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[1], QUALIFIER);
    delete.deleteColumn(FAMILIES[2], QUALIFIER);
    ht.delete(delete);

    scan = new Scan(ROWS[0]);
    scan.setReversed(true);
    scan.addFamily(FAMILIES[1]);
    scan.addFamily(FAMILIES[2]);
    scan.setMaxVersions(Integer.MAX_VALUE);
    result = getSingleScanResult(ht, scan);
    assertTrue("Expected 2 keys but received " + result.size(),
        result.size() == 2);
    assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, new long[] { ts[0],
        ts[1] }, new byte[][] { VALUES[0], VALUES[1] }, 0, 1);

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
        new long[] { ts[2] }, new byte[][] { VALUES[2] }, 0, 0);

    // Test if we delete the family first in one row (HBASE-1541)

    delete = new Delete(ROWS[3]);
    delete.deleteFamily(FAMILIES[1]);
    ht.delete(delete);

    put = new Put(ROWS[3]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[0]);
    ht.put(put);

    put = new Put(ROWS[4]);
    put.add(FAMILIES[1], QUALIFIER, VALUES[1]);
    put.add(FAMILIES[2], QUALIFIER, VALUES[2]);
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
    assertTrue(Bytes.equals(result.raw()[0].getRow(), ROWS[4]));
    assertTrue(Bytes.equals(result.raw()[1].getRow(), ROWS[4]));
    assertTrue(Bytes.equals(result.raw()[0].getValue(), VALUES[1]));
    assertTrue(Bytes.equals(result.raw()[1].getValue(), VALUES[2]));
    result = scanner.next();
    assertTrue("Expected 1 key but received " + result.size(),
        result.size() == 1);
    assertTrue(Bytes.equals(result.raw()[0].getRow(), ROWS[3]));
    assertTrue(Bytes.equals(result.raw()[0].getValue(), VALUES[0]));
    scanner.close();
    ht.close();
  }

  /**
   * Tests reversed scan under multi regions
   */
  @Test
  public void testReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testReversedScanUnderMultiRegions");
    byte[] maxByteArray = ReversedClientScanner.MAX_BYTE_ARRAY;
    byte[][] splitRows = new byte[][] { Bytes.toBytes("005"),
        Bytes.add(Bytes.toBytes("005"), Bytes.multiple(maxByteArray, 16)),
        Bytes.toBytes("006"),
        Bytes.add(Bytes.toBytes("006"), Bytes.multiple(maxByteArray, 8)),
        Bytes.toBytes("007"),
        Bytes.add(Bytes.toBytes("007"), Bytes.multiple(maxByteArray, 4)),
        Bytes.toBytes("008"), Bytes.multiple(maxByteArray, 2) };
    HTable table = TEST_UTIL.createTable(TABLE, FAMILY, splitRows);
    TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());

    assertEquals(splitRows.length + 1, table.getRegionLocations().size());
    // Insert one row each region
    int insertNum = splitRows.length;
    for (int i = 0; i < insertNum; i++) {
      Put put = new Put(splitRows[i]);
      put.add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
    }

    // scan forward
    ResultScanner scanner = table.getScanner(new Scan());
    int count = 0;
    for (Result r : scanner) {
      assertTrue(!r.isEmpty());
      count++;
    }
    assertEquals(insertNum, count);

    // scan backward
    Scan scan = new Scan();
    scan.setReversed(true);
    scanner = table.getScanner(scan);
    count = 0;
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
    table.close();
  }

  /**
   * Tests reversed scan under multi regions
   */
  @Test
  public void testSmallReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    TableName TABLE = TableName.valueOf("testSmallReversedScanUnderMultiRegions");
    byte[][] splitRows = new byte[][]{
        Bytes.toBytes("000"), Bytes.toBytes("002"), Bytes.toBytes("004"),
        Bytes.toBytes("006"), Bytes.toBytes("008"), Bytes.toBytes("010")};
    HTable table = TEST_UTIL.createTable(TABLE, FAMILY, splitRows);
    TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());

    assertEquals(splitRows.length + 1, table.getRegionLocations().size());
    for (byte[] splitRow : splitRows) {
      Put put = new Put(splitRow);
      put.add(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      byte[] nextRow = Bytes.copy(splitRow);
      nextRow[nextRow.length - 1]++;

      put = new Put(nextRow);
      put.add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
    }

    // scan forward
    ResultScanner scanner = table.getScanner(new Scan());
    int count = 0;
    for (Result r : scanner) {
      assertTrue(!r.isEmpty());
      count++;
    }
    assertEquals(12, count);

    reverseScanTest(table, false);
    reverseScanTest(table, true);

    table.close();
  }

  private void reverseScanTest(Table table, boolean small) throws IOException {
    // scan backward
    Scan scan = new Scan();
    scan.setReversed(true);
    ResultScanner scanner = table.getScanner(scan);
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

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("002"));
    scanner = table.getScanner(scan);
    count = 0;
    lastRow = null;
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

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("002"));
    scan.setStopRow(Bytes.toBytes("000"));
    scanner = table.getScanner(scan);
    count = 0;
    lastRow = null;
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

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("001"));
    scanner = table.getScanner(scan);
    count = 0;
    lastRow = null;
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

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("000"));
    scanner = table.getScanner(scan);
    count = 0;
    lastRow = null;
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

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("006"));
    scan.setStopRow(Bytes.toBytes("002"));
    scanner = table.getScanner(scan);
    count = 0;
    lastRow = null;
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

  @Test
  public void testGetStartEndKeysWithRegionReplicas() throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testGetStartEndKeys"));
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    htd.addFamily(fam);
    byte[][] KEYS = HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE;
    TEST_UTIL.getHBaseAdmin().createTable(htd, KEYS);
    List<HRegionInfo> regions = TEST_UTIL.getHBaseAdmin().getTableRegions(htd.getTableName());

    for (int regionReplication = 1; regionReplication < 4 ; regionReplication++) {
      List<RegionLocations> regionLocations = new ArrayList<RegionLocations>();

      // mock region locations coming from meta with multiple replicas
      for (HRegionInfo region : regions) {
        HRegionLocation[] arr = new HRegionLocation[regionReplication];
        for (int i = 0; i < arr.length; i++) {
          arr[i] = new HRegionLocation(RegionReplicaUtil.getRegionInfoForReplica(region, i), null);
        }
        regionLocations.add(new RegionLocations(arr));
      }

      HTable table = spy(new HTable(TEST_UTIL.getConfiguration(), htd.getTableName()));
      when(table.listRegionLocations()).thenReturn(regionLocations);

      Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();

      assertEquals(KEYS.length + 1, startEndKeys.getFirst().length);

      for (int i = 0; i < KEYS.length + 1; i++) {
        byte[] startKey = i == 0 ? HConstants.EMPTY_START_ROW : KEYS[i - 1];
        byte[] endKey = i == KEYS.length ? HConstants.EMPTY_END_ROW : KEYS[i];
        assertArrayEquals(startKey, startEndKeys.getFirst()[i]);
        assertArrayEquals(endKey, startEndKeys.getSecond()[i]);
      }

      table.close();
    }
  }

}
