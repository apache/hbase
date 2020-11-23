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
package org.apache.hadoop.hbase.tool;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;

/**
 * Test cases for the atomic load error handling of the bulk load functionality.
 */
@Category({ MiscTests.class, LargeTests.class })
public class TestLoadIncrementalHFilesSplitRecovery {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLoadIncrementalHFilesSplitRecovery.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHRegionServerBulkLoad.class);

  static HBaseTestingUtility util;
  // used by secure subclass
  static boolean useSecure = false;

  final static int NUM_CFS = 10;
  final static byte[] QUAL = Bytes.toBytes("qual");
  final static int ROWCOUNT = 100;

  private final static byte[][] families = new byte[NUM_CFS][];

  @Rule
  public TestName name = new TestName();

  static {
    for (int i = 0; i < NUM_CFS; i++) {
      families[i] = Bytes.toBytes(family(i));
    }
  }

  static byte[] rowkey(int i) {
    return Bytes.toBytes(String.format("row_%08d", i));
  }

  static String family(int i) {
    return String.format("family_%04d", i);
  }

  static byte[] value(int i) {
    return Bytes.toBytes(String.format("%010d", i));
  }

  public static void buildHFiles(FileSystem fs, Path dir, int value) throws IOException {
    byte[] val = value(value);
    for (int i = 0; i < NUM_CFS; i++) {
      Path testIn = new Path(dir, family(i));

      TestHRegionServerBulkLoad.createHFile(fs, new Path(testIn, "hfile_" + i),
        Bytes.toBytes(family(i)), QUAL, val, ROWCOUNT);
    }
  }

  private TableDescriptor createTableDesc(TableName name, int cfs) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name);
    IntStream.range(0, cfs).mapToObj(i -> ColumnFamilyDescriptorBuilder.of(family(i)))
        .forEachOrdered(builder::setColumnFamily);
    return builder.build();
  }

  /**
   * Creates a table with given table name and specified number of column families if the table does
   * not already exist.
   */
  private void setupTable(final Connection connection, TableName table, int cfs)
      throws IOException {
    try {
      LOG.info("Creating table " + table);
      try (Admin admin = connection.getAdmin()) {
        admin.createTable(createTableDesc(table, cfs));
      }
    } catch (TableExistsException tee) {
      LOG.info("Table " + table + " already exists");
    }
  }

  /**
   * Creates a table with given table name,specified number of column families<br>
   * and splitkeys if the table does not already exist.
   * @param table
   * @param cfs
   * @param SPLIT_KEYS
   */
  private void setupTableWithSplitkeys(TableName table, int cfs, byte[][] SPLIT_KEYS)
      throws IOException {
    try {
      LOG.info("Creating table " + table);
      util.createTable(createTableDesc(table, cfs), SPLIT_KEYS);
    } catch (TableExistsException tee) {
      LOG.info("Table " + table + " already exists");
    }
  }

  private Path buildBulkFiles(TableName table, int value) throws Exception {
    Path dir = util.getDataTestDirOnTestFS(table.getNameAsString());
    Path bulk1 = new Path(dir, table.getNameAsString() + value);
    FileSystem fs = util.getTestFileSystem();
    buildHFiles(fs, bulk1, value);
    return bulk1;
  }

  /**
   * Populate table with known values.
   */
  private void populateTable(final Connection connection, TableName table, int value)
      throws Exception {
    // create HFiles for different column families
    LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration());
    Path bulk1 = buildBulkFiles(table, value);
    try (Table t = connection.getTable(table);
        RegionLocator locator = connection.getRegionLocator(table);
        Admin admin = connection.getAdmin()) {
      lih.doBulkLoad(bulk1, admin, t, locator);
    }
  }

  /**
   * Split the known table in half. (this is hard coded for this test suite)
   */
  private void forceSplit(TableName table) {
    try {
      // need to call regions server to by synchronous but isn't visible.
      HRegionServer hrs = util.getRSForFirstRegionInTable(table);

      for (RegionInfo hri : ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices())) {
        if (hri.getTable().equals(table)) {
          util.getAdmin().splitRegionAsync(hri.getRegionName(), rowkey(ROWCOUNT / 2));
          // ProtobufUtil.split(null, hrs.getRSRpcServices(), hri, rowkey(ROWCOUNT / 2));
        }
      }

      // verify that split completed.
      int regions;
      do {
        regions = 0;
        for (RegionInfo hri : ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices())) {
          if (hri.getTable().equals(table)) {
            regions++;
          }
        }
        if (regions != 2) {
          LOG.info("Taking some time to complete split...");
          Thread.sleep(250);
        }
      } while (regions != 2);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    util = new HBaseTestingUtility();
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    util.startMiniCluster(1);
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * Checks that all columns have the expected value and that there is the expected number of rows.
   * @throws IOException
   */
  void assertExpectedTable(TableName table, int count, int value) throws IOException {
    TableDescriptor htd = util.getAdmin().getDescriptor(table);
    assertNotNull(htd);
    try (Table t = util.getConnection().getTable(table);
        ResultScanner sr = t.getScanner(new Scan())) {
      int i = 0;
      for (Result r; (r = sr.next()) != null;) {
        r.getNoVersionMap().values().stream().flatMap(m -> m.values().stream())
            .forEach(v -> assertArrayEquals(value(value), v));
        i++;
      }
      assertEquals(count, i);
    } catch (IOException e) {
      fail("Failed due to exception");
    }
  }

  /**
   * Test that shows that exception thrown from the RS side will result in an exception on the
   * LIHFile client.
   */
  @Test(expected = IOException.class)
  public void testBulkLoadPhaseFailure() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    final AtomicInteger attmptedCalls = new AtomicInteger();
    final AtomicInteger failedCalls = new AtomicInteger();
    util.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTable(connection, table, 10);
      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration()) {
        @Override
        protected List<LoadQueueItem> tryAtomicRegionLoad(Connection connection,
            TableName tableName, final byte[] first, Collection<LoadQueueItem> lqis,
            boolean copyFile) throws IOException {
          int i = attmptedCalls.incrementAndGet();
          if (i == 1) {
            Connection errConn;
            try {
              errConn = getMockedConnection(util.getConfiguration());
            } catch (Exception e) {
              LOG.error(HBaseMarkers.FATAL, "mocking cruft, should never happen", e);
              throw new RuntimeException("mocking cruft, should never happen");
            }
            failedCalls.incrementAndGet();
            return super.tryAtomicRegionLoad(errConn, tableName, first, lqis, true);
          }

          return super.tryAtomicRegionLoad(connection, tableName, first, lqis, true);
        }
      };
      try {
        // create HFiles for different column families
        Path dir = buildBulkFiles(table, 1);
        try (Table t = connection.getTable(table);
            RegionLocator locator = connection.getRegionLocator(table);
            Admin admin = connection.getAdmin()) {
          lih.doBulkLoad(dir, admin, t, locator);
        }
      } finally {
        util.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
      }
      fail("doBulkLoad should have thrown an exception");
    }
  }

  /**
   * Test that shows that exception thrown from the RS side will result in the expected number of
   * retries set by ${@link HConstants#HBASE_CLIENT_RETRIES_NUMBER} when
   * ${@link LoadIncrementalHFiles#RETRY_ON_IO_EXCEPTION} is set
   */
  @Test
  public void testRetryOnIOException() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    final AtomicInteger calls = new AtomicInteger(0);
    final Connection conn = ConnectionFactory.createConnection(util.getConfiguration());
    util.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    util.getConfiguration().setBoolean(LoadIncrementalHFiles.RETRY_ON_IO_EXCEPTION, true);
    final LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration()) {
      @Override
      protected ClientServiceCallable<byte[]> buildClientServiceCallable(Connection conn,
          TableName tableName, byte[] first, Collection<LoadQueueItem> lqis, boolean copyFile) {
        if (calls.get() < util.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER)) {
          calls.getAndIncrement();
          return new ClientServiceCallable<byte[]>(conn, tableName, first,
              new RpcControllerFactory(util.getConfiguration()).newController(),
              HConstants.PRIORITY_UNSET) {
            @Override
            public byte[] rpcCall() throws Exception {
              throw new IOException("Error calling something on RegionServer");
            }
          };
        } else {
          return super.buildClientServiceCallable(conn, tableName, first, lqis, true);
        }
      }
    };
    setupTable(conn, table, 10);
    Path dir = buildBulkFiles(table, 1);
    lih.doBulkLoad(dir, conn.getAdmin(), conn.getTable(table), conn.getRegionLocator(table));
    assertEquals(calls.get(), 2);
    util.getConfiguration().setBoolean(LoadIncrementalHFiles.RETRY_ON_IO_EXCEPTION, false);
  }

  private ClusterConnection getMockedConnection(final Configuration conf)
      throws IOException, org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {
    ClusterConnection c = Mockito.mock(ClusterConnection.class);
    Mockito.when(c.getConfiguration()).thenReturn(conf);
    Mockito.doNothing().when(c).close();
    // Make it so we return a particular location when asked.
    final HRegionLocation loc = new HRegionLocation(RegionInfoBuilder.FIRST_META_REGIONINFO,
        ServerName.valueOf("example.org", 1234, 0));
    Mockito.when(
      c.getRegionLocation((TableName) Mockito.any(), (byte[]) Mockito.any(), Mockito.anyBoolean()))
        .thenReturn(loc);
    Mockito.when(c.locateRegion((TableName) Mockito.any(), (byte[]) Mockito.any())).thenReturn(loc);
    ClientProtos.ClientService.BlockingInterface hri =
        Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
    Mockito
        .when(
          hri.bulkLoadHFile((RpcController) Mockito.any(), (BulkLoadHFileRequest) Mockito.any()))
        .thenThrow(new ServiceException(new IOException("injecting bulk load error")));
    Mockito.when(c.getClient(Mockito.any())).thenReturn(hri);
    return c;
  }

  /**
   * This test exercises the path where there is a split after initial validation but before the
   * atomic bulk load call. We cannot use presplitting to test this path, so we actually inject a
   * split just before the atomic region load.
   */
  @Test
  public void testSplitWhileBulkLoadPhase() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTable(connection, table, 10);
      populateTable(connection, table, 1);
      assertExpectedTable(table, ROWCOUNT, 1);

      // Now let's cause trouble. This will occur after checks and cause bulk
      // files to fail when attempt to atomically import. This is recoverable.
      final AtomicInteger attemptedCalls = new AtomicInteger();
      LoadIncrementalHFiles lih2 = new LoadIncrementalHFiles(util.getConfiguration()) {
        @Override
        protected void bulkLoadPhase(final Table htable, final Connection conn,
            ExecutorService pool, Deque<LoadQueueItem> queue,
            final Multimap<ByteBuffer, LoadQueueItem> regionGroups, boolean copyFile,
            Map<LoadQueueItem, ByteBuffer> item2RegionMap) throws IOException {
          int i = attemptedCalls.incrementAndGet();
          if (i == 1) {
            // On first attempt force a split.
            forceSplit(table);
          }
          super.bulkLoadPhase(htable, conn, pool, queue, regionGroups, copyFile, item2RegionMap);
        }
      };

      // create HFiles for different column families
      try (Table t = connection.getTable(table);
          RegionLocator locator = connection.getRegionLocator(table);
          Admin admin = connection.getAdmin()) {
        Path bulk = buildBulkFiles(table, 2);
        lih2.doBulkLoad(bulk, admin, t, locator);
      }

      // check that data was loaded
      // The three expected attempts are 1) failure because need to split, 2)
      // load of split top 3) load of split bottom
      assertEquals(3, attemptedCalls.get());
      assertExpectedTable(table, ROWCOUNT, 2);
    }
  }

  /**
   * This test splits a table and attempts to bulk load. The bulk import files should be split
   * before atomically importing.
   */
  @Test
  public void testGroupOrSplitPresplit() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTable(connection, table, 10);
      populateTable(connection, table, 1);
      assertExpectedTable(connection, table, ROWCOUNT, 1);
      forceSplit(table);

      final AtomicInteger countedLqis = new AtomicInteger();
      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration()) {
        @Override
        protected Pair<List<LoadQueueItem>, String> groupOrSplit(
            Multimap<ByteBuffer, LoadQueueItem> regionGroups, final LoadQueueItem item,
            final Table htable, final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
          Pair<List<LoadQueueItem>, String> lqis =
              super.groupOrSplit(regionGroups, item, htable, startEndKeys);
          if (lqis != null && lqis.getFirst() != null) {
            countedLqis.addAndGet(lqis.getFirst().size());
          }
          return lqis;
        }
      };

      // create HFiles for different column families
      Path bulk = buildBulkFiles(table, 2);
      try (Table t = connection.getTable(table);
          RegionLocator locator = connection.getRegionLocator(table);
          Admin admin = connection.getAdmin()) {
        lih.doBulkLoad(bulk, admin, t, locator);
      }
      assertExpectedTable(connection, table, ROWCOUNT, 2);
      assertEquals(20, countedLqis.get());
    }
  }

  @Test
  public void testCorrectSplitPoint() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("row_00000010"),
        Bytes.toBytes("row_00000020"), Bytes.toBytes("row_00000030"), Bytes.toBytes("row_00000040"),
        Bytes.toBytes("row_00000050"), Bytes.toBytes("row_00000060"),
        Bytes.toBytes("row_00000070") };
    setupTableWithSplitkeys(table, NUM_CFS, SPLIT_KEYS);

    final AtomicInteger bulkloadRpcTimes = new AtomicInteger();
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(util.getConfiguration()) {

      @Override
      protected void bulkLoadPhase(Table table, Connection conn, ExecutorService pool,
          Deque<LoadIncrementalHFiles.LoadQueueItem> queue,
          Multimap<ByteBuffer, LoadIncrementalHFiles.LoadQueueItem> regionGroups, boolean copyFile,
          Map<LoadIncrementalHFiles.LoadQueueItem, ByteBuffer> item2RegionMap) throws IOException {
        bulkloadRpcTimes.addAndGet(1);
        super.bulkLoadPhase(table, conn, pool, queue, regionGroups, copyFile, item2RegionMap);
      }
    };

    Path dir = buildBulkFiles(table, 1);
    loader.bulkLoad(table, dir);
    // before HBASE-25281 we need invoke bulkload rpc 8 times
    assertEquals(4, bulkloadRpcTimes.get());
  }

  /**
   * This test creates a table with many small regions. The bulk load files would be splitted
   * multiple times before all of them can be loaded successfully.
   */
  @Test
  public void testSplitTmpFileCleanUp() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("row_00000010"),
        Bytes.toBytes("row_00000020"), Bytes.toBytes("row_00000030"), Bytes.toBytes("row_00000040"),
        Bytes.toBytes("row_00000050") };
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTableWithSplitkeys(table, 10, SPLIT_KEYS);

      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration());

      // create HFiles
      Path bulk = buildBulkFiles(table, 2);
      try (Table t = connection.getTable(table);
          RegionLocator locator = connection.getRegionLocator(table);
          Admin admin = connection.getAdmin()) {
        lih.doBulkLoad(bulk, admin, t, locator);
      }
      // family path
      Path tmpPath = new Path(bulk, family(0));
      // TMP_DIR under family path
      tmpPath = new Path(tmpPath, LoadIncrementalHFiles.TMP_DIR);
      FileSystem fs = bulk.getFileSystem(util.getConfiguration());
      // HFiles have been splitted, there is TMP_DIR
      assertTrue(fs.exists(tmpPath));
      // TMP_DIR should have been cleaned-up
      assertNull(LoadIncrementalHFiles.TMP_DIR + " should be empty.",
        CommonFSUtils.listStatus(fs, tmpPath));
      assertExpectedTable(connection, table, ROWCOUNT, 2);
    }
  }

  /**
   * This simulates an remote exception which should cause LIHF to exit with an exception.
   */
  @Test(expected = IOException.class)
  public void testGroupOrSplitFailure() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTable(connection, tableName, 10);

      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration()) {
        int i = 0;

        @Override
        protected Pair<List<LoadQueueItem>, String> groupOrSplit(
            Multimap<ByteBuffer, LoadQueueItem> regionGroups, final LoadQueueItem item,
            final Table table, final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
          i++;

          if (i == 5) {
            throw new IOException("failure");
          }
          return super.groupOrSplit(regionGroups, item, table, startEndKeys);
        }
      };

      // create HFiles for different column families
      Path dir = buildBulkFiles(tableName, 1);
      try (Table t = connection.getTable(tableName);
          RegionLocator locator = connection.getRegionLocator(tableName);
          Admin admin = connection.getAdmin()) {
        lih.doBulkLoad(dir, admin, t, locator);
      }
    }

    fail("doBulkLoad should have thrown an exception");
  }

  @Test
  public void testGroupOrSplitWhenRegionHoleExistsInMeta() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("row_00000100") };
    // Share connection. We were failing to find the table with our new reverse scan because it
    // looks for first region, not any region -- that is how it works now. The below removes first
    // region in test. Was reliant on the Connection caching having first region.
    Connection connection = ConnectionFactory.createConnection(util.getConfiguration());
    Table table = connection.getTable(tableName);

    setupTableWithSplitkeys(tableName, 10, SPLIT_KEYS);
    Path dir = buildBulkFiles(tableName, 2);

    final AtomicInteger countedLqis = new AtomicInteger();
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration()) {

      @Override
      protected Pair<List<LoadQueueItem>, String> groupOrSplit(
          Multimap<ByteBuffer, LoadQueueItem> regionGroups, final LoadQueueItem item,
          final Table htable, final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
        Pair<List<LoadQueueItem>, String> lqis =
            super.groupOrSplit(regionGroups, item, htable, startEndKeys);
        if (lqis != null && lqis.getFirst() != null) {
          countedLqis.addAndGet(lqis.getFirst().size());
        }
        return lqis;
      }
    };

    // do bulkload when there is no region hole in hbase:meta.
    try (Table t = connection.getTable(tableName);
        RegionLocator locator = connection.getRegionLocator(tableName);
        Admin admin = connection.getAdmin()) {
      loader.doBulkLoad(dir, admin, t, locator);
    } catch (Exception e) {
      LOG.error("exeception=", e);
    }
    // check if all the data are loaded into the table.
    this.assertExpectedTable(tableName, ROWCOUNT, 2);

    dir = buildBulkFiles(tableName, 3);

    // Mess it up by leaving a hole in the hbase:meta
    List<RegionInfo> regionInfos = MetaTableAccessor.getTableRegions(connection, tableName);
    for (RegionInfo regionInfo : regionInfos) {
      if (Bytes.equals(regionInfo.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
        MetaTableAccessor.deleteRegionInfo(connection, regionInfo);
        break;
      }
    }

    try (Table t = connection.getTable(tableName);
        RegionLocator locator = connection.getRegionLocator(tableName);
        Admin admin = connection.getAdmin()) {
      loader.doBulkLoad(dir, admin, t, locator);
    } catch (Exception e) {
      LOG.error("exception=", e);
      assertTrue("IOException expected", e instanceof IOException);
    }

    table.close();

    // Make sure at least the one region that still exists can be found.
    regionInfos = MetaTableAccessor.getTableRegions(connection, tableName);
    assertTrue(regionInfos.size() >= 1);

    this.assertExpectedTable(connection, tableName, ROWCOUNT, 2);
    connection.close();
  }

  /**
   * Checks that all columns have the expected value and that there is the expected number of rows.
   * @throws IOException
   */
  void assertExpectedTable(final Connection connection, TableName table, int count, int value)
      throws IOException {
    TableDescriptor htd = util.getAdmin().getDescriptor(table);
    assertNotNull(htd);
    try (Table t = connection.getTable(table); ResultScanner sr = t.getScanner(new Scan())) {
      int i = 0;
      for (Result r; (r = sr.next()) != null;) {
        r.getNoVersionMap().values().stream().flatMap(m -> m.values().stream())
            .forEach(v -> assertArrayEquals(value(value), v));
        i++;
      }
      assertEquals(count, i);
    } catch (IOException e) {
      fail("Failed due to exception");
    }
  }
}
