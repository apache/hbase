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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Multimap;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test cases for the atomic load error handling of the bulk load functionality.
 */
@Category({MapReduceTests.class, LargeTests.class})
public class TestLoadIncrementalHFilesSplitRecovery {
  private static final Log LOG = LogFactory.getLog(TestHRegionServerBulkLoad.class);

  static HBaseTestingUtility util;
  //used by secure subclass
  static boolean useSecure = false;

  final static int NUM_CFS = 10;
  final static byte[] QUAL = Bytes.toBytes("qual");
  final static int ROWCOUNT = 100;

  private final static byte[][] families = new byte[NUM_CFS][];
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

  public static void buildHFiles(FileSystem fs, Path dir, int value)
      throws IOException {
    byte[] val = value(value);
    for (int i = 0; i < NUM_CFS; i++) {
      Path testIn = new Path(dir, family(i));

      TestHRegionServerBulkLoad.createHFile(fs, new Path(testIn, "hfile_" + i),
          Bytes.toBytes(family(i)), QUAL, val, ROWCOUNT);
    }
  }

  /**
   * Creates a table with given table name and specified number of column
   * families if the table does not already exist.
   */
  private void setupTable(final Connection connection, TableName table, int cfs)
  throws IOException {
    try {
      LOG.info("Creating table " + table);
      HTableDescriptor htd = new HTableDescriptor(table);
      for (int i = 0; i < cfs; i++) {
        htd.addFamily(new HColumnDescriptor(family(i)));
      }
      try (Admin admin = connection.getAdmin()) {
        admin.createTable(htd);
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
      HTableDescriptor htd = new HTableDescriptor(table);
      for (int i = 0; i < cfs; i++) {
        htd.addFamily(new HColumnDescriptor(family(i)));
      }

      util.createTable(htd, SPLIT_KEYS);
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
   * Split the known table in half.  (this is hard coded for this test suite)
   */
  private void forceSplit(TableName table) {
    try {
      // need to call regions server to by synchronous but isn't visible.
      HRegionServer hrs = util.getRSForFirstRegionInTable(table);

      for (HRegionInfo hri :
          ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices())) {
        if (hri.getTable().equals(table)) {
          // splitRegion doesn't work if startkey/endkey are null
          ProtobufUtil.split(null, hrs.getRSRpcServices(), hri, rowkey(ROWCOUNT / 2));
        }
      }

      // verify that split completed.
      int regions;
      do {
        regions = 0;
        for (HRegionInfo hri :
            ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices())) {
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
   * Checks that all columns have the expected value and that there is the
   * expected number of rows.
   * @throws IOException
   */
  void assertExpectedTable(TableName table, int count, int value) throws IOException {
    HTableDescriptor [] htds = util.getHBaseAdmin().listTables(table.getNameAsString());
    assertEquals(htds.length, 1);
    Table t = null;
    try {
      t = util.getConnection().getTable(table);
      Scan s = new Scan();
      ResultScanner sr = t.getScanner(s);
      int i = 0;
      for (Result r : sr) {
        i++;
        for (NavigableMap<byte[], byte[]> nm : r.getNoVersionMap().values()) {
          for (byte[] val : nm.values()) {
            assertTrue(Bytes.equals(val, value(value)));
          }
        }
      }
      assertEquals(count, i);
    } catch (IOException e) {
      fail("Failed due to exception");
    } finally {
      if (t != null) t.close();
    }
  }

  /**
   * Test that shows that exception thrown from the RS side will result in an
   * exception on the LIHFile client.
   */
  @Test(expected=IOException.class, timeout=120000)
  public void testBulkLoadPhaseFailure() throws Exception {
    TableName table = TableName.valueOf("bulkLoadPhaseFailure");
    final AtomicInteger attmptedCalls = new AtomicInteger();
    final AtomicInteger failedCalls = new AtomicInteger();
    util.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection connection = ConnectionFactory.createConnection(this.util.getConfiguration())) {
      setupTable(connection, table, 10);
      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration()) {
        @Override
        protected List<LoadQueueItem> tryAtomicRegionLoad(final Connection conn,
            TableName tableName, final byte[] first, Collection<LoadQueueItem> lqis)
                throws IOException {
          int i = attmptedCalls.incrementAndGet();
          if (i == 1) {
            Connection errConn = null;
            try {
              errConn = getMockedConnection(util.getConfiguration());
            } catch (Exception e) {
              LOG.fatal("mocking cruft, should never happen", e);
              throw new RuntimeException("mocking cruft, should never happen");
            }
            failedCalls.incrementAndGet();
            return super.tryAtomicRegionLoad((HConnection)errConn, tableName, first, lqis);
          }

          return super.tryAtomicRegionLoad((HConnection)conn, tableName, first, lqis);
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

  @SuppressWarnings("deprecation")
  private HConnection getMockedConnection(final Configuration conf)
  throws IOException, ServiceException {
    HConnection c = Mockito.mock(HConnection.class);
    Mockito.when(c.getConfiguration()).thenReturn(conf);
    Mockito.doNothing().when(c).close();
    // Make it so we return a particular location when asked.
    final HRegionLocation loc = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO,
        ServerName.valueOf("example.org", 1234, 0));
    Mockito.when(c.getRegionLocation((TableName) Mockito.any(),
        (byte[]) Mockito.any(), Mockito.anyBoolean())).
      thenReturn(loc);
    Mockito.when(c.locateRegion((TableName) Mockito.any(), (byte[]) Mockito.any())).
      thenReturn(loc);
    ClientProtos.ClientService.BlockingInterface hri =
      Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
    Mockito.when(hri.bulkLoadHFile((RpcController)Mockito.any(), (BulkLoadHFileRequest)Mockito.any())).
      thenThrow(new ServiceException(new IOException("injecting bulk load error")));
    Mockito.when(c.getClient(Mockito.any(ServerName.class))).
      thenReturn(hri);
    return c;
  }

  /**
   * This test exercises the path where there is a split after initial
   * validation but before the atomic bulk load call. We cannot use presplitting
   * to test this path, so we actually inject a split just before the atomic
   * region load.
   */
  @Test (timeout=120000)
  public void testSplitWhileBulkLoadPhase() throws Exception {
    final TableName table = TableName.valueOf("splitWhileBulkloadPhase");
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTable(connection, table, 10);
      populateTable(connection, table,1);
      assertExpectedTable(table, ROWCOUNT, 1);

      // Now let's cause trouble.  This will occur after checks and cause bulk
      // files to fail when attempt to atomically import.  This is recoverable.
      final AtomicInteger attemptedCalls = new AtomicInteger();
      LoadIncrementalHFiles lih2 = new LoadIncrementalHFiles(util.getConfiguration()) {
        @Override
        protected void bulkLoadPhase(final Table htable, final Connection conn,
            ExecutorService pool, Deque<LoadQueueItem> queue,
            final Multimap<ByteBuffer, LoadQueueItem> regionGroups) throws IOException {
          int i = attemptedCalls.incrementAndGet();
          if (i == 1) {
            // On first attempt force a split.
            forceSplit(table);
          }
          super.bulkLoadPhase(htable, conn, pool, queue, regionGroups);
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
      assertEquals(attemptedCalls.get(), 3);
      assertExpectedTable(table, ROWCOUNT, 2);
    }
  }

  /**
   * This test splits a table and attempts to bulk load.  The bulk import files
   * should be split before atomically importing.
   */
  @Test (timeout=120000)
  public void testGroupOrSplitPresplit() throws Exception {
    final TableName table = TableName.valueOf("groupOrSplitPresplit");
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTable(connection, table, 10);
      populateTable(connection, table, 1);
      assertExpectedTable(connection, table, ROWCOUNT, 1);
      forceSplit(table);

      final AtomicInteger countedLqis= new AtomicInteger();
      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(
          util.getConfiguration()) {
        @Override
        protected List<LoadQueueItem> groupOrSplit(
            Multimap<ByteBuffer, LoadQueueItem> regionGroups,
            final LoadQueueItem item, final Table htable,
            final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
          List<LoadQueueItem> lqis = super.groupOrSplit(regionGroups, item, htable, startEndKeys);
          if (lqis != null) {
            countedLqis.addAndGet(lqis.size());
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

  /**
   * This simulates an remote exception which should cause LIHF to exit with an
   * exception.
   */
  @Test(expected = IOException.class, timeout=120000)
  public void testGroupOrSplitFailure() throws Exception {
    TableName table = TableName.valueOf("groupOrSplitFailure");
    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
      setupTable(connection, table, 10);

      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(
          util.getConfiguration()) {
        int i = 0;

        @Override
        protected List<LoadQueueItem> groupOrSplit(
            Multimap<ByteBuffer, LoadQueueItem> regionGroups,
            final LoadQueueItem item, final Table table,
            final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
          i++;

          if (i == 5) {
            throw new IOException("failure");
          }
          return super.groupOrSplit(regionGroups, item, table, startEndKeys);
        }
      };

      // create HFiles for different column families
      Path dir = buildBulkFiles(table,1);
      try (Table t = connection.getTable(table);
          RegionLocator locator = connection.getRegionLocator(table);
          Admin admin = connection.getAdmin()) {
        lih.doBulkLoad(dir, admin, t, locator);
      }
    }

    fail("doBulkLoad should have thrown an exception");
  }

  @Test (timeout=120000)
  public void testGroupOrSplitWhenRegionHoleExistsInMeta() throws Exception {
    TableName tableName = TableName.valueOf("testGroupOrSplitWhenRegionHoleExistsInMeta");
    byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("row_00000100") };
    // Share connection. We were failing to find the table with our new reverse scan because it
    // looks for first region, not any region -- that is how it works now.  The below removes first
    // region in test.  Was reliant on the Connection caching having first region.
    Connection connection = ConnectionFactory.createConnection(util.getConfiguration());
    Table table = connection.getTable(tableName);

    setupTableWithSplitkeys(tableName, 10, SPLIT_KEYS);
    Path dir = buildBulkFiles(tableName, 2);

    final AtomicInteger countedLqis = new AtomicInteger();
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration()) {

      @Override
      protected List<LoadQueueItem> groupOrSplit(
          Multimap<ByteBuffer, LoadQueueItem> regionGroups,
          final LoadQueueItem item, final Table htable,
          final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
        List<LoadQueueItem> lqis = super.groupOrSplit(regionGroups, item, htable, startEndKeys);
        if (lqis != null) {
          countedLqis.addAndGet(lqis.size());
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
    List<HRegionInfo> regionInfos = MetaTableAccessor.getTableRegions(connection, tableName);
    for (HRegionInfo regionInfo : regionInfos) {
      if (Bytes.equals(regionInfo.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
        MetaTableAccessor.deleteRegion(connection, regionInfo);
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
   * Checks that all columns have the expected value and that there is the
   * expected number of rows.
   * @throws IOException
   */
  void assertExpectedTable(final Connection connection, TableName table, int count, int value)
  throws IOException {
    HTableDescriptor [] htds = util.getHBaseAdmin().listTables(table.getNameAsString());
    assertEquals(htds.length, 1);
    Table t = null;
    try {
      t = connection.getTable(table);
      Scan s = new Scan();
      ResultScanner sr = t.getScanner(s);
      int i = 0;
      for (Result r : sr) {
        i++;
        for (NavigableMap<byte[], byte[]> nm : r.getNoVersionMap().values()) {
          for (byte[] val : nm.values()) {
            assertTrue(Bytes.equals(val, value(value)));
          }
        }
      }
      assertEquals(count, i);
    } catch (IOException e) {
      fail("Failed due to exception");
    } finally {
      if (t != null) t.close();
    }
  }
}
