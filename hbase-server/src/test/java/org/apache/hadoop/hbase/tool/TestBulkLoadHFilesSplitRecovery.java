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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Test cases for the atomic load error handling of the bulk load functionality.
 */
@Category({ MiscTests.class, LargeTests.class })
public class TestBulkLoadHFilesSplitRecovery {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadHFilesSplitRecovery.class);

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
    Path dir = buildBulkFiles(table, value);
    BulkLoadHFiles.create(util.getConfiguration()).bulkLoad(table, dir);
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

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

  private static AsyncClusterConnection mockAndInjectError(AsyncClusterConnection conn) {
    AsyncClusterConnection errConn = spy(conn);
    doReturn(failedFuture(new IOException("injecting bulk load error"))).when(errConn)
      .bulkLoad(any(), anyList(), any(), anyBoolean(), any(), any(), anyBoolean());
    return errConn;
  }

  /**
   * Test that shows that exception thrown from the RS side will result in an exception on the
   * LIHFile client.
   */
  @Test(expected = IOException.class)
  public void testBulkLoadPhaseFailure() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    final AtomicInteger attemptedCalls = new AtomicInteger();
    Configuration conf = new Configuration(util.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(conf) {

      @Override
      protected void bulkLoadPhase(AsyncClusterConnection conn, TableName tableName,
          Deque<LoadQueueItem> queue, Multimap<ByteBuffer, LoadQueueItem> regionGroups,
          boolean copyFiles, Map<LoadQueueItem, ByteBuffer> item2RegionMap) throws IOException {
        AsyncClusterConnection c =
          attemptedCalls.incrementAndGet() == 1 ? mockAndInjectError(conn) : conn;
        super.bulkLoadPhase(c, tableName, queue, regionGroups, copyFiles, item2RegionMap);
      }
    };
    Path dir = buildBulkFiles(table, 1);
    loader.bulkLoad(table, dir);
  }

  /**
   * Test that shows that exception thrown from the RS side will result in the expected number of
   * retries set by ${@link HConstants#HBASE_CLIENT_RETRIES_NUMBER} when
   * ${@link BulkLoadHFiles#RETRY_ON_IO_EXCEPTION} is set
   */
  @Test
  public void testRetryOnIOException() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    AtomicInteger calls = new AtomicInteger(0);
    setupTable(util.getConnection(), table, 10);
    Configuration conf = new Configuration(util.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    conf.setBoolean(BulkLoadHFiles.RETRY_ON_IO_EXCEPTION, true);
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(conf) {

      @Override
      protected void bulkLoadPhase(AsyncClusterConnection conn, TableName tableName,
          Deque<LoadQueueItem> queue, Multimap<ByteBuffer, LoadQueueItem> regionGroups,
          boolean copyFiles, Map<LoadQueueItem, ByteBuffer> item2RegionMap) throws IOException {
        if (calls.get() < conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER)) {
          calls.incrementAndGet();
          super.bulkLoadPhase(mockAndInjectError(conn), tableName, queue, regionGroups, copyFiles,
            item2RegionMap);
        } else {
          super.bulkLoadPhase(conn, tableName, queue, regionGroups, copyFiles, item2RegionMap);
        }
      }
    };
    Path dir = buildBulkFiles(table, 1);
    loader.bulkLoad(table, dir);
    assertEquals(calls.get(), 2);
  }

  /**
   * This test exercises the path where there is a split after initial validation but before the
   * atomic bulk load call. We cannot use presplitting to test this path, so we actually inject a
   * split just before the atomic region load.
   */
  @Test
  public void testSplitWhileBulkLoadPhase() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    setupTable(util.getConnection(), table, 10);
    populateTable(util.getConnection(), table, 1);
    assertExpectedTable(table, ROWCOUNT, 1);

    // Now let's cause trouble. This will occur after checks and cause bulk
    // files to fail when attempt to atomically import. This is recoverable.
    final AtomicInteger attemptedCalls = new AtomicInteger();
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(util.getConfiguration()) {

      @Override
      protected void bulkLoadPhase(AsyncClusterConnection conn, TableName tableName,
          Deque<LoadQueueItem> queue, Multimap<ByteBuffer, LoadQueueItem> regionGroups,
          boolean copyFiles, Map<LoadQueueItem, ByteBuffer> item2RegionMap) throws IOException {
        int i = attemptedCalls.incrementAndGet();
        if (i == 1) {
          // On first attempt force a split.
          forceSplit(table);
        }
        super.bulkLoadPhase(conn, tableName, queue, regionGroups, copyFiles, item2RegionMap);
      }
    };

    // create HFiles for different column families
    Path dir = buildBulkFiles(table, 2);
    loader.bulkLoad(table, dir);

    // check that data was loaded
    // The three expected attempts are 1) failure because need to split, 2)
    // load of split top 3) load of split bottom
    assertEquals(3, attemptedCalls.get());
    assertExpectedTable(table, ROWCOUNT, 2);
  }

  /**
   * This test splits a table and attempts to bulk load. The bulk import files should be split
   * before atomically importing.
   */
  @Test
  public void testGroupOrSplitPresplit() throws Exception {
    final TableName table = TableName.valueOf(name.getMethodName());
    setupTable(util.getConnection(), table, 10);
    populateTable(util.getConnection(), table, 1);
    assertExpectedTable(util.getConnection(), table, ROWCOUNT, 1);
    forceSplit(table);

    final AtomicInteger countedLqis = new AtomicInteger();
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(util.getConfiguration()) {

      @Override
      protected Pair<List<LoadQueueItem>, String> groupOrSplit(AsyncClusterConnection conn,
          TableName tableName, Multimap<ByteBuffer, LoadQueueItem> regionGroups, LoadQueueItem item,
          List<Pair<byte[], byte[]>> startEndKeys) throws IOException {
        Pair<List<LoadQueueItem>, String> lqis =
          super.groupOrSplit(conn, tableName, regionGroups, item, startEndKeys);
        if (lqis != null && lqis.getFirst() != null) {
          countedLqis.addAndGet(lqis.getFirst().size());
        }
        return lqis;
      }
    };

    // create HFiles for different column families
    Path dir = buildBulkFiles(table, 2);
    loader.bulkLoad(table, dir);
    assertExpectedTable(util.getConnection(), table, ROWCOUNT, 2);
    assertEquals(20, countedLqis.get());
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
    setupTableWithSplitkeys(table, 10, SPLIT_KEYS);

    BulkLoadHFiles loader = BulkLoadHFiles.create(util.getConfiguration());

    // create HFiles
    Path dir = buildBulkFiles(table, 2);
    loader.bulkLoad(table, dir);
    // family path
    Path tmpPath = new Path(dir, family(0));
    // TMP_DIR under family path
    tmpPath = new Path(tmpPath, BulkLoadHFilesTool.TMP_DIR);
    FileSystem fs = dir.getFileSystem(util.getConfiguration());
    // HFiles have been splitted, there is TMP_DIR
    assertTrue(fs.exists(tmpPath));
    // TMP_DIR should have been cleaned-up
    assertNull(BulkLoadHFilesTool.TMP_DIR + " should be empty.", FSUtils.listStatus(fs, tmpPath));
    assertExpectedTable(util.getConnection(), table, ROWCOUNT, 2);
  }

  /**
   * This simulates an remote exception which should cause LIHF to exit with an exception.
   */
  @Test(expected = IOException.class)
  public void testGroupOrSplitFailure() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    setupTable(util.getConnection(), tableName, 10);
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(util.getConfiguration()) {

      private int i = 0;

      @Override
      protected Pair<List<LoadQueueItem>, String> groupOrSplit(AsyncClusterConnection conn,
          TableName tableName, Multimap<ByteBuffer, LoadQueueItem> regionGroups, LoadQueueItem item,
          List<Pair<byte[], byte[]>> startEndKeys) throws IOException {
        i++;

        if (i == 5) {
          throw new IOException("failure");
        }
        return super.groupOrSplit(conn, tableName, regionGroups, item, startEndKeys);
      }
    };

    // create HFiles for different column families
    Path dir = buildBulkFiles(tableName, 1);
    loader.bulkLoad(tableName, dir);
  }

  /**
   * Checks that all columns have the expected value and that there is the expected number of rows.
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
