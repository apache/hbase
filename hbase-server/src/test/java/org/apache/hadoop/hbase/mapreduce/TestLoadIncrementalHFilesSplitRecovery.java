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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
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
@Category(LargeTests.class)
public class TestLoadIncrementalHFilesSplitRecovery {
  final static Log LOG = LogFactory.getLog(TestHRegionServerBulkLoad.class);

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
  private void setupTable(String table, int cfs) throws IOException {
    try {
      LOG.info("Creating table " + table);
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
      for (int i = 0; i < cfs; i++) {
        htd.addFamily(new HColumnDescriptor(family(i)));
      }

      util.getHBaseAdmin().createTable(htd);
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
  private void setupTableWithSplitkeys(String table, int cfs, byte[][] SPLIT_KEYS)
      throws IOException {
    try {
      LOG.info("Creating table " + table);
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
      for (int i = 0; i < cfs; i++) {
        htd.addFamily(new HColumnDescriptor(family(i)));
      }

      util.getHBaseAdmin().createTable(htd, SPLIT_KEYS);
    } catch (TableExistsException tee) {
      LOG.info("Table " + table + " already exists");
    }
  }

  private Path buildBulkFiles(String table, int value) throws Exception {
    Path dir = util.getDataTestDirOnTestFS(table);
    Path bulk1 = new Path(dir, table+value);
    FileSystem fs = util.getTestFileSystem();
    buildHFiles(fs, bulk1, value);
    return bulk1;
  }

  /**
   * Populate table with known values.
   */
  private void populateTable(String table, int value) throws Exception {
    // create HFiles for different column families
    LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration());
    Path bulk1 = buildBulkFiles(table, value);
    HTable t = new HTable(util.getConfiguration(), Bytes.toBytes(table));
    lih.doBulkLoad(bulk1, t);
  }

  /**
   * Split the known table in half.  (this is hard coded for this test suite)
   */
  private void forceSplit(String table) {
    try {
      // need to call regions server to by synchronous but isn't visible.
      HRegionServer hrs = util.getRSForFirstRegionInTable(Bytes
          .toBytes(table));

      for (HRegionInfo hri : ProtobufUtil.getOnlineRegions(hrs)) {
        if (Bytes.equals(hri.getTable().getName(), Bytes.toBytes(table))) {
          // splitRegion doesn't work if startkey/endkey are null
          ProtobufUtil.split(hrs, hri, rowkey(ROWCOUNT / 2)); // hard code split
        }
      }

      // verify that split completed.
      int regions;
      do {
        regions = 0;
        for (HRegionInfo hri : ProtobufUtil.getOnlineRegions(hrs)) {
          if (Bytes.equals(hri.getTable().getName(), Bytes.toBytes(table))) {
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
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,"");
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
  void assertExpectedTable(String table, int count, int value) throws IOException {
    HTable t = null;
    try {
      assertEquals(util.getHBaseAdmin().listTables(table).length, 1);
      t = new HTable(util.getConfiguration(), table);
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
  @Test(expected=IOException.class)
  public void testBulkLoadPhaseFailure() throws Exception {
    String table = "bulkLoadPhaseFailure";
    setupTable(table, 10);

    final AtomicInteger attmptedCalls = new AtomicInteger();
    final AtomicInteger failedCalls = new AtomicInteger();
    util.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try {
      LoadIncrementalHFiles lih = new LoadIncrementalHFiles(util.getConfiguration()) {

        protected List<LoadQueueItem> tryAtomicRegionLoad(final HConnection conn,
            TableName tableName, final byte[] first, Collection<LoadQueueItem> lqis)
            throws IOException {
          int i = attmptedCalls.incrementAndGet();
          if (i == 1) {
            HConnection errConn = null;
            try {
              errConn = getMockedConnection(util.getConfiguration());
            } catch (Exception e) {
              LOG.fatal("mocking cruft, should never happen", e);
              throw new RuntimeException("mocking cruft, should never happen");
            }
            failedCalls.incrementAndGet();
            return super.tryAtomicRegionLoad(errConn, tableName, first, lqis);
          }

          return super.tryAtomicRegionLoad(conn, tableName, first, lqis);
        }
      };

      // create HFiles for different column families
      Path dir = buildBulkFiles(table, 1);
      HTable t = new HTable(util.getConfiguration(), Bytes.toBytes(table));
      lih.doBulkLoad(dir, t);
    } finally {
      util.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    }

    fail("doBulkLoad should have thrown an exception");
  }

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
  @Test
  public void testSplitWhileBulkLoadPhase() throws Exception {
    final String table = "splitWhileBulkloadPhase";
    setupTable(table, 10);
    populateTable(table,1);
    assertExpectedTable(table, ROWCOUNT, 1);

    // Now let's cause trouble.  This will occur after checks and cause bulk
    // files to fail when attempt to atomically import.  This is recoverable.
    final AtomicInteger attemptedCalls = new AtomicInteger();
    LoadIncrementalHFiles lih2 = new LoadIncrementalHFiles(
        util.getConfiguration()) {

      protected void bulkLoadPhase(final HTable htable, final HConnection conn,
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
    HTable t = new HTable(util.getConfiguration(), Bytes.toBytes(table));
    Path bulk = buildBulkFiles(table, 2);
    lih2.doBulkLoad(bulk, t);

    // check that data was loaded
    // The three expected attempts are 1) failure because need to split, 2)
    // load of split top 3) load of split bottom
    assertEquals(attemptedCalls.get(), 3);
    assertExpectedTable(table, ROWCOUNT, 2);
  }

  /**
   * This test splits a table and attempts to bulk load.  The bulk import files
   * should be split before atomically importing.
   */
  @Test
  public void testGroupOrSplitPresplit() throws Exception {
    final String table = "groupOrSplitPresplit";
    setupTable(table, 10);
    populateTable(table, 1);
    assertExpectedTable(table, ROWCOUNT, 1);
    forceSplit(table);

    final AtomicInteger countedLqis= new AtomicInteger();
    LoadIncrementalHFiles lih = new LoadIncrementalHFiles(
        util.getConfiguration()) {
      protected List<LoadQueueItem> groupOrSplit(
          Multimap<ByteBuffer, LoadQueueItem> regionGroups,
          final LoadQueueItem item, final HTable htable,
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
    HTable ht = new HTable(util.getConfiguration(), Bytes.toBytes(table));
    lih.doBulkLoad(bulk, ht);

    assertExpectedTable(table, ROWCOUNT, 2);
    assertEquals(20, countedLqis.get());
  }

  /**
   * This simulates an remote exception which should cause LIHF to exit with an
   * exception.
   */
  @Test(expected = IOException.class)
  public void testGroupOrSplitFailure() throws Exception {
    String table = "groupOrSplitFailure";
    setupTable(table, 10);

    LoadIncrementalHFiles lih = new LoadIncrementalHFiles(
        util.getConfiguration()) {
      int i = 0;

      protected List<LoadQueueItem> groupOrSplit(
          Multimap<ByteBuffer, LoadQueueItem> regionGroups,
          final LoadQueueItem item, final HTable table,
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
    HTable t = new HTable(util.getConfiguration(), Bytes.toBytes(table));
    lih.doBulkLoad(dir, t);

    fail("doBulkLoad should have thrown an exception");
  }

  @Test
  public void testGroupOrSplitWhenRegionHoleExistsInMeta() throws Exception {
    String tableName = "testGroupOrSplitWhenRegionHoleExistsInMeta";
    byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("row_00000100") };
    HTable table = new HTable(util.getConfiguration(), Bytes.toBytes(tableName));

    setupTableWithSplitkeys(tableName, 10, SPLIT_KEYS);
    Path dir = buildBulkFiles(tableName, 2);

    final AtomicInteger countedLqis = new AtomicInteger();
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(util.getConfiguration()) {

      protected List<LoadQueueItem>
          groupOrSplit(Multimap<ByteBuffer, LoadQueueItem> regionGroups, final LoadQueueItem item,
              final HTable htable, final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
        List<LoadQueueItem> lqis = super.groupOrSplit(regionGroups, item, htable, startEndKeys);
        if (lqis != null) {
          countedLqis.addAndGet(lqis.size());
        }
        return lqis;
      }
    };

    // do bulkload when there is no region hole in hbase:meta.
    try {
      loader.doBulkLoad(dir, table);
    } catch (Exception e) {
      LOG.error("exeception=", e);
    }
    // check if all the data are loaded into the table.
    this.assertExpectedTable(tableName, ROWCOUNT, 2);

    dir = buildBulkFiles(tableName, 3);

    // Mess it up by leaving a hole in the hbase:meta
    CatalogTracker ct = new CatalogTracker(util.getConfiguration());
    List<HRegionInfo> regionInfos = MetaReader.getTableRegions(ct, TableName.valueOf(tableName));
    for (HRegionInfo regionInfo : regionInfos) {
      if (Bytes.equals(regionInfo.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
        MetaEditor.deleteRegion(ct, regionInfo);
        break;
      }
    }

    try {
      loader.doBulkLoad(dir, table);
    } catch (Exception e) {
      LOG.error("exeception=", e);
      assertTrue("IOException expected", e instanceof IOException);
    }

    table.close();

    this.assertExpectedTable(tableName, ROWCOUNT, 2);
  }
}

