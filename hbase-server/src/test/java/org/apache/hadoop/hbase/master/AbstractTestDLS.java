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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Base class for testing distributed log splitting.
 */
public abstract class AbstractTestDLS {
  private static final Logger LOG = LoggerFactory.getLogger(TestSplitLogManager.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // Start a cluster with 2 masters and 5 regionservers
  private static final int NUM_MASTERS = 2;
  private static final int NUM_RS = 5;
  private static byte[] COLUMN_FAMILY = Bytes.toBytes("family");

  @Rule
  public TestName testName = new TestName();

  private TableName tableName;
  private MiniHBaseCluster cluster;
  private HMaster master;
  private Configuration conf;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setup() throws Exception {
    // Uncomment the following line if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    // Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  protected abstract String getWalProvider();

  private void startCluster(int numRS) throws Exception {
    SplitLogCounters.resetCounters();
    LOG.info("Starting cluster");
    conf.setLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 100.0); // no load balancing
    conf.setInt(HBASE_SPLIT_WAL_MAX_SPLITTER, 3);
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    conf.set("hbase.wal.provider", getWalProvider());
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(NUM_MASTERS).numRegionServers(numRS).build();
    TEST_UTIL.startMiniHBaseCluster(option);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getLiveRegionServerThreads().size() >= numRS;
      }
    });
  }

  @Before
  public void before() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    tableName = TableName.valueOf(testName.getMethodName());
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.getTestFileSystem().delete(CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration()),
      true);
    ZKUtil.deleteNodeRecursively(TEST_UTIL.getZooKeeperWatcher(), "/hbase");
  }

  @Test
  public void testMasterStartsUpWithLogSplittingWork() throws Exception {
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_RS - 1);
    startCluster(NUM_RS);

    int numRegionsToCreate = 40;
    int numLogLines = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    try (Table ht = installTable(numRegionsToCreate)) {
      HRegionServer hrs = findRSToKill(false);
      List<RegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      makeWAL(hrs, regions, numLogLines, 100);

      // abort master
      abortMaster(cluster);

      // abort RS
      LOG.info("Aborting region server: " + hrs.getServerName());
      hrs.abort("testing");

      // wait for abort completes
      TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return cluster.getLiveRegionServerThreads().size() <= NUM_RS - 1;
        }
      });

      Thread.sleep(2000);
      LOG.info("Current Open Regions:" + HBaseTestingUtility.getAllOnlineRegions(cluster).size());

      // wait for abort completes
      TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return (HBaseTestingUtility.getAllOnlineRegions(cluster)
              .size() >= (numRegionsToCreate + 1));
        }
      });

      LOG.info("Current Open Regions After Master Node Starts Up:" +
          HBaseTestingUtility.getAllOnlineRegions(cluster).size());

      assertEquals(numLogLines, TEST_UTIL.countRows(ht));
    }
  }

  @Test
  public void testThreeRSAbort() throws Exception {
    LOG.info("testThreeRSAbort");
    int numRegionsToCreate = 40;
    int numRowsPerRegion = 100;

    startCluster(NUM_RS); // NUM_RS=6.

    try (Table table = installTable(numRegionsToCreate)) {
      populateDataInTable(numRowsPerRegion);

      List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
      assertEquals(NUM_RS, rsts.size());
      cluster.killRegionServer(rsts.get(0).getRegionServer().getServerName());
      cluster.killRegionServer(rsts.get(1).getRegionServer().getServerName());
      cluster.killRegionServer(rsts.get(2).getRegionServer().getServerName());

      TEST_UTIL.waitFor(60000, new Waiter.ExplainingPredicate<Exception>() {

        @Override
        public boolean evaluate() throws Exception {
          return cluster.getLiveRegionServerThreads().size() <= NUM_RS - 3;
        }

        @Override
        public String explainFailure() throws Exception {
          return "Timed out waiting for server aborts.";
        }
      });
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      int rows;
      try {
        rows = TEST_UTIL.countRows(table);
      } catch (Exception e) {
        Threads.printThreadInfo(System.out, "Thread dump before fail");
        throw e;
      }
      assertEquals(numRegionsToCreate * numRowsPerRegion, rows);
    }
  }

  private Table installTable(int nrs) throws Exception {
    return installTable(nrs, 0);
  }

  private Table installTable(int nrs, int existingRegions) throws Exception {
    // Create a table with regions
    byte[] family = Bytes.toBytes("family");
    LOG.info("Creating table with " + nrs + " regions");
    Table table = TEST_UTIL.createMultiRegionTable(tableName, family, nrs);
    int numRegions = -1;
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      numRegions = r.getStartKeys().length;
    }
    assertEquals(nrs, numRegions);
    LOG.info("Waiting for no more RIT\n");
    blockUntilNoRIT();
    // disable-enable cycle to get rid of table's dead regions left behind
    // by createMultiRegions
    assertTrue(TEST_UTIL.getAdmin().isTableEnabled(tableName));
    LOG.debug("Disabling table\n");
    TEST_UTIL.getAdmin().disableTable(tableName);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT();
    NavigableSet<String> regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    LOG.debug("Verifying only catalog and namespace regions are assigned\n");
    if (regions.size() != 2) {
      for (String oregion : regions) {
        LOG.debug("Region still online: " + oregion);
      }
    }
    assertEquals(2 + existingRegions, regions.size());
    LOG.debug("Enabling table\n");
    TEST_UTIL.getAdmin().enableTable(tableName);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT();
    LOG.debug("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    assertEquals(numRegions + 2 + existingRegions, regions.size());
    return table;
  }

  void populateDataInTable(int nrows) throws Exception {
    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());

    for (RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      List<RegionInfo> hris = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (RegionInfo hri : hris) {
        if (hri.getTable().isSystemTable()) {
          continue;
        }
        LOG.debug(
          "adding data to rs = " + rst.getName() + " region = " + hri.getRegionNameAsString());
        Region region = hrs.getOnlineRegion(hri.getRegionName());
        assertTrue(region != null);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), COLUMN_FAMILY);
      }
    }

    for (MasterThread mt : cluster.getLiveMasterThreads()) {
      HRegionServer hrs = mt.getMaster();
      List<RegionInfo> hris;
      try {
        hris = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      } catch (ServerNotRunningYetException e) {
        // It's ok: this master may be a backup. Ignored.
        continue;
      }
      for (RegionInfo hri : hris) {
        if (hri.getTable().isSystemTable()) {
          continue;
        }
        LOG.debug(
          "adding data to rs = " + mt.getName() + " region = " + hri.getRegionNameAsString());
        Region region = hrs.getOnlineRegion(hri.getRegionName());
        assertTrue(region != null);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), COLUMN_FAMILY);
      }
    }
  }

  public void makeWAL(HRegionServer hrs, List<RegionInfo> regions, int num_edits, int edit_size)
      throws IOException {
    makeWAL(hrs, regions, num_edits, edit_size, true);
  }

  public void makeWAL(HRegionServer hrs, List<RegionInfo> regions, int numEdits, int editSize,
      boolean cleanShutdown) throws IOException {
    // remove root and meta region
    regions.remove(RegionInfoBuilder.FIRST_META_REGIONINFO);

    for (Iterator<RegionInfo> iter = regions.iterator(); iter.hasNext();) {
      RegionInfo regionInfo = iter.next();
      if (regionInfo.getTable().isSystemTable()) {
        iter.remove();
      }
    }
    byte[] value = new byte[editSize];

    List<RegionInfo> hris = new ArrayList<>();
    for (RegionInfo region : regions) {
      if (region.getTable() != tableName) {
        continue;
      }
      hris.add(region);
    }
    LOG.info("Creating wal edits across " + hris.size() + " regions.");
    for (int i = 0; i < editSize; i++) {
      value[i] = (byte) ('a' + (i % 26));
    }
    int n = hris.size();
    int[] counts = new int[n];
    // sync every ~30k to line up with desired wal rolls
    final int syncEvery = 30 * 1024 / editSize;
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    if (n > 0) {
      for (int i = 0; i < numEdits; i += 1) {
        WALEdit e = new WALEdit();
        RegionInfo curRegionInfo = hris.get(i % n);
        WAL log = hrs.getWAL(curRegionInfo);
        byte[] startRow = curRegionInfo.getStartKey();
        if (startRow == null || startRow.length == 0) {
          startRow = new byte[] { 0, 0, 0, 0, 1 };
        }
        byte[] row = Bytes.incrementBytes(startRow, counts[i % n]);
        row = Arrays.copyOfRange(row, 3, 8); // use last 5 bytes because
        // HBaseTestingUtility.createMultiRegions use 5 bytes key
        byte[] qualifier = Bytes.toBytes("c" + Integer.toString(i));
        e.add(new KeyValue(row, COLUMN_FAMILY, qualifier, System.currentTimeMillis(), value));
        log.appendData(curRegionInfo, new WALKeyImpl(curRegionInfo.getEncodedNameAsBytes(),
          tableName, System.currentTimeMillis(), mvcc), e);
        if (0 == i % syncEvery) {
          log.sync();
        }
        counts[i % n] += 1;
      }
    }
    // done as two passes because the regions might share logs. shutdown is idempotent, but sync
    // will cause errors if done after.
    for (RegionInfo info : hris) {
      WAL log = hrs.getWAL(info);
      log.sync();
    }
    if (cleanShutdown) {
      for (RegionInfo info : hris) {
        WAL log = hrs.getWAL(info);
        log.shutdown();
      }
    }
    for (int i = 0; i < n; i++) {
      LOG.info("region " + hris.get(i).getRegionNameAsString() + " has " + counts[i] + " edits");
    }
    return;
  }

  private int countWAL(Path log, FileSystem fs, Configuration conf) throws IOException {
    int count = 0;
    try (WAL.Reader in = WALFactory.createReader(fs, log, conf)) {
      WAL.Entry e;
      while ((e = in.next()) != null) {
        if (!WALEdit.isMetaEditFamily(e.getEdit().getCells().get(0))) {
          count++;
        }
      }
    }
    return count;
  }

  private void blockUntilNoRIT() throws Exception {
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
  }

  private void putData(Region region, byte[] startRow, int numRows, byte[] qf, byte[]... families)
      throws IOException {
    for (int i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.add(startRow, Bytes.toBytes(i)));
      for (byte[] family : families) {
        put.addColumn(family, qf, null);
      }
      region.put(put);
    }
  }

  private void waitForCounter(LongAdder ctr, long oldval, long newval, long timems)
      throws InterruptedException {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.sum() == oldval) {
        Thread.sleep(100);
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.sum());
        return;
      }
    }
    fail();
  }

  private void abortMaster(MiniHBaseCluster cluster) throws InterruptedException {
    for (MasterThread mt : cluster.getLiveMasterThreads()) {
      if (mt.getMaster().isActiveMaster()) {
        mt.getMaster().abort("Aborting for tests", new Exception("Trace info"));
        mt.join();
        break;
      }
    }
    LOG.debug("Master is aborted");
  }

  /**
   * Find a RS that has regions of a table.
   * @param hasMetaRegion when true, the returned RS has hbase:meta region as well
   */
  private HRegionServer findRSToKill(boolean hasMetaRegion) throws Exception {
    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    List<RegionInfo> regions = null;
    HRegionServer hrs = null;

    for (RegionServerThread rst : rsts) {
      hrs = rst.getRegionServer();
      while (rst.isAlive() && !hrs.isOnline()) {
        Thread.sleep(100);
      }
      if (!rst.isAlive()) {
        continue;
      }
      boolean isCarryingMeta = false;
      boolean foundTableRegion = false;
      regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (RegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
        }
        if (region.getTable() == tableName) {
          foundTableRegion = true;
        }
        if (foundTableRegion && (isCarryingMeta || !hasMetaRegion)) {
          break;
        }
      }
      if (isCarryingMeta && hasMetaRegion) {
        // clients ask for a RS with META
        if (!foundTableRegion) {
          HRegionServer destRS = hrs;
          // the RS doesn't have regions of the specified table so we need move one to this RS
          List<RegionInfo> tableRegions = TEST_UTIL.getAdmin().getRegions(tableName);
          RegionInfo hri = tableRegions.get(0);
          TEST_UTIL.getAdmin().move(hri.getEncodedNameAsBytes(), destRS.getServerName());
          // wait for region move completes
          RegionStates regionStates =
              TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
          TEST_UTIL.waitFor(45000, 200, new Waiter.Predicate<Exception>() {
            @Override
            public boolean evaluate() throws Exception {
              ServerName sn = regionStates.getRegionServerOfRegion(hri);
              return (sn != null && sn.equals(destRS.getServerName()));
            }
          });
        }
        return hrs;
      } else if (hasMetaRegion || isCarryingMeta) {
        continue;
      }
      if (foundTableRegion) {
        break;
      }
    }

    return hrs;
  }
}
