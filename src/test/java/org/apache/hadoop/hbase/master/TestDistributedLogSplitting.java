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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_wait_for_zk_delete;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_final_transistion_failed;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_preempt_task;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_acquired;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_done;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_err;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_resigned;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestDistributedLogSplitting {
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  // Start a cluster with 2 masters and 3 regionservers
  final int NUM_MASTERS = 2;
  final int NUM_RS = 6;

  MiniHBaseCluster cluster;
  HMaster master;
  Configuration conf;
  static HBaseTestingUtility TEST_UTIL;
  static Configuration originalConf;
  static MiniDFSCluster dfsCluster;
  static MiniZooKeeperCluster zkCluster;

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtility(HBaseConfiguration.create());
    dfsCluster = TEST_UTIL.startMiniDFSCluster(1);
    zkCluster = TEST_UTIL.startMiniZKCluster();
    originalConf = TEST_UTIL.getConfiguration();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
    TEST_UTIL.shutdownMiniHBaseCluster();
  }

  private void startCluster(int num_rs) throws Exception{
    conf = HBaseConfiguration.create();
    startCluster(NUM_MASTERS, num_rs, conf);
  }

  private void startCluster(int num_master, int num_rs, Configuration inConf) throws Exception {
    ZKSplitLog.Counters.resetCounters();
    LOG.info("Starting cluster");
    this.conf = inConf;
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.setDFSCluster(dfsCluster);
    TEST_UTIL.setZkCluster(zkCluster);
    TEST_UTIL.startMiniHBaseCluster(num_master, num_rs);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
    while (cluster.getLiveRegionServerThreads().size() < num_rs) {
      Threads.sleep(1);
    }
  }

  @Before
  public void before() throws Exception {
    // refresh configuration
    conf = HBaseConfiguration.create(originalConf);
  }
  
  @After
  public void after() throws Exception {
    if (TEST_UTIL.getHBaseCluster() != null) {
      for (MasterThread mt : TEST_UTIL.getHBaseCluster().getLiveMasterThreads()) {
        mt.getMaster().abort("closing...", new Exception("Trace info"));
      }
    }
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.getTestFileSystem().delete(FSUtils.getRootDir(TEST_UTIL.getConfiguration()), true);
    ZKUtil.deleteNodeRecursively(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL), "/hbase");
  }

  @Test (timeout=300000)
  public void testThreeRSAbort() throws Exception {
    LOG.info("testThreeRSAbort");
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_ROWS_PER_REGION = 100;

    startCluster(NUM_RS); // NUM_RS=6.

    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
        "distributed log splitting test", null);

    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);
    populateDataInTable(NUM_ROWS_PER_REGION, "family");


    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());
    rsts.get(0).getRegionServer().abort("testing");
    rsts.get(1).getRegionServer().abort("testing");
    rsts.get(2).getRegionServer().abort("testing");

    long start = EnvironmentEdgeManager.currentTimeMillis();
    while (cluster.getLiveRegionServerThreads().size() > (NUM_RS - 3)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    start = EnvironmentEdgeManager.currentTimeMillis();
    while (getAllOnlineRegions(cluster).size() < (NUM_REGIONS_TO_CREATE + 2)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    assertEquals(NUM_REGIONS_TO_CREATE * NUM_ROWS_PER_REGION,
        TEST_UTIL.countRows(ht));
    ht.close();
  }

  @Test (timeout=300000)
  public void testRecoveredEdits() throws Exception {
    LOG.info("testRecoveredEdits");
    startCluster(NUM_RS);
    final int NUM_LOG_LINES = 1000;
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    
    Path rootdir = FSUtils.getRootDir(conf);

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);
    byte[] table = Bytes.toBytes("table");
    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean foundRS = false;
      hrs = rsts.get(i).getRegionServer();
      regions = hrs.getOnlineRegions();
      for (HRegionInfo region : regions) {
        if (region.getTableNameAsString().equalsIgnoreCase("table")) {
          foundRS = true;
          break;
        }
      }
      if (foundRS) break;
    }
    final Path logDir = new Path(rootdir, HLog.getHLogDirectoryName(hrs
        .getServerName().toString()));
    
    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table",
        NUM_LOG_LINES, 100);

    slm.splitLogDistributed(logDir);

    int count = 0;
    for (HRegionInfo hri : regions) {

      Path tdir = HTableDescriptor.getTableDir(rootdir, table);
      Path editsdir =
        HLog.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
        hri.getEncodedName()));
      LOG.debug("checking edits dir " + editsdir);
      FileStatus[] files = fs.listStatus(editsdir);
      assertEquals(1, files.length);
      int c = countHLog(files[0].getPath(), fs, conf);
      count += c;
      LOG.info(c + " edits in " + files[0].getPath());
    }
    assertEquals(NUM_LOG_LINES, count);
  }

  @Test(timeout = 300000)
  public void testMasterStartsUpWithLogSplittingWork() throws Exception {
    LOG.info("testMasterStartsUpWithLogSplittingWork");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_RS - 1);
    startCluster(2, NUM_RS, curConf);

    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "f", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = hrs.getOnlineRegions();
      for (HRegionInfo region : regions) {
        if (region.isRootRegion() || region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table", NUM_LOG_LINES, 100);

    // abort master
    abortMaster(cluster);

    // abort RS
    int numRS = cluster.getLiveRegionServerThreads().size();
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for the RS dies
    long start = EnvironmentEdgeManager.currentTimeMillis();
    while (cluster.getLiveRegionServerThreads().size() > (numRS - 1)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    Thread.sleep(2000);
    LOG.info("Current Open Regions:" + getAllOnlineRegions(cluster).size());
    
    startMasterTillNoDeadServers(cluster);
    
    start = EnvironmentEdgeManager.currentTimeMillis();
    while (getAllOnlineRegions(cluster).size() < (NUM_REGIONS_TO_CREATE + 2)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue("Timedout", false);
      }
      Thread.sleep(200);
    }

    LOG.info("Current Open Regions After Master Node Starts Up:"
        + getAllOnlineRegions(cluster).size());

    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));

    ht.close();
  }

  /**
   * The original intention of this test was to force an abort of a region
   * server and to make sure that the failure path in the region servers is
   * properly evaluated. But it is difficult to ensure that the region server
   * doesn't finish the log splitting before it aborts. Also now, there is
   * this code path where the master will preempt the region server when master
   * detects that the region server has aborted.
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testWorkerAbort() throws Exception {
    LOG.info("testWorkerAbort");
    startCluster(1);
    final int NUM_LOG_LINES = 10000;
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    final List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs = rsts.get(0).getRegionServer();
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        HLog.getHLogDirectoryName(hrs.getServerName().toString()));

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);
    makeHLog(hrs.getWAL(), hrs.getOnlineRegions(), "table",
        NUM_LOG_LINES, 100);

    new Thread() {
      public void run() {
        waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
        for (RegionServerThread rst : rsts) {
          rst.getRegionServer().abort("testing");
        }
      }
    }.start();
    // slm.splitLogDistributed(logDir);
    FileStatus[] logfiles = fs.listStatus(logDir);
    TaskBatch batch = new TaskBatch();
    slm.enqueueSplitTask(logfiles[0].getPath().toString(), batch);
    //waitForCounter but for one of the 2 counters
    long curt = System.currentTimeMillis();
    long waitTime = 80000;
    long endt = curt + waitTime;
    while (curt < endt) {
      if ((tot_wkr_task_resigned.get() + tot_wkr_task_err.get() + 
          tot_wkr_final_transistion_failed.get() + tot_wkr_task_done.get() +
          tot_wkr_preempt_task.get()) == 0) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(1, (tot_wkr_task_resigned.get() + tot_wkr_task_err.get() + 
            tot_wkr_final_transistion_failed.get() + tot_wkr_task_done.get() +
            tot_wkr_preempt_task.get()));
        return;
      }
    }
    fail("none of the following counters went up in " + waitTime + 
        " milliseconds - " +
        "tot_wkr_task_resigned, tot_wkr_task_err, " +
        "tot_wkr_final_transistion_failed, tot_wkr_task_done, " +
        "tot_wkr_preempt_task");
  }

  @Test(timeout=30000)
  public void testDelayedDeleteOnFailure() throws Exception {
    LOG.info("testDelayedDeleteOnFailure");
    startCluster(1);
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path logDir = new Path(FSUtils.getRootDir(conf), "x");
    fs.mkdirs(logDir);
    ExecutorService executor = null;
    try {
      final Path corruptedLogFile = new Path(logDir, "x");
      FSDataOutputStream out;
      out = fs.create(corruptedLogFile);
      out.write(0);
      out.write(Bytes.toBytes("corrupted bytes"));
      out.close();
      slm.ignoreZKDeleteForTesting = true;
      executor = Executors.newSingleThreadExecutor();
      Runnable runnable = new Runnable() {
       @Override
       public void run() {
          try {
            // since the logDir is a fake, corrupted one, so the split log worker
            // will finish it quickly with error, and this call will fail and throw
            // an IOException.
            slm.splitLogDistributed(logDir);
          } catch (IOException ioe) {
            try {
              assertTrue(fs.exists(corruptedLogFile));
              // this call will block waiting for the task to be removed from the
              // tasks map which is not going to happen since ignoreZKDeleteForTesting
              // is set to true, until it is interrupted.
              slm.splitLogDistributed(logDir);
            } catch (IOException e) {
              assertTrue(Thread.currentThread().isInterrupted());
              return;
            }
            fail("did not get the expected IOException from the 2nd call");
          }
          fail("did not get the expected IOException from the 1st call");
        }
      };
      Future<?> result = executor.submit(runnable);
      try {
        result.get(2000, TimeUnit.MILLISECONDS);
      } catch (TimeoutException te) {
        // it is ok, expected.
      }
      waitForCounter(tot_mgr_wait_for_zk_delete, 0, 1, 10000);
      executor.shutdownNow();
      executor = null;

      // make sure the runnable is finished with no exception thrown.
      result.get();
    } finally {
      if (executor != null) {
        // interrupt the thread in case the test fails in the middle.
        // it has no effect if the thread is already terminated.
        executor.shutdownNow();
      }
      fs.delete(logDir, true);
    }
  }

  HTable installTable(ZooKeeperWatcher zkw, String tname, String fname,
      int nrs ) throws Exception {
    // Create a table with regions
    byte [] table = Bytes.toBytes(tname);
    byte [] family = Bytes.toBytes(fname);
    LOG.info("Creating table with " + nrs + " regions");
    HTable ht = TEST_UTIL.createTable(table, family);
    int numRegions = TEST_UTIL.createMultiRegions(conf, ht, family, nrs);
    assertEquals(nrs, numRegions);
      LOG.info("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    // disable-enable cycle to get rid of table's dead regions left behind
    // by createMultiRegions
    LOG.debug("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    NavigableSet<String> regions = getAllOnlineRegions(cluster);
    LOG.debug("Verifying only catalog regions are assigned\n");
    if (regions.size() != 2) {
      for (String oregion : regions)
        LOG.debug("Region still online: " + oregion);
    }
    assertEquals(2, regions.size());
    LOG.debug("Enabling table\n");
    TEST_UTIL.getHBaseAdmin().enableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    LOG.debug("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = getAllOnlineRegions(cluster);
    assertEquals(numRegions + 2, regions.size());
    return ht;
  }

  void populateDataInTable(int nrows, String fname) throws Exception {
    byte [] family = Bytes.toBytes(fname);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());

    for (RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      List<HRegionInfo> hris = hrs.getOnlineRegions();
      for (HRegionInfo hri : hris) {
        if (hri.isMetaTable()) {
          continue;
        }
        LOG.debug("adding data to rs = " + rst.getName() +
            " region = "+ hri.getRegionNameAsString());
        HRegion region = hrs.getOnlineRegion(hri.getRegionName());
        assertTrue(region != null);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }
  }

  public void makeHLog(HLog log,
      List<HRegionInfo> hris, String tname,
      int num_edits, int edit_size) throws IOException {

    // remove root and meta region
    hris.remove(HRegionInfo.ROOT_REGIONINFO);
    hris.remove(HRegionInfo.FIRST_META_REGIONINFO);
    byte[] table = Bytes.toBytes(tname);
    HTableDescriptor htd = new HTableDescriptor(tname);
    byte[] value = new byte[edit_size];
    for (int i = 0; i < edit_size; i++) {
      value[i] = (byte) ('a' + (i % 26));
    }
    int n = hris.size();
    int[] counts = new int[n];
    if (n > 0) {
      for (int i = 0; i < num_edits; i += 1) {
        WALEdit e = new WALEdit();
        HRegionInfo curRegionInfo = hris.get(i % n);
        byte[] startRow = curRegionInfo.getStartKey();
        if (startRow == null || startRow.length == 0) {
          startRow = new byte[] { 0, 0, 0, 0, 1 };
        }
        byte[] row = Bytes.incrementBytes(startRow, counts[i % n]);
        row = Arrays.copyOfRange(row, 3, 8); // use last 5 bytes because
                                             // HBaseTestingUtility.createMultiRegions use 5 bytes
                                             // key
        byte[] family = Bytes.toBytes("f");
        byte[] qualifier = Bytes.toBytes("c" + Integer.toString(i));
        e.add(new KeyValue(row, family, qualifier, System.currentTimeMillis(), value));
        log.append(curRegionInfo, table, e, System.currentTimeMillis(), htd);
        counts[i % n] += 1;
      }
    }
    log.sync();
    log.close();
    for (int i = 0; i < n; i++) {
      LOG.info("region " + hris.get(i).getRegionNameAsString() + " has " + counts[i] + " edits");
    }
    return;
  }

  private int countHLog(Path log, FileSystem fs, Configuration conf)
  throws IOException {
    int count = 0;
    HLog.Reader in = HLog.getReader(fs, log, conf);
    while (in.next() != null) {
      count++;
    }
    return count;
  }

  private void blockUntilNoRIT(ZooKeeperWatcher zkw, HMaster master)
  throws KeeperException, InterruptedException {
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
  }

  private void putData(HRegion region, byte[] startRow, int numRows, byte [] qf,
      byte [] ...families)
  throws IOException {
    for(int i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.add(startRow, Bytes.toBytes(i)));
      for(byte [] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  private NavigableSet<String> getAllOnlineRegions(MiniHBaseCluster cluster)
      throws IOException {
    NavigableSet<String> online = new TreeSet<String>();
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      for (HRegionInfo region : rst.getRegionServer().getOnlineRegions()) {
        online.add(region.getRegionNameAsString());
      }
    }
    return online;
  }

  private void waitForCounter(AtomicLong ctr, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() == oldval) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.get());
        return;
      }
    }
    assertTrue(false);
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

  private void startMasterTillNoDeadServers(MiniHBaseCluster cluster)
      throws IOException, InterruptedException {
    cluster.startMaster();
    HMaster master = cluster.getMaster();
    while (!master.isInitialized()) {
      Thread.sleep(100);
    }
    ServerManager serverManager = master.getServerManager();
    while (serverManager.areDeadServersInProgress()) {
      Thread.sleep(100);
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

