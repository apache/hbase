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

import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_wait_for_zk_delete;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_final_transition_failed;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_preempt_task;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_acquired;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_done;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_err;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_resigned;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.NonceGenerator;
import org.apache.hadoop.hbase.client.PerClientRandomNonceGenerator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination;
import org.apache.hadoop.hbase.exceptions.OperationConflictException;
import org.apache.hadoop.hbase.exceptions.RegionInRecoveryException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, LargeTests.class})
@SuppressWarnings("deprecation")
public class TestDistributedLogSplitting {
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);
  static {
    // Uncomment the following line if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);

    // test ThreeRSAbort fails under hadoop2 (2.0.2-alpha) if shortcircuit-read (scr) is on. this
    // turns it off for this test.  TODO: Figure out why scr breaks recovery.
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");

  }

  // Start a cluster with 2 masters and 6 regionservers
  static final int NUM_MASTERS = 2;
  static final int NUM_RS = 6;

  MiniHBaseCluster cluster;
  HMaster master;
  Configuration conf;
  static Configuration originalConf;
  static HBaseTestingUtility TEST_UTIL;
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
  public static void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniZKCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
    TEST_UTIL.shutdownMiniHBaseCluster();
  }

  private void startCluster(int num_rs) throws Exception {
    SplitLogCounters.resetCounters();
    LOG.info("Starting cluster");
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 100.0); // no load balancing
    conf.setInt("hbase.regionserver.wal.max.splitters", 3);
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.setDFSCluster(dfsCluster);
    TEST_UTIL.setZkCluster(zkCluster);
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, num_rs);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
    while (cluster.getLiveRegionServerThreads().size() < num_rs) {
      Threads.sleep(10);
    }
  }

  @Before
  public void before() throws Exception {
    // refresh configuration
    conf = HBaseConfiguration.create(originalConf);
  }

  @After
  public void after() throws Exception {
    try {
      if (TEST_UTIL.getHBaseCluster() != null) {
        for (MasterThread mt : TEST_UTIL.getHBaseCluster().getLiveMasterThreads()) {
          mt.getMaster().abort("closing...", null);
        }
      }
      TEST_UTIL.shutdownMiniHBaseCluster();
    } finally {
      TEST_UTIL.getTestFileSystem().delete(FSUtils.getRootDir(TEST_UTIL.getConfiguration()), true);
      ZKUtil.deleteNodeRecursively(TEST_UTIL.getZooKeeperWatcher(), "/hbase");
    }
  }

  @Test (timeout=300000)
  public void testRecoveredEdits() throws Exception {
    LOG.info("testRecoveredEdits");
    conf.setLong("hbase.regionserver.hlog.blocksize", 30 * 1024); // create more than one wal
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
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
    TableName table = TableName.valueOf("table");
    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean foundRs = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (HRegionInfo region : regions) {
        if (region.getTable().getNameAsString().equalsIgnoreCase("table")) {
          foundRs = true;
          break;
        }
      }
      if (foundRs) break;
    }
    final Path logDir = new Path(rootdir, DefaultWALProvider.getWALDirectoryName(hrs
        .getServerName().toString()));

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.getTable().getNamespaceAsString()
          .equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
        it.remove();
      }
    }
    
    makeWAL(hrs, regions, "table", "family", NUM_LOG_LINES, 100);

    slm.splitLogDistributed(logDir);

    int count = 0;
    for (HRegionInfo hri : regions) {

      Path tdir = FSUtils.getTableDir(rootdir, table);
      Path editsdir =
        WALSplitter.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir, hri.getEncodedName()));
      LOG.debug("checking edits dir " + editsdir);
      FileStatus[] files = fs.listStatus(editsdir);
      assertTrue("edits dir should have more than a single file in it. instead has " + files.length,
          files.length > 1);
      for (int i = 0; i < files.length; i++) {
        int c = countWAL(files[i].getPath(), fs, conf);
        count += c;
      }
      LOG.info(count + " edits in " + files.length + " recovered edits files.");
    }

    // check that the log file is moved
    assertFalse(fs.exists(logDir));

    assertEquals(NUM_LOG_LINES, count);
  }

  @Test(timeout = 300000)
  public void testLogReplayWithNonMetaRSDown() throws Exception {
    LOG.info("testLogReplayWithNonMetaRSDown");
    conf.setLong("hbase.regionserver.hlog.blocksize", 30 * 1024); // create more than one wal
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    HRegionServer hrs = findRSToKill(false, "table");
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    makeWAL(hrs, regions, "table", "family", NUM_LOG_LINES, 100);

    // wait for abort completes
    this.abortRSAndVerifyRecovery(hrs, ht, zkw, NUM_REGIONS_TO_CREATE, NUM_LOG_LINES);
    ht.close();
    zkw.close();
  }

  private static class NonceGeneratorWithDups extends PerClientRandomNonceGenerator {
    private boolean isDups = false;
    private LinkedList<Long> nonces = new LinkedList<Long>();

    public void startDups() {
      isDups = true;
    }

    @Override
    public long newNonce() {
      long nonce = isDups ? nonces.removeFirst() : super.newNonce();
      if (!isDups) {
        nonces.add(nonce);
      }
      return nonce;
    }
  }

  @Test(timeout = 300000)
  public void testNonceRecovery() throws Exception {
    LOG.info("testNonceRecovery");
    final String TABLE_NAME = "table";
    final String FAMILY_NAME = "family";
    final int NUM_REGIONS_TO_CREATE = 40;

    conf.setLong("hbase.regionserver.hlog.blocksize", 100*1024);
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    master.balanceSwitch(false);

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, TABLE_NAME, FAMILY_NAME, NUM_REGIONS_TO_CREATE);
    NonceGeneratorWithDups ng = new NonceGeneratorWithDups();
    NonceGenerator oldNg =
        ConnectionUtils.injectNonceGeneratorForTesting((ClusterConnection)ht.getConnection(), ng);

    try {
      List<Increment> reqs = new ArrayList<Increment>();
      for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
        HRegionServer hrs = rst.getRegionServer();
        List<HRegionInfo> hris = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
        for (HRegionInfo hri : hris) {
          if (TABLE_NAME.equalsIgnoreCase(hri.getTable().getNameAsString())) {
            byte[] key = hri.getStartKey();
            if (key == null || key.length == 0) {
              key = Bytes.copy(hri.getEndKey());
              --(key[key.length - 1]);
            }
            Increment incr = new Increment(key);
            incr.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("q"), 1);
            ht.increment(incr);
            reqs.add(incr);
          }
        }
      }

      HRegionServer hrs = findRSToKill(false, "table");
      abortRSAndWaitForRecovery(hrs, zkw, NUM_REGIONS_TO_CREATE);
      ng.startDups();
      for (Increment incr : reqs) {
        try {
          ht.increment(incr);
          fail("should have thrown");
        } catch (OperationConflictException ope) {
          LOG.debug("Caught as expected: " + ope.getMessage());
        }
      }
    } finally {
      ConnectionUtils.injectNonceGeneratorForTesting((ClusterConnection) ht.getConnection(), oldNg);
      ht.close();
      zkw.close();
    }
  }

  @Test(timeout = 300000)
  public void testLogReplayWithMetaRSDown() throws Exception {
    LOG.info("testRecoveredEditsReplayWithMetaRSDown");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    HRegionServer hrs = findRSToKill(true, "table");
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    makeWAL(hrs, regions, "table", "family", NUM_LOG_LINES, 100);

    this.abortRSAndVerifyRecovery(hrs, ht, zkw, NUM_REGIONS_TO_CREATE, NUM_LOG_LINES);
    ht.close();
    zkw.close();
  }

  private void abortRSAndVerifyRecovery(HRegionServer hrs, Table ht, final ZooKeeperWatcher zkw,
      final int numRegions, final int numofLines) throws Exception {

    abortRSAndWaitForRecovery(hrs, zkw, numRegions);
    assertEquals(numofLines, TEST_UTIL.countRows(ht));
  }

  private void abortRSAndWaitForRecovery(HRegionServer hrs, final ZooKeeperWatcher zkw,
      final int numRegions) throws Exception {
    final MiniHBaseCluster tmpCluster = this.cluster;

    // abort RS
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (tmpCluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (HBaseTestingUtility.getAllOnlineRegions(tmpCluster).size()
            >= (numRegions + 1));
      }
    });

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });
  }

  @Test(timeout = 300000)
  public void testMasterStartsUpWithLogSplittingWork() throws Exception {
    LOG.info("testMasterStartsUpWithLogSplittingWork");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_RS - 1);
    startCluster(NUM_RS);

    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    HRegionServer hrs = findRSToKill(false, "table");
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    makeWAL(hrs, regions, "table", "family", NUM_LOG_LINES, 100);

    // abort master
    abortMaster(cluster);

    // abort RS
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    Thread.sleep(2000);
    LOG.info("Current Open Regions:"
        + HBaseTestingUtility.getAllOnlineRegions(cluster).size());

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (HBaseTestingUtility.getAllOnlineRegions(cluster).size()
          >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    LOG.info("Current Open Regions After Master Node Starts Up:"
        + HBaseTestingUtility.getAllOnlineRegions(cluster).size());

    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));

    ht.close();
    zkw.close();
  }

  @Test(timeout = 300000)
  public void testMasterStartsUpWithLogReplayWork() throws Exception {
    LOG.info("testMasterStartsUpWithLogReplayWork");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_RS - 1);
    startCluster(NUM_RS);

    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    HRegionServer hrs = findRSToKill(false, "table");
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    makeWAL(hrs, regions, "table", "family", NUM_LOG_LINES, 100);

    // abort master
    abortMaster(cluster);

    // abort RS
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for the RS dies
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    Thread.sleep(2000);
    LOG.info("Current Open Regions:" + HBaseTestingUtility.getAllOnlineRegions(cluster).size());

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        boolean done = recoveringRegions != null && recoveringRegions.size() == 0;
        if (!done) {
          LOG.info("Recovering regions: " + recoveringRegions);
        }
        return done;
      }
    });

    LOG.info("Current Open Regions After Master Node Starts Up:"
        + HBaseTestingUtility.getAllOnlineRegions(cluster).size());

    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));

    ht.close();
    zkw.close();
  }


  @Test(timeout = 300000)
  public void testLogReplayTwoSequentialRSDown() throws Exception {
    LOG.info("testRecoveredEditsReplayTwoSequentialRSDown");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs1 = findRSToKill(false, "table");
    regions = ProtobufUtil.getOnlineRegions(hrs1.getRSRpcServices());

    makeWAL(hrs1, regions, "table", "family", NUM_LOG_LINES, 100);

    // abort RS1
    LOG.info("Aborting region server: " + hrs1.getServerName());
    hrs1.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (HBaseTestingUtility.getAllOnlineRegions(cluster).size()
            >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    // sleep a little bit in order to interrupt recovering in the middle
    Thread.sleep(300);
    // abort second region server
    rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs2 = rsts.get(0).getRegionServer();
    LOG.info("Aborting one more region server: " + hrs2.getServerName());
    hrs2.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 2));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (HBaseTestingUtility.getAllOnlineRegions(cluster).size()
            >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });

    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));
    ht.close();
    zkw.close();
  }

  @Test(timeout = 300000)
  public void testMarkRegionsRecoveringInZK() throws Exception {
    LOG.info("testMarkRegionsRecoveringInZK");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    master.balanceSwitch(false);
    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = master.getZooKeeper();
    Table ht = installTable(zkw, "table", "family", 40);
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;

    Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
    HRegionInfo region = null;
    HRegionServer hrs = null;
    ServerName firstFailedServer = null;
    ServerName secondFailedServer = null;
    for (int i = 0; i < NUM_RS; i++) {
      hrs = rsts.get(i).getRegionServer();
      List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      if (regions.isEmpty()) continue;
      region = regions.get(0);
      regionSet.add(region);
      firstFailedServer = hrs.getServerName();
      secondFailedServer = rsts.get((i + 1) % NUM_RS).getRegionServer().getServerName();
      break;
    }

    slm.markRegionsRecovering(firstFailedServer, regionSet);
    slm.markRegionsRecovering(secondFailedServer, regionSet);

    List<String> recoveringRegions = ZKUtil.listChildrenNoWatch(zkw,
      ZKUtil.joinZNode(zkw.recoveringRegionsZNode, region.getEncodedName()));

    assertEquals(recoveringRegions.size(), 2);

    // wait for splitLogWorker to mark them up because there is no WAL files recorded in ZK
    final HRegionServer tmphrs = hrs;
    TEST_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (tmphrs.getRecoveringRegions().size() == 0);
      }
    });
    ht.close();
  }

  @Test(timeout = 300000)
  public void testReplayCmd() throws Exception {
    LOG.info("testReplayCmd");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    final int NUM_REGIONS_TO_CREATE = 40;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      if (regions.size() > 0) break;
    }

    this.prepareData(ht, Bytes.toBytes("family"), Bytes.toBytes("c1"));
    String originalCheckSum = TEST_UTIL.checksumRows(ht);

    // abort RA and trigger replay
    abortRSAndWaitForRecovery(hrs, zkw, NUM_REGIONS_TO_CREATE);

    assertEquals("Data should remain after reopening of regions", originalCheckSum,
      TEST_UTIL.checksumRows(ht));

    ht.close();
    zkw.close();
  }

  @Test(timeout = 300000)
  public void testLogReplayForDisablingTable() throws Exception {
    LOG.info("testLogReplayForDisablingTable");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table disablingHT = installTable(zkw, "disableTable", "family", NUM_REGIONS_TO_CREATE);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE, NUM_REGIONS_TO_CREATE);

    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    boolean hasRegionsForBothTables = false;
    String tableName = null;
    for (int i = 0; i < NUM_RS; i++) {
      tableName = null;
      hasRegionsForBothTables = false;
      boolean isCarryingSystem = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (HRegionInfo region : regions) {
        if (region.getTable().isSystemTable()) {
          isCarryingSystem = true;
          break;
        }
        if (tableName != null &&
            !tableName.equalsIgnoreCase(region.getTable().getNameAsString())) {
          // make sure that we find a RS has online regions for both "table" and "disableTable"
          hasRegionsForBothTables = true;
          break;
        } else if (tableName == null) {
          tableName = region.getTable().getNameAsString();
        }
      }
      if (isCarryingSystem) {
        continue;
      }
      if (hasRegionsForBothTables) {
        break;
      }
    }

    // make sure we found a good RS
    Assert.assertTrue(hasRegionsForBothTables);

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeWAL(hrs, regions, "disableTable", "family", NUM_LOG_LINES, 100, false);
    makeWAL(hrs, regions, "table", "family", NUM_LOG_LINES, 100);

    LOG.info("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(TableName.valueOf("disableTable"));

    // abort RS
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (HBaseTestingUtility.getAllOnlineRegions(cluster).size()
            >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        ServerManager serverManager = master.getServerManager();
        return (!serverManager.areDeadServersInProgress() &&
            recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });

    int count = 0;
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path rootdir = FSUtils.getRootDir(conf);
    Path tdir = FSUtils.getTableDir(rootdir, TableName.valueOf("disableTable"));
    for (HRegionInfo hri : regions) {
      Path editsdir =
        WALSplitter.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir, hri.getEncodedName()));
      LOG.debug("checking edits dir " + editsdir);
      if(!fs.exists(editsdir)) continue;
      FileStatus[] files = fs.listStatus(editsdir);
      if(files != null) {
        for(FileStatus file : files) {
          int c = countWAL(file.getPath(), fs, conf);
          count += c;
          LOG.info(c + " edits in " + file.getPath());
        }
      }
    }

    LOG.info("Verify edits in recovered.edits files");
    assertEquals(NUM_LOG_LINES, count);
    LOG.info("Verify replayed edits");
    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));

    // clean up
    for (HRegionInfo hri : regions) {
      Path editsdir =
        WALSplitter.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir, hri.getEncodedName()));
      fs.delete(editsdir, true);
    }
    disablingHT.close();
    ht.close();
    zkw.close();
  }

  @Test(timeout = 300000)
  public void testDisallowWritesInRecovering() throws Exception {
    LOG.info("testDisallowWritesInRecovering");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    conf.setBoolean(HConstants.DISALLOW_WRITES_IN_RECOVERING, true);
    startCluster(NUM_RS);
    final int NUM_REGIONS_TO_CREATE = 40;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;

    Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
    HRegionInfo region = null;
    HRegionServer hrs = null;
    HRegionServer dstRS = null;
    for (int i = 0; i < NUM_RS; i++) {
      hrs = rsts.get(i).getRegionServer();
      List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      if (regions.isEmpty()) continue;
      region = regions.get(0);
      regionSet.add(region);
      dstRS = rsts.get((i+1) % NUM_RS).getRegionServer();
      break;
    }

    slm.markRegionsRecovering(hrs.getServerName(), regionSet);
    // move region in order for the region opened in recovering state
    final HRegionInfo hri = region;
    final HRegionServer tmpRS = dstRS;
    TEST_UTIL.getHBaseAdmin().move(region.getEncodedNameAsBytes(),
      Bytes.toBytes(dstRS.getServerName().getServerName()));
    // wait for region move completes
    final RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
    TEST_UTIL.waitFor(45000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ServerName sn = regionStates.getRegionServerOfRegion(hri);
        return (sn != null && sn.equals(tmpRS.getServerName()));
      }
    });

    try {
      byte[] key = region.getStartKey();
      if (key == null || key.length == 0) {
        key = new byte[] { 0, 0, 0, 0, 1 };
      }
      ht.setAutoFlushTo(true);
      Put put = new Put(key);
      put.add(Bytes.toBytes("family"), Bytes.toBytes("c1"), new byte[]{'b'});
      ht.put(put);
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof RetriesExhaustedWithDetailsException);
      RetriesExhaustedWithDetailsException re = (RetriesExhaustedWithDetailsException) ioe;
      boolean foundRegionInRecoveryException = false;
      for (Throwable t : re.getCauses()) {
        if (t instanceof RegionInRecoveryException) {
          foundRegionInRecoveryException = true;
          break;
        }
      }
      Assert.assertTrue(
        "No RegionInRecoveryException. Following exceptions returned=" + re.getCauses(),
        foundRegionInRecoveryException);
    }

    ht.close();
    zkw.close();
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
    startCluster(3);
    final int NUM_LOG_LINES = 10000;
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    final List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs = findRSToKill(false, "table");
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        DefaultWALProvider.getWALDirectoryName(hrs.getServerName().toString()));

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);

    makeWAL(hrs, ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices()),
      "table", "family", NUM_LOG_LINES, 100);

    new Thread() {
      @Override
      public void run() {
        waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
        for (RegionServerThread rst : rsts) {
          rst.getRegionServer().abort("testing");
          break;
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
          tot_wkr_final_transition_failed.get() + tot_wkr_task_done.get() +
          tot_wkr_preempt_task.get()) == 0) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertTrue(1 <= (tot_wkr_task_resigned.get() + tot_wkr_task_err.get() +
            tot_wkr_final_transition_failed.get() + tot_wkr_task_done.get() +
            tot_wkr_preempt_task.get()));
        return;
      }
    }
    fail("none of the following counters went up in " + waitTime +
        " milliseconds - " +
        "tot_wkr_task_resigned, tot_wkr_task_err, " +
        "tot_wkr_final_transition_failed, tot_wkr_task_done, " +
        "tot_wkr_preempt_task");
  }

  @Test (timeout=300000)
  public void testThreeRSAbort() throws Exception {
    LOG.info("testThreeRSAbort");
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_ROWS_PER_REGION = 100;

    startCluster(NUM_RS); // NUM_RS=6.

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
        "distributed log splitting test", null);

    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);
    populateDataInTable(NUM_ROWS_PER_REGION, "family");


    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());
    rsts.get(0).getRegionServer().abort("testing");
    rsts.get(1).getRegionServer().abort("testing");
    rsts.get(2).getRegionServer().abort("testing");

    long start = EnvironmentEdgeManager.currentTime();
    while (cluster.getLiveRegionServerThreads().size() > (NUM_RS - 3)) {
      if (EnvironmentEdgeManager.currentTime() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    start = EnvironmentEdgeManager.currentTime();
    while (HBaseTestingUtility.getAllOnlineRegions(cluster).size()
        < (NUM_REGIONS_TO_CREATE + 1)) {
      if (EnvironmentEdgeManager.currentTime() - start > 60000) {
        assertTrue("Timedout", false);
      }
      Thread.sleep(200);
    }

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });

    assertEquals(NUM_REGIONS_TO_CREATE * NUM_ROWS_PER_REGION,
        TEST_UTIL.countRows(ht));
    ht.close();
    zkw.close();
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
      ZKSplitLogManagerCoordination coordination =
          (ZKSplitLogManagerCoordination) ((BaseCoordinatedStateManager) master
              .getCoordinatedStateManager()).getSplitLogManagerCoordination();
      coordination.setIgnoreDeleteForTesting(true);
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

  @Test(timeout = 300000)
  public void testMetaRecoveryInZK() throws Exception {
    LOG.info("testMetaRecoveryInZK");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);

    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);

    // only testing meta recovery in ZK operation
    HRegionServer hrs = findRSToKill(true, null);
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());

    LOG.info("#regions = " + regions.size());
    Set<HRegionInfo> tmpRegions = new HashSet<HRegionInfo>();
    tmpRegions.add(HRegionInfo.FIRST_META_REGIONINFO);
    master.getMasterFileSystem().prepareLogReplay(hrs.getServerName(), tmpRegions);
    Set<HRegionInfo> userRegionSet = new HashSet<HRegionInfo>();
    userRegionSet.addAll(regions);
    master.getMasterFileSystem().prepareLogReplay(hrs.getServerName(), userRegionSet);
    boolean isMetaRegionInRecovery = false;
    List<String> recoveringRegions =
        zkw.getRecoverableZooKeeper().getChildren(zkw.recoveringRegionsZNode, false);
    for (String curEncodedRegionName : recoveringRegions) {
      if (curEncodedRegionName.equals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName())) {
        isMetaRegionInRecovery = true;
        break;
      }
    }
    assertTrue(isMetaRegionInRecovery);

    master.getMasterFileSystem().splitMetaLog(hrs.getServerName());

    isMetaRegionInRecovery = false;
    recoveringRegions =
        zkw.getRecoverableZooKeeper().getChildren(zkw.recoveringRegionsZNode, false);
    for (String curEncodedRegionName : recoveringRegions) {
      if (curEncodedRegionName.equals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName())) {
        isMetaRegionInRecovery = true;
        break;
      }
    }
    // meta region should be recovered
    assertFalse(isMetaRegionInRecovery);
    zkw.close();
  }

  @Test(timeout = 300000)
  public void testSameVersionUpdatesRecovery() throws Exception {
    LOG.info("testSameVersionUpdatesRecovery");
    conf.setLong("hbase.regionserver.hlog.blocksize", 15 * 1024);
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS);
    final AtomicLong sequenceId = new AtomicLong(100);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
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
      if (region.isMetaTable()
          || region.getEncodedName().equals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName())) {
        it.remove();
      }
    }
    if (regions.size() == 0) return;
    HRegionInfo curRegionInfo = regions.get(0);
    byte[] startRow = curRegionInfo.getStartKey();
    if (startRow == null || startRow.length == 0) {
      startRow = new byte[] { 0, 0, 0, 0, 1 };
    }
    byte[] row = Bytes.incrementBytes(startRow, 1);
    // use last 5 bytes because HBaseTestingUtility.createMultiRegions use 5 bytes key
    row = Arrays.copyOfRange(row, 3, 8);
    long value = 0;
    TableName tableName = TableName.valueOf("table");
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("c1");
    long timeStamp = System.currentTimeMillis();
    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor(family));
    final WAL wal = hrs.getWAL(curRegionInfo);
    for (int i = 0; i < NUM_LOG_LINES; i += 1) {
      WALEdit e = new WALEdit();
      value++;
      e.add(new KeyValue(row, family, qualifier, timeStamp, Bytes.toBytes(value)));
      wal.append(htd, curRegionInfo,
          new HLogKey(curRegionInfo.getEncodedNameAsBytes(), tableName, System.currentTimeMillis()),
          e, sequenceId, true, null);
    }
    wal.sync();
    wal.shutdown();

    // wait for abort completes
    this.abortRSAndWaitForRecovery(hrs, zkw, NUM_REGIONS_TO_CREATE);

    // verify we got the last value
    LOG.info("Verification Starts...");
    Get g = new Get(row);
    Result r = ht.get(g);
    long theStoredVal = Bytes.toLong(r.getValue(family, qualifier));
    assertEquals(value, theStoredVal);

    // after flush
    LOG.info("Verification after flush...");
    TEST_UTIL.getHBaseAdmin().flush(tableName);
    r = ht.get(g);
    theStoredVal = Bytes.toLong(r.getValue(family, qualifier));
    assertEquals(value, theStoredVal);
    ht.close();
  }

  @Test(timeout = 300000)
  public void testSameVersionUpdatesRecoveryWithCompaction() throws Exception {
    LOG.info("testSameVersionUpdatesRecoveryWithWrites");
    conf.setLong("hbase.regionserver.hlog.blocksize", 15 * 1024);
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 30 * 1024);
    conf.setInt("hbase.hstore.compactionThreshold", 3);
    startCluster(NUM_RS);
    final AtomicLong sequenceId = new AtomicLong(100);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 2000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
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
      if (region.isMetaTable()
          || region.getEncodedName().equals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName())) {
        it.remove();
      }
    }
    if (regions.size() == 0) return;
    HRegionInfo curRegionInfo = regions.get(0);
    byte[] startRow = curRegionInfo.getStartKey();
    if (startRow == null || startRow.length == 0) {
      startRow = new byte[] { 0, 0, 0, 0, 1 };
    }
    byte[] row = Bytes.incrementBytes(startRow, 1);
    // use last 5 bytes because HBaseTestingUtility.createMultiRegions use 5 bytes key
    row = Arrays.copyOfRange(row, 3, 8);
    long value = 0;
    final TableName tableName = TableName.valueOf("table");
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("c1");
    long timeStamp = System.currentTimeMillis();
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family));
    final WAL wal = hrs.getWAL(curRegionInfo);
    for (int i = 0; i < NUM_LOG_LINES; i += 1) {
      WALEdit e = new WALEdit();
      value++;
      e.add(new KeyValue(row, family, qualifier, timeStamp, Bytes.toBytes(value)));
      wal.append(htd, curRegionInfo, new HLogKey(curRegionInfo.getEncodedNameAsBytes(),
          tableName, System.currentTimeMillis()), e, sequenceId, true, null);
    }
    wal.sync();
    wal.shutdown();

    // wait for abort completes
    this.abortRSAndWaitForRecovery(hrs, zkw, NUM_REGIONS_TO_CREATE);

    // verify we got the last value
    LOG.info("Verification Starts...");
    Get g = new Get(row);
    Result r = ht.get(g);
    long theStoredVal = Bytes.toLong(r.getValue(family, qualifier));
    assertEquals(value, theStoredVal);

    // after flush & compaction
    LOG.info("Verification after flush...");
    TEST_UTIL.getHBaseAdmin().flush(tableName);
    TEST_UTIL.getHBaseAdmin().compact(tableName);

    // wait for compaction completes
    TEST_UTIL.waitFor(30000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (TEST_UTIL.getHBaseAdmin().getCompactionState(tableName) == CompactionState.NONE);
      }
    });

    r = ht.get(g);
    theStoredVal = Bytes.toLong(r.getValue(family, qualifier));
    assertEquals(value, theStoredVal);
    ht.close();
  }

  @Test(timeout = 300000)
  public void testReadWriteSeqIdFiles() throws Exception {
    LOG.info("testReadWriteSeqIdFiles");
    startCluster(2);
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", 10);
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), TableName.valueOf("table"));
    List<Path> regionDirs = FSUtils.getRegionDirs(fs, tableDir);
    WALSplitter.writeRegionOpenSequenceIdFile(fs, regionDirs.get(0) , 1L, 1000L);
    // current SeqId file has seqid=1001
    WALSplitter.writeRegionOpenSequenceIdFile(fs, regionDirs.get(0) , 1L, 1000L);
    // current SeqId file has seqid=2001
    assertEquals(3001, WALSplitter.writeRegionOpenSequenceIdFile(fs, regionDirs.get(0), 3L, 1000L));
    
    Path editsdir = WALSplitter.getRegionDirRecoveredEditsDir(regionDirs.get(0));
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        return WALSplitter.isSequenceIdFile(p);
      }
    });
    // only one seqid file should exist
    assertEquals(1, files.length);
    
    // verify all seqId files aren't treated as recovered.edits files
    NavigableSet<Path> recoveredEdits = WALSplitter.getSplitEditFilesSorted(fs, regionDirs.get(0));
    assertEquals(0, recoveredEdits.size());
    
    ht.close();
  } 
  
  HTable installTable(ZooKeeperWatcher zkw, String tname, String fname, int nrs) throws Exception {
    return installTable(zkw, tname, fname, nrs, 0);
  }

  HTable installTable(ZooKeeperWatcher zkw, String tname, String fname, int nrs,
      int existingRegions) throws Exception {
    // Create a table with regions
    TableName table = TableName.valueOf(tname);
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
    NavigableSet<String> regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    LOG.debug("Verifying only catalog and namespace regions are assigned\n");
    if (regions.size() != 2) {
      for (String oregion : regions)
        LOG.debug("Region still online: " + oregion);
    }
    assertEquals(2 + existingRegions, regions.size());
    LOG.debug("Enabling table\n");
    TEST_UTIL.getHBaseAdmin().enableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    LOG.debug("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    assertEquals(numRegions + 2 + existingRegions, regions.size());
    return ht;
  }

  void populateDataInTable(int nrows, String fname) throws Exception {
    byte [] family = Bytes.toBytes(fname);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());

    for (RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      List<HRegionInfo> hris = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (HRegionInfo hri : hris) {
        if (hri.getTable().isSystemTable()) {
          continue;
        }
        LOG.debug("adding data to rs = " + rst.getName() +
            " region = "+ hri.getRegionNameAsString());
        HRegion region = hrs.getOnlineRegion(hri.getRegionName());
        assertTrue(region != null);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }

    for (MasterThread mt : cluster.getLiveMasterThreads()) {
      HRegionServer hrs = mt.getMaster();
      List<HRegionInfo> hris;
      try {
        hris = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      } catch (ServerNotRunningYetException e) {
        // It's ok: this master may be a backup. Ignored.
        continue;
      }
      for (HRegionInfo hri : hris) {
        if (hri.getTable().isSystemTable()) {
          continue;
        }
        LOG.debug("adding data to rs = " + mt.getName() +
            " region = "+ hri.getRegionNameAsString());
        HRegion region = hrs.getOnlineRegion(hri.getRegionName());
        assertTrue(region != null);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }
  }

  public void makeWAL(HRegionServer hrs, List<HRegionInfo> regions, String tname, String fname,
      int num_edits, int edit_size) throws IOException {
    makeWAL(hrs, regions, tname, fname, num_edits, edit_size, true);
  }

  public void makeWAL(HRegionServer hrs, List<HRegionInfo> regions, String tname, String fname,
      int num_edits, int edit_size, boolean cleanShutdown) throws IOException {
    TableName fullTName = TableName.valueOf(tname);
    // remove root and meta region
    regions.remove(HRegionInfo.FIRST_META_REGIONINFO);
    // using one sequenceId for edits across all regions is ok.
    final AtomicLong sequenceId = new AtomicLong(10);


    for(Iterator<HRegionInfo> iter = regions.iterator(); iter.hasNext(); ) {
      HRegionInfo regionInfo = iter.next();
      if(regionInfo.getTable().isSystemTable()) {
         iter.remove();
      }
    }
    HTableDescriptor htd = new HTableDescriptor(fullTName);
    byte[] family = Bytes.toBytes(fname);
    htd.addFamily(new HColumnDescriptor(family));
    byte[] value = new byte[edit_size];

    List<HRegionInfo> hris = new ArrayList<HRegionInfo>();
    for (HRegionInfo region : regions) {
      if (!region.getTable().getNameAsString().equalsIgnoreCase(tname)) {
        continue;
      }
      hris.add(region);
    }
    LOG.info("Creating wal edits across " + hris.size() + " regions.");
    for (int i = 0; i < edit_size; i++) {
      value[i] = (byte) ('a' + (i % 26));
    }
    int n = hris.size();
    int[] counts = new int[n];
    // sync every ~30k to line up with desired wal rolls
    final int syncEvery = 30 * 1024 / edit_size;
    if (n > 0) {
      for (int i = 0; i < num_edits; i += 1) {
        WALEdit e = new WALEdit();
        HRegionInfo curRegionInfo = hris.get(i % n);
        final WAL log = hrs.getWAL(curRegionInfo);
        byte[] startRow = curRegionInfo.getStartKey();
        if (startRow == null || startRow.length == 0) {
          startRow = new byte[] { 0, 0, 0, 0, 1 };
        }
        byte[] row = Bytes.incrementBytes(startRow, counts[i % n]);
        row = Arrays.copyOfRange(row, 3, 8); // use last 5 bytes because
                                             // HBaseTestingUtility.createMultiRegions use 5 bytes
                                             // key
        byte[] qualifier = Bytes.toBytes("c" + Integer.toString(i));
        e.add(new KeyValue(row, family, qualifier, System.currentTimeMillis(), value));
        log.append(htd, curRegionInfo, new HLogKey(curRegionInfo.getEncodedNameAsBytes(), fullTName,
            System.currentTimeMillis()), e, sequenceId, true, null);
        if (0 == i % syncEvery) {
          log.sync();
        }
        counts[i % n] += 1;
      }
    }
    // done as two passes because the regions might share logs. shutdown is idempotent, but sync
    // will cause errors if done after.
    for (HRegionInfo info : hris) {
      final WAL log = hrs.getWAL(info);
      log.sync();
    }
    if (cleanShutdown) {
      for (HRegionInfo info : hris) {
        final WAL log = hrs.getWAL(info);
        log.shutdown();
      }
    }
    for (int i = 0; i < n; i++) {
      LOG.info("region " + hris.get(i).getRegionNameAsString() + " has " + counts[i] + " edits");
    }
    return;
  }

  private int countWAL(Path log, FileSystem fs, Configuration conf)
  throws IOException {
    int count = 0;
    WAL.Reader in = WALFactory.createReader(fs, log, conf);
    try {
      WAL.Entry e;
      while ((e = in.next()) != null) {
        if (!WALEdit.isMetaEditFamily(e.getEdit().getCells().get(0))) {
          count++;
        }
      }
    } finally {
      try {
        in.close();
      } catch (IOException exception) {
        LOG.warn("Problem closing wal: " + exception.getMessage());
        LOG.debug("exception details.", exception);
      }
    }
    return count;
  }

  private void blockUntilNoRIT(ZooKeeperWatcher zkw, HMaster master) throws Exception {
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
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

  /**
   * Load table with puts and deletes with expected values so that we can verify later
   */
  private void prepareData(final HTable t, final byte[] f, final byte[] column) throws IOException {
    t.setAutoFlushTo(false);
    byte[] k = new byte[3];

    // add puts
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(f, column, k);
          t.put(put);
        }
      }
    }
    t.flushCommits();
    // add deletes
    for (byte b3 = 'a'; b3 <= 'z'; b3++) {
      k[0] = 'a';
      k[1] = 'a';
      k[2] = b3;
      Delete del = new Delete(k);
      t.delete(del);
    }
    t.flushCommits();
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

  /**
   * Find a RS that has regions of a table.
   * @param hasMetaRegion when true, the returned RS has hbase:meta region as well
   * @param tableName
   * @return
   * @throws Exception
   */
  private HRegionServer findRSToKill(boolean hasMetaRegion, String tableName) throws Exception {
    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;

    for (RegionServerThread rst: rsts) {
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
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
        }
        if (tableName == null || region.getTable().getNameAsString().equals(tableName)) {
          foundTableRegion = true;
        }
        if (foundTableRegion && (isCarryingMeta || !hasMetaRegion)) {
          break;
        }
      }
      if (isCarryingMeta && hasMetaRegion) {
        // clients ask for a RS with META
        if (!foundTableRegion) {
          final HRegionServer destRS = hrs;
          // the RS doesn't have regions of the specified table so we need move one to this RS
          List<HRegionInfo> tableRegions =
              TEST_UTIL.getHBaseAdmin().getTableRegions(TableName.valueOf(tableName));
          final HRegionInfo hri = tableRegions.get(0);
          TEST_UTIL.getHBaseAdmin().move(hri.getEncodedNameAsBytes(),
            Bytes.toBytes(destRS.getServerName().getServerName()));
          // wait for region move completes
          final RegionStates regionStates =
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
      if (foundTableRegion) break;
    }

    return hrs;
  }

}
