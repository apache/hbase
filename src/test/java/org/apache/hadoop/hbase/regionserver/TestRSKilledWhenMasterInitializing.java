/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TestMasterFailover;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRSKilledWhenMasterInitializing {
  private static final Log LOG = LogFactory.getLog(TestMasterFailover.class);

  private static final HBaseTestingUtility TESTUTIL = new HBaseTestingUtility();
  private static final int NUM_MASTERS = 1;
  private static final int NUM_RS = 4;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set it so that this test runs with my custom master
    Configuration conf = TESTUTIL.getConfiguration();
    conf.setClass(HConstants.MASTER_IMPL, TestingMaster.class, HMaster.class);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 3);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 4);

    // Start up the cluster.
    TESTUTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (!TESTUTIL.getHBaseCluster().getMaster().isInitialized()) {
      // master is not initialized and is waiting something forever.
      for (MasterThread mt : TESTUTIL.getHBaseCluster().getLiveMasterThreads()) {
        mt.interrupt();
      }
    }
    TESTUTIL.shutdownMiniCluster();
  }

  /**
   * An HMaster instance used in this test. If 'TestingMaster.sleep' is set in
   * the Configuration, then we'll sleep after log is split and we'll also
   * return a custom RegionServerTracker.
   */
  public static class TestingMaster extends HMaster {
    private boolean logSplit = false;

    public TestingMaster(Configuration conf) throws IOException,
        KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    protected void splitLogAfterStartup(MasterFileSystem mfs) {
      super.splitLogAfterStartup(mfs);
      logSplit = true;
      // If "TestingMaster.sleep" is set, sleep after log split.
      if (getConfiguration().getBoolean("TestingMaster.sleep", false)) {
        int duration = getConfiguration().getInt(
            "TestingMaster.sleep.duration", 0);
        Threads.sleep(duration);
      }
    }


    public boolean isLogSplitAfterStartup() {
      return logSplit;
    }
  }

  @Test(timeout = 120000)
  public void testCorrectnessWhenMasterFailOver() throws Exception {
    final byte[] TABLENAME = Bytes.toBytes("testCorrectnessWhenMasterFailOver");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[][] SPLITKEYS = { Bytes.toBytes("b"), Bytes.toBytes("i") };

    MiniHBaseCluster cluster = TESTUTIL.getHBaseCluster();

    HTableDescriptor desc = new HTableDescriptor(TABLENAME);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    HBaseAdmin hbaseAdmin = TESTUTIL.getHBaseAdmin();
    hbaseAdmin.createTable(desc, SPLITKEYS);

    assertTrue(hbaseAdmin.isTableAvailable(TABLENAME));

    HTable table = new HTable(TESTUTIL.getConfiguration(), TABLENAME);
    List<Put> puts = new ArrayList<Put>();
    Put put1 = new Put(Bytes.toBytes("a"));
    put1.add(FAMILY, Bytes.toBytes("q1"), Bytes.toBytes("value"));
    Put put2 = new Put(Bytes.toBytes("h"));
    put2.add(FAMILY, Bytes.toBytes("q1"), Bytes.toBytes("value"));
    Put put3 = new Put(Bytes.toBytes("o"));
    put3.add(FAMILY, Bytes.toBytes("q1"), Bytes.toBytes("value"));
    puts.add(put1);
    puts.add(put2);
    puts.add(put3);
    table.put(puts);
    ResultScanner resultScanner = table.getScanner(new Scan());
    int count = 0;
    while (resultScanner.next() != null) {
      count++;
    }
    resultScanner.close();
    table.close();
    assertEquals(3, count);

    /* Starting test */
    cluster.getConfiguration().setBoolean("TestingMaster.sleep", true);
    cluster.getConfiguration().setInt("TestingMaster.sleep.duration", 10000);

    /* NO.1 .META. region correctness */
    // First abort master
    abortMaster(cluster);
    TestingMaster master = startMasterAndWaitUntilLogSplit(cluster);

    // Second kill meta server
    int metaServerNum = cluster.getServerWithMeta();
    int rootServerNum = cluster.getServerWith(HRegionInfo.ROOT_REGIONINFO
        .getRegionName());
    HRegionServer metaRS = cluster.getRegionServer(metaServerNum);
    LOG.debug("Killing metaRS and carryingRoot = "
        + (metaServerNum == rootServerNum));
    metaRS.kill();
    metaRS.join();

    /*
     * Sleep double time of TestingMaster.sleep.duration, so we can ensure that
     * master has already assigned ROOTandMETA or is blocking on assigning
     * ROOTandMETA
     */
    Thread.sleep(10000 * 2);

    waitUntilMasterIsInitialized(master);

    // Third check whether data is correct in meta region
    assertTrue(hbaseAdmin.isTableAvailable(TABLENAME));

    /*
     * NO.2 -ROOT- region correctness . If the .META. server killed in the NO.1
     * is also carrying -ROOT- region, it is not needed
     */
    if (rootServerNum != metaServerNum) {
      // First abort master
      abortMaster(cluster);
      master = startMasterAndWaitUntilLogSplit(cluster);

      // Second kill meta server
      HRegionServer rootRS = cluster.getRegionServer(rootServerNum);
      LOG.debug("Killing rootRS");
      rootRS.kill();
      rootRS.join();

      /*
       * Sleep double time of TestingMaster.sleep.duration, so we can ensure
       * that master has already assigned ROOTandMETA or is blocking on
       * assigning ROOTandMETA
       */
      Thread.sleep(10000 * 2);
      waitUntilMasterIsInitialized(master);

      // Third check whether data is correct in meta region
      assertTrue(hbaseAdmin.isTableAvailable(TABLENAME));
    }

    /* NO.3 data region correctness */
    ServerManager serverManager = cluster.getMaster().getServerManager();
    while (serverManager.areDeadServersInProgress()) {
      Thread.sleep(100);
    }
    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TESTUTIL);
    ZKAssign.blockUntilNoRIT(zkw);

    table = new HTable(TESTUTIL.getConfiguration(), TABLENAME);
    resultScanner = table.getScanner(new Scan());
    count = 0;
    while (resultScanner.next() != null) {
      count++;
    }
    resultScanner.close();
    table.close();
    assertEquals(3, count);
  }

  private void abortMaster(MiniHBaseCluster cluster)
      throws InterruptedException {
    for (MasterThread mt : cluster.getLiveMasterThreads()) {
      if (mt.getMaster().isActiveMaster()) {
        mt.getMaster().abort("Aborting for tests", new Exception("Trace info"));
        mt.join();
        break;
      }
    }
    LOG.debug("Master is aborted");
  }

  private TestingMaster startMasterAndWaitUntilLogSplit(MiniHBaseCluster cluster)
      throws IOException, InterruptedException {
    TestingMaster master = (TestingMaster) cluster.startMaster().getMaster();
    while (!master.isLogSplitAfterStartup()) {
      Thread.sleep(100);
    }
    LOG.debug("splitted:" + master.isLogSplitAfterStartup() + ",initialized:"
        + master.isInitialized());
    return master;
  }

  private void waitUntilMasterIsInitialized(HMaster master)
      throws InterruptedException {
    while (!master.isInitialized()) {
      Thread.sleep(100);
    }
    LOG.debug("master isInitialized");
  }
  
  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();

}
