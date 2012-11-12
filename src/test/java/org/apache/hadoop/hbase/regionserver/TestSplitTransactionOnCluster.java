/**
 * Copyright 2010 The Apache Software Foundation
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.handler.SplitRegionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Like {@link TestSplitTransaction} in that we're testing {@link SplitTransaction}
 * only the below tests are against a running cluster where {@link TestSplitTransaction}
 * is tests against a bare {@link HRegion}.
 */
@Category(LargeTests.class)
public class TestSplitTransactionOnCluster {
  private static final Log LOG =
    LogFactory.getLog(TestSplitTransactionOnCluster.class);
  private HBaseAdmin admin = null;
  private MiniHBaseCluster cluster = null;
  private static final int NB_SERVERS = 2;
  private static CountDownLatch latch = new CountDownLatch(1);
  private static boolean secondSplit = false;
  private static boolean callRollBack = false;
  private static boolean firstSplitCompleted = false;
  
  private static final HBaseTestingUtility TESTING_UTIL =
    new HBaseTestingUtility();

  @BeforeClass public static void before() throws Exception {
    TESTING_UTIL.getConfiguration().setInt("hbase.balancer.period", 60000);
    // Needed because some tests have splits happening on RS that are killed
    // We don't want to wait 3min for the master to figure it out
    TESTING_UTIL.getConfiguration().setInt(
        "hbase.master.assignment.timeoutmonitor.timeout", 4000);
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
  }

  @AfterClass public static void after() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    TESTING_UTIL.ensureSomeNonStoppedRegionServersAvailable(NB_SERVERS);
    this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    this.cluster = TESTING_UTIL.getMiniHBaseCluster();
  }

  private HRegionInfo getAndCheckSingleTableRegion(final List<HRegion> regions) {
    assertEquals(1, regions.size());
    return regions.get(0).getRegionInfo();
  }

  /**
   * A test that intentionally has master fail the processing of the split message.
   * Tests that the regionserver split ephemeral node gets cleaned up if it
   * crashes and that after we process server shutdown, the daughters are up on
   * line.
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test (timeout = 300000) public void testRSSplitEphemeralsDisappearButDaughtersAreOnlinedAfterShutdownHandling()
  throws IOException, InterruptedException, NodeExistsException, KeeperException {
    final byte [] tableName =
      Bytes.toBytes("ephemeral");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      SplitRegionHandler.TEST_SKIP = true;
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(t.getConnection().getZooKeeperWatcher(),
        hri.getEncodedName());
      Stat stats =
        t.getConnection().getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats=" + stats);
      RegionTransitionData rtd =
        ZKAssign.getData(t.getConnection().getZooKeeperWatcher(),
          hri.getEncodedName());
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLIT) ||
        rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLITTING));
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();

      // Wait till regions are back on line again.
      while(cluster.getRegions(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online.
      regions = cluster.getRegions(tableName);
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
      // Finally assert that the ephemeral SPLIT znode was cleaned up.
      stats = t.getConnection().getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE AFTER SERVER ABORT, path=" + path + ", stats=" + stats);
      assertTrue(stats == null);
    } finally {
      // Set this flag back.
      SplitRegionHandler.TEST_SKIP = false;
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  @Test (timeout = 300000) public void testExistingZnodeBlocksSplitAndWeRollback()
  throws IOException, InterruptedException, NodeExistsException, KeeperException {
    final byte [] tableName =
      Bytes.toBytes("testExistingZnodeBlocksSplitAndWeRollback");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Insert into zk a blocking znode, a znode of same name as region
      // so it gets in way of our splitting.
      ZKAssign.createNodeClosing(t.getConnection().getZooKeeperWatcher(),
        hri, new ServerName("any.old.server", 1234, -1));
      // Now try splitting.... should fail.  And each should successfully
      // rollback.
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      // Wait around a while and assert count of regions remains constant.
      for (int i = 0; i < 10; i++) {
        Thread.sleep(100);
        assertEquals(regionCount, server.getOnlineRegions().size());
      }
      // Now clear the zknode
      ZKAssign.deleteClosingNode(t.getConnection().getZooKeeperWatcher(), hri);
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // OK, so split happened after we cleared the blocking node.
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * Messy test that simulates case where SplitTransactions fails to add one
   * of the daughters up into the .META. table before crash.  We're testing
   * fact that the shutdown handler will fixup the missing daughter region
   * adding it back into .META.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout = 300000) public void testShutdownSimpleFixup()
  throws IOException, InterruptedException {
    final byte [] tableName = Bytes.toBytes("testShutdownSimpleFixup");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Now split.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Remove one of the daughters from .META. to simulate failed insert of
      // daughter region up into .META.
      removeDaughterFromMeta(daughters.get(0).getRegionName());
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      // Wait till regions are back on line again.
      while(cluster.getRegions(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online.
      regions = cluster.getRegions(tableName);
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * Test that if daughter split on us, we won't do the shutdown handler fixup
   * just because we can't find the immediate daughter of an offlined parent.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000) public void testShutdownFixupWhenDaughterHasSplit()
  throws IOException, InterruptedException {
    final byte [] tableName =
      Bytes.toBytes("testShutdownFixupWhenDaughterHasSplit");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Now split.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Now split one of the daughters.
      regionCount = server.getOnlineRegions().size();
      HRegionInfo daughter = daughters.get(0).getRegionInfo();
      // Compact first to ensure we have cleaned up references -- else the split
      // will fail.
      this.admin.compact(daughter.getRegionName());
      daughters = cluster.getRegions(tableName);
      HRegion daughterRegion = null;
      for (HRegion r: daughters) {
        if (r.getRegionInfo().equals(daughter)) daughterRegion = r;
      }
      assertTrue(daughterRegion != null);
      while (true) {
        if (!daughterRegion.hasReferences()) break;
        Threads.sleep(100);
      }
      split(daughter, server, regionCount);
      // Get list of daughters
      daughters = cluster.getRegions(tableName);
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      // Wait till regions are back on line again.
      while(cluster.getRegions(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online and ONLY the original daughters -- that
      // fixup didn't insert one during server shutdown recover.
      regions = cluster.getRegions(tableName);
      assertEquals(daughters.size(), regions.size());
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }
  
  /**
   * Verifies HBASE-5806.  When splitting is partially done and the master goes down
   * when the SPLIT node is in either SPLIT or SPLITTING state.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test(timeout = 300000)
  public void testMasterRestartWhenSplittingIsPartial()
      throws IOException, InterruptedException, NodeExistsException,
      KeeperException {
    final byte[] tableName = Bytes.toBytes("testMasterRestartWhenSplittingIsPartial");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      SplitRegionHandler.TEST_SKIP = true;
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      Stat stats = t.getConnection().getZooKeeperWatcher()
          .getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      RegionTransitionData rtd = ZKAssign.getData(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLIT)
          || rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLITTING));


      // abort and wait for new master.
      MockMasterWithoutCatalogJanitor master = abortAndWaitForMaster();

      this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());

      // update the hri to be offlined and splitted. 
      hri.setOffline(true);
      hri.setSplit(true);
      ServerName regionServerOfRegion = master.getAssignmentManager()
          .getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion != null);

    } finally {
      // Set this flag back.
      SplitRegionHandler.TEST_SKIP = false;
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }


  /**
   * Verifies HBASE-5806.  Here the case is that splitting is completed but before the
   * CJ could remove the parent region the master is killed and restarted.
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test (timeout = 300000)
  public void testMasterRestartAtRegionSplitPendingCatalogJanitor()
      throws IOException, InterruptedException, NodeExistsException,
      KeeperException {
    final byte[] tableName = Bytes.toBytes("testMasterRestartAtRegionSplitPendingCatalogJanitor");

    // Create table then get the single region for our new table.
    this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(HConstants.CATALOG_FAMILY);
    htd.addFamily(hcd);
    this.admin.createTable(htd);
    HTable t = new HTable(TESTING_UTIL.getConfiguration(), tableName);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = server.getOnlineRegions().size();
      
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() >= 2);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      Stat stats = t.getConnection().getZooKeeperWatcher()
          .getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      String node = ZKAssign.getNodeName(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(t.getConnection()
          .getZooKeeperWatcher(), node, stat);
      // ZKUtil.create
      while (data != null) {
        Thread.sleep(1000);
        data = ZKUtil.getDataNoWatch(t.getConnection().getZooKeeperWatcher(),
            node, stat);

      }
      
      MockMasterWithoutCatalogJanitor master = abortAndWaitForMaster();

      this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());

      hri.setOffline(true);
      hri.setSplit(true);
      ServerName regionServerOfRegion = master.getAssignmentManager()
          .getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion == null);
    } finally {
      // Set this flag back.
      SplitRegionHandler.TEST_SKIP = false;
      this.admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * While transitioning node from RS_ZK_REGION_SPLITTING to
   * RS_ZK_REGION_SPLITTING during region split,if zookeper went down split always
   * fails for the region. HBASE-6088 fixes this scenario.
   * This test case is to test the znode is deleted(if created) or not in roll back.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test
  public void testSplitBeforeSettingSplittingInZK() throws IOException,
      InterruptedException, KeeperException {
    testSplitBeforeSettingSplittingInZK(true);
    testSplitBeforeSettingSplittingInZK(false);
  }

  private void testSplitBeforeSettingSplittingInZK(boolean nodeCreated) throws IOException,
      KeeperException {
    final byte[] tableName = Bytes.toBytes("testSplitBeforeSettingSplittingInZK");
    
    HBaseAdmin admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    try {
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(htd);

      List<HRegion> regions = cluster.getRegions(tableName);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      SplitTransaction st = null;
      if (nodeCreated) {
        st = new MockedSplitTransaction(regions.get(0), null) {
          @Override
          int transitionNodeSplitting(ZooKeeperWatcher zkw, HRegionInfo parent,
              ServerName serverName, int version) throws KeeperException, IOException {
            throw new IOException();
          }
        };
      } else {
        st = new MockedSplitTransaction(regions.get(0), null) {
          @Override
          void createNodeSplitting(ZooKeeperWatcher zkw, HRegionInfo region, ServerName serverName)
              throws KeeperException, IOException {
            throw new IOException();
          }
        };
      }
      try {
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        String node = ZKAssign.getNodeName(regionServer.getZooKeeper(), regions.get(0)
            .getRegionInfo().getEncodedName());
        if (nodeCreated) {
          assertFalse(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        } else {
          assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        }
        assertTrue(st.rollback(regionServer, regionServer));
        assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
      }
    } finally {
      if (admin.isTableAvailable(tableName) && admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    }
  }
  
  @Test
  public void testShouldClearRITWhenNodeFoundInSplittingState() throws Exception {
    final byte[] tableName = Bytes.toBytes("testShouldClearRITWhenNodeFoundInSplittingState");
    HBaseAdmin admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    try {
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(htd);

      List<HRegion> regions = cluster.getRegions(tableName);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      SplitTransaction st = null;

      st = new MockedSplitTransaction(regions.get(0), null) {
        @Override
        void createSplitDir(FileSystem fs, Path splitdir) throws IOException {
          throw new IOException("");
        }
      };

      try {
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        String node = ZKAssign.getNodeName(regionServer.getZooKeeper(), regions.get(0)
            .getRegionInfo().getEncodedName());

        assertFalse(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        AssignmentManager am = cluster.getMaster().getAssignmentManager();
        while (!am.getRegionsInTransition().containsKey(regions.get(0).getRegionInfo().getEncodedName())) {
          Thread.sleep(200);
        }
        RegionState regionState = am.getRegionsInTransition().get(regions.get(0).getRegionInfo()
            .getEncodedName());
        assertTrue(regionState.getState() == RegionState.State.SPLITTING);
        assertTrue(st.rollback(regionServer, regionServer));
        assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        while (am.getRegionsInTransition().containsKey(regions.get(0).getRegionInfo().getEncodedName())) {
          // Just in case the nodeDeleted event did not get executed.
          Thread.sleep(200);
        }
      }
    } finally {
      if (admin.isTableAvailable(tableName) && admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
      }
    }
  }
  
  @Test(timeout = 2000000)
  public void testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack() throws Exception {
    final byte[] tableName = Bytes
        .toBytes("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack");
    HBaseAdmin admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    try {
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(htd);
      HTable t = new HTable(cluster.getConfiguration(), tableName);
      while (!(cluster.getRegions(tableName).size() == 1)) {
        Thread.sleep(100);
      }
      final List<HRegion> regions = cluster.getRegions(tableName);
      HRegionInfo hri = getAndCheckSingleTableRegion(regions);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
      final HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      insertData(tableName, admin, t);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      this.admin.setBalancerRunning(false, false);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);

      new Thread() {
        public void run() {
          SplitTransaction st = null;
          st = new MockedSplitTransaction(regions.get(0), Bytes.toBytes("row2"));
          try {
            st.prepare();
            st.execute(regionServer, regionServer);
          } catch (IOException e) {

          }
        }
      }.start();
      while (!callRollBack) {
        Thread.sleep(100);
      }
      SplitTransaction st = null;
      st = new MockedSplitTransaction(regions.get(0), Bytes.toBytes("row2"));
      try {
        secondSplit = true;
        st.prepare();
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        LOG.debug("Rollback started :"+ e.getMessage());
        st.rollback(regionServer, regionServer);
      }
      while (!firstSplitCompleted) {
        Thread.sleep(100);
      }
      NavigableMap<String, RegionState> rit = cluster.getMaster().getAssignmentManager()
          .getRegionsInTransition();
      while (rit.containsKey(hri.getTableNameAsString())) {
        Thread.sleep(100);
      }
      List<HRegion> onlineRegions = regionServer.getOnlineRegions(tableName);
      // Region server side split is successful.
      assertEquals("The parent region should be splitted", 2, onlineRegions.size());
      //Should be present in RIT
      List<HRegionInfo> regionsOfTable = cluster.getMaster().getAssignmentManager().getRegionsOfTable(tableName);
      // Master side should also reflect the same
      assertEquals("No of regions in master", 2, regionsOfTable.size());
    } finally {
      admin.setBalancerRunning(true, false);
      secondSplit = false;
      firstSplitCompleted = false;
      callRollBack = false;
      cluster.getMaster().setCatalogJanitorEnabled(true);
      if (admin.isTableAvailable(tableName) && admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
      }
    }
  }

  private void insertData(final byte[] tableName, HBaseAdmin admin, HTable t) throws IOException,
      InterruptedException {
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("1"));
    t.put(p);
    p = new Put(Bytes.toBytes("row2"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("2"));
    t.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("3"));
    t.put(p);
    p = new Put(Bytes.toBytes("row4"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("4"));
    t.put(p);
    admin.flush(tableName);
  }
  @Test(timeout = 15000)
  public void testShouldThrowIOExceptionIfStoreFileSizeIsEmptyAndSHouldSuccessfullyExecuteRollback()
      throws Exception {
    final byte[] tableName = Bytes
        .toBytes("testShouldThrowIOExceptionIfStoreFileSizeIsEmptyAndSHouldSuccessfullyExecuteRollback");
    HBaseAdmin admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    try {
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(htd);
      while (!(cluster.getRegions(tableName).size() == 1)) {
        Thread.sleep(100);
      }
      List<HRegion> regions = cluster.getRegions(tableName);
      HRegionInfo hri = getAndCheckSingleTableRegion(regions);
      int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      this.admin.setBalancerRunning(false, false);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      // Now split.
      SplitTransaction st = null;
      st = new MockedSplitTransaction(regions.get(0), null);
      try {
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        List<HRegion> daughters = cluster.getRegions(tableName);
        assertTrue(daughters.size() == 1);

        String node = ZKAssign.getNodeName(regionServer.getZooKeeper(), regions.get(0)
            .getRegionInfo().getEncodedName());
        assertFalse(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        assertTrue(st.rollback(regionServer, regionServer));
        assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
      }
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      if (admin.isTableAvailable(tableName) && admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
      }
    }
  }
  public static class MockedSplitTransaction extends SplitTransaction {

    private HRegion currentRegion;
    public MockedSplitTransaction(HRegion r, byte[] splitrow) {
      super(r, splitrow);
      this.currentRegion = r;
    }
    
    @Override
    void transitionZKNode(Server server, RegionServerServices services, HRegion a, HRegion b)
        throws IOException {
      if (this.currentRegion.getRegionInfo().getTableNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        try {
          if (!secondSplit){
            callRollBack = true;
            latch.await();
          }
        } catch (InterruptedException e) {
        }
       
      }
      super.transitionZKNode(server, services, a, b);
      if (this.currentRegion.getRegionInfo().getTableNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        firstSplitCompleted = true;
      }
    }
    @Override
    public boolean rollback(Server server, RegionServerServices services) throws IOException {
      if (this.currentRegion.getRegionInfo().getTableNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        if(secondSplit){
          super.rollback(server, services);
          latch.countDown();
          return true;
        }
      }
      return super.rollback(server, services);
    }

  }
  
  private MockMasterWithoutCatalogJanitor abortAndWaitForMaster() 
  throws IOException, InterruptedException {
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    cluster.getConfiguration().setClass(HConstants.MASTER_IMPL, 
        MockMasterWithoutCatalogJanitor.class, HMaster.class);
    MockMasterWithoutCatalogJanitor master = null;
    master = (MockMasterWithoutCatalogJanitor) cluster.startMaster().getMaster();
    cluster.waitForActiveAndReadyMaster();
    return master;
  }

  private void split(final HRegionInfo hri, final HRegionServer server,
      final int regionCount)
  throws IOException, InterruptedException {
    this.admin.split(hri.getRegionNameAsString());
    while (server.getOnlineRegions().size() <= regionCount) {
      LOG.debug("Waiting on region to split");
      Thread.sleep(100);
    }
  }

  private void removeDaughterFromMeta(final byte [] regionName) throws IOException {
    HTable metaTable =
      new HTable(TESTING_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);
    Delete d = new Delete(regionName);
    LOG.info("Deleted " + Bytes.toString(regionName));
    metaTable.delete(d);
  }

  /**
   * Ensure single table region is not on same server as the single .META. table
   * region.
   * @param admin
   * @param hri
   * @return Index of the server hosting the single table region
   * @throws UnknownRegionException
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws InterruptedException
   */
  private int ensureTableRegionNotOnSameServerAsMeta(final HBaseAdmin admin,
      final HRegionInfo hri)
  throws UnknownRegionException, MasterNotRunningException,
  ZooKeeperConnectionException, InterruptedException {
    MiniHBaseCluster cluster = TESTING_UTIL.getMiniHBaseCluster();
    // Now make sure that the table region is not on same server as that hosting
    // .META.  We don't want .META. replay polluting our test when we later crash
    // the table region serving server.
    int metaServerIndex = cluster.getServerWithMeta();
    assertTrue(metaServerIndex != -1);
    HRegionServer metaRegionServer = cluster.getRegionServer(metaServerIndex);
    int tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    HRegionServer tableRegionServer = cluster.getRegionServer(tableRegionIndex);
    if (metaRegionServer.getServerName().equals(tableRegionServer.getServerName())) {
      HRegionServer hrs = getOtherRegionServer(cluster, metaRegionServer);
      assertNotNull(hrs);
      assertNotNull(hri);
      LOG.
        info("Moving " + hri.getRegionNameAsString() + " to " +
        hrs.getServerName() + "; metaServerIndex=" + metaServerIndex);
      admin.move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(hrs.getServerName().toString()));
    }
    // Wait till table region is up on the server that is NOT carrying .META..
    while (true) {
      tableRegionIndex = cluster.getServerWith(hri.getRegionName());
      if (tableRegionIndex != -1 && tableRegionIndex != metaServerIndex) break;
      LOG.debug("Waiting on region move off the .META. server; current index " +
        tableRegionIndex + " and metaServerIndex=" + metaServerIndex);
      Thread.sleep(100);
    }
    // Verify for sure table region is not on same server as .META.
    tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    assertNotSame(metaServerIndex, tableRegionIndex);
    return tableRegionIndex;
  }

  /**
   * Find regionserver other than the one passed.
   * Can't rely on indexes into list of regionservers since crashed servers
   * occupy an index.
   * @param cluster
   * @param notThisOne
   * @return A regionserver that is not <code>notThisOne</code> or null if none
   * found
   */
  private HRegionServer getOtherRegionServer(final MiniHBaseCluster cluster,
      final HRegionServer notThisOne) {
    for (RegionServerThread rst: cluster.getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      if (hrs.getServerName().equals(notThisOne.getServerName())) continue;
      if (hrs.isStopping() || hrs.isStopped()) continue;
      return hrs;
    }
    return null;
  }

  private void printOutRegions(final HRegionServer hrs, final String prefix)
      throws IOException {
    List<HRegionInfo> regions = hrs.getOnlineRegions();
    for (HRegionInfo region: regions) {
      LOG.info(prefix + region.getRegionNameAsString());
    }
  }

  private void waitUntilRegionServerDead() throws InterruptedException {
    // Wait until the master processes the RS shutdown
    while (cluster.getMaster().getClusterStatus().
        getServers().size() == NB_SERVERS) {
      LOG.info("Waiting on server to go down");
      Thread.sleep(100);
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
  
  public static class MockMasterWithoutCatalogJanitor extends HMaster {

    public MockMasterWithoutCatalogJanitor(Configuration conf) throws IOException, KeeperException,
        InterruptedException {
      super(conf);
    }

    protected void startCatalogJanitorChore() {
      LOG.debug("Customised master executed.");
    }
  }
}

