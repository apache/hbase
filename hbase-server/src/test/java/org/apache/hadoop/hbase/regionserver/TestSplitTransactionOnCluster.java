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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TestReplicasClient.SlowMeCopro;
import org.apache.hadoop.hbase.coordination.ZKSplitTransactionCoordination;
import org.apache.hadoop.hbase.coordination.ZkCloseRegionCoordination;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.ZkOpenRegionCoordination;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

/**
 * Like TestSplitTransaction in that we're testing {@link SplitTransactionImpl}
 * only the below tests are against a running cluster where TestSplitTransaction
 * is tests against a bare {@link HRegion}.
 */
@Category(LargeTests.class)
@SuppressWarnings("deprecation")
public class TestSplitTransactionOnCluster {
  private static final Log LOG =
    LogFactory.getLog(TestSplitTransactionOnCluster.class);
  private HBaseAdmin admin = null;
  private MiniHBaseCluster cluster = null;
  private static final int NB_SERVERS = 3;
  private static CountDownLatch latch = new CountDownLatch(1);
  private static volatile boolean secondSplit = false;
  private static volatile boolean callRollBack = false;
  private static volatile boolean firstSplitCompleted = false;
  private static boolean useZKForAssignment;

  static final HBaseTestingUtility TESTING_UTIL =
    new HBaseTestingUtility();

  static void setupOnce() throws Exception {
    TESTING_UTIL.getConfiguration().setInt("hbase.balancer.period", 60000);
    useZKForAssignment = TESTING_UTIL.getConfiguration().getBoolean(
      "hbase.assignment.usezk", true);
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
  }

  @BeforeClass public static void before() throws Exception {
    // Use ZK for region assignment
    TESTING_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", true);
    setupOnce();
  }

  @AfterClass public static void after() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    TESTING_UTIL.ensureSomeNonStoppedRegionServersAvailable(NB_SERVERS);
    this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    this.cluster = TESTING_UTIL.getMiniHBaseCluster();
  }

  @After
  public void tearDown() throws Exception {
    this.admin.close();
  }

  private HRegionInfo getAndCheckSingleTableRegion(final List<HRegion> regions) {
    assertEquals(1, regions.size());
    HRegionInfo hri = regions.get(0).getRegionInfo();
    return waitOnRIT(hri);
  }

  /**
   * Often region has not yet fully opened.  If we try to use it -- do a move for instance -- it
   * will fail silently if the region is not yet opened.
   * @param hri Region to check if in Regions In Transition... wait until out of transition before
   * returning
   * @return Passed in <code>hri</code>
   */
  private HRegionInfo waitOnRIT(final HRegionInfo hri) {
    // Close worked but we are going to open the region elsewhere.  Before going on, make sure
    // this completes.
    while (TESTING_UTIL.getHBaseCluster().getMaster().getAssignmentManager().
        getRegionStates().isRegionInTransition(hri)) {
      LOG.info("Waiting on region in transition: " +
        TESTING_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates().
          getRegionTransitionState(hri));
      Threads.sleep(10);
    }
    return hri;
  }

  @Test(timeout = 60000)
  public void testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack() throws Exception {
    final TableName tableName =
        TableName.valueOf("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack");

    if (!useZKForAssignment) {
      // This test doesn't apply if not using ZK for assignment
      return;
    }

    try {
      // Create table then get the single region for our new table.
      HTable t = createTableAndWait(tableName, Bytes.toBytes("cf"));
      final List<HRegion> regions = cluster.getRegions(tableName);
      HRegionInfo hri = getAndCheckSingleTableRegion(regions);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionInfo()
        .getRegionName());
      final HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      insertData(tableName, admin, t);
      t.close();

      // Turn off balancer so it doesn't cut in and mess up our placements.
      this.admin.setBalancerRunning(false, true);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);

      // find a splittable region
      final HRegion region = findSplittableRegion(regions);
      assertTrue("not able to find a splittable region", region != null);
      MockedCoordinatedStateManager cp = new MockedCoordinatedStateManager();
      cp.initialize(regionServer, region);
      cp.start();
      regionServer.csm = cp;

      new Thread() {
        @Override
        public void run() {
          SplitTransaction st = null;
          st = new MockedSplitTransaction(region, Bytes.toBytes("row2"));
          try {
            st.prepare();
            st.execute(regionServer, regionServer);
          } catch (IOException e) {

          }
        }
      }.start();
      for (int i = 0; !callRollBack && i < 100; i++) {
        Thread.sleep(100);
      }
      assertTrue("Waited too long for rollback", callRollBack);
      SplitTransaction st = new MockedSplitTransaction(region, Bytes.toBytes("row3"));
      try {
        secondSplit = true;
        // make region splittable
        region.initialize();
        st.prepare();
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        LOG.debug("Rollback started :"+ e.getMessage());
        st.rollback(regionServer, regionServer);
      }
      for (int i=0; !firstSplitCompleted && i<100; i++) {
        Thread.sleep(100);
      }
      assertTrue("fist split did not complete", firstSplitCompleted);

      RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
      Map<String, RegionState> rit = regionStates.getRegionsInTransition();

      for (int i=0; rit.containsKey(hri.getTable()) && i<100; i++) {
        Thread.sleep(100);
      }
      assertFalse("region still in transition", rit.containsKey(
          rit.containsKey(hri.getTable())));

      List<Region> onlineRegions = regionServer.getOnlineRegions(tableName);
      // Region server side split is successful.
      assertEquals("The parent region should be splitted", 2, onlineRegions.size());
      //Should be present in RIT
      List<HRegionInfo> regionsOfTable = cluster.getMaster().getAssignmentManager()
          .getRegionStates().getRegionsOfTable(tableName);
      // Master side should also reflect the same
      assertEquals("No of regions in master", 2, regionsOfTable.size());
    } finally {
      admin.setBalancerRunning(true, false);
      secondSplit = false;
      firstSplitCompleted = false;
      callRollBack = false;
      cluster.getMaster().setCatalogJanitorEnabled(true);
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  @Test(timeout = 60000)
  public void testRITStateForRollback() throws Exception {
    final TableName tableName =
        TableName.valueOf("testRITStateForRollback");
    try {
      // Create table then get the single region for our new table.
      Table t = createTableAndWait(tableName, Bytes.toBytes("cf"));
      final List<HRegion> regions = cluster.getRegions(tableName);
      final HRegionInfo hri = getAndCheckSingleTableRegion(regions);
      insertData(tableName, admin, t);
      t.close();

      // Turn off balancer so it doesn't cut in and mess up our placements.
      this.admin.setBalancerRunning(false, true);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);

      // find a splittable region
      final HRegion region = findSplittableRegion(regions);
      assertTrue("not able to find a splittable region", region != null);

      // install region co-processor to fail splits
      region.getCoprocessorHost().load(FailingSplitRegionObserver.class,
        Coprocessor.PRIORITY_USER, region.getBaseConf());

      // split async
      this.admin.split(region.getRegionInfo().getRegionName(), new byte[] {42});

      // we have to wait until the SPLITTING state is seen by the master
      FailingSplitRegionObserver observer = (FailingSplitRegionObserver) region
          .getCoprocessorHost().findCoprocessor(FailingSplitRegionObserver.class.getName());
      assertNotNull(observer);
      observer.latch.await();

      LOG.info("Waiting for region to come out of RIT");
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          Map<String, RegionState> rit = regionStates.getRegionsInTransition();
          return (rit.size() == 0);
        }
      });
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      TESTING_UTIL.deleteTable(tableName);
    }
  }
  @Test(timeout = 60000)
  public void testSplitFailedCompactionAndSplit() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitFailedCompactionAndSplit");
    Configuration conf = TESTING_UTIL.getConfiguration();
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      byte[] cf = Bytes.toBytes("cf");
      htd.addFamily(new HColumnDescriptor(cf));
      admin.createTable(htd);

      for (int i = 0; cluster.getRegions(tableName).size() == 0 && i < 100; i++) {
        Thread.sleep(100);
      }
      assertEquals(1, cluster.getRegions(tableName).size());

      HRegion region = cluster.getRegions(tableName).get(0);
      Store store = region.getStore(cf);
      int regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);

      Table t = new HTable(conf, tableName);
      // insert data
      insertData(tableName, admin, t);
      insertData(tableName, admin, t);

      int fileNum = store.getStorefiles().size();
      // 0, Compaction Request
      store.triggerMajorCompaction();
      CompactionContext cc = store.requestCompaction();
      assertNotNull(cc);
      // 1, A timeout split
      // 1.1 close region
      assertEquals(2, region.close(false).get(cf).size());
      // 1.2 rollback and Region initialize again
      region.initialize();

      // 2, Run Compaction cc
      assertFalse(region.compact(cc, store, NoLimitCompactionThroughputController.INSTANCE));
      assertTrue(fileNum > store.getStorefiles().size());

      // 3, Split
      SplitTransaction st = new SplitTransactionImpl(region, Bytes.toBytes("row3"));
      assertTrue(st.prepare());
      st.execute(regionServer, regionServer);
      LOG.info("Waiting for region to come out of RIT");
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          Map<String, RegionState> rit = regionStates.getRegionsInTransition();
          return (rit.size() == 0);
        }
      });
      assertEquals(2, cluster.getRegions(tableName).size());
    } finally {
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  public static class FailingSplitRegionObserver extends BaseRegionObserver {
    volatile CountDownLatch latch;
    volatile CountDownLatch postSplit;
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      latch = new CountDownLatch(1);
      postSplit = new CountDownLatch(1);
    }
    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx,
        byte[] splitKey, List<Mutation> metaEntries) throws IOException {
      latch.countDown();
      LOG.info("Causing rollback of region split");
      throw new IOException("Causing rollback of region split");
    }
    @Override
    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
        throws IOException {
      postSplit.countDown();
      LOG.info("postCompleteSplit called");
    }
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
   * @throws DeserializationException
   */
  @Test (timeout = 300000) public void testRSSplitEphemeralsDisappearButDaughtersAreOnlinedAfterShutdownHandling()
  throws IOException, InterruptedException, NodeExistsException, KeeperException,
      DeserializationException, ServiceException {
    final TableName tableName =
      TableName.valueOf("testRSSplitEphemeralsDisappearButDaughtersAreOnlinedAfterShutdownHandling");

    // Create table then get the single region for our new table.
    HTable t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY, false);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      AssignmentManager.TEST_SKIP_SPLIT_HANDLING = true;
      // Now try splitting and it should work.
      split(hri, server, regionCount);

      String path = ZKAssign.getNodeName(TESTING_UTIL.getZooKeeperWatcher(),
        hri.getEncodedName());
      RegionTransition rt = null;
      Stat stats = null;
      List<HRegion> daughters = null;
      if (useZKForAssignment) {
        daughters = checkAndGetDaughters(tableName);

        // Wait till the znode moved to SPLIT
        for (int i=0; i<100; i++) {
          stats = TESTING_UTIL.getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
          rt = RegionTransition.parseFrom(ZKAssign.getData(TESTING_UTIL.getZooKeeperWatcher(),
            hri.getEncodedName()));
          if (rt.getEventType().equals(EventType.RS_ZK_REGION_SPLIT)) break;
          Thread.sleep(100);
        }
        LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats=" + stats);
        assertTrue(rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_SPLIT));
        // Now crash the server, for ZK-less assignment, the server is auto aborted
        cluster.abortRegionServer(tableRegionIndex);
      }
      waitUntilRegionServerDead();
      awaitDaughters(tableName, 2);
      if (useZKForAssignment) {
        regions = cluster.getRegions(tableName);
        for (HRegion r: regions) {
          assertTrue(daughters.contains(r));
        }

        // Finally assert that the ephemeral SPLIT znode was cleaned up.
        for (int i=0; i<100; i++) {
          // wait a bit (10s max) for the node to disappear
          stats = TESTING_UTIL.getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
          if (stats == null) break;
          Thread.sleep(100);
        }
        LOG.info("EPHEMERAL NODE AFTER SERVER ABORT, path=" + path + ", stats=" + stats);
        assertTrue(stats == null);
      }
    } finally {
      // Set this flag back.
      AssignmentManager.TEST_SKIP_SPLIT_HANDLING = false;
      cluster.getMaster().getAssignmentManager().regionOffline(hri);
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      cluster.startRegionServer();
      t.close();
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  @Test (timeout = 300000) public void testExistingZnodeBlocksSplitAndWeRollback()
  throws IOException, InterruptedException, NodeExistsException, KeeperException, ServiceException {
    final TableName tableName =
        TableName.valueOf("testExistingZnodeBlocksSplitAndWeRollback");

    // Create table then get the single region for our new table.
    HTable t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY, false);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
      // Insert into zk a blocking znode, a znode of same name as region
      // so it gets in way of our splitting.
      ServerName fakedServer = ServerName.valueOf("any.old.server", 1234, -1);
      if (useZKForAssignment) {
        ZKAssign.createNodeClosing(TESTING_UTIL.getZooKeeperWatcher(),
          hri, fakedServer);
      } else {
        regionStates.updateRegionState(hri, RegionState.State.CLOSING);
      }
      // Now try splitting.... should fail.  And each should successfully
      // rollback.
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      // Wait around a while and assert count of regions remains constant.
      for (int i = 0; i < 10; i++) {
        Thread.sleep(100);
        assertEquals(regionCount, ProtobufUtil.getOnlineRegions(
          server.getRSRpcServices()).size());
      }
      if (useZKForAssignment) {
        // Now clear the zknode
        ZKAssign.deleteClosingNode(TESTING_UTIL.getZooKeeperWatcher(),
          hri, fakedServer);
      } else {
        regionStates.regionOnline(hri, server.getServerName());
      }
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      checkAndGetDaughters(tableName);
      // OK, so split happened after we cleared the blocking node.
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
    }
  }

  /**
   * Test that if daughter split on us, we won't do the shutdown handler fixup
   * just because we can't find the immediate daughter of an offlined parent.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000) public void testShutdownFixupWhenDaughterHasSplit()
  throws IOException, InterruptedException, ServiceException {
    final TableName tableName =
        TableName.valueOf("testShutdownFixupWhenDaughterHasSplit");

    // Create table then get the single region for our new table.
    HTable t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
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
      int regionCount = ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
      // Now split.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = checkAndGetDaughters(tableName);
      // Now split one of the daughters.
      regionCount = ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
      HRegionInfo daughter = daughters.get(0).getRegionInfo();
      LOG.info("Daughter we are going to split: " + daughter);
      // Compact first to ensure we have cleaned up references -- else the split
      // will fail.
      this.admin.compact(daughter.getRegionName());
      daughters = cluster.getRegions(tableName);
      HRegion daughterRegion = null;
      for (HRegion r: daughters) {
        if (r.getRegionInfo().equals(daughter)) {
          daughterRegion = r;
          LOG.info("Found matching HRI: " + daughterRegion);
          break;
        }
      }
      assertTrue(daughterRegion != null);
      for (int i=0; i<100; i++) {
        if (!daughterRegion.hasReferences()) break;
        Threads.sleep(100);
      }
      assertFalse("Waiting for reference to be compacted", daughterRegion.hasReferences());
      LOG.info("Daughter hri before split (has been compacted): " + daughter);
      split(daughter, server, regionCount);
      // Get list of daughters
      daughters = cluster.getRegions(tableName);
      for (HRegion d: daughters) {
        LOG.info("Regions before crash: " + d);
      }
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      awaitDaughters(tableName, daughters.size());
      // Assert daughters are online and ONLY the original daughters -- that
      // fixup didn't insert one during server shutdown recover.
      regions = cluster.getRegions(tableName);
      for (HRegion d: daughters) {
        LOG.info("Regions after crash: " + d);
      }
      assertEquals(daughters.size(), regions.size());
      for (HRegion r: regions) {
        LOG.info("Regions post crash " + r);
        assertTrue("Missing region post crash " + r, daughters.contains(r));
      }
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
    }
  }

  @Test(timeout = 180000)
  public void testSplitShouldNotThrowNPEEvenARegionHasEmptySplitFiles() throws Exception {
    TableName userTableName =
        TableName.valueOf("testSplitShouldNotThrowNPEEvenARegionHasEmptySplitFiles");
    HTableDescriptor htd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    htd.addFamily(hcd);
    admin.createTable(htd);
    Table table = new HTable(TESTING_UTIL.getConfiguration(), userTableName);
    try {
      for (int i = 0; i <= 5; i++) {
        String row = "row" + i;
        Put p = new Put(row.getBytes());
        String val = "Val" + i;
        p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
        table.put(p);
        admin.flush(userTableName.getName());
        Delete d = new Delete(row.getBytes());
        // Do a normal delete
        table.delete(d);
        admin.flush(userTableName.getName());
      }
      admin.majorCompact(userTableName.getName());
      List<HRegionInfo> regionsOfTable = TESTING_UTIL.getMiniHBaseCluster()
          .getMaster().getAssignmentManager().getRegionStates()
          .getRegionsOfTable(userTableName);
      HRegionInfo hRegionInfo = regionsOfTable.get(0);
      Put p = new Put("row6".getBytes());
      p.add("col".getBytes(), "ql".getBytes(), "val".getBytes());
      table.put(p);
      p = new Put("row7".getBytes());
      p.add("col".getBytes(), "ql".getBytes(), "val".getBytes());
      table.put(p);
      p = new Put("row8".getBytes());
      p.add("col".getBytes(), "ql".getBytes(), "val".getBytes());
      table.put(p);
      admin.flush(userTableName.getName());
      admin.split(hRegionInfo.getRegionName(), "row7".getBytes());
      regionsOfTable = TESTING_UTIL.getMiniHBaseCluster().getMaster()
          .getAssignmentManager().getRegionStates()
          .getRegionsOfTable(userTableName);

      while (regionsOfTable.size() != 2) {
        Thread.sleep(2000);
        regionsOfTable = TESTING_UTIL.getMiniHBaseCluster().getMaster()
            .getAssignmentManager().getRegionStates()
            .getRegionsOfTable(userTableName);
      }
      Assert.assertEquals(2, regionsOfTable.size());
      Scan s = new Scan();
      ResultScanner scanner = table.getScanner(s);
      int mainTableCount = 0;
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        mainTableCount++;
      }
      Assert.assertEquals(3, mainTableCount);
    } finally {
      table.close();
    }
  }

  /**
   * Noop Abortable implementation used below in tests.
   */
  static class UselessTestAbortable implements Abortable {
    boolean aborted = false;
    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("ABORTED (But nothing to abort): why=" + why, e);
      aborted = true;
    }

    @Override
    public boolean isAborted() {
      return this.aborted;
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
   * @throws DeserializationException
   */
  @Test(timeout = 400000)
  public void testMasterRestartWhenSplittingIsPartial()
      throws IOException, InterruptedException, NodeExistsException,
      KeeperException, DeserializationException, ServiceException {
    final TableName tableName = TableName.valueOf("testMasterRestartWhenSplittingIsPartial");

    if (!useZKForAssignment) {
      // This test doesn't apply if not using ZK for assignment
      return;
    }

    // Create table then get the single region for our new table.
    HTable t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(t.getConfiguration(),
      "testMasterRestartWhenSplittingIsPartial", new UselessTestAbortable());
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY, false);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      AssignmentManager.TEST_SKIP_SPLIT_HANDLING = true;
      // Now try splitting and it should work.

      this.admin.split(hri.getRegionNameAsString());
      checkAndGetDaughters(tableName);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(zkw, hri.getEncodedName());
      Stat stats = zkw.getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      byte[] bytes = ZKAssign.getData(zkw, hri.getEncodedName());
      RegionTransition rtd = RegionTransition.parseFrom(bytes);
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLIT)
          || rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLITTING));

      // abort and wait for new master.
      MockMasterWithoutCatalogJanitor master = abortAndWaitForMaster();

      this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());

      // Update the region to be offline and split, so that HRegionInfo#equals
      // returns true in checking rebuilt region states map.
      hri.setOffline(true);
      hri.setSplit(true);
      ServerName regionServerOfRegion = master.getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion != null);

      // Remove the block so that split can move ahead.
      AssignmentManager.TEST_SKIP_SPLIT_HANDLING = false;
      String node = ZKAssign.getNodeName(zkw, hri.getEncodedName());
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(zkw, node, stat);
      // ZKUtil.create
      for (int i=0; data != null && i<60; i++) {
        Thread.sleep(1000);
        data = ZKUtil.getDataNoWatch(zkw, node, stat);
      }
      assertNull("Waited too long for ZK node to be removed: "+node, data);
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      assertTrue("Split parent should be in SPLIT state",
        regionStates.isRegionInState(hri, State.SPLIT));
      regionServerOfRegion = regionStates.getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion == null);
    } finally {
      // Set this flag back.
      AssignmentManager.TEST_SKIP_SPLIT_HANDLING = false;
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
      zkw.close();
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
      KeeperException, ServiceException {
    final TableName tableName = TableName
        .valueOf("testMasterRestartAtRegionSplitPendingCatalogJanitor");

    // Create table then get the single region for our new table.
    HTable t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(t.getConfiguration(),
      "testMasterRestartAtRegionSplitPendingCatalogJanitor", new UselessTestAbortable());
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY, false);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");

      this.admin.split(hri.getRegionNameAsString());
      checkAndGetDaughters(tableName);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(zkw, hri.getEncodedName());
      Stat stats = zkw.getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      String node = ZKAssign.getNodeName(zkw, hri.getEncodedName());
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(zkw, node, stat);
      // ZKUtil.create
      for (int i=0; data != null && i<60; i++) {
        Thread.sleep(1000);
        data = ZKUtil.getDataNoWatch(zkw, node, stat);
      }
      assertNull("Waited too long for ZK node to be removed: "+node, data);

      MockMasterWithoutCatalogJanitor master = abortAndWaitForMaster();

      this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());

      // Update the region to be offline and split, so that HRegionInfo#equals
      // returns true in checking rebuilt region states map.
      hri.setOffline(true);
      hri.setSplit(true);
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      assertTrue("Split parent should be in SPLIT state",
        regionStates.isRegionInState(hri, State.SPLIT));
      ServerName regionServerOfRegion = regionStates.getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion == null);
    } finally {
      this.admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
      zkw.close();
    }
  }

  /**
   *
   * While transitioning node from RS_ZK_REGION_SPLITTING to
   * RS_ZK_REGION_SPLITTING during region split,if zookeper went down split always
   * fails for the region. HBASE-6088 fixes this scenario.
   * This test case is to test the znode is deleted(if created) or not in roll back.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test(timeout = 60000)
  public void testSplitBeforeSettingSplittingInZK() throws Exception,
      InterruptedException, KeeperException {
    testSplitBeforeSettingSplittingInZKInternals();
  }

  @Test(timeout = 60000)
  public void testTableExistsIfTheSpecifiedTableRegionIsSplitParent() throws Exception {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TESTING_UTIL);
    final TableName tableName =
        TableName.valueOf("testTableExistsIfTheSpecifiedTableRegionIsSplitParent");
    // Create table then get the single region for our new table.
    Table t = createTableAndWait(tableName, Bytes.toBytes("cf"));
    List<HRegion> regions = null;
    try {
      regions = cluster.getRegions(tableName);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionInfo()
        .getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      insertData(tableName, admin, t);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      admin.setBalancerRunning(false, true);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);
      boolean tableExists = MetaTableAccessor.tableExists(regionServer.getConnection(),
        tableName);
      assertEquals("The specified table should present.", true, tableExists);
      final HRegion region = findSplittableRegion(regions);
      assertTrue("not able to find a splittable region", region != null);
      SplitTransactionImpl st = new SplitTransactionImpl(region, Bytes.toBytes("row2"));
      try {
        st.prepare();
        st.createDaughters(regionServer, regionServer, null);
      } catch (IOException e) {

      }
      tableExists = MetaTableAccessor.tableExists(regionServer.getConnection(),
        tableName);
      assertEquals("The specified table should present.", true, tableExists);
      Map<String, RegionState> rit = cluster.getMaster().getAssignmentManager().getRegionStates()
          .getRegionsInTransition();
      assertTrue(rit.size() == 3);
      cluster.getMaster().getAssignmentManager().regionOffline(st.getFirstDaughter());
      cluster.getMaster().getAssignmentManager().regionOffline(st.getSecondDaughter());
      cluster.getMaster().getAssignmentManager().regionOffline(region.getRegionInfo());
      rit = cluster.getMaster().getAssignmentManager().getRegionStates().getRegionsInTransition();
      assertTrue(rit.size() == 0);
    }
    finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitWithRegionReplicas() throws Exception {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TESTING_UTIL);
    final TableName tableName =
        TableName.valueOf("foobar");
    HTableDescriptor htd = TESTING_UTIL.createTableDescriptor("foobar");
    htd.setRegionReplication(2);
    htd.addCoprocessor(SlowMeCopro.class.getName());
    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(htd, new byte[][]{Bytes.toBytes("cf")},
        TESTING_UTIL.getConfiguration());
    int count;
    List<HRegion> oldRegions;
    do {
      oldRegions = cluster.getRegions(tableName);
      Thread.sleep(10);
    } while (oldRegions.size() != 2);
    for (HRegion h : oldRegions) LOG.debug("OLDREGION " + h.getRegionInfo());
    try {
      int regionServerIndex = cluster.getServerWith(oldRegions.get(0).getRegionInfo()
        .getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      insertData(tableName, admin, t);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      admin.setBalancerRunning(false, true);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);
      boolean tableExists = MetaTableAccessor.tableExists(regionServer.getConnection(),
          tableName);
      assertEquals("The specified table should be present.", true, tableExists);
      final HRegion region = findSplittableRegion(oldRegions);
      regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
      regionServer = cluster.getRegionServer(regionServerIndex);
      assertTrue("not able to find a splittable region", region != null);
      String node = ZKAssign.getNodeName(regionServer.getZooKeeper(),
          region.getRegionInfo().getEncodedName());
      regionServer.getZooKeeper().sync(node);
      SplitTransactionImpl st = new SplitTransactionImpl(region, Bytes.toBytes("row2"));
      try {
        st.prepare();
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        e.printStackTrace();
        fail("Split execution should have succeeded with no exceptions thrown " + e);
      }
      //TESTING_UTIL.waitUntilAllRegionsAssigned(tableName);
      List<HRegion> newRegions;
      do {
        newRegions = cluster.getRegions(tableName);
        for (HRegion h : newRegions) LOG.debug("NEWREGION " + h.getRegionInfo());
        Thread.sleep(1000);
      } while ((newRegions.contains(oldRegions.get(0)) || newRegions.contains(oldRegions.get(1)))
          || newRegions.size() != 4);
      tableExists = MetaTableAccessor.tableExists(regionServer.getConnection(),
          tableName);
      assertEquals("The specified table should be present.", true, tableExists);
      // exists works on stale and we see the put after the flush
      byte[] b1 = "row1".getBytes();
      Get g = new Get(b1);
      g.setConsistency(Consistency.STRONG);
      // The following GET will make a trip to the meta to get the new location of the 1st daughter
      // In the process it will also get the location of the replica of the daughter (initially
      // pointing to the parent's replica)
      Result r = t.get(g);
      Assert.assertFalse(r.isStale());
      LOG.info("exists stale after flush done");

      SlowMeCopro.getCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      // This will succeed because in the previous GET we get the location of the replica
      r = t.get(g);
      Assert.assertTrue(r.isStale());
      SlowMeCopro.getCdl().get().countDown();
    } finally {
      SlowMeCopro.getCdl().get().countDown();
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
    }
  }

  private void insertData(final TableName tableName, HBaseAdmin admin, Table t) throws IOException,
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

  /**
   * If a table has regions that have no store files in a region, they should split successfully
   * into two regions with no store files.
   */
  @Test(timeout = 60000)
  public void testSplitRegionWithNoStoreFiles()
      throws Exception {
    final TableName tableName =
        TableName.valueOf("testSplitRegionWithNoStoreFiles");
    // Create table then get the single region for our new table.
    createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);
    ensureTableRegionNotOnSameServerAsMeta(admin, hri);
    int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionInfo()
      .getRegionName());
    HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Precondition: we created a table with no data, no store files.
      printOutRegions(regionServer, "Initial regions: ");
      Configuration conf = cluster.getConfiguration();
      HBaseFsck.debugLsr(conf, new Path("/"));
      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = TESTING_UTIL.getDFSCluster().getFileSystem();
      Map<String, Path> storefiles =
          FSUtils.getTableStoreFilePathMap(null, fs, rootDir, tableName);
      assertEquals("Expected nothing but found " + storefiles.toString(), storefiles.size(), 0);

      // find a splittable region.  Refresh the regions list
      regions = cluster.getRegions(tableName);
      final HRegion region = findSplittableRegion(regions);
      assertTrue("not able to find a splittable region", region != null);

      // Now split.
      SplitTransactionImpl st = new MockedSplitTransaction(region, Bytes.toBytes("row2"));
      try {
        st.prepare();
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        fail("Split execution should have succeeded with no exceptions thrown");
      }

      // Postcondition: split the table with no store files into two regions, but still have not
      // store files
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertTrue(daughters.size() == 2);

      // check dirs
      HBaseFsck.debugLsr(conf, new Path("/"));
      Map<String, Path> storefilesAfter =
          FSUtils.getTableStoreFilePathMap(null, fs, rootDir, tableName);
      assertEquals("Expected nothing but found " + storefilesAfter.toString(),
          storefilesAfter.size(), 0);

      hri = region.getRegionInfo(); // split parent
      AssignmentManager am = cluster.getMaster().getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      long start = EnvironmentEdgeManager.currentTime();
      while (!regionStates.isRegionInState(hri, State.SPLIT)) {
        assertFalse("Timed out in waiting split parent to be in state SPLIT",
          EnvironmentEdgeManager.currentTime() - start > 60000);
        Thread.sleep(500);
      }

      // We should not be able to assign it again
      am.assign(hri, true, true);
      assertFalse("Split region can't be assigned",
        regionStates.isRegionInTransition(hri));
      assertTrue(regionStates.isRegionInState(hri, State.SPLIT));

      // We should not be able to unassign it either
      am.unassign(hri, true, null);
      assertFalse("Split region can't be unassigned",
        regionStates.isRegionInTransition(hri));
      assertTrue(regionStates.isRegionInState(hri, State.SPLIT));
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  @Test(timeout = 180000)
  public void testSplitHooksBeforeAndAfterPONR() throws Exception {
    TableName firstTable = TableName.valueOf("testSplitHooksBeforeAndAfterPONR_1");
    TableName secondTable = TableName.valueOf("testSplitHooksBeforeAndAfterPONR_2");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");

    HTableDescriptor desc = new HTableDescriptor(firstTable);
    desc.addCoprocessor(MockedRegionObserver.class.getName());
    desc.addFamily(hcd);
    admin.createTable(desc);
    TESTING_UTIL.waitUntilAllRegionsAssigned(firstTable);

    desc = new HTableDescriptor(secondTable);
    desc.addFamily(hcd);
    admin.createTable(desc);
    TESTING_UTIL.waitUntilAllRegionsAssigned(secondTable);

    List<HRegion> firstTableRegions = cluster.getRegions(firstTable);
    List<HRegion> secondTableRegions = cluster.getRegions(secondTable);

    // Check that both tables actually have regions.
    if (firstTableRegions.size() == 0 || secondTableRegions.size() == 0) {
      fail("Each table should have at least one region.");
    }
    ServerName serverName = cluster.getServerHoldingRegion(firstTable,
      firstTableRegions.get(0).getRegionInfo().getRegionName());
    admin.move(secondTableRegions.get(0).getRegionInfo().getEncodedNameAsBytes(),
      Bytes.toBytes(serverName.getServerName()));
    Table table1 = null;
    Table table2 = null;
    try {
      table1 = new HTable(TESTING_UTIL.getConfiguration(), firstTable);
      table2 = new HTable(TESTING_UTIL.getConfiguration(), firstTable);
      insertData(firstTable, admin, table1);
      insertData(secondTable, admin, table2);
      admin.split(firstTable, "row2".getBytes());
      firstTableRegions = cluster.getRegions(firstTable);
      while (firstTableRegions.size() != 2) {
        Thread.sleep(1000);
        firstTableRegions = cluster.getRegions(firstTable);
      }
      assertEquals("Number of regions after split should be 2.", 2, firstTableRegions.size());
      secondTableRegions = cluster.getRegions(secondTable);
      assertEquals("Number of regions after split should be 2.", 2, secondTableRegions.size());
    } finally {
      if (table1 != null) {
        table1.close();
      }
      if (table2 != null) {
        table2.close();
      }
      TESTING_UTIL.deleteTable(firstTable);
      TESTING_UTIL.deleteTable(secondTable);
    }
  }

  private void testSplitBeforeSettingSplittingInZKInternals() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitBeforeSettingSplittingInZK");
    try {
      // Create table then get the single region for our new table.
      createTableAndWait(tableName, Bytes.toBytes("cf"));

      List<HRegion> regions = awaitTableRegions(tableName);
      assertTrue("Table not online", cluster.getRegions(tableName).size() != 0);

      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionInfo()
        .getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      final HRegion region = findSplittableRegion(regions);
      assertTrue("not able to find a splittable region", region != null);
      SplitTransactionImpl st = new MockedSplitTransaction(region, Bytes.toBytes("row2")) {
        @Override
        public PairOfSameType<Region> stepsBeforePONR(final Server server,
            final RegionServerServices services, boolean testing) throws IOException {
          throw new SplittingNodeCreationFailedException ();
        }
      };
      String node = ZKAssign.getNodeName(regionServer.getZooKeeper(),
          region.getRegionInfo().getEncodedName());
      regionServer.getZooKeeper().sync(node);
      for (int i = 0; i < 100; i++) {
        // We expect the znode to be deleted by this time. Here the
        // znode could be in OPENED state and the
        // master has not yet deleted the znode.
        if (ZKUtil.checkExists(regionServer.getZooKeeper(), node) != -1) {
          Thread.sleep(100);
        }
      }
      try {
        st.prepare();
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        // check for the specific instance in case the Split failed due to the
        // existence of the znode in OPENED state.
        // This will at least make the test to fail;
        assertTrue("Should be instance of CreateSplittingNodeFailedException",
            e instanceof SplittingNodeCreationFailedException );
        node = ZKAssign.getNodeName(regionServer.getZooKeeper(),
            region.getRegionInfo().getEncodedName());
        {
          assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        }
        assertTrue(st.rollback(regionServer, regionServer));
        assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
      }
    } finally {
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testStoreFileReferenceCreationWhenSplitPolicySaysToSkipRangeCheck()
      throws Exception {
    final TableName tableName =
        TableName.valueOf("testStoreFileReferenceCreationWhenSplitPolicySaysToSkipRangeCheck");
    try {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("f"));
      htd.addFamily(new HColumnDescriptor("i_f"));
      htd.setRegionSplitPolicyClassName(CustomSplitPolicy.class.getName());
      admin.createTable(htd);
      List<HRegion> regions = awaitTableRegions(tableName);
      HRegion region = regions.get(0);
      for(int i = 3;i<9;i++) {
        Put p = new Put(Bytes.toBytes("row"+i));
        p.add(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("value"+i));
        p.add(Bytes.toBytes("i_f"), Bytes.toBytes("q"), Bytes.toBytes("value"+i));
        region.put(p);
      }
      region.flush(true);
      Store store = region.getStore(Bytes.toBytes("f"));
      Collection<StoreFile> storefiles = store.getStorefiles();
      assertEquals(storefiles.size(), 1);
      assertFalse(region.hasReferences());
      Path referencePath =
          region.getRegionFileSystem().splitStoreFile(region.getRegionInfo(), "f",
            storefiles.iterator().next(), Bytes.toBytes("row1"), false, region.getSplitPolicy());
      assertNull(referencePath);
      referencePath =
          region.getRegionFileSystem().splitStoreFile(region.getRegionInfo(), "i_f",
            storefiles.iterator().next(), Bytes.toBytes("row1"), false, region.getSplitPolicy());
      assertNotNull(referencePath);
    } finally {
      TESTING_UTIL.deleteTable(tableName);
    }
  }
  
  @Test(timeout = 120000)
  public void testFailedSplit() throws Exception {
    TableName tableName = TableName.valueOf("testFailedSplit");
    byte[] colFamily = Bytes.toBytes("info");
    TESTING_UTIL.createTable(tableName, colFamily);
    Connection connection = ConnectionFactory.createConnection(TESTING_UTIL.getConfiguration());
    HTable table = (HTable) connection.getTable(tableName);
    try {
      TESTING_UTIL.loadTable(table, colFamily);
      List<HRegionInfo> regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      final HRegion actualRegion = cluster.getRegions(tableName).get(0);
      actualRegion.getCoprocessorHost().load(FailingSplitRegionObserver.class,
        Coprocessor.PRIORITY_USER, actualRegion.getBaseConf());

      // The following split would fail.
      admin.split(tableName);
      FailingSplitRegionObserver observer = (FailingSplitRegionObserver) actualRegion
          .getCoprocessorHost().findCoprocessor(FailingSplitRegionObserver.class.getName());
      assertNotNull(observer);
      observer.latch.await();
      observer.postSplit.await();
      LOG.info("Waiting for region to come out of RIT: " + actualRegion);
      TESTING_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
          Map<String, RegionState> rit = regionStates.getRegionsInTransition();
          return (rit.size() == 0);
        }
      });
      regions = TESTING_UTIL.getHBaseAdmin().getTableRegions(tableName);
      assertTrue(regions.size() == 1);
      RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
      Map<String, RegionState> rit = regionStates.getRegionsInTransition();
      assertTrue(rit.size() == 0);
    } finally {
      table.close();
      connection.close();
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  @Test (timeout=300000)
  public void testSSHCleanupDaugtherRegionsOfAbortedSplit() throws Exception {
    TableName table = TableName.valueOf("testSSHCleanupDaugtherRegionsOfAbortedSplit");
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      admin.createTable(desc);
      HTable hTable = new HTable(cluster.getConfiguration(), desc.getTableName());
      for(int i = 1; i < 5; i++) {
        Put p1 = new Put(("r"+i).getBytes());
        p1.add(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        hTable.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      SplitTransactionImpl st = new SplitTransactionImpl(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      Path tableDir =
          FSUtils.getTableDir(cluster.getMaster().getMasterFileSystem().getRootDir(),
            desc.getTableName());
      tableDir.getFileSystem(cluster.getConfiguration());
      List<Path> regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(3,regionDirs.size());
      cluster.startRegionServer();
      regionServer.kill();
      cluster.getRegionServerThreads().get(serverWith).join();
      // Wait until finish processing of shutdown
      while (cluster.getMaster().getServerManager().areDeadServersInProgress()) {
        Thread.sleep(10);
      }
      AssignmentManager am = cluster.getMaster().getAssignmentManager();
      while(am.getRegionStates().isRegionsInTransition()) {
        Thread.sleep(10);
      }
      assertEquals(am.getRegionStates().getRegionsInTransition().toString(), 0, am
          .getRegionStates().getRegionsInTransition().size());
      regionDirs =
          FSUtils.getRegionDirs(tableDir.getFileSystem(cluster.getConfiguration()), tableDir);
      assertEquals(1,regionDirs.size());
    } finally {
      TESTING_UTIL.deleteTable(table);
    }
  }

    public static class MockedCoordinatedStateManager extends ZkCoordinatedStateManager {

        public void initialize(Server server, HRegion region) {
          this.server = server;
          this.watcher = server.getZooKeeper();
          splitTransactionCoordination = new MockedSplitTransactionCoordination(this, watcher, region);
          closeRegionCoordination = new ZkCloseRegionCoordination(this, watcher);
          openRegionCoordination = new ZkOpenRegionCoordination(this, watcher);
        }
      }

      public static class MockedSplitTransaction extends SplitTransactionImpl {

        private HRegion currentRegion;
        public MockedSplitTransaction(HRegion region, byte[] splitrow) {
          super(region, splitrow);
          this.currentRegion = region;
        }
        @Override
        public boolean rollback(Server server, RegionServerServices services) throws IOException {
          if (this.currentRegion.getRegionInfo().getTable().getNameAsString()
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

  public static class MockedSplitTransactionCoordination extends ZKSplitTransactionCoordination {

    private HRegion currentRegion;

    public MockedSplitTransactionCoordination(CoordinatedStateManager coordinationProvider,
        ZooKeeperWatcher watcher, HRegion region) {
      super(coordinationProvider, watcher);
      currentRegion = region;
    }

    @Override
    public void completeSplitTransaction(RegionServerServices services, Region a, Region b,
        SplitTransactionDetails std, Region parent) throws IOException {
      if (this.currentRegion.getRegionInfo().getTable().getNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        try {
          if (!secondSplit){
            callRollBack = true;
            latch.await();
          }
        } catch (InterruptedException e) {
        }

      }
      super.completeSplitTransaction(services, a, b, std, parent);
      if (this.currentRegion.getRegionInfo().getTable().getNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        firstSplitCompleted = true;
      }
    }
  }

  private HRegion findSplittableRegion(final List<HRegion> regions) throws InterruptedException {
    for (int i = 0; i < 5; ++i) {
      for (HRegion r: regions) {
        if (r.isSplittable() && r.getRegionInfo().getReplicaId() == 0) {
          return(r);
        }
      }
      Thread.sleep(100);
    }
    return(null);
  }

  private List<HRegion> checkAndGetDaughters(TableName tableName)
      throws InterruptedException {
    List<HRegion> daughters = null;
    // try up to 10s
    for (int i=0; i<100; i++) {
      daughters = cluster.getRegions(tableName);
      if (daughters.size() >= 2) break;
      Thread.sleep(100);
    }
    assertTrue(daughters.size() >= 2);
    return daughters;
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

  private void split(final HRegionInfo hri, final HRegionServer server, final int regionCount)
      throws IOException, InterruptedException {
    this.admin.split(hri.getRegionNameAsString());
    try {
      for (int i = 0; ProtobufUtil.getOnlineRegions(
          server.getRSRpcServices()).size() <= regionCount && i < 300; i++) {
        LOG.debug("Waiting on region to split");
        Thread.sleep(100);
      }

      assertFalse("Waited too long for split",
        ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size() <= regionCount);
    } catch (RegionServerStoppedException e) {
      if (useZKForAssignment) {
        // If not using ZK for assignment, the exception may be expected.
        LOG.error(e);
        throw e;
      }
    }
  }

  /**
   * Ensure single table region is not on same server as the single hbase:meta table
   * region.
   * @param admin
   * @param hri
   * @return Index of the server hosting the single table region
   * @throws UnknownRegionException
   * @throws MasterNotRunningException
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   * @throws InterruptedException
   */
  private int ensureTableRegionNotOnSameServerAsMeta(final Admin admin,
      final HRegionInfo hri)
  throws IOException, MasterNotRunningException,
  ZooKeeperConnectionException, InterruptedException {
    // Now make sure that the table region is not on same server as that hosting
    // hbase:meta  We don't want hbase:meta replay polluting our test when we later crash
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
      LOG.info("Moving " + hri.getRegionNameAsString() + " from " +
        metaRegionServer.getServerName() + " to " +
        hrs.getServerName() + "; metaServerIndex=" + metaServerIndex);
      admin.move(hri.getEncodedNameAsBytes(), Bytes.toBytes(hrs.getServerName().toString()));
    }
    // Wait till table region is up on the server that is NOT carrying hbase:meta.
    for (int i = 0; i < 20; i++) {
      tableRegionIndex = cluster.getServerWith(hri.getRegionName());
      if (tableRegionIndex != -1 && tableRegionIndex != metaServerIndex) break;
      LOG.debug("Waiting on region move off the hbase:meta server; current index " +
        tableRegionIndex + " and metaServerIndex=" + metaServerIndex);
      Thread.sleep(1000);
    }
    assertTrue("Region not moved off hbase:meta server", tableRegionIndex != -1
        && tableRegionIndex != metaServerIndex);
    // Verify for sure table region is not on same server as hbase:meta
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
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    for (HRegionInfo region: regions) {
      LOG.info(prefix + region.getRegionNameAsString());
    }
  }

  private void waitUntilRegionServerDead() throws InterruptedException, InterruptedIOException {
    // Wait until the master processes the RS shutdown
    for (int i=0; cluster.getMaster().getClusterStatus().
        getServers().size() > NB_SERVERS && i<100; i++) {
      LOG.info("Waiting on server to go down");
      Thread.sleep(100);
    }
    assertFalse("Waited too long for RS to die", cluster.getMaster().getClusterStatus().
        getServers().size() > NB_SERVERS);
  }

  private void awaitDaughters(TableName tableName, int numDaughters) throws InterruptedException {
    // Wait till regions are back on line again.
    for (int i=0; cluster.getRegions(tableName).size() < numDaughters && i<60; i++) {
      LOG.info("Waiting for repair to happen");
      Thread.sleep(1000);
    }
    if (cluster.getRegions(tableName).size() < numDaughters) {
      fail("Waiting too long for daughter regions");
    }
  }

  private List<HRegion> awaitTableRegions(final TableName tableName) throws InterruptedException {
    List<HRegion> regions = null;
    for (int i = 0; i < 100; i++) {
      regions = cluster.getRegions(tableName);
      if (regions.size() > 0) break;
      Thread.sleep(100);
    }
    return regions;
  }

  private HTable createTableAndWait(TableName tableName, byte[] cf) throws IOException,
      InterruptedException {
    HTable t = TESTING_UTIL.createTable(tableName, cf);
    awaitTableRegions(tableName);
    assertTrue("Table not online: " + tableName,
      cluster.getRegions(tableName).size() != 0);
    return t;
  }

  public static class MockMasterWithoutCatalogJanitor extends HMaster {

    public MockMasterWithoutCatalogJanitor(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }
  }

  private static class SplittingNodeCreationFailedException  extends IOException {
    private static final long serialVersionUID = 1652404976265623004L;

    public SplittingNodeCreationFailedException () {
      super();
    }
  }

  public static class MockedRegionObserver extends BaseRegionObserver {
    private SplitTransactionImpl st = null;
    private PairOfSameType<Region> daughterRegions = null;

    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx,
        byte[] splitKey, List<Mutation> metaEntries) throws IOException {
      RegionCoprocessorEnvironment environment = ctx.getEnvironment();
      HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
      List<Region> onlineRegions =
          rs.getOnlineRegions(TableName.valueOf("testSplitHooksBeforeAndAfterPONR_2"));
      Region region = onlineRegions.get(0);
      for (Region r : onlineRegions) {
        if (r.getRegionInfo().containsRow(splitKey)) {
          region = r;
          break;
        }
      }
      st = new SplitTransactionImpl((HRegion) region, splitKey);
      if (!st.prepare()) {
        LOG.error("Prepare for the table " + region.getTableDesc().getNameAsString()
            + " failed. So returning null. ");
        ctx.bypass();
        return;
      }
      ((HRegion)region).forceSplit(splitKey);
      daughterRegions = st.stepsBeforePONR(rs, rs, false);
      HRegionInfo copyOfParent = new HRegionInfo(region.getRegionInfo());
      copyOfParent.setOffline(true);
      copyOfParent.setSplit(true);
      // Put for parent
      Put putParent = MetaTableAccessor.makePutFromRegionInfo(copyOfParent);
      MetaTableAccessor.addDaughtersToPut(putParent, daughterRegions.getFirst().getRegionInfo(),
        daughterRegions.getSecond().getRegionInfo());
      metaEntries.add(putParent);
      // Puts for daughters
      Put putA = MetaTableAccessor.makePutFromRegionInfo(
        daughterRegions.getFirst().getRegionInfo());
      Put putB = MetaTableAccessor.makePutFromRegionInfo(
        daughterRegions.getSecond().getRegionInfo());
      st.addLocation(putA, rs.getServerName(), 1);
      st.addLocation(putB, rs.getServerName(), 1);
      metaEntries.add(putA);
      metaEntries.add(putB);
    }

    @Override
    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx)
        throws IOException {
      RegionCoprocessorEnvironment environment = ctx.getEnvironment();
      HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
      st.stepsAfterPONR(rs, rs, daughterRegions, null);
    }

  }

  static class CustomSplitPolicy extends RegionSplitPolicy {

    @Override
    protected boolean shouldSplit() {
      return true;
    }

    @Override
    public boolean skipStoreFileRangeCheck(String familyName) {
      if(familyName.startsWith("i_")) {
        return true;
      } else {
        return false;
      }
    }
  }
}

