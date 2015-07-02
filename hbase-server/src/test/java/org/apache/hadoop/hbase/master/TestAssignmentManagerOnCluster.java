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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * This tests AssignmentManager with a testing cluster.
 */
@Category(MediumTests.class)
@SuppressWarnings("deprecation")
public class TestAssignmentManagerOnCluster {
  private final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final static Configuration conf = TEST_UTIL.getConfiguration();
  private static HBaseAdmin admin;

  static void setupOnce() throws Exception {
    // Using the our load balancer to control region plans
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      MyLoadBalancer.class, LoadBalancer.class);
    conf.setClass(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      MyRegionObserver.class, RegionObserver.class);
    // Reduce the maximum attempts to speed up the test
    conf.setInt("hbase.assignment.maximum.attempts", 3);
    // Put meta on master to avoid meta server shutdown handling
    conf.set("hbase.balancer.tablesOnMaster", "hbase:meta");
    conf.setInt("hbase.master.maximum.ping.server.attempts", 3);
    conf.setInt("hbase.master.ping.server.retry.sleep.interval", 1);

    TEST_UTIL.startMiniCluster(1, 4, null, MyMaster.class, MyRegionServer.class);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Use ZK for region assignment
    conf.setBoolean("hbase.assignment.usezk", true);
    setupOnce();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This tests restarting meta regionserver
   */
  @Test (timeout=180000)
  public void testRestartMetaRegionServer() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    boolean stoppedARegionServer = false;
    try {
      HMaster master = cluster.getMaster();
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      ServerName metaServerName = regionStates.getRegionServerOfRegion(
        HRegionInfo.FIRST_META_REGIONINFO);
      if (master.getServerName().equals(metaServerName) || metaServerName == null
          || !metaServerName.equals(cluster.getServerHoldingMeta())) {
        // Move meta off master
        metaServerName = cluster.getLiveRegionServerThreads()
          .get(0).getRegionServer().getServerName();
        master.move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
          Bytes.toBytes(metaServerName.getServerName()));
        master.assignmentManager.waitUntilNoRegionsInTransition(60000);
      }
      RegionState metaState =
          MetaTableLocator.getMetaRegionState(master.getZooKeeper());
        assertEquals("Meta should be not in transition",
            metaState.getState(), RegionState.State.OPEN);
      assertNotEquals("Meta should be moved off master",
        metaServerName, master.getServerName());
      cluster.killRegionServer(metaServerName);
      stoppedARegionServer = true;
      cluster.waitForRegionServerToStop(metaServerName, 60000);
      // Wait for SSH to finish
      final ServerManager serverManager = master.getServerManager();
      TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !serverManager.areDeadServersInProgress();
        }
      });

      // Now, make sure meta is assigned
      assertTrue("Meta should be assigned",
        regionStates.isRegionOnline(HRegionInfo.FIRST_META_REGIONINFO));
      // Now, make sure meta is registered in zk
      metaState = MetaTableLocator.getMetaRegionState(master.getZooKeeper());
      assertEquals("Meta should be not in transition",
          metaState.getState(), RegionState.State.OPEN);
      assertEquals("Meta should be assigned", metaState.getServerName(),
        regionStates.getRegionServerOfRegion(HRegionInfo.FIRST_META_REGIONINFO));
      assertNotEquals("Meta should be assigned on a different server",
        metaState.getServerName(), metaServerName);
    } finally {
      if (stoppedARegionServer) {
        cluster.startRegionServer();
      }
    }
  }

  /**
   * This tests region assignment
   */
  @Test (timeout=60000)
  public void testAssignRegion() throws Exception {
    String table = "testAssignRegion";
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      am.waitForAssignment(hri);

      RegionStates regionStates = am.getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 6000);

      // Region is assigned now. Let's assign it again.
      // Master should not abort, and region should be assigned.
      RegionState oldState = regionStates.getRegionState(hri);
      TEST_UTIL.getHBaseAdmin().assign(hri.getRegionName());
      master.getAssignmentManager().waitForAssignment(hri);
      RegionState newState = regionStates.getRegionState(hri);
      assertTrue(newState.isOpened()
        && newState.getStamp() != oldState.getStamp());
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  // Simulate a scenario where the AssignCallable and SSH are trying to assign a region
  @Test (timeout=60000)
  public void testAssignRegionBySSH() throws Exception {
    if (!conf.getBoolean("hbase.assignment.usezk", true)) {
      return;
    }
    String table = "testAssignRegionBySSH";
    MyMaster master = (MyMaster) TEST_UTIL.getHBaseCluster().getMaster();
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HTable meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);
      // Add some dummy server for the region entry
      MetaTableAccessor.updateRegionLocation(TEST_UTIL.getHBaseCluster().getMaster().getConnection(), hri,
        ServerName.valueOf("example.org", 1234, System.currentTimeMillis()), 0, -1);
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      int i = TEST_UTIL.getHBaseCluster().getServerWithMeta();
      HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(i == 0 ? 1 : 0);
      // Choose a server other than meta to kill
      ServerName controlledServer = rs.getServerName();
      master.enableSSH(false);
      TEST_UTIL.getHBaseCluster().killRegionServer(controlledServer);
      TEST_UTIL.getHBaseCluster().waitForRegionServerToStop(controlledServer, -1);
      AssignmentManager am = master.getAssignmentManager();

      // Simulate the AssignCallable trying to assign the region. Have the region in OFFLINE state,
      // but not in transition and the server is the dead 'controlledServer'
      regionStates.createRegionState(hri, State.OFFLINE, controlledServer, null);
      am.assign(hri, true, true);
      // Region should remain OFFLINE and go to transition
      assertEquals(State.OFFLINE, regionStates.getRegionState(hri).getState());
      assertTrue (regionStates.isRegionInTransition(hri));

      master.enableSSH(true);
      am.waitForAssignment(hri);
      assertTrue (regionStates.getRegionState(hri).isOpened());
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnlyOnServer(hri, serverName, 6000);
    } finally {
      if (master != null) {
        master.enableSSH(true);
      }
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      TEST_UTIL.getHBaseCluster().startRegionServer();
    }
  }

  /**
   * This tests region assignment on a simulated restarted server
   */
  @Test (timeout=120000)
  public void testAssignRegionOnRestartedServer() throws Exception {
    String table = "testAssignRegionOnRestartedServer";
    TEST_UTIL.getMiniHBaseCluster().getConf().setInt("hbase.assignment.maximum.attempts", 20);
    TEST_UTIL.getMiniHBaseCluster().stopMaster(0);
    TEST_UTIL.getMiniHBaseCluster().startMaster(); //restart the master so that conf take into affect

    ServerName deadServer = null;
    HMaster master = null;
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      final HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      master = TEST_UTIL.getHBaseCluster().getMaster();
      Set<ServerName> onlineServers = master.serverManager.getOnlineServers().keySet();
      assertFalse("There should be some servers online", onlineServers.isEmpty());

      // Use the first server as the destination server
      ServerName destServer = onlineServers.iterator().next();

      // Created faked dead server
      deadServer = ServerName.valueOf(destServer.getHostname(),
          destServer.getPort(), destServer.getStartcode() - 100L);
      master.serverManager.recordNewServerWithLock(deadServer, ServerLoad.EMPTY_SERVERLOAD);

      final AssignmentManager am = master.getAssignmentManager();
      RegionPlan plan = new RegionPlan(hri, null, deadServer);
      am.addPlan(hri.getEncodedName(), plan);
      master.assignRegion(hri);

      int version = ZKAssign.transitionNode(master.getZooKeeper(), hri,
        destServer, EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, 0);
      assertEquals("TansitionNode should fail", -1, version);

      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return ! am.getRegionStates().isRegionInTransition(hri);
        }
      });

    assertFalse("Region should be assigned", am.getRegionStates().isRegionInTransition(hri));
    } finally {
      if (deadServer != null) {
        master.serverManager.expireServer(deadServer);
      }

      TEST_UTIL.deleteTable(Bytes.toBytes(table));

      // reset the value for other tests
      TEST_UTIL.getMiniHBaseCluster().getConf().setInt("hbase.assignment.maximum.attempts", 3);
      ServerName masterServerName = TEST_UTIL.getMiniHBaseCluster().getMaster().getServerName();
      TEST_UTIL.getMiniHBaseCluster().stopMaster(masterServerName);
      TEST_UTIL.getMiniHBaseCluster().startMaster();
      // Wait till master is active and is initialized
      while (TEST_UTIL.getMiniHBaseCluster().getMaster() == null ||
          !TEST_UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
        Threads.sleep(1);
      }
    }
  }

  /**
   * This tests offlining a region
   */
  @Test (timeout=60000)
  public void testOfflineRegion() throws Exception {
    TableName table =
        TableName.valueOf("testOfflineRegion");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(table);

      RegionStates regionStates = TEST_UTIL.getHBaseCluster().
        getMaster().getAssignmentManager().getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 6000);
      admin.offline(hri.getRegionName());

      long timeoutTime = System.currentTimeMillis() + 800;
      while (true) {
        if (regionStates.getRegionByStateOfTable(table)
            .get(RegionState.State.OFFLINE).contains(hri))
          break;
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to offline the region in time");
          break;
        }
        Thread.sleep(10);
      }
      RegionState regionState = regionStates.getRegionState(hri);
      assertTrue(regionState.isOffline());
    } finally {
      TEST_UTIL.deleteTable(table);
    }
  }

  /**
   * This tests moving a region
   */
  @Test (timeout=50000)
  public void testMoveRegion() throws Exception {
    TableName table =
        TableName.valueOf("testMoveRegion");
    try {
      HRegionInfo hri = createTableAndGetOneRegion(table);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      ServerManager serverManager = master.getServerManager();
      ServerName destServerName = null;
      List<JVMClusterUtil.RegionServerThread> regionServers =
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads();
      for (JVMClusterUtil.RegionServerThread regionServer: regionServers) {
        HRegionServer destServer = regionServer.getRegionServer();
        destServerName = destServer.getServerName();
        if (!destServerName.equals(serverName)
            && serverManager.isServerOnline(destServerName)) {
          break;
        }
      }
      assertTrue(destServerName != null
        && !destServerName.equals(serverName));
      TEST_UTIL.getHBaseAdmin().move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(destServerName.getServerName()));

      long timeoutTime = System.currentTimeMillis() + 30000;
      while (true) {
        ServerName sn = regionStates.getRegionServerOfRegion(hri);
        if (sn != null && sn.equals(destServerName)) {
          TEST_UTIL.assertRegionOnServer(hri, sn, 6000);
          break;
        }
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to move the region in time: "
            + regionStates.getRegionState(hri));
        }
        regionStates.waitForUpdate(50);
      }

    } finally {
      TEST_UTIL.deleteTable(table);
    }
  }

  /**
   * If a table is deleted, we should not be able to move it anymore.
   * Otherwise, the region will be brought back.
   * @throws Exception
   */
  @Test (timeout=50000)
  public void testMoveRegionOfDeletedTable() throws Exception {
    TableName table =
        TableName.valueOf("testMoveRegionOfDeletedTable");
    Admin admin = TEST_UTIL.getHBaseAdmin();
    try {
      HRegionInfo hri = createTableAndGetOneRegion(table);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      AssignmentManager am = master.getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      ServerName destServerName = null;
      for (int i = 0; i < 3; i++) {
        HRegionServer destServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        if (!destServer.getServerName().equals(serverName)) {
          destServerName = destServer.getServerName();
          break;
        }
      }
      assertTrue(destServerName != null
        && !destServerName.equals(serverName));

      TEST_UTIL.deleteTable(table);

      try {
        admin.move(hri.getEncodedNameAsBytes(),
          Bytes.toBytes(destServerName.getServerName()));
        fail("We should not find the region");
      } catch (IOException ioe) {
        assertTrue(ioe instanceof UnknownRegionException);
      }

      am.balance(new RegionPlan(hri, serverName, destServerName));
      assertFalse("The region should not be in transition",
        regionStates.isRegionInTransition(hri));
    } finally {
      if (admin.tableExists(table)) {
        TEST_UTIL.deleteTable(table);
      }
    }
  }

  HRegionInfo createTableAndGetOneRegion(
      final TableName tableName) throws IOException, InterruptedException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5);

    // wait till the table is assigned
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    long timeoutTime = System.currentTimeMillis() + 1000;
    while (true) {
      List<HRegionInfo> regions = master.getAssignmentManager().
        getRegionStates().getRegionsOfTable(tableName);
      if (regions.size() > 3) {
        return regions.get(2);
      }
      long now = System.currentTimeMillis();
      if (now > timeoutTime) {
        fail("Could not find an online region");
      }
      Thread.sleep(10);
    }
  }

  /**
   * This test should not be flaky. If it is flaky, it means something
   * wrong with AssignmentManager which should be reported and fixed
   *
   * This tests forcefully assign a region while it's closing and re-assigned.
   */
  @Test (timeout=60000)
  public void testForceAssignWhileClosing() throws Exception {
    String table = "testForceAssignWhileClosing";
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertTrue(am.waitForAssignment(hri));

      ServerName sn = am.getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, sn, 6000);
      MyRegionObserver.preCloseEnabled.set(true);
      am.unassign(hri);
      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_CLOSE, state.getState());

      MyRegionObserver.preCloseEnabled.set(false);
      am.unassign(hri, true);

      // region is closing now, will be re-assigned automatically.
      // now, let's forcefully assign it again. it should be
      // assigned properly and no double-assignment
      am.assign(hri, true, true);

      // let's check if it's assigned after it's out of transition
      am.waitOnRegionToClearRegionsInTransition(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnlyOnServer(hri, serverName, 200);
    } finally {
      MyRegionObserver.preCloseEnabled.set(false);
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests region close failed
   */
  @Test (timeout=60000)
  public void testCloseFailed() throws Exception {
    String table = "testCloseFailed";
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertTrue(am.waitForAssignment(hri));
      ServerName sn = am.getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, sn, 6000);

      MyRegionObserver.preCloseEnabled.set(true);
      am.unassign(hri);
      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_CLOSE, state.getState());

      MyRegionObserver.preCloseEnabled.set(false);
      am.unassign(hri, true);

      // region may still be assigned now since it's closing,
      // let's check if it's assigned after it's out of transition
      am.waitOnRegionToClearRegionsInTransition(hri);

      // region should be closed and re-assigned
      assertTrue(am.waitForAssignment(hri));
      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 6000);
    } finally {
      MyRegionObserver.preCloseEnabled.set(false);
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests region open failed
   */
  @Test (timeout=60000)
  public void testOpenFailed() throws Exception {
    String table = "testOpenFailed";
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      MyLoadBalancer.controledRegion = hri.getEncodedName();

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertFalse(am.waitForAssignment(hri));

      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_OPEN, state.getState());
      // Failed to open since no plan, so it's on no server
      assertNull(state.getServerName());

      MyLoadBalancer.controledRegion = null;
      master.assignRegion(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 6000);
    } finally {
      MyLoadBalancer.controledRegion = null;
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests region open failure which is not recoverable
   */
  @Test (timeout=60000)
  public void testOpenFailedUnrecoverable() throws Exception {
    TableName table =
        TableName.valueOf("testOpenFailedUnrecoverable");
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      FileSystem fs = FileSystem.get(conf);
      Path tableDir= FSUtils.getTableDir(FSUtils.getRootDir(conf), table);
      Path regionDir = new Path(tableDir, hri.getEncodedName());
      // create a file named the same as the region dir to
      // mess up with region opening
      fs.create(regionDir, true);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertFalse(am.waitForAssignment(hri));

      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_OPEN, state.getState());
      // Failed to open due to file system issue. Region state should
      // carry the opening region server so that we can force close it
      // later on before opening it again. See HBASE-9092.
      assertNotNull(state.getServerName());

      // remove the blocking file, so that region can be opened
      fs.delete(regionDir, true);
      master.assignRegion(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 6000);
    } finally {
      TEST_UTIL.deleteTable(table);
    }
  }

  @Test (timeout=60000)
  public void testSSHWhenDisablingTableRegionsInOpeningOrPendingOpenState() throws Exception {
    final TableName table =
        TableName.valueOf
            ("testSSHWhenDisablingTableRegionsInOpeningOrPendingOpenState");
    AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    HRegionInfo hri = null;
    ServerName serverName = null;
    try {
      hri = createTableAndGetOneRegion(table);
      serverName = am.getRegionStates().getRegionServerOfRegion(hri);
      ServerName destServerName = null;
      HRegionServer destServer = null;
      for (int i = 0; i < 3; i++) {
        destServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        if (!destServer.getServerName().equals(serverName)) {
          destServerName = destServer.getServerName();
          break;
        }
      }
      am.regionOffline(hri);
      ZooKeeperWatcher zkw = TEST_UTIL.getHBaseCluster().getMaster().getZooKeeper();
      am.getRegionStates().updateRegionState(hri, State.PENDING_OPEN, destServerName);
      if (ConfigUtil.useZKForAssignment(conf)) {
        ZKAssign.createNodeOffline(zkw, hri, destServerName);
        ZKAssign.transitionNodeOpening(zkw, hri, destServerName);

        // Wait till the event is processed and the region is in transition
        long timeoutTime = System.currentTimeMillis() + 20000;
        while (!am.getRegionStates().isRegionInTransition(hri)) {
          assertTrue("Failed to process ZK opening event in time",
            System.currentTimeMillis() < timeoutTime);
          Thread.sleep(100);
        }
      }

      am.getTableStateManager().setTableState(table, ZooKeeperProtos.Table.State.DISABLING);
      List<HRegionInfo> toAssignRegions = am.processServerShutdown(destServerName);
      assertTrue("Regions to be assigned should be empty.", toAssignRegions.isEmpty());
      assertTrue("Regions to be assigned should be empty.", am.getRegionStates()
          .getRegionState(hri).isOffline());
    } finally {
      if (hri != null && serverName != null) {
        am.regionOnline(hri, serverName);
      }
      am.getTableStateManager().setTableState(table, ZooKeeperProtos.Table.State.DISABLED);
      TEST_UTIL.deleteTable(table);
    }
  }

  /**
   * This tests region close hanging
   */
  @Test (timeout=60000)
  public void testCloseHang() throws Exception {
    String table = "testCloseHang";
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertTrue(am.waitForAssignment(hri));
      ServerName sn = am.getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, sn, 6000);

      MyRegionObserver.postCloseEnabled.set(true);
      am.unassign(hri);
      // Now region should pending_close or closing
      // Unassign it again forcefully so that we can trigger already
      // in transition exception. This test is to make sure this scenario
      // is handled properly.
      am.server.getConfiguration().setLong(
        AssignmentManager.ALREADY_IN_TRANSITION_WAITTIME, 1000);
      am.unassign(hri, true);
      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_CLOSE, state.getState());

      // Let region closing move ahead. The region should be closed
      // properly and re-assigned automatically
      MyRegionObserver.postCloseEnabled.set(false);

      // region may still be assigned now since it's closing,
      // let's check if it's assigned after it's out of transition
      am.waitOnRegionToClearRegionsInTransition(hri);

      // region should be closed and re-assigned
      assertTrue(am.waitForAssignment(hri));
      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 6000);
    } finally {
      MyRegionObserver.postCloseEnabled.set(false);
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests region close racing with open
   */
  @Test (timeout=60000)
  public void testOpenCloseRacing() throws Exception {
    String table = "testOpenCloseRacing";
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);
      meta.close();

      MyRegionObserver.postOpenEnabled.set(true);
      MyRegionObserver.postOpenCalled = false;
      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      // Region will be opened, but it won't complete
      master.assignRegion(hri);
      long end = EnvironmentEdgeManager.currentTime() + 20000;
      // Wait till postOpen is called
      while (!MyRegionObserver.postOpenCalled ) {
        assertFalse("Timed out waiting for postOpen to be called",
          EnvironmentEdgeManager.currentTime() > end);
        Thread.sleep(300);
      }

      AssignmentManager am = master.getAssignmentManager();
      // Now let's unassign it, it should do nothing
      am.unassign(hri);
      RegionState state = am.getRegionStates().getRegionState(hri);
      ServerName oldServerName = state.getServerName();
      assertTrue(state.isPendingOpenOrOpening() && oldServerName != null);

      // Now the region is stuck in opening
      // Let's forcefully re-assign it to trigger closing/opening
      // racing. This test is to make sure this scenario
      // is handled properly.
      ServerName destServerName = null;
      int numRS = TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads().size();
      for (int i = 0; i < numRS; i++) {
        HRegionServer destServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        if (!destServer.getServerName().equals(oldServerName)) {
          destServerName = destServer.getServerName();
          break;
        }
      }
      assertNotNull(destServerName);
      assertFalse("Region should be assigned on a new region server",
        oldServerName.equals(destServerName));
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
      regions.add(hri);
      am.assign(destServerName, regions);

      // let region open continue
      MyRegionObserver.postOpenEnabled.set(false);

      // let's check if it's assigned after it's out of transition
      am.waitOnRegionToClearRegionsInTransition(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnlyOnServer(hri, serverName, 6000);
    } finally {
      MyRegionObserver.postOpenEnabled.set(false);
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * Test force unassign/assign a region hosted on a dead server
   */
  @Test (timeout=60000)
  public void testAssignRacingWithSSH() throws Exception {
    String table = "testAssignRacingWithSSH";
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MyMaster master = null;
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      // Assign the region
      master = (MyMaster)cluster.getMaster();
      master.assignRegion(hri);

      // Hold SSH before killing the hosting server
      master.enableSSH(false);

      AssignmentManager am = master.getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      ServerName metaServer = regionStates.getRegionServerOfRegion(
        HRegionInfo.FIRST_META_REGIONINFO);
      while (true) {
        assertTrue(am.waitForAssignment(hri));
        RegionState state = regionStates.getRegionState(hri);
        ServerName oldServerName = state.getServerName();
        if (!ServerName.isSameHostnameAndPort(oldServerName, metaServer)) {
          // Kill the hosting server, which doesn't have meta on it.
          cluster.killRegionServer(oldServerName);
          cluster.waitForRegionServerToStop(oldServerName, -1);
          break;
        }
        int i = cluster.getServerWithMeta();
        HRegionServer rs = cluster.getRegionServer(i == 0 ? 1 : 0);
        oldServerName = rs.getServerName();
        master.move(hri.getEncodedNameAsBytes(),
          Bytes.toBytes(oldServerName.getServerName()));
      }

      // You can't assign a dead region before SSH
      am.assign(hri, true, true);
      RegionState state = regionStates.getRegionState(hri);
      assertTrue(state.isFailedClose());

      // You can't unassign a dead region before SSH either
      am.unassign(hri, true);
      assertTrue(state.isFailedClose());

      // Enable SSH so that log can be split
      master.enableSSH(true);

      // let's check if it's assigned after it's out of transition.
      // no need to assign it manually, SSH should do it
      am.waitOnRegionToClearRegionsInTransition(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnlyOnServer(hri, serverName, 6000);
    } finally {
      if (master != null) {
        master.enableSSH(true);
      }
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      cluster.startRegionServer();
    }
  }

  /**
   * Test force unassign/assign a region of a disabled table
   */
  @Test (timeout=60000)
  public void testAssignDisabledRegion() throws Exception {
    TableName table = TableName.valueOf("testAssignDisabledRegion");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MyMaster master = null;
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      // Assign the region
      master = (MyMaster)cluster.getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      assertTrue(am.waitForAssignment(hri));

      // Disable the table
      admin.disableTable(table);
      assertTrue(regionStates.isRegionOffline(hri));

      // You can't assign a disabled region
      am.assign(hri, true, true);
      assertTrue(regionStates.isRegionOffline(hri));

      // You can't unassign a disabled region either
      am.unassign(hri, true);
      assertTrue(regionStates.isRegionOffline(hri));
    } finally {
      TEST_UTIL.deleteTable(table);
    }
  }

  /**
   * Test offlined region is assigned by SSH
   */
  @Test (timeout=60000)
  public void testAssignOfflinedRegionBySSH() throws Exception {
    String table = "testAssignOfflinedRegionBySSH";
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MyMaster master = null;
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      // Assign the region
      master = (MyMaster)cluster.getMaster();
      master.assignRegion(hri);

      AssignmentManager am = master.getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      ServerName metaServer = regionStates.getRegionServerOfRegion(
        HRegionInfo.FIRST_META_REGIONINFO);
      ServerName oldServerName = null;
      while (true) {
        assertTrue(am.waitForAssignment(hri));
        RegionState state = regionStates.getRegionState(hri);
        oldServerName = state.getServerName();
        if (!ServerName.isSameHostnameAndPort(oldServerName, metaServer)) {
          // Mark the hosting server aborted, but don't actually kill it.
          // It doesn't have meta on it.
          MyRegionServer.abortedServer = oldServerName;
          break;
        }
        int i = cluster.getServerWithMeta();
        HRegionServer rs = cluster.getRegionServer(i == 0 ? 1 : 0);
        oldServerName = rs.getServerName();
        master.move(hri.getEncodedNameAsBytes(),
          Bytes.toBytes(oldServerName.getServerName()));
      }

      // Make sure the region is assigned on the dead server
      assertTrue(regionStates.isRegionOnline(hri));
      assertEquals(oldServerName, regionStates.getRegionServerOfRegion(hri));

      // Kill the hosting server, which doesn't have meta on it.
      cluster.killRegionServer(oldServerName);
      cluster.waitForRegionServerToStop(oldServerName, -1);

      ServerManager serverManager = master.getServerManager();
      while (!serverManager.isServerDead(oldServerName)
          || serverManager.getDeadServers().areDeadServersInProgress()) {
        Thread.sleep(100);
      }

      // Let's check if it's assigned after it's out of transition.
      // no need to assign it manually, SSH should do it
      am.waitOnRegionToClearRegionsInTransition(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnlyOnServer(hri, serverName, 200);
    } finally {
      MyRegionServer.abortedServer = null;
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      cluster.startRegionServer();
    }
  }

  /**
   * Test SSH waiting for extra region server for assignment
   */
  @Test (timeout=300000)
  public void testSSHWaitForServerToAssignRegion() throws Exception {
    TableName table = TableName.valueOf("testSSHWaitForServerToAssignRegion");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    boolean startAServer = false;
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HMaster master = cluster.getMaster();
      final ServerManager serverManager = master.getServerManager();
      MyLoadBalancer.countRegionServers = Integer.valueOf(
        serverManager.countOfRegionServers());
      HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(table);
      assertNotNull("First region should be assigned", rs);
      final ServerName serverName = rs.getServerName();
      // Wait till SSH tried to assign regions a several times
      int counter = MyLoadBalancer.counter.get() + 5;
      cluster.killRegionServer(serverName);
      startAServer = true;
      cluster.waitForRegionServerToStop(serverName, -1);
      while (counter > MyLoadBalancer.counter.get()) {
        Thread.sleep(1000);
      }
      cluster.startRegionServer();
      startAServer = false;
      // Wait till the dead server is processed by SSH
      TEST_UTIL.waitFor(120000, 1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return serverManager.isServerDead(serverName)
            && !serverManager.areDeadServersInProgress();
        }
      });
      TEST_UTIL.waitUntilAllRegionsAssigned(table, 300000);

      rs = TEST_UTIL.getRSForFirstRegionInTable(table);
      assertTrue("First region should be re-assigned to a different server",
        rs != null && !serverName.equals(rs.getServerName()));
    } finally {
      MyLoadBalancer.countRegionServers = null;
      TEST_UTIL.deleteTable(table);
      if (startAServer) {
        cluster.startRegionServer();
      }
    }
  }

  /**
   * Test disabled region is ignored by SSH
   */
  @Test (timeout=60000)
  public void testAssignDisabledRegionBySSH() throws Exception {
    String table = "testAssignDisabledRegionBySSH";
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MyMaster master = null;
    try {
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);

      // Assign the region
      master = (MyMaster)cluster.getMaster();
      master.assignRegion(hri);

      AssignmentManager am = master.getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      ServerName metaServer = regionStates.getRegionServerOfRegion(
        HRegionInfo.FIRST_META_REGIONINFO);
      ServerName oldServerName = null;
      while (true) {
        assertTrue(am.waitForAssignment(hri));
        RegionState state = regionStates.getRegionState(hri);
        oldServerName = state.getServerName();
        if (!ServerName.isSameHostnameAndPort(oldServerName, metaServer)) {
          // Mark the hosting server aborted, but don't actually kill it.
          // It doesn't have meta on it.
          MyRegionServer.abortedServer = oldServerName;
          break;
        }
        int i = cluster.getServerWithMeta();
        HRegionServer rs = cluster.getRegionServer(i == 0 ? 1 : 0);
        oldServerName = rs.getServerName();
        master.move(hri.getEncodedNameAsBytes(),
          Bytes.toBytes(oldServerName.getServerName()));
      }

      // Make sure the region is assigned on the dead server
      assertTrue(regionStates.isRegionOnline(hri));
      assertEquals(oldServerName, regionStates.getRegionServerOfRegion(hri));

      // Disable the table now.
      master.disableTable(hri.getTable());

      // Kill the hosting server, which doesn't have meta on it.
      cluster.killRegionServer(oldServerName);
      cluster.waitForRegionServerToStop(oldServerName, -1);

      ServerManager serverManager = master.getServerManager();
      while (!serverManager.isServerDead(oldServerName)
          || serverManager.getDeadServers().areDeadServersInProgress()) {
        Thread.sleep(100);
      }

      // Wait till no more RIT, the region should be offline.
      am.waitUntilNoRegionsInTransition(60000);
      assertTrue(regionStates.isRegionOffline(hri));
    } finally {
      MyRegionServer.abortedServer = null;
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
      cluster.startRegionServer();
    }
  }

  /**
   * Test that region state transition call is idempotent
   */
  @Test(timeout = 60000)
  public void testReportRegionStateTransition() throws Exception {
    String table = "testReportRegionStateTransition";
    try {
      MyRegionServer.simulateRetry = true;
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);
      Table meta = new HTable(conf, TableName.META_TABLE_NAME);
      HRegionInfo hri =
          new HRegionInfo(desc.getTableName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaTableAccessor.addRegionToMeta(meta, hri);
      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      am.waitForAssignment(hri);
      RegionStates regionStates = am.getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      // Assert the the region is actually open on the server
      TEST_UTIL.assertRegionOnServer(hri, serverName, 6000);
      // Closing region should just work fine
      admin.disableTable(TableName.valueOf(table));
      assertTrue(regionStates.isRegionOffline(hri));
      List<HRegionInfo> regions = TEST_UTIL.getHBaseAdmin().getOnlineRegions(serverName);
      assertTrue(!regions.contains(hri));
    } finally {
      MyRegionServer.simulateRetry = false;
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * Test concurrent updates to meta when meta is not on master
   * @throws Exception
   */
  @Test(timeout = 30000)
  public void testUpdatesRemoteMeta() throws Exception {
    // Not for zk less assignment
    if (conf.getBoolean("hbase.assignment.usezk", true)) {
      return;
    }
    conf.setInt("hbase.regionstatestore.meta.connection", 3);
    final RegionStateStore rss =
        new RegionStateStore(new MyRegionServer(conf, new ZkCoordinatedStateManager()));
    rss.start();
    // Create 10 threads and make each do 10 puts related to region state update
    Thread[] th = new Thread[10];
    List<String> nameList = new ArrayList<String>();
    List<TableName> tableNameList = new ArrayList<TableName>();
    for (int i = 0; i < th.length; i++) {
      th[i] = new Thread() {
        @Override
        public void run() {
          HRegionInfo[] hri = new HRegionInfo[10];
          ServerName serverName = ServerName.valueOf("dummyhost", 1000, 1234);
          for (int i = 0; i < 10; i++) {
            hri[i] = new HRegionInfo(TableName.valueOf(Thread.currentThread().getName() + "_" + i));
            RegionState newState = new RegionState(hri[i], RegionState.State.OPEN, serverName);
            RegionState oldState =
                new RegionState(hri[i], RegionState.State.PENDING_OPEN, serverName);
            rss.updateRegionState(1, newState, oldState);
          }
        }
      };
      th[i].start();
      nameList.add(th[i].getName());
    }
    for (int i = 0; i < th.length; i++) {
      th[i].join();
    }
    // Add all the expected table names in meta to tableNameList
    for (String name : nameList) {
      for (int i = 0; i < 10; i++) {
        tableNameList.add(TableName.valueOf(name + "_" + i));
      }
    }
    List<Result> metaRows = MetaTableAccessor.fullScanOfMeta(admin.getConnection());
    int count = 0;
    // Check all 100 rows are in meta
    for (Result result : metaRows) {
      if (tableNameList.contains(HRegionInfo.getTable(result.getRow()))) {
        count++;
        if (count == 100) {
          break;
        }
      }
    }
    assertTrue(count == 100);
    rss.stop();
  }

  static class MyLoadBalancer extends StochasticLoadBalancer {
    // For this region, if specified, always assign to nowhere
    static volatile String controledRegion = null;

    static volatile Integer countRegionServers = null;
    static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public ServerName randomAssignment(HRegionInfo regionInfo,
        List<ServerName> servers) {
      if (regionInfo.getEncodedName().equals(controledRegion)) {
        return null;
      }
      return super.randomAssignment(regionInfo, servers);
    }

    @Override
    public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
        List<HRegionInfo> regions, List<ServerName> servers) {
      if (countRegionServers != null && services != null) {
        int regionServers = services.getServerManager().countOfRegionServers();
        if (regionServers < countRegionServers.intValue()) {
          // Let's wait till more region servers join in.
          // Before that, fail region assignments.
          counter.incrementAndGet();
          return null;
        }
      }
      return super.roundRobinAssignment(regions, servers);
    }
  }

  public static class MyMaster extends HMaster {
    AtomicBoolean enabled = new AtomicBoolean(true);

    public MyMaster(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }

    @Override
    public boolean isServerShutdownHandlerEnabled() {
      return enabled.get() && super.isServerShutdownHandlerEnabled();
    }

    public void enableSSH(boolean enabled) {
      this.enabled.set(enabled);
      if (enabled) {
        serverManager.processQueuedDeadServers();
      }
    }
  }

  public static class MyRegionServer extends MiniHBaseClusterRegionServer {
    static volatile ServerName abortedServer = null;
    static volatile boolean simulateRetry = false;

    public MyRegionServer(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }

    @Override
    public boolean reportRegionStateTransition(TransitionCode code, long openSeqNum,
        HRegionInfo... hris) {
      if (simulateRetry) {
        // Simulate retry by calling the method twice
        super.reportRegionStateTransition(code, openSeqNum, hris);
        return super.reportRegionStateTransition(code, openSeqNum, hris);
      }
      return super.reportRegionStateTransition(code, openSeqNum, hris);
    }

    @Override
    public boolean isAborted() {
      return getServerName().equals(abortedServer) || super.isAborted();
    }
  }

  public static class MyRegionObserver extends BaseRegionObserver {
    // If enabled, fail all preClose calls
    static AtomicBoolean preCloseEnabled = new AtomicBoolean(false);

    // If enabled, stall postClose calls
    static AtomicBoolean postCloseEnabled = new AtomicBoolean(false);

    // If enabled, stall postOpen calls
    static AtomicBoolean postOpenEnabled = new AtomicBoolean(false);

    // A flag to track if postOpen is called
    static volatile boolean postOpenCalled = false;

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c,
        boolean abortRequested) throws IOException {
      if (preCloseEnabled.get()) throw new IOException("fail preClose from coprocessor");
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> c,
        boolean abortRequested) {
      stallOnFlag(postCloseEnabled);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
      postOpenCalled = true;
      stallOnFlag(postOpenEnabled);
    }

    private void stallOnFlag(final AtomicBoolean flag) {
      try {
        // If enabled, stall
        while (flag.get()) {
          Thread.sleep(1000);
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
