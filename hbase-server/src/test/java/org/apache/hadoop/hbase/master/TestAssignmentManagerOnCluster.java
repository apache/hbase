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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This tests AssignmentManager with a testing cluster.
 */
@Category(MediumTests.class)
public class TestAssignmentManagerOnCluster {
  private final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private static HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Using the mock load balancer to control region plans
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      MockLoadBalancer.class, LoadBalancer.class);
    conf.setClass(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      MockRegionObserver.class, RegionObserver.class);
    // Reduce the maximum attempts to speed up the test
    conf.setInt("hbase.assignment.maximum.attempts", 3);

    TEST_UTIL.startMiniCluster(3);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This tests region assignment
   */
  @Test (timeout=60000)
  public void testAssignRegion() throws Exception {
    String table = "testAssignRegion";
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaEditor.addRegionToMeta(meta, hri);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      master.getAssignmentManager().waitForAssignment(hri);

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests region assignment on a simulated restarted server
   */
  @Test (timeout=60000)
  public void testAssignRegionOnRestartedServer() throws Exception {
    String table = "testAssignRegionOnRestartedServer";
    ServerName deadServer = null;
    HMaster master = null;
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaEditor.addRegionToMeta(meta, hri);

      master = TEST_UTIL.getHBaseCluster().getMaster();
      Set<ServerName> onlineServers = master.serverManager.getOnlineServers().keySet();
      assertFalse("There should be some servers online", onlineServers.isEmpty());

      // Use the first server as the destination server
      ServerName destServer = onlineServers.iterator().next();

      // Created faked dead server
      deadServer = new ServerName(destServer.getHostname(),
        destServer.getPort(), destServer.getStartcode() - 100L);
      master.serverManager.recordNewServer(deadServer, ServerLoad.EMPTY_SERVERLOAD);

      AssignmentManager am = master.getAssignmentManager();
      RegionPlan plan = new RegionPlan(hri, null, deadServer);
      am.addPlan(hri.getEncodedName(), plan);
      master.assignRegion(hri);

      int version = ZKAssign.transitionNode(master.getZooKeeper(), hri,
        destServer, EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, 0);
      assertEquals("TansitionNode should fail", -1, version);

      // Give region 2 seconds to assign, which may not be enough.
      // However, if HBASE-8545 is broken, this test will be flaky.
      // Otherwise, this test should never be flaky.
      Thread.sleep(2000);

      assertTrue("Region should still be in transition",
        am.getRegionStates().isRegionInTransition(hri));
      assertEquals("Assign node should still be in version 0", 0,
        ZKAssign.getVersion(master.getZooKeeper(), hri));
    } finally {
      if (deadServer != null) {
        master.serverManager.expireServer(deadServer);
      }

      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests offlining a region
   */
  @Test (timeout=60000)
  public void testOfflineRegion() throws Exception {
    String table = "testOfflineRegion";
    try {
      HRegionInfo hri = createTableAndGetOneRegion(table);

      RegionStates regionStates = TEST_UTIL.getHBaseCluster().
        getMaster().getAssignmentManager().getRegionStates();
      ServerName serverName = regionStates.getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
      admin.offline(hri.getRegionName());

      long timeoutTime = System.currentTimeMillis() + 800;
      while (true) {
        List<HRegionInfo> regions =
          regionStates.getRegionsOfTable(Bytes.toBytes(table));
        if (!regions.contains(hri)) break;
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
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests moving a region
   */
  @Test (timeout=50000)
  public void testMoveRegion() throws Exception {
    String table = "testMoveRegion";
    try {
      HRegionInfo hri = createTableAndGetOneRegion(table);

      RegionStates regionStates = TEST_UTIL.getHBaseCluster().
        getMaster().getAssignmentManager().getRegionStates();
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
      TEST_UTIL.getHBaseAdmin().move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(destServerName.getServerName()));

      long timeoutTime = System.currentTimeMillis() + 30000;
      while (true) {
        ServerName sn = regionStates.getRegionServerOfRegion(hri);
        if (sn != null && sn.equals(destServerName)) {
          TEST_UTIL.assertRegionOnServer(hri, sn, 200);
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
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  HRegionInfo createTableAndGetOneRegion(
      final String tableName) throws IOException, InterruptedException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5);

    // wait till the table is assigned
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    long timeoutTime = System.currentTimeMillis() + 1000;
    while (true) {
      List<HRegionInfo> regions = master.getAssignmentManager().
        getRegionStates().getRegionsOfTable(Bytes.toBytes(tableName));
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
   * This tests forcefully assign a region
   * while it's closing and re-assigned.
   *
   * This test should not be flaky. If it is flaky, it means something
   * wrong with AssignmentManager which should be reported and fixed
   */
  @Test (timeout=60000)
  public void testForceAssignWhileClosing() throws Exception {
    String table = "testForceAssignWhileClosing";
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaEditor.addRegionToMeta(meta, hri);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertTrue(am.waitForAssignment(hri));

      MockRegionObserver.enabled = true;
      am.unassign(hri);
      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_CLOSE, state.getState());

      MockRegionObserver.enabled = false;
      am.unassign(hri, true);

      // region is closing now, will be re-assigned automatically.
      // now, let's forcefully assign it again. it should be
      // assigned properly and no double-assignment
      am.assign(hri, true, true);

      // region should be closed and re-assigned
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
    } finally {
      MockRegionObserver.enabled = false;
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
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaEditor.addRegionToMeta(meta, hri);

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertTrue(am.waitForAssignment(hri));

      MockRegionObserver.enabled = true;
      am.unassign(hri);
      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_CLOSE, state.getState());

      MockRegionObserver.enabled = false;
      am.unassign(hri, true);

      // region may still be assigned now since it's closing,
      // let's check if it's assigned after it's out of transition
      am.waitOnRegionToClearRegionsInTransition(hri);

      // region should be closed and re-assigned
      assertTrue(am.waitForAssignment(hri));
      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
    } finally {
      MockRegionObserver.enabled = false;
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
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaEditor.addRegionToMeta(meta, hri);

      MockLoadBalancer.controledRegion = hri.getEncodedName();

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertFalse(am.waitForAssignment(hri));

      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_OPEN, state.getState());

      MockLoadBalancer.controledRegion = null;
      master.assignRegion(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
    } finally {
      MockLoadBalancer.controledRegion = null;
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests region open failure which is not recoverable
   */
  @Test (timeout=60000)
  public void testOpenFailedUnrecoverable() throws Exception {
    String table = "testOpenFailedUnrecoverable";
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(FAMILY));
      admin.createTable(desc);

      HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
      HRegionInfo hri = new HRegionInfo(
        desc.getName(), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      MetaEditor.addRegionToMeta(meta, hri);

      FileSystem fs = FileSystem.get(conf);
      Path tableDir= FSUtils.getTablePath(FSUtils.getRootDir(conf), table);
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

      // remove the blocking file, so that region can be opened
      fs.delete(regionDir, true);
      master.assignRegion(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  @Test (timeout=60000)
  public void testSSHWhenDisablingTableRegionsInOpeningOrPendingOpenState() throws Exception {
    final String table = "testSSHWhenDisablingTableRegionsInOpeningOrPendingOpenState";
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
      ZKAssign.createNodeOffline(TEST_UTIL.getHBaseCluster().getMaster().getZooKeeper(), hri,
        destServerName);
      ZKAssign.transitionNodeOpening(TEST_UTIL.getHBaseCluster().getMaster().getZooKeeper(), hri,
        destServerName);
      am.getZKTable().setDisablingTable(table);
      List<HRegionInfo> toAssignRegions = am.processServerShutdown(destServerName);
      assertTrue("Regions to be assigned should be empty.", toAssignRegions.isEmpty());
      assertTrue("Regions to be assigned should be empty.", am.getRegionStates()
          .getRegionState(hri).isOffline());
    } finally {
      if (hri != null && serverName != null) {
        am.regionOnline(hri, serverName);
      }
      am.getZKTable().setDisabledTable(table);
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  static class MockLoadBalancer extends StochasticLoadBalancer {
    // For this region, if specified, always assign to nowhere
    static volatile String controledRegion = null;

    @Override
    public ServerName randomAssignment(HRegionInfo regionInfo,
        List<ServerName> servers) {
      if (regionInfo.getEncodedName().equals(controledRegion)) {
        return null;
      }
      return super.randomAssignment(regionInfo, servers);
    }
  }

  public static class MockRegionObserver extends BaseRegionObserver {
    // If enabled, fail all preClose calls
    static volatile boolean enabled = false;

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c,
        boolean abortRequested) throws IOException {
      if (enabled) throw new IOException("fail preClose from coprocessor");
    }
  }
}
