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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
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
    // Using the test load balancer to control region plans
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      TestLoadBalancer.class, LoadBalancer.class);
    conf.setClass(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      TestRegionObserver.class, RegionObserver.class);

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
  @Test
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
   * This tests offlining a region
   */
  @Test
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
  @Test
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

      long timeoutTime = System.currentTimeMillis() + 800;
      while (true) {
        ServerName sn = regionStates.getRegionServerOfRegion(hri);
        if (sn != null && sn.equals(destServerName)) {
          TEST_UTIL.assertRegionOnServer(hri, sn, 200);
          break;
        }
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to move the region in time");
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
    long timeoutTime = System.currentTimeMillis() + 100;
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
   * This tests region close failed
   */
  @Test
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

      TestRegionObserver.enabled = true;
      am.unassign(hri);
      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_CLOSE, state.getState());

      TestRegionObserver.enabled = false;
      am.unassign(hri, true);
      state = am.getRegionStates().getRegionState(hri);
      assertTrue(RegionState.State.FAILED_CLOSE != state.getState());

      am.assign(hri, true, true);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
    } finally {
      TestRegionObserver.enabled = false;
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests region open failed
   */
  @Test
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

      TestLoadBalancer.controledRegion = hri.getEncodedName();

      HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
      master.assignRegion(hri);
      AssignmentManager am = master.getAssignmentManager();
      assertFalse(am.waitForAssignment(hri));

      RegionState state = am.getRegionStates().getRegionState(hri);
      assertEquals(RegionState.State.FAILED_OPEN, state.getState());

      TestLoadBalancer.controledRegion = null;
      master.assignRegion(hri);
      assertTrue(am.waitForAssignment(hri));

      ServerName serverName = master.getAssignmentManager().
        getRegionStates().getRegionServerOfRegion(hri);
      TEST_UTIL.assertRegionOnServer(hri, serverName, 200);
    } finally {
      TestLoadBalancer.controledRegion = null;
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  static class TestLoadBalancer extends StochasticLoadBalancer {
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

  public static class TestRegionObserver extends BaseRegionObserver {
    // If enabled, fail all preClose calls
    static volatile boolean enabled = false;

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c,
        boolean abortRequested) throws IOException {
      if (enabled) throw new IOException("fail preClose from coprocessor");
    }
  }
}
