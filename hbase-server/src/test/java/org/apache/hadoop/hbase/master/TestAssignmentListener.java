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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.getZNodePaths();
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestAssignmentListener {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAssignmentListener.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentListener.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  private static final Abortable abortable = new Abortable() {
    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void abort(String why, Throwable e) {
    }
  };
  static class DummyListener {
    protected AtomicInteger modified = new AtomicInteger(0);

    public void awaitModifications(int count) throws InterruptedException {
      while (!modified.compareAndSet(count, 0)) {
        Thread.sleep(100);
      }
    }
  }

  static class DummyAssignmentListener extends DummyListener implements AssignmentListener {
    private AtomicInteger closeCount = new AtomicInteger(0);
    private AtomicInteger openCount = new AtomicInteger(0);

    public DummyAssignmentListener() {
    }

    @Override
    public void regionOpened(final RegionInfo regionInfo, final ServerName serverName) {
      LOG.info("Assignment open region=" + regionInfo + " server=" + serverName);
      openCount.incrementAndGet();
      modified.incrementAndGet();
    }

    @Override
    public void regionClosed(final RegionInfo regionInfo) {
      LOG.info("Assignment close region=" + regionInfo);
      closeCount.incrementAndGet();
      modified.incrementAndGet();
    }

    public void reset() {
      openCount.set(0);
      closeCount.set(0);
    }

    public int getLoadCount() {
      return openCount.get();
    }

    public int getCloseCount() {
      return closeCount.get();
    }
  }

  static class DummyServerListener extends DummyListener implements ServerListener {
    private AtomicInteger removedCount = new AtomicInteger(0);
    private AtomicInteger addedCount = new AtomicInteger(0);

    public DummyServerListener() {
    }

    @Override
    public void serverAdded(final ServerName serverName) {
      LOG.info("Server added " + serverName);
      addedCount.incrementAndGet();
      modified.incrementAndGet();
    }

    @Override
    public void serverRemoved(final ServerName serverName) {
      LOG.info("Server removed " + serverName);
      removedCount.incrementAndGet();
      modified.incrementAndGet();
    }

    public void reset() {
      addedCount.set(0);
      removedCount.set(0);
    }

    public int getAddedCount() {
      return addedCount.get();
    }

    public int getRemovedCount() {
      return removedCount.get();
    }
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testServerListener() throws IOException, InterruptedException {
    ServerManager serverManager = TEST_UTIL.getHBaseCluster().getMaster().getServerManager();

    DummyServerListener listener = new DummyServerListener();
    serverManager.registerListener(listener);
    try {
      MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();

      // Start a new Region Server
      miniCluster.startRegionServer();
      listener.awaitModifications(1);
      assertEquals(1, listener.getAddedCount());
      assertEquals(0, listener.getRemovedCount());

      // Start another Region Server
      listener.reset();
      miniCluster.startRegionServer();
      listener.awaitModifications(1);
      assertEquals(1, listener.getAddedCount());
      assertEquals(0, listener.getRemovedCount());

      int nrs = miniCluster.getRegionServerThreads().size();

      // Stop a Region Server
      listener.reset();
      miniCluster.stopRegionServer(nrs - 1);
      listener.awaitModifications(1);
      assertEquals(0, listener.getAddedCount());
      assertEquals(1, listener.getRemovedCount());

      // Stop another Region Server
      listener.reset();
      miniCluster.stopRegionServer(nrs - 2);
      listener.awaitModifications(1);
      assertEquals(0, listener.getAddedCount());
      assertEquals(1, listener.getRemovedCount());
    } finally {
      serverManager.unregisterListener(listener);
    }
  }

  @Test
  public void testAssignmentListener() throws IOException, InterruptedException {
    AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    Admin admin = TEST_UTIL.getAdmin();

    DummyAssignmentListener listener = new DummyAssignmentListener();
    am.registerListener(listener);
    try {
      final TableName tableName = TableName.valueOf(name.getMethodName());
      final byte[] FAMILY = Bytes.toBytes("cf");

      // Create a new table, with a single region
      LOG.info("Create Table");
      TEST_UTIL.createTable(tableName, FAMILY);
      listener.awaitModifications(1);
      assertEquals(1, listener.getLoadCount());
      assertEquals(0, listener.getCloseCount());

      // Add some data
      Table table = TEST_UTIL.getConnection().getTable(tableName);
      try {
        for (int i = 0; i < 10; ++i) {
          byte[] key = Bytes.toBytes("row-" + i);
          Put put = new Put(key);
          put.addColumn(FAMILY, null, key);
          table.put(put);
        }
      } finally {
        table.close();
      }

      // Split the table in two
      LOG.info("Split Table");
      listener.reset();
      admin.split(tableName, Bytes.toBytes("row-3"));
      listener.awaitModifications(3);
      assertEquals(2, listener.getLoadCount());     // daughters added
      assertEquals(1, listener.getCloseCount());    // parent removed

      // Wait for the Regions to be mergeable
      MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();
      int mergeable = 0;
      while (mergeable < 2) {
        Thread.sleep(100);
        admin.majorCompact(tableName);
        mergeable = 0;
        for (JVMClusterUtil.RegionServerThread regionThread: miniCluster.getRegionServerThreads()) {
          for (Region region: regionThread.getRegionServer().getRegions(tableName)) {
            mergeable += ((HRegion)region).isMergeable() ? 1 : 0;
          }
        }
      }

      // Merge the two regions
      LOG.info("Merge Regions");
      listener.reset();
      List<RegionInfo> regions = admin.getRegions(tableName);
      assertEquals(2, regions.size());
      boolean sameServer = areAllRegionsLocatedOnSameServer(tableName);
      // If the regions are located by different server, we need to move
      // regions to same server before merging. So the expected modifications
      // will increaes to 5. (open + close)
      final int expectedModifications = sameServer ? 3 : 5;
      final int expectedLoadCount = sameServer ? 1 : 2;
      final int expectedCloseCount = sameServer ? 2 : 3;
      admin.mergeRegionsAsync(regions.get(0).getEncodedNameAsBytes(),
        regions.get(1).getEncodedNameAsBytes(), true);
      listener.awaitModifications(expectedModifications);
      assertEquals(1, admin.getTableRegions(tableName).size());
      assertEquals(expectedLoadCount, listener.getLoadCount());     // new merged region added
      assertEquals(expectedCloseCount, listener.getCloseCount());    // daughters removed

      // Delete the table
      LOG.info("Drop Table");
      listener.reset();
      TEST_UTIL.deleteTable(tableName);
      listener.awaitModifications(1);
      assertEquals(0, listener.getLoadCount());
      assertEquals(1, listener.getCloseCount());
    } finally {
      am.unregisterListener(listener);
    }
  }

  private boolean areAllRegionsLocatedOnSameServer(TableName TABLE_NAME) {
    MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();
    int serverCount = 0;
    for (JVMClusterUtil.RegionServerThread regionThread: miniCluster.getRegionServerThreads()) {
      if (!regionThread.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        ++serverCount;
      }
      if (serverCount > 1) {
        return false;
      }
    }
    return serverCount == 1;
  }

  @Test
  public void testAddNewServerThatExistsInDraining() throws Exception {
    // Under certain circumstances, such as when we failover to the Backup
    // HMaster, the DrainingServerTracker is started with existing servers in
    // draining before all of the Region Servers register with the
    // ServerManager as "online".  This test is to ensure that Region Servers
    // are properly added to the ServerManager.drainingServers when they
    // register with the ServerManager under these circumstances.
    Configuration conf = TEST_UTIL.getConfiguration();
    ZKWatcher zooKeeper = new ZKWatcher(conf,
        "zkWatcher-NewServerDrainTest", abortable, true);
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String drainingZNode = ZNodePaths.joinZNode(baseZNode,
        conf.get("zookeeper.znode.draining.rs", "draining"));

    HMaster master = Mockito.mock(HMaster.class);
    Mockito.when(master.getConfiguration()).thenReturn(conf);

    ServerName SERVERNAME_A = ServerName.valueOf("mockserverbulk_a.org", 1000, 8000);
    ServerName SERVERNAME_B = ServerName.valueOf("mockserverbulk_b.org", 1001, 8000);
    ServerName SERVERNAME_C = ServerName.valueOf("mockserverbulk_c.org", 1002, 8000);

    // We'll start with 2 servers in draining that existed before the
    // HMaster started.
    ArrayList<ServerName> drainingServers = new ArrayList<>();
    drainingServers.add(SERVERNAME_A);
    drainingServers.add(SERVERNAME_B);

    // We'll have 2 servers that come online AFTER the DrainingServerTracker
    // is started (just as we see when we failover to the Backup HMaster).
    // One of these will already be a draining server.
    HashMap<ServerName, ServerLoad> onlineServers = new HashMap<>();
    onlineServers.put(SERVERNAME_A, new ServerLoad(ServerMetricsBuilder.of(SERVERNAME_A)));
    onlineServers.put(SERVERNAME_C, new ServerLoad(ServerMetricsBuilder.of(SERVERNAME_C)));

    // Create draining znodes for the draining servers, which would have been
    // performed when the previous HMaster was running.
    for (ServerName sn : drainingServers) {
      String znode = ZNodePaths.joinZNode(drainingZNode, sn.getServerName());
      ZKUtil.createAndFailSilent(zooKeeper, znode);
    }

    // Now, we follow the same order of steps that the HMaster does to setup
    // the ServerManager, RegionServerTracker, and DrainingServerTracker.
    ServerManager serverManager = new ServerManager(master);

    RegionServerTracker regionServerTracker = new RegionServerTracker(
        zooKeeper, master, serverManager);
    regionServerTracker.start();

    DrainingServerTracker drainingServerTracker = new DrainingServerTracker(
        zooKeeper, master, serverManager);
    drainingServerTracker.start();

    // Confirm our ServerManager lists are empty.
    Assert.assertEquals(new HashMap<ServerName, ServerLoad>(), serverManager.getOnlineServers());
    Assert.assertEquals(new ArrayList<ServerName>(), serverManager.getDrainingServersList());

    // checkAndRecordNewServer() is how servers are added to the ServerManager.
    ArrayList<ServerName> onlineDrainingServers = new ArrayList<>();
    for (ServerName sn : onlineServers.keySet()){
      // Here's the actual test.
      serverManager.checkAndRecordNewServer(sn, onlineServers.get(sn));
      if (drainingServers.contains(sn)){
        onlineDrainingServers.add(sn);  // keeping track for later verification
      }
    }

    // Verify the ServerManager lists are correctly updated.
    Assert.assertEquals(onlineServers, serverManager.getOnlineServers());
    Assert.assertEquals(onlineDrainingServers, serverManager.getDrainingServersList());
  }
}
