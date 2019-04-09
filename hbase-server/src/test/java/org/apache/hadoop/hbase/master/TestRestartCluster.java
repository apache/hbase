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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.master.assignment.ServerStateNode;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({ MasterTests.class, LargeTests.class })
public class TestRestartCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRestartCluster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRestartCluster.class);
  private HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Parameterized.Parameter
  public boolean splitWALCoordinatedByZK;

  private static final TableName[] TABLES = {
      TableName.valueOf("restartTableOne"),
      TableName.valueOf("restartTableTwo"),
      TableName.valueOf("restartTableThree")
  };
  private static final byte[] FAMILY = Bytes.toBytes("family");

  @Before
  public void setup() throws Exception {
    LOG.info("WAL splitting coordinated by zk? {}", splitWALCoordinatedByZK);
    UTIL.getConfiguration().setBoolean(HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK,
      splitWALCoordinatedByZK);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private ServerStateNode getServerStateNode(ServerName serverName) {
    return UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
      .getServerNode(serverName);
  }

  @Test
  public void testClusterRestartFailOver() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.waitFor(60000, () -> UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    // wait for all SCPs finished
    UTIL.waitFor(60000, () -> UTIL.getHBaseCluster().getMaster().getProcedures().stream()
      .noneMatch(p -> p instanceof ServerCrashProcedure));
    TableName tableName = TABLES[0];
    ServerName testServer = UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    UTIL.waitFor(10000, () -> getServerStateNode(testServer) != null);
    ServerStateNode serverNode = getServerStateNode(testServer);
    Assert.assertNotNull(serverNode);
    Assert.assertTrue("serverNode should be ONLINE when cluster runs normally",
      serverNode.isInState(ServerState.ONLINE));
    UTIL.createMultiRegionTable(tableName, FAMILY);
    UTIL.waitTableEnabled(tableName);
    Table table = UTIL.getConnection().getTable(tableName);
    for (int i = 0; i < 100; i++) {
      UTIL.loadTable(table, FAMILY);
    }
    List<Integer> ports =
        UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServersList().stream()
            .map(serverName -> serverName.getPort()).collect(Collectors.toList());
    LOG.info("Shutting down cluster");
    UTIL.getHBaseCluster().killAll();
    UTIL.getHBaseCluster().waitUntilShutDown();
    LOG.info("Starting cluster the second time");
    UTIL.restartHBaseCluster(3, ports);
    UTIL.waitFor(10000, () -> UTIL.getHBaseCluster().getMaster().isInitialized());
    serverNode = UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
        .getServerNode(testServer);
    Assert.assertNotNull("serverNode should not be null when restart whole cluster", serverNode);
    Assert.assertFalse(serverNode.isInState(ServerState.ONLINE));
    LOG.info("start to find the procedure of SCP for the severName we choose");
    UTIL.waitFor(60000,
      () -> UTIL.getHBaseCluster().getMaster().getProcedures().stream()
          .anyMatch(procedure -> (procedure instanceof ServerCrashProcedure)
              && ((ServerCrashProcedure) procedure).getServerName().equals(testServer)));
    Assert.assertFalse("serverNode should not be ONLINE during SCP processing",
      serverNode.isInState(ServerState.ONLINE));
    LOG.info("start to submit the SCP for the same serverName {} which should fail", testServer);
    Assert.assertFalse(
      UTIL.getHBaseCluster().getMaster().getServerManager().expireServer(testServer));
    Procedure<?> procedure = UTIL.getHBaseCluster().getMaster().getProcedures().stream()
        .filter(p -> (p instanceof ServerCrashProcedure)
            && ((ServerCrashProcedure) p).getServerName().equals(testServer))
        .findAny().get();
    UTIL.waitFor(60000, () -> procedure.isFinished());
    LOG.info("even when the SCP is finished, the duplicate SCP should not be scheduled for {}",
      testServer);
    Assert.assertFalse(
      UTIL.getHBaseCluster().getMaster().getServerManager().expireServer(testServer));
    serverNode = UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
        .getServerNode(testServer);
    Assert.assertNull("serverNode should be deleted after SCP finished", serverNode);
  }

  @Test
  public void testClusterRestart() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.waitFor(60000, () -> UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    LOG.info("\n\nCreating tables");
    for(TableName TABLE : TABLES) {
      UTIL.createTable(TABLE, FAMILY);
    }
    for(TableName TABLE : TABLES) {
      UTIL.waitTableEnabled(TABLE);
    }

    List<RegionInfo> allRegions = MetaTableAccessor.getAllRegions(UTIL.getConnection(), false);
    assertEquals(3, allRegions.size());

    LOG.info("\n\nShutting down cluster");
    UTIL.shutdownMiniHBaseCluster();

    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);

    LOG.info("\n\nStarting cluster the second time");
    UTIL.restartHBaseCluster(3);

    // Need to use a new 'Configuration' so we make a new Connection.
    // Otherwise we're reusing an Connection that has gone stale because
    // the shutdown of the cluster also called shut of the connection.
    allRegions = MetaTableAccessor.getAllRegions(UTIL.getConnection(), false);
    assertEquals(3, allRegions.size());
    LOG.info("\n\nWaiting for tables to be available");
    for(TableName TABLE: TABLES) {
      try {
        UTIL.createTable(TABLE, FAMILY);
        assertTrue("Able to create table that should already exist", false);
      } catch(TableExistsException tee) {
        LOG.info("Table already exists as expected");
      }
      UTIL.waitTableAvailable(TABLE);
    }
  }

  /**
   * This tests retaining assignments on a cluster restart
   */
  @Test
  public void testRetainAssignmentOnRestart() throws Exception {
    UTIL.startMiniCluster(2);
    // Turn off balancer
    UTIL.getMiniHBaseCluster().getMaster().getMasterRpcServices().synchronousBalanceSwitch(false);
    LOG.info("\n\nCreating tables");
    for (TableName TABLE : TABLES) {
      UTIL.createTable(TABLE, FAMILY);
    }
    for (TableName TABLE : TABLES) {
      UTIL.waitTableEnabled(TABLE);
    }

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    UTIL.waitUntilNoRegionsInTransition(120000);

    // We don't have to use SnapshotOfRegionAssignmentFromMeta.
    // We use it here because AM used to use it to load all user region placements
    SnapshotOfRegionAssignmentFromMeta snapshot = new SnapshotOfRegionAssignmentFromMeta(
      master.getConnection());
    snapshot.initialize();
    Map<RegionInfo, ServerName> regionToRegionServerMap
      = snapshot.getRegionToRegionServerMap();

    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> threads = cluster.getLiveRegionServerThreads();
    assertEquals(2, threads.size());
    int[] rsPorts = new int[3];
    for (int i = 0; i < 2; i++) {
      rsPorts[i] = threads.get(i).getRegionServer().getServerName().getPort();
    }
    rsPorts[2] = cluster.getMaster().getServerName().getPort();
    for (ServerName serverName: regionToRegionServerMap.values()) {
      boolean found = false; // Test only, no need to optimize
      for (int k = 0; k < 3 && !found; k++) {
        found = serverName.getPort() == rsPorts[k];
      }
      assertTrue(found);
    }

    LOG.info("\n\nShutting down HBase cluster");
    cluster.stopMaster(0);
    cluster.shutdown();
    cluster.waitUntilShutDown();

    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);

    LOG.info("\n\nStarting cluster the second time with the same ports");
    try {
      cluster.getConf().setInt(
          ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 3);
      master = cluster.startMaster().getMaster();
      for (int i = 0; i < 3; i++) {
        cluster.getConf().setInt(HConstants.REGIONSERVER_PORT, rsPorts[i]);
        cluster.startRegionServer();
      }
    } finally {
      // Reset region server port so as not to conflict with other tests
      cluster.getConf().setInt(HConstants.REGIONSERVER_PORT, 0);
      cluster.getConf().setInt(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 2);
    }

    // Make sure live regionservers are on the same host/port
    List<ServerName> localServers = master.getServerManager().getOnlineServersList();
    assertEquals(3, localServers.size());
    for (int i = 0; i < 3; i++) {
      boolean found = false;
      for (ServerName serverName: localServers) {
        if (serverName.getPort() == rsPorts[i]) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }

    // Wait till master is initialized and all regions are assigned
    for (TableName TABLE : TABLES) {
      UTIL.waitTableAvailable(TABLE);
    }

    snapshot = new SnapshotOfRegionAssignmentFromMeta(master.getConnection());
    snapshot.initialize();
    Map<RegionInfo, ServerName> newRegionToRegionServerMap =
      snapshot.getRegionToRegionServerMap();
    assertEquals(regionToRegionServerMap.size(), newRegionToRegionServerMap.size());
    for (Map.Entry<RegionInfo, ServerName> entry : newRegionToRegionServerMap.entrySet()) {
      ServerName oldServer = regionToRegionServerMap.get(entry.getKey());
      ServerName currentServer = entry.getValue();
      LOG.info(
        "Key=" + entry.getKey() + " oldServer=" + oldServer + ", currentServer=" + currentServer);
      assertEquals(entry.getKey().toString(), oldServer.getAddress(), currentServer.getAddress());
      assertNotEquals(oldServer.getStartcode(), currentServer.getStartcode());
    }
  }

  @Test
  public void testNewStartedRegionServerVersion() throws Exception {
    UTIL.startMiniCluster(1);

    // Start 3 new region server
    Thread t = new Thread(() -> {
      for (int i = 0; i < 3; i++) {
        try {
          JVMClusterUtil.RegionServerThread newRS = UTIL.getMiniHBaseCluster().startRegionServer();
          newRS.waitForServerOnline();
        } catch (IOException e) {
          LOG.error("Failed to start a new RS", e);
        }
      }
    });
    t.start();

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    while (t.isAlive()) {
      List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
      for (ServerName serverName : serverNames) {
        assertNotEquals(0, master.getServerManager().getVersionNumber(serverName));
      }
      Thread.sleep(100);
    }
  }

  @Parameterized.Parameters
  public static Collection<?> coordinatedByZK() {
    return Arrays.asList(false, true);
  }
}
