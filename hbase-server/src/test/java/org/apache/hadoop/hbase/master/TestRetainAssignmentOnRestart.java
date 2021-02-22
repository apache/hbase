/*
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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestRetainAssignmentOnRestart extends AbstractTestRestartCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRetainAssignmentOnRestart.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRetainAssignmentOnRestart.class);

  private static int NUM_OF_RS = 3;

  @Override
  protected boolean splitWALCoordinatedByZk() {
    return true;
  }

  /**
   * This tests retaining assignments on a cluster restart
   */
  @Test
  public void testRetainAssignmentOnClusterRestart() throws Exception {
    setupCluster();
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> threads = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_OF_RS, threads.size());
    int[] rsPorts = new int[NUM_OF_RS];
    for (int i = 0; i < NUM_OF_RS; i++) {
      rsPorts[i] = threads.get(i).getRegionServer().getServerName().getPort();
    }

    // We don't have to use SnapshotOfRegionAssignmentFromMeta. We use it here because AM used to
    // use it to load all user region placements
    SnapshotOfRegionAssignmentFromMeta snapshot =
        new SnapshotOfRegionAssignmentFromMeta(master.getConnection());
    snapshot.initialize();
    Map<RegionInfo, ServerName> regionToRegionServerMap = snapshot.getRegionToRegionServerMap();
    for (ServerName serverName : regionToRegionServerMap.values()) {
      boolean found = false; // Test only, no need to optimize
      for (int k = 0; k < NUM_OF_RS && !found; k++) {
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
    cluster.getConf().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 3);
    master = cluster.startMaster().getMaster();
    for (int i = 0; i < NUM_OF_RS; i++) {
      cluster.getConf().setInt(HConstants.REGIONSERVER_PORT, rsPorts[i]);
      cluster.startRegionServer();
    }

    ensureServersWithSamePort(master, rsPorts);

    // Wait till master is initialized and all regions are assigned
    for (TableName TABLE : TABLES) {
      UTIL.waitTableAvailable(TABLE);
    }
    UTIL.waitUntilNoRegionsInTransition(60000);

    snapshot = new SnapshotOfRegionAssignmentFromMeta(master.getConnection());
    snapshot.initialize();
    Map<RegionInfo, ServerName> newRegionToRegionServerMap = snapshot.getRegionToRegionServerMap();
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

  /**
   * This tests retaining assignments on a single node restart
   */
  @Test
  public void testRetainAssignmentOnSingleRSRestart() throws Exception {
    setupCluster();
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> threads = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_OF_RS, threads.size());
    int[] rsPorts = new int[NUM_OF_RS];
    for (int i = 0; i < NUM_OF_RS; i++) {
      rsPorts[i] = threads.get(i).getRegionServer().getServerName().getPort();
    }

    // We don't have to use SnapshotOfRegionAssignmentFromMeta. We use it here because AM used to
    // use it to load all user region placements
    SnapshotOfRegionAssignmentFromMeta snapshot =
        new SnapshotOfRegionAssignmentFromMeta(master.getConnection());
    snapshot.initialize();
    Map<RegionInfo, ServerName> regionToRegionServerMap = snapshot.getRegionToRegionServerMap();
    for (ServerName serverName : regionToRegionServerMap.values()) {
      boolean found = false; // Test only, no need to optimize
      for (int k = 0; k < NUM_OF_RS && !found; k++) {
        found = serverName.getPort() == rsPorts[k];
      }
      assertTrue(found);
    }

    // Server to be restarted
    ServerName deadRS = threads.get(0).getRegionServer().getServerName();
    LOG.info("\n\nStopping HMaster and {} server", deadRS);
    // Stopping master first so that region server SCP will not be initiated
    cluster.stopMaster(0);
    cluster.waitForMasterToStop(master.getServerName(), 5000);
    cluster.stopRegionServer(deadRS);

    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);

    LOG.info("\n\nStarting HMaster and region server {} second time with the same port", deadRS);
    cluster.getConf().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 3);
    master = cluster.startMaster().getMaster();
    cluster.getConf().setInt(HConstants.REGIONSERVER_PORT, deadRS.getPort());
    cluster.startRegionServer();

    ensureServersWithSamePort(master, rsPorts);

    // Wait till master is initialized and all regions are assigned
    for (TableName TABLE : TABLES) {
      UTIL.waitTableAvailable(TABLE);
    }
    UTIL.waitUntilNoRegionsInTransition(60000);

    snapshot = new SnapshotOfRegionAssignmentFromMeta(master.getConnection());
    snapshot.initialize();
    Map<RegionInfo, ServerName> newRegionToRegionServerMap = snapshot.getRegionToRegionServerMap();
    assertEquals(regionToRegionServerMap.size(), newRegionToRegionServerMap.size());
    for (Map.Entry<RegionInfo, ServerName> entry : newRegionToRegionServerMap.entrySet()) {
      ServerName oldServer = regionToRegionServerMap.get(entry.getKey());
      ServerName currentServer = entry.getValue();
      LOG.info(
        "Key=" + entry.getKey() + " oldServer=" + oldServer + ", currentServer=" + currentServer);
      assertEquals(entry.getKey().toString(), oldServer.getAddress(), currentServer.getAddress());

      if (deadRS.getPort() == oldServer.getPort()) {
        // Restarted RS start code wont be same
        assertNotEquals(oldServer.getStartcode(), currentServer.getStartcode());
      } else {
        assertEquals(oldServer.getStartcode(), currentServer.getStartcode());
      }
    }
  }

  private void setupCluster() throws Exception, IOException, InterruptedException {
    // Set Zookeeper based connection registry since we will stop master and start a new master
    // without populating the underlying config for the connection.
    UTIL.getConfiguration().set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    // Enable retain assignment during ServerCrashProcedure
    UTIL.getConfiguration().setBoolean(ServerCrashProcedure.MASTER_SCP_RETAIN_ASSIGNMENT, true);
    UTIL.startMiniCluster(NUM_OF_RS);

    // Turn off balancer
    UTIL.getMiniHBaseCluster().getMaster().getMasterRpcServices().synchronousBalanceSwitch(false);

    LOG.info("\n\nCreating tables");
    for (TableName TABLE : TABLES) {
      UTIL.createTable(TABLE, FAMILY);
    }
    for (TableName TABLE : TABLES) {
      UTIL.waitTableEnabled(TABLE);
    }

    UTIL.getMiniHBaseCluster().getMaster();
    UTIL.waitUntilNoRegionsInTransition(60000);
  }

  private void ensureServersWithSamePort(HMaster master, int[] rsPorts) {
    // Make sure live regionservers are on the same host/port
    List<ServerName> localServers = master.getServerManager().getOnlineServersList();
    assertEquals(NUM_OF_RS, localServers.size());
    for (int i = 0; i < NUM_OF_RS; i++) {
      boolean found = false;
      for (ServerName serverName : localServers) {
        if (serverName.getPort() == rsPorts[i]) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }
}