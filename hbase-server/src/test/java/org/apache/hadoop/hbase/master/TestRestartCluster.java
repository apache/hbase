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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestRestartCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRestartCluster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRestartCluster.class);
  private HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName[] TABLES = {
      TableName.valueOf("restartTableOne"),
      TableName.valueOf("restartTableTwo"),
      TableName.valueOf("restartTableThree")
  };
  private static final byte[] FAMILY = Bytes.toBytes("family");

  @After public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testClusterRestart() throws Exception {
    UTIL.startMiniCluster(3);
    while (!UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
      Threads.sleep(1);
    }
    LOG.info("\n\nCreating tables");
    for(TableName TABLE : TABLES) {
      UTIL.createTable(TABLE, FAMILY);
    }
    for(TableName TABLE : TABLES) {
      UTIL.waitTableEnabled(TABLE);
    }

    List<RegionInfo> allRegions = MetaTableAccessor.getAllRegions(UTIL.getConnection(), false);
    assertEquals(4, allRegions.size());

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
    assertEquals(4, allRegions.size());
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
  public void testRoundRobinAssignmentOnRestart() throws Exception {
    final int regionNum = 10;
    final int rsNum = 2;
    UTIL.startMiniCluster(rsNum);
    // Turn off balancer
    UTIL.getMiniHBaseCluster().getMaster().getMasterRpcServices().synchronousBalanceSwitch(false);
    LOG.info("\n\nCreating tables");
    for (TableName TABLE : TABLES) {
      UTIL.createMultiRegionTable(TABLE, FAMILY, regionNum);
    }
    // Wait until all regions are assigned
    for (TableName TABLE : TABLES) {
      UTIL.waitTableEnabled(TABLE);
    }
    UTIL.waitUntilNoRegionsInTransition(120000);

    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> threads = cluster.getLiveRegionServerThreads();
    assertEquals(rsNum, threads.size());

    ServerName testServer = threads.get(0).getRegionServer().getServerName();
    int port = testServer.getPort();
    List<RegionInfo> regionInfos =
        cluster.getMaster().getAssignmentManager().getRegionStates().getServerNode(testServer)
            .getRegionInfoList();
    LOG.debug("RegionServer {} has {} regions", testServer, regionInfos.size());
    assertTrue(regionInfos.size() >= (TABLES.length * regionNum / rsNum));

    // Restart 1 regionserver
    cluster.stopRegionServer(testServer);
    cluster.waitForRegionServerToStop(testServer, 60000);
    cluster.getConf().setInt(HConstants.REGIONSERVER_PORT, port);
    cluster.startRegionServer();

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    List<ServerName> localServers = master.getServerManager().getOnlineServersList();
    ServerName newTestServer = null;
    for (ServerName serverName : localServers) {
      if (serverName.getAddress().equals(testServer.getAddress())) {
        newTestServer = serverName;
        break;
      }
    }
    assertNotNull(newTestServer);

    // Wait until all regions are assigned
    for (TableName TABLE : TABLES) {
      UTIL.waitTableAvailable(TABLE);
    }
    UTIL.waitUntilNoRegionsInTransition(60000);

    List<RegionInfo> newRegionInfos =
        cluster.getMaster().getAssignmentManager().getRegionStates().getServerNode(newTestServer)
            .getRegionInfoList();
    LOG.debug("RegionServer {} has {} regions", newTestServer, newRegionInfos.size());
    assertTrue("Should not retain all regions when restart",
        newRegionInfos.size() < regionInfos.size());
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
}
