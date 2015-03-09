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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, LargeTests.class})
public class TestRestartCluster {
  private static final Log LOG = LogFactory.getLog(TestRestartCluster.class);
  private HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName[] TABLES = {
      TableName.valueOf("restartTableOne"),
      TableName.valueOf("restartTableTwo"),
      TableName.valueOf("restartTableThree")
  };
  private static final byte [] FAMILY = Bytes.toBytes("family");

  @After public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test (timeout=300000)
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

    List<HRegionInfo> allRegions = MetaTableAccessor.getAllRegions(UTIL.getConnection(), false);
    assertEquals(4, allRegions.size());

    LOG.info("\n\nShutting down cluster");
    UTIL.shutdownMiniHBaseCluster();

    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);

    LOG.info("\n\nStarting cluster the second time");
    UTIL.restartHBaseCluster(3);

    // Need to use a new 'Configuration' so we make a new HConnection.
    // Otherwise we're reusing an HConnection that has gone stale because
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
  @Test (timeout=300000)
  public void testRetainAssignmentOnRestart() throws Exception {
    UTIL.startMiniCluster(2);
    while (!UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
      Threads.sleep(1);
    }
    // Turn off balancer
    UTIL.getMiniHBaseCluster().getMaster().
      getMasterRpcServices().synchronousBalanceSwitch(false);
    LOG.info("\n\nCreating tables");
    for(TableName TABLE : TABLES) {
      UTIL.createTable(TABLE, FAMILY);
    }
    for(TableName TABLE : TABLES) {
      UTIL.waitTableEnabled(TABLE);
    }

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    UTIL.waitUntilNoRegionsInTransition(120000);

    // We don't have to use SnapshotOfRegionAssignmentFromMeta.
    // We use it here because AM used to use it to load all user region placements
    SnapshotOfRegionAssignmentFromMeta snapshot = new SnapshotOfRegionAssignmentFromMeta(
      master.getConnection());
    snapshot.initialize();
    Map<HRegionInfo, ServerName> regionToRegionServerMap
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
    cluster.shutdown();
    cluster.waitUntilShutDown();

    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);

    LOG.info("\n\nStarting cluster the second time with the same ports");
    try {
      cluster.getConf().setInt(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 4);
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
    assertEquals(4, localServers.size());
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
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    int expectedRegions = regionToRegionServerMap.size() + 1;
    while (!master.isInitialized()
        || regionStates.getRegionAssignments().size() != expectedRegions) {
      Threads.sleep(100);
    }

    snapshot =new SnapshotOfRegionAssignmentFromMeta(master.getConnection());
    snapshot.initialize();
    Map<HRegionInfo, ServerName> newRegionToRegionServerMap =
      snapshot.getRegionToRegionServerMap();
    assertEquals(regionToRegionServerMap.size(), newRegionToRegionServerMap.size());
    for (Map.Entry<HRegionInfo, ServerName> entry: newRegionToRegionServerMap.entrySet()) {
      if (TableName.NAMESPACE_TABLE_NAME.equals(entry.getKey().getTable())) continue;
      ServerName oldServer = regionToRegionServerMap.get(entry.getKey());
      ServerName currentServer = entry.getValue();
      assertEquals(oldServer.getHostAndPort(), currentServer.getHostAndPort());
      assertNotEquals(oldServer.getStartcode(), currentServer.getStartcode());
    }
  }
}
