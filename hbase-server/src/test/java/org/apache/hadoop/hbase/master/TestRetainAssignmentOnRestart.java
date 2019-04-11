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

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
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

  @Override
  protected boolean splitWALCoordinatedByZk() {
    return true;
  }

  /**
   * This tests retaining assignments on a cluster restart
   */
  @Test
  public void test() throws Exception {
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
    UTIL.waitUntilNoRegionsInTransition(60000);

    // We don't have to use SnapshotOfRegionAssignmentFromMeta.
    // We use it here because AM used to use it to load all user region placements
    SnapshotOfRegionAssignmentFromMeta snapshot =
      new SnapshotOfRegionAssignmentFromMeta(master.getConnection());
    snapshot.initialize();
    Map<RegionInfo, ServerName> regionToRegionServerMap = snapshot.getRegionToRegionServerMap();

    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> threads = cluster.getLiveRegionServerThreads();
    assertEquals(2, threads.size());
    int[] rsPorts = new int[3];
    for (int i = 0; i < 2; i++) {
      rsPorts[i] = threads.get(i).getRegionServer().getServerName().getPort();
    }
    rsPorts[2] = cluster.getMaster().getServerName().getPort();
    for (ServerName serverName : regionToRegionServerMap.values()) {
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
    cluster.getConf().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 3);
    master = cluster.startMaster().getMaster();
    for (int i = 0; i < 3; i++) {
      cluster.getConf().setInt(HConstants.REGIONSERVER_PORT, rsPorts[i]);
      cluster.startRegionServer();
    }

    // Make sure live regionservers are on the same host/port
    List<ServerName> localServers = master.getServerManager().getOnlineServersList();
    assertEquals(3, localServers.size());
    for (int i = 0; i < 3; i++) {
      boolean found = false;
      for (ServerName serverName : localServers) {
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

}
