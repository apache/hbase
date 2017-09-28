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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Maps;

@Category(LargeTests.class)
public class TestFavoredStochasticBalancerPickers extends BalancerTestBase {

  private static final Log LOG = LogFactory.getLog(TestFavoredStochasticBalancerPickers.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int SLAVES = 6;
  private static final int REGIONS = SLAVES * 3;
  private static Configuration conf;

  private Admin admin;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Enable favored nodes based load balancer
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        LoadOnlyFavoredStochasticBalancer.class, LoadBalancer.class);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 30000);
    conf.setInt("hbase.master.balancer.stochastic.moveCost", 0);
    conf.setBoolean("hbase.master.balancer.stochastic.execute.maxSteps", true);
    conf.set(BaseLoadBalancer.TABLES_ON_MASTER, "none");
  }

  @Before
  public void startCluster() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
    TEST_UTIL.getDFSCluster().waitClusterUp();
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster(120*1000);
    admin = TEST_UTIL.getAdmin();
    admin.setBalancerRunning(false, true);
  }

  @After
  public void stopCluster() throws Exception {
    TEST_UTIL.cleanupTestDir();
    TEST_UTIL.shutdownMiniCluster();
  }


  @Ignore @Test
  public void testPickers() throws Exception {

    TableName tableName = TableName.valueOf("testPickers");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), REGIONS);
    TEST_UTIL.loadTable(admin.getConnection().getTable(tableName), HConstants.CATALOG_FAMILY);
    admin.flush(tableName);

    ServerName masterServerName = TEST_UTIL.getMiniHBaseCluster().getServerHoldingMeta();
    final ServerName mostLoadedServer = getRSWithMaxRegions(Lists.newArrayList(masterServerName));
    assertNotNull(mostLoadedServer);
    int numRegions = admin.getOnlineRegions(mostLoadedServer).size();
    ServerName source = getRSWithMaxRegions(Lists.newArrayList(masterServerName, mostLoadedServer));
    assertNotNull(source);
    int regionsToMove = admin.getOnlineRegions(source).size()/2;
    List<RegionInfo> hris = admin.getRegions(source);
    for (int i = 0; i < regionsToMove; i++) {
      admin.move(hris.get(i).getEncodedNameAsBytes(), Bytes.toBytes(mostLoadedServer.getServerName()));
      LOG.info("Moving region: " + hris.get(i).getRegionNameAsString() + " to " + mostLoadedServer);
    }
    final int finalRegions = numRegions + regionsToMove;
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        int numRegions = TEST_UTIL.getAdmin().getOnlineRegions(mostLoadedServer).size();
        return (numRegions == finalRegions);
      }
    });
    TEST_UTIL.getHBaseCluster().startRegionServerAndWait(60000);

    Map<ServerName, List<RegionInfo>> serverAssignments = Maps.newHashMap();
    ClusterStatus status = admin.getClusterStatus(EnumSet.of(Option.LIVE_SERVERS));
    for (ServerName sn : status.getServers()) {
      if (!ServerName.isSameAddress(sn, masterServerName)) {
        serverAssignments.put(sn, admin.getRegions(sn));
      }
    }
    RegionLocationFinder regionFinder = new RegionLocationFinder();
    regionFinder.setClusterStatus(admin.getClusterStatus(EnumSet.of(Option.LIVE_SERVERS)));
    regionFinder.setConf(conf);
    regionFinder.setServices(TEST_UTIL.getMiniHBaseCluster().getMaster());
    Cluster cluster = new Cluster(serverAssignments, null, regionFinder, new RackManager(conf));
    LoadOnlyFavoredStochasticBalancer balancer = (LoadOnlyFavoredStochasticBalancer) TEST_UTIL
        .getMiniHBaseCluster().getMaster().getLoadBalancer();
    FavoredNodesManager fnm = TEST_UTIL.getMiniHBaseCluster().getMaster().getFavoredNodesManager();
    cluster.sortServersByRegionCount();
    Integer[] servers = cluster.serverIndicesSortedByRegionCount;
    LOG.info("Servers sorted by region count:" + Arrays.toString(servers));
    LOG.info("Cluster dump: " + cluster);
    if (!mostLoadedServer.equals(cluster.servers[servers[servers.length -1]])) {
      LOG.error("Most loaded server: " + mostLoadedServer + " does not match: "
          + cluster.servers[servers[servers.length -1]]);
    }
    assertEquals(mostLoadedServer, cluster.servers[servers[servers.length -1]]);
    FavoredStochasticBalancer.FavoredNodeLoadPicker loadPicker = balancer.new FavoredNodeLoadPicker();
    boolean userRegionPicked = false;
    for (int i = 0; i < 100; i++) {
      if (userRegionPicked) {
        break;
      } else {
        Cluster.Action action = loadPicker.generate(cluster);
        if (action.type == Cluster.Action.Type.MOVE_REGION) {
          Cluster.MoveRegionAction moveRegionAction = (Cluster.MoveRegionAction) action;
          RegionInfo region = cluster.regions[moveRegionAction.region];
          assertNotEquals(-1, moveRegionAction.toServer);
          ServerName destinationServer = cluster.servers[moveRegionAction.toServer];
          assertEquals(cluster.servers[moveRegionAction.fromServer], mostLoadedServer);
          if (!region.getTable().isSystemTable()) {
            List<ServerName> favNodes = fnm.getFavoredNodes(region);
            assertTrue(favNodes.contains(ServerName.valueOf(destinationServer.getHostAndPort(), -1)));
            userRegionPicked = true;
          }
        }
      }
    }
    assertTrue("load picker did not pick expected regions in 100 iterations.", userRegionPicked);
  }

  private ServerName getRSWithMaxRegions(ArrayList<ServerName> excludeNodes) throws IOException {
    int maxRegions = 0;
    ServerName maxLoadedServer = null;

    for (ServerName sn : admin.getClusterStatus(EnumSet.of(Option.LIVE_SERVERS)).getServers()) {
      if (admin.getOnlineRegions(sn).size() > maxRegions) {
        if (excludeNodes == null || !doesMatchExcludeNodes(excludeNodes, sn)) {
          maxRegions = admin.getOnlineRegions(sn).size();
          maxLoadedServer = sn;
        }
      }
    }
    return maxLoadedServer;
  }

  private boolean doesMatchExcludeNodes(ArrayList<ServerName> excludeNodes, ServerName sn) {
    for (ServerName excludeSN : excludeNodes) {
      if (ServerName.isSameAddress(sn, excludeSN)) {
        return true;
      }
    }
    return false;
  }
}
