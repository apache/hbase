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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

@Category(LargeTests.class)
public class TestFavoredStochasticBalancerPickers extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFavoredStochasticBalancerPickers.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFavoredStochasticBalancerPickers.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int SLAVES = 6;
  private static final int REGIONS = SLAVES * 3;
  private static Configuration conf;

  private Admin admin;
  private MiniHBaseCluster cluster;

  @Rule
  public TestName name = new TestName();

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
    cluster = TEST_UTIL.getMiniHBaseCluster();
    admin = TEST_UTIL.getAdmin();
    admin.setBalancerRunning(false, true);
  }

  @After
  public void stopCluster() throws Exception {
    TEST_UTIL.cleanupTestDir();
    TEST_UTIL.shutdownMiniCluster();
  }


  @Test
  public void testPickers() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    ColumnFamilyDescriptor columnFamilyDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY).build();
    TableDescriptor desc = TableDescriptorBuilder
        .newBuilder(tableName)
        .setColumnFamily(columnFamilyDescriptor)
        .build();
    admin.createTable(desc, Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), REGIONS);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    TEST_UTIL.loadTable(admin.getConnection().getTable(tableName), HConstants.CATALOG_FAMILY);
    admin.flush(tableName);

    HMaster master = cluster.getMaster();
    FavoredNodesManager fnm = master.getFavoredNodesManager();
    ServerName masterServerName = master.getServerName();
    List<ServerName> excludedServers = Lists.newArrayList(masterServerName);
    final ServerName mostLoadedServer = getRSWithMaxRegions(tableName, excludedServers);
    assertNotNull(mostLoadedServer);
    int numRegions = getTableRegionsFromServer(tableName, mostLoadedServer).size();
    excludedServers.add(mostLoadedServer);
    // Lets find another server with more regions to calculate number of regions to move
    ServerName source = getRSWithMaxRegions(tableName, excludedServers);
    assertNotNull(source);
    int regionsToMove = getTableRegionsFromServer(tableName, source).size()/2;

    // Since move only works if the target is part of favored nodes of the region, lets get all
    // regions that are movable to mostLoadedServer
    List<RegionInfo> hris = getRegionsThatCanBeMoved(tableName, mostLoadedServer);
    RegionStates rst = master.getAssignmentManager().getRegionStates();
    for (int i = 0; i < regionsToMove; i++) {
      final RegionInfo regionInfo = hris.get(i);
      admin.move(regionInfo.getEncodedNameAsBytes(), mostLoadedServer);
      LOG.info("Moving region: " + hris.get(i).getRegionNameAsString() + " to " + mostLoadedServer);
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return ServerName.isSameAddress(
              rst.getRegionServerOfRegion(regionInfo), mostLoadedServer);
        }
      });
    }
    final int finalRegions = numRegions + regionsToMove;
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        int numRegions = getTableRegionsFromServer(tableName, mostLoadedServer).size();
        return (numRegions == finalRegions);
      }
    });
    TEST_UTIL.getHBaseCluster().startRegionServerAndWait(60000);

    Map<ServerName, List<RegionInfo>> serverAssignments = Maps.newHashMap();
    ClusterMetrics status = admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
    for (ServerName sn : status.getLiveServerMetrics().keySet()) {
      if (!ServerName.isSameAddress(sn, masterServerName)) {
        serverAssignments.put(sn, getTableRegionsFromServer(tableName, sn));
      }
    }
    RegionLocationFinder regionFinder = new RegionLocationFinder();
    regionFinder.setClusterMetrics(admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
    regionFinder.setConf(conf);
    regionFinder.setServices(TEST_UTIL.getMiniHBaseCluster().getMaster());
    Cluster cluster = new Cluster(serverAssignments, null, regionFinder, new RackManager(conf));
    LoadOnlyFavoredStochasticBalancer balancer = (LoadOnlyFavoredStochasticBalancer) TEST_UTIL
        .getMiniHBaseCluster().getMaster().getLoadBalancer();

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
            assertTrue(favNodes.contains(
              ServerName.valueOf(destinationServer.getAddress(), -1)));
            userRegionPicked = true;
          }
        }
      }
    }
    assertTrue("load picker did not pick expected regions in 100 iterations.", userRegionPicked);
  }

  /*
   * A region can only be moved to one of its favored node. Hence this method helps us to
   * get that list which makes it easy to write non-flaky tests.
   */
  private List<RegionInfo> getRegionsThatCanBeMoved(TableName tableName,
      ServerName serverName) {
    List<RegionInfo> regions = Lists.newArrayList();
    RegionStates rst = cluster.getMaster().getAssignmentManager().getRegionStates();
    FavoredNodesManager fnm = cluster.getMaster().getFavoredNodesManager();
    for (RegionInfo regionInfo : fnm.getRegionsOfFavoredNode(serverName)) {
      if (regionInfo.getTable().equals(tableName) &&
          !ServerName.isSameAddress(rst.getRegionServerOfRegion(regionInfo), serverName)) {
        regions.add(regionInfo);
      }
    }
    return regions;
  }

  private List<RegionInfo> getTableRegionsFromServer(TableName tableName, ServerName source)
      throws IOException {
    List<RegionInfo> regionInfos = Lists.newArrayList();
    HRegionServer regionServer = cluster.getRegionServer(source);
    for (Region region : regionServer.getRegions(tableName)) {
      regionInfos.add(region.getRegionInfo());
    }
    return regionInfos;
  }

  private ServerName getRSWithMaxRegions(TableName tableName, List<ServerName> excludeNodes)
      throws IOException {

    int maxRegions = 0;
    ServerName maxLoadedServer = null;

    for (JVMClusterUtil.RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      List<HRegion> regions = rst.getRegionServer().getRegions(tableName);
      LOG.debug("Server: " + rst.getRegionServer().getServerName() + " regions: " + regions.size());
      if (regions.size() > maxRegions) {
        if (excludeNodes == null ||
            !doesMatchExcludeNodes(excludeNodes, rst.getRegionServer().getServerName())) {
          maxRegions = regions.size();
          maxLoadedServer = rst.getRegionServer().getServerName();
        }
      }
    }
    return maxLoadedServer;
  }

  private boolean doesMatchExcludeNodes(List<ServerName> excludeNodes, ServerName sn) {
    for (ServerName excludeSN : excludeNodes) {
      if (ServerName.isSameAddress(sn, excludeSN)) {
        return true;
      }
    }
    return false;
  }
}
