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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.MockNoopMasterServices;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.ServerLocalityCostFunction;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({FlakeyTests.class, MediumTests.class})
public class TestStochasticLoadBalancer extends BalancerTestBase {
  public static final String REGION_KEY = "testRegion";
  private static final Log LOG = LogFactory.getLog(TestStochasticLoadBalancer.class);

  // Mapping of locality test -> expected locality
  private float[] expectedLocalities = {1.0f, 0.0f, 0.50f, 0.25f, 1.0f};

  /**
   * Data set for testLocalityCost:
   * [test][0][0] = mapping of server to number of regions it hosts
   * [test][region + 1][0] = server that region is hosted on
   * [test][region + 1][server + 1] = locality for region on server
   */

  private int[][][] clusterRegionLocationMocks = new int[][][]{

      // Test 1: each region is entirely on server that hosts it
      new int[][]{
          new int[]{2, 1, 1},
          new int[]{2, 0, 0, 100},   // region 0 is hosted and entirely local on server 2
          new int[]{0, 100, 0, 0},   // region 1 is hosted and entirely on server 0
          new int[]{0, 100, 0, 0},   // region 2 is hosted and entirely on server 0
          new int[]{1, 0, 100, 0},   // region 1 is hosted and entirely on server 1
      },

      // Test 2: each region is 0% local on the server that hosts it
      new int[][]{
          new int[]{1, 2, 1},
          new int[]{0, 0, 0, 100},   // region 0 is hosted and entirely local on server 2
          new int[]{1, 100, 0, 0},   // region 1 is hosted and entirely on server 0
          new int[]{1, 100, 0, 0},   // region 2 is hosted and entirely on server 0
          new int[]{2, 0, 100, 0},   // region 1 is hosted and entirely on server 1
      },

      // Test 3: each region is 25% local on the server that hosts it (and 50% locality is possible)
      new int[][]{
          new int[]{1, 2, 1},
          new int[]{0, 25, 0, 50},   // region 0 is hosted and entirely local on server 2
          new int[]{1, 50, 25, 0},   // region 1 is hosted and entirely on server 0
          new int[]{1, 50, 25, 0},   // region 2 is hosted and entirely on server 0
          new int[]{2, 0, 50, 25},   // region 1 is hosted and entirely on server 1
      },

      // Test 4: each region is 25% local on the server that hosts it (and 100% locality is possible)
      new int[][]{
          new int[]{1, 2, 1},
          new int[]{0, 25, 0, 100},   // region 0 is hosted and entirely local on server 2
          new int[]{1, 100, 25, 0},   // region 1 is hosted and entirely on server 0
          new int[]{1, 100, 25, 0},   // region 2 is hosted and entirely on server 0
          new int[]{2, 0, 100, 25},   // region 1 is hosted and entirely on server 1
      },

      // Test 5: each region is 75% local on the server that hosts it (and 75% locality is possible everywhere)
      new int[][]{
          new int[]{1, 2, 1},
          new int[]{0, 75, 75, 75},   // region 0 is hosted and entirely local on server 2
          new int[]{1, 75, 75, 75},   // region 1 is hosted and entirely on server 0
          new int[]{1, 75, 75, 75},   // region 2 is hosted and entirely on server 0
          new int[]{2, 75, 75, 75},   // region 1 is hosted and entirely on server 1
      },
  };


  @Test
  public void testKeepRegionLoad() throws Exception {

    ServerName sn = ServerName.valueOf("test:8080", 100);
    int numClusterStatusToAdd = 20000;
    for (int i = 0; i < numClusterStatusToAdd; i++) {
      ServerLoad sl = mock(ServerLoad.class);

      RegionLoad rl = mock(RegionLoad.class);
      when(rl.getStores()).thenReturn(i);

      Map<byte[], RegionLoad> regionLoadMap =
          new TreeMap<byte[], RegionLoad>(Bytes.BYTES_COMPARATOR);
      regionLoadMap.put(Bytes.toBytes(REGION_KEY), rl);
      when(sl.getRegionsLoad()).thenReturn(regionLoadMap);

      ClusterStatus clusterStatus = mock(ClusterStatus.class);
      when(clusterStatus.getServers()).thenReturn(Arrays.asList(sn));
      when(clusterStatus.getLoad(sn)).thenReturn(sl);

      loadBalancer.setClusterStatus(clusterStatus);
    }

    String regionNameAsString = HRegionInfo.getRegionNameAsString(Bytes.toBytes(REGION_KEY));
    assertTrue(loadBalancer.loads.get(regionNameAsString) != null);
    assertTrue(loadBalancer.loads.get(regionNameAsString).size() == 15);

    Queue<RegionLoad> loads = loadBalancer.loads.get(regionNameAsString);
    int i = 0;
    while(loads.size() > 0) {
      RegionLoad rl = loads.remove();
      assertEquals(i + (numClusterStatusToAdd - 15), rl.getStores());
      i ++;
    }
  }

  @Test
  public void testNeedBalance() {
    float minCost = conf.getFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.05f);
    conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 1.0f);
    try {
      loadBalancer.setConf(conf);
      for (int[] mockCluster : clusterStateMocks) {
        Map<ServerName, List<HRegionInfo>> servers = mockClusterServers(mockCluster);
        List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
        assertNull(plans);
      }
    } finally {
      // reset config
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", minCost);
      loadBalancer.setConf(conf);
    }
  }

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers should be hosting either floor(average) or
   * ceiling(average)
   *
   * @throws Exception
   */
  @Test
  public void testBalanceCluster() throws Exception {

    for (int[] mockCluster : clusterStateMocks) {
      Map<ServerName, List<HRegionInfo>> servers = mockClusterServers(mockCluster);
      List<ServerAndLoad> list = convertToList(servers);
      LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));
      List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
      List<ServerAndLoad> balancedCluster = reconcile(list, plans, servers);
      LOG.info("Mock Balance : " + printMock(balancedCluster));
      assertClusterAsBalanced(balancedCluster);
      List<RegionPlan> secondPlans =  loadBalancer.balanceCluster(servers);
      assertNull(secondPlans);
      for (Map.Entry<ServerName, List<HRegionInfo>> entry : servers.entrySet()) {
        returnRegions(entry.getValue());
        returnServer(entry.getKey());
      }
    }
  }

  @Test
  public void testLocalityCost() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    MockNoopMasterServices master = new MockNoopMasterServices();
    StochasticLoadBalancer.CostFunction
        costFunction = new ServerLocalityCostFunction(conf, master);

    for (int test = 0; test < clusterRegionLocationMocks.length; test++) {
      int[][] clusterRegionLocations = clusterRegionLocationMocks[test];
      MockCluster cluster = new MockCluster(clusterRegionLocations);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      double expected = 1 - expectedLocalities[test];
      assertEquals(expected, cost, 0.001);
    }
  }

  @Test
  public void testMoveCost() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.MoveCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertEquals(0.0f, cost, 0.001);

      // cluster region number is smaller than maxMoves=600
      cluster.setNumRegions(200);
      cluster.setNumMovedRegions(10);
      cost = costFunction.cost();
      assertEquals(0.05f, cost, 0.001);
      cluster.setNumMovedRegions(100);
      cost = costFunction.cost();
      assertEquals(0.5f, cost, 0.001);
      cluster.setNumMovedRegions(200);
      cost = costFunction.cost();
      assertEquals(1.0f, cost, 0.001);


      // cluster region number is bigger than maxMoves=2500
      cluster.setNumRegions(10000);
      cluster.setNumMovedRegions(250);
      cost = costFunction.cost();
      assertEquals(0.1f, cost, 0.001);
      cluster.setNumMovedRegions(1250);
      cost = costFunction.cost();
      assertEquals(0.5f, cost, 0.001);
      cluster.setNumMovedRegions(2500);
      cost = costFunction.cost();
      assertEquals(1.0f, cost, 0.01);
    }
  }

  @Test
  public void testSkewCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionCountSkewCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      costFunction.init(mockCluster(mockCluster));
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }

    costFunction.init(mockCluster(new int[]{0, 0, 0, 0, 1}));
    assertEquals(0,costFunction.cost(), 0.01);
    costFunction.init(mockCluster(new int[]{0, 0, 0, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.init(mockCluster(new int[]{0, 0, 1, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.init(mockCluster(new int[]{0, 1, 1, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.init(mockCluster(new int[]{1, 1, 1, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.init(mockCluster(new int[]{10000, 0, 0, 0, 0}));
    assertEquals(1, costFunction.cost(), 0.01);
  }

  @Test
  public void testCostAfterUndoAction() {
    final int runs = 10;
    loadBalancer.setConf(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      loadBalancer.initCosts(cluster);
      for (int i = 0; i != runs; ++i) {
        final double expectedCost = loadBalancer.computeCost(cluster, Double.MAX_VALUE);
        Cluster.Action action = loadBalancer.nextAction(cluster);
        cluster.doAction(action);
        loadBalancer.updateCostsWithAction(cluster, action);
        Cluster.Action undoAction = action.undoAction();
        cluster.doAction(undoAction);
        loadBalancer.updateCostsWithAction(cluster, undoAction);
        final double actualCost = loadBalancer.computeCost(cluster, Double.MAX_VALUE);
        assertEquals(expectedCost, actualCost, 0);
      }
    }
  }

  @Test
  public void testTableSkewCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.TableSkewCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }

  @Test
  public void testRegionLoadCost() {
    List<RegionLoad> regionLoads = new ArrayList<>();
    for (int i = 1; i < 5; i++) {
      RegionLoad regionLoad = mock(RegionLoad.class);
      when(regionLoad.getReadRequestsCount()).thenReturn(new Long(i));
      when(regionLoad.getStorefileSizeMB()).thenReturn(i);
      regionLoads.add(regionLoad);
    }

    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.ReadRequestCostFunction readCostFunction =
        new StochasticLoadBalancer.ReadRequestCostFunction(conf);
    double rateResult = readCostFunction.getRegionLoadCost(regionLoads);
    // read requests are treated as a rate so the average rate here is simply 1
    assertEquals(1, rateResult, 0.01);

    StochasticLoadBalancer.StoreFileCostFunction storeFileCostFunction =
        new StochasticLoadBalancer.StoreFileCostFunction(conf);
    double result = storeFileCostFunction.getRegionLoadCost(regionLoads);
    // storefile size cost is simply an average of it's value over time
    assertEquals(2.5, result, 0.01);
  }

  @Test
  public void testCostFromArray() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFromRegionLoadFunction
        costFunction = new StochasticLoadBalancer.MemstoreSizeCostFunction(conf);
    costFunction.init(mockCluster(new int[]{0, 0, 0, 0, 1}));

    double[] statOne = new double[100];
    for (int i =0; i < 100; i++) {
      statOne[i] = 10;
    }
    assertEquals(0, costFunction.costFromArray(statOne), 0.01);

    double[] statTwo= new double[101];
    for (int i =0; i < 100; i++) {
      statTwo[i] = 0;
    }
    statTwo[100] = 100;
    assertEquals(1, costFunction.costFromArray(statTwo), 0.01);

    double[] statThree = new double[200];
    for (int i =0; i < 100; i++) {
      statThree[i] = (0);
      statThree[i+100] = 100;
    }
    assertEquals(0.5, costFunction.costFromArray(statThree), 0.01);
  }

  @Test(timeout =  60000)
  public void testLosingRs() throws Exception {
    int numNodes = 3;
    int numRegions = 20;
    int numRegionsPerServer = 3; //all servers except one
    int replication = 1;
    int numTables = 2;

    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    List<ServerAndLoad> list = convertToList(serverMap);


    List<RegionPlan> plans = loadBalancer.balanceCluster(serverMap);
    assertNotNull(plans);

    // Apply the plan to the mock cluster.
    List<ServerAndLoad> balancedCluster = reconcile(list, plans, serverMap);

    assertClusterAsBalanced(balancedCluster);

    ServerName sn = serverMap.keySet().toArray(new ServerName[serverMap.size()])[0];

    ServerName deadSn = ServerName.valueOf(sn.getHostname(), sn.getPort(), sn.getStartcode() - 100);

    serverMap.put(deadSn, new ArrayList<HRegionInfo>(0));

    plans = loadBalancer.balanceCluster(serverMap);
    assertNull(plans);
  }

  @Test
  public void testReplicaCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }

  @Test
  public void testReplicaCostForReplicas() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);

    int [] servers = new int[] {3,3,3,3,3};
    TreeMap<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(servers);

    BaseLoadBalancer.Cluster cluster;

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWithoutReplicas = costFunction.cost();
    assertEquals(0, costWithoutReplicas, 0);

    // replicate the region from first server to the last server
    HRegionInfo replica1 = RegionReplicaUtil.getRegionInfoForReplica(
      clusterState.firstEntry().getValue().get(0),1);
    clusterState.lastEntry().getValue().add(replica1);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaDifferentServer = costFunction.cost();

    assertEquals(0, costWith1ReplicaDifferentServer, 0);

    // add a third replica to the last server
    HRegionInfo replica2 = RegionReplicaUtil.getRegionInfoForReplica(replica1, 2);
    clusterState.lastEntry().getValue().add(replica2);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaSameServer = costFunction.cost();

    assertTrue(costWith1ReplicaDifferentServer < costWith1ReplicaSameServer);

    // test with replication = 4 for following:

    HRegionInfo replica3;
    Iterator<Entry<ServerName, List<HRegionInfo>>> it;
    Entry<ServerName, List<HRegionInfo>> entry;

    clusterState = mockClusterServers(servers);
    it = clusterState.entrySet().iterator();
    entry = it.next(); //first server
    HRegionInfo hri = entry.getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);
    entry.getValue().add(replica1);
    entry.getValue().add(replica2);
    it.next().getValue().add(replica3); //2nd server

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith3ReplicasSameServer = costFunction.cost();

    clusterState = mockClusterServers(servers);
    hri = clusterState.firstEntry().getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);

    clusterState.firstEntry().getValue().add(replica1);
    clusterState.lastEntry().getValue().add(replica2);
    clusterState.lastEntry().getValue().add(replica3);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith2ReplicasOnTwoServers = costFunction.cost();

    assertTrue(costWith2ReplicasOnTwoServers < costWith3ReplicasSameServer);
  }

  @Test
  public void testNeedsBalanceForColocatedReplicas() {
    // check for the case where there are two hosts and with one rack, and where
    // both the replicas are hosted on the same server
    List<HRegionInfo> regions = randomRegions(1);
    ServerName s1 = ServerName.valueOf("host1", 1000, 11111);
    ServerName s2 = ServerName.valueOf("host11", 1000, 11111);
    Map<ServerName, List<HRegionInfo>> map = new HashMap<ServerName, List<HRegionInfo>>();
    map.put(s1, regions);
    regions.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    // until the step above s1 holds two replicas of a region
    regions = randomRegions(1);
    map.put(s2, regions);
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null, null)));
    // check for the case where there are two hosts on the same rack and there are two racks
    // and both the replicas are on the same rack
    map.clear();
    regions = randomRegions(1);
    List<HRegionInfo> regionsOnS2 = new ArrayList<HRegionInfo>(1);
    regionsOnS2.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    map.put(s1, regions);
    map.put(s2, regionsOnS2);
    // add another server so that the cluster has some host on another rack
    map.put(ServerName.valueOf("host2", 1000, 11111), randomRegions(1));
    assertTrue(loadBalancer.needsBalance(new Cluster(map, null, null,
        new ForTestRackManagerOne())));
  }

  @Test (timeout = 60000)
  public void testSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Test (timeout = 60000)
  public void testSmallCluster2() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 40; //all servers except one
    int replication = 1;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Test (timeout = 60000)
  public void testSmallCluster3() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 1; // all servers except one
    int replication = 1;
    int numTables = 10;
    /* fails because of max moves */
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, false, false);
  }

  @Test (timeout = 800000)
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; // all servers except one
    int replication = 1;
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Test (timeout = 800000)
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; // all servers except one
    int replication = 1;
    int numTables = 400;
    testWithCluster(numNodes,
        numRegions,
        numRegionsPerServer,
        replication,
        numTables,
        false, /* num large num regions means may not always get to best balance with one run */
        false);
  }


  @Test (timeout = 800000)
  public void testMidCluster3() {
    int numNodes = 100;
    int numRegions = 2000;
    int numRegionsPerServer = 9; // all servers except one
    int replication = 1;
    int numTables = 110;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
    // TODO(eclark): Make sure that the tables are well distributed.
  }

  @Test
  public void testLargeCluster() {
    int numNodes = 1000;
    int numRegions = 100000; //100 regions per RS
    int numRegionsPerServer = 80; //all servers except one
    int numTables = 100;
    int replication = 1;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Test (timeout = 800000)
  public void testRegionReplicasOnSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 80; //all regions are mostly balanced
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Ignore @Test (timeout = 800000) // Test is flakey. TODO: Fix!
  public void testRegionReplicationOnMidClusterSameHosts() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 2000000L);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 90 * 1000); // 90 sec
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    loadBalancer.setConf(conf);
    int numHosts = 100;
    int numRegions = 100 * 100;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 5;
    int numTables = 10;
    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numHosts, numRegions, numRegionsPerServer, replication, numTables);
    int numNodesPerHost = 4;

    // create a new map with 4 RS per host.
    Map<ServerName, List<HRegionInfo>> newServerMap = new TreeMap<ServerName, List<HRegionInfo>>(serverMap);
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : serverMap.entrySet()) {
      for (int i=1; i < numNodesPerHost; i++) {
        ServerName s1 = entry.getKey();
        ServerName s2 = ServerName.valueOf(s1.getHostname(), s1.getPort() + i, 1); // create an RS for the same host
        newServerMap.put(s2, new ArrayList<HRegionInfo>());
      }
    }

    testWithCluster(newServerMap, null, true, true);
  }

  private static class ForTestRackManager extends RackManager {
    int numRacks;
    public ForTestRackManager(int numRacks) {
      this.numRacks = numRacks;
    }
    @Override
    public String getRack(ServerName server) {
      return "rack_" + (server.hashCode() % numRacks);
    }
  }

  private static class ForTestRackManagerOne extends RackManager {
  @Override
    public String getRack(ServerName server) {
      return server.getHostname().endsWith("1") ? "rack1" : "rack2";
    }
  }

  @Test (timeout = 800000)
  public void testRegionReplicationOnMidClusterWithRacks() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 10000000L);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 120 * 1000); // 120 sec
    loadBalancer.setConf(conf);
    int numNodes = 30;
    int numRegions = numNodes * 30;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 28;
    int numTables = 10;
    int numRacks = 4; // all replicas should be on a different rack
    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    RackManager rm = new ForTestRackManager(numRacks);

    testWithCluster(serverMap, rm, false, true);
  }

  @Test
  public void testAdditionalCostFunction() {
    conf.set(StochasticLoadBalancer.COST_FUNCTIONS_COST_FUNCTIONS_KEY,
            DummyCostFunction.class.getName());

    loadBalancer.setConf(conf);
    System.out.println(Arrays.toString(loadBalancer.getCostFunctionNames()));
    assertTrue(Arrays.
            asList(loadBalancer.getCostFunctionNames()).
            contains(DummyCostFunction.class.getSimpleName()));
  }

  // This mock allows us to test the LocalityCostFunction
  private class MockCluster extends BaseLoadBalancer.Cluster {

    private int[][] localities = null;   // [region][server] = percent of blocks

    public MockCluster(int[][] regions) {

      // regions[0] is an array where index = serverIndex an value = number of regions
      super(mockClusterServers(regions[0], 1), null, null, null);

      localities = new int[regions.length - 1][];
      for (int i = 1; i < regions.length; i++) {
        int regionIndex = i - 1;
        localities[regionIndex] = new int[regions[i].length - 1];
        regionIndexToServerIndex[regionIndex] = regions[i][0];
        for (int j = 1; j < regions[i].length; j++) {
          int serverIndex = j - 1;
          localities[regionIndex][serverIndex] = regions[i][j] > 100 ? regions[i][j] % 100 : regions[i][j];
        }
      }
    }

    @Override
    float getLocalityOfRegion(int region, int server) {
      // convert the locality percentage to a fraction
      return localities[region][server] / 100.0f;
    }

    @Override
    public int getRegionSizeMB(int region) {
      return 1;
    }

  }

}
