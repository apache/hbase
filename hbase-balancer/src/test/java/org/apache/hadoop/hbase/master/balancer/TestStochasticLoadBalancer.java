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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

@Category({ MasterTests.class, MediumTests.class })
public class TestStochasticLoadBalancer extends StochasticBalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticLoadBalancer.class);

  private static final String REGION_KEY = "testRegion";

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
      new int[]{1, 0, 100, 0},   // region 3 is hosted and entirely on server 1
    },

    // Test 2: each region is 0% local on the server that hosts it
    new int[][]{
      new int[]{1, 2, 1},
      new int[]{0, 0, 0, 100},   // region 0 is hosted and entirely local on server 2
      new int[]{1, 100, 0, 0},   // region 1 is hosted and entirely on server 0
      new int[]{1, 100, 0, 0},   // region 2 is hosted and entirely on server 0
      new int[]{2, 0, 100, 0},   // region 3 is hosted and entirely on server 1
    },

    // Test 3: each region is 25% local on the server that hosts it (and 50% locality is possible)
    new int[][]{
      new int[]{1, 2, 1},
      new int[]{0, 25, 0, 50},   // region 0 is hosted and entirely local on server 2
      new int[]{1, 50, 25, 0},   // region 1 is hosted and entirely on server 0
      new int[]{1, 50, 25, 0},   // region 2 is hosted and entirely on server 0
      new int[]{2, 0, 50, 25},   // region 3 is hosted and entirely on server 1
    },

    // Test 4: each region is 25% local on the server that hosts it (and 100% locality is possible)
    new int[][]{
      new int[]{1, 2, 1},
      new int[]{0, 25, 0, 100},   // region 0 is hosted and entirely local on server 2
      new int[]{1, 100, 25, 0},   // region 1 is hosted and entirely on server 0
      new int[]{1, 100, 25, 0},   // region 2 is hosted and entirely on server 0
      new int[]{2, 0, 100, 25},   // region 3 is hosted and entirely on server 1
    },

    // Test 5: each region is 75% local on the server that hosts it (and 75% locality is possible
    // everywhere)
    new int[][]{
      new int[]{1, 2, 1},
      new int[]{0, 75, 75, 75},   // region 0 is hosted and entirely local on server 2
      new int[]{1, 75, 75, 75},   // region 1 is hosted and entirely on server 0
      new int[]{1, 75, 75, 75},   // region 2 is hosted and entirely on server 0
      new int[]{2, 75, 75, 75},   // region 3 is hosted and entirely on server 1
    },
  };

  private ServerMetrics mockServerMetricsWithCpRequests(List<RegionInfo> regionsOnServer,
    long cpRequestCount) {
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    Map<byte[], RegionMetrics> regionLoadMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (RegionInfo info : regionsOnServer) {
      RegionMetrics rl = mock(RegionMetrics.class);
      when(rl.getReadRequestCount()).thenReturn(0L);
      when(rl.getCpRequestCount()).thenReturn(cpRequestCount);
      when(rl.getWriteRequestCount()).thenReturn(0L);
      when(rl.getMemStoreSize()).thenReturn(Size.ZERO);
      when(rl.getStoreFileSize()).thenReturn(Size.ZERO);
      regionLoadMap.put(info.getRegionName(), rl);
    }
    when(serverMetrics.getRegionMetrics()).thenReturn(regionLoadMap);
    return serverMetrics;
  }

  @Test
  public void testCPRequestCost() {
    // in order to pass needsBalance judgement
    conf.setFloat("hbase.master.balancer.stochastic.cpRequestCost", 10000f);
    loadBalancer.onConfigurationChange(conf);
    // mock cluster State
    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<ServerName, List<RegionInfo>>();
    ServerName serverA = randomServer(3).getServerName();
    ServerName serverB = randomServer(3).getServerName();
    ServerName serverC = randomServer(3).getServerName();
    List<RegionInfo> regionsOnServerA = randomRegions(3);
    List<RegionInfo> regionsOnServerB = randomRegions(3);
    List<RegionInfo> regionsOnServerC = randomRegions(3);
    clusterState.put(serverA, regionsOnServerA);
    clusterState.put(serverB, regionsOnServerB);
    clusterState.put(serverC, regionsOnServerC);
    // mock ClusterMetrics
    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(serverA, mockServerMetricsWithCpRequests(regionsOnServerA, 0));
    serverMetricsMap.put(serverB, mockServerMetricsWithCpRequests(regionsOnServerB, 0));
    serverMetricsMap.put(serverC, mockServerMetricsWithCpRequests(regionsOnServerC, 0));
    ClusterMetrics clusterStatus = mock(ClusterMetrics.class);
    when(clusterStatus.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterStatus);

    // CPRequestCostFunction are Rate based, So doing setClusterMetrics again
    // this time, regions on serverA with more cpRequestCount load
    // serverA : 1000,1000,1000
    // serverB : 0,0,0
    // serverC : 0,0,0
    // so should move two regions from serverA to serverB & serverC
    serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(serverA, mockServerMetricsWithCpRequests(regionsOnServerA, 1000));
    serverMetricsMap.put(serverB, mockServerMetricsWithCpRequests(regionsOnServerB, 0));
    serverMetricsMap.put(serverC, mockServerMetricsWithCpRequests(regionsOnServerC, 0));
    clusterStatus = mock(ClusterMetrics.class);
    when(clusterStatus.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterStatus);

    List<RegionPlan> plans =
        loadBalancer.balanceTable(HConstants.ENSEMBLE_TABLE_NAME, clusterState);
    Set<RegionInfo> regionsMoveFromServerA = new HashSet<>();
    Set<ServerName> targetServers = new HashSet<>();
    for (RegionPlan plan : plans) {
      if (plan.getSource().equals(serverA)) {
        regionsMoveFromServerA.add(plan.getRegionInfo());
        targetServers.add(plan.getDestination());
      }
    }
    // should move 2 regions from serverA, one moves to serverB, the other moves to serverC
    assertEquals(2, regionsMoveFromServerA.size());
    assertEquals(2, targetServers.size());
    assertTrue(regionsOnServerA.containsAll(regionsMoveFromServerA));
    // reset config
    conf.setFloat("hbase.master.balancer.stochastic.cpRequestCost", 5f);
    loadBalancer.onConfigurationChange(conf);
  }

  @Test
  public void testKeepRegionLoad() throws Exception {
    ServerName sn = ServerName.valueOf("test:8080", 100);
    int numClusterStatusToAdd = 20000;
    for (int i = 0; i < numClusterStatusToAdd; i++) {
      ServerMetrics sl = mock(ServerMetrics.class);

      RegionMetrics rl = mock(RegionMetrics.class);
      when(rl.getReadRequestCount()).thenReturn(0L);
      when(rl.getCpRequestCount()).thenReturn(0L);
      when(rl.getWriteRequestCount()).thenReturn(0L);
      when(rl.getMemStoreSize()).thenReturn(Size.ZERO);
      when(rl.getStoreFileSize()).thenReturn(new Size(i, Size.Unit.MEGABYTE));

      Map<byte[], RegionMetrics> regionLoadMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      regionLoadMap.put(Bytes.toBytes(REGION_KEY), rl);
      when(sl.getRegionMetrics()).thenReturn(regionLoadMap);

      ClusterMetrics clusterStatus = mock(ClusterMetrics.class);
      Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
      serverMetricsMap.put(sn, sl);
      when(clusterStatus.getLiveServerMetrics()).thenReturn(serverMetricsMap);

      loadBalancer.updateClusterMetrics(clusterStatus);
    }

    String regionNameAsString = RegionInfo.getRegionNameAsString(Bytes.toBytes(REGION_KEY));
    assertTrue(loadBalancer.loads.get(regionNameAsString) != null);
    assertTrue(loadBalancer.loads.get(regionNameAsString).size() == 15);

    Queue<BalancerRegionLoad> loads = loadBalancer.loads.get(regionNameAsString);
    int i = 0;
    while(loads.size() > 0) {
      BalancerRegionLoad rl = loads.remove();
      assertEquals(i + (numClusterStatusToAdd - 15), rl.getStorefileSizeMB());
      i ++;
    }
  }

  @Test
  public void testUpdateBalancerLoadInfo(){
    int[] cluster = new int[] { 10, 0 };
    Map<ServerName, List<RegionInfo>> servers = mockClusterServers(cluster);
    BalancerClusterState clusterState = mockCluster(cluster);
    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(servers);
    try {
      boolean[] perTableBalancerConfigs = { true, false };
      for (boolean isByTable : perTableBalancerConfigs) {
        conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, isByTable);
        loadBalancer.onConfigurationChange(conf);
        dummyMetricsStochasticBalancer.clearDummyMetrics();
        loadBalancer.updateBalancerLoadInfo(LoadOfAllTable);
        assertTrue("Metrics should be recorded!",
          dummyMetricsStochasticBalancer.getDummyCostsMap() != null && !dummyMetricsStochasticBalancer.getDummyCostsMap().isEmpty());

        String metricRecordKey;
        if (isByTable) {
          metricRecordKey = "table1#" + StochasticLoadBalancer.OVERALL_COST_FUNCTION_NAME;
        } else {
          metricRecordKey = HConstants.ENSEMBLE_TABLE_NAME + "#" + StochasticLoadBalancer.OVERALL_COST_FUNCTION_NAME;
        }
        double curOverallCost = loadBalancer.computeCost(clusterState, Double.MAX_VALUE);
        double curOverallCostInMetrics =
          dummyMetricsStochasticBalancer.getDummyCostsMap().get(metricRecordKey);
        assertEquals(curOverallCost, curOverallCostInMetrics, 0.001);
      }
    }finally {
      conf.unset(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE);
      loadBalancer.onConfigurationChange(conf);
    }
  }

  @Test
  public void testUpdateStochasticCosts() {
    float minCost = conf.getFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.05f);
    try {
      int[] cluster = new int[] { 10, 0 };
      Map<ServerName, List<RegionInfo>> servers = mockClusterServers(cluster);
      BalancerClusterState clusterState = mockCluster(cluster);
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 1.0f);
      conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, false);
      loadBalancer.onConfigurationChange(conf);
      dummyMetricsStochasticBalancer.clearDummyMetrics();
      List<RegionPlan> plans = loadBalancer.balanceCluster((Map)mockClusterServersWithTables(servers));

      assertTrue("Balance plan should not be empty!", plans != null && !plans.isEmpty());
      assertTrue("There should be metrics record in MetricsStochasticBalancer",
        !dummyMetricsStochasticBalancer.getDummyCostsMap().isEmpty());

      double overallCostOfCluster = loadBalancer.computeCost(clusterState, Double.MAX_VALUE);
      double overallCostInMetrics = dummyMetricsStochasticBalancer.getDummyCostsMap().get(
        HConstants.ENSEMBLE_TABLE_NAME + "#" + StochasticLoadBalancer.OVERALL_COST_FUNCTION_NAME);
      assertEquals(overallCostOfCluster, overallCostInMetrics, 0.001);
    } finally {
      //reset config
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", minCost);
      conf.unset(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE);
      loadBalancer.onConfigurationChange(conf);
    }
  }

  @Test
  public void testUpdateStochasticCostsIfBalanceNotRan() {
    float minCost = conf.getFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.05f);
    try {
      int[] cluster = new int[] { 10, 10 };
      Map<ServerName, List<RegionInfo>> servers = mockClusterServers(cluster);
      BalancerClusterState clusterState = mockCluster(cluster);
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", Float.MAX_VALUE);
      conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, false);
      loadBalancer.onConfigurationChange(conf);
      dummyMetricsStochasticBalancer.clearDummyMetrics();
      List<RegionPlan> plans = loadBalancer.balanceCluster( (Map) mockClusterServersWithTables(servers));

      assertTrue("Balance plan should be empty!", plans == null || plans.isEmpty());
      assertTrue("There should be metrics record in MetricsStochasticBalancer!",
        !dummyMetricsStochasticBalancer.getDummyCostsMap().isEmpty());

      double overallCostOfCluster = loadBalancer.computeCost(clusterState, Double.MAX_VALUE);
      double overallCostInMetrics = dummyMetricsStochasticBalancer.getDummyCostsMap().get(
        HConstants.ENSEMBLE_TABLE_NAME + "#" + StochasticLoadBalancer.OVERALL_COST_FUNCTION_NAME);
      assertEquals(overallCostOfCluster, overallCostInMetrics, 0.001);
    } finally {
      //reset config
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", minCost);
      conf.unset(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE);
      loadBalancer.onConfigurationChange(conf);
    }
  }

  @Test
  public void testNeedBalance() {
    float minCost = conf.getFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.05f);
    conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 1.0f);
    try {
      // Test with/without per table balancer.
      boolean[] perTableBalancerConfigs = {true, false};
      for (boolean isByTable : perTableBalancerConfigs) {
        conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, isByTable);
        loadBalancer.onConfigurationChange(conf);
        for (int[] mockCluster : clusterStateMocks) {
          Map<ServerName, List<RegionInfo>> servers = mockClusterServers(mockCluster);
          Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
              (Map) mockClusterServersWithTables(servers);
          List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
          boolean emptyPlans = plans == null || plans.isEmpty();
          assertTrue(emptyPlans || needsBalanceIdleRegion(mockCluster));
        }
      }
    } finally {
      // reset config
      conf.unset(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE);
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", minCost);
      loadBalancer.onConfigurationChange(conf);
    }
  }

  @Test
  public void testLocalityCost() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    CostFunction
        costFunction = new ServerLocalityCostFunction(conf);

    for (int test = 0; test < clusterRegionLocationMocks.length; test++) {
      int[][] clusterRegionLocations = clusterRegionLocationMocks[test];
      MockCluster cluster = new MockCluster(clusterRegionLocations);
      costFunction.prepare(cluster);
      double cost = costFunction.cost();
      double expected = 1 - expectedLocalities[test];
      assertEquals(expected, cost, 0.001);
    }
  }

  @Test
  public void testMoveCostMultiplier() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    ClusterInfoProvider provider = mock(ClusterInfoProvider.class);
    CostFunction costFunction =
      new MoveCostFunction(conf, provider);
    when(provider.isOffPeakHour()).thenReturn(false);
    BalancerClusterState cluster = mockCluster(clusterStateMocks[0]);
    costFunction.prepare(cluster);
    costFunction.cost();
    assertEquals(MoveCostFunction.DEFAULT_MOVE_COST,
      costFunction.getMultiplier(), 0.01);

    // In offpeak hours, the multiplier of move cost should be lower
    when(provider.isOffPeakHour()).thenReturn(true);
    costFunction.prepare(cluster);
    costFunction.cost();
    assertEquals(MoveCostFunction.DEFAULT_MOVE_COST_OFFPEAK,
      costFunction.getMultiplier(), 0.01);
  }

  @Test
  public void testMoveCost() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    CostFunction costFunction = new MoveCostFunction(conf, new DummyClusterInfoProvider(conf));
    for (int[] mockCluster : clusterStateMocks) {
      BalancerClusterState cluster = mockCluster(mockCluster);
      costFunction.prepare(cluster);
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
      assertEquals(0.025f, cost, 0.001);
      cluster.setNumMovedRegions(1250);
      cost = costFunction.cost();
      assertEquals(0.125f, cost, 0.001);
      cluster.setNumMovedRegions(2500);
      cost = costFunction.cost();
      assertEquals(0.25f, cost, 0.01);
    }
  }

  @Test
  public void testSkewCost() {
    Configuration conf = HBaseConfiguration.create();
    CostFunction
        costFunction = new RegionCountSkewCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      costFunction.prepare(mockCluster(mockCluster));
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }

    costFunction.prepare(mockCluster(new int[]{0, 0, 0, 0, 1}));
    assertEquals(0,costFunction.cost(), 0.01);
    costFunction.prepare(mockCluster(new int[]{0, 0, 0, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.prepare(mockCluster(new int[]{0, 0, 1, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.prepare(mockCluster(new int[]{0, 1, 1, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.prepare(mockCluster(new int[]{1, 1, 1, 1, 1}));
    assertEquals(0, costFunction.cost(), 0.01);
    costFunction.prepare(mockCluster(new int[]{10000, 0, 0, 0, 0}));
    assertEquals(1, costFunction.cost(), 0.01);
  }

  @Test
  public void testCostAfterUndoAction() {
    final int runs = 10;
    loadBalancer.onConfigurationChange(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BalancerClusterState cluster = mockCluster(mockCluster);
      loadBalancer.initCosts(cluster);
      for (int i = 0; i != runs; ++i) {
        final double expectedCost = loadBalancer.computeCost(cluster, Double.MAX_VALUE);
        BalanceAction action = loadBalancer.nextAction(cluster);
        cluster.doAction(action);
        loadBalancer.updateCostsAndWeightsWithAction(cluster, action);
        BalanceAction undoAction = action.undoAction();
        cluster.doAction(undoAction);
        loadBalancer.updateCostsAndWeightsWithAction(cluster, undoAction);
        final double actualCost = loadBalancer.computeCost(cluster, Double.MAX_VALUE);
        assertEquals(expectedCost, actualCost, 0);
      }
    }
  }

  @Test
  public void testTableSkewCost() {
    Configuration conf = HBaseConfiguration.create();
    CostFunction
        costFunction = new TableSkewCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BalancerClusterState cluster = mockCluster(mockCluster);
      costFunction.prepare(cluster);
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }

  @Test
  public void testRegionLoadCost() {
    List<BalancerRegionLoad> regionLoads = new ArrayList<>();
    for (int i = 1; i < 5; i++) {
      BalancerRegionLoad regionLoad = mock(BalancerRegionLoad.class);
      when(regionLoad.getReadRequestsCount()).thenReturn((long) i);
      when(regionLoad.getCpRequestsCount()).thenReturn((long) i);
      when(regionLoad.getStorefileSizeMB()).thenReturn(i);
      regionLoads.add(regionLoad);
    }

    Configuration conf = HBaseConfiguration.create();
    ReadRequestCostFunction readCostFunction =
        new ReadRequestCostFunction(conf);
    double rateResult = readCostFunction.getRegionLoadCost(regionLoads);
    // read requests are treated as a rate so the average rate here is simply 1
    assertEquals(1, rateResult, 0.01);

    CPRequestCostFunction cpCostFunction =
        new CPRequestCostFunction(conf);
    rateResult = cpCostFunction.getRegionLoadCost(regionLoads);
    // coprocessor requests are treated as a rate so the average rate here is simply 1
    assertEquals(1, rateResult, 0.01);

    StoreFileCostFunction storeFileCostFunction =
        new StoreFileCostFunction(conf);
    double result = storeFileCostFunction.getRegionLoadCost(regionLoads);
    // storefile size cost is simply an average of it's value over time
    assertEquals(2.5, result, 0.01);
  }

  @Test
  public void testRegionLoadCostWhenDecrease() {
    List<BalancerRegionLoad> regionLoads = new ArrayList<>();
    // test region loads of [1,2,1,4]
    for (int i = 1; i < 5; i++) {
      int load = i == 3 ? 1 : i;
      BalancerRegionLoad regionLoad = mock(BalancerRegionLoad.class);
      when(regionLoad.getReadRequestsCount()).thenReturn((long)load);
      when(regionLoad.getCpRequestsCount()).thenReturn((long)load);
      regionLoads.add(regionLoad);
    }

    Configuration conf = HBaseConfiguration.create();
    ReadRequestCostFunction readCostFunction =
      new ReadRequestCostFunction(conf);
    double rateResult = readCostFunction.getRegionLoadCost(regionLoads);
    // read requests are treated as a rate so the average rate here is simply 1
    assertEquals(1.67, rateResult, 0.01);

    CPRequestCostFunction cpCostFunction =
      new CPRequestCostFunction(conf);
    rateResult = cpCostFunction.getRegionLoadCost(regionLoads);
    // coprocessor requests are treated as a rate so the average rate here is simply 1
    assertEquals(1.67, rateResult, 0.01);
  }

  @Test
  public void testLosingRs() throws Exception {
    int numNodes = 3;
    int numRegions = 20;
    int numRegionsPerServer = 3; //all servers except one
    int replication = 1;
    int numTables = 2;

    Map<ServerName, List<RegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    List<ServerAndLoad> list = convertToList(serverMap);


    List<RegionPlan> plans = loadBalancer.balanceTable(HConstants.ENSEMBLE_TABLE_NAME, serverMap);
    assertNotNull(plans);

    // Apply the plan to the mock cluster.
    List<ServerAndLoad> balancedCluster = reconcile(list, plans, serverMap);

    assertClusterAsBalanced(balancedCluster);

    ServerName sn = serverMap.keySet().toArray(new ServerName[serverMap.size()])[0];

    ServerName deadSn = ServerName.valueOf(sn.getHostname(), sn.getPort(), sn.getStartcode() - 100);

    serverMap.put(deadSn, new ArrayList<>(0));

    plans = loadBalancer.balanceTable(HConstants.ENSEMBLE_TABLE_NAME, serverMap);
    assertNull(plans);
  }

  @Test
  public void testAdditionalCostFunction() {
    try {
      conf.set(StochasticLoadBalancer.COST_FUNCTIONS_COST_FUNCTIONS_KEY,
        DummyCostFunction.class.getName());

      loadBalancer.onConfigurationChange(conf);
      assertTrue(Arrays.
        asList(loadBalancer.getCostFunctionNames()).
        contains(DummyCostFunction.class.getSimpleName()));
    } finally {
      conf.unset(StochasticLoadBalancer.COST_FUNCTIONS_COST_FUNCTIONS_KEY);
      loadBalancer.onConfigurationChange(conf);
    }
  }

  @Test
  public void testDefaultCostFunctionList() {
    List<String> expected = Arrays.asList(
      RegionCountSkewCostFunction.class.getSimpleName(),
      PrimaryRegionCountSkewCostFunction.class.getSimpleName(),
      MoveCostFunction.class.getSimpleName(),
      RackLocalityCostFunction.class.getSimpleName(),
      TableSkewCostFunction.class.getSimpleName(),
      RegionReplicaHostCostFunction.class.getSimpleName(),
      RegionReplicaRackCostFunction.class.getSimpleName(),
      ReadRequestCostFunction.class.getSimpleName(),
      CPRequestCostFunction.class.getSimpleName(),
      WriteRequestCostFunction.class.getSimpleName(),
      MemStoreSizeCostFunction.class.getSimpleName(),
      StoreFileCostFunction.class.getSimpleName()
    );

    List<String> actual = Arrays.asList(loadBalancer.getCostFunctionNames());
    assertTrue("ExpectedCostFunctions: " + expected + " ActualCostFunctions: " + actual,
      CollectionUtils.isEqualCollection(expected, actual));
  }

  private boolean needsBalanceIdleRegion(int[] cluster) {
    return Arrays.stream(cluster).anyMatch(x -> x > 1) &&
      Arrays.stream(cluster).anyMatch(x -> x < 1);
  }

  // This mock allows us to test the LocalityCostFunction
  private class MockCluster extends BalancerClusterState {

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
          localities[regionIndex][serverIndex] =
            regions[i][j] > 100 ? regions[i][j] % 100 : regions[i][j];
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
