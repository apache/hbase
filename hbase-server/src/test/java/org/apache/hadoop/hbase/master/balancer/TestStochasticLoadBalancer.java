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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(MediumTests.class)
public class TestStochasticLoadBalancer extends BalancerTestBase {
  public static final String REGION_KEY = "testRegion";
  private static StochasticLoadBalancer loadBalancer;
  private static final Log LOG = LogFactory.getLog(TestStochasticLoadBalancer.class);

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 0.75f);
    conf.setFloat("hbase.regions.slop", 0.0f);
    conf.setFloat("hbase.master.balancer.stochastic.localityCost", 0);
    conf.setInt("hbase.master.balancer.stochastic.maxSteps", 10000000);
    loadBalancer = new StochasticLoadBalancer();
    loadBalancer.setConf(conf);
  }

  int[] largeCluster = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 56 };

  // int[testnum][servernumber] -> numregions
  int[][] clusterStateMocks = new int[][]{
      // 1 node
      new int[]{0},
      new int[]{1},
      new int[]{10},
      // 2 node
      new int[]{0, 0},
      new int[]{2, 0},
      new int[]{2, 1},
      new int[]{2, 2},
      new int[]{2, 3},
      new int[]{2, 4},
      new int[]{1, 1},
      new int[]{0, 1},
      new int[]{10, 1},
      new int[]{514, 1432},
      new int[]{48, 53},
      // 3 node
      new int[]{0, 1, 2},
      new int[]{1, 2, 3},
      new int[]{0, 2, 2},
      new int[]{0, 3, 0},
      new int[]{0, 4, 0},
      new int[]{20, 20, 0},
      // 4 node
      new int[]{0, 1, 2, 3},
      new int[]{4, 0, 0, 0},
      new int[]{5, 0, 0, 0},
      new int[]{6, 6, 0, 0},
      new int[]{6, 2, 0, 0},
      new int[]{6, 1, 0, 0},
      new int[]{6, 0, 0, 0},
      new int[]{4, 4, 4, 7},
      new int[]{4, 4, 4, 8},
      new int[]{0, 0, 0, 7},
      // 5 node
      new int[]{1, 1, 1, 1, 4},
      // more nodes
      new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
      new int[]{6, 6, 5, 6, 6, 6, 6, 6, 6, 1},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 54},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 55},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 56},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 16},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 8},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 9},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 10},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 123},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 155},
      new int[]{10, 7, 12, 8, 11, 10, 9, 14},
      new int[]{13, 14, 6, 10, 10, 10, 8, 10},
      new int[]{130, 14, 60, 10, 100, 10, 80, 10},
      new int[]{130, 140, 60, 100, 100, 100, 80, 100},
      new int[]{0, 5 , 5, 5, 5},
      largeCluster,

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
    assertTrue(loadBalancer.loads.get(REGION_KEY) != null);
    assertTrue(loadBalancer.loads.get(REGION_KEY).size() == 15);

    Queue<RegionLoad> loads = loadBalancer.loads.get(REGION_KEY);
    int i = 0;
    while(loads.size() > 0) {
      RegionLoad rl = loads.remove();
      assertEquals(i + (numClusterStatusToAdd - 15), rl.getStores());
      i ++;
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
  public void testSkewCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.RegionCountSkewCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      double cost = costFunction.cost(mockCluster(mockCluster));
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }

    assertEquals(0,
        costFunction.cost(mockCluster(new int[]{0, 0, 0, 0, 1})), 0.01);
    assertEquals(0,
        costFunction.cost(mockCluster(new int[]{0, 0, 0, 1, 1})), 0.01);
    assertEquals(0,
        costFunction.cost(mockCluster(new int[]{0, 0, 1, 1, 1})), 0.01);
    assertEquals(0,
        costFunction.cost(mockCluster(new int[]{0, 1, 1, 1, 1})), 0.01);
    assertEquals(0,
        costFunction.cost(mockCluster(new int[]{1, 1, 1, 1, 1})), 0.01);
    assertEquals(0,
        costFunction.cost(mockCluster(new int[]{10, 10, 10, 10, 10})), 0.01);
    assertEquals(1,
        costFunction.cost(mockCluster(new int[]{10000, 0, 0, 0, 0})), 0.01);
  }

  @Test
  public void testTableSkewCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction
        costFunction = new StochasticLoadBalancer.TableSkewCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      double cost = costFunction.cost(cluster);
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }

  @Test
  public void testCostFromArray() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFromRegionLoadFunction
        costFunction = new StochasticLoadBalancer.MemstoreSizeCostFunction(conf);

    double[] statOne = new double[100];
    for (int i =0; i < 100; i++) {
      statOne[i] = 10;
    }
    assertEquals(0, costFunction.costFromArray(statOne), 0.01);

    double[] statTwo= new double[101];
    for (int i =0; i < 100; i++) {
      statTwo[i] = 0;
    }
    statTwo[100] = 101;
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
    int numTables = 2;

    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, numTables);
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

  @Test (timeout = 60000)
  public void testSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int numRegionsPerServer = 40; //all servers except one
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables, true);
  }

  @Test (timeout = 60000)
  public void testSmallCluster2() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 40; //all servers except one
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables, true);
  }

  @Test (timeout = 60000)
  public void testSmallCluster3() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 1; // all servers except one
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables, false /* max moves */);
  }

  @Test (timeout = 800000)
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; // all servers except one
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables, true);
  }

  @Test (timeout = 800000)
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; // all servers except one
    int numTables = 400;
    testWithCluster(numNodes,
        numRegions,
        numRegionsPerServer,
        numTables,
        false /* num large num regions means may not always get to best balance with one run */);
  }


  @Test (timeout = 800000)
  public void testMidCluster3() {
    int numNodes = 100;
    int numRegions = 2000;
    int numRegionsPerServer = 9; // all servers except one
    int numTables = 110;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables, true);
    // TODO(eclark): Make sure that the tables are well distributed.
  }

  @Test
  public void testLargeCluster() {
    int numNodes = 1000;
    int numRegions = 100000; //100 regions per RS
    int numRegionsPerServer = 80; //all servers except one
    int numTables = 100;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables, true);
  }

  protected void testWithCluster(int numNodes,
                                 int numRegions,
                                 int numRegionsPerServer,
                                 int numTables,
                                 boolean assertFullyBalanced) {
    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, numTables);

    List<ServerAndLoad> list = convertToList(serverMap);
    LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));

    // Run the balancer.
    List<RegionPlan> plans = loadBalancer.balanceCluster(serverMap);
    assertNotNull(plans);

    // Check to see that this actually got to a stable place.
    if (assertFullyBalanced) {
      // Apply the plan to the mock cluster.
      List<ServerAndLoad> balancedCluster = reconcile(list, plans, serverMap);

      // Print out the cluster loads to make debugging easier.
      LOG.info("Mock Balance : " + printMock(balancedCluster));
      assertClusterAsBalanced(balancedCluster);
      List<RegionPlan> secondPlans =  loadBalancer.balanceCluster(serverMap);
      assertNull(secondPlans);
    }
  }

  private Map<ServerName, List<HRegionInfo>> createServerMap(int numNodes,
                                                             int numRegions,
                                                             int numRegionsPerServer,
                                                             int numTables) {
    //construct a cluster of numNodes, having  a total of numRegions. Each RS will hold
    //numRegionsPerServer many regions except for the last one, which will host all the
    //remaining regions
    int[] cluster = new int[numNodes];
    for (int i =0; i < numNodes; i++) {
      cluster[i] = numRegionsPerServer;
    }
    cluster[cluster.length - 1] = numRegions - ((cluster.length - 1) * numRegionsPerServer);
    return mockClusterServers(cluster, numTables);
  }
}
