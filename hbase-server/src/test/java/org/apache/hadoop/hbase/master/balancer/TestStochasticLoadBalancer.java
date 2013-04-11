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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestStochasticLoadBalancer extends BalancerTestBase {
  private static StochasticLoadBalancer loadBalancer;
  private static final Log LOG = LogFactory.getLog(TestStochasticLoadBalancer.class);

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    loadBalancer = new StochasticLoadBalancer();
    loadBalancer.setConf(conf);
  }

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
      new int[]{47, 53},
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
  };

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
      List<ServerAndLoad> balancedCluster = reconcile(list, plans);
      LOG.info("Mock Balance : " + printMock(balancedCluster));
      assertClusterAsBalanced(balancedCluster);
      for (Map.Entry<ServerName, List<HRegionInfo>> entry : servers.entrySet()) {
        returnRegions(entry.getValue());
        returnServer(entry.getKey());
      }
    }

  }

  @Test
  public void testSkewCost() {
    for (int[] mockCluster : clusterStateMocks) {
      double cost = loadBalancer.computeSkewLoadCost(mockCluster(mockCluster));
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
    assertEquals(1,
      loadBalancer.computeSkewLoadCost(mockCluster(new int[] { 0, 0, 0, 0, 1 })), 0.01);
    assertEquals(.75,
      loadBalancer.computeSkewLoadCost(mockCluster(new int[] { 0, 0, 0, 1, 1 })), 0.01);
    assertEquals(.5,
      loadBalancer.computeSkewLoadCost(mockCluster(new int[] { 0, 0, 1, 1, 1 })), 0.01);
    assertEquals(.25,
      loadBalancer.computeSkewLoadCost(mockCluster(new int[] { 0, 1, 1, 1, 1 })), 0.01);
    assertEquals(0,
      loadBalancer.computeSkewLoadCost(mockCluster(new int[] { 1, 1, 1, 1, 1 })), 0.01);
    assertEquals(0,
        loadBalancer.computeSkewLoadCost(mockCluster(new int[] { 10, 10, 10, 10, 10 })), 0.01);
  }

  @Test
  public void testTableSkewCost() {
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      double cost = loadBalancer.computeTableSkewLoadCost(cluster);
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }

  @Test
  public void testCostFromStats() {
    DescriptiveStatistics statOne = new DescriptiveStatistics();
    for (int i =0; i < 100; i++) {
      statOne.addValue(10);
    }
    assertEquals(0, loadBalancer.costFromStats(statOne), 0.01);

    DescriptiveStatistics statTwo = new DescriptiveStatistics();
    for (int i =0; i < 100; i++) {
      statTwo.addValue(0);
    }
    statTwo.addValue(100);
    assertEquals(1, loadBalancer.costFromStats(statTwo), 0.01);

    DescriptiveStatistics statThree = new DescriptiveStatistics();
    for (int i =0; i < 100; i++) {
      statThree.addValue(0);
      statThree.addValue(100);
    }
    assertEquals(0.5, loadBalancer.costFromStats(statThree), 0.01);
  }

  @Test (timeout = 20000)
  public void testSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int numRegionsPerServer = 40; //all servers except one
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables);
  }

  @Test (timeout = 20000)
  public void testSmallCluster2() {
    int numNodes = 20;
    int numRegions = 2000;
    int numRegionsPerServer = 40; //all servers except one
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables);
  }

  @Test (timeout = 40000)
  public void testMidCluster() {
    int numNodes = 100;
    int numRegions = 10000;
    int numRegionsPerServer = 60; //all servers except one
    int numTables = 40;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables);
  }

  @Test (timeout = 1200000)
  public void testMidCluster2() {
    int numNodes = 200;
    int numRegions = 100000;
    int numRegionsPerServer = 40; //all servers except one
    int numTables = 400;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables);
  }

  @Test
  @Ignore
  //TODO: This still does not finish, making the LoadBalancer unusable at this scale. We should solve this.
  //There are two reasons so far;
  // - It takes too long for iterating for all servers
  // - Moving one region out of the loaded server only costs a slight decrease in the cost of regionCountSkewCost
  // but also a slight increase on the moveCost. loadMultiplier / moveCostMultiplier is not high enough to bring down
  // the total cost, so that the eager selection cannot continue. This can be solved by smt like
  // http://en.wikipedia.org/wiki/Simulated_annealing instead of random walk with eager selection
  public void testLargeCluster() {
    int numNodes = 1000;
    int numRegions = 100000; //100 regions per RS
    int numRegionsPerServer = 80; //all servers except one
    int numTables = 100;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, numTables);
  }

  protected void testWithCluster(int numNodes, int numRegions, int numRegionsPerServer, int numTables) {
    //construct a cluster of numNodes, having  a total of numRegions. Each RS will hold
    //numRegionsPerServer many regions except for the last one, which will host all the
    //remaining regions
    int[] cluster = new int[numNodes];
    for (int i =0; i < numNodes; i++) {
      cluster[i] = numRegionsPerServer;
    }
    cluster[cluster.length - 1] = numRegions - ((cluster.length - 1) * numRegionsPerServer);

    assertNotNull(loadBalancer.balanceCluster(mockClusterServers(cluster, numTables)));
  }

}
