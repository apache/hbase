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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the load balancer that is created by default.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestSimpleLoadBalancer extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSimpleLoadBalancer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSimpleLoadBalancer.class);

  private static SimpleLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    conf.set("hbase.regions.slop", "0");
    loadBalancer = new SimpleLoadBalancer();
    loadBalancer.setConf(conf);
  }

  // int[testnum][servernumber] -> numregions
  int[][] clusterStateMocks = new int[][] {
      // 1 node
      new int[] { 0 },
      new int[] { 1 },
      new int[] { 10 },
      // 2 node
      new int[] { 0, 0 },
      new int[] { 2, 0 },
      new int[] { 2, 1 },
      new int[] { 2, 2 },
      new int[] { 2, 3 },
      new int[] { 2, 4 },
      new int[] { 1, 1 },
      new int[] { 0, 1 },
      new int[] { 10, 1 },
      new int[] { 14, 1432 },
      new int[] { 47, 53 },
      // 3 node
      new int[] { 0, 1, 2 },
      new int[] { 1, 2, 3 },
      new int[] { 0, 2, 2 },
      new int[] { 0, 3, 0 },
      new int[] { 0, 4, 0 },
      new int[] { 20, 20, 0 },
      // 4 node
      new int[] { 0, 1, 2, 3 },
      new int[] { 4, 0, 0, 0 },
      new int[] { 5, 0, 0, 0 },
      new int[] { 6, 6, 0, 0 },
      new int[] { 6, 2, 0, 0 },
      new int[] { 6, 1, 0, 0 },
      new int[] { 6, 0, 0, 0 },
      new int[] { 4, 4, 4, 7 },
      new int[] { 4, 4, 4, 8 },
      new int[] { 0, 0, 0, 7 },
      // 5 node
      new int[] { 1, 1, 1, 1, 4 },
      // more nodes
      new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
      new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 10 }, new int[] { 6, 6, 5, 6, 6, 6, 6, 6, 6, 1 },
      new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 54 }, new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 55 },
      new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 56 }, new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 16 },
      new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 8 }, new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 9 },
      new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 10 }, new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 123 },
      new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 155 },
      new int[] { 0, 0, 144, 1, 1, 1, 1, 1123, 133, 138, 12, 1444 },
      new int[] { 0, 0, 144, 1, 0, 4, 1, 1123, 133, 138, 12, 1444 },
      new int[] { 1538, 1392, 1561, 1557, 1535, 1553, 1385, 1542, 1619 } };

  int [] mockUniformCluster = new int[] { 5, 5, 5, 5, 5 ,0};

  @Rule
  public TestName name = new TestName();

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers should be hosting either floor(average) or
   * ceiling(average) at both table level and cluster level
   *
   */
  @Test
  public void testBalanceClusterOverall() throws Exception {
    Map<TableName, Map<ServerName, List<RegionInfo>>> clusterLoad = new TreeMap<>();
    for (int[] mockCluster : clusterStateMocks) {
      Map<ServerName, List<RegionInfo>> clusterServers = mockClusterServers(mockCluster, 30);
      List<ServerAndLoad> clusterList = convertToList(clusterServers);
      clusterLoad.put(TableName.valueOf(name.getMethodName()), clusterServers);
      HashMap<TableName, TreeMap<ServerName, List<RegionInfo>>> result =
          mockClusterServersWithTables(clusterServers);
      loadBalancer.setClusterLoad(clusterLoad);
      List<RegionPlan> clusterplans = new ArrayList<>();
      List<Pair<TableName, Integer>> regionAmountList = new ArrayList<>();
      for (Map.Entry<TableName, TreeMap<ServerName, List<RegionInfo>>> mapEntry : result
          .entrySet()) {
        TableName tableName = mapEntry.getKey();
        TreeMap<ServerName, List<RegionInfo>> servers = mapEntry.getValue();
        List<ServerAndLoad> list = convertToList(servers);
        LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));
        List<RegionPlan> partialplans = loadBalancer.balanceTable(tableName, servers);
        if(partialplans != null) clusterplans.addAll(partialplans);
        List<ServerAndLoad> balancedClusterPerTable = reconcile(list, partialplans, servers);
        LOG.info("Mock Balance : " + printMock(balancedClusterPerTable));
        assertClusterAsBalanced(balancedClusterPerTable);
        for (Map.Entry<ServerName, List<RegionInfo>> entry : servers.entrySet()) {
          returnRegions(entry.getValue());
          returnServer(entry.getKey());
        }
      }
      List<ServerAndLoad> balancedCluster = reconcile(clusterList, clusterplans, clusterServers);
      assertTrue(assertClusterOverallAsBalanced(balancedCluster, result.keySet().size()));
    }
  }

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers should be hosting either floor(average) or
   * ceiling(average) at both table level and cluster level
   * Deliberately generate a special case to show the overall strategy can achieve cluster
   * level balance while the bytable strategy cannot
   * @throws Exception
   */
  @Test
  public void testImpactOfBalanceClusterOverall() throws Exception {
    testImpactOfBalanceClusterOverall(false);
  }

  @Test
  public void testImpactOfBalanceClusterOverallWithLoadOfAllTable() throws Exception {
    testImpactOfBalanceClusterOverall(true);
  }

  private void testImpactOfBalanceClusterOverall(boolean useLoadOfAllTable) throws Exception {
    Map<TableName, Map<ServerName, List<RegionInfo>>> clusterLoad = new TreeMap<>();
    Map<ServerName, List<RegionInfo>> clusterServers =
        mockUniformClusterServers(mockUniformCluster);
    List<ServerAndLoad> clusterList = convertToList(clusterServers);
    clusterLoad.put(TableName.valueOf(name.getMethodName()), clusterServers);
    // use overall can achieve both table and cluster level balance
    HashMap<TableName, TreeMap<ServerName, List<RegionInfo>>> LoadOfAllTable =
        mockClusterServersWithTables(clusterServers);
    if (useLoadOfAllTable) {
      loadBalancer.setClusterLoad((Map) LoadOfAllTable);
    } else {
      loadBalancer.setClusterLoad(clusterLoad);
    }
    List<RegionPlan> clusterplans1 = new ArrayList<RegionPlan>();
    List<Pair<TableName, Integer>> regionAmountList = new ArrayList<Pair<TableName, Integer>>();
    for (Map.Entry<TableName, TreeMap<ServerName, List<RegionInfo>>> mapEntry : LoadOfAllTable
        .entrySet()) {
      TableName tableName = mapEntry.getKey();
      TreeMap<ServerName, List<RegionInfo>> servers = mapEntry.getValue();
      List<ServerAndLoad> list = convertToList(servers);
      LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));
      List<RegionPlan> partialplans = loadBalancer.balanceTable(tableName, servers);
      if (partialplans != null) clusterplans1.addAll(partialplans);
      List<ServerAndLoad> balancedClusterPerTable = reconcile(list, partialplans, servers);
      LOG.info("Mock Balance : " + printMock(balancedClusterPerTable));
      assertClusterAsBalanced(balancedClusterPerTable);
      for (Map.Entry<ServerName, List<RegionInfo>> entry : servers.entrySet()) {
        returnRegions(entry.getValue());
        returnServer(entry.getKey());
      }
    }
    List<ServerAndLoad> balancedCluster1 = reconcile(clusterList, clusterplans1, clusterServers);
    assertTrue(assertClusterOverallAsBalanced(balancedCluster1, LoadOfAllTable.keySet().size()));
  }

  @Test
  public void testBalanceClusterOverallStrictly() throws Exception {
    int[] regionNumOfTable1PerServer = { 3, 3, 4, 4, 4, 4, 5, 5, 5 };
    int[] regionNumOfTable2PerServer = { 2, 2, 2, 2, 2, 2, 2, 2, 1 };
    TreeMap<ServerName, List<RegionInfo>> serverRegionInfo = new TreeMap<>();
    List<ServerAndLoad> serverAndLoads = new ArrayList<>();
    for (int i = 0; i < regionNumOfTable1PerServer.length; i++) {
      ServerName serverName = ServerName.valueOf("server" + i, 1000, -1);
      List<RegionInfo> regions1 =
          createRegions(regionNumOfTable1PerServer[i], TableName.valueOf("table1"));
      List<RegionInfo> regions2 =
          createRegions(regionNumOfTable2PerServer[i], TableName.valueOf("table2"));
      regions1.addAll(regions2);
      serverRegionInfo.put(serverName, regions1);
      ServerAndLoad serverAndLoad = new ServerAndLoad(serverName,
          regionNumOfTable1PerServer[i] + regionNumOfTable2PerServer[i]);
      serverAndLoads.add(serverAndLoad);
    }
    HashMap<TableName, TreeMap<ServerName, List<RegionInfo>>> LoadOfAllTable =
        mockClusterServersWithTables(serverRegionInfo);
    loadBalancer.setClusterLoad((Map) LoadOfAllTable);
    List<RegionPlan> partialplans = loadBalancer.balanceTable(TableName.valueOf("table1"),
      LoadOfAllTable.get(TableName.valueOf("table1")));
    List<ServerAndLoad> balancedServerLoads =
        reconcile(serverAndLoads, partialplans, serverRegionInfo);
    for (ServerAndLoad serverAndLoad : balancedServerLoads) {
      assertEquals(6, serverAndLoad.getLoad());
    }
  }

}
