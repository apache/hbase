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
    loadBalancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    loadBalancer.initialize();
  }

  int[] mockUniformCluster = new int[] { 5, 5, 5, 5, 5, 0 };

  @Rule
  public TestName name = new TestName();

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers should be hosting either floor(average) or
   * ceiling(average) at both table level and cluster level
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
