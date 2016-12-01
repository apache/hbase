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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the load balancer that is created by default.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestDefaultLoadBalancer extends BalancerTestBase {
  private static final Log LOG = LogFactory.getLog(TestDefaultLoadBalancer.class);

  private static LoadBalancer loadBalancer;

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


  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers should be hosting either floor(average) or
   * ceiling(average) at both table level and cluster level
   *
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testBalanceClusterOverall() throws Exception {
    Map<TableName, Map<ServerName, List<HRegionInfo>>> clusterLoad
            = new TreeMap<TableName, Map<ServerName, List<HRegionInfo>>>();
    for (int[] mockCluster : clusterStateMocks) {
      Map<ServerName, List<HRegionInfo>> clusterServers = mockClusterServers(mockCluster, 50);
      List<ServerAndLoad> clusterList = convertToList(clusterServers);
      clusterLoad.put(TableName.valueOf("ensemble"), clusterServers);
      HashMap<TableName, TreeMap<ServerName, List<HRegionInfo>>> result = mockClusterServersWithTables(clusterServers);
      loadBalancer.setClusterLoad(clusterLoad);
      List<RegionPlan> clusterplans = new ArrayList<RegionPlan>();
      List<Pair<TableName, Integer>> regionAmountList = new ArrayList<Pair<TableName, Integer>>();
      for(TreeMap<ServerName, List<HRegionInfo>> servers : result.values()){
        List<ServerAndLoad> list = convertToList(servers);
        LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));
        List<RegionPlan> partialplans = loadBalancer.balanceCluster(servers);
        if(partialplans != null) clusterplans.addAll(partialplans);
        List<ServerAndLoad> balancedClusterPerTable = reconcile(list, partialplans, servers);
        LOG.info("Mock Balance : " + printMock(balancedClusterPerTable));
        assertClusterAsBalanced(balancedClusterPerTable);
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : servers.entrySet()) {
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
  @Test (timeout=60000)
  public void testImpactOfBalanceClusterOverall() throws Exception {
    Map<TableName, Map<ServerName, List<HRegionInfo>>> clusterLoad
            = new TreeMap<TableName, Map<ServerName, List<HRegionInfo>>>();
    Map<ServerName, List<HRegionInfo>> clusterServers = mockUniformClusterServers(mockUniformCluster);
    List<ServerAndLoad> clusterList = convertToList(clusterServers);
    clusterLoad.put(TableName.valueOf("ensemble"), clusterServers);
    // use overall can achieve both table and cluster level balance
    HashMap<TableName, TreeMap<ServerName, List<HRegionInfo>>> result1 = mockClusterServersWithTables(clusterServers);
    loadBalancer.setClusterLoad(clusterLoad);
    List<RegionPlan> clusterplans1 = new ArrayList<RegionPlan>();
    List<Pair<TableName, Integer>> regionAmountList = new ArrayList<Pair<TableName, Integer>>();
    for(TreeMap<ServerName, List<HRegionInfo>> servers : result1.values()){
      List<ServerAndLoad> list = convertToList(servers);
      LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));
      List<RegionPlan> partialplans = loadBalancer.balanceCluster(servers);
      if(partialplans != null) clusterplans1.addAll(partialplans);
      List<ServerAndLoad> balancedClusterPerTable = reconcile(list, partialplans, servers);
      LOG.info("Mock Balance : " + printMock(balancedClusterPerTable));
      assertClusterAsBalanced(balancedClusterPerTable);
      for (Map.Entry<ServerName, List<HRegionInfo>> entry : servers.entrySet()) {
        returnRegions(entry.getValue());
        returnServer(entry.getKey());
      }
    }
    List<ServerAndLoad> balancedCluster1 = reconcile(clusterList, clusterplans1, clusterServers);
    assertTrue(assertClusterOverallAsBalanced(balancedCluster1, result1.keySet().size()));
  }
}
