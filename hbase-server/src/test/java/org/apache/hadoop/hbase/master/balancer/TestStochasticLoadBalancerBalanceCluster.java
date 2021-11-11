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

import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestStochasticLoadBalancerBalanceCluster extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticLoadBalancerBalanceCluster.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStochasticLoadBalancerBalanceCluster.class);

  /**
   * Test the load balancing algorithm.
   * <p>
   * Invariant is that all servers should be hosting either floor(average) or ceiling(average)
   */
  @Test
  public void testBalanceCluster() throws Exception {
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 2 * 60 * 1000); // 300 sec
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 20000000L);
    loadBalancer.setConf(conf);
    for (int[] mockCluster : clusterStateMocks) {
      Map<ServerName, List<RegionInfo>> servers = mockClusterServers(mockCluster);
      List<ServerAndLoad> list = convertToList(servers);
      LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));

      Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
          (Map) mockClusterServersWithTables(servers);
      List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
      List<ServerAndLoad> balancedCluster = reconcile(list, plans, servers);
      LOG.info("Mock Balance : " + printMock(balancedCluster));
      assertClusterAsBalanced(balancedCluster);
      LoadOfAllTable = (Map) mockClusterServersWithTables(servers);
      List<RegionPlan> secondPlans = loadBalancer.balanceCluster(LoadOfAllTable);
      assertNull(secondPlans);
      for (Map.Entry<ServerName, List<RegionInfo>> entry : servers.entrySet()) {
        returnRegions(entry.getValue());
        returnServer(entry.getKey());
      }
    }
  }
}
