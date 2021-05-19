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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StochasticBalancerTestBase extends BalancerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StochasticBalancerTestBase.class);

  protected static StochasticLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 0.75f);
    conf.setFloat("hbase.regions.slop", 0.0f);
    conf.setFloat("hbase.master.balancer.stochastic.localityCost", 0);
    loadBalancer = new StochasticLoadBalancer();
    loadBalancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    loadBalancer.initialize();
  }

  protected void testWithCluster(int numNodes, int numRegions, int numRegionsPerServer,
    int replication, int numTables, boolean assertFullyBalanced,
    boolean assertFullyBalancedForReplicas) {
    Map<ServerName, List<RegionInfo>> serverMap =
      createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    testWithCluster(serverMap, null, assertFullyBalanced, assertFullyBalancedForReplicas);
  }

  protected void testWithCluster(Map<ServerName, List<RegionInfo>> serverMap,
    RackManager rackManager, boolean assertFullyBalanced, boolean assertFullyBalancedForReplicas) {
    List<ServerAndLoad> list = convertToList(serverMap);
    LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));

    loadBalancer.setRackManager(rackManager);
    // Run the balancer.
    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(serverMap);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    assertNotNull("Initial cluster balance should produce plans.", plans);

    // Check to see that this actually got to a stable place.
    if (assertFullyBalanced || assertFullyBalancedForReplicas) {
      // Apply the plan to the mock cluster.
      List<ServerAndLoad> balancedCluster = reconcile(list, plans, serverMap);

      // Print out the cluster loads to make debugging easier.
      LOG.info("Mock Balance : " + printMock(balancedCluster));

      if (assertFullyBalanced) {
        assertClusterAsBalanced(balancedCluster);
        LoadOfAllTable = (Map) mockClusterServersWithTables(serverMap);
        List<RegionPlan> secondPlans = loadBalancer.balanceCluster(LoadOfAllTable);
        assertNull("Given a requirement to be fully balanced, second attempt at plans should " +
          "produce none.", secondPlans);
      }

      if (assertFullyBalancedForReplicas) {
        assertRegionReplicaPlacement(serverMap, rackManager);
      }
    }
  }
}
