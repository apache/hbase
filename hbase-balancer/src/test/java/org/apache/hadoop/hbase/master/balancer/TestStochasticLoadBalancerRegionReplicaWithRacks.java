/*
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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestStochasticLoadBalancerRegionReplicaWithRacks extends StochasticBalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStochasticLoadBalancerRegionReplicaWithRacks.class);

  private static class ForTestRackManager extends RackManager {

    int numRacks;
    Map<String, Integer> serverIndexes = new HashMap<String, Integer>();
    int numServers = 0;

    public ForTestRackManager(int numRacks) {
      this.numRacks = numRacks;
    }

    @Override
    public String getRack(ServerName server) {
      String key = server.getServerName();
      if (!serverIndexes.containsKey(key)) {
        serverIndexes.put(key, numServers++);
      }
      return "rack_" + serverIndexes.get(key) % numRacks;
    }
  }

  @Test
  public void testRegionReplicationOnMidClusterWithRacks() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 100000000L);
    conf.setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);
    setMaxRunTime(Duration.ofSeconds(5));
    loadBalancer.onConfigurationChange(conf);
    int numNodes = 5;
    int numRegions = numNodes * 1;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 1;
    int numTables = 1;
    int numRacks = 3; // all replicas should be on a different rack
    Map<ServerName, List<RegionInfo>> serverMap =
      createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    RackManager rm = new ForTestRackManager(numRacks);
    testWithClusterWithIteration(serverMap, rm, true, true);
  }

  @Test
  public void testRegionReplicationOnLargeClusterWithRacks() {
    conf.setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 100000000L);
    setMaxRunTime(Duration.ofSeconds(10));
    loadBalancer.onConfigurationChange(conf);
    int numNodes = 100;
    int numRegions = numNodes * 30;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 28;
    int numTables = 1;
    int numRacks = 4; // all replicas should be on a different rack
    Map<ServerName, List<RegionInfo>> serverMap =
      createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    RackManager rm = new ForTestRackManager(numRacks);

    testWithClusterWithIteration(serverMap, rm, true, true);
  }
}
