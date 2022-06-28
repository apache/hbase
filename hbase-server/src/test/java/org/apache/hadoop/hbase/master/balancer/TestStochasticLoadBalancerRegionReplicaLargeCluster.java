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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestStochasticLoadBalancerRegionReplicaLargeCluster extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStochasticLoadBalancerRegionReplicaLargeCluster.class);

  @Test
  public void testRegionReplicasOnLargeCluster() {
    // With default values for moveCost and tableSkewCost, the balancer makes much slower progress.
    // Since we're only looking for balance in region counts and no colocated replicas, we can
    // ignore these two cost functions to allow us to make any move that helps other functions.
    conf.setFloat("hbase.master.balancer.stochastic.moveCost", 0f);
    conf.setFloat("hbase.master.balancer.stochastic.tableSkewCost", 0f);
    loadBalancer.onConfigurationChange(conf);
    int numNodes = 1000;
    int numRegions = 20 * numNodes; // 20 * replication regions per RS
    int numRegionsPerServer = 19; // all servers except one
    int numTables = 100;
    int replication = 3;
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 20000000L);
    testWithClusterWithIteration(numNodes, numRegions, numRegionsPerServer, replication, numTables,
      true, true);
  }
}
