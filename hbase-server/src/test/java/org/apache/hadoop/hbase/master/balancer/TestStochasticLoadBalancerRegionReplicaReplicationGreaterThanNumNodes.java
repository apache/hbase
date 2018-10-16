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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestStochasticLoadBalancerRegionReplicaReplicationGreaterThanNumNodes
    extends BalancerTestBase2 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
      .forClass(TestStochasticLoadBalancerRegionReplicaReplicationGreaterThanNumNodes.class);

  @Test
  public void testRegionReplicationOnMidClusterReplicationGreaterThanNumNodes() {
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 120 * 1000); // 120 sec
    loadBalancer.setConf(conf);
    int numNodes = 40;
    int numRegions = 6 * 50;
    int replication = 50; // 50 replicas per region, more than numNodes
    int numRegionsPerServer = 6;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, false);
  }
}
