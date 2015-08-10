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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestStochasticLoadBalancer2 extends TestStochasticLoadBalancer {
  private static final Log LOG = LogFactory.getLog(TestStochasticLoadBalancer2.class);

  @Test (timeout = 800000)
  public void testRegionReplicasOnMidCluster() {
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 2000000L);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 90 * 1000); // 90 sec
    TestStochasticLoadBalancer.loadBalancer.setConf(conf);
    int numNodes = 200;
    int numRegions = 40 * 200;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 30; //all regions are mostly balanced
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Test (timeout = 800000)
  public void testRegionReplicasOnLargeCluster() {
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 2000000L);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 90 * 1000); // 90 sec
    loadBalancer.setConf(conf);
    int numNodes = 1000;
    int numRegions = 20 * numNodes; // 20 * replication regions per RS
    int numRegionsPerServer = 19; // all servers except one
    int numTables = 100;
    int replication = 3;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  @Test (timeout = 800000)
  public void testRegionReplicasOnMidClusterHighReplication() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 4000000L);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 120 * 1000); // 120 sec
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    loadBalancer.setConf(conf);
    int numNodes = 80;
    int numRegions = 6 * numNodes;
    int replication = 80; // 80 replicas per region, one for each server
    int numRegionsPerServer = 5;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, false, true);
  }

  @Test (timeout = 800000)
  public void testRegionReplicationOnMidClusterReplicationGreaterThanNumNodes() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 2000000L);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 120 * 1000); // 120 sec
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 1.0f);
    loadBalancer.setConf(conf);
    int numNodes = 40;
    int numRegions = 6 * 50;
    int replication = 50; // 50 replicas per region, more than numNodes
    int numRegionsPerServer = 6;
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, false);
  }
}
