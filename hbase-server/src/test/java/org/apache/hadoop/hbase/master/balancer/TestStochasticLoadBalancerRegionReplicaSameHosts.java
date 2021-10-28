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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestStochasticLoadBalancerRegionReplicaSameHosts extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticLoadBalancerRegionReplicaSameHosts.class);

  @Test
  public void testRegionReplicationOnMidClusterSameHosts() {
    conf.setLong(StochasticLoadBalancer.MAX_STEPS_KEY, 2000000L);
    conf.setLong("hbase.master.balancer.stochastic.maxRunningTime", 90 * 1000); // 90 sec
    loadBalancer.setConf(conf);
    int numHosts = 30;
    int numRegions = 30 * 30;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 5;
    int numTables = 10;
    Map<ServerName, List<RegionInfo>> serverMap =
        createServerMap(numHosts, numRegions, numRegionsPerServer, replication, numTables);
    int numNodesPerHost = 4;

    // create a new map with 4 RS per host.
    Map<ServerName, List<RegionInfo>> newServerMap = new TreeMap<>(serverMap);
    for (Map.Entry<ServerName, List<RegionInfo>> entry : serverMap.entrySet()) {
      for (int i = 1; i < numNodesPerHost; i++) {
        ServerName s1 = entry.getKey();
        // create an RS for the same host
        ServerName s2 = ServerName.valueOf(s1.getHostname(), s1.getPort() + i, 1);
        newServerMap.put(s2, new ArrayList<>());
      }
    }

    testWithCluster(newServerMap, null, true, true);
  }
}
