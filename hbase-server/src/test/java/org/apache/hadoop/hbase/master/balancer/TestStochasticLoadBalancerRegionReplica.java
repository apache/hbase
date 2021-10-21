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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestStochasticLoadBalancerRegionReplica extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStochasticLoadBalancerRegionReplica.class);

  @Test
  public void testReplicaCost() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction costFunction =
        new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);
    for (int[] mockCluster : clusterStateMocks) {
      BaseLoadBalancer.Cluster cluster = mockCluster(mockCluster);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertTrue(cost >= 0);
      assertTrue(cost <= 1.01);
    }
  }

  @Test
  public void testReplicaCostForReplicas() {
    Configuration conf = HBaseConfiguration.create();
    StochasticLoadBalancer.CostFunction costFunction =
        new StochasticLoadBalancer.RegionReplicaHostCostFunction(conf);

    int[] servers = new int[] { 3, 3, 3, 3, 3 };
    TreeMap<ServerName, List<RegionInfo>> clusterState = mockClusterServers(servers);

    BaseLoadBalancer.Cluster cluster;

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWithoutReplicas = costFunction.cost();
    assertEquals(0, costWithoutReplicas, 0);

    // replicate the region from first server to the last server
    RegionInfo replica1 =
        RegionReplicaUtil.getRegionInfoForReplica(clusterState.firstEntry().getValue().get(0), 1);
    clusterState.lastEntry().getValue().add(replica1);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaDifferentServer = costFunction.cost();

    assertEquals(0, costWith1ReplicaDifferentServer, 0);

    // add a third replica to the last server
    RegionInfo replica2 = RegionReplicaUtil.getRegionInfoForReplica(replica1, 2);
    clusterState.lastEntry().getValue().add(replica2);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith1ReplicaSameServer = costFunction.cost();

    assertTrue(costWith1ReplicaDifferentServer < costWith1ReplicaSameServer);

    // test with replication = 4 for following:

    RegionInfo replica3;
    Iterator<Entry<ServerName, List<RegionInfo>>> it;
    Entry<ServerName, List<RegionInfo>> entry;

    clusterState = mockClusterServers(servers);
    it = clusterState.entrySet().iterator();
    entry = it.next(); // first server
    RegionInfo hri = entry.getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);
    entry.getValue().add(replica1);
    entry.getValue().add(replica2);
    it.next().getValue().add(replica3); // 2nd server

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith3ReplicasSameServer = costFunction.cost();

    clusterState = mockClusterServers(servers);
    hri = clusterState.firstEntry().getValue().get(0);
    replica1 = RegionReplicaUtil.getRegionInfoForReplica(hri, 1);
    replica2 = RegionReplicaUtil.getRegionInfoForReplica(hri, 2);
    replica3 = RegionReplicaUtil.getRegionInfoForReplica(hri, 3);

    clusterState.firstEntry().getValue().add(replica1);
    clusterState.lastEntry().getValue().add(replica2);
    clusterState.lastEntry().getValue().add(replica3);

    cluster = new BaseLoadBalancer.Cluster(clusterState, null, null, null);
    costFunction.init(cluster);
    double costWith2ReplicasOnTwoServers = costFunction.cost();

    assertTrue(costWith2ReplicasOnTwoServers < costWith3ReplicasSameServer);
  }

  @Test
  public void testNeedsBalanceForColocatedReplicas() {
    // check for the case where there are two hosts and with one rack, and where
    // both the replicas are hosted on the same server
    List<RegionInfo> regions = randomRegions(1);
    ServerName s1 = ServerName.valueOf("host1", 1000, 11111);
    ServerName s2 = ServerName.valueOf("host11", 1000, 11111);
    Map<ServerName, List<RegionInfo>> map = new HashMap<>();
    map.put(s1, regions);
    regions.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    // until the step above s1 holds two replicas of a region
    regions = randomRegions(1);
    map.put(s2, regions);
    assertTrue(loadBalancer.needsBalance(HConstants.ENSEMBLE_TABLE_NAME,
      new Cluster(map, null, null, null)));
    // check for the case where there are two hosts on the same rack and there are two racks
    // and both the replicas are on the same rack
    map.clear();
    regions = randomRegions(1);
    List<RegionInfo> regionsOnS2 = new ArrayList<>(1);
    regionsOnS2.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(0), 1));
    map.put(s1, regions);
    map.put(s2, regionsOnS2);
    // add another server so that the cluster has some host on another rack
    map.put(ServerName.valueOf("host2", 1000, 11111), randomRegions(1));
    assertFalse(
      loadBalancer.needsBalance(HConstants.ENSEMBLE_TABLE_NAME,
        new Cluster(map, null, null, new ForTestRackManagerOne())));
  }

  @Test
  public void testRegionReplicasOnSmallCluster() {
    int numNodes = 10;
    int numRegions = 1000;
    int replication = 3; // 3 replicas per region
    int numRegionsPerServer = 80; // all regions are mostly balanced
    int numTables = 10;
    testWithCluster(numNodes, numRegions, numRegionsPerServer, replication, numTables, true, true);
  }

  private static class ForTestRackManagerOne extends RackManager {
    @Override
    public String getRack(ServerName server) {
      return server.getHostname().endsWith("1") ? "rack1" : "rack2";
    }
  }
}
