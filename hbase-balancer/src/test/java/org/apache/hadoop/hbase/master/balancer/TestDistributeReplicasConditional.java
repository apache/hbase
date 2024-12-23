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

import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.createMockBalancerClusterState;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDistributeReplicasConditional {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDistributeReplicasConditional.class);

  @Test
  public void testInevitableViolationWithSingleLocation() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    int[][] serversPerLocation = { { 0, 1 } };
    int[] serverToLocationIndex = { 0, 0 }; // Both servers belong to the same location
    int[][] regionsPerServer = { { 0 }, { 1 } };
    int destinationServerIndex = 1;
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("No violation when only one location exists",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, "Host", false));
  }

  @Test
  public void testReplicaPlacedInSameHostAsPrimary() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    // Simulate two servers (Server 0 and Server 1) on the same host (Host 0)
    int[][] serversPerLocation = { { 0, 1 } };
    int[] serverToLocationIndex = { 0, 0 }; // Both servers belong to the same host (index 0)
    int[][] regionsPerServer = { { 0 }, { 1 } }; // Server 0 hosts primary, Server 1 hosts replica
    int destinationServerIndex = 1; // Replica is being moved to Server 1
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("Host violation detected when replica is placed on the same host as its primary",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, "Host", false));
  }

  @Test
  public void testPrimaryPlacedInSameHostAsReplica() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    int[][] serversPerLocation = { { 0, 1 } };
    int[] serverToLocationIndex = { 0, 0 }; // Both servers belong to the same host
    int[][] regionsPerServer = { { 0 }, { 1 } };
    int destinationServerIndex = 0;
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("Violation detected when the primary is placed on the same host as its replica",
      DistributeReplicasConditional.checkViolation(regions, primaryRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, "Host", false));
  }

  @Test
  public void testReplicaPlacedInSameRackAsPrimary() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    int[][] serversPerLocation = { { 0, 1 }, { 2 } };
    int[] serverToLocationIndex = { 0, 0, 1 };
    int[][] regionsPerServer = { { 0 }, { 1 }, {} };
    int destinationServerIndex = 1;
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("Rack violation detected when replica is placed on the same rack as its primary",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, "Rack", false));
  }

  @Test
  public void testNoViolationOnDifferentHostAndRack() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    int[][] serversPerLocation = { { 0 }, { 1 } };
    int[] serverToLocationIndex = { 0, 1 };

    // Assign regions to servers
    // Server 0 hosts both primary and replica regions initially
    // Server 1 hosts no regions initially
    int[][] regionsPerServer = { { 0, 1 }, {} };

    // Define the destination server index as Server 1
    int destinationServerIndex = 1;

    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertFalse("No violation when replica is placed on a different host and rack",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, "Host", false));
  }

  @Test
  public void testCandidateGenerator() {
    List<RegionInfo> regionsList = new ArrayList<>();
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);
    regionsList.add(primaryRegion);
    regionsList.add(replicaRegion);

    DistributeReplicasCandidateGenerator candidateGenerator =
      new DistributeReplicasCandidateGenerator();
    Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();
    serverToRegions.put(ServerName.valueOf("0", 0, 0L), regionsList);
    serverToRegions.put(ServerName.valueOf("1", 0, 0L), new ArrayList<>());

    BalancerClusterState cluster = createMockBalancerClusterState(serverToRegions);
    BalanceAction action = candidateGenerator.generateCandidate(cluster, false);
    DistributeReplicasConditional conditional =
      new DistributeReplicasConditional(new Configuration(), cluster);
    List<RegionPlan> regionPlan = cluster.doAction(action);
    List<RegionPlan> inversePlan = cluster.doAction(action.undoAction());

    regionPlan.forEach(plan -> assertFalse(conditional.isViolating(plan)));
    inversePlan.forEach(plan -> assertTrue(conditional.isViolating(plan)));
  }

  private static RegionInfo createRegionInfo(String tableName, int startKey, int stopKey,
    int replicaId) {
    return RegionInfoBuilder.newBuilder(TableName.valueOf(tableName))
      .setStartKey(new byte[] { (byte) startKey }).setEndKey(new byte[] { (byte) stopKey })
      .setReplicaId(replicaId).build();
  }
}
