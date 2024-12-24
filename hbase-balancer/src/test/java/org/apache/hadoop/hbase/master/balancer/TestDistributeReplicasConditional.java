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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
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
    int primaryRegionIndex = 0;
    int destinationServerIndex = 1;
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("No violation when only one location exists",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, primaryRegionIndex, "Host",
        false));
  }

  @Test
  public void testReplicaPlacedInSameHostAsPrimary() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    // Simulate two servers (Server 0 and Server 1) on the same host (Host 0)
    int[][] serversPerLocation = { { 0, 1 } };
    int[] serverToLocationIndex = { 0, 0 }; // Both servers belong to the same host (index 0)
    int[][] regionsPerServer = { { 0 }, { 1 } }; // Server 0 hosts primary, Server 1 hosts replica
    int primaryRegionIndex = 0; // Index of primary region
    int destinationServerIndex = 1; // Replica is being moved to Server 1
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("Host violation detected when replica is placed on the same host as its primary",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, primaryRegionIndex, "Host",
        false));
  }

  @Test
  public void testPrimaryPlacedInSameHostAsReplica() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    int[][] serversPerLocation = { { 0, 1 } };
    int[] serverToLocationIndex = { 0, 0 }; // Both servers belong to the same host
    int[][] regionsPerServer = { { 0 }, { 1 } };
    int primaryRegionIndex = 0;
    int destinationServerIndex = 0;
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("Violation detected when the primary is placed on the same host as its replica",
      DistributeReplicasConditional.checkViolation(regions, primaryRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, primaryRegionIndex, "Host",
        false));
  }

  @Test
  public void testReplicaPlacedInSameRackAsPrimary() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    int[][] serversPerLocation = { { 0, 1 }, { 2 } };
    int[] serverToLocationIndex = { 0, 0, 1 };
    int[][] regionsPerServer = { { 0 }, { 1 }, {} };
    int primaryRegionIndex = 0;
    int destinationServerIndex = 1;
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertTrue("Rack violation detected when replica is placed on the same rack as its primary",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, primaryRegionIndex, "Rack",
        false));
  }

  @Test
  public void testNoViolationOnDifferentHostAndRack() {
    RegionInfo primaryRegion = createRegionInfo("table1", 0, 100, 0);
    RegionInfo replicaRegion = createRegionInfo("table1", 0, 100, 1);

    int[][] serversPerLocation = { { 0 }, { 1 } };
    int[] serverToLocationIndex = { 0, 1 };
    int[][] regionsPerServer = { { 0 }, { 1 } };
    int primaryRegionIndex = 0;
    int destinationServerIndex = 1;
    RegionInfo[] regions = { primaryRegion, replicaRegion };

    assertFalse("No violation when replica is placed on a different host and rack",
      DistributeReplicasConditional.checkViolation(regions, replicaRegion, destinationServerIndex,
        serversPerLocation, serverToLocationIndex, regionsPerServer, primaryRegionIndex, "Host",
        false));
  }

  private static RegionInfo createRegionInfo(String tableName, int startKey, int stopKey,
    int replicaId) {
    return RegionInfoBuilder.newBuilder(TableName.valueOf(tableName))
      .setStartKey(new byte[] { (byte) startKey }).setEndKey(new byte[] { (byte) stopKey })
      .setReplicaId(replicaId).build();
  }
}
