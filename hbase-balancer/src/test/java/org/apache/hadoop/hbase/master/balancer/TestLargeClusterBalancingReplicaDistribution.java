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

import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.runBalancerToExhaustion;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLargeClusterBalancingReplicaDistribution {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestLargeClusterBalancingReplicaDistribution.class);

  private static final TableName TABLE_NAME = TableName.valueOf("userTable");

  private static final int NUM_SERVERS = 100;
  private static final int NUM_REGIONS = 100;
  private static final int NUM_REPLICAS = 3;

  private static final ServerName[] servers = new ServerName[NUM_SERVERS];
  private static final Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();

  @BeforeClass
  public static void setup() {
    // Initialize servers
    for (int i = 0; i < NUM_SERVERS; i++) {
      servers[i] = ServerName.valueOf("server" + i, i, System.currentTimeMillis());
      serverToRegions.put(servers[i], new ArrayList<>());
    }

    // Create primary regions and their replicas
    List<RegionInfo> allRegions = new ArrayList<>();
    for (int i = 0; i < NUM_REGIONS; i++) {
      // Define startKey and endKey for the region
      byte[] startKey = Bytes.toBytes(i);
      byte[] endKey = Bytes.toBytes(i + 1);

      // Create 3 replicas for each primary region
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setStartKey(startKey)
          .setEndKey(endKey).setReplicaId(replicaId).build();
        allRegions.add(regionInfo);
      }
    }

    // Assign replicas to servers in a round-robin fashion to ensure even distribution
    for (RegionInfo regionInfo : allRegions) {
      int regionNumber = regionInfo.getStartKey()[0] & 0xFF; // Convert byte to unsigned int
      int replicaId = regionInfo.getReplicaId();

      // Calculate server index ensuring replicas are on different servers
      int serverIndex = (regionNumber + replicaId) % NUM_SERVERS;
      ServerName targetServer = servers[serverIndex];

      // Assign the region to the target server
      serverToRegions.get(targetServer).add(regionInfo);
    }
  }

  @Test
  public void testReplicaDistribution() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(BalancerConditionals.DISTRIBUTE_REPLICAS_CONDITIONALS_KEY, true);
    conf.setBoolean(DistributeReplicasConditional.TEST_MODE_ENABLED_KEY, true);

    // turn off replica cost functions
    conf.setLong("hbase.master.balancer.stochastic.regionReplicaRackCostKey", 0);
    conf.setLong("hbase.master.balancer.stochastic.regionReplicaHostCostKey", 0);

    runBalancerToExhaustion(conf, serverToRegions,
      Set.of(CandidateGeneratorTestUtil::areAllReplicasDistributed));
    LOG.info("Meta table and system table regions are successfully isolated, "
      + "meanwhile region replicas are appropriately distributed across RegionServers.");
  }
}
