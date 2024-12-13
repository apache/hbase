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

import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.isTableColocated;
import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.isTableIsolated;
import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.runBalancerToExhaustion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestLargeClusterBalancingMultiTableIsolationReplicaDistribution {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
    .forClass(TestLargeClusterBalancingMultiTableIsolationReplicaDistribution.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestLargeClusterBalancingMultiTableIsolationReplicaDistribution.class);

  private static final TableName META_TABLE_NAME = TableName.valueOf("hbase:meta");
  private static final TableName SYSTEM_TABLE_NAME = TableName.valueOf("hbase:system");
  private static final TableName NON_ISOLATED_TABLE_NAME = TableName.valueOf("userTable");

  private static final int NUM_SERVERS = 100;
  private static final int NUM_REGIONS = 4_000;
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
      TableName tableName;
      if (i < 1) {
        tableName = META_TABLE_NAME;
      } else if (i < 10) {
        tableName = SYSTEM_TABLE_NAME;
      } else {
        tableName = NON_ISOLATED_TABLE_NAME;
      }

      // Define startKey and endKey for the region
      byte[] startKey = new byte[1];
      startKey[0] = (byte) i;
      byte[] endKey = new byte[1];
      endKey[0] = (byte) (i + 1);

      // Create 3 replicas for each primary region
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey)
          .setEndKey(endKey).setReplicaId(replicaId).build();
        allRegions.add(regionInfo);
      }
    }

    // Assign all regions to one server
    for (RegionInfo regionInfo : allRegions) {
      serverToRegions.get(servers[0]).add(regionInfo);
    }
  }

  @Test
  public void testMultiTableIsolationReplicaDistribution() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);
    conf.setBoolean(BalancerConditionals.ISOLATE_SYSTEM_TABLES_KEY, true);
    conf.setBoolean(BalancerConditionals.DISTRIBUTE_REPLICAS_KEY, true);
    conf.setBoolean(DistributeReplicasConditional.TEST_MODE_ENABLED_KEY, true);

    runBalancerToExhaustion(conf, serverToRegions,
      Set.of(this::isMetaTableIsolated, this::isSystemTableIsolated,
        CandidateGeneratorTestUtil::areAllReplicasDistributed, this::isSystemTableColocated),
      0.1f);
    LOG.info("Meta table, system table, and replicas are successfully isolated.");
  }

  /**
   * Validates whether all meta table regions are isolated.
   */
  private boolean isMetaTableIsolated(BalancerClusterState cluster) {
    return isTableIsolated(cluster, META_TABLE_NAME, "Meta");
  }

  /**
   * Validates whether all system table regions are isolated.
   */
  private boolean isSystemTableIsolated(BalancerClusterState cluster) {
    return isTableIsolated(cluster, SYSTEM_TABLE_NAME, "System");
  }

  /**
   * Validates whether all system tables regions are colocated. We probably don't want 10 primary
   * system table regions, for example, to take up 10 isolated servers. This is enforced by the
   * balancer's cost functions.
   */
  private boolean isSystemTableColocated(BalancerClusterState cluster) {
    return isTableColocated(cluster, SYSTEM_TABLE_NAME, "System", NUM_REPLICAS)
      && isTableColocated(cluster, TableName.META_TABLE_NAME, "Meta", NUM_REPLICAS);
  }
}
