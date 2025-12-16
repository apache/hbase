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

import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.isTableIsolated;
import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.runBalancerToExhaustion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.balancer.BalancerTestBase.MockMapping;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class, MasterTests.class })
public class TestLargeClusterBalancingTableIsolationAndReplicaDistribution {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
    .forClass(TestLargeClusterBalancingTableIsolationAndReplicaDistribution.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestLargeClusterBalancingTableIsolationAndReplicaDistribution.class);
  private static final TableName SYSTEM_TABLE_NAME = TableName.valueOf("hbase:system");
  private static final TableName NON_ISOLATED_TABLE_NAME = TableName.valueOf("userTable");

  private static final int NUM_SERVERS = 500;
  private static final int NUM_REGIONS = 2_500;
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
    for (int i = 0; i < NUM_REGIONS; i++) {
      TableName tableName;
      if (i < 1) {
        tableName = MetaTableName.getInstance();
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

      Random random = new Random();
      // Create 3 replicas for each primary region
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey)
          .setEndKey(endKey).setReplicaId(replicaId).build();
        // Assign region to random server
        int randomServer = random.nextInt(servers.length);
        serverToRegions.get(servers[randomServer]).add(regionInfo);
      }
    }
  }

  @Test
  public void testTableIsolationAndReplicaDistribution() {
    Configuration conf = new Configuration(false);
    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    conf.setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);
    conf.setBoolean(BalancerConditionals.ISOLATE_SYSTEM_TABLES_KEY, true);
    DistributeReplicasTestConditional.enableConditionalReplicaDistributionForTest(conf);

    runBalancerToExhaustion(conf, serverToRegions,
      Set.of(this::isMetaTableIsolated, this::isSystemTableIsolated,
        CandidateGeneratorTestUtil::areAllReplicasDistributed),
      10.0f, 60_000, CandidateGeneratorTestUtil.ExhaustionType.COST_GOAL_ACHIEVED);
    LOG.info("Meta table regions are successfully isolated, "
      + "and region replicas are appropriately distributed.");
  }

  /**
   * Validates whether all meta table regions are isolated.
   */
  private boolean isMetaTableIsolated(BalancerClusterState cluster) {
    return isTableIsolated(cluster, MetaTableName.getInstance(), "Meta");
  }

  /**
   * Validates whether all meta table regions are isolated.
   */
  private boolean isSystemTableIsolated(BalancerClusterState cluster) {
    return isTableIsolated(cluster, SYSTEM_TABLE_NAME, "System");
  }
}
