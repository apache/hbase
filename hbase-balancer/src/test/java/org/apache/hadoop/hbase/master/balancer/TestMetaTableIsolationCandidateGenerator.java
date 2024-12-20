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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMetaTableIsolationCandidateGenerator {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestMetaTableIsolationCandidateGenerator.class);

  private static final TableName META_TABLE_NAME = TableName.valueOf("hbase:meta");
  private static final TableName NON_META_TABLE_NAME = TableName.valueOf("userTable");

  private static final int NUM_SERVERS = 100;
  private static final int NUM_REGIONS = 10_000;

  private static final ServerName[] servers = new ServerName[NUM_SERVERS];
  private static final Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();

  @BeforeClass
  public static void setup() {
    // Initialize servers
    for (int i = 0; i < NUM_SERVERS; i++) {
      servers[i] = ServerName.valueOf("server" + i, i, System.currentTimeMillis());
    }

    // Create regions
    List<RegionInfo> allRegions = new ArrayList<>();
    for (int i = 0; i < NUM_REGIONS; i++) {
      TableName tableName = i < 3 ? META_TABLE_NAME : NON_META_TABLE_NAME;
      byte[] startKey = new byte[1];
      startKey[0] = (byte) i;
      byte[] endKey = new byte[1];
      endKey[0] = (byte) (i + 1);

      RegionInfo regionInfo =
        RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
      allRegions.add(regionInfo);
    }

    // Assign all regions to the first server
    serverToRegions.put(servers[0], new ArrayList<>(allRegions));
    for (int i = 1; i < NUM_SERVERS; i++) {
      serverToRegions.put(servers[i], new ArrayList<>());
    }
  }

  @Test
  public void testMetaTableIsolation() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);
    runBalancerToExhaustion(conf, serverToRegions, Set.of(this::isMetaTableIsolated));
  }

  /**
   * Validates whether all meta table regions are isolated, meaning they are not on the same server
   * as any non-meta table regions.
   * @param cluster The cluster state to validate
   * @return true if all meta table regions are isolated, false otherwise
   */
  private boolean isMetaTableIsolated(BalancerClusterState cluster) {
    for (int i = 0; i < cluster.numServers; i++) {
      int[] regionsOnServer = cluster.regionsPerServer[i];
      if (regionsOnServer == null || regionsOnServer.length == 0) {
        continue; // Skip empty servers
      }

      boolean hasMetaTableRegion = false;
      boolean hasNonMetaTableRegion = false;

      for (int regionIndex : regionsOnServer) {
        RegionInfo regionInfo = cluster.regions[regionIndex];
        if (regionInfo.getTable().equals(META_TABLE_NAME)) {
          hasMetaTableRegion = true;
        } else if (regionInfo.getTable().equals(NON_META_TABLE_NAME)) {
          hasNonMetaTableRegion = true;
        }

        // If both meta and non-meta table regions exist on this server, isolation is violated
        if (hasMetaTableRegion && hasNonMetaTableRegion) {
          LOG.warn("Server {} has both meta and non-meta table regions, violating isolation.",
            cluster.servers[i].getServerName());
          return false;
        }
      }
    }
    LOG.info("Meta table isolation validation passed.");
    return true;
  }

}
