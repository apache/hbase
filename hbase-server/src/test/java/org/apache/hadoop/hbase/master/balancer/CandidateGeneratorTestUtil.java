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

import static org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.MAX_RUNNING_TIME_KEY;
import static org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.MIN_COST_NEED_BALANCE_KEY;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.replicas.ReplicaKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CandidateGeneratorTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CandidateGeneratorTestUtil.class);

  private CandidateGeneratorTestUtil() {
  }

  static void runBalancerToExhaustion(Configuration conf,
    Map<ServerName, List<RegionInfo>> serverToRegions,
    Set<Function<BalancerClusterState, Boolean>> expectations, float targetMaxBalancerCost) {
    // Do the full plan. We're testing with a lot of regions
    conf.setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);
    conf.setLong(MAX_RUNNING_TIME_KEY, 15000);

    conf.setFloat(MIN_COST_NEED_BALANCE_KEY, targetMaxBalancerCost);

    BalancerClusterState cluster = createMockBalancerClusterState(serverToRegions);
    StochasticLoadBalancer stochasticLoadBalancer = buildStochasticLoadBalancer(cluster, conf);
    printClusterDistribution(cluster, 0);
    int balancerRuns = 0;
    int actionsTaken = 0;
    long balancingMillis = 0;
    boolean isBalanced = false;
    while (!isBalanced) {
      balancerRuns++;
      if (balancerRuns > 1000) {
        throw new RuntimeException("Balancer failed to find balance & meet expectations");
      }
      long start = System.currentTimeMillis();
      List<RegionPlan> regionPlans =
        stochasticLoadBalancer.balanceCluster(partitionRegionsByTable(serverToRegions));
      balancingMillis += System.currentTimeMillis() - start;
      actionsTaken++;
      if (regionPlans != null) {
        // Apply all plans to serverToRegions
        for (RegionPlan rp : regionPlans) {
          ServerName source = rp.getSource();
          ServerName dest = rp.getDestination();
          RegionInfo region = rp.getRegionInfo();

          // Update serverToRegions
          serverToRegions.get(source).remove(region);
          serverToRegions.get(dest).add(region);
          actionsTaken++;
        }

        // Now rebuild cluster and balancer from updated serverToRegions
        cluster = createMockBalancerClusterState(serverToRegions);
        stochasticLoadBalancer = buildStochasticLoadBalancer(cluster, conf);
      }
      printClusterDistribution(cluster, actionsTaken);
      isBalanced = true;
      for (Function<BalancerClusterState, Boolean> condition : expectations) {
        // Check if we've met all expectations for the candidate generator
        if (!condition.apply(cluster)) {
          isBalanced = false;
          break;
        }
      }
      if (isBalanced) { // Check if the balancer thinks we're done too
        LOG.info("All balancer conditions passed. Checking if balancer thinks it's done.");
        if (stochasticLoadBalancer.needsBalance(HConstants.ENSEMBLE_TABLE_NAME, cluster)) {
          LOG.info("Balancer would still like to run");
          isBalanced = false;
        } else {
          LOG.info("Balancer is done");
        }
      }
    }
    LOG.info("Balancing took {}sec", Duration.ofMillis(balancingMillis).toMinutes());
  }

  /**
   * Prints the current cluster distribution of regions per table per server
   */
  static void printClusterDistribution(BalancerClusterState cluster, long actionsTaken) {
    LOG.info("=== Cluster Distribution after {} balancer actions taken ===", actionsTaken);

    for (int i = 0; i < cluster.numServers; i++) {
      int[] regions = cluster.regionsPerServer[i];
      int regionCount = (regions == null) ? 0 : regions.length;

      LOG.info("Server {}: {} regions", cluster.servers[i].getServerName(), regionCount);

      if (regionCount > 0) {
        Map<TableName, Integer> tableRegionCounts = new HashMap<>();

        for (int regionIndex : regions) {
          RegionInfo regionInfo = cluster.regions[regionIndex];
          TableName tableName = regionInfo.getTable();
          tableRegionCounts.put(tableName, tableRegionCounts.getOrDefault(tableName, 0) + 1);
        }

        tableRegionCounts
          .forEach((table, count) -> LOG.info("  - Table {}: {} regions", table, count));
      }
    }

    LOG.info("===========================================");
  }

  /**
   * Partitions the given serverToRegions map by table The tables are derived from the RegionInfo
   * objects found in serverToRegions.
   * @param serverToRegions The map of servers to their assigned regions.
   * @return A map of tables to their server-to-region assignments.
   */
  public static Map<TableName, Map<ServerName, List<RegionInfo>>>
    partitionRegionsByTable(Map<ServerName, List<RegionInfo>> serverToRegions) {

    // First, gather all tables from the regions
    Set<TableName> allTables = new HashSet<>();
    for (List<RegionInfo> regions : serverToRegions.values()) {
      for (RegionInfo region : regions) {
        allTables.add(region.getTable());
      }
    }

    Map<TableName, Map<ServerName, List<RegionInfo>>> tablesToServersToRegions = new HashMap<>();

    // Initialize each table with all servers mapped to empty lists
    for (TableName table : allTables) {
      Map<ServerName, List<RegionInfo>> serverMap = new HashMap<>();
      for (ServerName server : serverToRegions.keySet()) {
        serverMap.put(server, new ArrayList<>());
      }
      tablesToServersToRegions.put(table, serverMap);
    }

    // Distribute regions to their respective tables
    for (Map.Entry<ServerName, List<RegionInfo>> serverAndRegions : serverToRegions.entrySet()) {
      ServerName server = serverAndRegions.getKey();
      List<RegionInfo> regions = serverAndRegions.getValue();

      for (RegionInfo region : regions) {
        TableName regionTable = region.getTable();
        // Now we know for sure regionTable is in allTables
        Map<ServerName, List<RegionInfo>> tableServerMap =
          tablesToServersToRegions.get(regionTable);
        tableServerMap.get(server).add(region);
      }
    }

    return tablesToServersToRegions;
  }

  static StochasticLoadBalancer buildStochasticLoadBalancer(BalancerClusterState cluster,
    Configuration conf) {
    StochasticLoadBalancer stochasticLoadBalancer =
      new StochasticLoadBalancer(new DummyMetricsStochasticBalancer());
    stochasticLoadBalancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    stochasticLoadBalancer.loadConf(conf);
    stochasticLoadBalancer.initCosts(cluster);
    return stochasticLoadBalancer;
  }

  static BalancerClusterState
    createMockBalancerClusterState(Map<ServerName, List<RegionInfo>> serverToRegions) {
    return new BalancerClusterState(serverToRegions, null, null, null, null);
  }

  /**
   * Validates that each replica is isolated from its others. Ensures that no server hosts more than
   * one replica of the same region (i.e., regions with identical start and end keys).
   * @param cluster The current state of the cluster.
   * @return true if all replicas are properly isolated, false otherwise.
   */
  static boolean areAllReplicasDistributed(BalancerClusterState cluster) {
    // Iterate over each server
    for (int[] regionsPerServer : cluster.regionsPerServer) {
      if (regionsPerServer == null || regionsPerServer.length == 0) {
        continue; // Skip empty servers
      }

      Set<ReplicaKey> foundKeys = new HashSet<>();
      for (int regionIndex : regionsPerServer) {
        RegionInfo regionInfo = cluster.regions[regionIndex];
        ReplicaKey replicaKey = new ReplicaKey(regionInfo);
        if (foundKeys.contains(replicaKey)) {
          // Violation: Multiple replicas of the same region on the same server
          LOG.warn("Replica isolation violated: one server hosts multiple replicas of key [{}].",
            generateRegionKey(regionInfo));
          return false;
        }

        foundKeys.add(replicaKey);
      }
    }

    LOG.info(
      "Replica isolation validation passed: No server hosts multiple replicas of the same region.");
    return true;
  }

  /**
   * Generates a unique key for a region based on its start and end keys. This method ensures that
   * regions with identical start and end keys have the same key.
   * @param regionInfo The RegionInfo object.
   * @return A string representing the unique key of the region.
   */
  private static String generateRegionKey(RegionInfo regionInfo) {
    // Using Base64 encoding for byte arrays to ensure uniqueness and readability
    String startKey = Base64.getEncoder().encodeToString(regionInfo.getStartKey());
    String endKey = Base64.getEncoder().encodeToString(regionInfo.getEndKey());

    return regionInfo.getTable().getNameAsString() + ":" + startKey + ":" + endKey;
  }

}
