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

import static org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer.MIN_COST_NEED_BALANCE_KEY;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CandidateGeneratorTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CandidateGeneratorTestUtil.class);

  static void runBalancerToExhaustion(Configuration conf,
    Map<ServerName, List<RegionInfo>> serverToRegions,
    Set<Function<BalancerClusterState, Boolean>> expectations) {
    // Do the full plan. We're testing with a lot of regions
    conf.setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);

    // Somewhat less strict than the default.
    // This is acknowledging that we will be skewing loads to some degree
    // in order to maintain isolation.
    conf.setFloat(MIN_COST_NEED_BALANCE_KEY, 0.1f);

    Set<TableName> userTablesToBalance =
      serverToRegions.entrySet().stream().map(Map.Entry::getValue).flatMap(Collection::stream)
        .map(RegionInfo::getTable).filter(t -> !t.isSystemTable()).collect(Collectors.toSet());
    BalancerClusterState cluster = createMockBalancerClusterState(serverToRegions);
    StochasticLoadBalancer stochasticLoadBalancer = buildStochasticLoadBalancer(cluster, conf);
    printClusterDistribution(cluster, 0);
    int balancerRuns = 0;
    int actionsTaken = 0;
    long balancingMillis = 0;
    boolean isBalanced = false;
    while (!isBalanced) {
      balancerRuns++;
      if (balancerRuns > 10) {
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
      if (isBalanced) {
        // Check if the user tables look good too
        for (TableName tableName : userTablesToBalance) {
          if (stochasticLoadBalancer.needsBalance(tableName, cluster)) {
            isBalanced = false;
            break;
          }
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
   * Partitions the given serverToRegions map by table, returning a structure of Map<TableName,
   * Map<ServerName, List<RegionInfo>>>. The tables are derived from the RegionInfo objects found in
   * serverToRegions.
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
    StochasticLoadBalancer stochasticLoadBalancer = new StochasticLoadBalancer();
    stochasticLoadBalancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    stochasticLoadBalancer.loadConf(conf);
    stochasticLoadBalancer.initCosts(cluster);
    return stochasticLoadBalancer;
  }

  static BalancerClusterState
    createMockBalancerClusterState(Map<ServerName, List<RegionInfo>> serverToRegions) {
    return new BalancerClusterState(serverToRegions, null, null, null, null);
  }

  static Map<ServerName, List<RegionInfo>> syncServerToRegionsWithCluster(
    BalancerClusterState cluster, Map<ServerName, List<RegionInfo>> serverToRegions) {
    Map<ServerName, List<RegionInfo>> newServerToRegions = new HashMap<>();
    for (int i = 0; i < cluster.numServers; i++) {
      if (cluster.regionsPerServer[i] == null) {
        LOG.warn("Server {} has no region assignment array. Adding an empty list.",
          cluster.servers[i]);
        serverToRegions.put(cluster.servers[i], new ArrayList<>());
        continue;
      }

      List<RegionInfo> regions = new ArrayList<>();
      for (int regionIdx : cluster.regionsPerServer[i]) {
        regions.add(cluster.regions[regionIdx]);
      }
      serverToRegions.put(cluster.servers[i], regions);
    }
    return newServerToRegions;
  }

}
