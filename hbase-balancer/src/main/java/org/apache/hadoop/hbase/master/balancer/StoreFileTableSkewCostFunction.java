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

import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Lightweight cost function that mirrors TableSkewCostFunction but aggregates storefile sizes (in
 * MB) per table using the CostFromRegionLoadFunction framework. For each table, it computes a
 * per-server aggregated storefile size by summing the average storefile size for each region (if
 * there are multiple load metrics, it averages them). The imbalance cost (as computed by
 * DoubleArrayCost) is then used to drive the balancer to reduce differences between servers.
 */
@InterfaceAudience.Private
public class StoreFileTableSkewCostFunction extends CostFromRegionLoadFunction {

  private static final String STOREFILE_TABLE_SKEW_COST_KEY =
    "hbase.master.balancer.stochastic.storefileTableSkewCost";
  private static final float DEFAULT_STOREFILE_TABLE_SKEW_COST = 35;

  // One DoubleArrayCost instance per table.
  private DoubleArrayCost[] costsPerTable;

  public StoreFileTableSkewCostFunction(Configuration conf) {
    this.setMultiplier(
      conf.getFloat(STOREFILE_TABLE_SKEW_COST_KEY, DEFAULT_STOREFILE_TABLE_SKEW_COST));
  }

  @Override
  public void prepare(BalancerClusterState cluster) {
    // First, set the cluster state and allocate one DoubleArrayCost per table.
    this.cluster = cluster;
    costsPerTable = new DoubleArrayCost[cluster.numTables];
    for (int tableIdx = 0; tableIdx < cluster.numTables; tableIdx++) {
      costsPerTable[tableIdx] = new DoubleArrayCost();
      costsPerTable[tableIdx].prepare(cluster.numServers);
      final int tableIndex = tableIdx;
      costsPerTable[tableIdx].applyCostsChange(costs -> {
        // For each server, compute the aggregated storefile size for this table.
        for (int server = 0; server < cluster.numServers; server++) {
          double totalStorefileMB = 0;
          // Sum over all regions on this server that belong to the given table.
          for (int region : cluster.regionsPerServer[server]) {
            if (cluster.regionIndexToTableIndex[region] == tableIndex) {
              Collection<BalancerRegionLoad> loads = cluster.getRegionLoads()[region];
              double regionCost = 0;
              if (loads != null && !loads.isEmpty()) {
                // Average the storefile sizes if there are multiple measurements.
                for (BalancerRegionLoad rl : loads) {
                  regionCost += getCostFromRl(rl);
                }
                regionCost /= loads.size();
              }
              totalStorefileMB += regionCost;
            }
          }
          costs[server] = totalStorefileMB;
        }
      });
    }
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    // Determine the affected table.
    int tableIdx = cluster.regionIndexToTableIndex[region];
    costsPerTable[tableIdx].applyCostsChange(costs -> {
      // Recompute for the old server if applicable.
      updateStoreFilePerServerPerTableCosts(oldServer, tableIdx, costs);
      // Recompute for the new server.
      updateStoreFilePerServerPerTableCosts(newServer, tableIdx, costs);
    });
  }

  private void updateStoreFilePerServerPerTableCosts(int newServer, int tableIdx, double[] costs) {
    if (newServer >= 0) {
      double totalStorefileMB = 0;
      for (int r : cluster.regionsPerServer[newServer]) {
        if (cluster.regionIndexToTableIndex[r] == tableIdx) {
          Collection<BalancerRegionLoad> loads = cluster.getRegionLoads()[r];
          double regionCost = 0;
          if (loads != null && !loads.isEmpty()) {
            for (BalancerRegionLoad rl : loads) {
              regionCost += getCostFromRl(rl);
            }
            regionCost /= loads.size();
          }
          totalStorefileMB += regionCost;
        }
      }
      costs[newServer] = totalStorefileMB;
    }
  }

  @Override
  protected double cost() {
    double totalCost = 0;
    // Sum the imbalance cost over all tables.
    for (DoubleArrayCost dac : costsPerTable) {
      totalCost += dac.cost();
    }
    return totalCost;
  }

  @Override
  protected double getCostFromRl(BalancerRegionLoad rl) {
    // Use storefile size in MB as the metric.
    return rl.getStorefileSizeMB();
  }
}
