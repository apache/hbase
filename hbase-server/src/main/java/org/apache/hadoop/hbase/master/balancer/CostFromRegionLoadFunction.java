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
import java.util.Deque;
import java.util.Map;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class the allows writing costs functions from rolling average of some number from
 * RegionLoad.
 */
@InterfaceAudience.Private
abstract class CostFromRegionLoadFunction extends CostFunction {

  private ClusterMetrics clusterStatus;
  private Map<String, Deque<BalancerRegionLoad>> loads;
  private double[] stats;

  void setClusterMetrics(ClusterMetrics status) {
    this.clusterStatus = status;
  }

  void setLoads(Map<String, Deque<BalancerRegionLoad>> l) {
    this.loads = l;
  }

  @Override
  protected final double cost() {
    if (clusterStatus == null || loads == null) {
      return 0;
    }

    if (stats == null || stats.length != cluster.numServers) {
      stats = new double[cluster.numServers];
    }

    for (int i = 0; i < stats.length; i++) {
      // Cost this server has from RegionLoad
      long cost = 0;

      // for every region on this server get the rl
      for (int regionIndex : cluster.regionsPerServer[i]) {
        Collection<BalancerRegionLoad> regionLoadList = cluster.regionLoads[regionIndex];

        // Now if we found a region load get the type of cost that was requested.
        if (regionLoadList != null) {
          cost = (long) (cost + getRegionLoadCost(regionLoadList));
        }
      }

      // Add the total cost to the stats.
      stats[i] = cost;
    }

    // Now return the scaled cost from data held in the stats object.
    return costFromArray(stats);
  }

  protected double getRegionLoadCost(Collection<BalancerRegionLoad> regionLoadList) {
    double cost = 0;
    for (BalancerRegionLoad rl : regionLoadList) {
      cost += getCostFromRl(rl);
    }
    return cost / regionLoadList.size();
  }

  protected abstract double getCostFromRl(BalancerRegionLoad rl);
}