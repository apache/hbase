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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class the allows writing costs functions from rolling average of some number from
 * RegionLoad.
 */
@InterfaceAudience.Private
abstract class CostFromRegionLoadFunction extends CostFunction {

  private final DoubleArrayCost cost = new DoubleArrayCost();

  private double computeCostForRegionServer(int regionServerIndex) {
    // Cost this server has from RegionLoad
    double cost = 0;

    // for every region on this server get the rl
    for (int regionIndex : cluster.regionsPerServer[regionServerIndex]) {
      Collection<BalancerRegionLoad> regionLoadList = cluster.regionLoads[regionIndex];

      // Now if we found a region load get the type of cost that was requested.
      if (regionLoadList != null) {
        cost += getRegionLoadCost(regionLoadList);
      }
    }
    return cost;
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    cost.prepare(cluster.numServers);
    cost.applyCostsChange(costs -> {
      for (int i = 0; i < costs.length; i++) {
        costs[i] = computeCostForRegionServer(i);
      }
    });
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    // recompute the stat for the given two region servers
    cost.applyCostsChange(costs -> {
      costs[oldServer] = computeCostForRegionServer(oldServer);
      costs[newServer] = computeCostForRegionServer(newServer);
    });
  }

  @Override
  protected double cost() {
    return cost.cost();
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
