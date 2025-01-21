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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Compute the cost of a potential cluster state from skew in number of regions on a cluster.
 */
@InterfaceAudience.Private
class RegionCountSkewCostFunction extends CostFunction {
  static final String REGION_COUNT_SKEW_COST_KEY =
    "hbase.master.balancer.stochastic.regionCountCost";
  static final float DEFAULT_REGION_COUNT_SKEW_COST = 500;

  private final DoubleArrayCost cost = new DoubleArrayCost();

  RegionCountSkewCostFunction(Configuration conf) {
    // Load multiplier should be the greatest as it is the most general way to balance data.
    this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    cost.prepare(cluster.numServers);
    cost.applyCostsChange(costs -> {
      for (int i = 0; i < cluster.numServers; i++) {
        costs[i] = cluster.regionsPerServer[i].length;
      }
    });
  }

  @Override
  protected double cost() {
    return cost.cost();
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    cost.applyCostsChange(costs -> {
      costs[oldServer] = cluster.regionsPerServer[oldServer].length;
      costs[newServer] = cluster.regionsPerServer[newServer].length;
    });
  }

  @Override
  public final void updateWeight(Map<Class<? extends CandidateGenerator>, Double> weights) {
    weights.merge(LoadCandidateGenerator.class, cost(), Double::sum);
  }
}
