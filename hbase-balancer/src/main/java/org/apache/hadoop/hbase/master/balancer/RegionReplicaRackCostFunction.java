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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A cost function for region replicas for the rack distribution. We give a relatively high cost to
 * hosting replicas of the same region in the same rack. We do not prevent the case though.
 */
@InterfaceAudience.Private
class RegionReplicaRackCostFunction extends RegionReplicaGroupingCostFunction {

  private static final String REGION_REPLICA_RACK_COST_KEY =
    "hbase.master.balancer.stochastic.regionReplicaRackCostKey";
  private static final float DEFAULT_REGION_REPLICA_RACK_COST_KEY = 10000;

  public RegionReplicaRackCostFunction(Configuration conf) {
    this.setMultiplier(
      conf.getFloat(REGION_REPLICA_RACK_COST_KEY, DEFAULT_REGION_REPLICA_RACK_COST_KEY));
  }

  @Override
  protected void loadCosts() {
    if (cluster.numRacks <= 1) {
      maxCost = 0;
      return; // disabled for 1 rack
    }
    // max cost is the case where every region replica is hosted together regardless of rack
    maxCost = getMaxCost(cluster);
    costsPerGroup = new long[cluster.numRacks];
    for (int i = 0; i < cluster.colocatedReplicaCountsPerRack.length; i++) {
      costsPerGroup[i] = costPerGroup(cluster.colocatedReplicaCountsPerRack[i]);
    }
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    if (maxCost <= 0) {
      return; // no need to compute
    }
    int oldRack = cluster.serverIndexToRackIndex[oldServer];
    int newRack = cluster.serverIndexToRackIndex[newServer];
    if (newRack != oldRack) {
      costsPerGroup[oldRack] = costPerGroup(cluster.colocatedReplicaCountsPerRack[oldRack]);
      costsPerGroup[newRack] = costPerGroup(cluster.colocatedReplicaCountsPerRack[newRack]);
    }
  }
}
