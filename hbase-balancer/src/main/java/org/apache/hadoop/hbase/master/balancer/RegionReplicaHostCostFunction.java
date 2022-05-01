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

import org.agrona.collections.Int2IntCounterMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A cost function for region replicas. We give a very high cost to hosting replicas of the same
 * region in the same host. We do not prevent the case though, since if numReplicas >
 * numRegionServers, we still want to keep the replica open.
 */
@InterfaceAudience.Private
class RegionReplicaHostCostFunction extends RegionReplicaGroupingCostFunction {

  private static final String REGION_REPLICA_HOST_COST_KEY =
    "hbase.master.balancer.stochastic.regionReplicaHostCostKey";
  private static final float DEFAULT_REGION_REPLICA_HOST_COST_KEY = 100000;

  private Int2IntCounterMap[] colocatedReplicaCountsPerGroup;

  public RegionReplicaHostCostFunction(Configuration conf) {
    this.setMultiplier(
      conf.getFloat(REGION_REPLICA_HOST_COST_KEY, DEFAULT_REGION_REPLICA_HOST_COST_KEY));
  }

  @Override
  protected void loadCosts() {
    // max cost is the case where every region replica is hosted together regardless of host
    maxCost = cluster.numHosts > 1 ? getMaxCost(cluster) : 0;
    costsPerGroup = new long[cluster.numHosts];
    // either server based or host based
    colocatedReplicaCountsPerGroup = cluster.multiServersPerHost
      ? cluster.colocatedReplicaCountsPerHost
      : cluster.colocatedReplicaCountsPerServer;
    for (int i = 0; i < colocatedReplicaCountsPerGroup.length; i++) {
      costsPerGroup[i] = costPerGroup(colocatedReplicaCountsPerGroup[i]);
    }
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    if (maxCost <= 0) {
      return; // no need to compute
    }
    if (cluster.multiServersPerHost) {
      int oldHost = cluster.serverIndexToHostIndex[oldServer];
      int newHost = cluster.serverIndexToHostIndex[newServer];
      if (newHost != oldHost) {
        costsPerGroup[oldHost] = costPerGroup(cluster.colocatedReplicaCountsPerHost[oldHost]);
        costsPerGroup[newHost] = costPerGroup(cluster.colocatedReplicaCountsPerHost[newHost]);
      }
    } else {
      costsPerGroup[oldServer] = costPerGroup(cluster.colocatedReplicaCountsPerServer[oldServer]);
      costsPerGroup[newServer] = costPerGroup(cluster.colocatedReplicaCountsPerServer[newServer]);
    }
  }
}
