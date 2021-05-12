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

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A cost function for region replicas. We give a very high cost to hosting replicas of the same
 * region in the same host. We do not prevent the case though, since if numReplicas >
 * numRegionServers, we still want to keep the replica open.
 */
@InterfaceAudience.Private
class RegionReplicaHostCostFunction extends CostFunction {

  private static final String REGION_REPLICA_HOST_COST_KEY =
    "hbase.master.balancer.stochastic.regionReplicaHostCostKey";
  private static final float DEFAULT_REGION_REPLICA_HOST_COST_KEY = 100000;

  long maxCost = 0;
  long[] costsPerGroup; // group is either server, host or rack
  int[][] primariesOfRegionsPerGroup;

  public RegionReplicaHostCostFunction(Configuration conf) {
    this.setMultiplier(
      conf.getFloat(REGION_REPLICA_HOST_COST_KEY, DEFAULT_REGION_REPLICA_HOST_COST_KEY));
  }

  @Override
  void init(BalancerClusterState cluster) {
    super.init(cluster);
    // max cost is the case where every region replica is hosted together regardless of host
    maxCost = cluster.numHosts > 1 ? getMaxCost(cluster) : 0;
    costsPerGroup = new long[cluster.numHosts];
    primariesOfRegionsPerGroup = cluster.multiServersPerHost // either server based or host based
      ? cluster.primariesOfRegionsPerHost : cluster.primariesOfRegionsPerServer;
    for (int i = 0; i < primariesOfRegionsPerGroup.length; i++) {
      costsPerGroup[i] = costPerGroup(primariesOfRegionsPerGroup[i]);
    }
  }

  protected final long getMaxCost(BalancerClusterState cluster) {
    if (!cluster.hasRegionReplicas) {
      return 0; // short circuit
    }
    // max cost is the case where every region replica is hosted together regardless of host
    int[] primariesOfRegions = new int[cluster.numRegions];
    System.arraycopy(cluster.regionIndexToPrimaryIndex, 0, primariesOfRegions, 0,
      cluster.regions.length);

    Arrays.sort(primariesOfRegions);

    // compute numReplicas from the sorted array
    return costPerGroup(primariesOfRegions);
  }

  @Override
  boolean isNeeded() {
    return cluster.hasRegionReplicas;
  }

  @Override
  protected double cost() {
    if (maxCost <= 0) {
      return 0;
    }

    long totalCost = 0;
    for (int i = 0; i < costsPerGroup.length; i++) {
      totalCost += costsPerGroup[i];
    }
    return scale(0, maxCost, totalCost);
  }

  /**
   * For each primary region, it computes the total number of replicas in the array (numReplicas)
   * and returns a sum of numReplicas-1 squared. For example, if the server hosts regions a, b, c,
   * d, e, f where a and b are same replicas, and c,d,e are same replicas, it returns (2-1) * (2-1)
   * + (3-1) * (3-1) + (1-1) * (1-1).
   * @param primariesOfRegions a sorted array of primary regions ids for the regions hosted
   * @return a sum of numReplicas-1 squared for each primary region in the group.
   */
  protected final long costPerGroup(int[] primariesOfRegions) {
    long cost = 0;
    int currentPrimary = -1;
    int currentPrimaryIndex = -1;
    // primariesOfRegions is a sorted array of primary ids of regions. Replicas of regions
    // sharing the same primary will have consecutive numbers in the array.
    for (int j = 0; j <= primariesOfRegions.length; j++) {
      int primary = j < primariesOfRegions.length ? primariesOfRegions[j] : -1;
      if (primary != currentPrimary) { // we see a new primary
        int numReplicas = j - currentPrimaryIndex;
        // square the cost
        if (numReplicas > 1) { // means consecutive primaries, indicating co-location
          cost += (numReplicas - 1) * (numReplicas - 1);
        }
        currentPrimary = primary;
        currentPrimaryIndex = j;
      }
    }

    return cost;
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
        costsPerGroup[oldHost] = costPerGroup(cluster.primariesOfRegionsPerHost[oldHost]);
        costsPerGroup[newHost] = costPerGroup(cluster.primariesOfRegionsPerHost[newHost]);
      }
    } else {
      costsPerGroup[oldServer] = costPerGroup(cluster.primariesOfRegionsPerServer[oldServer]);
      costsPerGroup[newServer] = costPerGroup(cluster.primariesOfRegionsPerServer[newServer]);
    }
  }
}