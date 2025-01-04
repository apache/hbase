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
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntCounterMap;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A cost function for region replicas. We give a high cost for hosting replicas of the same region
 * in the same server, host or rack. We do not prevent the case though, since if numReplicas >
 * numRegionServers, we still want to keep the replica open.
 */
@InterfaceAudience.Private
abstract class RegionReplicaGroupingCostFunction extends CostFunction {
  protected long maxCost = 0;
  protected long[] costsPerGroup; // group is either server, host or rack

  @Override
  final void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    if (!isNeeded()) {
      return;
    }
    loadCosts();
  }

  protected abstract void loadCosts();

  protected final long getMaxCost(BalancerClusterState cluster) {
    // max cost is the case where every region replica is hosted together regardless of host
    Int2IntCounterMap colocatedReplicaCounts =
      new Int2IntCounterMap(cluster.numRegions, Hashing.DEFAULT_LOAD_FACTOR, 0);
    for (int i = 0; i < cluster.regionIndexToPrimaryIndex.length; i++) {
      colocatedReplicaCounts.getAndIncrement(cluster.regionIndexToPrimaryIndex[i]);
    }
    // compute numReplicas from the sorted array
    return costPerGroup(colocatedReplicaCounts);
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

  @Override
  public final void updateWeight(Map<Class<? extends CandidateGenerator>, Double> weights) {
    weights.merge(RegionReplicaRackCandidateGenerator.class, cost(), Double::sum);
  }

  /**
   * For each primary region, it computes the total number of replicas in the array (numReplicas)
   * and returns a sum of numReplicas-1 squared. For example, if the server hosts regions a, b, c,
   * d, e, f where a and b are same replicas, and c,d,e are same replicas, it returns (2-1) * (2-1)
   * + (3-1) * (3-1) + (1-1) * (1-1).
   * @param colocatedReplicaCounts a sorted array of primary regions ids for the regions hosted
   * @return a sum of numReplicas-1 squared for each primary region in the group.
   */
  protected final long costPerGroup(Int2IntCounterMap colocatedReplicaCounts) {
    final AtomicLong cost = new AtomicLong(0);
    // colocatedReplicaCounts is a sorted array of primary ids of regions. Replicas of regions
    // sharing the same primary will have consecutive numbers in the array.
    colocatedReplicaCounts.forEach((primary, count) -> {
      if (count > 1) { // means consecutive primaries, indicating co-location
        cost.getAndAdd((count - 1) * (count - 1));
      }
    });
    return cost.longValue();
  }
}
