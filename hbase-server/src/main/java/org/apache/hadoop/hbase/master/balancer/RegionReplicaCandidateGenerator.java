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

import java.util.concurrent.ThreadLocalRandom;
import org.agrona.collections.Int2IntCounterMap;
import org.agrona.collections.IntArrayList;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Generates candidates which moves the replicas out of the region server for co-hosted region
 * replicas
 */
@InterfaceAudience.Private
class RegionReplicaCandidateGenerator extends CandidateGenerator {

  protected final RandomCandidateGenerator randomGenerator = new RandomCandidateGenerator();

  /**
   * Randomly select one regionIndex out of all region replicas co-hosted in the same group (a group
   * is a server, host or rack)
   * @param colocatedReplicaCountsPerGroup either Cluster.colocatedReplicaCountsPerServer,
   *                                       colocatedReplicaCountsPerHost or
   *                                       colocatedReplicaCountsPerRack
   * @param regionsPerGroup                either Cluster.regionsPerServer, regionsPerHost or
   *                                       regionsPerRack
   * @param regionIndexToPrimaryIndex      Cluster.regionsIndexToPrimaryIndex
   * @return a regionIndex for the selected primary or -1 if there is no co-locating
   */
  int selectCoHostedRegionPerGroup(Int2IntCounterMap colocatedReplicaCountsPerGroup,
    int[] regionsPerGroup, int[] regionIndexToPrimaryIndex) {
    final IntArrayList colocated = new IntArrayList(colocatedReplicaCountsPerGroup.size(), -1);
    colocatedReplicaCountsPerGroup.forEach((primary, count) -> {
      if (count > 1) { // means consecutive primaries, indicating co-location
        colocated.add(primary);
      }
    });

    if (!colocated.isEmpty()) {
      int rand = ThreadLocalRandom.current().nextInt(colocated.size());
      int selectedPrimaryIndex = colocated.get(rand);
      // we have found the primary id for the region to move. Now find the actual regionIndex
      // with the given primary, prefer to move the secondary region.
      for (int regionIndex : regionsPerGroup) {
        if (selectedPrimaryIndex == regionIndexToPrimaryIndex[regionIndex]) {
          // always move the secondary, not the primary
          if (selectedPrimaryIndex != regionIndex) {
            return regionIndex;
          }
        }
      }
    }
    return -1;
  }

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    int serverIndex = pickRandomServer(cluster);
    if (cluster.numServers <= 1 || serverIndex == -1) {
      return BalanceAction.NULL_ACTION;
    }

    int regionIndex =
      selectCoHostedRegionPerGroup(cluster.colocatedReplicaCountsPerServer[serverIndex],
        cluster.regionsPerServer[serverIndex], cluster.regionIndexToPrimaryIndex);

    // if there are no pairs of region replicas co-hosted, default to random generator
    if (regionIndex == -1) {
      // default to randompicker
      return randomGenerator.generate(cluster);
    }

    int toServerIndex = pickOtherRandomServer(cluster, serverIndex);
    int toRegionIndex = pickRandomRegion(cluster, toServerIndex, 0.9f);
    return getAction(serverIndex, regionIndex, toServerIndex, toRegionIndex);
  }
}
