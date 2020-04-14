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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Generates candidates which moves the replicas out of the region server for
 * co-hosted region replicas
 */
@InterfaceAudience.Private
class RegionReplicaCandidateGenerator extends CandidateGenerator {

  StochasticLoadBalancer.RandomCandidateGenerator randomGenerator =
    new StochasticLoadBalancer.RandomCandidateGenerator();

  /**
   * Randomly select one regionIndex out of all region replicas co-hosted in the same group
   * (a group is a server, host or rack)
   *
   * @param primariesOfRegionsPerGroup either Cluster.primariesOfRegionsPerServer,
   *   primariesOfRegionsPerHost or primariesOfRegionsPerRack
   * @param regionsPerGroup either Cluster.regionsPerServer, regionsPerHost or regionsPerRack
   * @param regionIndexToPrimaryIndex Cluster.regionsIndexToPrimaryIndex
   * @return a regionIndex for the selected primary or -1 if there is no co-locating
   */
  int selectCoHostedRegionPerGroup(int[] primariesOfRegionsPerGroup, int[] regionsPerGroup,
      int[] regionIndexToPrimaryIndex) {
    int currentPrimary = -1;
    int currentPrimaryIndex = -1;
    int selectedPrimaryIndex = -1;
    double currentLargestRandom = -1;
    // primariesOfRegionsPerGroup is a sorted array. Since it contains the primary region
    // ids for the regions hosted in server, a consecutive repetition means that replicas
    // are co-hosted
    for (int j = 0; j <= primariesOfRegionsPerGroup.length; j++) {
      int primary = j < primariesOfRegionsPerGroup.length
        ? primariesOfRegionsPerGroup[j] : -1;
      if (primary != currentPrimary) { // check for whether we see a new primary
        int numReplicas = j - currentPrimaryIndex;
        if (numReplicas > 1) { // means consecutive primaries, indicating co-location
          // decide to select this primary region id or not
          double currentRandom = StochasticLoadBalancer.RANDOM.nextDouble();
          // we don't know how many region replicas are co-hosted, we will randomly select one
          // using reservoir sampling (http://gregable.com/2007/10/reservoir-sampling.html)
          if (currentRandom > currentLargestRandom) {
            selectedPrimaryIndex = currentPrimary;
            currentLargestRandom = currentRandom;
          }
        }
        currentPrimary = primary;
        currentPrimaryIndex = j;
      }
    }

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
    return -1;
  }

  @Override
  BaseLoadBalancer.Cluster.Action generate(BaseLoadBalancer.Cluster cluster) {
    int serverIndex = pickRandomServer(cluster);
    if (cluster.numServers <= 1 || serverIndex == -1) {
      return BaseLoadBalancer.Cluster.NullAction;
    }

    int regionIndex = selectCoHostedRegionPerGroup(
      cluster.primariesOfRegionsPerServer[serverIndex],
      cluster.regionsPerServer[serverIndex],
      cluster.regionIndexToPrimaryIndex);

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
