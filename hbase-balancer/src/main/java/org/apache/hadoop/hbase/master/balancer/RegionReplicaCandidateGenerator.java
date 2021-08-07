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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
   * @param primariesOfRegionsPerGroup either Cluster.primariesOfRegionsPerServer,
   *          primariesOfRegionsPerHost or primariesOfRegionsPerRack
   * @param regionsPerGroup either Cluster.regionsPerServer, regionsPerHost or regionsPerRack
   * @param regionIndexToPrimaryIndex Cluster.regionsIndexToPrimaryIndex
   * @return a regionIndex for the selected primary or -1 if there is no co-locating
   */
  int selectCoHostedRegionPerGroup(HashMap<Integer, HashSet<Integer>> primariesOfRegionsPerGroup,
    Collection<Integer> regionsPerGroup, int[] regionIndexToPrimaryIndex) {

    double currentLargestRandom = -1;
    Map.Entry<Integer, HashSet<Integer>> selectedPrimaryIndexEntry = null;

    // primariesOfRegionsPerGroup is a hashmap of count of primary index on a server. a count > 1
    // means that replicas are co-hosted
    for(Map.Entry<Integer, HashSet<Integer>> pair : primariesOfRegionsPerGroup.entrySet()) {
      if (pair.getValue().size() > 1) { // indicating co-location
        // decide to select this primary region id or not
        double currentRandom = ThreadLocalRandom.current().nextDouble();
        // we don't know how many region replicas are co-hosted, we will randomly select one
        // using reservoir sampling (http://gregable.com/2007/10/reservoir-sampling.html)
        if (currentRandom > currentLargestRandom) {
          selectedPrimaryIndexEntry = pair;
          currentLargestRandom = currentRandom;
        }
      }
    }

    if (selectedPrimaryIndexEntry == null) {
      return -1;
    }

    // we have found the primary id and the set of regions for the region to move.
    // now to return one of the secondary
    for (Integer regionIndex : selectedPrimaryIndexEntry.getValue()) {
      if (regionIndex != selectedPrimaryIndexEntry.getKey()) {
        // always move the secondary, not the primary
          return regionIndex;
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

    int regionIndex = selectCoHostedRegionPerGroup(cluster.primariesOfRegionsPerServer.get(serverIndex),
      cluster.regionsPerServer.get(serverIndex), cluster.regionIndexToPrimaryIndex);

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
