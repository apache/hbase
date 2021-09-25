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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Generates candidates which moves the replicas out of the rack for co-hosted region replicas in
 * the same rack
 */
@InterfaceAudience.Private
class RegionReplicaRackCandidateGenerator extends RegionReplicaCandidateGenerator {

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    int rackIndex = pickRandomRack(cluster);
    if (cluster.numRacks <= 1 || rackIndex == -1) {
      return super.generate(cluster);
    }

    int regionIndex = selectCoHostedRegionPerGroup(cluster.colocatedReplicaCountsPerRack[rackIndex],
      cluster.regionsPerRack[rackIndex], cluster.regionIndexToPrimaryIndex);

    // if there are no pairs of region replicas co-hosted, default to random generator
    if (regionIndex == -1) {
      // default to randompicker
      return randomGenerator.generate(cluster);
    }

    int serverIndex = cluster.regionIndexToServerIndex[regionIndex];
    int toRackIndex = pickOtherRandomRack(cluster, rackIndex);

    int rand = ThreadLocalRandom.current().nextInt(cluster.serversPerRack[toRackIndex].length);
    int toServerIndex = cluster.serversPerRack[toRackIndex][rand];
    int toRegionIndex = pickRandomRegion(cluster, toServerIndex, 0.9f);
    return getAction(serverIndex, regionIndex, toServerIndex, toRegionIndex);
  }
}
