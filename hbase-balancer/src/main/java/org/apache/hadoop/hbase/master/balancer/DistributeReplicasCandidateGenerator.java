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

import static java.util.Collections.shuffle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CandidateGenerator to distribute colocated replicas across different servers.
 */
class DistributeReplicasCandidateGenerator extends RegionPlanConditionalCandidateGenerator {

  private static final Logger LOG =
    LoggerFactory.getLogger(DistributeReplicasCandidateGenerator.class);

  /**
   * Generates a balancing action to distribute colocated replicas. Moves one replica of a colocated
   * region to a different server.
   * @param cluster    Current state of the cluster.
   * @param isWeighing Flag indicating if the generator is being used for weighing.
   * @return A BalanceAction to move a replica or NULL_ACTION if no action is needed.
   */
  @Override
  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    return generateCandidate(cluster, isWeighing, false);
  }

  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing,
    boolean isForced) {
    // Shuffle server indices to add some randomness to the moves
    List<Integer> shuffledServerIndices = new ArrayList<>(cluster.numServers);
    for (int i = 0; i < cluster.servers.length; i++) {
      shuffledServerIndices.add(i);
    }
    shuffle(shuffledServerIndices);

    // Iterate through each server to find colocated replicas
    boolean foundColocatedReplicas = false;
    for (int sourceIndex : shuffledServerIndices) {
      int[] serverRegions = cluster.regionsPerServer[sourceIndex];
      Set<DistributeReplicasConditional.ReplicaKey> replicaKeys =
        new HashSet<>(serverRegions.length);
      for (int regionIndex : serverRegions) {
        DistributeReplicasConditional.ReplicaKey replicaKey =
          new DistributeReplicasConditional.ReplicaKey(cluster.regions[regionIndex]);
        if (replicaKeys.contains(replicaKey)) {
          foundColocatedReplicas = true;
          if (isWeighing) {
            // if weighing, fast exit with an actionable move
            return getAction(sourceIndex, regionIndex, pickOtherRandomServer(cluster, sourceIndex),
              -1);
          } else {
            // if not weighing, pick a good move
            for (int destinationIndex : shuffledServerIndices) {
              if (destinationIndex == sourceIndex) {
                continue;
              }
              BalanceAction possibleAction =
                getAction(sourceIndex, regionIndex, destinationIndex, -1);
              if (isForced || willBeAccepted(cluster, possibleAction)) {
                return possibleAction;
              } else if (LOG.isTraceEnabled()) {
                // Find regions on the destination server that block movement because they share a
                // replica with the regionIndex
                Set<Integer> blockingRegionIndices = new HashSet<>();
                int[] destinationServerRegions = cluster.regionsPerServer[destinationIndex];
                for (int destinationRegionIndex : destinationServerRegions) {
                  DistributeReplicasConditional.ReplicaKey destinationReplicaKey =
                    new DistributeReplicasConditional.ReplicaKey(
                      cluster.regions[destinationRegionIndex]);
                  if (destinationReplicaKey.equals(replicaKey)) {
                    blockingRegionIndices.add(destinationRegionIndex);
                  }
                }
                if (blockingRegionIndices.isEmpty()) {
                  LOG.trace(
                    "Can't move region {} from server {} to server {} because OTHER conditionals reject it",
                    regionIndex, cluster.servers[sourceIndex].getServerName(),
                    cluster.servers[destinationIndex].getServerName());
                } else {
                  LOG.trace(
                    "Can't move region {} from server {} to server {} because this destination has regions that share this replica: {}",
                    regionIndex, cluster.servers[sourceIndex].getServerName(),
                    cluster.servers[destinationIndex].getServerName(), blockingRegionIndices);
                }
              }
            }
          }
        } else {
          replicaKeys.add(replicaKey);
        }
      }
    }

    // If no colocated replicas are found, return NULL_ACTION
    if (foundColocatedReplicas) {
      LOG.warn("Could not find a place to put a colocated replica! We will force a move.");
      return generateCandidate(cluster, isWeighing, true);
    } else {
      LOG.trace("No colocated replicas found. No balancing action required.");
    }
    return BalanceAction.NULL_ACTION;
  }
}
