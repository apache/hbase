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

import static org.apache.hadoop.hbase.master.balancer.DistributeReplicasConditional.getReplicaKey;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CandidateGenerator to distribute colocated replicas across different servers.
 */
@InterfaceAudience.Private
final class DistributeReplicasCandidateGenerator extends RegionPlanConditionalCandidateGenerator {

  static DistributeReplicasCandidateGenerator INSTANCE = new DistributeReplicasCandidateGenerator();

  private static final Logger LOG =
    LoggerFactory.getLogger(DistributeReplicasCandidateGenerator.class);
  private static final int BATCH_SIZE = 100_000;

  private DistributeReplicasCandidateGenerator() {
  }

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
    // Iterate through shuffled servers to find colocated replicas
    boolean foundColocatedReplicas = false;
    List<MoveRegionAction> moveRegionActions = new ArrayList<>();
    for (int sourceIndex : cluster.getShuffledServerIndices()) {
      int[] serverRegions = cluster.regionsPerServer[sourceIndex];
      Set<DistributeReplicasConditional.ReplicaKey> replicaKeys =
        new HashSet<>(serverRegions.length);
      for (int regionIndex : serverRegions) {
        DistributeReplicasConditional.ReplicaKey replicaKey =
          getReplicaKey(cluster.regions[regionIndex]);
        if (replicaKeys.contains(replicaKey)) {
          foundColocatedReplicas = true;
          if (isWeighing) {
            // If weighing, fast exit with an actionable move
            return getAction(sourceIndex, regionIndex, pickOtherRandomServer(cluster, sourceIndex),
              -1);
          } else {
            // If not weighing, pick a good move
            for (int i = 0; i < cluster.numServers; i++) {
              // Randomize destination ordering so we aren't overloading one destination
              int destinationIndex = pickOtherRandomServer(cluster, sourceIndex);
              if (destinationIndex == sourceIndex) {
                continue;
              }
              MoveRegionAction possibleAction =
                new MoveRegionAction(regionIndex, sourceIndex, destinationIndex);
              if (isForced) {
                return possibleAction;
              } else if (willBeAccepted(cluster, possibleAction)) {
                cluster.doAction(possibleAction); // Update cluster state to reflect move
                moveRegionActions.add(possibleAction);
                break;
              }
            }
          }
        } else {
          replicaKeys.add(replicaKey);
        }
        if (moveRegionActions.size() >= BATCH_SIZE) {
          break;
        }
      }
      if (moveRegionActions.size() >= BATCH_SIZE) {
        break;
      }
    }

    if (!moveRegionActions.isEmpty()) {
      MoveBatchAction batchAction = new MoveBatchAction(moveRegionActions);
      undoBatchAction(cluster, batchAction); // Reset cluster state to before batch
      return batchAction;
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
