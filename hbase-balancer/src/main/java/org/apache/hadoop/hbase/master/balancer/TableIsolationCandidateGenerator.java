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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class TableIsolationCandidateGenerator
  extends RegionPlanConditionalCandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(TableIsolationCandidateGenerator.class);

  TableIsolationCandidateGenerator(BalancerConditionals balancerConditionals) {
    super(balancerConditionals);
  }

  abstract boolean shouldBeIsolated(RegionInfo regionInfo);

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    return generateCandidate(cluster, false);
  }

  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    if (!getBalancerConditionals().isTableIsolationEnabled()) {
      return BalanceAction.NULL_ACTION;
    }

    List<MoveRegionAction> moves = new ArrayList<>();
    List<Integer> serverIndicesHoldingIsolatedRegions = new ArrayList<>();
    int isolatedTableMaxReplicaCount = 1;
    for (int serverIdx : cluster.getShuffledServerIndices()) {
      if (EnvironmentEdgeManager.currentTime() > cluster.getStopRequestedAt()) {
        break;
      }
      boolean hasRegionsToIsolate = false;
      Set<Integer> regionsToMove = new HashSet<>();

      // Move non-target regions away from target regions,
      // and track replica counts so we know how many isolated hosts we need
      for (int regionIdx : cluster.regionsPerServer[serverIdx]) {
        RegionInfo regionInfo = cluster.regions[regionIdx];
        if (shouldBeIsolated(regionInfo)) {
          hasRegionsToIsolate = true;
          int replicaCount = regionInfo.getReplicaId() + 1;
          if (replicaCount > isolatedTableMaxReplicaCount) {
            isolatedTableMaxReplicaCount = replicaCount;
          }
        } else {
          regionsToMove.add(regionIdx);
        }
      }

      if (hasRegionsToIsolate) {
        serverIndicesHoldingIsolatedRegions.add(serverIdx);
      }

      // Generate non-system regions to move, if applicable
      if (hasRegionsToIsolate && !regionsToMove.isEmpty()) {
        for (int regionToMove : regionsToMove) {
          for (int i = 0; i < cluster.numServers; i++) {
            int targetServer = pickOtherRandomServer(cluster, serverIdx);
            MoveRegionAction possibleMove =
              new MoveRegionAction(regionToMove, serverIdx, targetServer);
            if (!getBalancerConditionals().isViolating(cluster, possibleMove)) {
              if (isWeighing) {
                return possibleMove;
              }
              cluster.doAction(possibleMove); // Update cluster state to reflect move
              moves.add(possibleMove);
              break;
            }
          }
        }
      }
    }

    // Try to consolidate regions on only n servers, where n is the number of replicas
    if (serverIndicesHoldingIsolatedRegions.size() > isolatedTableMaxReplicaCount) {
      // One target per replica
      List<Integer> targetServerIndices = new ArrayList<>();
      for (int i = 0; i < isolatedTableMaxReplicaCount; i++) {
        targetServerIndices.add(serverIndicesHoldingIsolatedRegions.get(i));
      }
      // Move all isolated regions from non-targets to targets
      for (int i = isolatedTableMaxReplicaCount; i
          < serverIndicesHoldingIsolatedRegions.size(); i++) {
        int fromServer = serverIndicesHoldingIsolatedRegions.get(i);
        for (int regionIdx : cluster.regionsPerServer[fromServer]) {
          RegionInfo regionInfo = cluster.regions[regionIdx];
          if (shouldBeIsolated(regionInfo)) {
            int targetServer = targetServerIndices.get(i % isolatedTableMaxReplicaCount);
            MoveRegionAction possibleMove =
              new MoveRegionAction(regionIdx, fromServer, targetServer);
            if (!getBalancerConditionals().isViolating(cluster, possibleMove)) {
              if (isWeighing) {
                return possibleMove;
              }
              cluster.doAction(possibleMove); // Update cluster state to reflect move
              moves.add(possibleMove);
            }
          }
        }
      }
    }
    return batchMovesAndResetClusterState(cluster, moves);
  }
}
