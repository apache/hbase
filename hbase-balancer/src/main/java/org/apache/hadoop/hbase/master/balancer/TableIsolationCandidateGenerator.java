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
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class TableIsolationCandidateGenerator
  extends RegionPlanConditionalCandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(TableIsolationCandidateGenerator.class);

  abstract boolean shouldBeIsolated(RegionInfo regionInfo);

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    return generateCandidate(cluster, false);
  }

  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    if (
      !BalancerConditionals.INSTANCE.isSystemTableIsolationEnabled()
        && !BalancerConditionals.INSTANCE.isMetaTableIsolationEnabled()
    ) {
      return BalanceAction.NULL_ACTION;
    }

    List<MoveRegionAction> moves = new ArrayList<>();
    for (int serverIdx : cluster.getShuffledServerIndices()) {
      boolean hasRegionsToIsolate = false;
      Set<Integer> regionsToMove = new HashSet<>();

      // Check all regions on the server
      for (int regionIdx : cluster.regionsPerServer[serverIdx]) {
        RegionInfo regionInfo = cluster.regions[regionIdx];
        if (shouldBeIsolated(regionInfo)) {
          hasRegionsToIsolate = true;
        } else {
          regionsToMove.add(regionIdx);
        }
      }

      // Generate non-system regions to move, if applicable
      if (hasRegionsToIsolate && !regionsToMove.isEmpty()) {
        for (int regionToMove : regionsToMove) {
          for (int i = 0; i < cluster.numServers; i++) {
            int targetServer = pickOtherRandomServer(cluster, serverIdx);
            MoveRegionAction possibleMove =
              new MoveRegionAction(regionToMove, serverIdx, targetServer);
            if (!BalancerConditionals.INSTANCE.isViolating(cluster, possibleMove)) {
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
    if (moves.isEmpty()) {
      return BalanceAction.NULL_ACTION;
    } else {
      return batchMovesAndResetClusterState(cluster, moves);
    }
  }
}
