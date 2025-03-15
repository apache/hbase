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
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple candidate generator that attempts to move regions from the most-loaded servers to the
 * least-loaded servers.
 */
@InterfaceAudience.Private
final class SlopFixingCandidateGenerator extends RegionPlanConditionalCandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SlopFixingCandidateGenerator.class);

  private final float slop;

  SlopFixingCandidateGenerator(BalancerConditionals balancerConditionals) {
    super(balancerConditionals);
    this.slop = balancerConditionals.getConf().getFloat(BaseLoadBalancer.REGIONS_SLOP_KEY,
      BaseLoadBalancer.REGIONS_SLOP_DEFAULT);
  }

  @Override
  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    boolean isTableIsolationEnabled = getBalancerConditionals().isTableIsolationEnabled();
    ClusterLoadState cs = new ClusterLoadState(cluster.clusterState);
    float average = cs.getLoadAverage();
    int ceiling = (int) Math.ceil(average * (1 + slop));
    Set<Integer> sloppyServerIndices = new HashSet<>();
    for (int i = 0; i < cluster.numServers; i++) {
      int regionCount = cluster.regionsPerServer[i].length;
      if (regionCount > ceiling) {
        sloppyServerIndices.add(i);
      }
    }

    if (sloppyServerIndices.isEmpty()) {
      LOG.trace("No action to take because no sloppy servers exist.");
      return BalanceAction.NULL_ACTION;
    }

    List<MoveRegionAction> moves = new ArrayList<>();
    Set<ServerAndLoad> fixedServers = new HashSet<>();
    for (int sourceServer : sloppyServerIndices) {
      if (
        isTableIsolationEnabled
          && getBalancerConditionals().isServerHostingIsolatedTables(cluster, sourceServer)
      ) {
        // Don't fix sloppiness of servers hosting isolated tables
        continue;
      }
      for (int regionIdx : cluster.regionsPerServer[sourceServer]) {
        boolean regionFoundMove = false;
        for (ServerAndLoad serverAndLoad : cs.getServersByLoad().keySet()) {
          ServerName destinationServer = serverAndLoad.getServerName();
          int destinationServerIdx = cluster.serversToIndex.get(destinationServer.getAddress());
          int regionsOnDestination = cluster.regionsPerServer[destinationServerIdx].length;
          if (regionsOnDestination < average) {
            MoveRegionAction move =
              new MoveRegionAction(regionIdx, sourceServer, destinationServerIdx);
            if (willBeAccepted(cluster, move)) {
              if (isWeighing) {
                // Fast exit for weighing candidate
                return move;
              }
              moves.add(move);
              cluster.doAction(move);
              regionFoundMove = true;
              break;
            }
          } else {
            fixedServers.add(serverAndLoad);
          }
        }
        fixedServers.forEach(s -> cs.getServersByLoad().remove(s));
        fixedServers.clear();
        if (!regionFoundMove && LOG.isTraceEnabled()) {
          LOG.trace("Could not find a destination for region {} from server {}.", regionIdx,
            sourceServer);
        }
        if (cluster.regionsPerServer[sourceServer].length <= ceiling) {
          break;
        }
      }
    }

    return batchMovesAndResetClusterState(cluster, moves);
  }
}
