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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This will generate candidates that colocate a table on the number of RegionServers equal to its
 * number of replicas. For example, this is useful when isolating system tables.
 */
@InterfaceAudience.Private
public final class TableColocationCandidateGenerator
  extends RegionPlanConditionalCandidateGenerator {

  private static final Logger LOG =
    LoggerFactory.getLogger(TableColocationCandidateGenerator.class);

  private final TableName tableName;

  TableColocationCandidateGenerator(TableName tableName) {
    this.tableName = tableName;
  }

  @Override
  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    int maxReplicaId = 0;
    Map<Integer, Integer> regionIdxToServerIdx = new HashMap<>();
    for (RegionInfo region : cluster.regions) {
      if (region.getTable().equals(tableName)) {
        int regionIdx = cluster.regionsToIndex.get(region);
        regionIdxToServerIdx.put(regionIdx, cluster.regionIndexToServerIndex[regionIdx]);
        if (region.getReplicaId() > maxReplicaId) {
          maxReplicaId = region.getReplicaId();
        }
      }
    }
    if (regionIdxToServerIdx.isEmpty()) {
      LOG.trace("No regions found for table {}", tableName.getNameAsString());
      return BalanceAction.NULL_ACTION;
    }
    int numReplicas = maxReplicaId + 1;
    Set<Integer> serversHostingTable = new HashSet<>(regionIdxToServerIdx.values());
    if (serversHostingTable.size() <= numReplicas) {
      return BalanceAction.NULL_ACTION;
    }
    List<Integer> desiredHosts = serversHostingTable.stream().sorted().limit(numReplicas).toList();
    Set<Integer> serversToEvacuate = new HashSet<>(serversHostingTable);
    LOG.trace("Moving {} regions off of {} and onto {}", tableName.getNameAsString(),
      serversToEvacuate, desiredHosts);
    serversToEvacuate.removeAll(desiredHosts);
    List<MoveRegionAction> moves = new ArrayList<>();
    int i = 0;
    for (Map.Entry<Integer, Integer> regionAndServer : regionIdxToServerIdx.entrySet()) {
      if (serversToEvacuate.contains(regionAndServer.getValue())) {
        boolean accepted = false;
        for (int j = 0; j < desiredHosts.size(); j++) {
          int desiredHostKey = (i + j) % desiredHosts.size();
          MoveRegionAction mra = new MoveRegionAction(regionAndServer.getKey(),
            regionAndServer.getValue(), desiredHosts.get(desiredHostKey));
          if (isWeighing) {
            return mra;
          } else if (willBeAccepted(cluster, mra)) {
            moves.add(mra);
            cluster.doAction(mra);
            accepted = true;
            break;
          }
        }
        if (!accepted) {
          LOG.warn(
            "Could not find placement for region {} on table {} from server {} to desired hosts {}",
            regionAndServer.getKey(), tableName.getNameAsString(), regionAndServer.getValue(),
            desiredHosts);
        }
        i++;
      }
    }
    MoveBatchAction mba = new MoveBatchAction(moves);
    undoBatchAction(cluster, mba);
    return mba;
  }
}
