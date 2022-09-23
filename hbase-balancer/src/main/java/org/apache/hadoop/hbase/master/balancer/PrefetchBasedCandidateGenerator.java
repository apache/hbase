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

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class PrefetchBasedCandidateGenerator extends CandidateGenerator{
  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    // iterate through regions until you find one that is not on ideal host
    // start from a random point to avoid always balance the regions in front
    if (cluster.numRegions > 0) {
      int startIndex = ThreadLocalRandom.current().nextInt(cluster.numRegions);
      for (int i = 0; i < cluster.numRegions; i++) {
        int region = (startIndex + i) % cluster.numRegions;
        int currentServer = cluster.regionIndexToServerIndex[region];
        if (
          currentServer != cluster.getOrComputeServerWithBestPrefetchRatio()[region]
        ) {
          Optional<BalanceAction> potential = tryMoveOrSwap(cluster,
            currentServer, region, cluster.getOrComputeServerWithBestPrefetchRatio()[region]);
          if (potential.isPresent()) {
            return potential.get();
          }
        }
      }
    }
    return BalanceAction.NULL_ACTION;
  }

  private Optional<BalanceAction> tryMoveOrSwap(BalancerClusterState cluster,
    int fromServer, int fromRegion, int toServer) {
    // Try move first. We know apriori fromRegion has the highest locality on toServer
    if (cluster.serverHasTooFewRegions(toServer)) {
      return Optional.of(getAction(fromServer, fromRegion, toServer, -1));
    }
    // Compare prefetch gain/loss from swapping fromRegion with regions on toServer
    float fromRegionPrefetchDelta = getWeightedPrefetch(cluster, fromRegion, toServer)
      - getWeightedPrefetch(cluster, fromRegion, fromServer);
    int toServertotalRegions = cluster.regionsPerServer[toServer].length;
    if (toServertotalRegions > 0) {
      int startIndex = ThreadLocalRandom.current().nextInt(toServertotalRegions);
      for (int i = 0; i < toServertotalRegions; i++) {
        int toRegionIndex = (startIndex + i) % toServertotalRegions;
        int toRegion = cluster.regionsPerServer[toServer][toRegionIndex];
        float toRegionPrefetchDelta = getWeightedPrefetch(cluster, toRegion, fromServer)
          - getWeightedPrefetch(cluster, toRegion, toServer);
        // If prefetch would remain neutral or improve, attempt the swap
        if (fromRegionPrefetchDelta + toRegionPrefetchDelta >= 0) {
          return Optional.of(getAction(fromServer, fromRegion, toServer, toRegion));
        }
      }
    }
    return Optional.empty();
  }

  private float getWeightedPrefetch(BalancerClusterState cluster, int region, int server) {
    return cluster.getOrComputeWeightedPrefetchRatio(region, server);
  }
}
