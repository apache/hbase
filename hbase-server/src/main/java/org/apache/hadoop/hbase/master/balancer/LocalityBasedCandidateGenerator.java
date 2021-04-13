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
class LocalityBasedCandidateGenerator extends CandidateGenerator {

  @Override
  BaseLoadBalancer.Cluster.Action generate(BaseLoadBalancer.Cluster cluster) {
    // iterate through regions until you find one that is not on ideal host
    // start from a random point to avoid always balance the regions in front
    if (cluster.numRegions > 0) {
      int startIndex = ThreadLocalRandom.current().nextInt(cluster.numRegions);
      for (int i = 0; i < cluster.numRegions; i++) {
        int region = (startIndex + i) % cluster.numRegions;
        int currentServer = cluster.regionIndexToServerIndex[region];
        if (currentServer != cluster.getOrComputeRegionsToMostLocalEntities(
          BaseLoadBalancer.Cluster.LocalityType.SERVER)[region]) {
          Optional<BaseLoadBalancer.Cluster.Action> potential = tryMoveOrSwap(cluster,
            currentServer, region, cluster.getOrComputeRegionsToMostLocalEntities(
              BaseLoadBalancer.Cluster.LocalityType.SERVER)[region]);
          if (potential.isPresent()) {
            return potential.get();
          }
        }
      }
    }
    return BaseLoadBalancer.Cluster.NullAction;
  }

  private Optional<BaseLoadBalancer.Cluster.Action> tryMoveOrSwap(BaseLoadBalancer.Cluster cluster,
      int fromServer, int fromRegion, int toServer) {
    // Try move first. We know apriori fromRegion has the highest locality on toServer
    if (cluster.serverHasTooFewRegions(toServer)) {
      return Optional.of(getAction(fromServer, fromRegion, toServer, -1));
    }
    // Compare locality gain/loss from swapping fromRegion with regions on toServer
    double fromRegionLocalityDelta = getWeightedLocality(cluster, fromRegion, toServer)
      - getWeightedLocality(cluster, fromRegion, fromServer);
    int toServertotalRegions =  cluster.regionsPerServer[toServer].length;
    if (toServertotalRegions > 0) {
      int startIndex = ThreadLocalRandom.current().nextInt(toServertotalRegions);
      for (int i = 0; i < toServertotalRegions; i++) {
        int toRegionIndex = (startIndex + i) % toServertotalRegions;
        int toRegion = cluster.regionsPerServer[toServer][toRegionIndex];
        double toRegionLocalityDelta = getWeightedLocality(cluster, toRegion, fromServer) -
          getWeightedLocality(cluster, toRegion, toServer);
        // If locality would remain neutral or improve, attempt the swap
        if (fromRegionLocalityDelta + toRegionLocalityDelta >= 0) {
          return Optional.of(getAction(fromServer, fromRegion, toServer, toRegion));
        }
      }
    }
    return Optional.empty();
  }

  private double getWeightedLocality(BaseLoadBalancer.Cluster cluster, int region, int server) {
    return cluster.getOrComputeWeightedLocality(region, server,
      BaseLoadBalancer.Cluster.LocalityType.SERVER);
  }
}
