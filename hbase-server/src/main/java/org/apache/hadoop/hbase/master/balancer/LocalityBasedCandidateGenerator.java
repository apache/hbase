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

import org.apache.hadoop.hbase.master.MasterServices;

import org.apache.hbase.thirdparty.com.google.common.base.Optional;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class LocalityBasedCandidateGenerator extends CandidateGenerator {

  private MasterServices masterServices;

  LocalityBasedCandidateGenerator(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override
  BaseLoadBalancer.Cluster.Action generate(BaseLoadBalancer.Cluster cluster) {
    if (this.masterServices == null) {
      int thisServer = pickRandomServer(cluster);
      // Pick the other server
      int otherServer = pickOtherRandomServer(cluster, thisServer);
      return pickRandomRegions(cluster, thisServer, otherServer);
    }

    // Randomly iterate through regions until you find one that is not on ideal host
    for (int region : getRandomIterationOrder(cluster.numRegions)) {
      int currentServer = cluster.regionIndexToServerIndex[region];
      if (currentServer != cluster.getOrComputeRegionsToMostLocalEntities(
        BaseLoadBalancer.Cluster.LocalityType.SERVER)[region]) {
        Optional<BaseLoadBalancer.Cluster.Action> potential = tryMoveOrSwap(cluster,
          currentServer, region,
          cluster.getOrComputeRegionsToMostLocalEntities(
            BaseLoadBalancer.Cluster.LocalityType.SERVER)[region]
        );
        if (potential.isPresent()) {
          return potential.get();
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
    for (int toRegionIndex : getRandomIterationOrder(cluster.regionsPerServer[toServer].length)) {
      int toRegion = cluster.regionsPerServer[toServer][toRegionIndex];
      double toRegionLocalityDelta = getWeightedLocality(cluster, toRegion, fromServer)
        - getWeightedLocality(cluster, toRegion, toServer);
      // If locality would remain neutral or improve, attempt the swap
      if (fromRegionLocalityDelta + toRegionLocalityDelta >= 0) {
        return Optional.of(getAction(fromServer, fromRegion, toServer, toRegion));
      }
    }
    return Optional.absent();
  }

  private double getWeightedLocality(BaseLoadBalancer.Cluster cluster, int region, int server) {
    return cluster.getOrComputeWeightedLocality(region, server,
      BaseLoadBalancer.Cluster.LocalityType.SERVER);
  }

  void setServices(MasterServices services) {
    this.masterServices = services;
  }

}
