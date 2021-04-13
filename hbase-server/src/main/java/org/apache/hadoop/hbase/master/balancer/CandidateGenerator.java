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

import java.util.Map;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Generates a candidate action to be applied to the cluster for cost function search
 */
@InterfaceAudience.Private
abstract class CandidateGenerator {

  abstract BaseLoadBalancer.Cluster.Action generate(BaseLoadBalancer.Cluster cluster);

  /**
   * From a list of regions pick a random one. Null can be returned which
   * {@link StochasticLoadBalancer#balanceCluster(Map)} recognize as signal to try a region move
   * rather than swap.
   *
   * @param cluster The state of the cluster
   * @param server index of the server
   * @param chanceOfNoSwap Chance that this will decide to try a move rather
   *   than a swap.
   * @return a random {@link RegionInfo} or null if an asymmetrical move is
   *   suggested.
   */
  int pickRandomRegion(BaseLoadBalancer.Cluster cluster, int server,
    double chanceOfNoSwap) {
    // Check to see if this is just a move.
    if (cluster.regionsPerServer[server].length == 0
        || StochasticLoadBalancer.RANDOM.nextFloat() < chanceOfNoSwap) {
      // signal a move only.
      return -1;
    }
    int rand = StochasticLoadBalancer.RANDOM.nextInt(cluster.regionsPerServer[server].length);
    return cluster.regionsPerServer[server][rand];
  }

  int pickRandomServer(BaseLoadBalancer.Cluster cluster) {
    if (cluster.numServers < 1) {
      return -1;
    }

    return StochasticLoadBalancer.RANDOM.nextInt(cluster.numServers);
  }

  int pickRandomRack(BaseLoadBalancer.Cluster cluster) {
    if (cluster.numRacks < 1) {
      return -1;
    }

    return StochasticLoadBalancer.RANDOM.nextInt(cluster.numRacks);
  }

  int pickOtherRandomServer(BaseLoadBalancer.Cluster cluster, int serverIndex) {
    if (cluster.numServers < 2) {
      return -1;
    }
    while (true) {
      int otherServerIndex = pickRandomServer(cluster);
      if (otherServerIndex != serverIndex) {
        return otherServerIndex;
      }
    }
  }

  int pickOtherRandomRack(BaseLoadBalancer.Cluster cluster, int rackIndex) {
    if (cluster.numRacks < 2) {
      return -1;
    }
    while (true) {
      int otherRackIndex = pickRandomRack(cluster);
      if (otherRackIndex != rackIndex) {
        return otherRackIndex;
      }
    }
  }

  BaseLoadBalancer.Cluster.Action pickRandomRegions(BaseLoadBalancer.Cluster cluster,
    int thisServer, int otherServer) {
    if (thisServer < 0 || otherServer < 0) {
      return BaseLoadBalancer.Cluster.NullAction;
    }

    // Decide who is most likely to need another region
    int thisRegionCount = cluster.getNumRegions(thisServer);
    int otherRegionCount = cluster.getNumRegions(otherServer);

    // Assign the chance based upon the above
    double thisChance = (thisRegionCount > otherRegionCount) ? 0 : 0.5;
    double otherChance = (thisRegionCount <= otherRegionCount) ? 0 : 0.5;

    int thisRegion = pickRandomRegion(cluster, thisServer, thisChance);
    int otherRegion = pickRandomRegion(cluster, otherServer, otherChance);

    return getAction(thisServer, thisRegion, otherServer, otherRegion);
  }

  protected BaseLoadBalancer.Cluster.Action getAction(int fromServer, int fromRegion,
      int toServer, int toRegion) {
    if (fromServer < 0 || toServer < 0) {
      return BaseLoadBalancer.Cluster.NullAction;
    }
    if (fromRegion >= 0 && toRegion >= 0) {
      return new BaseLoadBalancer.Cluster.SwapRegionsAction(fromServer, fromRegion,
        toServer, toRegion);
    } else if (fromRegion >= 0) {
      return new BaseLoadBalancer.Cluster.MoveRegionAction(fromRegion, fromServer, toServer);
    } else if (toRegion >= 0) {
      return new BaseLoadBalancer.Cluster.MoveRegionAction(toRegion, toServer, fromServer);
    } else {
      return BaseLoadBalancer.Cluster.NullAction;
    }
  }
}
