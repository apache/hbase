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

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class TableIsolationCandidateGenerator extends CandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(TableIsolationCandidateGenerator.class);

  abstract boolean shouldBeIsolated(RegionInfo regionInfo);

  double getWeight(BalancerClusterState cluster) {
    if (generateCandidate(cluster, true) != BalanceAction.NULL_ACTION) {
      // If this generator has something to do, then it's important
      return Double.MAX_VALUE;
    } else {
      return 0;
    }
  }

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    return generateCandidate(cluster, false);
  }

  private BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    if (!BalancerConditionals.INSTANCE.isTableIsolationEnabled()) {
      return BalanceAction.NULL_ACTION;
    }

    int fromServer = -1;
    int lastMovableRegion = -1;

    // Find regions that need to move
    for (int serverIndex = 0; serverIndex < cluster.servers.length; serverIndex++) {
      boolean hasRegionToBeIsolated = false;
      lastMovableRegion = -1;
      for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
        RegionInfo regionInfo = cluster.regions[regionIndex];
        if (shouldBeIsolated(regionInfo)) {
          hasRegionToBeIsolated = true;
        } else {
          lastMovableRegion = regionIndex;
        }
        if (hasRegionToBeIsolated && lastMovableRegion != -1) {
          fromServer = serverIndex;
          break;
        }
      }
      if (fromServer != -1) {
        break;
      }
    }
    if (fromServer == -1) {
      return BalanceAction.NULL_ACTION;
    }

    int toServer = pickOtherRandomServer(cluster, fromServer);

    if (!isWeighing) {
      LOG.debug("Should move region {} off of server {} to server {}",
        cluster.regions[lastMovableRegion].getEncodedName(),
        cluster.servers[fromServer].getServerName(), cluster.servers[toServer].getServerName());
    }

    return getAction(fromServer, lastMovableRegion, toServer, -1);
  }
}
