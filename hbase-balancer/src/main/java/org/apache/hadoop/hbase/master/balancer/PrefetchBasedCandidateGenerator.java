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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
class PrefetchBasedCandidateGenerator extends CandidateGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(PrefetchBasedCandidateGenerator.class);

  private static float PREFETCH_RATIO_DIFF_FACTOR = 1.25f;

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    // iterate through regions until you find one that is not on ideal host
    // start from a random point to avoid always balance the regions in front
    if (cluster.numRegions > 0) {
      int startRegionIndex = ThreadLocalRandom.current().nextInt(cluster.numRegions);
      int toServerIndex;
      for (int i = 0; i < cluster.numRegions; i++) {
        int region = (startRegionIndex + i) % cluster.numRegions;
        int currentServerIndex = cluster.regionIndexToServerIndex[region];
        float currentPrefetchRatio =
          cluster.getOrComputeWeightedPrefetchRatio(region, currentServerIndex);

        // Check if there is a server with a better historical prefetch ratio
        toServerIndex = pickOtherRandomServer(cluster, currentServerIndex);
        float toServerPrefetchRatio =
          cluster.getOrComputeWeightedPrefetchRatio(region, toServerIndex);

        // If the prefetch ratio on the target server is significantly higher, move the region.
        if (currentPrefetchRatio > 0 &&
          (toServerPrefetchRatio / currentPrefetchRatio) > PREFETCH_RATIO_DIFF_FACTOR) {
          return getAction(currentServerIndex, region, toServerIndex, -1);
        }
      }
    }
    return BalanceAction.NULL_ACTION;
  }
}
