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

import java.time.Duration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RegionPlanConditionalCandidateGenerator extends CandidateGenerator {

  private static final Logger LOG =
    LoggerFactory.getLogger(RegionPlanConditionalCandidateGenerator.class);

  private static final Duration WEIGHT_CACHE_TTL = Duration.ofMinutes(1);
  private long lastWeighedAt = -1;
  private double lastWeight = 0.0;

  abstract BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing);

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    BalanceAction balanceAction = generateCandidate(cluster, false);
    if (!willBeAccepted(cluster, balanceAction)) {
      LOG.debug("Generated action is not widely accepted by all conditionals. "
        + "Likely we are finding our way out of a deadlock. balanceAction={}", balanceAction);
    }
    return balanceAction;
  }

  boolean willBeAccepted(BalancerClusterState cluster, BalanceAction action) {
    return !BalancerConditionals.INSTANCE.isViolating(cluster, action);
  }

  void undoBatchAction(BalancerClusterState cluster, MoveBatchAction batchAction) {
    for (int i = batchAction.getMoveActions().size() - 1; i >= 0; i--) {
      MoveRegionAction action = batchAction.getMoveActions().get(i);
      cluster.doAction(action.undoAction());
    }
  }

  void clearWeightCache() {
    lastWeighedAt = -1;
  }

  double getWeight(BalancerClusterState cluster) {
    boolean hasCandidate = false;

    // Candidate generation is expensive, so for re-weighing generators we will cache
    // the value for a bit
    if (System.currentTimeMillis() - lastWeighedAt < WEIGHT_CACHE_TTL.toMillis()) {
      return lastWeight;
    } else {
      hasCandidate = generateCandidate(cluster, true) != BalanceAction.NULL_ACTION;
      lastWeighedAt = System.currentTimeMillis();
    }

    if (hasCandidate) {
      // If this generator has something to do, then it's important
      lastWeight = CandidateGenerator.MAX_WEIGHT;
    } else {
      lastWeight = 0;
    }
    return lastWeight;
  }
}
