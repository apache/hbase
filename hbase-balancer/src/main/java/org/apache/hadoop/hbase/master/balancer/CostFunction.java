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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class of StochasticLoadBalancer's Cost Functions.
 */
@InterfaceAudience.Private
abstract class CostFunction {

  public static double getCostEpsilon(double cost) {
    return Math.ulp(cost);
  }

  private float multiplier = 0;

  protected BalancerClusterState cluster;

  boolean isNeeded() {
    return true;
  }

  float getMultiplier() {
    return multiplier;
  }

  void setMultiplier(float m) {
    this.multiplier = m;
  }

  /**
   * Called once per LB invocation to give the cost function to initialize it's state, and perform
   * any costly calculation.
   */
  void prepare(BalancerClusterState cluster) {
    this.cluster = cluster;
  }

  /**
   * Called once per cluster Action to give the cost function an opportunity to update it's state.
   * postAction() is always called at least once before cost() is called with the cluster that this
   * action is performed on.
   */
  void postAction(BalanceAction action) {
    switch (action.getType()) {
      case NULL:
        break;
      case ASSIGN_REGION:
        AssignRegionAction ar = (AssignRegionAction) action;
        regionMoved(ar.getRegion(), -1, ar.getServer());
        break;
      case MOVE_REGION:
        MoveRegionAction mra = (MoveRegionAction) action;
        regionMoved(mra.getRegion(), mra.getFromServer(), mra.getToServer());
        break;
      case SWAP_REGIONS:
        SwapRegionsAction a = (SwapRegionsAction) action;
        regionMoved(a.getFromRegion(), a.getFromServer(), a.getToServer());
        regionMoved(a.getToRegion(), a.getToServer(), a.getFromServer());
        break;
      case MOVE_BATCH:
        MoveBatchAction mba = (MoveBatchAction) action;
        for (MoveRegionAction moveRegionAction : mba.getMoveActions()) {
          regionMoved(moveRegionAction.getRegion(), moveRegionAction.getFromServer(),
            moveRegionAction.getToServer());
        }
        break;
      default:
        throw new RuntimeException("Uknown action:" + action.getType());
    }
  }

  protected void regionMoved(int region, int oldServer, int newServer) {
  }

  protected abstract double cost();

  /**
   * Add the cost of this cost function to the weight of the candidate generator that is optimized
   * for this cost function. By default it is the RandomCandiateGenerator for a cost function.
   * Called once per init or after postAction.
   * @param weights the weights for every generator.
   */
  public void updateWeight(Map<Class<? extends CandidateGenerator>, Double> weights) {
    weights.merge(RandomCandidateGenerator.class, cost(), Double::sum);
  }

  /**
   * Scale the value between 0 and 1.
   * @param min   Min value
   * @param max   The Max value
   * @param value The value to be scaled.
   * @return The scaled value.
   */
  protected static double scale(double min, double max, double value) {
    double costEpsilon = getCostEpsilon(max);
    if (
      max <= min || value <= min || Math.abs(max - min) <= costEpsilon
        || Math.abs(value - min) <= costEpsilon
    ) {
      return 0;
    }
    return Math.max(0d, Math.min(1d, (value - min) / (max - min)));
  }
}
