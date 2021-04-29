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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class of StochasticLoadBalancer's Cost Functions.
 */
@InterfaceAudience.Private
abstract class CostFunction {

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
  void init(BalancerClusterState cluster) {
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
      default:
        throw new RuntimeException("Uknown action:" + action.getType());
    }
  }

  protected void regionMoved(int region, int oldServer, int newServer) {
  }

  protected abstract double cost();

  @SuppressWarnings("checkstyle:linelength")
  /**
   * Function to compute a scaled cost using
   * {@link org.apache.commons.math3.stat.descriptive.DescriptiveStatistics#DescriptiveStatistics()}.
   * It assumes that this is a zero sum set of costs. It assumes that the worst case possible is all
   * of the elements in one region server and the rest having 0.
   * @param stats the costs
   * @return a scaled set of costs.
   */
  protected final double costFromArray(double[] stats) {
    double totalCost = 0;
    double total = getSum(stats);

    double count = stats.length;
    double mean = total / count;

    // Compute max as if all region servers had 0 and one had the sum of all costs. This must be
    // a zero sum cost for this to make sense.
    double max = ((count - 1) * mean) + (total - mean);

    // It's possible that there aren't enough regions to go around
    double min;
    if (count > total) {
      min = ((count - total) * mean) + ((1 - mean) * total);
    } else {
      // Some will have 1 more than everything else.
      int numHigh = (int) (total - (Math.floor(mean) * count));
      int numLow = (int) (count - numHigh);

      min = (numHigh * (Math.ceil(mean) - mean)) + (numLow * (mean - Math.floor(mean)));

    }
    min = Math.max(0, min);
    for (int i = 0; i < stats.length; i++) {
      double n = stats[i];
      double diff = Math.abs(mean - n);
      totalCost += diff;
    }

    double scaled = scale(min, max, totalCost);
    return scaled;
  }

  private double getSum(double[] stats) {
    double total = 0;
    for (double s : stats) {
      total += s;
    }
    return total;
  }

  /**
   * Scale the value between 0 and 1.
   * @param min Min value
   * @param max The Max value
   * @param value The value to be scaled.
   * @return The scaled value.
   */
  protected final double scale(double min, double max, double value) {
    if (max <= min || value <= min) {
      return 0;
    }
    if ((max - min) == 0) {
      return 0;
    }

    return Math.max(0d, Math.min(1d, (value - min) / (max - min)));
  }
}