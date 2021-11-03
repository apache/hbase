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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Given the starting state of the regions and a potential ending state compute cost based upon the
 * number of regions that have moved.
 */
@InterfaceAudience.Private
class MoveCostFunction extends CostFunction {
  private static final String MOVE_COST_KEY = "hbase.master.balancer.stochastic.moveCost";
  private static final String MOVE_COST_OFFPEAK_KEY =
    "hbase.master.balancer.stochastic.moveCost.offpeak";
  private static final String MAX_MOVES_PERCENT_KEY =
    "hbase.master.balancer.stochastic.maxMovePercent";
  static final float DEFAULT_MOVE_COST = 7;
  static final float DEFAULT_MOVE_COST_OFFPEAK = 3;
  private static final int DEFAULT_MAX_MOVES = 600;
  private static final float DEFAULT_MAX_MOVE_PERCENT = 1.0f;

  private final float maxMovesPercent;
  private final OffPeakHours offPeakHours;
  private final float moveCost;
  private final float moveCostOffPeak;

  MoveCostFunction(Configuration conf) {
    // What percent of the number of regions a single run of the balancer can move.
    maxMovesPercent = conf.getFloat(MAX_MOVES_PERCENT_KEY, DEFAULT_MAX_MOVE_PERCENT);
    offPeakHours = OffPeakHours.getInstance(conf);
    moveCost = conf.getFloat(MOVE_COST_KEY, DEFAULT_MOVE_COST);
    moveCostOffPeak = conf.getFloat(MOVE_COST_OFFPEAK_KEY, DEFAULT_MOVE_COST_OFFPEAK);
    // Initialize the multiplier so that addCostFunction will add this cost function.
    // It may change during later evaluations, due to OffPeakHours.
    this.setMultiplier(moveCost);
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    // Move cost multiplier should be the same cost or higher than the rest of the costs to ensure
    // that large benefits are need to overcome the cost of a move.
    if (offPeakHours.isOffPeakHour()) {
      this.setMultiplier(moveCostOffPeak);
    } else {
      this.setMultiplier(moveCost);
    }
  }

  @Override
  protected double cost() {
    // Try and size the max number of Moves, but always be prepared to move some.
    int maxMoves = Math.max((int) (cluster.numRegions * maxMovesPercent), DEFAULT_MAX_MOVES);

    double moveCost = cluster.numMovedRegions;

    // Don't let this single balance move more than the max moves.
    // This allows better scaling to accurately represent the actual cost of a move.
    if (moveCost > maxMoves) {
      return 1000000; // return a number much greater than any of the other cost
    }

    return scale(0, Math.min(cluster.numRegions, maxMoves), moveCost);
  }
}
