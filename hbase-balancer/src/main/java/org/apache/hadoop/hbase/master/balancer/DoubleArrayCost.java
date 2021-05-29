/**
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

import java.util.function.Consumer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A helper class to compute a scaled cost using
 * {@link org.apache.commons.math3.stat.descriptive.DescriptiveStatistics#DescriptiveStatistics()}.
 * It assumes that this is a zero sum set of costs. It assumes that the worst case possible is all
 * of the elements in one region server and the rest having 0.
 */
@InterfaceAudience.Private
final class DoubleArrayCost {

  private double[] costs;

  // computeCost call is expensive so we use this flag to indicate whether we need to recalculate
  // the cost by calling computeCost
  private boolean costsChanged;

  private double cost;

  void prepare(int length) {
    if (costs == null || costs.length != length) {
      costs = new double[length];
    }
  }

  void setCosts(Consumer<double[]> consumer) {
    consumer.accept(costs);
    costsChanged = true;
  }

  double cost() {
    if (costsChanged) {
      cost = computeCost(costs);
      costsChanged = false;
    }
    return cost;
  }

  private static double computeCost(double[] stats) {
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

    double scaled = CostFunction.scale(min, max, totalCost);
    return scaled;
  }

  private static double getSum(double[] stats) {
    double total = 0;
    for (double s : stats) {
      total += s;
    }
    return total;
  }
}
