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

  /**
   * We do not want to introduce a getCosts method to let upper layer get the cost array directly,
   * so here we introduce this method to take a {@link Consumer} as parameter, where we will pass
   * the actual cost array in, so you can change the element of the cost array in the
   * {@link Consumer} implementation.
   * <p/>
   * Usually, in prepare method, you need to fill all the elements of the cost array, while in
   * regionMoved method, you just need to update the element for the effect region servers.
   */
  void applyCostsChange(Consumer<double[]> consumer) {
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
    if (stats == null || stats.length == 0) {
      return 0;
    }
    double totalCost = 0;
    double total = getSum(stats);

    double count = stats.length;
    double mean = total / count;

    for (int i = 0; i < stats.length; i++) {
      double n = stats[i];
      double diff = (mean - n) * (mean - n);
      totalCost += diff;
    }
    // No need to compute standard deviation with division by cluster size when scaling.
    totalCost = Math.sqrt(totalCost);
    return StochasticLoadBalancer.scale(getMinSkew(total, count),
      getMaxSkew(total, count), totalCost);
  }

  private static double getSum(double[] stats) {
    double total = 0;
    for (double s : stats) {
      total += s;
    }
    return total;
  }

  /**
   * Return the min skew of distribution
   * @param total is total number of regions
   */
  public static double getMinSkew(double total, double numServers) {
    if (numServers == 0) {
      return 0;
    }
    double mean = total / numServers;
    // It's possible that there aren't enough regions to go around
    double min;
    if (numServers > total) {
      min = ((numServers - total) * mean * mean + (1 - mean) * (1 - mean) * total);
    } else {
      // Some will have 1 more than everything else.
      int numHigh = (int) (total - (Math.floor(mean) * numServers));
      int numLow = (int) (numServers - numHigh);
      min = numHigh * (Math.ceil(mean) - mean) * (Math.ceil(mean) - mean) +
        numLow * (mean - Math.floor(mean)) * (mean - Math.floor(mean));
    }
    return Math.sqrt(min);
  }

  /**
   * Return the max deviation of distribution
   * Compute max as if all region servers had 0 and one had the sum of all costs.  This must be
   * a zero sum cost for this to make sense.
   * @param total is total number of regions
   */
  public static double getMaxSkew(double total, double numServers) {
    if (numServers == 0) {
      return 0;
    }
    double mean = total / numServers;
    return Math.sqrt((total - mean) * (total - mean) + (numServers - 1) * mean * mean);
  }
}
