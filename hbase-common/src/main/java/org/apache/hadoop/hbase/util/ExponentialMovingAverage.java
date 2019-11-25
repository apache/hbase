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

package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * EMA is similar to {@link WeightedMovingAverage} in weighted, but the weighting factor decrease
 * exponentially. It brings benefits that it is more sensitive, and can see the trends easily.
 */
@InterfaceAudience.Private
public class ExponentialMovingAverage extends WindowMovingAverage {
  private double alpha;
  private double previousAverage;
  private double currentAverage;

  public ExponentialMovingAverage(String label) {
    this(label, DEFAULT_SIZE);
  }

  public ExponentialMovingAverage(String label, double alpha) {
    this(label, DEFAULT_SIZE, alpha);
  }

  public ExponentialMovingAverage(String label, int size) {
    this(label, size, (double) 2 / (1 + size));
  }

  public ExponentialMovingAverage(String label, int size, double alpha) {
    super(label, size);
    this.previousAverage = -1.0;
    this.currentAverage = 0.0;
    this.alpha = alpha;
  }

  @Override
  public void updateMostRecentTime(long elapsed) {
    if (!enoughStatistics()) {
      previousAverage = super.getAverageTime();
      super.updateMostRecentTime(elapsed);
      if (!enoughStatistics()) {
        return;
      }
    }
    // CurrentEMA = α * currentValue + (1 - α) * previousEMA =>
    // CurrentEMA = (currentValue - previousEMA) * α + previousEMA
    // This will reduce multiplication.
    currentAverage = (elapsed - previousAverage) * alpha + previousAverage;
    previousAverage = currentAverage;
  }

  @Override
  public double getAverageTime() {
    if (!enoughStatistics()) {
      return super.getAverageTime();
    }
    return currentAverage;
  }

  double getPrevious() {
    return previousAverage;
  }
}
