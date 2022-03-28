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
 * Different from SMA {@link SimpleMovingAverage}, WeightedMovingAverage gives each data different
 * weight. And it is based on {@link WindowMovingAverage}, such that it only focus on the last N.
 */
@InterfaceAudience.Private
public class WeightedMovingAverage extends WindowMovingAverage {
  private int[] coefficient;
  private int denominator;

  public WeightedMovingAverage(String label) {
    this(label, DEFAULT_SIZE);
  }

  public WeightedMovingAverage(String label, int size) {
    super(label, size);
    int length = getNumberOfStatistics();
    denominator = length * (length + 1) / 2;
    coefficient = new int[length];
    // E.g. default size is 5, coefficient should be [1, 2, 3, 4, 5]
    for (int i = 0; i < length; i++) {
      coefficient[i] = i + 1;
    }
  }

  @Override
  public double getAverageTime() {
    if (!enoughStatistics()) {
      return super.getAverageTime();
    }
    // only we get enough statistics, then start WMA.
    double average = 0.0;
    int coIndex = 0;
    int length = getNumberOfStatistics();
    // tmIndex, it points to the oldest data.
    for (int tmIndex = (getMostRecentPosistion() + 1) % length;
         coIndex < length;
         coIndex++, tmIndex = (++tmIndex) % length) {
      // start the multiplication from oldest to newest
      average += coefficient[coIndex] * getStatisticsAtIndex(tmIndex);
    }
    return average / denominator;
  }
}
