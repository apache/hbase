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
 * Instead of calculate a whole time average, this class focus on the last N.
 * The last N is stored in a circle array.
 */
@InterfaceAudience.Private
public class WindowMovingAverage extends MovingAverage {
  protected final static int DEFAULT_SIZE = 5;

  // The last n statistics.
  protected long[] lastN;
  // The index of the most recent statistics.
  protected int mostRecent;
  // If it travels a round.
  protected boolean oneRound;

  public WindowMovingAverage(String label) {
    this(label, DEFAULT_SIZE);
  }

  public WindowMovingAverage(String label, int size) {
    super(label);
    this.lastN = new long[size <= 0 ? DEFAULT_SIZE : size];
    this.mostRecent = -1;
    this.oneRound = false;
  }

  @Override
  protected void updateMostRecentTime(long elapsed) {
    int index = moveForwardMostRecentPosistion();
    lastN[index] = elapsed;
  }

  @Override
  public double getAverageTime() {
    return enoughStatistics() ?
      (double) sum(getNumberOfStatistics()) / getNumberOfStatistics() :
      (double) sum(getMostRecentPosistion() + 1) / (getMostRecentPosistion() + 1);
  }

  /**
   * Check if there are enough statistics.
   * @return true if lastN is full
   */
  protected boolean enoughStatistics() {
    return oneRound;
  }

  /**
   * @return number of statistics
   */
  protected int getNumberOfStatistics() {
    return lastN.length;
  }

  /**
   * Get statistics at index.
   * @param index index of bar
   * @return statistics
   */
  protected long getStatisticsAtIndex(int index) {
    if (index < 0 || index >= getNumberOfStatistics()) {
      // This case should not happen, but a prudent check.
      throw new IndexOutOfBoundsException();
    }
    return lastN[index];
  }

  /**
   * @return index of most recent
   */
  protected int getMostRecentPosistion() {
    return mostRecent;
  }

  /**
   * Move forward the most recent index.
   * @return the most recent index
   */
  protected int moveForwardMostRecentPosistion() {
    int index = ++mostRecent;
    if (!oneRound && index == getNumberOfStatistics()) {
      // Back to the head of the lastN, from now on will
      // start to evict oldest value.
      oneRound = true;
    }
    mostRecent = index % getNumberOfStatistics();
    return mostRecent;
  }

  private long sum(int bound) {
    long sum = 0;
    for (int i = 0; i < bound; i++) {
      sum += getStatisticsAtIndex(i);
    }
    return sum;
  }
}
