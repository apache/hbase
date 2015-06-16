/**
 *
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

/**
 * This class maintains mean and variation for any sequence of input provided to it.
 * It is initialized with number of rolling periods which basically means the number of past
 * inputs whose data will be considered to maintain mean and variation.
 * It will use O(N) memory to maintain these statistics, where N is number of look up periods it
 * was initialized with.
 * If zero is passed during initialization then it will maintain mean and variance from the
 * start. It will use O(1) memory only. But note that since it will maintain mean / variance
 * from the start the statistics may behave like constants and may ignore short trends.
 * All operations are O(1) except the initialization which is O(N).
 */
public class RollingStatCalculator {
  private double currentSum;
  private double currentSqrSum;
  // Total number of data values whose statistic is currently present
  private long numberOfDataValues;
  private int rollingPeriod;
  private int currentIndexPosition;
  // to be used only if we have non-zero rolling period
  private long [] dataValues;

  /**
   * Creates a RollingStatCalculator with given number of rolling periods.
   * @param rollingPeriod
   */
  public RollingStatCalculator(int rollingPeriod) {
    this.rollingPeriod = rollingPeriod;
    this.dataValues = fillWithZeros(rollingPeriod);
    this.currentSum = 0.0;
    this.currentSqrSum = 0.0;
    this.currentIndexPosition = 0;
    this.numberOfDataValues = 0;
  }

  /**
   * Inserts given data value to array of data values to be considered for statistics calculation
   * @param data
   */
  public void insertDataValue(long data) {
    // if current number of data points already equals rolling period and rolling period is
    // non-zero then remove one data and update the statistics
    if(numberOfDataValues >= rollingPeriod && rollingPeriod > 0) {
      this.removeData(dataValues[currentIndexPosition]);
    }
    numberOfDataValues++;
    currentSum = currentSum + (double)data;
    currentSqrSum = currentSqrSum + ((double)data * data);
    if (rollingPeriod >0)
    {
      dataValues[currentIndexPosition] = data;
      currentIndexPosition = (currentIndexPosition + 1) % rollingPeriod;
    }
  }

  /**
   * Update the statistics after removing the given data value
   * @param data
   */
  private void removeData(long data) {
    currentSum = currentSum - (double)data;
    currentSqrSum = currentSqrSum - ((double)data * data);
    numberOfDataValues--;
  }

  /**
   * @return mean of the data values that are in the current list of data values
   */
  public double getMean() {
    return this.currentSum / (double)numberOfDataValues;
  }

  /**
   * @return deviation of the data values that are in the current list of data values
   */
  public double getDeviation() {
    double variance = (currentSqrSum - (currentSum*currentSum)/(double)(numberOfDataValues))/
        numberOfDataValues;
    return Math.sqrt(variance);
  }

  /**
   * @param size
   * @return an array of given size initialized with zeros
   */
  private long [] fillWithZeros(int size) {
    long [] zeros = new long [size];
    for (int i=0; i<size; i++) {
      zeros[i] = 0L;
    }
    return zeros;
  }
}
