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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of introduction of {@link MovingAverage} mainly is to measure execution time of a
 * specific method, which can help us to know its performance fluctuation in response to different
 * machine states or situations, better case, then to act accordingly.
 * <br>
 * In different situation, different {@link MovingAverage} algorithm can be used based on needs.
 */
@InterfaceAudience.Private
public abstract class MovingAverage<T> {
  private final static Logger LOG = LoggerFactory.getLogger(MovingAverage.class);

  protected final String label;

  protected MovingAverage(String label) {
    this.label = label;
  }

  /**
   * Mark start time of an execution.
   * @return time in ns.
   */
  protected long start() {
    return System.nanoTime();
  }

  /**
   * Mark end time of an execution, and return its interval.
   * @param startTime start time of an execution
   * @return elapsed time
   */
  protected long stop(long startTime) {
    return System.nanoTime() - startTime;
  }

  /**
   * Measure elapsed time of a measurable method.
   * @param measurable method implements {@link TimeMeasurable}
   * @return T it refers to the original return type of the measurable method
   */
  public T measure(TimeMeasurable<T> measurable) {
    long startTime = start();
    LOG.trace("{} - start to measure at: {} ns.", label, startTime);
    // Here may throw exceptions which should be taken care by caller, not here.
    // If exception occurs, this time wouldn't count.
    T result = measurable.measure();
    long elapsed = stop(startTime);
    LOG.trace("{} - elapse: {} ns.", label, elapsed);
    updateMostRecentTime(elapsed);
    return result;
  }

  /**
   * Update the most recent data.
   * @param elapsed elapsed time of the most recent measurement
   */
  protected abstract void updateMostRecentTime(long elapsed);

  /**
   * Get average execution time of the measured method.
   * @return average time in ns
   */
  public abstract double getAverageTime();
}
