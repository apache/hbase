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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class that can be used to implement
 * queues with limited capacity (in terms of memory).
 * It maintains internal counter and provides
 * two operations: increase and decrease.
 * Increase blocks until internal counter is lower than
 * given threshold and then increases internal counter.
 * Decrease decreases internal counter and wakes up
 * waiting threads if counter is lower than threshold.
 *
 * This implementation allows you to set the value of internal
 * counter to be greater than threshold. It happens
 * when internal counter is lower than threshold and
 * increase method is called with parameter 'delta' big enough
 * so that sum of delta and internal counter is greater than
 * threshold. This is not a bug, this is a feature.
 * It solves some problems:
 *   - thread calling increase with big parameter will not be
 *     starved by other threads calling increase with small
 *     arguments.
 *   - thread calling increase with argument greater than
 *     threshold won't deadlock. This is useful when throttling
 *     queues - you can submit object that is bigger than limit.
 *
 * This implementation introduces small costs in terms of
 * synchronization (no synchronization in most cases at all), but is
 * vulnerable to races. For details see documentation of
 * increase method.
 */
public class SizeBasedThrottler {

  private final long threshold;
  private final AtomicLong currentSize;

  /**
   * Creates SizeBoundary with provided threshold
   *
   * @param threshold threshold used by instance
   */
  public SizeBasedThrottler(long threshold) {
    if (threshold <= 0) {
      throw new IllegalArgumentException("Treshold must be greater than 0");
    }
    this.threshold = threshold;
    this.currentSize = new AtomicLong(0);
  }

  /**
   * Blocks until internal counter is lower than threshold
   * and then increases value of internal counter.
   *
   * THIS METHOD IS VULNERABLE TO RACES.
   * It may happen that increment operation will
   * succeed immediately, even if it should block. This happens when
   * at least two threads call increase at the some moment. The decision
   * whether to block is made at the beginning, without synchronization.
   * If value of currentSize is lower than threshold at that time, call
   * will succeed immediately. It is possible, that 2 threads will make
   * decision not to block, even if one of them should block.
   *
   * @param delta increase internal counter by this value
   * @return new value of internal counter
   * @throws InterruptedException when interrupted during waiting
   */
  public synchronized long increase(long delta) throws InterruptedException{
    if (currentSize.get() >= threshold) {
      synchronized (this) {
        while (currentSize.get() >= threshold) {
          wait();
        }
      }
    }

    return currentSize.addAndGet(delta);
  }


  /**
   * Decreases value of internal counter. Wakes up waiting threads if required.
   *
   * @param delta decrease internal counter by this value
   * @return new value of internal counter
   */
  public synchronized long decrease(long delta) {
    final long newSize = currentSize.addAndGet(-delta);

    if (newSize < threshold && newSize + delta >= threshold) {
      synchronized (this) {
        notifyAll();
      }
    }

    return newSize;
  }

  /**
   *
   * @return current value of internal counter
   */
  public synchronized long getCurrentValue(){
    return currentSize.get();
  }

  /**
   * @return threshold
   */
  public long getThreshold(){
    return threshold;
  }
}
