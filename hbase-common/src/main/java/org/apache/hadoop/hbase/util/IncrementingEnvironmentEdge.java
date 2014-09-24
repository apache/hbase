/*
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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Uses an incrementing algorithm instead of the default.
 */
@InterfaceAudience.Private
public class IncrementingEnvironmentEdge implements EnvironmentEdge {

  private long timeIncrement;

  /**
   * Construct an incremental edge starting from currentTimeMillis
   */
  public IncrementingEnvironmentEdge() {
    this(System.currentTimeMillis());
  }

  /**
   * Construct an incremental edge with an initial amount
   * @param initialAmount the initial value to start with
   */
  public IncrementingEnvironmentEdge(long initialAmount) {
    this.timeIncrement = initialAmount;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * This method increments a known value for the current time each time this
   * method is called. The first value is 1.
   */
  @Override
  public synchronized long currentTime() {
    return timeIncrement++;
  }

  /**
   * Increment the time by the given amount
   */
  public synchronized long incrementTime(long amount) {
    timeIncrement += amount;
    return timeIncrement;
  }
}
