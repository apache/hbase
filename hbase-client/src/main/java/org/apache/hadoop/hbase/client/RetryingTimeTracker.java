/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Tracks the amount of time remaining for an operation.
 */
class RetryingTimeTracker {
  private long globalStartTime = -1;

  public void start() {
    if (this.globalStartTime < 0) {
      this.globalStartTime = EnvironmentEdgeManager.currentTime();
    }
  }

  public int getRemainingTime(int callTimeout) {
    if (callTimeout <= 0) {
      return 0;
    } else {
      if (callTimeout == Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      long remaining = EnvironmentEdgeManager.currentTime() - this.globalStartTime;
      long remainingTime = callTimeout - remaining;
      if (remainingTime < 1) {
        // If there is no time left, we're trying anyway. It's too late.
        // 0 means no timeout, and it's not the intent here. So we secure both cases by
        // resetting to the minimum.
        remainingTime = 1;
      }
      if (remainingTime > Integer.MAX_VALUE) {
        throw new RuntimeException("remainingTime=" + remainingTime +
            " which is > Integer.MAX_VALUE");
      }
      return (int)remainingTime;
    }
  }

  public long getStartTime() {
    return this.globalStartTime;
  }
}
