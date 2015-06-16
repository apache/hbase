/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import com.google.common.annotations.VisibleForTesting;

/**
 * This limiter will refill resources at every TimeUnit/resources interval. For example: For a
 * limiter configured with 10resources/second, then 1 resource will be refilled after every 100ms
 * (1sec/10resources)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AverageIntervalRateLimiter extends RateLimiter {
  private long nextRefillTime = -1L;

  @Override
  public long refill(long limit) {
    final long now = EnvironmentEdgeManager.currentTime();
    if (nextRefillTime == -1) {
      // Till now no resource has been consumed.
      nextRefillTime = EnvironmentEdgeManager.currentTime();
      return limit;
    }

    long delta = (limit * (now - nextRefillTime)) / super.getTimeUnitInMillis();
    if (delta > 0) {
      this.nextRefillTime = now;
      return Math.min(limit, delta);
    }
    return 0;
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    if (nextRefillTime == -1) {
      return 0;
    }
    long timeUnitInMillis = super.getTimeUnitInMillis();
    return ((amount * timeUnitInMillis) / limit) - ((available * timeUnitInMillis) / limit);
  }

  // This method is for strictly testing purpose only
  @VisibleForTesting
  public void setNextRefillTime(long nextRefillTime) {
    this.nextRefillTime = nextRefillTime;
  }

  @VisibleForTesting
  public long getNextRefillTime() {
    return this.nextRefillTime;
  }

}
