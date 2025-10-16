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
package org.apache.hadoop.hbase.quotas;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * With this limiter resources will be refilled only after a fixed interval of time.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FixedIntervalRateLimiter extends RateLimiter {

  /**
   * The FixedIntervalRateLimiter can be harsh from a latency/backoff perspective, which makes it
   * difficult to fully and consistently utilize a quota allowance. By configuring the
   * {@link #RATE_LIMITER_REFILL_INTERVAL_MS} to a lower value you will encourage the rate limiter
   * to throw smaller wait intervals for requests which may be fulfilled in timeframes shorter than
   * the quota's full interval. For example, if you're saturating a 100MB/sec read IO quota with a
   * ton of tiny gets, then configuring this to a value like 100ms will ensure that your retry
   * backoffs approach ~100ms, rather than 1sec. Be careful not to configure this too low, or you
   * may produce a dangerous amount of retry volume.
   */
  public static final String RATE_LIMITER_REFILL_INTERVAL_MS =
    "hbase.quota.rate.limiter.refill.interval.ms";

  private static final Logger LOG = LoggerFactory.getLogger(FixedIntervalRateLimiter.class);

  private long nextRefillTime = -1L;
  private final long refillInterval;

  public FixedIntervalRateLimiter() {
    this(DEFAULT_TIME_UNIT);
  }

  public FixedIntervalRateLimiter(long refillInterval) {
    super();
    long timeUnit = getTimeUnitInMillis();
    if (refillInterval > timeUnit) {
      LOG.warn(
        "Refill interval {} is larger than time unit {}. This is invalid. "
          + "Instead, we will use the time unit {} as the refill interval",
        refillInterval, timeUnit, timeUnit);
    }
    this.refillInterval = Math.min(timeUnit, refillInterval);
  }

  @Override
  public long refill(long limit) {
    final long now = EnvironmentEdgeManager.currentTime();
    if (nextRefillTime == -1) {
      nextRefillTime = now + refillInterval;
      return limit;
    }
    if (now < nextRefillTime) {
      return 0;
    }
    long diff = refillInterval + now - nextRefillTime;
    long refills = diff / refillInterval;
    nextRefillTime = now + refillInterval;
    long refillAmount = refills * getRefillIntervalAdjustedLimit(limit);
    return Math.min(limit, refillAmount);
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    // adjust the limit based on the refill interval
    limit = getRefillIntervalAdjustedLimit(limit);

    if (nextRefillTime == -1) {
      return 0;
    }
    final long now = EnvironmentEdgeManager.currentTime();
    final long refillTime = nextRefillTime;
    long diff = amount - available;
    // We will add limit at next interval. If diff is less than that limit, the wait interval
    // is just time between now and then.
    long nextRefillInterval = refillTime - now;
    if (diff <= limit) {
      return nextRefillInterval;
    }

    // Otherwise, we need to figure out how many refills are needed.
    // There will be one at nextRefillInterval, and then some number of extra refills.
    // Division will round down if not even, so we can just add that to our next interval
    long extraRefillsNecessary = diff / limit;
    // If it's even, subtract one since that will be covered by nextRefillInterval
    if (diff % limit == 0) {
      extraRefillsNecessary--;
    }
    return nextRefillInterval + (extraRefillsNecessary * refillInterval);
  }

  private long getRefillIntervalAdjustedLimit(long limit) {
    return (long) Math.ceil(refillInterval / (double) getTimeUnitInMillis() * limit);
  }

  // This method is for strictly testing purpose only
  @Override
  public void setNextRefillTime(long nextRefillTime) {
    this.nextRefillTime = nextRefillTime;
  }

  @Override
  public long getNextRefillTime() {
    return this.nextRefillTime;
  }
}
