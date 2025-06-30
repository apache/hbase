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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AtomicDouble;

/**
 * This rate limiter works much like the FixedIntervalRateLimiter, except that:
 * <ol>
 * <li>it will increase backpressure on multithreaded clients</li>
 * <li>it will allow over-subscription to hit consistent, full allowance utilization</li>
 * </ol>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FeedbackAdaptiveRateLimiter extends RateLimiter {

  /**
   * Amount to increase the backoff multiplier when contention is detected per refill interval.
   */
  public static final String FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_INCREMENT =
    "hbase.quota.rate.limiter.feedback.adaptive.backoff.multiplier.increment";
  public static final double DEFAULT_BACKOFF_MULTIPLIER_INCREMENT = 0.0001;

  /**
   * Amount to decrease the backoff multiplier when no contention is detected per refill interval.
   */
  public static final String FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_DECREMENT =
    "hbase.quota.rate.limiter.feedback.adaptive.backoff.multiplier.decrement";
  public static final double DEFAULT_BACKOFF_MULTIPLIER_DECREMENT = 0.0001;

  /**
   * Maximum ceiling for the backoff multiplier to avoid unbounded waits.
   */
  public static final String FEEDBACK_ADAPTIVE_MAX_BACKOFF_MULTIPLIER =
    "hbase.quota.rate.limiter.feedback.adaptive.max.backoff.multiplier";
  public static final double DEFAULT_MAX_BACKOFF_MULTIPLIER = 10.0;

  /**
   * Amount to increase the oversubscription proportion when utilization is below target.
   */
  public static final String FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_INCREMENT =
    "hbase.quota.rate.limiter.feedback.adaptive.oversubscription.increment";
  public static final double DEFAULT_OVERSUBSCRIPTION_INCREMENT = 0.00001;

  /**
   * Amount to decrease the oversubscription proportion when utilization exceeds target.
   */
  public static final String FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_DECREMENT =
    "hbase.quota.rate.limiter.feedback.adaptive.oversubscription.decrement";
  public static final double DEFAULT_OVERSUBSCRIPTION_DECREMENT = 0.00001;

  /**
   * Maximum ceiling for oversubscription to prevent unbounded bursts.
   */
  public static final String FEEDBACK_ADAPTIVE_MAX_OVERSUBSCRIPTION =
    "hbase.quota.rate.limiter.feedback.adaptive.max.oversubscription";
  public static final double DEFAULT_MAX_OVERSUBSCRIPTION = 0.05;

  /**
   * Acceptable deviation around full utilization (1.0) for adjusting oversubscription.
   */
  public static final String FEEDBACK_ADAPTIVE_UTILIZATION_ERROR_BUDGET =
    "hbase.quota.rate.limiter.feedback.adaptive.utilization.error.budget";
  public static final double DEFAULT_UTILIZATION_ERROR_BUDGET = 0.025;

  private static final int WINDOW_TIME_MS = 60_000;

  public static class FeedbackAdaptiveRateLimiterFactory {

    private final long refillInterval;
    private final double backoffMultiplierIncrement;
    private final double backoffMultiplierDecrement;
    private final double maxBackoffMultiplier;
    private final double oversubscriptionIncrement;
    private final double oversubscriptionDecrement;
    private final double maxOversubscription;
    private final double utilizationErrorBudget;

    public FeedbackAdaptiveRateLimiterFactory(Configuration conf) {
      refillInterval = conf.getLong(FixedIntervalRateLimiter.RATE_LIMITER_REFILL_INTERVAL_MS,
        RateLimiter.DEFAULT_TIME_UNIT);

      maxBackoffMultiplier =
        conf.getDouble(FEEDBACK_ADAPTIVE_MAX_BACKOFF_MULTIPLIER, DEFAULT_MAX_BACKOFF_MULTIPLIER);

      backoffMultiplierIncrement = conf.getDouble(FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_INCREMENT,
        DEFAULT_BACKOFF_MULTIPLIER_INCREMENT);
      backoffMultiplierDecrement = conf.getDouble(FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_DECREMENT,
        DEFAULT_BACKOFF_MULTIPLIER_DECREMENT);

      oversubscriptionIncrement = conf.getDouble(FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_INCREMENT,
        DEFAULT_OVERSUBSCRIPTION_INCREMENT);
      oversubscriptionDecrement = conf.getDouble(FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_DECREMENT,
        DEFAULT_OVERSUBSCRIPTION_DECREMENT);

      maxOversubscription =
        conf.getDouble(FEEDBACK_ADAPTIVE_MAX_OVERSUBSCRIPTION, DEFAULT_MAX_OVERSUBSCRIPTION);
      utilizationErrorBudget = conf.getDouble(FEEDBACK_ADAPTIVE_UTILIZATION_ERROR_BUDGET,
        DEFAULT_UTILIZATION_ERROR_BUDGET);
    }

    public FeedbackAdaptiveRateLimiter create() {
      return new FeedbackAdaptiveRateLimiter(refillInterval, backoffMultiplierIncrement,
        backoffMultiplierDecrement, maxBackoffMultiplier, oversubscriptionIncrement,
        oversubscriptionDecrement, maxOversubscription, utilizationErrorBudget);
    }
  }

  private long nextRefillTime = -1L;
  private final long refillInterval;
  private final double backoffMultiplierIncrement;
  private final double backoffMultiplierDecrement;
  private final double maxBackoffMultiplier;
  private final double oversubscriptionIncrement;
  private final double oversubscriptionDecrement;
  private final double maxOversubscription;
  private final double minTargetUtilization;
  private final double maxTargetUtilization;

  // Adaptive backoff state
  private final AtomicDouble currentBackoffMultiplier = new AtomicDouble(1.0);
  private volatile boolean hadContentionThisInterval = false;

  // Over-subscription proportion state
  private final AtomicDouble oversubscriptionProportion = new AtomicDouble(0.0);

  // EWMA tracking
  private final double emaAlpha;
  private volatile double utilizationEma = 0.0;
  private final AtomicLong lastIntervalConsumed;

  FeedbackAdaptiveRateLimiter(long refillInterval, double backoffMultiplierIncrement,
    double backoffMultiplierDecrement, double maxBackoffMultiplier,
    double oversubscriptionIncrement, double oversubscriptionDecrement, double maxOversubscription,
    double utilizationErrorBudget) {
    super();
    Preconditions.checkArgument(getTimeUnitInMillis() >= refillInterval, String.format(
      "Refill interval %s must be ≤ TimeUnit millis %s", refillInterval, getTimeUnitInMillis()));

    Preconditions.checkArgument(backoffMultiplierIncrement > 0.0,
      String.format("Backoff multiplier increment %s must be > 0.0", backoffMultiplierIncrement));
    Preconditions.checkArgument(backoffMultiplierDecrement > 0.0,
      String.format("Backoff multiplier decrement %s must be > 0.0", backoffMultiplierDecrement));
    Preconditions.checkArgument(maxBackoffMultiplier > 1.0,
      String.format("Max backoff multiplier %s must be > 1.0", maxBackoffMultiplier));
    Preconditions.checkArgument(utilizationErrorBudget > 0.0 && utilizationErrorBudget <= 1.0,
      String.format("Utilization error budget %s must be between 0.0 and 1.0",
        utilizationErrorBudget));

    this.refillInterval = refillInterval;
    this.backoffMultiplierIncrement = backoffMultiplierIncrement;
    this.backoffMultiplierDecrement = backoffMultiplierDecrement;
    this.maxBackoffMultiplier = maxBackoffMultiplier;
    this.oversubscriptionIncrement = oversubscriptionIncrement;
    this.oversubscriptionDecrement = oversubscriptionDecrement;
    this.maxOversubscription = maxOversubscription;
    this.minTargetUtilization = 1.0 - utilizationErrorBudget;
    this.maxTargetUtilization = 1.0 + utilizationErrorBudget;

    this.emaAlpha = refillInterval / (double) (WINDOW_TIME_MS + refillInterval);
    this.lastIntervalConsumed = new AtomicLong(0);
  }

  @Override
  public long refill(long limit) {
    final long now = EnvironmentEdgeManager.currentTime();
    if (nextRefillTime == -1) {
      nextRefillTime = now + refillInterval;
      hadContentionThisInterval = false;
      return getOversubscribedLimit(limit);
    }
    if (now < nextRefillTime) {
      return 0;
    }
    long diff = refillInterval + now - nextRefillTime;
    long refills = diff / refillInterval;
    nextRefillTime = now + refillInterval;

    long intendedUsage = getRefillIntervalAdjustedLimit(limit);
    if (intendedUsage > 0) {
      long consumed = lastIntervalConsumed.get();
      if (consumed > 0) {
        double util = (double) consumed / intendedUsage;
        utilizationEma = emaAlpha * util + (1.0 - emaAlpha) * utilizationEma;
      }
    }

    if (hadContentionThisInterval) {
      currentBackoffMultiplier.set(Math
        .min(currentBackoffMultiplier.get() + backoffMultiplierIncrement, maxBackoffMultiplier));
    } else {
      currentBackoffMultiplier
        .set(Math.max(currentBackoffMultiplier.get() - backoffMultiplierDecrement, 1.0));
    }

    double avgUtil = utilizationEma;
    if (avgUtil < minTargetUtilization) {
      oversubscriptionProportion.set(Math
        .min(oversubscriptionProportion.get() + oversubscriptionIncrement, maxOversubscription));
    } else if (avgUtil >= maxTargetUtilization) {
      oversubscriptionProportion
        .set(Math.max(oversubscriptionProportion.get() - oversubscriptionDecrement, 0.0));
    }

    hadContentionThisInterval = false;
    lastIntervalConsumed.set(0);

    long refillAmount = refills * getRefillIntervalAdjustedLimit(limit);
    long maxRefill = getOversubscribedLimit(limit);
    return Math.min(maxRefill, refillAmount);
  }

  private long getOversubscribedLimit(long limit) {
    return limit + (long) (limit * oversubscriptionProportion.get());
  }

  @Override
  public void consume(long amount) {
    super.consume(amount);
    lastIntervalConsumed.addAndGet(amount);
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    limit = getRefillIntervalAdjustedLimit(limit);
    if (nextRefillTime == -1) return 0;

    final long now = EnvironmentEdgeManager.currentTime();
    final long refillTime = nextRefillTime;
    long diff = amount - available;
    if (diff > 0) hadContentionThisInterval = true;

    long nextInterval = refillTime - now;
    if (diff <= limit) {
      return applyBackoffMultiplier(nextInterval);
    }

    long extra = diff / limit;
    if (diff % limit == 0) extra--;
    long baseWait = nextInterval + (extra * refillInterval);
    return applyBackoffMultiplier(baseWait);
  }

  private long getRefillIntervalAdjustedLimit(long limit) {
    return (long) Math.ceil(refillInterval / (double) getTimeUnitInMillis() * limit);
  }

  private long applyBackoffMultiplier(long baseWaitInterval) {
    return (long) (baseWaitInterval * currentBackoffMultiplier.get());
  }

  // strictly for testing
  @Override
  public void setNextRefillTime(long nextRefillTime) {
    this.nextRefillTime = nextRefillTime;
  }

  @Override
  public long getNextRefillTime() {
    return this.nextRefillTime;
  }
}
