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
 * An adaptive rate limiter that dynamically adjusts its behavior based on observed usage patterns
 * to achieve stable, full utilization of configured quota allowances while managing client
 * contention.
 * <p>
 * <b>Core Algorithm:</b> This rate limiter divides time into fixed refill intervals (configurable
 * via {@code hbase.quota.rate.limiter.refill.interval.ms}, default is 1 refill per TimeUnit of the
 * RateLimiter). At the beginning of each interval, a fresh allocation of resources becomes
 * available based on the configured limit. Clients consume resources as they make requests. When
 * resources are exhausted, clients must wait until the next refill, or until enough resources
 * become available.
 * <p>
 * <b>Adaptive Backpressure:</b> When multiple threads compete for limited resources (contention),
 * this limiter detects the contention and applies increasing backpressure by extending wait
 * intervals. This prevents thundering herd behavior where many threads wake simultaneously and
 * compete for the same resources. The backoff multiplier increases by a small increment (see
 * {@link #FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_INCREMENT}) per interval when contention occurs, and
 * decreases (see {@link #FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_DECREMENT}) when no contention is
 * detected, converging toward optimal throughput. The multiplier is capped at a maximum value (see
 * {@link #FEEDBACK_ADAPTIVE_MAX_BACKOFF_MULTIPLIER}) to prevent unbounded waits.
 * <p>
 * Contention is detected when {@link #getWaitInterval} is called with insufficient available
 * resources (i.e., {@code amount > available}), indicating a thread needs to wait for resources. If
 * this occurs more than once in a refill interval, the limiter identifies it as contention
 * requiring increased backpressure.
 * <p>
 * <b>Oversubscription for Full Utilization:</b> In practice, synchronization overhead and timing
 * variations often prevent clients from consuming exactly their full allowance, resulting in
 * consistent under-utilization. This limiter addresses this by tracking utilization via an
 * exponentially weighted moving average (EWMA). When average utilization falls below the target
 * range (determined by {@link #FEEDBACK_ADAPTIVE_UTILIZATION_ERROR_BUDGET}), the limiter gradually
 * increases the oversubscription proportion (see
 * {@link #FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_INCREMENT}), allowing more resources per interval than
 * the base limit. Conversely, when utilization exceeds the target range, oversubscription is
 * decreased (see {@link #FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_DECREMENT}). Oversubscription is capped
 * (see {@link #FEEDBACK_ADAPTIVE_MAX_OVERSUBSCRIPTION}) to prevent excessive bursts while still
 * enabling consistent full utilization.
 * <p>
 * <b>Example Scenario:</b> Consider a quota of 1000 requests per second with a 1-second refill
 * interval. Without oversubscription, clients might typically achieve only 950 req/s due to
 * coordination delays. This limiter would detect the under-utilization, gradually increase
 * oversubscription, allowing slightly more resources per interval, which compensates for
 * inefficiencies and achieves stable throughput closer to the configured quota. If multiple threads
 * simultaneously try to consume resources and repeatedly wait, the backoff multiplier increases
 * their wait times, spreading out their retry attempts and reducing wasted CPU cycles.
 * <p>
 * <b>Configuration Parameters:</b>
 * <ul>
 * <li>{@link #FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_INCREMENT}: Controls rate of backpressure
 * increase</li>
 * <li>{@link #FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_DECREMENT}: Controls rate of backpressure
 * decrease</li>
 * <li>{@link #FEEDBACK_ADAPTIVE_MAX_BACKOFF_MULTIPLIER}: Caps the maximum wait time extension</li>
 * <li>{@link #FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_INCREMENT}: Controls rate of oversubscription
 * increase</li>
 * <li>{@link #FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_DECREMENT}: Controls rate of oversubscription
 * decrease</li>
 * <li>{@link #FEEDBACK_ADAPTIVE_MAX_OVERSUBSCRIPTION}: Caps the maximum burst capacity</li>
 * <li>{@link #FEEDBACK_ADAPTIVE_UTILIZATION_ERROR_BUDGET}: Defines the acceptable range around full
 * utilization</li>
 * </ul>
 * <p>
 * This algorithm converges toward stable operation where: (1) wait intervals are just long enough
 * to prevent excessive contention, and (2) oversubscription is just high enough to achieve
 * consistent full utilization of the configured allowance.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FeedbackAdaptiveRateLimiter extends RateLimiter {

  /**
   * Amount to increase the backoff multiplier when contention is detected per refill interval. In
   * other words, if we are throttling more than once per refill interval, then we will increase our
   * wait intervals (increase backpressure, decrease throughput).
   */
  public static final String FEEDBACK_ADAPTIVE_BACKOFF_MULTIPLIER_INCREMENT =
    "hbase.quota.rate.limiter.feedback.adaptive.backoff.multiplier.increment";
  public static final double DEFAULT_BACKOFF_MULTIPLIER_INCREMENT = 0.0005;

  /**
   * Amount to decrease the backoff multiplier when no contention is detected per refill interval.
   * In other words, if we are only throttling once per refill interval, then we will decrease our
   * wait interval (decrease backpressure, increase throughput).
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
   * Amount to increase the oversubscription proportion when utilization is below (1.0-errorBudget).
   */
  public static final String FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_INCREMENT =
    "hbase.quota.rate.limiter.feedback.adaptive.oversubscription.increment";
  public static final double DEFAULT_OVERSUBSCRIPTION_INCREMENT = 0.001;

  /**
   * Amount to decrease the oversubscription proportion when utilization exceeds (1.0+errorBudget).
   */
  public static final String FEEDBACK_ADAPTIVE_OVERSUBSCRIPTION_DECREMENT =
    "hbase.quota.rate.limiter.feedback.adaptive.oversubscription.decrement";
  public static final double DEFAULT_OVERSUBSCRIPTION_DECREMENT = 0.00005;

  /**
   * Maximum ceiling for oversubscription to prevent unbounded bursts. Some oversubscription can be
   * nice, because it allows you to balance the inefficiency and latency of retries, landing on
   * stable usage at approximately your configured allowance. Without adequate oversubscription,
   * your steady state may often seem significantly, and suspiciously, lower than your configured
   * allowance.
   */
  public static final String FEEDBACK_ADAPTIVE_MAX_OVERSUBSCRIPTION =
    "hbase.quota.rate.limiter.feedback.adaptive.max.oversubscription";
  public static final double DEFAULT_MAX_OVERSUBSCRIPTION = 0.25;

  /**
   * Acceptable deviation around full utilization (1.0) for adjusting oversubscription. If stable
   * throttle usage is typically under (1.0-errorBudget), then we will allow more oversubscription.
   * If stable throttle usage is typically over (1.0+errorBudget), then we will pull back
   * oversubscription.
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

  private volatile long nextRefillTime = -1L;
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
    if (nextRefillTime == -1) {
      return 0;
    }

    final long now = EnvironmentEdgeManager.currentTime();
    final long refillTime = nextRefillTime;
    long diff = amount - available;
    if (diff > 0) {
      hadContentionThisInterval = true;
    }

    long nextInterval = refillTime - now;
    if (diff <= limit) {
      return applyBackoffMultiplier(nextInterval);
    }

    long extra = diff / limit;
    if (diff % limit == 0) {
      extra--;
    }
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
