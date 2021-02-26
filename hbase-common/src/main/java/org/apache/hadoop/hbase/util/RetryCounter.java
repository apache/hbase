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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Operation retry accounting.
 * Use to calculate wait period, {@link #getBackoffTimeAndIncrementAttempts()}}, or for performing
 * wait, {@link #sleepUntilNextRetry()}, in accordance with a {@link RetryConfig}, initial
 * settings, and a Retry Policy, (See org.apache.hadoop.io.retry.RetryPolicy).
 * Like <a href=https://github.com/rholder/guava-retrying>guava-retrying</a>.
 * @since 0.92.0
 * @see RetryCounterFactory
 */
@InterfaceAudience.Private
public class RetryCounter {
  /**
   *  Configuration for a retry counter
   */
  public static class RetryConfig {
    private int maxAttempts;
    private long sleepInterval;
    private long maxSleepTime;
    private TimeUnit timeUnit;
    private BackoffPolicy backoffPolicy;
    private float jitter;

    private static final BackoffPolicy DEFAULT_BACKOFF_POLICY = new ExponentialBackoffPolicy();

    public RetryConfig() {
      maxAttempts    = 1;
      sleepInterval = 1000;
      maxSleepTime  = -1;
      timeUnit = TimeUnit.MILLISECONDS;
      backoffPolicy = DEFAULT_BACKOFF_POLICY;
      jitter = 0.0f;
    }

    public RetryConfig(int maxAttempts, long sleepInterval, long maxSleepTime,
        TimeUnit timeUnit, BackoffPolicy backoffPolicy) {
      this.maxAttempts = maxAttempts;
      this.sleepInterval = sleepInterval;
      this.maxSleepTime = maxSleepTime;
      this.timeUnit = timeUnit;
      this.backoffPolicy = backoffPolicy;
    }

    public RetryConfig setBackoffPolicy(BackoffPolicy backoffPolicy) {
      this.backoffPolicy = backoffPolicy;
      return this;
    }

    public RetryConfig setMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public RetryConfig setMaxSleepTime(long maxSleepTime) {
      this.maxSleepTime = maxSleepTime;
      return this;
    }

    public RetryConfig setSleepInterval(long sleepInterval) {
      this.sleepInterval = sleepInterval;
      return this;
    }

    public RetryConfig setTimeUnit(TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
      return this;
    }

    public RetryConfig setJitter(float jitter) {
      Preconditions.checkArgument(jitter >= 0.0f && jitter < 1.0f,
        "Invalid jitter: %s, should be in range [0.0, 1.0)", jitter);
      this.jitter = jitter;
      return this;
    }

    public int getMaxAttempts() {
      return maxAttempts;
    }

    public long getMaxSleepTime() {
      return maxSleepTime;
    }

    public long getSleepInterval() {
      return sleepInterval;
    }

    public TimeUnit getTimeUnit() {
      return timeUnit;
    }

    public float getJitter() {
      return jitter;
    }

    public BackoffPolicy getBackoffPolicy() {
      return backoffPolicy;
    }
  }

  private static long addJitter(long interval, float jitter) {
    long jitterInterval = (long) (interval * ThreadLocalRandom.current().nextFloat() * jitter);
    return interval + jitterInterval;
  }

  /**
   * Policy for calculating sleeping intervals between retry attempts
   */
  public static class BackoffPolicy {
    public long getBackoffTime(RetryConfig config, int attempts) {
      return addJitter(config.getSleepInterval(), config.getJitter());
    }
  }

  public static class ExponentialBackoffPolicy extends BackoffPolicy {
    @Override
    public long getBackoffTime(RetryConfig config, int attempts) {
      long backoffTime = (long) (config.getSleepInterval() * Math.pow(2, attempts));
      return addJitter(backoffTime, config.getJitter());
    }
  }

  public static class ExponentialBackoffPolicyWithLimit extends ExponentialBackoffPolicy {
    @Override
    public long getBackoffTime(RetryConfig config, int attempts) {
      long backoffTime = super.getBackoffTime(config, attempts);
      return config.getMaxSleepTime() > 0 ? Math.min(backoffTime, config.getMaxSleepTime()) : backoffTime;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(RetryCounter.class);

  private RetryConfig retryConfig;
  private int attempts;

  public RetryCounter(int maxAttempts, long sleepInterval, TimeUnit timeUnit) {
    this(new RetryConfig(maxAttempts, sleepInterval, -1, timeUnit, new ExponentialBackoffPolicy()));
  }

  public RetryCounter(RetryConfig retryConfig) {
    this.attempts = 0;
    this.retryConfig = retryConfig;
  }

  public int getMaxAttempts() {
    return retryConfig.getMaxAttempts();
  }

  /**
   * Sleep for a back off time as supplied by the backoff policy, and increases the attempts
   */
  public void sleepUntilNextRetry() throws InterruptedException {
    int attempts = getAttemptTimes();
    long sleepTime = getBackoffTime();
    LOG.trace("Sleeping {} ms before retry #{}...", sleepTime, attempts);
    retryConfig.getTimeUnit().sleep(sleepTime);
    useRetry();
  }

  public boolean shouldRetry() {
    return attempts < retryConfig.getMaxAttempts();
  }

  public void useRetry() {
    attempts++;
  }

  public boolean isRetry() {
    return attempts > 0;
  }

  public int getAttemptTimes() {
    return attempts;
  }

  public long getBackoffTime() {
    return this.retryConfig.backoffPolicy.getBackoffTime(this.retryConfig, getAttemptTimes());
  }

  public long getBackoffTimeAndIncrementAttempts() {
    long backoffTime = getBackoffTime();
    useRetry();
    return backoffTime;
  }
}
