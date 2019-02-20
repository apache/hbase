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

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

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

    private static final BackoffPolicy DEFAULT_BACKOFF_POLICY = new ExponentialBackoffPolicy();

    public RetryConfig() {
      maxAttempts    = 1;
      sleepInterval = 1000;
      maxSleepTime  = -1;
      timeUnit = TimeUnit.MILLISECONDS;
      backoffPolicy = DEFAULT_BACKOFF_POLICY;
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

    public BackoffPolicy getBackoffPolicy() {
      return backoffPolicy;
    }
  }

  /**
   * Policy for calculating sleeping intervals between retry attempts
   */
  public static class BackoffPolicy {
    public long getBackoffTime(RetryConfig config, int attempts) {
      return config.getSleepInterval();
    }
  }

  public static class ExponentialBackoffPolicy extends BackoffPolicy {
    @Override
    public long getBackoffTime(RetryConfig config, int attempts) {
      long backoffTime = (long) (config.getSleepInterval() * Math.pow(2, attempts));
      return backoffTime;
    }
  }

  public static class ExponentialBackoffPolicyWithLimit extends ExponentialBackoffPolicy {
    @Override
    public long getBackoffTime(RetryConfig config, int attempts) {
      long backoffTime = super.getBackoffTime(config, attempts);
      return config.getMaxSleepTime() > 0 ? Math.min(backoffTime, config.getMaxSleepTime()) : backoffTime;
    }
  }

  private static final Log LOG = LogFactory.getLog(RetryCounter.class);

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
   * @throws InterruptedException
   */
  public void sleepUntilNextRetry() throws InterruptedException {
    int attempts = getAttemptTimes();
    long sleepTime = retryConfig.backoffPolicy.getBackoffTime(retryConfig, attempts);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sleeping " + sleepTime + "ms before retry #" + attempts + "...");
    }
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
}
