package org.apache.hadoop.hbase.util;

import java.util.concurrent.TimeUnit;

public class RetryCounter {
  private final int maxRetries;
  private int retriesRemaining;
  private final int retryIntervalMillis;
  private final TimeUnit timeUnit;

  public RetryCounter(int maxRetries,
  int retryIntervalMillis, TimeUnit timeUnit) {
    this.maxRetries = maxRetries;
    this.retriesRemaining = maxRetries;
    this.retryIntervalMillis = retryIntervalMillis;
    this.timeUnit = timeUnit;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void sleepUntilNextRetry() throws InterruptedException {
    timeUnit.sleep(retryIntervalMillis);
  }

  public boolean shouldRetry() {
    return retriesRemaining > 0;
  }

  public void useRetry() {
    retriesRemaining--;
  }

  public int getAttemptTimes() {
    return maxRetries-retriesRemaining+1;
  }
}