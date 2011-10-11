package org.apache.hadoop.hbase.util;

import java.util.concurrent.TimeUnit;

public class RetryCounterFactory {
  private final int maxRetries;
  private final int retryIntervalMillis;

  public RetryCounterFactory(int maxRetries, int retryIntervalMillis) {
    this.maxRetries = maxRetries;
    this.retryIntervalMillis = retryIntervalMillis;
  }

  public RetryCounter create() {
    return
      new RetryCounter(
        maxRetries, retryIntervalMillis, TimeUnit.MILLISECONDS
      );
  }
}
