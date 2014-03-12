package org.apache.hadoop.hbase.util;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;

public class RetryCounter {
  private static final Log LOG = LogFactory.getLog(RetryCounter.class);
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

  /**
   * Sleep for a exponentially back off time
   * @throws InterruptedException
   */
  public void sleepUntilNextRetry() throws InterruptedException {
    int attempts = getAttemptTimes();
    long sleepTime = (long) (retryIntervalMillis * Math.pow(2, attempts));
    LOG.info("The " + attempts + " times to retry  after sleeping " + sleepTime 
        + " ms");
    timeUnit.sleep(sleepTime);
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