/**
 *
 */
package org.apache.hadoop.hbase.client;

import java.net.ConnectException;

/**
 * Thrown when the client believes that we are trying to communicate to has
 * been repeatedly unresponsive for a while.
 *
 * On receiving such an exception. The HConnectionManager will skip all
 * retries and fast fail the operation.
 */
public class PreemptiveFastFailException extends ConnectException {
  private static final long serialVersionUID = 7129103682617007177L;
  private long failureCount, timeOfFirstFailureMilliSec, timeOfLatestAttemptMilliSec;

  /**
   * @param count
   * @param timeOfFirstFailureMilliSec
   * @param timeOfLatestAttemptMilliSec
   * @param serverName
   */
  public PreemptiveFastFailException(long count, long timeOfFirstFailureMilliSec,
      long timeOfLatestAttemptMilliSec, String serverName) {
    super("Exception happened " + count + " times. to" + serverName);
    this.failureCount = count;
    this.timeOfFirstFailureMilliSec = timeOfFirstFailureMilliSec;
    this.timeOfLatestAttemptMilliSec = timeOfLatestAttemptMilliSec;
  }

  public long getFirstFailureAt() {
    return timeOfFirstFailureMilliSec;
  }

  public long getLastAttemptAt() {
    return timeOfLatestAttemptMilliSec;
  }

  public long getFailureCount() {
    return failureCount;
  }

  public boolean wasOperationAttemptedByServer() {
    return false;
  }
}
