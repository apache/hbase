package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when a write is attempted on a read-only HBase cluster.
 */
@InterfaceAudience.Public
public class WriteAttemptedOnReadOnlyClusterException extends DoNotRetryIOException {

  private static final long serialVersionUID = 1L;

  public WriteAttemptedOnReadOnlyClusterException() {
    super();
  }

  /**
   * @param message the message for this exception
   */
  public WriteAttemptedOnReadOnlyClusterException(String message) {
    super(message);
  }

  /**
   * @param message   the message for this exception
   * @param throwable the {@link Throwable} to use for this exception
   */
  public WriteAttemptedOnReadOnlyClusterException(String message, Throwable throwable) {
    super(message, throwable);
  }

  /**
   * @param throwable the {@link Throwable} to use for this exception
   */
  public WriteAttemptedOnReadOnlyClusterException(Throwable throwable) {
    super(throwable);
  }
}
