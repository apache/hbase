package org.apache.hadoop.hbase.util;

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;

/**
 * This class handles the different interruption classes.
 * It can be:
 * - InterruptedException
 * - InterruptedIOException (inherits IOException); used in IO
 * - ClosedByInterruptException (inherits IOException)
 * , - SocketTimeoutException inherits InterruptedIOException but is not a real
 * interruption, so we have to distinguish the case. This pattern is unfortunately common.
 */
public class ExceptionUtil {

  /**
   * @return true if the throwable comes an interruption, false otherwise.
   */
  public static boolean isInterrupt(Throwable t) {
    if (t instanceof InterruptedException) return true;
    if (t instanceof SocketTimeoutException) return false;
    return (t instanceof InterruptedIOException);
  }

  /**
   * @throws InterruptedIOException if t was an interruption. Does nothing otherwise.
   */
  public static void rethrowIfInterrupt(Throwable t) throws InterruptedIOException {
    InterruptedIOException iie = asInterrupt(t);
    if (iie != null) throw iie;
  }

  /**
   * @return an InterruptedIOException if t was an interruption, null otherwise
   */
  public static InterruptedIOException asInterrupt(Throwable t) {
    if (t instanceof SocketTimeoutException) return null;

    if (t instanceof InterruptedIOException) return (InterruptedIOException) t;

    if (t instanceof InterruptedException) {
      InterruptedIOException iie = new InterruptedIOException();
      iie.initCause(t);
      return iie;
    }

    return null;
  }

  private ExceptionUtil() {
  }
}
