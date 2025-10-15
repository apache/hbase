package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * When a fatal issue is encountered during an RPC which is not retriable, and the issue
 * is encountered deep in a call stack where throwing a checked DoNotRetryIOException is not
 * possible (e.g filter/comparator application), this unchecked exception can be thrown
 * and will be wrapped in a checked DoNotRetryIOException at the RPC boundary before returning
 * to the client to prevent client retries and to propagate the exception message cleanly.
 * You should use this exception only when absolutely necessary. Wherever possible, use a checked
 * DoNotRetryIOException
 */
@InterfaceAudience.Private
public class DoNotRetryRuntimeException extends RuntimeException {
  public DoNotRetryRuntimeException(String message) {
    super(message);
  }
}
