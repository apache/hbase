package org.apache.hadoop.hbase.consensus.rmap;

public class RMapException extends Exception {
  public RMapException(final String message) {
    super(message);
  }

  public RMapException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
