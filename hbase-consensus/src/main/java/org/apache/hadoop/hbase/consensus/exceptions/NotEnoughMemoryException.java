package org.apache.hadoop.hbase.consensus.exceptions;

import java.io.IOException;

public class NotEnoughMemoryException extends IOException {

  final int requiredBytes;
  public NotEnoughMemoryException(String msg, int bytes) {
    super(msg);
    this.requiredBytes = bytes;
  }

  public int getRequiredBytes() {
    return requiredBytes;
  }
}
