package org.apache.hadoop.hbase.consensus.exceptions;

import java.io.IOException;

public class LeaderNotReadyException extends IOException {
  public LeaderNotReadyException(String msg) {
    super(msg);
  }
}
