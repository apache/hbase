package org.apache.hadoop.hbase.consensus.exceptions;

import org.apache.hadoop.hbase.regionserver.RegionOverloadedException;

public class CommitQueueOverloadedException extends RegionOverloadedException {
  public CommitQueueOverloadedException(final String msg, long waitMillis) {
    super(msg, waitMillis);
  }
}
