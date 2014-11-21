package org.apache.hadoop.hbase.consensus.quorum;

public enum SessionResult {
  MAJORITY_ACKED,
  STEP_DOWN,
  RETRY,
  NOT_COMPLETED,
  CANCELED
}
