package org.apache.hadoop.hbase.consensus.quorum;

public interface TimeoutEventHandler {
  void onTimeout();
}
