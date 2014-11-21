package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.consensus.protocol.EditId;

public interface ConsensusSession {
  boolean isComplete();
  SessionResult getResult();
  EditId getSessionId();
}
