package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;

public interface VoteConsensusSessionInterface extends ConsensusSession {
  void incrementAck(final EditId id, final String address);
  void incrementNack(final EditId id, final String address);
  VoteRequest getRequest();
  void setVoteSessionFailed(final EditId id);
}
