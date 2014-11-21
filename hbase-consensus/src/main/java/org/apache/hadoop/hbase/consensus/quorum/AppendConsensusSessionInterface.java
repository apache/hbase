package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;

public interface AppendConsensusSessionInterface extends ConsensusSession {
  AppendRequest getAppendRequest();
  boolean isTimeout();
  ReplicateEntriesEvent getReplicateEntriesEvent();
  void incrementAck(final EditId id, final String address, final int rank,
                    boolean canTakeover);
  void incrementHighTermCnt(final EditId id, final String address);
  void incrementLagCnt(final EditId id, final String address);
  void reset();
  void cancel();
}
