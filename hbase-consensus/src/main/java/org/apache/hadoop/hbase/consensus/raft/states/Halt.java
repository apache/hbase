package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class Halt extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(Halt.class);

  public Halt(MutableRaftContext context) {
    super(RaftStateType.HALT, context);
  }

  public void onEntry(final Event e) {
    LOG.error("Entering HALT state " + c + " Current State: " + t + ", OnEntryEvent: " + e);
    super.onEntry(e);
    c.clearLeader();
    c.leaderStepDown();
    c.candidateStepDown();
    c.getProgressTimer().stop();
    c.getHeartbeatTimer().stop();
    c.getConsensusMetrics().setRaftState(PeerStatus.RAFT_STATE.HALT);
  }
}
