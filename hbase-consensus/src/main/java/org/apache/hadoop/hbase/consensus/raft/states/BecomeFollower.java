package org.apache.hadoop.hbase.consensus.raft.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class BecomeFollower extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(BecomeFollower.class);

  public BecomeFollower(MutableRaftContext context) {
    super(RaftStateType.BECOME_FOLLOWER, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);

    c.leaderStepDown();
    c.candidateStepDown();

    assert c.getOutstandingAppendSession() == null ;
    assert c.getOutstandingElectionSession() == null ;

    c.getHeartbeatTimer().stop();
    c.getProgressTimer().start();
  }
}
