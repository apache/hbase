package org.apache.hadoop.hbase.consensus.raft.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class Candidate extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(Follower.class);

  public Candidate(MutableRaftContext context) {
    super(RaftStateType.CANDIDATE, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);
    assert c.isCandidate();
  }
}
