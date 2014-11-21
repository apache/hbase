package org.apache.hadoop.hbase.consensus.raft.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class Follower extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(Follower.class);

  public Follower(MutableRaftContext context) {
    super(RaftStateType.FOLLOWER, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);
    //assert c.isFollower();
  }
}
