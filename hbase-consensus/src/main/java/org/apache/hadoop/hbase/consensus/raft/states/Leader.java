package org.apache.hadoop.hbase.consensus.raft.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class Leader extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(Leader.class);

  public Leader(MutableRaftContext context) {
    super(RaftStateType.LEADER, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);
    assert c.isLeader();
  }
}
