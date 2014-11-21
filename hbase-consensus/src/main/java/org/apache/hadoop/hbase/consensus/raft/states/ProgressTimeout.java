package org.apache.hadoop.hbase.consensus.raft.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class ProgressTimeout extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(ProgressTimeout.class);

  public ProgressTimeout(MutableRaftContext context) {
    super(RaftStateType.PROGRESS_TIMEOUT, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);
  }
}
