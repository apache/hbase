package org.apache.hadoop.hbase.consensus.raft.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.fsm.State;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class RaftState extends State {
  private static Logger LOG = LoggerFactory.getLogger(RaftState.class);

  protected MutableRaftContext c;
  RaftState(final RaftStateType t, MutableRaftContext c) {
    super(t);
    this.c = c;
  }

  public void onEntry(final Event e) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(c + " Current State: " + t + ", OnEntryEvent: " + e);
    }
  }

  public void onExit(final Event e) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(c + " Current State: " + t + ", OnExitEvent: " + e);
    }
  }
}
