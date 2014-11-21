package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSessionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSession;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;

public class AppendNotCompleted implements Conditional {
  private static Logger LOG = LoggerFactory.getLogger(AppendNotCompleted.class);

  ImmutableRaftContext c;

  public AppendNotCompleted(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(Event e) {
    AppendConsensusSessionInterface session = c.getAppendSession(c.getCurrentEdit());

    // Handle the stale request
    if (session == null) {
      return true;
    }

    return !session.isComplete();
  }
}

