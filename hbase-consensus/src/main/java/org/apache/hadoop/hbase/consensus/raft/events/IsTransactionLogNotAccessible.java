package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;

public class IsTransactionLogNotAccessible implements Conditional {
  private ImmutableRaftContext context;

  public IsTransactionLogNotAccessible(final ImmutableRaftContext context) {
    this.context = context;
  }

  @Override
  public boolean isMet(Event e) {
    return !context.isLogAccessible();
  }
}

