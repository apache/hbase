package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoQuorumMembershipChangeInProgress implements Conditional {
  private static Logger LOG = LoggerFactory.getLogger(AppendNotCompleted.class);

  ImmutableRaftContext c;

  public NoQuorumMembershipChangeInProgress(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(Event e) {
    return c.getUpdateMembershipRequest() == null;
  }
}

