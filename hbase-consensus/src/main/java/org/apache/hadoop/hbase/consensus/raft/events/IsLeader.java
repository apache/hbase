package org.apache.hadoop.hbase.consensus.raft.events;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;

public class IsLeader implements Conditional {
  private static Logger LOG = LoggerFactory.getLogger(IsLeader.class);

  ImmutableRaftContext c;

  public IsLeader(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(Event e) {
    return c.isLeader();
  }
}
