package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSession;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.VoteConsensusSession;
import org.apache.hadoop.hbase.consensus.quorum.VoteConsensusSessionInterface;

public class VoteNotCompleted implements Conditional {
  ImmutableRaftContext c;

  public VoteNotCompleted(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(Event e) {
    // Get the current outstanding election session
    VoteConsensusSessionInterface session =
      c.getElectionSession(this.c.getCurrentEdit());

    // Handle the stale request
    if (session == null) {
      return true;
    }

    return !session.isComplete();
  }
}

