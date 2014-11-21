package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.*;

public class VoteSucceeded implements Conditional {
  ImmutableRaftContext c;

  public VoteSucceeded(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(Event e) {
    // Get the current outstanding election session
    VoteConsensusSessionInterface session =
      c.getElectionSession(this.c.getCurrentEdit());

    // Handle the stale request
    if (session == null) {
      return false;
    }

    // return true if the majority has sent the Ack response
    return session.getResult().equals(SessionResult.MAJORITY_ACKED);
  }
}

