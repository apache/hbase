package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSession;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSessionInterface;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.SessionResult;

public class AppendRetry implements Conditional {
  ImmutableRaftContext c;

  public AppendRetry(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(Event e) {
    AppendConsensusSessionInterface session = c.getAppendSession(c.getCurrentEdit());

    // Handle the stale request
    if (session == null) {
      return false;
    }

    return session.getResult().equals(SessionResult.RETRY);
  }
}
