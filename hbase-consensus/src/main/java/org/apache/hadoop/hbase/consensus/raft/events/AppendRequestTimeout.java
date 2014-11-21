package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSessionInterface;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;

public class AppendRequestTimeout implements Conditional {
  final ImmutableRaftContext c;

  public AppendRequestTimeout(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(final Event e) {
    AppendConsensusSessionInterface appendSession =
            c.getOutstandingAppendSession();
    if (appendSession != null) {
      return appendSession.isTimeout();
    }
    return false;
  }
}
