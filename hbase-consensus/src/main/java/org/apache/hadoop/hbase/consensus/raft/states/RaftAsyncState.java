package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

/**
 * A {@link RaftState} which has async tasks.
 */
public class RaftAsyncState extends RaftState {
  public RaftAsyncState(final RaftStateType t, MutableRaftContext c) {
    super(t, c);
  }

  @Override
  public boolean isAsyncState() {
    return true;
  }
}
