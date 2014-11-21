package org.apache.hadoop.hbase.consensus.server.peer.states;

import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;

/**
 * A {@link PeerServerState} which has async tasks.
 */
public class PeerServerAsyncState extends PeerServerState {
  public PeerServerAsyncState(final PeerServerStateType t,
                              PeerServerMutableContext c) {
    super(t, c);
  }

  @Override
  public boolean isAsyncState() {
    return true;
  }
}
