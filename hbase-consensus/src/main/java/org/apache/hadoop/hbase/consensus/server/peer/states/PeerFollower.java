package org.apache.hadoop.hbase.consensus.server.peer.states;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;

public class PeerFollower extends PeerServerState {
  private static Logger LOG = LoggerFactory.getLogger(PeerFollower.class);

  public PeerFollower(PeerServerMutableContext context) {
    super(PeerServerStateType.PEER_FOLLOWER, context);
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);
    // TODO
  }
}
