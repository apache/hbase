package org.apache.hadoop.hbase.consensus.server.peer.states;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;

public class PeerHandleVoteResponse extends PeerServerState {
  private static Logger LOG = LoggerFactory.getLogger(PeerHandleVoteResponse.class);

  public PeerHandleVoteResponse(PeerServerMutableContext context) {
    super(PeerServerStateType.HANDLE_VOTE_RESPONSE, context);
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);
    // TODO
  }
}
