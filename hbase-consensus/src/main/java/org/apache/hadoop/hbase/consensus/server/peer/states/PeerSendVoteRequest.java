package org.apache.hadoop.hbase.consensus.server.peer.states;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;

import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerVoteRequestEvent;

public class PeerSendVoteRequest extends PeerServerState {
  private static Logger LOG = LoggerFactory.getLogger(PeerSendVoteRequest.class);

  public PeerSendVoteRequest(PeerServerMutableContext context) {
    super(PeerServerStateType.SEND_VOTE_REQUEST, context);
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);
    PeerVoteRequestEvent voteRequestEvent = (PeerVoteRequestEvent)e;
    this.c.sendVoteRequestWithCallBack(voteRequestEvent.getVoteRequest());
  }
}
