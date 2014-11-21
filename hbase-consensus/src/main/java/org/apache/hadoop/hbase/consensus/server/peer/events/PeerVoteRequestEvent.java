package org.apache.hadoop.hbase.consensus.server.peer.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;

public class PeerVoteRequestEvent extends Event {

  private final VoteRequest request;

  public PeerVoteRequestEvent(VoteRequest request) {
    super(PeerServerEventType.PEER_VOTE_REQUEST_RECEIVED);
    this.request = request;
  }

  public VoteRequest getVoteRequest() {
    return this.request;
  }
}

