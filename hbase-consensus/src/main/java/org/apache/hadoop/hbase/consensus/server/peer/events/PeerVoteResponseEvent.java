package org.apache.hadoop.hbase.consensus.server.peer.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;

public class PeerVoteResponseEvent extends Event {

  private final VoteResponse response;

  public PeerVoteResponseEvent(VoteResponse response) {
    super(PeerServerEventType.PEER_VOTE_RESPONSE_RECEIVED);
    this.response = response;
  }

  public VoteResponse getVoteResponse() {
    return this.response;
  }
}
