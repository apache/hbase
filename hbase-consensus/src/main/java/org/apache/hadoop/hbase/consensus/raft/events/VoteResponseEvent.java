package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;

public class VoteResponseEvent extends Event {
  VoteResponse response;

  public VoteResponseEvent(final VoteResponse response) {
    super(RaftEventType.VOTE_RESPONSE_RECEIVED);
    this.response = response;
  }

  public VoteResponse getResponse() {
    return response;
  }
}
