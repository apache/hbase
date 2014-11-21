package org.apache.hadoop.hbase.consensus.server.peer.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;

public class PeerAppendResponseEvent extends Event {

  private final AppendResponse response;

  public PeerAppendResponseEvent(AppendResponse response) {
    super(PeerServerEventType.PEER_APPEND_RESPONSE_RECEIVED);
    this.response = response;
  }

  public AppendResponse getAppendResponse() {
    return this.response;
  }
}
