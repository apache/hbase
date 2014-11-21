package org.apache.hadoop.hbase.consensus.server.peer.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;

public class PeerAppendRequestEvent extends Event {

  private final AppendRequest request;

  public PeerAppendRequestEvent(AppendRequest request) {
    super(PeerServerEventType.PEER_APPEND_REQUEST_RECEIVED);
    this.request = request;
  }

  public AppendRequest getAppendRequest() {
    return this.request;
  }
}
