package org.apache.hadoop.hbase.consensus.server.peer;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.TimeoutEventHandler;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerServerEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconnectTimeoutCallback implements TimeoutEventHandler {
  private static final Logger
    LOG = LoggerFactory.getLogger(ReconnectTimeoutCallback.class);
  private final PeerServer peer;

  public ReconnectTimeoutCallback(final PeerServer peer) {
    this.peer = peer;
  }
  public void onTimeout() {
    LOG.debug(peer + " Reconnect timeout Triggered");
    peer.enqueueEvent(new Event(PeerServerEventType.PEER_REACHABLE));
  }
}
