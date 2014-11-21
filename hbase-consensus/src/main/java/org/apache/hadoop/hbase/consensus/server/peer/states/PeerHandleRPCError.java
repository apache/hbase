package org.apache.hadoop.hbase.consensus.server.peer.states;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerAppendResponseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerHandleRPCError extends PeerServerState {
  private static Logger LOG = LoggerFactory.getLogger(PeerHandleRPCError.class);

  public PeerHandleRPCError(PeerServerMutableContext context) {
    super(PeerServerStateType.HANDLE_RPC_ERROR, context);
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);
    c.updatePeerAvailabilityStatus(false);
  }

  @Override
  public void onExit(final Event e) {
    super.onExit(e);
    c.enqueueEvent(new PeerAppendResponseEvent(null));
  }
}
