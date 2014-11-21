package org.apache.hadoop.hbase.consensus.server.peer.states;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServerMutableContext;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerAppendRequestEvent;

public class PeerSendAppendRequest extends PeerServerState {
  private static Logger LOG = LoggerFactory.getLogger(PeerSendAppendRequest.class);

  public PeerSendAppendRequest(PeerServerMutableContext context) {
    super(PeerServerStateType.SEND_APPEND_REQUEST, context);
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);

    final AppendRequest request = (((PeerAppendRequestEvent)e)).getAppendRequest();

    // Seen a new request, update the latest request
    AppendRequest lastSeenAppendRequest = c.getLatestRequest();
    if (lastSeenAppendRequest == null ||
      lastSeenAppendRequest.getLogId(0).compareTo(request.getLogId(0)) < 0) {
      c.setLatestRequest(request);
    }
    if (request.isTraceable()) {
      LOG.debug("[AppendRequest Trace] " + c.toString() + " is processing " + request + " " +
        "lastRequest " + c.getLatestRequest() + " lastEditID " + c.getLastEditID());
    }
    c.calculateAndSetAppendLag();

    // Send the request only if a) the latest EditId ack'ed by the peer is the
    // as the EditId of the previous request or b) if the latest ack'ed Id is
    // the same as the current Id.
    //
    // Case b is important if no majority has been reached yet on the request,
    // and it needs to be retried. It needs to be resent to the peer to act as a
    // heartbeat and keep its progress timer from firing. Failing to do so would
    // lead to unnecessary leader election.
    if (request.getPrevLogId().compareTo(c.getLastEditID()) == 0 ||
            request.getLogId(0).compareTo(c.getLastEditID()) == 0) {
      c.sendAppendRequestWithCallBack(request);
    } else if (request.isTraceable()) {
        LOG.debug("[AppendRequest Trace] Not able to send the new append request out: " +
          "request: " + request + " and the lastEditID is " + c.getLastEditID());
    }
  }
}
