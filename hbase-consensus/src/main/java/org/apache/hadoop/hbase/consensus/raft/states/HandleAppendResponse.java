package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSessionInterface;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.raft.events.AppendResponseEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleAppendResponse extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(HandleAppendResponse.class);

  public HandleAppendResponse(MutableRaftContext context) {
    super(RaftStateType.HANDLE_APPEND_RESPONSE, context);
  }

  public void onEntry(final Event event) {
    super.onEntry(event);

    AppendResponseEvent e = (AppendResponseEvent)event;
    final AppendResponse response = e.getResponse();

    if (LOG.isTraceEnabled()) {
      LOG.trace(c.toString() + " handling " + response);
    }

    EditId currentEdit = c.getCurrentEdit();
    EditId remoteCurrentEdit = response.getId();

    // Ignore the old response
    if (!currentEdit.equals(remoteCurrentEdit)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Older response " + response.getId());
      }
      return;
    }

    // Verify the session is valid
    AppendConsensusSessionInterface session = c.getAppendSession(response.getId());
    if (session == null) {
      return;
    }

    switch (response.getResult()) {
      case SUCCESS:
        session.incrementAck(
          remoteCurrentEdit, response.getAddress(), response.getRank(), response.canTakeover());
        c.updatePeerAckedId(response.getAddress(), remoteCurrentEdit);
        break;
      case HIGHER_TERM:
        session.incrementHighTermCnt(remoteCurrentEdit, response.getAddress());
        break;
      case LAGGING:
        session.incrementLagCnt(remoteCurrentEdit, response.getAddress());
        break;
      default:
        LOG.error("[Error] AppendSession received an unexpected response. Current edit is "
          + currentEdit + " , remote edit is " + remoteCurrentEdit + "; " +
          "And response is " + response);
    }
  }
}
