package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSessionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSession;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.SessionResult;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;

public class ReSendAppendRequest extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(ReSendAppendRequest.class);

  public ReSendAppendRequest(MutableRaftContext context) {
    super(RaftStateType.RESEND_APPEND_REQUEST, context);
  }

  public void onEntry(final Event event) {
    super.onEntry(event);

    // Get and check the current outstanding append session
    AppendConsensusSessionInterface session = c.getAppendSession(c.getCurrentEdit());
    assert session != null && session.getResult().equals(SessionResult.RETRY);

    if (!session.isTimeout()) {
      c.resendOutstandingAppendRequest();
    }
  }
}
