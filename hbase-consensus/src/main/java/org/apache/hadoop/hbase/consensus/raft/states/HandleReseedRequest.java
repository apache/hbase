package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.quorum.ReseedRequest;
import org.apache.hadoop.hbase.consensus.raft.events.ReseedRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

import java.io.IOException;

public class HandleReseedRequest extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(HandleVoteRequest.class);

  public HandleReseedRequest(MutableRaftContext context) {
    super(RaftStateType.HANDLE_RESEED_REQUEST, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);

    ReseedRequest request = ((ReseedRequestEvent)e).getRequest();

    // In case you are the leader, just acknowledge it and move on
    if (c.isLeader()) {
      request.setResponse(true);
      return;
    }

    try {
      c.reseedStartIndex(request.getReseedIndex());
    } catch (IOException e1) {
      LOG.error("Cannot complete the reseed request ", e1);
      request.getResult().setException(e1);
    }

    request.getResult().set(true);
    c.candidateStepDown();
  }
}
