package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSessionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSession;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;

public class AckClient extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(AckClient.class);

  public AckClient(MutableRaftContext context) {
    super(RaftStateType.ACK_CLIENT, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);

    AppendConsensusSessionInterface session = c.getAppendSession(c.getCurrentEdit());

    assert session != null;

    c.advanceCommittedIndex(session.getSessionId());
    c.setAppendSession(null);
  }
}
