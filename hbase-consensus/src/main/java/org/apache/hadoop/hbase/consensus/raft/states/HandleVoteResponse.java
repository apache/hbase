package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.quorum.VoteConsensusSessionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.raft.events.VoteResponseEvent;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;

public class HandleVoteResponse extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(HandleVoteResponse.class);

  public HandleVoteResponse(MutableRaftContext context) {
    super(RaftStateType.HANDLE_VOTE_RESPONSE, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);

    // Get the vote response from the event
    final VoteResponse response = ((VoteResponseEvent)e).getResponse();

    // Update the ElectionSession. If the currentEdit has been increased,
    // then there must be a new leader or candidate with much higher term in the quorum.
    VoteConsensusSessionInterface session =
      c.getElectionSession(this.c.getCurrentEdit());
    if (session != null) {
      if (response.isSuccess()) {
        session.incrementAck(this.c.getCurrentEdit(), response.getAddress());
      } else {
        if (response.isWrongQuorum()) {
          LOG.warn("As per the VoteResponse from " + response.getAddress() +
            ", it is possible that I am in the wrong quorum. ");
          if (!c.isPartOfNewQuorum()) {
            LOG.info("There was a Joint Quorum Configuration change in the " +
              "past, wherein I would not be a part of the new quorum. " +
              "Closing myself.");
            session.setVoteSessionFailed(this.c.getCurrentEdit());
            c.stop(false);
            if (c.getDataStoreEventListener() != null) {
              LOG.debug("Issuing a request to close the quorum: " +
                c.getQuorumName());
              c.getDataStoreEventListener().closeDataStore();
            } else {
              LOG.debug("No event listener registered, so can't stop the quorum: " +
                c.getQuorumName());
            }
          }
        }
        session.incrementNack(this.c.getCurrentEdit(), response.getAddress());
      }
    } else {
      if (LOG.isWarnEnabled()) {
        LOG.warn(c.toString() + ": VoteConsensusSession is null for " + c.getCurrentEdit());
      }
    }
  }
}
