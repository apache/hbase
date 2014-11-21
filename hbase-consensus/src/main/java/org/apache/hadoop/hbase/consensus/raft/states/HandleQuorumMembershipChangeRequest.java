package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.QuorumMembershipChangeRequest;
import org.apache.hadoop.hbase.consensus.raft.events
  .QuorumMembershipChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HandleQuorumMembershipChangeRequest extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(Leader.class);

  public HandleQuorumMembershipChangeRequest(MutableRaftContext context) {
    super(RaftStateType.HANDLE_QUORUM_MEMBERSHIP_CHANGE_REQUEST, context);
  }

  public void onEntry(final Event e) {
    QuorumMembershipChangeRequest request = null;
    if (e instanceof QuorumMembershipChangeEvent) {
      request = ((QuorumMembershipChangeEvent)e).getRequest();
    }
    if (request == null) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Change event did not include a request");
      }
      return;
    }

    // Make sure we have a valid change request for this FSM before moving on.
    final QuorumInfo newConfig = request.getConfig();
    final QuorumInfo currConfig = c.getQuorumInfo();
    if (currConfig == null || newConfig == null ||
            !currConfig.getQuorumName().equals(newConfig.getQuorumName())) {
      LOG.warn("Quorum name in current and new info does not match");
      request.setResponse(false);
      return;
    }

    // Make sure there is no existing Quorum Membership Change request in flight
    if (c.getUpdateMembershipRequest() != null) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("There is an existing quorum membership change request in " +
          "progress. " + c.getUpdateMembershipRequest() +
          " Cannot accept a new request.");
      }
      request.setResponse(false);
      return;
    }

    // If the given QuorumInfo is not different from the current QuorumInfo
    // assume a duplicate and just move on.
    if (currConfig.equals(newConfig)) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Current and new QuorumInfo are equal." +
                " Ignoring the request as it is most likely a duplicate");
      }
      request.setResponse(true);
      return;
    }

    // If the replica set does not change the quorum info can be updated
    // without going through the protocol.
    if (currConfig.hasEqualReplicaSet(newConfig)) {
      c.setQuorumInfo(newConfig);
      if (LOG.isInfoEnabled()) {
        LOG.info(String.format("Updating %s, new config: %s", c.getQuorumName(),
                newConfig.getPeersWithRank()));
      }
      request.setResponse(true);
      return;
    }

    // Do not allow change of majority of peers in the quorum. This can lead to
    // issues where we can encounter data loss.
    int disjointPeerSetCount = 0;
    for (String peer : newConfig.getPeersAsString()) {
      if (!currConfig.getPeersAsString().contains(peer)) {
        ++disjointPeerSetCount;
      }
    }

    if (disjointPeerSetCount >= c.getMajorityCnt()) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Cannot change majority of the peers in quorum at the same time.");
      }
      request.setResponse(false);
      return;
    }

    // Only if this replica is leader should an info change be requested through
    // the protocol.
    if (c.isLeader()) {
      if (LOG.isInfoEnabled()) {
        LOG.info(String.format("Updating %s, new config: %s", c.getQuorumName(),
                newConfig.getPeersWithRank()));
      }
      c.setUpdateMembershipRequest(request);
      return;
    }

    // At this point this request is either not valid or this replica is
    // follower or candidate and it is probably safe to reject at this point.
    LOG.debug("Cannot process quorum request change. The request is either invalid, or replica is a follower/candidate");
    request.setResponse(false);
  }
}
