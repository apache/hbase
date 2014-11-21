package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.QuorumMembershipChangeRequest;

public class QuorumMembershipChangeEvent extends Event {

  private final QuorumMembershipChangeRequest request;

  public QuorumMembershipChangeEvent(final QuorumMembershipChangeRequest request) {
    super(RaftEventType.QUORUM_MEMBERSHIP_CHANGE);
    this.request = request;
  }

  public QuorumMembershipChangeRequest getRequest() {
    return request;
  }
}
