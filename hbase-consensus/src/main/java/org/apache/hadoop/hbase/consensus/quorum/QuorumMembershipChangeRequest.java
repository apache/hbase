package org.apache.hadoop.hbase.consensus.quorum;

import com.google.common.util.concurrent.AbstractFuture;

public class QuorumMembershipChangeRequest extends AbstractFuture<Boolean> {

  public enum QuorumMembershipChangeState {
    PENDING,
    JOINT_CONFIG_COMMIT_IN_PROGRESS,
    NEW_CONFIG_COMMIT_IN_PROGRESS,
    COMPLETE,
    FAILED
  };

  final QuorumInfo config;
  QuorumMembershipChangeState currentState;

  public QuorumMembershipChangeRequest(QuorumInfo config) {
    this.config = config;
    currentState = QuorumMembershipChangeState.PENDING;
  }

  public QuorumInfo getConfig() {
    return config;
  }

  public QuorumMembershipChangeState getCurrentState() {
    return currentState;
  }

  public void setCurrentState(QuorumMembershipChangeState currentState) {
    this.currentState = currentState;
  }

  public void setResponse(boolean b) {
    set(b);
  }
}
