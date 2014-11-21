package org.apache.hadoop.hbase.consensus.exceptions;

import java.io.IOException;

/**
 * Thrown by the ConsensusState whenever a request is made to a non Leader
 * State.
 */
public class NewLeaderException extends IOException {
  private String newLeaderAddress;

  public NewLeaderException(String newLeaderAddress) {
    super("The new leader in the quorum is " + newLeaderAddress);
    this.newLeaderAddress = newLeaderAddress;
  }

  public String getNewLeaderAddress() {
    return newLeaderAddress;
  }
}
