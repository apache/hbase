package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.consensus.raft.events.ProgressTimeoutEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ProgressTimeoutCallback implements TimeoutEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
    ProgressTimeoutCallback.class);
  private final RaftQuorumContext replica;

  public ProgressTimeoutCallback(final RaftQuorumContext replica) {
    this.replica = replica;
  }

  public void onTimeout() {
    LOG.info(replica + " has a progress timeout! " +
      " current edit: " +
      replica.getCurrentEdit() + ", Last AppendRequest was received at : " +
      new Date(replica.getLastAppendRequestReceivedTime()));

    if (System.currentTimeMillis() - replica.getLastAppendRequestReceivedTime() >=
      replica.getProgressTimeoutForMeMillis()) {
      replica.getConsensusMetrics().incProgressTimeouts();
      replica.offerEvent(new ProgressTimeoutEvent());
    } else {
      LOG.info(replica + " Ignoring the progress timer.");
    }
  }
}
