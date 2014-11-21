package org.apache.hadoop.hbase.consensus.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import java.nio.ByteBuffer;

public class
  HeartbeatTimeoutCallback implements TimeoutEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
          HeartbeatTimeoutCallback.class);
  private final RaftQuorumContext replica;
  private static final ReplicateEntriesEvent HEARTBEAT_EVENT = new ReplicateEntriesEvent(true,
      ByteBuffer.allocate(1));

  public HeartbeatTimeoutCallback(final RaftQuorumContext replica) {
    this.replica = replica;
  }

  @Override
  public void onTimeout() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("HeartBeat Triggered on " + replica);
    }

    // When there is no append request for a long time, in order to avoid
    // progress timeouts, we offer heartbeats which are no-ops.
    replica.getConsensusMetrics().incHeartBeatTimeouts();
    replica.offerEvent(HEARTBEAT_EVENT);
  }
}
