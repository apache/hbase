package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;

public class ProgressTimeoutEvent extends Event {
  public ProgressTimeoutEvent() {
    super(RaftEventType.PROGRESS_TIMEOUT);
  }
}
