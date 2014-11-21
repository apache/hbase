package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;

public class StartEvent extends Event {
  public StartEvent() {
    super(RaftEventType.START);
  }
}
