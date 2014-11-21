package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;

public class AppendResponseEvent extends Event {
  private AppendResponse response;

  public AppendResponseEvent(final AppendResponse response) {
    super(RaftEventType.APPEND_RESPONSE_RECEIVED);
    this.response = response;
  }

  public AppendResponse getResponse() {
    return response;
  }
}
