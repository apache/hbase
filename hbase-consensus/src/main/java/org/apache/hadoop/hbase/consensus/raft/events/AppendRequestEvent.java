package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

public class AppendRequestEvent extends Event {
  private AppendRequest request;

  public AppendRequestEvent(AppendRequest request) {
    super(RaftEventType.APPEND_REQUEST_RECEIVED);
    this.request = request;
  }

  public AppendRequest getRequest() {
    return request;
  }

  public void abort(final String message) {
    request.setError(new ThriftHBaseException(new Exception("Aborted AppendRequestEvent: "
        + message)));
  }
}
