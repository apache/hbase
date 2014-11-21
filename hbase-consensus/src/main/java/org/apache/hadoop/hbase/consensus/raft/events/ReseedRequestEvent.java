package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.ReseedRequest;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

public class ReseedRequestEvent extends Event {
  private ReseedRequest request;

  public ReseedRequestEvent(ReseedRequest request) {
    super(RaftEventType.RESEED_REQUEST_RECEIVED);
    this.request = request;
  }

  public ReseedRequest getRequest() {
    return request;
  }

  public void abort(final String message) {
    request.getResult().setException(
      new ThriftHBaseException(new Exception("Aborted AppendRequestEvent: " + message)));
  }
}
