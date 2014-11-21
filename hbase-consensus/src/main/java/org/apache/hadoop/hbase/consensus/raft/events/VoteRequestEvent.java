package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

public class VoteRequestEvent extends Event {

  private final VoteRequest request;

  public VoteRequestEvent(VoteRequest request) {
    super(RaftEventType.VOTE_REQUEST_RECEIVED);
    this.request = request;
  }

  public VoteRequest getVoteRequest() {
    return this.request;
  }

  public void abort(final String message) {
    request.setError(new ThriftHBaseException(
      new Exception("Aborted VoteRequestEvent: " + message)));
  }
}
