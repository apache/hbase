package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.raft.events.VoteRequestEvent;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class HandleVoteRequest extends RaftAsyncState {
  private static Logger LOG = LoggerFactory.getLogger(HandleVoteRequest.class);
  private SettableFuture<Void> handleVoteRequestFuture;

  public HandleVoteRequest(MutableRaftContext context) {
    super(RaftStateType.HANDLE_VOTE_REQUEST, context);
  }

  @Override
  public boolean isComplete() {
    return handleVoteRequestFuture == null || handleVoteRequestFuture.isDone();
  }

  @Override
  public ListenableFuture<?> getAsyncCompletion() {
    return handleVoteRequestFuture;
  }

  @Override
  public void onEntry(final Event e) {
    handleVoteRequestFuture = null;
    super.onEntry(e);
    // Assume there is only type of event here
    final VoteRequest request =  ((VoteRequestEvent)e).getVoteRequest();

    // First check if we received a VoteRequest from a non-member. This can
    // happen in cases when some peers did not transition properly to a new
    // quorum during a quorum membership change.
    if (!c.getPeerManager().getPeerServers().keySet()
      .contains(request.getAddress())) {
      LOG.info("Received VoteRequest from " + request.getAddress() +
        ", even though it is not part of the quorum: " +
        c.getPeerManager().getPeerServers().keySet());
      finishRequest(request, VoteResponse.VoteResult.WRONGQUORUM);
    } else if (c.getCurrentEdit().getTerm() < request.getTerm() &&
      c.getLogManager().getLastEditID().compareTo(request.getPrevEditID()) <= 0) {

      handleVoteRequestFuture = SettableFuture.create();
      c.getProgressTimer().reset();

      if (c.isLeader()) {
        LOG.debug(c + " has stepped down from leader state due to vote request: " + request);
      }

      c.clearLeader();
      ListenableFuture<Void> votedForDone = c.setVotedFor(new ConsensusHost(request.getTerm(), request.getAddress()));
      Futures.addCallback(votedForDone, new FutureCallback<Void>() {

        @Override
        public void onSuccess(Void result) {
          c.setCurrentEditId(
            new EditId(request.getTerm(), c.getCurrentEdit().getIndex()));
          finishRequest(request, VoteResponse.VoteResult.SUCCESS);
          handleVoteRequestFuture.set(null);
        }

        @Override
        public void onFailure(Throwable t) {
          request.setError(t);
          // Set the remote machine reachable when receiving any RPC request
          c.setPeerReachable(request.getAddress());
          LOG.error("setVotedFor failed for quorum: " + c.getQuorumInfo().getQuorumName(), t);
          handleVoteRequestFuture.set(null);
        }
      });
    } else {
      finishRequest(request, VoteResponse.VoteResult.FAILURE);
    }
  }

  /**
   * @param request
   * @param voteResult
   */
  private void finishRequest(final VoteRequest request, VoteResponse.VoteResult voteResult) {
    // Set the response for this request
    VoteResponse response = new VoteResponse(this.c.getMyAddress(),
      request.getTerm(), voteResult);
    request.setResponse(response);
    if (LOG.isDebugEnabled()) {
      LOG.debug(c + " is sending vote response: " + response +
        " for request " + request);
    }

    // Set the remote machine reachable when receiving any RPC request
    c.setPeerReachable(request.getAddress());
  }
}
