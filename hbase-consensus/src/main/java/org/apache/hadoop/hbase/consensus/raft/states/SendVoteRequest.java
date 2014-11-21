package org.apache.hadoop.hbase.consensus.raft.states;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.VoteConsensusSessionInterface;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class SendVoteRequest extends RaftAsyncState {
  private SettableFuture<Void> handleSendVoteRequestFuture;

  @Override
  public boolean isComplete() {
    return handleSendVoteRequestFuture == null || handleSendVoteRequestFuture.isDone();
  }

  @Override
  public ListenableFuture<?> getAsyncCompletion() {
    return handleSendVoteRequestFuture;
  }

  public SendVoteRequest(MutableRaftContext context) {
    super(RaftStateType.SEND_VOTE_REQUEST, context);
  }

  @Override
  public void onEntry(final Event e) {
    super.onEntry(e);

    handleSendVoteRequestFuture = null;
    EditId prevEditID = c.getCurrentEdit();
    EditId nextEditId = prevEditID;

    final VoteConsensusSessionInterface oldVoteConsensusSession =
      c.getElectionSession(prevEditID);

    // In case the current election session is not complete, retry the election
    // with the same edit id, else get a new election id
    if (oldVoteConsensusSession != null &&
      !oldVoteConsensusSession.isComplete()) {
      finish(oldVoteConsensusSession.getRequest());
    } else {
      handleSendVoteRequestFuture = SettableFuture.create();

      // Increment the current term and index
      nextEditId = EditId.getElectionEditID(prevEditID, c.getRanking(), 0);
      c.setCurrentEditId(nextEditId);

      // Set the VoteFor and reset the leader
      c.clearLeader();
      final EditId finalNextEditId = nextEditId;
      ListenableFuture<Void> votedForDone = c.setVotedFor(new ConsensusHost(nextEditId.getTerm(), c.getMyAddress()));
      Futures.addCallback(votedForDone, new FutureCallback<Void>() {

        @Override
        public void onSuccess(Void result) {
          // Prepare the VoteRequest
          VoteRequest request = new VoteRequest(c.getQuorumName(), c.getMyAddress(),
            finalNextEditId.getTerm(), c.getPreviousEdit());

          // Prepare the vote session
          VoteConsensusSessionInterface electionSession =
            c.getPeerManager().createVoteConsensusSession(
              c.getMajorityCnt(), request, c.getConsensusMetrics());

          // Set the outstanding the election session
          c.setElectionSession(electionSession);

          // Increment the ack for the local server
          electionSession.incrementAck(finalNextEditId, c.getMyAddress());

          finish(request);
          handleSendVoteRequestFuture.set(null);

        }

        @Override
        public void onFailure(Throwable t) {
          handleSendVoteRequestFuture.set(null);
        }
      });
    }
  }

  /**
   * @param request
   */
  private void finish(final VoteRequest request) {
    // Send the vote request for each peer server
    c.sendVoteRequestToQuorum(request);

    c.getConsensusMetrics().setRaftState(PeerStatus.RAFT_STATE.CANDIDATE);
  }
}
