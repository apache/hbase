package org.apache.hadoop.hbase.consensus.raft;

import org.apache.hadoop.hbase.consensus.fsm.*;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.raft.events.*;
import org.apache.hadoop.hbase.consensus.raft.states.*;
import org.apache.hadoop.hbase.consensus.raft.transitions.RaftTransitionType;

public class RaftStateMachine extends FiniteStateMachine {
  public RaftStateMachine(final String name, final MutableRaftContext c) {
    super(name);

    State start = new Start(c);
    State halt = new Halt(c);

    State becomeFollower = new BecomeFollower(c);
    State follower = new Follower(c);

    State becomeLeader = new BecomeLeader(c);
    State leader = new Leader(c);

    State candidate = new Candidate(c);

    State handleVoteRequest = new HandleVoteRequest(c);
    State handleVoteResponse = new HandleVoteResponse(c);
    State handleAppendRequest = new HandleAppendRequest(c);
    State handleAppendResponse = new HandleAppendResponse(c);
    State handleQuorumMembershipChangeRequest = new HandleQuorumMembershipChangeRequest(c);

    State progressTimeout = new ProgressTimeout(c);

    State sendAppendRequest = new SendAppendRequest(c);
    State reSendAppendRequest = new ReSendAppendRequest(c);
    State sendVoteRequest = new SendVoteRequest(c);
    State ackClient = new AckClient(c);
    State quorumMembershipChange = new ChangeQuorumMembership(c);
    State handleReseedRequest = new HandleReseedRequest(c);

    Transition onStart =
            new Transition(RaftTransitionType.ON_START,
                    new OnEvent(RaftEventType.START));

    Transition isLeader =
            new Transition(RaftTransitionType.IS_LEADER,
                    new IsLeader(c));

    Transition isCandidate =
            new Transition(RaftTransitionType.IS_CANDIDATE,
                    new IsCandidate(c));

    Transition isFollower =
            new Transition(RaftTransitionType.IS_FOLLOWER,
                    new IsFollower(c));

    Transition unConditional =
            new Transition(RaftTransitionType.UNCONDITIONAL,
                    new Unconditional());

    Transition onVoteRequestReceived =
            new Transition(RaftTransitionType.ON_VOTE_REQUEST,
                    new OnEvent(RaftEventType.VOTE_REQUEST_RECEIVED));

    Transition onVoteResponseReceived =
            new Transition(RaftTransitionType.ON_VOTE_RESPONSE,
                    new OnEvent(RaftEventType.VOTE_RESPONSE_RECEIVED));

    Transition onAppendRequestReceived =
            new Transition(RaftTransitionType.ON_APPEND_REQUEST,
                    new OnEvent(RaftEventType.APPEND_REQUEST_RECEIVED));

    Transition onAppendResponseReceived =
            new Transition(RaftTransitionType.ON_APPEND_RESPONSE,
                    new OnEvent(RaftEventType.APPEND_RESPONSE_RECEIVED));

    Transition onProgressTimeout =
            new Transition(RaftTransitionType.ON_PROGRESS_TIMEOUT,
                    new OnEvent(RaftEventType.PROGRESS_TIMEOUT));

    Transition onSendAppendEntries =
            new Transition(RaftTransitionType.ON_REPLICATED_ENTRIES,
                    new OnEvent(RaftEventType.REPLICATE_ENTRIES));

    Transition onAppendSucceeded =
            new Transition(RaftTransitionType.ON_APPEND_SUCCEEDED,
                    new AppendSucceeded(c));

    Transition onAppendTimeout =
            new Transition(RaftTransitionType.ON_APPEND_TIMEOUT,
                    new AppendRequestTimeout(c));

    Transition onNeedStepDown =
            new Transition(RaftTransitionType.ON_NEED_STEPDOWN,
                    new NeedStepDown(c));

    Transition onAppendNotCompleted =
            new Transition(RaftTransitionType.ON_APPEND_NOT_COMPLETED,
                    new AppendNotCompleted(c));

    Transition onAppendRetry =
            new Transition(RaftTransitionType.ON_APPEND_RETRY,
                    new AppendRetry(c));

    Transition onVoteSucceeded =
            new Transition(RaftTransitionType.ON_VOTE_SUCCEEDED,
                    new VoteSucceeded(c));

    Transition onVoteFailed =
            new Transition(RaftTransitionType.ON_VOTE_FAILED,
                    new VoteFailed(c));

    Transition onVoteNotCompleted =
            new Transition(RaftTransitionType.ON_VOTE_NOT_COMPLETED,
                    new VoteNotCompleted(c));

    Transition onTransactionLogNotAccessible =
            new Transition(RaftTransitionType.ON_TRANSACTION_LOG_NOT_ACCESSIBLE,
                    new IsTransactionLogNotAccessible(c));

    Transition onHalt = new Transition(RaftTransitionType.ON_HALT,
            new OnEvent(RaftEventType.HALT));
    Transition onQuorumMembershipChangeRequest = new Transition(RaftTransitionType.ON_QUORUM_MEMBERSHIP_CHANGE_REQUEST,
      new OnEvent(RaftEventType.QUORUM_MEMBERSHIP_CHANGE));

    Transition quorumMembershipChangeInProgress = new Transition(RaftTransitionType.IS_QUORUM_MEMBERSHIP_CHANGE_IN_PROGRESS,
      new QuorumMembershipChangeInProgress(c));
    Transition noQuorumMembershipChangeNotInProgress = new Transition(RaftTransitionType.QUORUM_MEMBERSHIP_CHANGE_NOT_IN_PROGRESS,
      new NoQuorumMembershipChangeInProgress(c));

    Transition onReseedRequest = new Transition(RaftTransitionType.ON_RESEED_REQUEST,
      new OnEvent(RaftEventType.RESEED_REQUEST_RECEIVED));

    // BEGIN
    addTransition(start, becomeFollower, onStart);

    // BECOME_FOLLOWER
    addTransition(becomeFollower, follower, unConditional);

    // FOLLOWER
    addTransition(follower, progressTimeout, onProgressTimeout);
    addTransition(follower, handleAppendRequest, onAppendRequestReceived);
    addTransition(follower, handleVoteRequest, onVoteRequestReceived);
    addTransition(follower, handleQuorumMembershipChangeRequest, onQuorumMembershipChangeRequest);
    addTransition(follower, halt, onHalt);
    addTransition(follower, handleReseedRequest, onReseedRequest);

    // CANDIDATE
    addTransition(candidate, handleVoteRequest, onVoteRequestReceived);
    addTransition(candidate, handleAppendRequest, onAppendRequestReceived);
    addTransition(candidate, handleVoteResponse, onVoteResponseReceived);
    addTransition(candidate, progressTimeout, onProgressTimeout);
    addTransition(candidate, handleQuorumMembershipChangeRequest, onQuorumMembershipChangeRequest);
    addTransition(candidate, halt, onHalt);
    addTransition(candidate, handleReseedRequest, onReseedRequest);

    // BECOME_LEADER
    addTransition(becomeLeader, leader, isLeader);
    addTransition(becomeLeader, halt, onTransactionLogNotAccessible);

    // LEADER
    addTransition(leader, handleVoteRequest, onVoteRequestReceived);
    addTransition(leader, handleAppendRequest, onAppendRequestReceived);
    addTransition(leader, sendAppendRequest, onSendAppendEntries);
    addTransition(leader, becomeFollower, onAppendTimeout);
    addTransition(leader, handleAppendResponse, onAppendResponseReceived);
    addTransition(leader, handleQuorumMembershipChangeRequest, onQuorumMembershipChangeRequest);
    addTransition(leader, halt, onHalt);
    addTransition(leader, handleReseedRequest, onReseedRequest);

    // SEND_APPEND_REQUEST
    addTransition(sendAppendRequest, leader, isLeader);
    addTransition(sendAppendRequest, follower, isFollower);
    addTransition(sendAppendRequest, candidate, isCandidate);
    addTransition(sendAppendRequest, halt, onTransactionLogNotAccessible);

    // HANDLE_APPEND_REQUEST
    addTransition(handleAppendRequest, leader, isLeader);
    addTransition(handleAppendRequest, becomeFollower, isFollower);
    addTransition(handleAppendRequest, candidate, isCandidate);
    addTransition(handleAppendRequest, halt, onTransactionLogNotAccessible);

    // HANDLE_APPEND_RESPONSE
    addTransition(handleAppendResponse, ackClient, onAppendSucceeded);
    addTransition(handleAppendResponse, leader, onAppendNotCompleted);
    addTransition(handleAppendResponse, reSendAppendRequest, onAppendRetry);
    addTransition(handleAppendResponse, becomeFollower, onNeedStepDown);
    addTransition(handleAppendResponse, halt, onTransactionLogNotAccessible);

    // SEND_VOTE_REQUEST
    addTransition(sendVoteRequest, candidate, isCandidate);
    addTransition(sendVoteRequest, becomeFollower, isFollower);
    addTransition(sendVoteRequest, halt, onTransactionLogNotAccessible);

    // HANDLE_VOTE_REQUEST
    addTransition(handleVoteRequest, leader, isLeader);
    addTransition(handleVoteRequest, candidate, isCandidate);
    addTransition(handleVoteRequest, becomeFollower, isFollower);
    addTransition(handleVoteRequest, halt, onTransactionLogNotAccessible);

    // HANDLE_VOTE_RESPONSE
    addTransition(handleVoteResponse, becomeLeader, onVoteSucceeded);
    addTransition(handleVoteResponse, candidate, onVoteNotCompleted);
    addTransition(handleVoteResponse, becomeFollower, onVoteFailed);

    // HANDLE_QUORUM_MEMBERSHIP_CHANGE_REQUEST
    addTransition(handleQuorumMembershipChangeRequest, leader, isLeader);
    addTransition(handleQuorumMembershipChangeRequest, follower, isFollower);
    addTransition(handleQuorumMembershipChangeRequest, candidate, isCandidate);

    // ACK_CLIENT
    addTransition(ackClient, quorumMembershipChange, quorumMembershipChangeInProgress);
    addTransition(ackClient, leader, noQuorumMembershipChangeNotInProgress);

    // QUORUM_MEMBERSHIP_CHANGE
    addTransition(quorumMembershipChange, leader, isLeader);
    addTransition(quorumMembershipChange, follower, isFollower);
    addTransition(quorumMembershipChange, candidate, isCandidate);

    // PROGRESS_TIMEOUT
    addTransition(progressTimeout, sendVoteRequest, unConditional);

    // RESEND_APPEND_REQUEST
    addTransition(reSendAppendRequest, leader, unConditional);

    // HANDLE_RESEED_REQUEST
    addTransition(handleReseedRequest, leader, isLeader);
    addTransition(handleReseedRequest, candidate, isCandidate);
    addTransition(handleReseedRequest, follower, isFollower);

    setStartState(start);
  }
}
