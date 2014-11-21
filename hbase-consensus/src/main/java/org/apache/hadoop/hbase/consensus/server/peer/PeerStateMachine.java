package org.apache.hadoop.hbase.consensus.server.peer;

import org.apache.hadoop.hbase.consensus.fsm.*;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerServerEventType;
import org.apache.hadoop.hbase.consensus.server.peer.states.*;
import org.apache.hadoop.hbase.consensus.server.peer.transition.PeerServerTransitionType;

public class PeerStateMachine extends FiniteStateMachine {

  public PeerStateMachine(final String name, final PeerServer peerServer) {
    super(name);

    // Define all the states:
    State start = new Start(peerServer);
    State peerFollower = new PeerFollower(peerServer);
    State handleVoteResponse = new PeerHandleVoteResponse(peerServer);
    State sendVoteRequest = new PeerSendVoteRequest(peerServer);
    State handleAppendResponse = new PeerHandleAppendResponse(peerServer);
    State sendAppendRequest = new PeerSendAppendRequest(peerServer);
    State handleRPCError = new PeerHandleRPCError(peerServer);
    State halt = new PeerServerState(PeerServerStateType.HALT, peerServer);

    // Define all the transitions:
    Transition onStart =
            new Transition(PeerServerTransitionType.ON_START,
                    new OnEvent(PeerServerEventType.START));

    Transition onAppendRequestReceived =
            new Transition(PeerServerTransitionType.ON_APPEND_REQUEST,
                    new OnEvent(PeerServerEventType.PEER_APPEND_REQUEST_RECEIVED));

    Transition onAppendResponseReceived =
            new Transition(PeerServerTransitionType.ON_APPEND_RESPONSE,
                    new OnEvent(PeerServerEventType.PEER_APPEND_RESPONSE_RECEIVED));

    Transition onVoteRequestReceived =
            new Transition(PeerServerTransitionType.ON_VOTE_REQUEST,
                    new OnEvent(PeerServerEventType.PEER_VOTE_REQUEST_RECEIVED));

    Transition onVoteResponseReceived =
            new Transition(PeerServerTransitionType.ON_VOTE_RESPONSE,
                    new OnEvent(PeerServerEventType.PEER_VOTE_RESPONSE_RECEIVED));

    Transition onRPCErrorReceived =
            new Transition(PeerServerTransitionType.ON_RPC_ERROR,
                    new OnEvent(PeerServerEventType.PEER_RPC_ERROR));

    Transition peerReachable =
            new Transition(PeerServerTransitionType.ON_PEER_IS_REACHABLE,
                    new OnEvent(PeerServerEventType.PEER_REACHABLE));

    Transition unConditional =
            new Transition(PeerServerTransitionType.UNCONDITIONAL,
                    new Unconditional());

    Transition onHalt = new Transition(PeerServerTransitionType.ON_HALT,
                    new OnEvent(PeerServerEventType.HALT));

    // Add the transitions and states into the state machine
    addTransition(start, peerFollower, onStart);

    addTransition(peerFollower, handleVoteResponse, onVoteResponseReceived);
    addTransition(peerFollower, handleAppendResponse, onAppendResponseReceived);
    addTransition(peerFollower, handleAppendResponse, peerReachable);
    addTransition(peerFollower, sendVoteRequest, onVoteRequestReceived);
    addTransition(peerFollower, sendAppendRequest, onAppendRequestReceived);
    addTransition(peerFollower, handleRPCError, onRPCErrorReceived);
    addTransition(peerFollower, halt, onHalt);

    addTransition(handleAppendResponse, peerFollower, unConditional);

    addTransition(sendVoteRequest, peerFollower, unConditional);

    addTransition(sendAppendRequest, peerFollower, unConditional);

    addTransition(handleVoteResponse, peerFollower, unConditional);

    addTransition(handleRPCError, peerFollower, peerReachable);
    addTransition(handleRPCError, sendVoteRequest, onVoteRequestReceived);

    // Set the initial state
    setStartState(start);
  }
}
