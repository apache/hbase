package org.apache.hadoop.hbase.consensus.server.peer.states;

import org.apache.hadoop.hbase.consensus.fsm.StateType;

public enum PeerServerStateType implements StateType {
  START,
  PEER_FOLLOWER,
  SEND_VOTE_REQUEST,
  HANDLE_VOTE_RESPONSE,
  SEND_APPEND_REQUEST,
  HANDLE_APPEND_RESPONSE,
  HANDLE_RPC_ERROR,
  RECOVERY,
  HALT,
  MAX
}
