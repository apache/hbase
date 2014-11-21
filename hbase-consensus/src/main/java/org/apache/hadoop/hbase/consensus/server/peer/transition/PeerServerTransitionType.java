package org.apache.hadoop.hbase.consensus.server.peer.transition;

import org.apache.hadoop.hbase.consensus.fsm.TransitionType;

public enum PeerServerTransitionType implements TransitionType {
  NONE,
  UNCONDITIONAL,
  ON_START,
  ON_VOTE_REQUEST,
  ON_VOTE_RESPONSE,

  ON_APPEND_REQUEST,
  ON_APPEND_RESPONSE,

  ON_APPEND_NACK_RESPONSE,
  ON_APPEND_ACK_RESPONSE,
  ON_PEER_IS_REACHABLE,
  ON_RPC_ERROR,

  ON_HALT,
  MAX
}
