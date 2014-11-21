package org.apache.hadoop.hbase.consensus.server.peer.events;

import org.apache.hadoop.hbase.consensus.fsm.EventType;

public enum PeerServerEventType implements EventType {
  NONE,
  START,
  PEER_VOTE_REQUEST_RECEIVED,
  PEER_VOTE_RESPONSE_RECEIVED,
  PEER_APPEND_REQUEST_RECEIVED,
  PEER_APPEND_RESPONSE_RECEIVED,
  PEER_RPC_ERROR,
  PEER_REACHABLE,
  HALT,
  MAX
}
