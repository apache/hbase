package org.apache.hadoop.hbase.consensus.raft.events;

import org.apache.hadoop.hbase.consensus.fsm.EventType;

public enum RaftEventType implements EventType {
  NONE,
  START,
  VOTE_REQUEST_RECEIVED,
  VOTE_RESPONSE_RECEIVED,
  APPEND_REQUEST_RECEIVED,
  APPEND_RESPONSE_RECEIVED,
  RESEED_REQUEST_RECEIVED,
  REPLICATE_ENTRIES,
  PROGRESS_TIMEOUT,
  TERM_TIMEOUT,
  HALT,
  QUORUM_MEMBERSHIP_CHANGE,
  MAX
}
