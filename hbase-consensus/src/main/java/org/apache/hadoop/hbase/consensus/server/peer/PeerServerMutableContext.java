package org.apache.hadoop.hbase.consensus.server.peer;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.metrics.PeerMetrics;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.Timer;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;

public interface PeerServerMutableContext extends PeerServerImmutableContext {
  void sendVoteRequestWithCallBack(VoteRequest request);
  void sendAppendRequestWithCallBack(AppendRequest request);
  MutableRaftContext getQuorumContext();
  void enqueueEvent(final Event e);
  void setLastEditID(final EditId id);
  void stop();
  void resetPeerContext();
  void updatePeerAvailabilityStatus(boolean isAvailable);
  PeerMetrics getMetrics();
  void setLatestRequest(AppendRequest latestRequest);
  void calculateAndSetAppendLag();
}
