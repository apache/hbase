package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.consensus.metrics.ConsensusMetrics;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServer;

import java.util.List;
import java.util.Map;

public interface PeerManagerInterface {
  void initializePeers();
  void setPeerServers(final Map<String, PeerServer> peers);
  Map<String, PeerServer> getPeerServers();
  void resetPeers();
  void setPeerReachable(String address);
  void sendVoteRequestToQuorum(VoteRequest request);
  void sendAppendRequestToQuorum(AppendRequest request);
  void updatePeerAckedId(String address, EditId remoteEdit);
  long getMinUnPersistedIndexAcrossQuorum();
  void setMinAckedIndexAcrossAllPeers(long index);
  void stop();
  String getState();
  AppendConsensusSessionInterface createAppendConsensusSession(int majorityCount,
                                                     final AppendRequest request,
                                                     final ReplicateEntriesEvent event,
                                                     final ConsensusMetrics metrics,
                                                     final int rank,
                                                     final boolean enableStepDownOnHigherRankCaughtUp);
  VoteConsensusSessionInterface createVoteConsensusSession(
    int majorityCount,
    final VoteRequest request,
    final ConsensusMetrics metrics);

  List<QuorumInfo> getConfigs();
  boolean isInJointQuorumMode();
}
