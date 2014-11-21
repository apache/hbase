package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.metrics.ConsensusMetrics;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServer;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerServerEventType;

import java.util.*;

public class SingleConsensusPeerManager extends AbstractPeerManager {

  /** The map of the peer servers, excluding the current server */
  private Map<String, PeerServer> peerServers;

  public SingleConsensusPeerManager(final MutableRaftContext c) {
    super(c);
  }

  public void initializePeers() {
    if (peerServers == null) {
      peerServers = super.initializePeers(c.getQuorumInfo().getPeersWithRank());
    }
  }

  public void setPeerServers(final Map<String, PeerServer> peers) {
    peerServers = peers;
  }

  public Map<String, PeerServer> getPeerServers() {
    return peerServers;
  }

  public void resetPeers() {
    super.resetPeers(peerServers);
  }

  public void setPeerReachable(String address) {
    PeerServer server;
    if ((server = peerServers.get(address)) != null) {
      server.enqueueEvent(
        new Event(PeerServerEventType.PEER_REACHABLE));
    }
  }

  public void sendVoteRequestToQuorum(VoteRequest request) {
    super.broadcastVoteRequest(peerServers, request);
  }

  public void sendAppendRequestToQuorum(AppendRequest request) {
    super.broadcastAppendRequest(peerServers, request);
  }

  public void stop() {
    super.stop(peerServers);
  }

  public String getState() {
    return super.getState(peerServers);
  }

  @Override
  public AppendConsensusSessionInterface createAppendConsensusSession(
          int majorityCount, AppendRequest request, ReplicateEntriesEvent event,
          ConsensusMetrics metrics, int rank,
    boolean enableStepDownOnHigherRankCaughtUp) {
    return new AppendConsensusSession(c, majorityCount, request, event,
            metrics, rank, enableStepDownOnHigherRankCaughtUp,
            c.getAppendEntriesMaxTries(), c.getQuorumInfo().getPeersAsString());
  }

  @Override
  public VoteConsensusSessionInterface createVoteConsensusSession(
          int majorityCount, VoteRequest request, ConsensusMetrics metrics) {
    return new VoteConsensusSession(majorityCount, request, metrics, c.getQuorumInfo().getPeersAsString());
  }

  @Override public List<QuorumInfo> getConfigs() {
    return Arrays.asList(c.getQuorumInfo());
  }
}
