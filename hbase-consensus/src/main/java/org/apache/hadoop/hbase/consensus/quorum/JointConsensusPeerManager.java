package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.metrics.ConsensusMetrics;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServer;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerServerEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class to handle peers, leader elections, transactions while a quorum change is
 * in progress. Particularly, it has the knowledge about both the old and the new
 * config and makes sure, that all the decisions are made in agreement with both
 * the configs.
 *
 * TODO: Get rid of oldPeerServers/newPeerServers notion.
 */
public class JointConsensusPeerManager extends AbstractPeerManager {
  private static final Logger LOG = LoggerFactory.getLogger(
    JointConsensusPeerManager.class);

  // Peers in the old config
  private Map<String, PeerServer> oldPeerServers;

  // Peers in the new config
  private Map<String, PeerServer> newPeerServers;

  // Contains all the peers involved in both the configs
  private Map<String, PeerServer> allPeers;

  private final QuorumInfo newConfig;

  public JointConsensusPeerManager(final MutableRaftContext c,
                                   final QuorumInfo newConfig) {
    super(c);
    this.newConfig = newConfig;
    allPeers = new HashMap<>();
  }

  @Override
  public void initializePeers() {

    // Initialize the old peers
    if (oldPeerServers == null) {
      oldPeerServers = super.initializePeers(c.getQuorumInfo().getPeersWithRank());
    }
    allPeers.putAll(oldPeerServers);

    // Initialize the new peers
    if (newPeerServers == null) {
      newPeerServers = new HashMap<>();
      Map<HServerAddress, Integer> newPeers = new HashMap<>();

      // There can be an overlap between the new and old configuration. Hence,
      // we should initialize a peer only once. So, lets remove the peers which
      // are already initialized.
      newPeers.putAll(newConfig.getPeersWithRank());
      Iterator<Map.Entry<HServerAddress, Integer>> newPeerIterator =
        newPeers.entrySet().iterator();
      HServerAddress peerAddress;
      while (newPeerIterator.hasNext()) {
        Map.Entry<HServerAddress, Integer> e = newPeerIterator.next();
        peerAddress = e.getKey();
        int newPeerRank = e.getValue();
        String consensusServerAddress =
          RaftUtil.getLocalConsensusAddress(peerAddress).getHostAddressWithPort();
        if (oldPeerServers.get(consensusServerAddress) != null) {
          PeerServer oldPeerServer = oldPeerServers.get(consensusServerAddress);
          oldPeerServer.setRank(newPeerRank);
          newPeerServers.put(consensusServerAddress, oldPeerServer);
          oldPeerServers.remove(consensusServerAddress);
          newPeerIterator.remove();
        }
      }

      // Initialize the remaining peers
      final Map<String, PeerServer> newServers = super.initializePeers(newPeers);
      newPeerServers.putAll(newServers);
      allPeers.putAll(newServers);
    }
  }

  @Override
  public void setPeerServers(Map<String, PeerServer> peers) {
    oldPeerServers = peers;
    newPeerServers = peers;
  }

  public void setOldPeerServers(Map<String, PeerServer> peers) {
    oldPeerServers = peers;
  }

  public Map<String, PeerServer> getNewPeerServers() {
    return newPeerServers;
  }

  @Override
  public Map<String, PeerServer> getPeerServers() {
    return allPeers;
  }

  @Override
  public void resetPeers() {
    super.resetPeers(allPeers);
  }

  @Override
  public void setPeerReachable(String address) {

    PeerServer server = null;
    if ((server = allPeers.get(address)) != null) {
      server.enqueueEvent(new Event(PeerServerEventType.PEER_REACHABLE));
    }
  }

  @Override
  public void sendVoteRequestToQuorum(VoteRequest request) {
    super.broadcastVoteRequest(allPeers, request);
  }

  @Override
  public void sendAppendRequestToQuorum(AppendRequest request) {
    LOG.info("Sending an appendRequest to quorum " + c.getQuorumName() +
      " via the JointConsensusPeerManager ");
    super.broadcastAppendRequest(allPeers, request);
  }

  @Override
  public void stop() {
    super.stop(allPeers);
  }

  public void stopOldPeers() {
    for (String peer : oldPeerServers.keySet()) {
      if (newPeerServers.get(peer) == null) {
        oldPeerServers.get(peer).stop();
      }
    }
  }

  @Override
  public String getState() {
   return super.getState(allPeers);
  }

  @Override
  public AppendConsensusSessionInterface createAppendConsensusSession(
          int majorityCount, AppendRequest request, ReplicateEntriesEvent event,
          ConsensusMetrics metrics, int rank,
          boolean enableStepDownOnHigherRankCaughtUp) {
    return new JointAppendConsensusSession(c, majorityCount, request, event,
      metrics, rank, enableStepDownOnHigherRankCaughtUp,
      c.getAppendEntriesMaxTries(), c.getQuorumInfo().getPeersAsString(),
      newConfig.getPeersAsString());
  }

  @Override
  public VoteConsensusSessionInterface createVoteConsensusSession(
    int majorityCount, VoteRequest request, ConsensusMetrics metrics) {
    return new JointVoteConsensusSession(majorityCount, request, metrics, c.getQuorumInfo().getPeersAsString(),
      newConfig.getPeersAsString());
  }

  public List<QuorumInfo> getConfigs() {
    return Arrays.asList(c.getQuorumInfo(), newConfig);
  }

  @Override
  public boolean isInJointQuorumMode() {
    return true;
  }
}
