package org.apache.hadoop.hbase.consensus.quorum;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.server.peer.PeerConsensusServer;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServer;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractPeerManager implements PeerManagerInterface {
  // Map of last acked edits of each peer
  protected Map<String, Long> lastAckedIndexMap;

  // Min acked id across all peers
  protected volatile long minAckedIndexAcrossAllPeers =
    HConstants.UNDEFINED_TERM_INDEX;
  protected final MutableRaftContext c;

  public long getMinAckedIndexAcrossAllPeers() {
    return minAckedIndexAcrossAllPeers;
  }

  public Map<String, Long> getLastAckedIndexMap() {
    return lastAckedIndexMap;
  }

  public void updateLastPeerAckedIdMap(Map<String, Long> currentMap) {
    for (String peer : this.lastAckedIndexMap.keySet()) {
      if (currentMap.containsKey(peer)) {
        lastAckedIndexMap.put(peer, currentMap.get(peer));
      }
    }
  }

  public AbstractPeerManager(final MutableRaftContext c) {
    this.c = c;
    minAckedIndexAcrossAllPeers = HConstants.UNDEFINED_TERM_INDEX;
    lastAckedIndexMap = new HashMap<>();
  }

  public void updatePeerAckedId(String address, EditId remoteEdit) {
    lastAckedIndexMap.put(address, remoteEdit.getIndex());

    minAckedIndexAcrossAllPeers =
      Collections.min(lastAckedIndexMap.values());
  }

  /**
   * Returns the min across all the persisted index across peers in the quorum,
   * and the locally flushed edits.
   */
  public long getMinUnPersistedIndexAcrossQuorum() {
    return minAckedIndexAcrossAllPeers;
  }

  public void setMinAckedIndexAcrossAllPeers(long index) {
    minAckedIndexAcrossAllPeers = index;
  }

  // Utility functions
  protected void resetPeers(final Map<String, PeerServer> peers) {
    for (PeerServer server : peers.values()) {
      server.resetPeerContext();
    }
  }

  // Utility functions
  protected Map<String, PeerServer> initializePeers(final Map<HServerAddress, Integer> peers) {

    Map<String, PeerServer> peerServers = new HashMap<>();
    // Initialize the RaftQuorum by setting up the PeerServer
    for (HServerAddress hostAddress : peers.keySet()) {
      if (!hostAddress.equals(c.getServerAddress())) {
        // Generate the PeerServer port: RegionServer Port + fixed port jump
        HServerAddress peerAddress = RaftUtil
          .getLocalConsensusAddress(hostAddress);
        int peerRank = peers.get(hostAddress);

        peerServers.put(peerAddress.getHostAddressWithPort(),
          new PeerConsensusServer(peerAddress, peerRank, c, c.getConf()));
        lastAckedIndexMap.put(peerAddress.getHostAddressWithPort(),
          HConstants.UNDEFINED_TERM_INDEX);
      }
    }

    for (PeerServer peer : peerServers.values()) {
      peer.initialize();
      if (c.getDataStoreEventListener() != null) {
        peer.registerDataStoreEventListener(c.getDataStoreEventListener());
      }
    }
    return peerServers;
  }

  protected void broadcastVoteRequest(final Map<String, PeerServer> peerServers,
                                      final VoteRequest request) {
    for (PeerServer peerServer : peerServers.values()) {
      peerServer.sendRequestVote(request);
    }
  }

  protected void broadcastAppendRequest(final Map<String, PeerServer> peerServers,
                                        AppendRequest request) {
    for (PeerServer peerServer : peerServers.values()) {
      peerServer.sendAppendEntries(request);
    }
  }

  protected void stop(final Map<String, PeerServer> peerServers) {
    for (PeerServer peer : peerServers.values()) {
      peer.stop();
    }
  }

  protected String getState(final Map<String, PeerServer> peerServers) {
    StringBuilder sb = new StringBuilder();
    for (PeerServer peer : peerServers.values()) {
      sb.append(peer.getRank()).append("->").append(peer.getLastEditID().getIndex())
          .append(" ; ");
    }
    return sb.toString();
  }

  @Override
  public boolean isInJointQuorumMode() {
    return false;
  }
}
