package org.apache.hadoop.hbase.consensus.quorum;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
