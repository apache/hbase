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
