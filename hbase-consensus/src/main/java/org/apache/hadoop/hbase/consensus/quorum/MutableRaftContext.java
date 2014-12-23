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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.util.Arena;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Set of methods used by the states in the raft protocol to update the state
 * machine on various events;
 */
public interface MutableRaftContext extends ImmutableRaftContext {
  /**
   * Updates the current edit
   * @param id
   */
  void setCurrentEditId(final EditId id);

  /**
   * Updates the last committed edit id.
   * @param id
   */
  void advanceCommittedIndex(final EditId id);

  /**
   * Set the leader id.
   * @param hostId
   */
  void setLeader(final ConsensusHost hostId);

  /**
   * Updates the last committed edit id.
   * @param id
   */
  void setPreviousEditId(final EditId id);

  /**
   * Set the last voted for id.
   * @param hostId
   */
  ListenableFuture<Void> setVotedFor(final ConsensusHost hostId);

  /**
   * clear the leader id.
   */
  void clearLeader();

  /**
   * Clear the last voted for id.
   */
  void clearVotedFor();

  void appendToLog(final EditId currLogId, final long commitIndex,
                   final ByteBuffer data);

  void setElectionSession(VoteConsensusSessionInterface session);

  void setAppendSession(AppendConsensusSessionInterface session);

  void sendVoteRequestToQuorum(VoteRequest request);

  void truncateLogEntries(final EditId lastValidEntryId) throws IOException;
  boolean offerEvent(final Event e);

  Timer getHeartbeatTimer();

  Timer getProgressTimer();

  ListenableFuture<?> sendAppendRequest(ReplicateEntriesEvent event);

  void setLastAppendRequestReceivedTime(long timeMillis);

  ListenableFuture<?> sendEmptyAppendRequest();

  void leaderStepDown();

  void candidateStepDown();

  void resendOutstandingAppendRequest();

  void resetPeers();

  void setPeerReachable(String address);

  String getLeaderNotReadyMsg();

  void updatePeerAckedId(String address, EditId remoteEdit);

  void setMinAckedIndexAcrossAllPeers(long index);

  void setUpdateMembershipRequest(
    QuorumMembershipChangeRequest request);

  PeerManagerInterface getPeerManager();

  HServerAddress getServerAddress();

  void updateToJointQuorumMembership(final QuorumInfo config) throws IOException;

  void updateToNewQuorumMembership(final QuorumInfo config)
    throws IOException;

  void handleQuorumChangeRequest(final ByteBuffer buffer) throws IOException;

  Arena getArena();

  void reseedStartIndex(long index) throws IOException;

  void setQuorumInfo(final QuorumInfo update);

  void cleanUpJointStates();

  boolean canTakeOver();

  ExecutorService getExecServiceForThriftClients();
}
