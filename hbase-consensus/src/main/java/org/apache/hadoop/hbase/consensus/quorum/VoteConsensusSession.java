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
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class VoteConsensusSession implements VoteConsensusSessionInterface {

  private final int majorityCount;
  private final VoteRequest request;
  private final EditId sessionId;

  private final Set<String> ackSet;
  private final Set<String> nackSet;
  private final Set<String> peers;

  private final long sessionStartTime;
  private final ConsensusMetrics metrics;

  private SessionResult currentResult = SessionResult.NOT_COMPLETED;

  public VoteConsensusSession(int majorityCount, final VoteRequest request,
          final ConsensusMetrics metrics, final Set<String> peers) {
    this.majorityCount = majorityCount;
    this.request = request;
    sessionId = new EditId(request.getTerm(), request.getPrevEditID().getIndex());
    ackSet = new HashSet<>();
    nackSet = new HashSet<>();
    this.metrics = metrics;
    this.metrics.incLeaderElectionAttempts();
    sessionStartTime = System.currentTimeMillis();
    this.peers = peers;
  }

  @Override
  public boolean isComplete() {
    return getResult().equals(SessionResult.NOT_COMPLETED) ? false : true;
  }

  @Override
  public SessionResult getResult() {
    generateResult();
    return currentResult;
  }

  private void generateResult() {
    if (!currentResult.equals(SessionResult.NOT_COMPLETED)) {
      return;
    }

    if (ackSet.size() >= majorityCount) {
      // Leader election succeeded
      long elapsed = System.currentTimeMillis() - sessionStartTime;
      metrics.getLeaderElectionLatency().add(elapsed, TimeUnit.MILLISECONDS);
      currentResult = SessionResult.MAJORITY_ACKED;
    } else if (nackSet.size() >= majorityCount) {
      // Leader election failed
      metrics.incLeaderElectionFailures();
      currentResult = SessionResult.STEP_DOWN;
    }

  }

  @Override
  public EditId getSessionId() {
    return sessionId;
  }

  @Override
  public void incrementAck(final EditId id, final String address) {
    assert this.sessionId.equals(id);
    if (peers.contains(address)) {
      ackSet.add(address);
    }
  }

  @Override
  public void incrementNack(final EditId id, final String address) {
    assert this.sessionId.equals(id);
    if (peers.contains(address)) {
      nackSet.add(address);
    }
  }

  @Override
  public void setVoteSessionFailed(EditId id) {
    assert this.sessionId.equals(id);
    for (String peer : peers) {
      incrementNack(id, peer);
    }
    generateResult();
  }

  public final VoteRequest getRequest() {
    return request;
  }
}
