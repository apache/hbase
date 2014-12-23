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

import java.util.Set;

public class JointAppendConsensusSession implements AppendConsensusSessionInterface {

  private AppendConsensusSession oldConfigSession;
  private AppendConsensusSession newConfigSession;

  public JointAppendConsensusSession(ImmutableRaftContext c,
                                int majorityCount,
                                final AppendRequest request,
                                final ReplicateEntriesEvent event,
                                final ConsensusMetrics metrics,
                                final int rank,
                                final boolean enableStepDownOnHigherRankCaughtUp,
                                final int maxTries,
                                final Set<String> oldPeers,
                                final Set<String> newPeers) {
    oldConfigSession = new AppendConsensusSession(c, majorityCount, request,
      event, metrics, rank, enableStepDownOnHigherRankCaughtUp,
            maxTries, oldPeers);
    newConfigSession = new AppendConsensusSession(c, majorityCount, request,
      event, metrics, rank, enableStepDownOnHigherRankCaughtUp,
            maxTries, newPeers);
  }

  @Override
  public boolean isComplete() {
    return oldConfigSession.isComplete() && newConfigSession.isComplete();
  }

  @Override
  public SessionResult getResult() {
    if (oldConfigSession.getResult() != newConfigSession.getResult()) {
      return SessionResult.NOT_COMPLETED;
    }

    return newConfigSession.getResult();
  }

  @Override
  public EditId getSessionId() {
    return oldConfigSession.getSessionId();
  }

  @Override
  public void incrementAck(final EditId id, final String address, final int rank,
                           boolean canTakeover) {
    oldConfigSession.incrementAck(id, address, rank, canTakeover);
    newConfigSession.incrementAck(id, address, rank, canTakeover);
  }

  @Override
  public void incrementHighTermCnt(final EditId id, final String address) {
    oldConfigSession.incrementHighTermCnt(id, address);
    newConfigSession.incrementHighTermCnt(id, address);
  }

  @Override
  public void incrementLagCnt(final EditId id, final String address) {
    oldConfigSession.incrementLagCnt(id, address);
    newConfigSession.incrementLagCnt(id, address);
  }

  @Override
  public ReplicateEntriesEvent getReplicateEntriesEvent() {
    return oldConfigSession.getReplicateEntriesEvent();
  }

  @Override
  public void reset() {
    oldConfigSession.reset();
    newConfigSession.reset();
  }

  @Override
  public AppendRequest getAppendRequest() {
    return oldConfigSession.getAppendRequest();
  }

  @Override
  public void cancel() {
    oldConfigSession.cancel();
    newConfigSession.cancel();
  }

  @Override
  public boolean isTimeout() {
    return oldConfigSession.isTimeout() || newConfigSession.isTimeout();
  }
}
