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

import java.util.Set;

public class JointVoteConsensusSession implements VoteConsensusSessionInterface {
  VoteConsensusSession oldConfigSession;
  VoteConsensusSession newConfigSession;

  public JointVoteConsensusSession(int majorityCount, final VoteRequest request,
                                   final ConsensusMetrics metrics,
                                   final Set<String> oldPeers,
                                   final Set<String> newPeers) {
    oldConfigSession = new VoteConsensusSession(majorityCount, request, metrics, oldPeers);
    newConfigSession = new VoteConsensusSession(majorityCount, request, metrics, newPeers);
  }


  @Override public void incrementAck(EditId id, String address) {
    oldConfigSession.incrementAck(id, address);
    newConfigSession.incrementAck(id, address);
  }

  @Override public void incrementNack(EditId id, String address) {
    oldConfigSession.incrementNack(id, address);
    newConfigSession.incrementNack(id, address);
  }

  @Override public void setVoteSessionFailed(EditId id) {
    oldConfigSession.setVoteSessionFailed(id);
    newConfigSession.setVoteSessionFailed(id);
  }

  @Override public VoteRequest getRequest() {
    return oldConfigSession.getRequest();
  }

  @Override public boolean isComplete() {
    return oldConfigSession.isComplete() && newConfigSession.isComplete();
  }

  @Override public SessionResult getResult() {
    if (oldConfigSession.getResult() != newConfigSession.getResult()) {
      return SessionResult.NOT_COMPLETED;
    }

    return oldConfigSession.getResult();
  }

  @Override public EditId getSessionId() {
    return oldConfigSession.getSessionId();
  }
}
