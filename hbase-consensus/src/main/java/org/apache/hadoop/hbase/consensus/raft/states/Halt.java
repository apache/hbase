package org.apache.hadoop.hbase.consensus.raft.states;

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


import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

public class Halt extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(Halt.class);

  public Halt(MutableRaftContext context) {
    super(RaftStateType.HALT, context);
  }

  public void onEntry(final Event e) {
    LOG.error("Entering HALT state " + c + " Current State: " + t + ", OnEntryEvent: " + e);
    super.onEntry(e);
    c.clearLeader();
    c.leaderStepDown();
    c.candidateStepDown();
    c.getProgressTimer().stop();
    c.getHeartbeatTimer().stop();
    c.getConsensusMetrics().setRaftState(PeerStatus.RAFT_STATE.HALT);
  }
}
