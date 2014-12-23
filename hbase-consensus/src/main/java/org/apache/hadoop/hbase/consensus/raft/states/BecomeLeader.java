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


import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.quorum.JointConsensusPeerManager;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.QuorumMembershipChangeRequest;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BecomeLeader extends RaftAsyncState {
  private static Logger LOG = LoggerFactory.getLogger(BecomeLeader.class);

  private ListenableFuture<?> sendAppendRequestFuture = null;

  public BecomeLeader(MutableRaftContext context) {
    super(RaftStateType.BECOME_LEADER, context);
  }

  @Override
  public boolean isComplete() {
    return sendAppendRequestFuture == null || sendAppendRequestFuture.isDone();
  }

  public void onEntry(final Event e) {
    super.onEntry(e);
    // Clear the election session and sanity check the append session
    c.setElectionSession(null);
    assert c.getOutstandingAppendSession() == null;

    // Set up as the leader
    this.c.setLeader(new ConsensusHost(this.c.getCurrentEdit().getTerm(), this.c.getMyAddress()));
    this.c.getConsensusMetrics().setRaftState(PeerStatus.RAFT_STATE.LEADER);
    assert c.isLeader();

    if (LOG.isInfoEnabled()) {
      LOG.info(c + " is leader with edit: " + c.getCurrentEdit());
    }

    c.getProgressTimer().stop();
    c.resetPeers();
    c.getHeartbeatTimer().start();

    if (c.getDataStoreEventListener() != null) {
      // Notify the data store to start serving reads/writes
      try {
        sendAppendRequestFuture = c.sendAppendRequest(new ReplicateEntriesEvent(false,
          c.getDataStoreEventListener().becameLeader()));
      } catch (IOException ioe) {
        LOG.error(String.format(
                "%s Caught IOException while generating AppendEntries."
                + " This is very unexpected, so stepping down.", c), ioe);
        c.clearLeader();
      }
    } else {
      sendAppendRequestFuture = c.sendEmptyAppendRequest();
    }

    // We are in middle of Quorum Membership Change, lets continue it
    if (c.getPeerManager() instanceof JointConsensusPeerManager) {
      QuorumMembershipChangeRequest request =
        new QuorumMembershipChangeRequest(c.getPeerManager().getConfigs().get(1));
      c.setUpdateMembershipRequest(request);
      request.setCurrentState(
        QuorumMembershipChangeRequest.QuorumMembershipChangeState.JOINT_CONFIG_COMMIT_IN_PROGRESS);
    }
  }
}
