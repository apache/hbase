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


import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSessionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.AppendConsensusSession;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;

public class AckClient extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(AckClient.class);

  public AckClient(MutableRaftContext context) {
    super(RaftStateType.ACK_CLIENT, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);

    AppendConsensusSessionInterface session = c.getAppendSession(c.getCurrentEdit());

    assert session != null;

    c.advanceCommittedIndex(session.getSessionId());
    c.setAppendSession(null);
  }
}
