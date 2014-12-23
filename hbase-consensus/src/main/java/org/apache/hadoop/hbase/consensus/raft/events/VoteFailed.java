package org.apache.hadoop.hbase.consensus.raft.events;

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


import org.apache.hadoop.hbase.consensus.fsm.Conditional;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.*;

public class VoteFailed implements Conditional {
  ImmutableRaftContext c;

  public VoteFailed(final ImmutableRaftContext c) {
    this.c = c;
  }

  @Override
  public boolean isMet(Event e) {
    // Get the current outstanding election session
    VoteConsensusSessionInterface session = this.c.getElectionSession(this.c.getCurrentEdit());

    // Handle the stale request
    if (session == null) {
      return false;
    }

    // return true if the majority has sent the step down response
    return session.getResult().equals(SessionResult.STEP_DOWN);
  }
}

