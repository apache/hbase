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


import org.apache.hadoop.hbase.consensus.quorum.ReseedRequest;
import org.apache.hadoop.hbase.consensus.raft.events.ReseedRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;

import java.io.IOException;

public class HandleReseedRequest extends RaftState {
  private static Logger LOG = LoggerFactory.getLogger(HandleVoteRequest.class);

  public HandleReseedRequest(MutableRaftContext context) {
    super(RaftStateType.HANDLE_RESEED_REQUEST, context);
  }

  public void onEntry(final Event e) {
    super.onEntry(e);

    ReseedRequest request = ((ReseedRequestEvent)e).getRequest();

    // In case you are the leader, just acknowledge it and move on
    if (c.isLeader()) {
      request.setResponse(true);
      return;
    }

    try {
      c.reseedStartIndex(request.getReseedIndex());
    } catch (IOException e1) {
      LOG.error("Cannot complete the reseed request ", e1);
      request.getResult().setException(e1);
    }

    request.getResult().set(true);
    c.candidateStepDown();
  }
}
