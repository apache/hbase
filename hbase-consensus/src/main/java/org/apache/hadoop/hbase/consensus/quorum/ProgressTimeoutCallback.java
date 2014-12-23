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


import org.apache.hadoop.hbase.consensus.raft.events.ProgressTimeoutEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ProgressTimeoutCallback implements TimeoutEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
    ProgressTimeoutCallback.class);
  private final RaftQuorumContext replica;

  public ProgressTimeoutCallback(final RaftQuorumContext replica) {
    this.replica = replica;
  }

  public void onTimeout() {
    LOG.info(replica + " has a progress timeout! " +
      " current edit: " +
      replica.getCurrentEdit() + ", Last AppendRequest was received at : " +
      new Date(replica.getLastAppendRequestReceivedTime()));

    if (System.currentTimeMillis() - replica.getLastAppendRequestReceivedTime() >=
      replica.getProgressTimeoutForMeMillis()) {
      replica.getConsensusMetrics().incProgressTimeouts();
      replica.offerEvent(new ProgressTimeoutEvent());
    } else {
      LOG.info(replica + " Ignoring the progress timer.");
    }
  }
}
