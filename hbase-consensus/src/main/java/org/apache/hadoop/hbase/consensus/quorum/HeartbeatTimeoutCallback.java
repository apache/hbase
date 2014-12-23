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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import java.nio.ByteBuffer;

public class
  HeartbeatTimeoutCallback implements TimeoutEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
          HeartbeatTimeoutCallback.class);
  private final RaftQuorumContext replica;
  private static final ReplicateEntriesEvent HEARTBEAT_EVENT = new ReplicateEntriesEvent(true,
      ByteBuffer.allocate(1));

  public HeartbeatTimeoutCallback(final RaftQuorumContext replica) {
    this.replica = replica;
  }

  @Override
  public void onTimeout() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("HeartBeat Triggered on " + replica);
    }

    // When there is no append request for a long time, in order to avoid
    // progress timeouts, we offer heartbeats which are no-ops.
    replica.getConsensusMetrics().incHeartBeatTimeouts();
    replica.offerEvent(HEARTBEAT_EVENT);
  }
}
