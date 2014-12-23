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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class represents a session of an in-progress AppendRequest, and holds
 * information such as if the session is complete, majority acked, or it failed
 * for some reason.
 */
public class AppendConsensusSession implements AppendConsensusSessionInterface {
  private static Logger LOG = LoggerFactory.getLogger(AppendConsensusSession.class);
  private final int majorityCount;
  private final EditId id;
  private final AppendRequest request;
  private final ReplicateEntriesEvent replicateEntriesEvent;

  /** The key of ackMap is the peer host name and value is the time elapsed for the ack */
  private final Map<String, Long> ackMap = new HashMap<>();
  private final Set<String> lagSet = new HashSet<>();
  private final Set<String> highTermSet = new HashSet<>();
  private final Set<String> peers;
  private final ConsensusMetrics metrics;
  private long sessionStartTime;

  private final int currentRank;

  /** This boolean flag indicates whether there is any higher rank peer caught up the transacations
   * and potentially ready for taking over the leadership. */
  private boolean isHigherRankPeerCaughtup = false;

  private final boolean enableStepDownOnHigherRankCaughtUp;

  private static final long WRITE_OUTLIERS_DEFAULT_MS = 5; // 5 ms

  private SessionResult currentResult = SessionResult.NOT_COMPLETED;

  private static final int APPEND_RETRY_MAX = 5;

  private int tries = 0;
  private final int maxTries;

  ImmutableRaftContext c;

  /**
   * To construct a AppendConsensusSession.
   * @param majorityCount The majority number of the quorum size.
   * @param request The append request.
   * @param event   The ReplicatedEntriesEvent for this corresponding appendRequest.
   * @param rank The rank of the current peer
   * @param enableStepDownOnHigherRankCaughtUp Whether to step down voluntarily if the higher rank peer
   *                                       has caught up.
   */
  public AppendConsensusSession(ImmutableRaftContext c,
                                int majorityCount,
                                final AppendRequest request,
                                final ReplicateEntriesEvent event,
                                final ConsensusMetrics metrics,
                                final int rank,
                                final boolean enableStepDownOnHigherRankCaughtUp,
                                final int maxTries,
                                final Set<String> peers) {
    this.c = c;
    this.majorityCount = majorityCount;
    this.request = request;
    assert request.validateFields();
    assert request.logCount() == 1;
    this.id = request.getLogId(0).clone(); //??
    this.replicateEntriesEvent = event;
    this.metrics = metrics;
    this.currentRank = rank;
    this.enableStepDownOnHigherRankCaughtUp = enableStepDownOnHigherRankCaughtUp;
    this.peers = peers;
    this.maxTries = maxTries;
    this.sessionStartTime = System.nanoTime();
    resetInvariants();
  }

  @Override
  public void reset() {
    resetInvariants();

    if (++tries >= APPEND_RETRY_MAX) {
      this.request.enableTraceable();
    }
  }

  private void resetInvariants() {
    ackMap.clear();
    lagSet.clear();
    highTermSet.clear();
    isHigherRankPeerCaughtup = false;
    currentResult = SessionResult.NOT_COMPLETED;
  }

  @Override
  public boolean isComplete() {
    return getResult().equals(SessionResult.NOT_COMPLETED) ? false : true;
  }

  private void traceLogAppendOutliers() {
    long elapsed = System.nanoTime() - sessionStartTime;
    if (elapsed > (WRITE_OUTLIERS_DEFAULT_MS * 1000)) {
      StringBuffer sb = new StringBuffer();
      sb.append("AppendConsensusSession outlier: " + request.toString() +
        " took " + elapsed + " ns; [");
      for (Map.Entry<String, Long> entry : ackMap.entrySet()) {
        sb.append(entry.getKey() + " -> " + entry.getValue() + " ; ");
      }
      sb.append("]");
      if (LOG.isTraceEnabled()) {
        LOG.trace(sb.toString());
      }
    }
  }

  private void generateResult() {
    if (!currentResult.equals(SessionResult.NOT_COMPLETED)) {
      return;
    }

    // Check if the majority has been reached and the I (leader) have ACK-ed too.
    // We do this because we cannot serve reads until the leader has ACK-ed.
    // If I am not part of the peers, my vote doesn't matter.
    if (ackMap.size() >= majorityCount
      && (!peers.contains(c.getMyAddress()) ||
      ackMap.containsKey(c.getMyAddress()))) {
      long elapsed = System.nanoTime() - sessionStartTime;
      metrics.getAppendEntriesLatency().add(elapsed, TimeUnit.NANOSECONDS);

      if (isStepDownOnHigherRankCaughtUpEnabled() && isHigherRankPeerCaughtup) {
        // When there is at least one higher rank peer caught up all the transactions,
        // the current leader needs to step down voluntarily.
        metrics.incHigherRankCaughtUpStepDown();
        currentResult = SessionResult.STEP_DOWN;
        LOG.debug("Going to step down voluntarily from leadership");
      } else {
        // Otherwise, return majority acked.
        if (LOG.isTraceEnabled()) {
          traceLogAppendOutliers();
        }
        currentResult = SessionResult.MAJORITY_ACKED;
      }

    } else if (highTermSet.size() >= majorityCount) {
      metrics.incAppendEntriesStepDown();
      currentResult =  SessionResult.STEP_DOWN;
    } else if (lagSet.size() + highTermSet.size() >= majorityCount) {
      metrics.incAppendEntriesRetries();
      currentResult = SessionResult.RETRY;
    }
  }

  private boolean isStepDownOnHigherRankCaughtUpEnabled() {
    return enableStepDownOnHigherRankCaughtUp;
  }

  @Override
  public SessionResult getResult() {
    generateResult();
    if (request.isTraceable()) {
      LOG.debug(String.format("[AppendRequest Trace] %s and current result is %s",
        request.toString(), currentResult.toString()));
    }
    return currentResult;
  }

  @Override
  public EditId getSessionId() {
    return id;
  }

  @Override
  public void cancel() {
    this.metrics.incHeartBeatCanceled();
    this.currentResult = SessionResult.CANCELED;
  }

  @Override
  public boolean isTimeout() {
    boolean timedout = tries >= maxTries;
    if (timedout) {
      LOG.info(String.format("Append Request (%s) timed out", this.request.toString()));
    }
    return timedout;
  }

  @Override
  public AppendRequest getAppendRequest() {
    return request;
  }

  @Override
  public ReplicateEntriesEvent getReplicateEntriesEvent() {
    return replicateEntriesEvent;
  }

  @Override
  public void incrementAck(final EditId id, final String address, final int rank,
                           boolean canTakeover) {
    assert this.id.equals(id);
    if (peers.contains(address)) {
      ackMap.put(address, System.nanoTime() - sessionStartTime);
      if (rank > currentRank && canTakeover) {
        isHigherRankPeerCaughtup = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Append Request (%s) received a higher" +
            "rank ack from %s", request.toString(), address));
        }
      }
    }
  }

  @Override
  public void incrementHighTermCnt(final EditId id, final String address) {
    assert this.id.equals(id);
    if (peers.contains(address)) {
      highTermSet.add(address);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Append Request (%s) received a higher term" +
            "response from %s", request.toString(), address));
      }
    }
  }

  @Override
  public void incrementLagCnt(final EditId id, final String address) {
    assert this.id.equals(id);
    if (peers.contains(address)) {
      lagSet.add(address);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Append Request (%s) received a nak response " +
          "from %s", request.toString(), address));
      }
    }
  }
}
