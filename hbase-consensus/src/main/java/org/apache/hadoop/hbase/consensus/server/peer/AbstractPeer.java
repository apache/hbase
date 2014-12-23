package org.apache.hadoop.hbase.consensus.server.peer;

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


import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.fsm.Event;
import org.apache.hadoop.hbase.consensus.fsm.FiniteStateMachine;
import org.apache.hadoop.hbase.consensus.fsm.FiniteStateMachineService;
import org.apache.hadoop.hbase.consensus.fsm.FiniteStateMachineServiceImpl;
import org.apache.hadoop.hbase.consensus.fsm.Util;
import org.apache.hadoop.hbase.consensus.log.TransactionLogManager;
import org.apache.hadoop.hbase.consensus.metrics.PeerMetrics;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.MutableRaftContext;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.raft.events.AppendResponseEvent;
import org.apache.hadoop.hbase.consensus.raft.events.VoteResponseEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;
import org.apache.hadoop.hbase.consensus.server.ConsensusService;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerAppendRequestEvent;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerAppendResponseEvent;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerServerEventType;
import org.apache.hadoop.hbase.consensus.server.peer.events.PeerVoteRequestEvent;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.regionserver.RaftEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.JmxException;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NotThreadSafe
public abstract class AbstractPeer implements PeerServer {
  public int rank;

  protected MutableRaftContext raftContext;
  protected FiniteStateMachineService stateMachine;
  protected int connectionRetry = 1;
  protected int connectionRetryInterval;

  private static Logger LOG = LoggerFactory.getLogger(AbstractPeer.class);
  private final HServerAddress address;
  private String sourceString = null;
  /** Last successfully acked Edit Id by the peer */
  private EditId lastEditID = TransactionLogManager.UNDEFINED_EDIT_ID;
  private AppendRequest latestRequest;
  private volatile boolean isStopped = false;
  protected RaftEventListener dataStoreEventListener;
  private final static long DEFAULT_ASYNC_SEND_OUTLIER = 50 * 1000 * 1000; // 50ms
  private boolean useAggregateTimer;
  private long timeLastRequestWasSent = 0;
  private boolean useMuxForPSM;

  private final PeerMetrics metrics;

  public AbstractPeer(final HServerAddress address, final int rank,
                      final MutableRaftContext replica,
                      final Configuration conf) {
    this.rank = rank;
    this.address = address;
    this.raftContext = replica;
    connectionRetry = conf.getInt(
      HConstants.RAFT_PEERSERVER_CONNECTION_RETRY_CNT, 1);
    connectionRetryInterval = conf.getInt(
      HConstants.RAFT_PEERSERVER_CONNECTION_RETRY_INTERVAL, 1000);

    // MBean name properties can not contain colons, so replace with a period.
    // TODO (arjen): Make sure this works for IPv6.
    String peerId = address.getHostNameWithPort().replace(':', '.');
    metrics = replica.getConsensusMetrics().createPeerMetrics(peerId);
    useAggregateTimer = conf.getBoolean(
      HConstants.QUORUM_USE_AGGREGATE_TIMER_KEY,
      HConstants.QUORUM_USE_AGGREGATE_TIMER_DEFAULT);
    useMuxForPSM = conf.getBoolean(HConstants.USE_FSMMUX_FOR_PSM,
      HConstants.USE_FSMMUX_FOR_PSM_DEFAULT);
  }

  public HServerAddress getAddress() {
    return address;
  }

  @Override
  public int getRank() {
    return rank;
  }

  @Override
  public void setRank(int rank) {
    this.rank = rank;
  }

  @Override
  public AppendRequest getLatestRequest() {
    return latestRequest;
  }

  @Override
  public void setLatestRequest(final AppendRequest latestRequest) {
    this.latestRequest = latestRequest;
  }

  protected MutableRaftContext getRaftContext() {
    return raftContext;
  }

  @Override
  public Configuration getConf() {
    return raftContext.getConf();
  }

  @Override
  public void registerDataStoreEventListener(RaftEventListener listener) {
    this.dataStoreEventListener = listener;
  }

  @Override
  public void updatePeerAvailabilityStatus(boolean isAvailable) {
    if (this.dataStoreEventListener != null) {
      this.dataStoreEventListener.updatePeerAvailabilityStatus(
        this.address.getHostAddressWithPort(), isAvailable);
    } else {
      LOG.warn("dataStoreEventListener has not been registered for this peer: " + toString());
    }
  }

  public FiniteStateMachineService getStateMachineService() {
    return stateMachine;
  }

  abstract protected ConsensusService getConsensusServiceAgent();
  abstract protected void resetConnection()
    throws ExecutionException, InterruptedException, TimeoutException;
  abstract protected boolean clearConsensusServiceAgent(ConsensusService localAgent);

  public void setStateMachineService(final FiniteStateMachineService fsm) {
    stateMachine = fsm;
  }

  public void initializeStateMachine() {
    if (stateMachine == null) {
      FiniteStateMachine fsm = new PeerStateMachine(
        "PeerFSM-" + raftContext.getQuorumName() + ":" +
          raftContext.getRanking() + "-->" +
          this.address.getHostAddressWithPort() +  ":" + this.rank,
        this);

      FiniteStateMachineService fsmService;
      if (useMuxForPSM) {
        fsmService = ((RaftQuorumContext) raftContext)
          .createFiniteStateMachineService(fsm);
      } else {
        fsmService = new FiniteStateMachineServiceImpl(fsm);
      }
      setStateMachineService(fsmService);
    }
    stateMachine.offer(new Event(PeerServerEventType.START));
  }

  @Override
  public void initialize() {
    isStopped = false;
    lastEditID = raftContext.getLastLogIndex();
    try {
      raftContext.getConsensusMetrics().exportPeerMetrics(metrics);
    } catch (JmxException e) {
      LOG.warn(String.format("Could not export metrics MBean: %s, reason: %s",
              metrics, e.getReason()));
    }
    initializeStateMachine();
  }

  @Override
  public void resetPeerContext() {
    lastEditID = raftContext.getLastLogIndex();
    latestRequest = null;
  }

  @Override
  public void sendAppendEntries(AppendRequest request) {
    // Append the event to the state machine
    stateMachine.offer(new PeerAppendRequestEvent(request));
  }

  @Override
  public void sendRequestVote(VoteRequest request) {
    // Append the event to the state machine
    stateMachine.offer(new PeerVoteRequestEvent(request));
  }

  @Override
  public String getPeerServerName() {
    return address.getHostAddressWithPort();
  }

  @Override
  public String toString() {
    return "[PeerServer: " + this.address + ", rank:" + this.rank + "]";
  }

  @Override
  public MutableRaftContext getQuorumContext() {
    return raftContext;
  }

  @Override
  public EditId getLastEditID() {
    return lastEditID;
  }

  @Override
  public void enqueueEvent(final Event e) {
    stateMachine.offer(e);
  }

  @Override
  public void setLastEditID(final EditId lastEditID) {
    this.lastEditID = lastEditID;
  }

  @Override
  public void stop() {
    isStopped = true;
    stateMachine.offer(new Event(PeerServerEventType.HALT));
    stateMachine.shutdown();
    if (!Util.awaitTermination(
        stateMachine, 3, 3, TimeUnit.SECONDS)) {
      LOG.error("State Machine Service " + stateMachine.getName() +
              " did not shutdown");
    }
    try {
      raftContext.getConsensusMetrics().unexportPeerMetrics(metrics);
    } catch (JmxException e) {
      LOG.warn(String.format(
                      "Could not un-export metrics MBean: %s, reason: %s",
                      metrics, e.getReason()));
    }
  }

  private String getTargetString() {
    return this.toString();
  }

  private String getSourceString() {
    // we don't have to synchronize it
    if (sourceString == null) {
      if (getRaftContext() != null) {
        sourceString = getRaftContext().getMyAddress();
        return sourceString;
      } else {
        return "?:?";
      }
    } else {
      return sourceString;
    }
  }

  @Override
  public void sendAppendRequestWithCallBack(final AppendRequest request) {
    if (isStopped) {
      if (LOG.isTraceEnabled() || request.isTraceable()) {
        LOG.debug("[AppendRequest Trace] " + getSourceString() + " not sending " + request + " to "
          + getTargetString() + "; because we are stopped!");
      }
      return;
    }

    final ConsensusService localAgent = getConsensusServiceAgent();
    if (localAgent == null) {
      LOG.warn("Failed to get localAgent for " + this + " for the request " + request);
      triggerRPCErrorEvent();
      return;
    }

    assert request.validateFields();
    assert request.getLeaderId() != null;

    if (LOG.isTraceEnabled() || request.isTraceable()) {
      LOG.debug("[AppendRequest Trace] " + getSourceString() + ": sending " + request + " to " +
        getTargetString());
    }
    final long start = System.nanoTime();

    long time = System.currentTimeMillis();

    if (time - timeLastRequestWasSent > 1000) {
      LOG.warn(getSourceString() + "-->" +
        getTargetString() + " did not send append request in last " +
        (time - timeLastRequestWasSent) + " ms . Current req " + request);
    }
    timeLastRequestWasSent = time;

    RaftUtil.getThriftClientManager().getNiftyChannel(localAgent).executeInIoThread(
      new Runnable() {
        @Override public void run() {
          try {

            ListenableFuture<AppendResponse> futureResponse =
              localAgent.appendEntries(request);
            Futures.addCallback(futureResponse, new FutureCallback<AppendResponse>() {
              @Override
              public void onSuccess(AppendResponse response) {
                getQuorumContext().offerEvent(new AppendResponseEvent(response));
                metrics.getAppendEntriesLatency().add(System.nanoTime() - start,
                  TimeUnit.NANOSECONDS);
                if (LOG.isTraceEnabled() || request.isTraceable()) {
                  LOG.debug("[AppendRequest Trace] Received the response: " + response);
                }
                getStateMachineService().offer(new PeerAppendResponseEvent(response));
              }

              @Override
              public void onFailure(Throwable e) {
                handleFailure(localAgent, request, e);
              }
            });
          } catch (Throwable t) {
            LOG.error("Could not send async request", t);
            handleFailure(localAgent, request, t);
          }
        }
      });

  }

  private void handleFailure(ConsensusService localAgent, AppendRequest request, Throwable e) {
    if (LOG.isDebugEnabled()) {
      if (e instanceof ThriftHBaseException) {
        e = ((ThriftHBaseException)e).getServerJavaException();
      }

      LOG.debug(request + " (" + getSourceString() + "-->" +
          getTargetString() + ") FAILED due to ", e);
    }
    if (shouldRetry(e)) {
      appendRequestFailed(localAgent);
    }
  }

  private boolean shouldRetry(Throwable e) {
    return RaftUtil.isNetworkError(e);
  }

  protected void appendRequestFailed(ConsensusService localAgent) {
    metrics.incAppendEntriesFailures();

    // If we were the first one to fail and continue to retry
    if (clearConsensusServiceAgent(localAgent)) {
      // Sending no response to process, will automatically let it to recover from
      // the last acked Edit id.
      stateMachine.offer(new PeerAppendResponseEvent(null));
    }
  }

  @Override
  public void sendVoteRequestWithCallBack(final VoteRequest request) {
    if (isStopped) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(getSourceString() + " not sending " + request + " to "
          + getTargetString() + "; because we are stopped!");
      }
      return;
    }

    final ConsensusService localAgent = getConsensusServiceAgent();
    if (localAgent == null) {
      voteRequestFailed(localAgent);
      LOG.warn("Failed to get localAgent for " + this);
      this.triggerRPCErrorEvent();
      return;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(getSourceString() + ": sending " + request + " to " +
        getTargetString());
    }

    RaftUtil.getThriftClientManager().getNiftyChannel(localAgent).executeInIoThread(
      new Runnable() {
        @Override public void run() {
          try {
            ListenableFuture<VoteResponse> futureResponse =
              localAgent.requestVote(request);
            if (futureResponse == null) {
              LOG.error(getSourceString() + ": got a null response when sending " +
                request + " to " + getTargetString());
              return;
            }

            Futures.addCallback(futureResponse, new FutureCallback<VoteResponse>() {
              @Override
              public void onSuccess(VoteResponse response) {
                // Add the event to the state machine
                getRaftContext().offerEvent(new VoteResponseEvent(response));
              }

              @Override
              public void onFailure(Throwable e) {
                LOG.error(request + " (" + getSourceString() + "-->" +
                  getTargetString() + ") FAILED due to ", e);
                voteRequestFailed(localAgent);
              }
            });
          } catch (Throwable e) {
            voteRequestFailed(localAgent);
          }

        }
      });
 }

  protected void voteRequestFailed(ConsensusService localAgent) {
    metrics.incVoteRequestFailures();
    getRaftContext().offerEvent(new VoteResponseEvent(
        new VoteResponse(this.getPeerServerName(),
          HConstants.UNDEFINED_TERM_INDEX, VoteResponse.VoteResult.FAILURE)
    ));
    if (localAgent != null) {
      clearConsensusServiceAgent(localAgent);
    }
  }

  protected void triggerRPCErrorEvent() {
    metrics.incRPCErrorEvents();
    // Create an RPCError Event and add it to the SM
    stateMachine.offer(new Event(PeerServerEventType.PEER_RPC_ERROR));
  }

  @Override
  public PeerMetrics getMetrics() {
    return metrics;
  }

  public void calculateAndSetAppendLag() {
    getMetrics().setAppendEntriesLag(
            latestRequest.getLogId(0).getIndex() - getLastEditID().getIndex());
  }
}
