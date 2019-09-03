/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.htrace.core.Tracer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The context, and return value, for a single submit/submitAll call.
 * Note on how this class (one AP submit) works. Initially, all requests are split into groups
 * by server; request is sent to each server in parallel; the RPC calls are not async so a
 * thread per server is used. Every time some actions fail, regions/locations might have
 * changed, so we re-group them by server and region again and send these groups in parallel
 * too. The result, in case of retries, is a "tree" of threads, with parent exiting after
 * scheduling children. This is why lots of code doesn't require any synchronization.
 */
@InterfaceAudience.Private
class AsyncRequestFutureImpl<CResult> implements AsyncRequestFuture {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncRequestFutureImpl.class);

  private RetryingTimeTracker tracker;

  /**
   * Runnable (that can be submitted to thread pool) that waits for when it's time
   * to issue replica calls, finds region replicas, groups the requests by replica and
   * issues the calls (on separate threads, via sendMultiAction).
   * This is done on a separate thread because we don't want to wait on user thread for
   * our asynchronous call, and usually we have to wait before making replica calls.
   */
  private final class ReplicaCallIssuingRunnable implements Runnable {
    private final long startTime;
    private final List<Action> initialActions;

    public ReplicaCallIssuingRunnable(List<Action> initialActions, long startTime) {
      this.initialActions = initialActions;
      this.startTime = startTime;
    }

    @Override
    public void run() {
      boolean done = false;
      if (asyncProcess.primaryCallTimeoutMicroseconds > 0) {
        try {
          done = waitUntilDone(startTime * 1000L + asyncProcess.primaryCallTimeoutMicroseconds);
        } catch (InterruptedException ex) {
          LOG.error("Replica thread interrupted - no replica calls {}", ex.getMessage());
          return;
        }
      }
      if (done) return; // Done within primary timeout
      Map<ServerName, MultiAction> actionsByServer = new HashMap<>();
      List<Action> unknownLocActions = new ArrayList<>();
      if (replicaGetIndices == null) {
        for (int i = 0; i < results.length; ++i) {
          addReplicaActions(i, actionsByServer, unknownLocActions);
        }
      } else {
        for (int replicaGetIndice : replicaGetIndices) {
          addReplicaActions(replicaGetIndice, actionsByServer, unknownLocActions);
        }
      }
      if (!actionsByServer.isEmpty()) {
        sendMultiAction(actionsByServer, 1, null, unknownLocActions.isEmpty());
      }
      if (!unknownLocActions.isEmpty()) {
        actionsByServer = new HashMap<>();
        for (Action action : unknownLocActions) {
          addReplicaActionsAgain(action, actionsByServer);
        }
        // Some actions may have completely failed, they are handled inside addAgain.
        if (!actionsByServer.isEmpty()) {
          sendMultiAction(actionsByServer, 1, null, true);
        }
      }
    }

    /**
     * Add replica actions to action map by server.
     * @param index Index of the original action.
     * @param actionsByServer The map by server to add it to.
     */
    private void addReplicaActions(int index, Map<ServerName, MultiAction> actionsByServer,
                                   List<Action> unknownReplicaActions) {
      if (results[index] != null) return; // opportunistic. Never goes from non-null to null.
      Action action = initialActions.get(index);
      RegionLocations loc = findAllLocationsOrFail(action, true);
      if (loc == null) return;
      HRegionLocation[] locs = loc.getRegionLocations();
      if (locs.length == 1) {
        LOG.warn("No replicas found for {}", action.getAction());
        return;
      }
      synchronized (replicaResultLock) {
        // Don't run replica calls if the original has finished. We could do it e.g. if
        // original has already failed before first replica call (unlikely given retries),
        // but that would require additional synchronization w.r.t. returning to caller.
        if (results[index] != null) return;
        // We set the number of calls here. After that any path must call setResult/setError.
        // True even for replicas that are not found - if we refuse to send we MUST set error.
        updateResult(index, new ReplicaResultState(locs.length));
      }
      for (int i = 1; i < locs.length; ++i) {
        Action replicaAction = new Action(action, i);
        if (locs[i] != null) {
          asyncProcess.addAction(locs[i].getServerName(), locs[i].getRegionInfo().getRegionName(),
              replicaAction, actionsByServer, nonceGroup);
        } else {
          unknownReplicaActions.add(replicaAction);
        }
      }
    }

    private void addReplicaActionsAgain(
        Action action, Map<ServerName, MultiAction> actionsByServer) {
      if (action.getReplicaId() == RegionReplicaUtil.DEFAULT_REPLICA_ID) {
        throw new AssertionError("Cannot have default replica here");
      }
      HRegionLocation loc = getReplicaLocationOrFail(action);
      if (loc == null) return;
      asyncProcess.addAction(loc.getServerName(), loc.getRegionInfo().getRegionName(),
          action, actionsByServer, nonceGroup);
    }
  }

  /**
   * Runnable (that can be submitted to thread pool) that submits MultiAction to a
   * single server. The server call is synchronous, therefore we do it on a thread pool.
   */
  @VisibleForTesting
  final class SingleServerRequestRunnable implements Runnable {
    private final MultiAction multiAction;
    private final int numAttempt;
    private final ServerName server;
    private final Set<CancellableRegionServerCallable> callsInProgress;
    @VisibleForTesting
    SingleServerRequestRunnable(
        MultiAction multiAction, int numAttempt, ServerName server,
        Set<CancellableRegionServerCallable> callsInProgress) {
      this.multiAction = multiAction;
      this.numAttempt = numAttempt;
      this.server = server;
      this.callsInProgress = callsInProgress;
    }

    @Override
    public void run() {
      AbstractResponse res = null;
      CancellableRegionServerCallable callable = currentCallable;
      try {
        // setup the callable based on the actions, if we don't have one already from the request
        if (callable == null) {
          callable = createCallable(server, tableName, multiAction);
        }
        RpcRetryingCaller<AbstractResponse> caller = asyncProcess.createCaller(callable,rpcTimeout);
        try {
          if (callsInProgress != null) {
            callsInProgress.add(callable);
          }
          res = caller.callWithoutRetries(callable, operationTimeout);
          if (res == null) {
            // Cancelled
            return;
          }
        } catch (IOException e) {
          // The service itself failed . It may be an error coming from the communication
          //   layer, but, as well, a functional error raised by the server.
          receiveGlobalFailure(multiAction, server, numAttempt, e);
          return;
        } catch (Throwable t) {
          // This should not happen. Let's log & retry anyway.
          LOG.error("id=" + asyncProcess.id + ", caught throwable. Unexpected." +
              " Retrying. Server=" + server + ", tableName=" + tableName, t);
          receiveGlobalFailure(multiAction, server, numAttempt, t);
          return;
        }
        if (res.type() == AbstractResponse.ResponseType.MULTI) {
          // Normal case: we received an answer from the server, and it's not an exception.
          receiveMultiAction(multiAction, server, (MultiResponse) res, numAttempt);
        } else {
          if (results != null) {
            SingleResponse singleResponse = (SingleResponse) res;
            updateResult(0, singleResponse.getEntry());
          }
          decActionCounter(1);
        }
      } catch (Throwable t) {
        // Something really bad happened. We are on the send thread that will now die.
        LOG.error("id=" + asyncProcess.id + " error for " + tableName + " processing " + server, t);
        throw new RuntimeException(t);
      } finally {
        asyncProcess.decTaskCounters(multiAction.getRegions(), server);
        if (callsInProgress != null && callable != null && res != null) {
          callsInProgress.remove(callable);
        }
      }
    }
  }

  private final Batch.Callback<CResult> callback;
  private final BatchErrors errors;
  private final ConnectionImplementation.ServerErrorTracker errorsByServer;
  private final ExecutorService pool;
  private final Set<CancellableRegionServerCallable> callsInProgress;


  private final TableName tableName;
  private final AtomicLong actionsInProgress = new AtomicLong(-1);
  /**
   * The lock controls access to results. It is only held when populating results where
   * there might be several callers (eventual consistency gets). For other requests,
   * there's one unique call going on per result index.
   */
  private final Object replicaResultLock = new Object();
  /**
   * Result array.  Null if results are not needed. Otherwise, each index corresponds to
   * the action index in initial actions submitted. For most request types, has null-s for
   * requests that are not done, and result/exception for those that are done.
   * For eventual-consistency gets, initially the same applies; at some point, replica calls
   * might be started, and ReplicaResultState is put at the corresponding indices. The
   * returning calls check the type to detect when this is the case. After all calls are done,
   * ReplicaResultState-s are replaced with results for the user.
   */
  private final Object[] results;
  /**
   * Indices of replica gets in results. If null, all or no actions are replica-gets.
   */
  private final int[] replicaGetIndices;
  private final boolean hasAnyReplicaGets;
  private final long nonceGroup;
  private final CancellableRegionServerCallable currentCallable;
  private final int operationTimeout;
  private final int rpcTimeout;
  private final AsyncProcess asyncProcess;

  /**
   * For {@link AsyncRequestFutureImpl#manageError(int, Row, Retry, Throwable, ServerName)}. Only
   * used to make logging more clear, we don't actually care why we don't retry.
   */
  public enum Retry {
    YES,
    NO_LOCATION_PROBLEM,
    NO_NOT_RETRIABLE,
    NO_RETRIES_EXHAUSTED,
    NO_OTHER_SUCCEEDED
  }

  /** Sync point for calls to multiple replicas for the same user request (Get).
   * Created and put in the results array (we assume replica calls require results) when
   * the replica calls are launched. See results for details of this process.
   * POJO, all fields are public. To modify them, the object itself is locked. */
  private static class ReplicaResultState {
    public ReplicaResultState(int callCount) {
      this.callCount = callCount;
    }

    /** Number of calls outstanding, or 0 if a call succeeded (even with others outstanding). */
    int callCount;
    /** Errors for which it is not decided whether we will report them to user. If one of the
     * calls succeeds, we will discard the errors that may have happened in the other calls. */
    BatchErrors replicaErrors = null;

    @Override
    public String toString() {
      return "[call count " + callCount + "; errors " + replicaErrors + "]";
    }
  }

  public AsyncRequestFutureImpl(AsyncProcessTask task, List<Action> actions,
      long nonceGroup, AsyncProcess asyncProcess) {
    this.pool = task.getPool();
    this.callback = task.getCallback();
    this.nonceGroup = nonceGroup;
    this.tableName = task.getTableName();
    this.actionsInProgress.set(actions.size());
    if (task.getResults() == null) {
      results = task.getNeedResults() ? new Object[actions.size()] : null;
    } else {
      if (task.getResults().length != actions.size()) {
        throw new AssertionError("results.length");
      }
      this.results = task.getResults();
      for (int i = 0; i != this.results.length; ++i) {
        results[i] = null;
      }
    }
    List<Integer> replicaGetIndices = null;
    boolean hasAnyReplicaGets = false;
    if (results != null) {
      // Check to see if any requests might require replica calls.
      // We expect that many requests will consist of all or no multi-replica gets; in such
      // cases we would just use a boolean (hasAnyReplicaGets). If there's a mix, we will
      // store the list of action indexes for which replica gets are possible, and set
      // hasAnyReplicaGets to true.
      boolean hasAnyNonReplicaReqs = false;
      int posInList = 0;
      for (Action action : actions) {
        boolean isReplicaGet = AsyncProcess.isReplicaGet(action.getAction());
        if (isReplicaGet) {
          hasAnyReplicaGets = true;
          if (hasAnyNonReplicaReqs) { // Mixed case
            if (replicaGetIndices == null) {
              replicaGetIndices = new ArrayList<>(actions.size() - 1);
            }
            replicaGetIndices.add(posInList);
          }
        } else if (!hasAnyNonReplicaReqs) {
          // The first non-multi-replica request in the action list.
          hasAnyNonReplicaReqs = true;
          if (posInList > 0) {
            // Add all the previous requests to the index lists. We know they are all
            // replica-gets because this is the first non-multi-replica request in the list.
            replicaGetIndices = new ArrayList<>(actions.size() - 1);
            for (int i = 0; i < posInList; ++i) {
              replicaGetIndices.add(i);
            }
          }
        }
        ++posInList;
      }
    }
    this.hasAnyReplicaGets = hasAnyReplicaGets;
    if (replicaGetIndices != null) {
      this.replicaGetIndices = new int[replicaGetIndices.size()];
      int i = 0;
      for (Integer el : replicaGetIndices) {
        this.replicaGetIndices[i++] = el;
      }
    } else {
      this.replicaGetIndices = null;
    }
    this.callsInProgress = !hasAnyReplicaGets ? null :
        Collections.newSetFromMap(
            new ConcurrentHashMap<CancellableRegionServerCallable, Boolean>());
    this.asyncProcess = asyncProcess;
    this.errorsByServer = createServerErrorTracker();
    this.errors = new BatchErrors();
    this.operationTimeout = task.getOperationTimeout();
    this.rpcTimeout = task.getRpcTimeout();
    this.currentCallable = task.getCallable();
    if (task.getCallable() == null) {
      tracker = new RetryingTimeTracker().start();
    }
  }

  @VisibleForTesting
  protected Set<CancellableRegionServerCallable> getCallsInProgress() {
    return callsInProgress;
  }

  @VisibleForTesting
  SingleServerRequestRunnable createSingleServerRequest(MultiAction multiAction, int numAttempt, ServerName server,
        Set<CancellableRegionServerCallable> callsInProgress) {
    return new SingleServerRequestRunnable(multiAction, numAttempt, server, callsInProgress);
  }

  /**
   * Group a list of actions per region servers, and send them.
   *
   * @param currentActions - the list of row to submit
   * @param numAttempt - the current numAttempt (first attempt is 1)
   */
  void groupAndSendMultiAction(List<Action> currentActions, int numAttempt) {
    Map<ServerName, MultiAction> actionsByServer = new HashMap<>();

    boolean isReplica = false;
    List<Action> unknownReplicaActions = null;
    for (Action action : currentActions) {
      RegionLocations locs = findAllLocationsOrFail(action, true);
      if (locs == null) continue;
      boolean isReplicaAction = !RegionReplicaUtil.isDefaultReplica(action.getReplicaId());
      if (isReplica && !isReplicaAction) {
        // This is the property of the current implementation, not a requirement.
        throw new AssertionError("Replica and non-replica actions in the same retry");
      }
      isReplica = isReplicaAction;
      HRegionLocation loc = locs.getRegionLocation(action.getReplicaId());
      if (loc == null || loc.getServerName() == null) {
        if (isReplica) {
          if (unknownReplicaActions == null) {
            unknownReplicaActions = new ArrayList<>(1);
          }
          unknownReplicaActions.add(action);
        } else {
          // TODO: relies on primary location always being fetched
          manageLocationError(action, null);
        }
      } else {
        byte[] regionName = loc.getRegionInfo().getRegionName();
        AsyncProcess.addAction(loc.getServerName(), regionName, action, actionsByServer, nonceGroup);
      }
    }
    boolean doStartReplica = (numAttempt == 1 && !isReplica && hasAnyReplicaGets);
    boolean hasUnknown = unknownReplicaActions != null && !unknownReplicaActions.isEmpty();

    if (!actionsByServer.isEmpty()) {
      // If this is a first attempt to group and send, no replicas, we need replica thread.
      sendMultiAction(actionsByServer, numAttempt, (doStartReplica && !hasUnknown)
          ? currentActions : null, numAttempt > 1 && !hasUnknown);
    }

    if (hasUnknown) {
      actionsByServer = new HashMap<>();
      for (Action action : unknownReplicaActions) {
        HRegionLocation loc = getReplicaLocationOrFail(action);
        if (loc == null) continue;
        byte[] regionName = loc.getRegionInfo().getRegionName();
        AsyncProcess.addAction(loc.getServerName(), regionName, action, actionsByServer, nonceGroup);
      }
      if (!actionsByServer.isEmpty()) {
        sendMultiAction(
            actionsByServer, numAttempt, doStartReplica ? currentActions : null, true);
      }
    }
  }

  private HRegionLocation getReplicaLocationOrFail(Action action) {
    // We are going to try get location once again. For each action, we'll do it once
    // from cache, because the previous calls in the loop might populate it.
    int replicaId = action.getReplicaId();
    RegionLocations locs = findAllLocationsOrFail(action, true);
    if (locs == null) return null; // manageError already called
    HRegionLocation loc = locs.getRegionLocation(replicaId);
    if (loc == null || loc.getServerName() == null) {
      locs = findAllLocationsOrFail(action, false);
      if (locs == null) return null; // manageError already called
      loc = locs.getRegionLocation(replicaId);
    }
    if (loc == null || loc.getServerName() == null) {
      manageLocationError(action, null);
      return null;
    }
    return loc;
  }

  private void manageLocationError(Action action, Exception ex) {
    String msg = "Cannot get replica " + action.getReplicaId()
        + " location for " + action.getAction();
    LOG.error(msg);
    if (ex == null) {
      ex = new IOException(msg);
    }
    manageError(action.getOriginalIndex(), action.getAction(),
        Retry.NO_LOCATION_PROBLEM, ex, null);
  }

  private RegionLocations findAllLocationsOrFail(Action action, boolean useCache) {
    if (action.getAction() == null) throw new IllegalArgumentException("#" + asyncProcess.id +
        ", row cannot be null");
    RegionLocations loc = null;
    try {
      loc = asyncProcess.connection.locateRegion(
          tableName, action.getAction().getRow(), useCache, true, action.getReplicaId());
    } catch (IOException ex) {
      manageLocationError(action, ex);
    }
    return loc;
  }

  /**
   * Send a multi action structure to the servers, after a delay depending on the attempt
   * number. Asynchronous.
   *
   * @param actionsByServer the actions structured by regions
   * @param numAttempt the attempt number.
   * @param actionsForReplicaThread original actions for replica thread; null on non-first call.
   */
  void sendMultiAction(Map<ServerName, MultiAction> actionsByServer,
                               int numAttempt, List<Action> actionsForReplicaThread, boolean reuseThread) {
    // Run the last item on the same thread if we are already on a send thread.
    // We hope most of the time it will be the only item, so we can cut down on threads.
    int actionsRemaining = actionsByServer.size();
    // This iteration is by server (the HRegionLocation comparator is by server portion only).
    for (Map.Entry<ServerName, MultiAction> e : actionsByServer.entrySet()) {
      ServerName server = e.getKey();
      MultiAction multiAction = e.getValue();
      Collection<? extends Runnable> runnables = getNewMultiActionRunnable(server, multiAction,
          numAttempt);
      // make sure we correctly count the number of runnables before we try to reuse the send
      // thread, in case we had to split the request into different runnables because of backoff
      if (runnables.size() > actionsRemaining) {
        actionsRemaining = runnables.size();
      }

      // run all the runnables
      // HBASE-17475: Do not reuse the thread after stack reach a certain depth to prevent stack overflow
      // for now, we use HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER to control the depth
      for (Runnable runnable : runnables) {
        if ((--actionsRemaining == 0) && reuseThread
            && numAttempt % HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER != 0) {
          runnable.run();
        } else {
          try {
            pool.submit(runnable);
          } catch (Throwable t) {
            if (t instanceof RejectedExecutionException) {
              // This should never happen. But as the pool is provided by the end user,
              // let's secure this a little.
              LOG.warn("id=" + asyncProcess.id + ", task rejected by pool. Unexpected." +
                  " Server=" + server.getServerName(), t);
            } else {
              // see #HBASE-14359 for more details
              LOG.warn("Caught unexpected exception/error: ", t);
            }
            asyncProcess.decTaskCounters(multiAction.getRegions(), server);
            // We're likely to fail again, but this will increment the attempt counter,
            // so it will finish.
            receiveGlobalFailure(multiAction, server, numAttempt, t);
          }
        }
      }
    }

    if (actionsForReplicaThread != null) {
      startWaitingForReplicaCalls(actionsForReplicaThread);
    }
  }

  private Collection<? extends Runnable> getNewMultiActionRunnable(ServerName server,
                                                                   MultiAction multiAction,
                                                                   int numAttempt) {
    // no stats to manage, just do the standard action
    if (asyncProcess.connection.getStatisticsTracker() == null) {
      if (asyncProcess.connection.getConnectionMetrics() != null) {
        asyncProcess.connection.getConnectionMetrics().incrNormalRunners();
      }
      asyncProcess.incTaskCounters(multiAction.getRegions(), server);
      SingleServerRequestRunnable runnable = createSingleServerRequest(
              multiAction, numAttempt, server, callsInProgress);
      Tracer tracer = Tracer.curThreadTracer();

      if (tracer == null) {
        return Collections.singletonList(runnable);
      } else {
        return Collections.singletonList(tracer.wrap(runnable, "AsyncProcess.sendMultiAction"));
      }
    }

    // group the actions by the amount of delay
    Map<Long, DelayingRunner> actions = new HashMap<>(multiAction.size());

    // split up the actions
    for (Map.Entry<byte[], List<Action>> e : multiAction.actions.entrySet()) {
      Long backoff = getBackoff(server, e.getKey());
      DelayingRunner runner = actions.get(backoff);
      if (runner == null) {
        actions.put(backoff, new DelayingRunner(backoff, e));
      } else {
        runner.add(e);
      }
    }

    List<Runnable> toReturn = new ArrayList<>(actions.size());
    for (DelayingRunner runner : actions.values()) {
      asyncProcess.incTaskCounters(runner.getActions().getRegions(), server);
      String traceText = "AsyncProcess.sendMultiAction";
      Runnable runnable = createSingleServerRequest(runner.getActions(), numAttempt, server, callsInProgress);
      // use a delay runner only if we need to sleep for some time
      if (runner.getSleepTime() > 0) {
        runner.setRunner(runnable);
        traceText = "AsyncProcess.clientBackoff.sendMultiAction";
        runnable = runner;
        if (asyncProcess.connection.getConnectionMetrics() != null) {
          asyncProcess.connection.getConnectionMetrics()
            .incrDelayRunnersAndUpdateDelayInterval(runner.getSleepTime());
        }
      } else {
        if (asyncProcess.connection.getConnectionMetrics() != null) {
          asyncProcess.connection.getConnectionMetrics().incrNormalRunners();
        }
      }
      runnable = TraceUtil.wrap(runnable, traceText);
      toReturn.add(runnable);

    }
    return toReturn;
  }

  /**
   * @param server server location where the target region is hosted
   * @param regionName name of the region which we are going to write some data
   * @return the amount of time the client should wait until it submit a request to the
   * specified server and region
   */
  private Long getBackoff(ServerName server, byte[] regionName) {
    ServerStatisticTracker tracker = asyncProcess.connection.getStatisticsTracker();
    ServerStatistics stats = tracker.getStats(server);
    return asyncProcess.connection.getBackoffPolicy()
        .getBackoffTime(server, regionName, stats);
  }

  /**
   * Starts waiting to issue replica calls on a different thread; or issues them immediately.
   */
  private void startWaitingForReplicaCalls(List<Action> actionsForReplicaThread) {
    long startTime = EnvironmentEdgeManager.currentTime();
    ReplicaCallIssuingRunnable replicaRunnable = new ReplicaCallIssuingRunnable(
        actionsForReplicaThread, startTime);
    if (asyncProcess.primaryCallTimeoutMicroseconds == 0) {
      // Start replica calls immediately.
      replicaRunnable.run();
    } else {
      // Start the thread that may kick off replica gets.
      // TODO: we could do it on the same thread, but it's a user thread, might be a bad idea.
      try {
        pool.submit(replicaRunnable);
      } catch (RejectedExecutionException ree) {
        LOG.warn("id=" + asyncProcess.id + " replica task rejected by pool; no replica calls", ree);
      }
    }
  }

  /**
   * Check that we can retry acts accordingly: logs, set the error status.
   *
   * @param originalIndex the position in the list sent
   * @param row           the row
   * @param canRetry      if false, we won't retry whatever the settings.
   * @param throwable     the throwable, if any (can be null)
   * @param server        the location, if any (can be null)
   * @return true if the action can be retried, false otherwise.
   */
  Retry manageError(int originalIndex, Row row, Retry canRetry,
                                        Throwable throwable, ServerName server) {
    if (canRetry == Retry.YES
        && throwable != null && throwable instanceof DoNotRetryIOException) {
      canRetry = Retry.NO_NOT_RETRIABLE;
    }

    if (canRetry != Retry.YES) {
      // Batch.Callback<Res> was not called on failure in 0.94. We keep this.
      setError(originalIndex, row, throwable, server);
    } else if (isActionComplete(originalIndex, row)) {
      canRetry = Retry.NO_OTHER_SUCCEEDED;
    }
    return canRetry;
  }

  /**
   * Resubmit all the actions from this multiaction after a failure.
   *
   * @param rsActions  the actions still to do from the initial list
   * @param server   the destination
   * @param numAttempt the number of attempts so far
   * @param t the throwable (if any) that caused the resubmit
   */
  private void receiveGlobalFailure(
      MultiAction rsActions, ServerName server, int numAttempt, Throwable t) {
    errorsByServer.reportServerError(server);
    Retry canRetry = errorsByServer.canTryMore(numAttempt)
        ? Retry.YES : Retry.NO_RETRIES_EXHAUSTED;

    cleanServerCache(server, t);
    int failed = 0;
    int stopped = 0;
    List<Action> toReplay = new ArrayList<>();
    for (Map.Entry<byte[], List<Action>> e : rsActions.actions.entrySet()) {
      byte[] regionName = e.getKey();
      byte[] row = e.getValue().get(0).getAction().getRow();
      // Do not use the exception for updating cache because it might be coming from
      // any of the regions in the MultiAction.
      updateCachedLocations(server, regionName, row,
        ClientExceptionsUtil.isMetaClearingException(t) &&
            !ClientExceptionsUtil.isRegionServerOverloadedException(t) ? null : t);
      for (Action action : e.getValue()) {
        Retry retry = manageError(
            action.getOriginalIndex(), action.getAction(), canRetry, t, server);
        if (retry == Retry.YES) {
          toReplay.add(action);
        } else if (retry == Retry.NO_OTHER_SUCCEEDED) {
          ++stopped;
        } else {
          ++failed;
        }
      }
    }

    if (toReplay.isEmpty()) {
      logNoResubmit(server, numAttempt, rsActions.size(), t, failed, stopped);
    } else {
      resubmit(server, toReplay, numAttempt, rsActions.size(), t);
    }
  }

  /**
   * Log as much info as possible, and, if there is something to replay,
   * submit it again after a back off sleep.
   */
  private void resubmit(ServerName oldServer, List<Action> toReplay,
                        int numAttempt, int failureCount, Throwable throwable) {
    // We have something to replay. We're going to sleep a little before.

    // We have two contradicting needs here:
    //  1) We want to get the new location after having slept, as it may change.
    //  2) We want to take into account the location when calculating the sleep time.
    //  3) If all this is just because the response needed to be chunked try again FAST.
    // It should be possible to have some heuristics to take the right decision. Short term,
    //  we go for one.
    boolean retryImmediately = throwable instanceof RetryImmediatelyException;
    int nextAttemptNumber = retryImmediately ? numAttempt : numAttempt + 1;
    long backOffTime;
    if (retryImmediately) {
      backOffTime = 0;
    } else if (throwable instanceof CallQueueTooBigException) {
      // Give a special check on CQTBE, see #HBASE-17114
      backOffTime = errorsByServer.calculateBackoffTime(oldServer, asyncProcess.pauseForCQTBE);
    } else {
      backOffTime = errorsByServer.calculateBackoffTime(oldServer, asyncProcess.pause);
    }
    if (numAttempt > asyncProcess.startLogErrorsCnt) {
      // We use this value to have some logs when we have multiple failures, but not too many
      //  logs, as errors are to be expected when a region moves, splits and so on
      LOG.info(createLog(numAttempt, failureCount, toReplay.size(),
          oldServer, throwable, backOffTime, true, null, -1, -1));
    }

    try {
      if (backOffTime > 0) {
        Thread.sleep(backOffTime);
      }
    } catch (InterruptedException e) {
      LOG.warn("#" + asyncProcess.id + ", not sent: " + toReplay.size() + " operations, " + oldServer, e);
      Thread.currentThread().interrupt();
      return;
    }

    groupAndSendMultiAction(toReplay, nextAttemptNumber);
  }

  private void logNoResubmit(ServerName oldServer, int numAttempt,
                             int failureCount, Throwable throwable, int failed, int stopped) {
    if (failureCount != 0 || numAttempt > asyncProcess.startLogErrorsCnt + 1) {
      String timeStr = new Date(errorsByServer.getStartTrackingTime()).toString();
      String logMessage = createLog(numAttempt, failureCount, 0, oldServer,
          throwable, -1, false, timeStr, failed, stopped);
      if (failed != 0) {
        // Only log final failures as warning
        LOG.warn(logMessage);
      } else {
        LOG.info(logMessage);
      }
    }
  }

  /**
   * Called when we receive the result of a server query.
   *
   * @param multiAction    - the multiAction we sent
   * @param server       - the location. It's used as a server name.
   * @param responses      - the response, if any
   * @param numAttempt     - the attempt
   */
  private void receiveMultiAction(MultiAction multiAction, ServerName server,
      MultiResponse responses, int numAttempt) {
    assert responses != null;
    updateStats(server, responses);
    // Success or partial success
    // Analyze detailed results. We can still have individual failures to be redo.
    // two specific throwables are managed:
    //  - DoNotRetryIOException: we continue to retry for other actions
    //  - RegionMovedException: we update the cache with the new region location
    Map<byte[], MultiResponse.RegionResult> results = responses.getResults();
    List<Action> toReplay = new ArrayList<>();
    Throwable lastException = null;
    int failureCount = 0;
    int failed = 0;
    int stopped = 0;
    Retry retry = null;
    // Go by original action.
    for (Map.Entry<byte[], List<Action>> regionEntry : multiAction.actions.entrySet()) {
      byte[] regionName = regionEntry.getKey();

      Throwable regionException = responses.getExceptions().get(regionName);
      if (regionException != null) {
        cleanServerCache(server, regionException);
      }

      Map<Integer, Object> regionResults =
        results.containsKey(regionName) ? results.get(regionName).result : Collections.emptyMap();
      boolean regionFailureRegistered = false;
      for (Action sentAction : regionEntry.getValue()) {
        Object result = regionResults.get(sentAction.getOriginalIndex());
        if (result == null) {
          if (regionException == null) {
            LOG.error("Server sent us neither results nor exceptions for "
              + Bytes.toStringBinary(regionName)
              + ", numAttempt:" + numAttempt);
            regionException = new RuntimeException("Invalid response");
          }
          // If the row operation encounters the region-lever error, the exception of action may be
          // null.
          result = regionException;
        }
        // Failure: retry if it's make sense else update the errors lists
        if (result instanceof Throwable) {
          Throwable actionException = (Throwable) result;
          Row row = sentAction.getAction();
          lastException = regionException != null ? regionException
            : ClientExceptionsUtil.findException(actionException);
          // Register corresponding failures once per server/once per region.
          if (!regionFailureRegistered) {
            regionFailureRegistered = true;
            updateCachedLocations(server, regionName, row.getRow(), actionException);
          }
          if (retry == null) {
            errorsByServer.reportServerError(server);
            // We determine canRetry only once for all calls, after reporting server failure.
            retry = errorsByServer.canTryMore(numAttempt) ?
              Retry.YES : Retry.NO_RETRIES_EXHAUSTED;
          }
          ++failureCount;
          switch (manageError(sentAction.getOriginalIndex(), row, retry, actionException,
            server)) {
            case YES:
              toReplay.add(sentAction);
              break;
            case NO_OTHER_SUCCEEDED:
              ++stopped;
              break;
            default:
              ++failed;
              break;
          }
        } else {
          invokeCallBack(regionName, sentAction.getAction().getRow(), (CResult) result);
          setResult(sentAction, result);
        }
      }
    }
    if (toReplay.isEmpty()) {
      logNoResubmit(server, numAttempt, failureCount, lastException, failed, stopped);
    } else {
      resubmit(server, toReplay, numAttempt, failureCount, lastException);
    }
  }

  private void updateCachedLocations(ServerName server, byte[] regionName, byte[] row,
    Throwable rowException) {
    if (tableName == null) {
      return;
    }
    try {
      asyncProcess.connection
        .updateCachedLocations(tableName, regionName, row, rowException, server);
    } catch (Throwable ex) {
      // That should never happen, but if it did, we want to make sure
      // we still process errors
      LOG.error("Couldn't update cached region locations: " + ex);
    }
  }

  private void invokeCallBack(byte[] regionName, byte[] row, CResult result) {
    if (callback != null) {
      try {
        //noinspection unchecked
        // TODO: would callback expect a replica region name if it gets one?
        this.callback.update(regionName, row, result);
      } catch (Throwable t) {
        LOG.error("User callback threw an exception for "
          + Bytes.toStringBinary(regionName) + ", ignoring", t);
      }
    }
  }

  private void cleanServerCache(ServerName server, Throwable regionException) {
    if (ClientExceptionsUtil.isMetaClearingException(regionException)
        && !ClientExceptionsUtil.isRegionServerOverloadedException(regionException)) {
      // We want to make sure to clear the cache in case there were location-related exceptions.
      // We don't to clear the cache for every possible exception that comes through, however.
      asyncProcess.connection.clearCaches(server);
    }
  }

  @VisibleForTesting
  protected void updateStats(ServerName server, MultiResponse resp) {
    ConnectionUtils.updateStats(Optional.ofNullable(asyncProcess.connection.getStatisticsTracker()),
      Optional.ofNullable(asyncProcess.connection.getConnectionMetrics()), server, resp);
  }


  private String createLog(int numAttempt, int failureCount, int replaySize, ServerName sn,
                           Throwable error, long backOffTime, boolean willRetry, String startTime,
                           int failed, int stopped) {
    StringBuilder sb = new StringBuilder();
    sb.append("id=").append(asyncProcess.id).append(", table=").append(tableName).
        append(", attempt=").append(numAttempt).append("/").append(asyncProcess.numTries).
        append(", ");

    if (failureCount > 0 || error != null){
      sb.append("failureCount=").append(failureCount).append("ops").append(", last exception=").
          append(error);
    } else {
      sb.append("succeeded");
    }

    sb.append(" on ").append(sn).append(", tracking started ").append(startTime);

    if (willRetry) {
      sb.append(", retrying after=").append(backOffTime).append("ms").
          append(", operationsToReplay=").append(replaySize);
    } else if (failureCount > 0) {
      if (stopped > 0) {
        sb.append("; NOT retrying, stopped=").append(stopped).
            append(" because successful operation on other replica");
      }
      if (failed > 0) {
        sb.append("; NOT retrying, failed=").append(failed).append(" -- final attempt!");
      }
    }

    return sb.toString();
  }

  /**
   * Sets the non-error result from a particular action.
   * @param action Action (request) that the server responded to.
   * @param result The result.
   */
  private void setResult(Action action, Object result) {
    if (result == null) {
      throw new RuntimeException("Result cannot be null");
    }
    ReplicaResultState state = null;
    boolean isStale = !RegionReplicaUtil.isDefaultReplica(action.getReplicaId());
    int index = action.getOriginalIndex();
    if (results == null) {
      decActionCounter(index);
      return; // Simple case, no replica requests.
    }
    state = trySetResultSimple(index, action.getAction(), false, result, null, isStale);
    if (state == null) {
      return; // Simple case, no replica requests.
    }
    // At this point we know that state is set to replica tracking class.
    // It could be that someone else is also looking at it; however, we know there can
    // only be one state object, and only one thread can set callCount to 0. Other threads
    // will either see state with callCount 0 after locking it; or will not see state at all
    // we will replace it with the result.
    synchronized (state) {
      if (state.callCount == 0) {
        return; // someone already set the result
      }
      state.callCount = 0;
    }
    synchronized (replicaResultLock) {
      if (results[index] != state) {
        throw new AssertionError("We set the callCount but someone else replaced the result");
      }
      updateResult(index, result);
    }

    decActionCounter(index);
  }

  /**
   * Sets the error from a particular action.
   * @param index Original action index.
   * @param row Original request.
   * @param throwable The resulting error.
   * @param server The source server.
   */
  private void setError(int index, Row row, Throwable throwable, ServerName server) {
    ReplicaResultState state = null;
    if (results == null) {
      // Note that we currently cannot have replica requests with null results. So it shouldn't
      // happen that multiple replica calls will call dAC for same actions with results == null.
      // Only one call per action should be present in this case.
      errors.add(throwable, row, server);
      decActionCounter(index);
      return; // Simple case, no replica requests.
    }
    state = trySetResultSimple(index, row, true, throwable, server, false);
    if (state == null) {
      return; // Simple case, no replica requests.
    }
    BatchErrors target = null; // Error will be added to final errors, or temp replica errors.
    boolean isActionDone = false;
    synchronized (state) {
      switch (state.callCount) {
        case 0: return; // someone already set the result
        case 1: { // All calls failed, we are the last error.
          target = errors;
          isActionDone = true;
          break;
        }
        default: {
          assert state.callCount > 1;
          if (state.replicaErrors == null) {
            state.replicaErrors = new BatchErrors();
          }
          target = state.replicaErrors;
          break;
        }
      }
      --state.callCount;
    }
    target.add(throwable, row, server);
    if (isActionDone) {
      if (state.replicaErrors != null) { // last call, no need to lock
        errors.merge(state.replicaErrors);
      }
      // See setResult for explanations.
      synchronized (replicaResultLock) {
        if (results[index] != state) {
          throw new AssertionError("We set the callCount but someone else replaced the result");
        }
        updateResult(index, throwable);
      }
      decActionCounter(index);
    }
  }

  /**
   * Checks if the action is complete; used on error to prevent needless retries.
   * Does not synchronize, assuming element index/field accesses are atomic.
   * This is an opportunistic optimization check, doesn't have to be strict.
   * @param index Original action index.
   * @param row Original request.
   */
  private boolean isActionComplete(int index, Row row) {
    if (!AsyncProcess.isReplicaGet(row)) return false;
    Object resObj = results[index];
    return (resObj != null) && (!(resObj instanceof ReplicaResultState)
        || ((ReplicaResultState)resObj).callCount == 0);
  }

  /**
   * Tries to set the result or error for a particular action as if there were no replica calls.
   * @return null if successful; replica state if there were in fact replica calls.
   */
  private ReplicaResultState trySetResultSimple(int index, Row row, boolean isError,
                                                Object result, ServerName server, boolean isFromReplica) {
    Object resObj = null;
    if (!AsyncProcess.isReplicaGet(row)) {
      if (isFromReplica) {
        throw new AssertionError("Unexpected stale result for " + row);
      }
      updateResult(index, result);
    } else {
      synchronized (replicaResultLock) {
        resObj = results[index];
        if (resObj == null) {
          if (isFromReplica) {
            throw new AssertionError("Unexpected stale result for " + row);
          }
          updateResult(index, result);
        }
      }
    }

    ReplicaResultState rrs =
        (resObj instanceof ReplicaResultState) ? (ReplicaResultState)resObj : null;
    if (rrs == null && isError) {
      // The resObj is not replica state (null or already set).
      errors.add((Throwable)result, row, server);
    }

    if (resObj == null) {
      // resObj is null - no replica calls were made.
      decActionCounter(index);
      return null;
    }
    return rrs;
  }

  private void decActionCounter(int index) {
    long actionsRemaining = actionsInProgress.decrementAndGet();
    if (actionsRemaining < 0) {
      String error = buildDetailedErrorMsg("Incorrect actions in progress", index);
      throw new AssertionError(error);
    } else if (actionsRemaining == 0) {
      synchronized (actionsInProgress) {
        actionsInProgress.notifyAll();
      }
    }
  }

  private String buildDetailedErrorMsg(String string, int index) {
    StringBuilder error = new StringBuilder(128);
    error.append(string).append("; called for ").append(index).append(", actionsInProgress ")
        .append(actionsInProgress.get()).append("; replica gets: ");
    if (replicaGetIndices != null) {
      for (int i = 0; i < replicaGetIndices.length; ++i) {
        error.append(replicaGetIndices[i]).append(", ");
      }
    } else {
      error.append(hasAnyReplicaGets ? "all" : "none");
    }
    error.append("; results ");
    if (results != null) {
      for (int i = 0; i < results.length; ++i) {
        Object o = results[i];
        error.append(((o == null) ? "null" : o.toString())).append(", ");
      }
    }
    return error.toString();
  }

  @Override
  public void waitUntilDone() throws InterruptedIOException {
    try {
      waitUntilDone(Long.MAX_VALUE);
    } catch (InterruptedException iex) {
      throw new InterruptedIOException(iex.getMessage());
    } finally {
      if (callsInProgress != null) {
        for (CancellableRegionServerCallable clb : callsInProgress) {
          clb.cancel();
        }
      }
    }
  }

  private boolean waitUntilDone(long cutoff) throws InterruptedException {
    boolean hasWait = cutoff != Long.MAX_VALUE;
    long lastLog = EnvironmentEdgeManager.currentTime();
    long currentInProgress;
    while (0 != (currentInProgress = actionsInProgress.get())) {
      long now = EnvironmentEdgeManager.currentTime();
      if (hasWait && (now * 1000L) > cutoff) {
        return false;
      }
      if (!hasWait) { // Only log if wait is infinite.
        if (now > lastLog + 10000) {
          lastLog = now;
          LOG.info("#" + asyncProcess.id + ", waiting for " + currentInProgress
              + "  actions to finish on table: " + tableName);
        }
      }
      synchronized (actionsInProgress) {
        if (actionsInProgress.get() == 0) break;
        if (!hasWait) {
          actionsInProgress.wait(10);
        } else {
          long waitMicroSecond = Math.min(100000L, (cutoff - now * 1000L));
          TimeUnit.MICROSECONDS.timedWait(actionsInProgress, waitMicroSecond);
        }
      }
    }
    return true;
  }

  @Override
  public boolean hasError() {
    return errors.hasErrors();
  }

  @Override
  public List<? extends Row> getFailedOperations() {
    return errors.actions;
  }

  @Override
  public RetriesExhaustedWithDetailsException getErrors() {
    return errors.makeException(asyncProcess.logBatchErrorDetails);
  }

  @Override
  public Object[] getResults() throws InterruptedIOException {
    waitUntilDone();
    return results;
  }

  /**
   * Creates the server error tracker to use inside process.
   * Currently, to preserve the main assumption about current retries, and to work well with
   * the retry-limit-based calculation, the calculation is local per Process object.
   * We may benefit from connection-wide tracking of server errors.
   * @return ServerErrorTracker to use, null if there is no ServerErrorTracker on this connection
   */
  private ConnectionImplementation.ServerErrorTracker createServerErrorTracker() {
    return new ConnectionImplementation.ServerErrorTracker(
        asyncProcess.serverTrackerTimeout, asyncProcess.numTries);
  }

  /**
   * Create a callable. Isolated to be easily overridden in the tests.
   */
  private MultiServerCallable createCallable(final ServerName server, TableName tableName,
      final MultiAction multi) {
    return new MultiServerCallable(asyncProcess.connection, tableName, server,
        multi, asyncProcess.rpcFactory.newController(), rpcTimeout, tracker, multi.getPriority());
  }

  private void updateResult(int index, Object result) {
    Object current = results[index];
    if (current != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("The result is assigned repeatedly! current:" + current
          + ", new:" + result);
      }
    }
    results[index] = result;
  }

  @VisibleForTesting
  long getNumberOfActionsInProgress() {
    return actionsInProgress.get();
  }
}
