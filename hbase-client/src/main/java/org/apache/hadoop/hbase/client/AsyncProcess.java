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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.cloudera.htrace.Trace;

/**
 * This class  allows a continuous flow of requests. It's written to be compatible with a
 * synchronous caller such as HTable.
 * <p>
 * The caller sends a buffer of operation, by calling submit. This class extract from this list
 * the operations it can send, i.e. the operations that are on region that are not considered
 * as busy. The process is asynchronous, i.e. it returns immediately when if has finished to
 * iterate on the list. If, and only if, the maximum number of current task is reached, the call
 * to submit will block.
 * </p>
 * <p>
 * The class manages internally the retries.
 * </p>
 * <p>
 * The class includes an error marker: it allows to know if an operation has failed or not, and
 * to get the exception details, i.e. the full list of throwables for each attempt. This marker
 * is here to help the backward compatibility in HTable. In most (new) cases, it should be
 * managed by the callbacks.
 * </p>
 * <p>
 * A callback is available, in order to: <list>
 * <li>Get the result of the operation (failure or success)</li>
 * <li>When an operation fails but could be retried, allows or not to retry</li>
 * <li>When an operation fails for good (can't be retried or already retried the maximum number
 * time), register the error or not.
 * </list>
 * <p>
 * This class is not thread safe externally; only one thread should submit operations at a time.
 * Internally, the class is thread safe enough to manage simultaneously new submission and results
 * arising from older operations.
 * </p>
 * <p>
 * Internally, this class works with {@link Row}, this mean it could be theoretically used for
 * gets as well.
 * </p>
 */
class AsyncProcess<CResult> {
  private static final Log LOG = LogFactory.getLog(AsyncProcess.class);
  protected final HConnection hConnection;
  protected final TableName tableName;
  protected final ExecutorService pool;
  protected final AsyncProcessCallback<CResult> callback;
  protected final BatchErrors errors = new BatchErrors();
  protected final BatchErrors retriedErrors = new BatchErrors();
  protected final AtomicBoolean hasError = new AtomicBoolean(false);
  protected final AtomicLong tasksSent = new AtomicLong(0);
  protected final AtomicLong tasksDone = new AtomicLong(0);
  protected final ConcurrentMap<String, AtomicInteger> taskCounterPerRegion =
      new ConcurrentHashMap<String, AtomicInteger>();
  protected final int maxTotalConcurrentTasks;
  protected final int maxConcurrentTasksPerRegion;
  protected final long pause;
  protected int numTries;
  protected final boolean useServerTrackerForRetries;
  protected int serverTrackerTimeout;
  protected RpcRetryingCallerFactory rpcCallerFactory;


  /**
   * This interface allows to keep the interface of the previous synchronous interface, that uses
   * an array of object to return the result.
   * <p/>
   * This interface allows the caller to specify the behavior on errors: <list>
   * <li>If we have not yet reach the maximum number of retries, the user can nevertheless
   * specify if this specific operation should be retried or not.
   * </li>
   * <li>If an operation fails (i.e. is not retried or fails after all retries), the user can
   * specify is we should mark this AsyncProcess as in error or not.
   * </li>
   * </list>
   */
  interface AsyncProcessCallback<CResult> {

    /**
     * Called on success. originalIndex holds the index in the action list.
     */
    void success(int originalIndex, byte[] region, Row row, CResult result);

    /**
     * called on failure, if we don't retry (i.e. called once per failed operation).
     *
     * @return true if we should store the error and tag this async process as being in error.
     *         false if the failure of this operation can be safely ignored, and does not require
     *         the current process to be stopped without proceeding with the other operations in
     *         the queue.
     */
    boolean failure(int originalIndex, byte[] region, Row row, Throwable t);

    /**
     * Called on a failure we plan to retry. This allows the user to stop retrying. Will be
     * called multiple times for a single action if it fails multiple times.
     *
     * @return false if we should retry, true otherwise.
     */
    boolean retriableFailure(int originalIndex, Row row, byte[] region, Throwable exception);
  }

  private static class BatchErrors {
    private List<Throwable> throwables = new ArrayList<Throwable>();
    private List<Row> actions = new ArrayList<Row>();
    private List<String> addresses = new ArrayList<String>();

    public void add(Throwable ex, Row row, HRegionLocation location) {
      throwables.add(ex);
      actions.add(row);
      addresses.add(location != null ? location.getHostnamePort() : "null location");
    }

    private RetriesExhaustedWithDetailsException makeException() {
      return new RetriesExhaustedWithDetailsException(
          new ArrayList<Throwable>(throwables),
          new ArrayList<Row>(actions), new ArrayList<String>(addresses));
    }

    public void clear() {
      throwables.clear();
      actions.clear();
      addresses.clear();
    }
  }

  public AsyncProcess(HConnection hc, TableName tableName, ExecutorService pool,
      AsyncProcessCallback<CResult> callback, Configuration conf,
      RpcRetryingCallerFactory rpcCaller) {
    this.hConnection = hc;
    this.tableName = tableName;
    this.pool = pool;
    this.callback = callback;

    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numTries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);

    this.maxTotalConcurrentTasks = conf.getInt("hbase.client.max.total.tasks", 200);

    // With one, we ensure that the ordering of the queries is respected: we don't start
    //  a set of operations on a region before the previous one is done. As well, this limits
    //  the pressure we put on the region server.
    this.maxConcurrentTasksPerRegion = conf.getInt("hbase.client.max.perregion.tasks", 1);

    this.useServerTrackerForRetries =
        conf.getBoolean(HConnectionManager.RETRIES_BY_SERVER_KEY, true);

    if (this.useServerTrackerForRetries) {
      // Server tracker allows us to do faster, and yet useful (hopefully), retries.
      // However, if we are too useful, we might fail very quickly due to retry count limit.
      // To avoid this, we are going to cheat for now (see HBASE-7659), and calculate maximum
      // retry time if normal retries were used. Then we will retry until this time runs out.
      // If we keep hitting one server, the net effect will be the incremental backoff, and
      // essentially the same number of retries as planned. If we have to do faster retries,
      // we will do more retries in aggregate, but the user will be none the wiser.
      this.serverTrackerTimeout = 0;
      for (int i = 0; i < this.numTries; ++i) {
        serverTrackerTimeout += ConnectionUtils.getPauseTime(this.pause, i);
      }
    }

    this.rpcCallerFactory = rpcCaller;
  }

  /**
   * Extract from the rows list what we can submit. The rows we can not submit are kept in the
   * list.
   *
   * @param rows - the submitted row. Modified by the method: we remove the rows we took.
   * @param atLeastOne true if we should submit at least a subset.
   */
  public void submit(List<? extends Row> rows, boolean atLeastOne) throws InterruptedIOException {
    if (rows.isEmpty()){
      return;
    }

    Map<HRegionLocation, MultiAction<Row>> actionsByServer =
        new HashMap<HRegionLocation, MultiAction<Row>>();
    List<Action<Row>> retainedActions = new ArrayList<Action<Row>>(rows.size());

    do {
      Map<String, Boolean> regionIncluded = new HashMap<String, Boolean>();
      long currentTaskNumber = waitForMaximumCurrentTasks(maxTotalConcurrentTasks);
      int posInList = -1;
      Iterator<? extends Row> it = rows.iterator();
      while (it.hasNext()) {
        Row r = it.next();
        HRegionLocation loc = findDestLocation(r, 1, posInList, false, regionIncluded);

        if (loc != null) {   // loc is null if the dest is too busy or there is an error
          Action<Row> action = new Action<Row>(r, ++posInList);
          retainedActions.add(action);
          addAction(loc, action, actionsByServer);
          it.remove();
        }
      }

      if (retainedActions.isEmpty() && atLeastOne && !hasError()) {
        waitForNextTaskDone(currentTaskNumber);
      }

    } while (retainedActions.isEmpty() && atLeastOne && !hasError());

    HConnectionManager.ServerErrorTracker errorsByServer = createServerErrorTracker();
    sendMultiAction(retainedActions, actionsByServer, 1, errorsByServer);
  }

  /**
   * Group the actions per region server.
   *
   * @param loc - the destination. Must not be null.
   * @param action - the action to add to the multiaction
   * @param actionsByServer the multiaction per server
   */
  private void addAction(HRegionLocation loc, Action<Row> action, Map<HRegionLocation,
      MultiAction<Row>> actionsByServer) {
    final byte[] regionName = loc.getRegionInfo().getRegionName();
    MultiAction<Row> multiAction = actionsByServer.get(loc);
    if (multiAction == null) {
      multiAction = new MultiAction<Row>();
      actionsByServer.put(loc, multiAction);
    }

    multiAction.add(regionName, action);
  }

  /**
   * Find the destination, if this destination is not considered as busy.
   *
   * @param row          the row
   * @param numAttempt   the num attempt
   * @param posInList    the position in the list
   * @param force        if we must submit whatever the server load
   * @param regionStatus the
   * @return null if we should not submit, the destination otherwise.
   */
  private HRegionLocation findDestLocation(Row row, int numAttempt,
                                           int posInList, boolean force,
                                           Map<String, Boolean> regionStatus) {
    HRegionLocation loc = null;
    IOException locationException = null;
    try {
      loc = hConnection.locateRegion(this.tableName, row.getRow());
      if (loc == null) {
        locationException = new IOException("No location found, aborting submit for" +
            " tableName=" + tableName +
            " rowkey=" + Arrays.toString(row.getRow()));
      }
    } catch (IOException e) {
      locationException = e;
    }
    if (locationException != null) {
      // There are multiple retries in locateRegion already. No need to add new.
      // We can't continue with this row, hence it's the last retry.
      manageError(numAttempt, posInList, row, false, locationException, null);
      return null;
    }

    if (force) {
      return loc;
    }

    String regionName = loc.getRegionInfo().getEncodedName();
    Boolean addIt = regionStatus.get(regionName);
    if (addIt == null) {
      addIt = canTakeNewOperations(regionName);
      regionStatus.put(regionName, addIt);
    }

    return addIt ? loc : null;
  }


  /**
   * Check if we should send new operations to this region.
   *
   * @param encodedRegionName region name
   * @return true if this region is considered as busy.
   */
  protected boolean canTakeNewOperations(String encodedRegionName) {
    AtomicInteger ct = taskCounterPerRegion.get(encodedRegionName);
    return ct == null || ct.get() < maxConcurrentTasksPerRegion;
  }

  /**
   * Submit immediately the list of rows, whatever the server status. Kept for backward
   * compatibility: it allows to be used with the batch interface that return an array of objects.
   *
   * @param rows the list of rows.
   */
  public void submitAll(List<? extends Row> rows) {
    List<Action<Row>> actions = new ArrayList<Action<Row>>(rows.size());

    // The position will be used by the processBatch to match the object array returned.
    int posInList = -1;
    for (Row r : rows) {
      posInList++;
      Action<Row> action = new Action<Row>(r, posInList);
      actions.add(action);
    }
    HConnectionManager.ServerErrorTracker errorsByServer = createServerErrorTracker();
    submit(actions, actions, 1, true, errorsByServer);
  }


  /**
   * Group a list of actions per region servers, and send them. The created MultiActions are
   * added to the inProgress list.
   *
   * @param initialActions - the full list of the actions in progress
   * @param currentActions - the list of row to submit
   * @param numAttempt - the current numAttempt (first attempt is 1)
   * @param force - true if we submit the rowList without taking into account the server load
   */
  private void submit(List<Action<Row>> initialActions,
                      List<Action<Row>> currentActions, int numAttempt, boolean force,
                      final HConnectionManager.ServerErrorTracker errorsByServer) {
    // group per location => regions server
    final Map<HRegionLocation, MultiAction<Row>> actionsByServer =
        new HashMap<HRegionLocation, MultiAction<Row>>();

    // We have the same policy for a single region per call to submit: we don't want
    //  to send half of the actions because the status changed in the middle. So we keep the
    //  status
    Map<String, Boolean> regionIncluded = new HashMap<String, Boolean>();

    for (Action<Row> action : currentActions) {
      HRegionLocation loc = findDestLocation(
          action.getAction(), 1, action.getOriginalIndex(), force, regionIncluded);

      if (loc != null) {
        addAction(loc, action, actionsByServer);
      }
    }

    if (!actionsByServer.isEmpty()) {
      sendMultiAction(initialActions, actionsByServer, numAttempt, errorsByServer);
    }
  }

  /**
   * Send a multi action structure to the servers, after a delay depending on the attempt
   * number. Asynchronous.
   *
   * @param initialActions  the list of the actions, flat.
   * @param actionsByServer the actions structured by regions
   * @param numAttempt      the attempt number.
   */
  public void sendMultiAction(final List<Action<Row>> initialActions,
                              Map<HRegionLocation, MultiAction<Row>> actionsByServer,
                              final int numAttempt,
                              final HConnectionManager.ServerErrorTracker errorsByServer) {

    // Send the queries and add them to the inProgress list
    for (Map.Entry<HRegionLocation, MultiAction<Row>> e : actionsByServer.entrySet()) {
      final HRegionLocation loc = e.getKey();
      final MultiAction<Row> multi = e.getValue();
      final String regionName = loc.getRegionInfo().getEncodedName();

      incTaskCounters(regionName);

      Runnable runnable = Trace.wrap("AsyncProcess.sendMultiAction", new Runnable() {
        @Override
        public void run() {
          MultiResponse res;
          try {
            MultiServerCallable<Row> callable = createCallable(loc, multi);
            try {
              res = createCaller(callable).callWithoutRetries(callable);
            } catch (IOException e) {
              LOG.warn("The call to the RS failed, we don't know where we stand, " + loc, e);
              resubmitAll(initialActions, multi, loc, numAttempt + 1, e, errorsByServer);
              return;
            }

            receiveMultiAction(initialActions, multi, loc, res, numAttempt, errorsByServer);
          } finally {
            decTaskCounters(regionName);
          }
        }
      });

      try {
        this.pool.submit(runnable);
      } catch (RejectedExecutionException ree) {
        // This should never happen. But as the pool is provided by the end user, let's secure
        //  this a little.
        decTaskCounters(regionName);
        LOG.warn("The task was rejected by the pool. This is unexpected. " + loc, ree);
        // We're likely to fail again, but this will increment the attempt counter, so it will
        //  finish.
        resubmitAll(initialActions, multi, loc, numAttempt + 1, ree, errorsByServer);
      }
    }
  }

  /**
   * Create a callable. Isolated to be easily overridden in the tests.
   */
  protected MultiServerCallable<Row> createCallable(final HRegionLocation location,
      final MultiAction<Row> multi) {
    return new MultiServerCallable<Row>(hConnection, tableName, location, multi);
  }

  /**
   * For tests.
   * @param callable
   * @return Returns a caller.
   */
  protected RpcRetryingCaller<MultiResponse> createCaller(MultiServerCallable<Row> callable) {
    // callable is unused.
    return rpcCallerFactory.<MultiResponse> newCaller();
  }

  /**
   * Check that we can retry acts accordingly: logs, set the error status, call the callbacks.
   *
   * @param numAttempt    the number of this attempt
   * @param originalIndex the position in the list sent
   * @param row           the row
   * @param canRetry      if false, we won't retry whatever the settings.
   * @param throwable     the throwable, if any (can be null)
   * @param location      the location, if any (can be null)
   * @return true if the action can be retried, false otherwise.
   */
  private boolean manageError(int numAttempt, int originalIndex, Row row, boolean canRetry,
                              Throwable throwable, HRegionLocation location) {
    if (canRetry) {
      if (numAttempt >= numTries ||
          (throwable != null && throwable instanceof DoNotRetryIOException)) {
        canRetry = false;
      }
    }
    byte[] region = location == null ? null : location.getRegionInfo().getEncodedNameAsBytes();

    if (canRetry && callback != null) {
      canRetry = callback.retriableFailure(originalIndex, row, region, throwable);
    }

    if (canRetry) {
      if (LOG.isTraceEnabled()) {
        retriedErrors.add(throwable, row, location);
      }
    } else {
      if (callback != null) {
        callback.failure(originalIndex, region, row, throwable);
      }
      errors.add(throwable, row, location);
      this.hasError.set(true);
    }

    return canRetry;
  }

  /**
   * Resubmit all the actions from this multiaction after a failure.
   *
   * @param initialActions the full initial action list
   * @param rsActions  the actions still to do from the initial list
   * @param location   the destination
   * @param numAttempt the number of attempts so far
   * @param t the throwable (if any) that caused the resubmit
   */
  private void resubmitAll(List<Action<Row>> initialActions, MultiAction<Row> rsActions,
                           HRegionLocation location, int numAttempt, Throwable t,
                           HConnectionManager.ServerErrorTracker errorsByServer) {
    // Do not use the exception for updating cache because it might be coming from
    // any of the regions in the MultiAction.
    hConnection.updateCachedLocations(tableName,
        rsActions.actions.values().iterator().next().get(0).getAction().getRow(), null, location);
    errorsByServer.reportServerError(location);

    List<Action<Row>> toReplay = new ArrayList<Action<Row>>();
    for (List<Action<Row>> actions : rsActions.actions.values()) {
      for (Action<Row> action : actions) {
        if (manageError(numAttempt, action.getOriginalIndex(), action.getAction(),
            true, t, location)) {
          toReplay.add(action);
        }
      }
    }

    if (toReplay.isEmpty()) {
      LOG.warn("Attempt #" + numAttempt + "/" + numTries + " failed for all " +
        initialActions.size() + "ops, NOT resubmitting, " + location);
    } else {
      submit(initialActions, toReplay, numAttempt, true, errorsByServer);
    }
  }

  /**
   * Called when we receive the result of a server query.
   *
   * @param initialActions - the whole action list
   * @param rsActions      - the actions for this location
   * @param location       - the location
   * @param responses      - the response, if any
   * @param numAttempt     - the attempt
   */
  private void receiveMultiAction(List<Action<Row>> initialActions,
                                  MultiAction<Row> rsActions, HRegionLocation location,
                                  MultiResponse responses, int numAttempt,
                                  HConnectionManager.ServerErrorTracker errorsByServer) {

    if (responses == null) {
      LOG.info("Attempt #" + numAttempt + "/" + numTries + " failed all ops, trying resubmit," +
        location);
      resubmitAll(initialActions, rsActions, location, numAttempt + 1, null, errorsByServer);
      return;
    }

    // Success or partial success
    // Analyze detailed results. We can still have individual failures to be redo.
    // two specific throwables are managed:
    //  - DoNotRetryIOException: we continue to retry for other actions
    //  - RegionMovedException: we update the cache with the new region location

    List<Action<Row>> toReplay = new ArrayList<Action<Row>>();
    Throwable throwable = null;

    int failureCount = 0;
    boolean canRetry = true;
    for (Map.Entry<byte[], List<Pair<Integer, Object>>> resultsForRS :
        responses.getResults().entrySet()) {

      for (Pair<Integer, Object> regionResult : resultsForRS.getValue()) {
        Object result = regionResult.getSecond();

        // Failure: retry if it's make sense else update the errors lists
        if (result == null || result instanceof Throwable) {
          throwable = (Throwable) result;
          Action<Row> correspondingAction = initialActions.get(regionResult.getFirst());
          Row row = correspondingAction.getAction();

          if (failureCount++ == 0) { // We're doing this once per location.
            hConnection.updateCachedLocations(this.tableName, row.getRow(), result, location);
            if (errorsByServer != null) {
              errorsByServer.reportServerError(location);
              canRetry = errorsByServer.canRetryMore();
            }
          }

          if (manageError(numAttempt, correspondingAction.getOriginalIndex(), row, canRetry,
              throwable, location)) {
            toReplay.add(correspondingAction);
          }
        } else { // success
          if (callback != null) {
            Action<Row> correspondingAction = initialActions.get(regionResult.getFirst());
            Row row = correspondingAction.getAction();
            //noinspection unchecked
            this.callback.success(correspondingAction.getOriginalIndex(),
                resultsForRS.getKey(), row, (CResult) result);
          }
        }
      }
    }

    if (!toReplay.isEmpty()) {
      long backOffTime = (errorsByServer != null ?
          errorsByServer.calculateBackoffTime(location, pause) :
          ConnectionUtils.getPauseTime(pause, numAttempt));
      if (numAttempt > 3 && LOG.isDebugEnabled()) {
        // We use this value to have some logs when we have multiple failures, but not too many
        //  logs as errors are to be expected wehn region moves, split and so on
        LOG.debug("Attempt #" + numAttempt + "/" + numTries + " failed " + failureCount +
          " ops , resubmitting " + toReplay.size() + ", " + location + ", last exception was: " +
          throwable.getMessage() + ", sleeping " + backOffTime + "ms");
      }
      try {
        Thread.sleep(backOffTime);
      } catch (InterruptedException e) {
        LOG.warn("Not sent: " + toReplay.size() +
            " operations, " + location, e);
        Thread.interrupted();
        return;
      }

      submit(initialActions, toReplay, numAttempt + 1, true, errorsByServer);
    } else if (failureCount != 0) {
      LOG.warn("Attempt #" + numAttempt + "/" + numTries + " failed for " + failureCount +
          " ops on " + location.getServerName() + " NOT resubmitting." + location);
    }
  }

  /**
   * Waits for another task to finish.
   * @param currentNumberOfTask - the number of task finished when calling the method.
   */
  protected void waitForNextTaskDone(long currentNumberOfTask) throws InterruptedIOException {
    while (currentNumberOfTask == tasksDone.get()) {
      try {
        synchronized (this.tasksDone) {
          this.tasksDone.wait(100);
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted." +
            " currentNumberOfTask=" + currentNumberOfTask +
            ",  tableName=" + tableName + ", tasksDone=" + tasksDone.get());
      }
    }
  }

  /**
   * Wait until the async does not have more than max tasks in progress.
   */
  private long waitForMaximumCurrentTasks(int max) throws InterruptedIOException {
    long lastLog = EnvironmentEdgeManager.currentTimeMillis();
    long currentTasksDone = this.tasksDone.get();

    while ((tasksSent.get() - currentTasksDone) > max) {
      long now = EnvironmentEdgeManager.currentTimeMillis();
      if (now > lastLog + 10000) {
        lastLog = now;
        LOG.info(": Waiting for the global number of running tasks to be equals or less than "
            + max + ", tasksSent=" + tasksSent.get() + ", tasksDone=" + tasksDone.get() +
            ", currentTasksDone=" + currentTasksDone + ", tableName=" + tableName);
      }
      waitForNextTaskDone(currentTasksDone);
      currentTasksDone = this.tasksDone.get();
    }

    return currentTasksDone;
  }

  /**
   * Wait until all tasks are executed, successfully or not.
   */
  public void waitUntilDone() throws InterruptedIOException {
    waitForMaximumCurrentTasks(0);
  }


  public boolean hasError() {
    return hasError.get();
  }

  public List<? extends Row> getFailedOperations() {
    return errors.actions;
  }

  /**
   * Clean the errors stacks. Should be called only when there are no actions in progress.
   */
  public void clearErrors() {
    errors.clear();
    retriedErrors.clear();
    hasError.set(false);
  }

  public RetriesExhaustedWithDetailsException getErrors() {
    return errors.makeException();
  }

  /**
   * incrementer the tasks counters for a given region. MT safe.
   */
  protected void incTaskCounters(String encodedRegionName) {
    tasksSent.incrementAndGet();

    AtomicInteger counterPerServer = taskCounterPerRegion.get(encodedRegionName);
    if (counterPerServer == null) {
      taskCounterPerRegion.putIfAbsent(encodedRegionName, new AtomicInteger());
      counterPerServer = taskCounterPerRegion.get(encodedRegionName);
    }
    counterPerServer.incrementAndGet();
  }

  /**
   * Decrements the counters for a given region
   */
  protected void decTaskCounters(String encodedRegionName) {
    AtomicInteger counterPerServer = taskCounterPerRegion.get(encodedRegionName);
    counterPerServer.decrementAndGet();

    tasksDone.incrementAndGet();
    synchronized (tasksDone) {
      tasksDone.notifyAll();
    }
  }

  /**
   * Creates the server error tracker to use inside process.
   * Currently, to preserve the main assumption about current retries, and to work well with
   * the retry-limit-based calculation, the calculation is local per Process object.
   * We may benefit from connection-wide tracking of server errors.
   * @return ServerErrorTracker to use, null if there is no ServerErrorTracker on this connection
   */
  protected HConnectionManager.ServerErrorTracker createServerErrorTracker() {
    if (useServerTrackerForRetries){
      return new HConnectionManager.ServerErrorTracker(this.serverTrackerTimeout);
    }else {
      return null;
    }
  }
}
