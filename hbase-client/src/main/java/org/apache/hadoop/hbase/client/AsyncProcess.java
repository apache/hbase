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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.cloudera.htrace.Trace;

import com.google.common.base.Preconditions;

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

  /**
   * Configure the number of failures after which the client will start logging. A few failures
   * is fine: region moved, then is not opened, then is overloaded. We try to have an acceptable
   * heuristic for the number of errors we don't log. 9 was chosen because we wait for 1s at
   * this stage.
   */
  public static final String START_LOG_ERRORS_AFTER_COUNT_KEY =
      "hbase.client.start.log.errors.counter";
  public static final int DEFAULT_START_LOG_ERRORS_AFTER_COUNT = 9;

  protected static final AtomicLong COUNTER = new AtomicLong();
  protected final long id;
  private final int startLogErrorsCnt;
  protected final HConnection hConnection;
  protected final TableName tableName;
  protected final ExecutorService pool;
  protected final AsyncProcessCallback<CResult> callback;
  protected final BatchErrors errors = new BatchErrors();
  protected final AtomicBoolean hasError = new AtomicBoolean(false);
  protected final AtomicLong tasksSent = new AtomicLong(0);
  protected final AtomicLong tasksDone = new AtomicLong(0);
  protected final AtomicLong retriesCnt = new AtomicLong(0);
  protected final ConcurrentMap<byte[], AtomicInteger> taskCounterPerRegion =
      new ConcurrentSkipListMap<byte[], AtomicInteger>(Bytes.BYTES_COMPARATOR);
  protected final ConcurrentMap<ServerName, AtomicInteger> taskCounterPerServer =
      new ConcurrentHashMap<ServerName, AtomicInteger>();
  protected final int timeout;

  /**
   * The number of tasks simultaneously executed on the cluster.
   */
  protected final int maxTotalConcurrentTasks;

  /**
   * The number of tasks we run in parallel on a single region.
   * With 1 (the default) , we ensure that the ordering of the queries is respected: we don't start
   * a set of operations on a region before the previous one is done. As well, this limits
   * the pressure we put on the region server.
   */
  protected final int maxConcurrentTasksPerRegion;

  /**
   * The number of task simultaneously executed on a single region server.
   */
  protected final int maxConcurrentTasksPerServer;
  protected final long pause;
  protected int numTries;
  protected int serverTrackerTimeout;
  protected RpcRetryingCallerFactory rpcCallerFactory;
  private RpcControllerFactory rpcFactory;


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
    private final List<Throwable> throwables = new ArrayList<Throwable>();
    private final List<Row> actions = new ArrayList<Row>();
    private final List<String> addresses = new ArrayList<String>();

    public synchronized void add(Throwable ex, Row row, HRegionLocation location) {
      if (row == null){
        throw new IllegalArgumentException("row cannot be null. location=" + location);
      }

      throwables.add(ex);
      actions.add(row);
      addresses.add(location != null ? location.getServerName().toString() : "null location");
    }

    private synchronized RetriesExhaustedWithDetailsException makeException() {
      return new RetriesExhaustedWithDetailsException(
          new ArrayList<Throwable>(throwables),
          new ArrayList<Row>(actions), new ArrayList<String>(addresses));
    }

    public synchronized void clear() {
      throwables.clear();
      actions.clear();
      addresses.clear();
    }
  }

  public AsyncProcess(HConnection hc, TableName tableName, ExecutorService pool,
      AsyncProcessCallback<CResult> callback, Configuration conf,
      RpcRetryingCallerFactory rpcCaller, RpcControllerFactory rpcFactory) {
    if (hc == null){
      throw new IllegalArgumentException("HConnection cannot be null.");
    }

    this.hConnection = hc;
    this.tableName = tableName;
    this.pool = pool;
    this.callback = callback;

    this.id = COUNTER.incrementAndGet();

    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numTries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.timeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);


    this.maxTotalConcurrentTasks = conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
      HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS);
    this.maxConcurrentTasksPerServer = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_PERSERVER_TASKS);
    this.maxConcurrentTasksPerRegion = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_PERREGION_TASKS);

    this.startLogErrorsCnt =
        conf.getInt(START_LOG_ERRORS_AFTER_COUNT_KEY, DEFAULT_START_LOG_ERRORS_AFTER_COUNT);

    if (this.maxTotalConcurrentTasks <= 0) {
      throw new IllegalArgumentException("maxTotalConcurrentTasks=" + maxTotalConcurrentTasks);
    }
    if (this.maxConcurrentTasksPerServer <= 0) {
      throw new IllegalArgumentException("maxConcurrentTasksPerServer=" +
          maxConcurrentTasksPerServer);
    }
    if (this.maxConcurrentTasksPerRegion <= 0) {
      throw new IllegalArgumentException("maxConcurrentTasksPerRegion=" +
          maxConcurrentTasksPerRegion);
    }

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

    this.rpcCallerFactory = rpcCaller;
    Preconditions.checkNotNull(rpcFactory);
    this.rpcFactory = rpcFactory;
  }

  /**
   * Extract from the rows list what we can submit. The rows we can not submit are kept in the
   * list.
   *
   * @param rows - the submitted row. Modified by the method: we remove the rows we took.
   * @param atLeastOne true if we should submit at least a subset.
   */
  public void submit(List<? extends Row> rows, boolean atLeastOne) throws InterruptedIOException {
    submit(rows, atLeastOne, null);
  }

  /**
   * Extract from the rows list what we can submit. The rows we can not submit are kept in the
   * list.
   *
   * @param rows - the submitted row. Modified by the method: we remove the rows we took.
   * @param atLeastOne true if we should submit at least a subset.
   * @param batchCallback Batch callback. Only called on success
   */
  public void submit(List<? extends Row> rows, boolean atLeastOne,
      Batch.Callback<CResult> batchCallback) throws InterruptedIOException {
    if (rows.isEmpty()) {
      return;
    }

    // This looks like we are keying by region but HRegionLocation has a comparator that compares
    // on the server portion only (hostname + port) so this Map collects regions by server.
    Map<HRegionLocation, MultiAction<Row>> actionsByServer =
      new HashMap<HRegionLocation, MultiAction<Row>>();
    List<Action<Row>> retainedActions = new ArrayList<Action<Row>>(rows.size());

    long currentTaskCnt = tasksDone.get();
    boolean alreadyLooped = false;

    NonceGenerator ng = this.hConnection.getNonceGenerator();
    do {
      if (alreadyLooped){
        // if, for whatever reason, we looped, we want to be sure that something has changed.
        waitForNextTaskDone(currentTaskCnt);
        currentTaskCnt = tasksDone.get();
      } else {
        alreadyLooped = true;
      }

      // Wait until there is at least one slot for a new task.
      waitForMaximumCurrentTasks(maxTotalConcurrentTasks - 1);

      // Remember the previous decisions about regions or region servers we put in the
      //  final multi.
      Map<Long, Boolean> regionIncluded = new HashMap<Long, Boolean>();
      Map<ServerName, Boolean> serverIncluded = new HashMap<ServerName, Boolean>();

      int posInList = -1;
      Iterator<? extends Row> it = rows.iterator();
      while (it.hasNext()) {
        Row r = it.next();
        HRegionLocation loc = findDestLocation(r, posInList);

        if (loc == null) { // loc is null if there is an error such as meta not available.
          it.remove();
        } else if (canTakeOperation(loc, regionIncluded, serverIncluded)) {
          Action<Row> action = new Action<Row>(r, ++posInList);
          setNonce(ng, r, action);
          retainedActions.add(action);
          addAction(loc, action, actionsByServer, ng);
          it.remove();
        }
      }
    } while (retainedActions.isEmpty() && atLeastOne && !hasError());

    HConnectionManager.ServerErrorTracker errorsByServer = createServerErrorTracker();
    sendMultiAction(retainedActions, actionsByServer, 1, errorsByServer, batchCallback);
  }

  /**
   * Group the actions per region server.
   *
   * @param loc - the destination. Must not be null.
   * @param action - the action to add to the multiaction
   * @param actionsByServer the multiaction per server
   * @param ng Nonce generator, or null if no nonces are needed.
   */
  private void addAction(HRegionLocation loc, Action<Row> action, Map<HRegionLocation,
      MultiAction<Row>> actionsByServer, NonceGenerator ng) {
    final byte[] regionName = loc.getRegionInfo().getRegionName();
    MultiAction<Row> multiAction = actionsByServer.get(loc);
    if (multiAction == null) {
      multiAction = new MultiAction<Row>();
      actionsByServer.put(loc, multiAction);
    }
    if (action.hasNonce() && !multiAction.hasNonceGroup()) {
      // TODO: this code executes for every (re)try, and calls getNonceGroup again
      //       for the same action. It must return the same value across calls.
      multiAction.setNonceGroup(ng.getNonceGroup());
    }

    multiAction.add(regionName, action);
  }

  /**
   * Find the destination.
   *
   * @param row          the row
   * @param posInList    the position in the list
   * @return the destination. Null if we couldn't find it.
   */
  private HRegionLocation findDestLocation(Row row, int posInList) {
    if (row == null) throw new IllegalArgumentException("#" + id + ", row cannot be null");
    HRegionLocation loc = null;
    IOException locationException = null;
    try {
      loc = hConnection.locateRegion(this.tableName, row.getRow());
      if (loc == null) {
        locationException = new IOException("#" + id + ", no location found, aborting submit for" +
            " tableName=" + tableName +
            " rowkey=" + Arrays.toString(row.getRow()));
      }
    } catch (IOException e) {
      locationException = e;
    }
    if (locationException != null) {
      // There are multiple retries in locateRegion already. No need to add new.
      // We can't continue with this row, hence it's the last retry.
      manageError(posInList, row, false, locationException, null);
      return null;
    }

    return loc;
  }

  /**
   * Check if we should send new operations to this region or region server.
   * We're taking into account the past decision; if we have already accepted
   * operation on a given region, we accept all operations for this region.
   *
   * @param loc; the region and the server name we want to use.
   * @return true if this region is considered as busy.
   */
  protected boolean canTakeOperation(HRegionLocation loc,
                                     Map<Long, Boolean> regionsIncluded,
                                     Map<ServerName, Boolean> serversIncluded) {
    long regionId = loc.getRegionInfo().getRegionId();
    Boolean regionPrevious = regionsIncluded.get(regionId);

    if (regionPrevious != null) {
      // We already know what to do with this region.
      return regionPrevious;
    }

    Boolean serverPrevious = serversIncluded.get(loc.getServerName());
    if (Boolean.FALSE.equals(serverPrevious)) {
      // It's a new region, on a region server that we have already excluded.
      regionsIncluded.put(regionId, Boolean.FALSE);
      return false;
    }

    AtomicInteger regionCnt = taskCounterPerRegion.get(loc.getRegionInfo().getRegionName());
    if (regionCnt != null && regionCnt.get() >= maxConcurrentTasksPerRegion) {
      // Too many tasks on this region already.
      regionsIncluded.put(regionId, Boolean.FALSE);
      return false;
    }

    if (serverPrevious == null) {
      // The region is ok, but we need to decide for this region server.
      int newServers = 0; // number of servers we're going to contact so far
      for (Map.Entry<ServerName, Boolean> kv : serversIncluded.entrySet()) {
        if (kv.getValue()) {
          newServers++;
        }
      }

      // Do we have too many total tasks already?
      boolean ok = (newServers + getCurrentTasksCount()) < maxTotalConcurrentTasks;

      if (ok) {
        // If the total is fine, is it ok for this individual server?
        AtomicInteger serverCnt = taskCounterPerServer.get(loc.getServerName());
        ok = (serverCnt == null || serverCnt.get() < maxConcurrentTasksPerServer);
      }

      if (!ok) {
        regionsIncluded.put(regionId, Boolean.FALSE);
        serversIncluded.put(loc.getServerName(), Boolean.FALSE);
        return false;
      }

      serversIncluded.put(loc.getServerName(), Boolean.TRUE);
    } else {
      assert serverPrevious.equals(Boolean.TRUE);
    }

    regionsIncluded.put(regionId, Boolean.TRUE);

    return true;
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
    NonceGenerator ng = this.hConnection.getNonceGenerator();
    for (Row r : rows) {
      posInList++;
      if (r instanceof Put) {
        Put put = (Put) r;
        if (put.isEmpty()) {
          throw new IllegalArgumentException("No columns to insert for #" + (posInList+1)+ " item");
        }
      }
      Action<Row> action = new Action<Row>(r, posInList);
      setNonce(ng, r, action);
      actions.add(action);
    }
    HConnectionManager.ServerErrorTracker errorsByServer = createServerErrorTracker();
    submit(actions, actions, 1, errorsByServer);
  }

  private void setNonce(NonceGenerator ng, Row r, Action<Row> action) {
    if (!(r instanceof Append) && !(r instanceof Increment)) return;
    action.setNonce(ng.newNonce()); // Action handles NO_NONCE, so it's ok if ng is disabled.
  }


  /**
   * Group a list of actions per region servers, and send them. The created MultiActions are
   * added to the inProgress list. Does not take into account the region/server load.
   *
   * @param initialActions - the full list of the actions in progress
   * @param currentActions - the list of row to submit
   * @param numAttempt - the current numAttempt (first attempt is 1)
   */
  private void submit(List<Action<Row>> initialActions,
                      List<Action<Row>> currentActions, int numAttempt,
                      final HConnectionManager.ServerErrorTracker errorsByServer) {

    if (numAttempt > 1){
      retriesCnt.incrementAndGet();
    }

    // group per location => regions server
    final Map<HRegionLocation, MultiAction<Row>> actionsByServer =
        new HashMap<HRegionLocation, MultiAction<Row>>();

    NonceGenerator ng = this.hConnection.getNonceGenerator();
    for (Action<Row> action : currentActions) {
      HRegionLocation loc = findDestLocation(action.getAction(), action.getOriginalIndex());
      if (loc != null) {
        addAction(loc, action, actionsByServer, ng);
      }
    }

    if (!actionsByServer.isEmpty()) {
      sendMultiAction(initialActions, actionsByServer, numAttempt, errorsByServer, null);
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
                              final HConnectionManager.ServerErrorTracker errorsByServer,
                              Batch.Callback<CResult> batchCallback) {
    // Send the queries and add them to the inProgress list
    // This iteration is by server (the HRegionLocation comparator is by server portion only).
    for (Map.Entry<HRegionLocation, MultiAction<Row>> e : actionsByServer.entrySet()) {
      HRegionLocation loc = e.getKey();
      MultiAction<Row> multiAction = e.getValue();
      Collection<? extends Runnable> runnables = getNewMultiActionRunnable(initialActions, loc,
        multiAction, numAttempt, errorsByServer, batchCallback);
      for (Runnable runnable: runnables) {
        try {
          incTaskCounters(multiAction.getRegions(), loc.getServerName());
          this.pool.submit(runnable);
        } catch (RejectedExecutionException ree) {
          // This should never happen. But as the pool is provided by the end user, let's secure
          //  this a little.
          decTaskCounters(multiAction.getRegions(), loc.getServerName());
          LOG.warn("#" + id + ", the task was rejected by the pool. This is unexpected." +
            " Server is " + loc.getServerName(), ree);
          // We're likely to fail again, but this will increment the attempt counter, so it will
          //  finish.
          receiveGlobalFailure(initialActions, multiAction, loc, numAttempt, ree, errorsByServer);
        }
      }
    }
  }

  private Runnable getNewSingleServerRunnable(
      final List<Action<Row>> initialActions,
      final HRegionLocation loc,
      final MultiAction<Row> multiAction,
      final int numAttempt,
      final HConnectionManager.ServerErrorTracker errorsByServer,
      final Batch.Callback<CResult> batchCallback) {
    return new Runnable() {
      @Override
      public void run() {
        MultiResponse res;
        try {
          MultiServerCallable<Row> callable = createCallable(loc, multiAction);
          try {
            res = createCaller(callable).callWithoutRetries(callable, timeout);
          } catch (IOException e) {
            // The service itself failed . It may be an error coming from the communication
            //   layer, but, as well, a functional error raised by the server.
            receiveGlobalFailure(initialActions, multiAction, loc, numAttempt, e,
                errorsByServer);
            return;
          } catch (Throwable t) {
            // This should not happen. Let's log & retry anyway.
            LOG.error("#" + id + ", Caught throwable while calling. This is unexpected." +
                " Retrying. Server is " + loc.getServerName() + ", tableName=" + tableName, t);
            receiveGlobalFailure(initialActions, multiAction, loc, numAttempt, t,
                errorsByServer);
            return;
          }

          // Nominal case: we received an answer from the server, and it's not an exception.
          receiveMultiAction(initialActions, multiAction, loc, res, numAttempt, errorsByServer,
            batchCallback);

        } finally {
          decTaskCounters(multiAction.getRegions(), loc.getServerName());
        }
      }
    };
  }

  private Collection<? extends Runnable> getNewMultiActionRunnable(
      final List<Action<Row>> initialActions,
      final HRegionLocation loc,
      final MultiAction<Row> multiAction,
      final int numAttempt,
      final HConnectionManager.ServerErrorTracker errorsByServer,
      final Batch.Callback<CResult> batchCallback) {
    // no stats to manage, just do the standard action
    if (AsyncProcess.this.hConnection.getStatisticsTracker() == null) {
      List<Runnable> toReturn = new ArrayList<Runnable>(1);
      toReturn.add(Trace.wrap("AsyncProcess.sendMultiAction", 
        getNewSingleServerRunnable(initialActions, loc, multiAction, numAttempt,
          errorsByServer, batchCallback)));
      return toReturn;
    } else {
      // group the actions by the amount of delay
      Map<Long, DelayingRunner> actions = new HashMap<Long, DelayingRunner>(multiAction
        .size());

      // split up the actions
      for (Map.Entry<byte[], List<Action<Row>>> e : multiAction.actions.entrySet()) {
        Long backoff = getBackoff(loc);
        DelayingRunner runner = actions.get(backoff);
        if (runner == null) {
          actions.put(backoff, new DelayingRunner(backoff, e));
        } else {
          runner.add(e);
        }
      }

      List<Runnable> toReturn = new ArrayList<Runnable>(actions.size());
      for (DelayingRunner runner : actions.values()) {
        String traceText = "AsyncProcess.sendMultiAction";
        Runnable runnable = getNewSingleServerRunnable(initialActions, loc, runner.getActions(),
          numAttempt, errorsByServer, batchCallback);
        // use a delay runner only if we need to sleep for some time
        if (runner.getSleepTime() > 0) {
          runner.setRunner(runnable);
          traceText = "AsyncProcess.clientBackoff.sendMultiAction";
          runnable = runner;
        }
        runnable = Trace.wrap(traceText, runnable);
        toReturn.add(runnable);
      }
      return toReturn;
    }
  }

  /**
   * @param server server location where the target region is hosted
   * @param regionName name of the region which we are going to write some data
   * @return the amount of time the client should wait until it submit a request to the
   * specified server and region
   */
  private Long getBackoff(HRegionLocation location) {
    ServerStatisticTracker tracker = AsyncProcess.this.hConnection.getStatisticsTracker();
    ServerStatistics stats = tracker.getStats(location.getServerName());
    return AsyncProcess.this.hConnection.getBackoffPolicy()
      .getBackoffTime(location.getServerName(), location.getRegionInfo().getRegionName(),
        stats);
  }

  /**
   * Create a callable. Isolated to be easily overridden in the tests.
   */
  protected MultiServerCallable<Row> createCallable(final HRegionLocation location,
      final MultiAction<Row> multi) {
    return new MultiServerCallable<Row>(hConnection, tableName, location, this.rpcFactory, multi);
  }

  /**
   * For tests.
   * @param callable: used in tests.
   * @return Returns a caller.
   */
  protected RpcRetryingCaller<MultiResponse> createCaller(MultiServerCallable<Row> callable) {
    return rpcCallerFactory.<MultiResponse> newCaller();
  }

  /**
   * Check that we can retry acts accordingly: logs, set the error status, call the callbacks.
   *
   * @param originalIndex the position in the list sent
   * @param row           the row
   * @param canRetry      if false, we won't retry whatever the settings.
   * @param throwable     the throwable, if any (can be null)
   * @param location      the location, if any (can be null)
   * @return true if the action can be retried, false otherwise.
   */
  private boolean manageError(int originalIndex, Row row, boolean canRetry,
                              Throwable throwable, HRegionLocation location) {
    if (canRetry && throwable != null && throwable instanceof DoNotRetryIOException) {
      canRetry = false;
    }

    byte[] region = null;
    if (canRetry && callback != null) {
      region = location == null ? null : location.getRegionInfo().getEncodedNameAsBytes();
      canRetry = callback.retriableFailure(originalIndex, row, region, throwable);
    }

    if (!canRetry) {
      if (callback != null) {
        if (region == null && location != null) {
          region = location.getRegionInfo().getEncodedNameAsBytes();
        }
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
  private void receiveGlobalFailure(List<Action<Row>> initialActions, MultiAction<Row> rsActions,
                                    HRegionLocation location, int numAttempt, Throwable t,
                                    HConnectionManager.ServerErrorTracker errorsByServer) {
    // Do not use the exception for updating cache because it might be coming from
    // any of the regions in the MultiAction.
    hConnection.updateCachedLocations(tableName,
      rsActions.actions.values().iterator().next().get(0).getAction().getRow(), null, location);
    errorsByServer.reportServerError(location);
    boolean canRetry = errorsByServer.canRetryMore(numAttempt);

    List<Action<Row>> toReplay = new ArrayList<Action<Row>>(initialActions.size());
    for (Map.Entry<byte[], List<Action<Row>>> e : rsActions.actions.entrySet()) {
      for (Action<Row> action : e.getValue()) {
        if (manageError(action.getOriginalIndex(), action.getAction(), canRetry, t, location)) {
          toReplay.add(action);
        }
      }
    }

    logAndResubmit(initialActions, location, toReplay, numAttempt, rsActions.size(),
        t, errorsByServer);
  }

  /**
   * Log as many info as possible, and, if there is something to replay, submit it again after
   *  a back off sleep.
   */
  private void logAndResubmit(List<Action<Row>> initialActions, HRegionLocation oldLocation,
                              List<Action<Row>> toReplay, int numAttempt, int failureCount,
                              Throwable throwable,
                              HConnectionManager.ServerErrorTracker errorsByServer) {
    if (toReplay.isEmpty()) {
      // it's either a success or a last failure
      if (failureCount != 0) {
        // We have a failure but nothing to retry. We're done, it's a final failure..
        LOG.warn(createLog(numAttempt, failureCount, toReplay.size(),
            oldLocation.getServerName(), throwable, -1, false,
            errorsByServer.getStartTrackingTime()));
      } else if (numAttempt > startLogErrorsCnt + 1) {
        // The operation was successful, but needed several attempts. Let's log this.
        LOG.info(createLog(numAttempt, failureCount, 0,
            oldLocation.getServerName(), throwable, -1, false,
            errorsByServer.getStartTrackingTime()));
      }
      return;
    }

    // We have something to replay. We're going to sleep a little before.

    // We have two contradicting needs here:
    //  1) We want to get the new location after having slept, as it may change.
    //  2) We want to take into account the location when calculating the sleep time.
    // It should be possible to have some heuristics to take the right decision. Short term,
    //  we go for one.
    long backOffTime = errorsByServer.calculateBackoffTime(oldLocation, pause);

    if (numAttempt > startLogErrorsCnt) {
      // We use this value to have some logs when we have multiple failures, but not too many
      //  logs, as errors are to be expected when a region moves, splits and so on
      LOG.info(createLog(numAttempt, failureCount, toReplay.size(),
          oldLocation.getServerName(), throwable, backOffTime, true,
          errorsByServer.getStartTrackingTime()));
    }

    try {
      Thread.sleep(backOffTime);
    } catch (InterruptedException e) {
      LOG.warn("#" + id + ", not sent: " + toReplay.size() + " operations, " + oldLocation, e);
      Thread.currentThread().interrupt();
      return;
    }

    submit(initialActions, toReplay, numAttempt + 1, errorsByServer);
  }

  /**
   * Called when we receive the result of a server query.
   *
   * @param initialActions - the whole action list
   * @param multiAction    - the multiAction we sent
   * @param location       - the location. It's used as a server name.
   * @param responses      - the response, if any
   * @param numAttempt     - the attempt
   */
  private void receiveMultiAction(List<Action<Row>> initialActions, MultiAction<Row> multiAction,
                                  HRegionLocation location,
                                  MultiResponse responses, int numAttempt,
                                  HConnectionManager.ServerErrorTracker errorsByServer,
                                  Batch.Callback<CResult> batchCallback) {
     assert responses != null;

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

      boolean regionFailureRegistered = false;
      for (Pair<Integer, Object> regionResult : resultsForRS.getValue()) {
        Object result = regionResult.getSecond();

        // Failure: retry if it's make sense else update the errors lists
        if (result == null || result instanceof Throwable) {
          throwable = (Throwable) result;
          Action<Row> correspondingAction = initialActions.get(regionResult.getFirst());
          Row row = correspondingAction.getAction();
          failureCount++;
          if (!regionFailureRegistered) { // We're doing this once per location.
            regionFailureRegistered= true;
            // The location here is used as a server name.
            hConnection.updateCachedLocations(this.tableName, row.getRow(), result, location);
            if (failureCount == 1) {
              errorsByServer.reportServerError(location);
              canRetry = errorsByServer.canRetryMore(numAttempt);
            }
          }

          if (manageError(correspondingAction.getOriginalIndex(), row, canRetry,
              throwable, location)) {
            toReplay.add(correspondingAction);
          }
        } else { // success
          if (callback != null || batchCallback != null) {
            int index = regionResult.getFirst();
            Action<Row> correspondingAction = initialActions.get(index);
            Row row = correspondingAction.getAction();
            if (callback != null) {
              //noinspection unchecked
              this.callback.success(index, resultsForRS.getKey(), row, (CResult) result);
            }
            if (batchCallback != null) {
              batchCallback.update(resultsForRS.getKey(), row.getRow(), (CResult) result);
            }
          }
        }
      }
    }

    // The failures global to a region. We will use for multiAction we sent previously to find the
    //   actions to replay.

    for (Map.Entry<byte[], Throwable> throwableEntry : responses.getExceptions().entrySet()) {
      throwable = throwableEntry.getValue();
      byte[] region =throwableEntry.getKey();
      List<Action<Row>> actions = multiAction.actions.get(region);
      if (actions == null || actions.isEmpty()) {
        throw new IllegalStateException("Wrong response for the region: " +
            HRegionInfo.encodeRegionName(region));
      }

      if (failureCount == 0) {
        errorsByServer.reportServerError(location);
        canRetry = errorsByServer.canRetryMore(numAttempt);
      }
      hConnection.updateCachedLocations(this.tableName, actions.get(0).getAction().getRow(),
          throwable, location);
      failureCount += actions.size();

      for (Action<Row> action : actions) {
        Row row = action.getAction();
        if (manageError(action.getOriginalIndex(), row, canRetry, throwable, location)) {
          toReplay.add(action);
        }
      }
    }

    logAndResubmit(initialActions, location, toReplay, numAttempt, failureCount,
        throwable, errorsByServer);
  }

  private String createLog(int numAttempt, int failureCount, int replaySize, ServerName sn,
                           Throwable error, long backOffTime, boolean willRetry, String startTime){
    StringBuilder sb = new StringBuilder();

    sb.append("#").append(id).append(", table=").append(tableName).
        append(", attempt=").append(numAttempt).append("/").append(numTries).append(" ");

    if (failureCount > 0 || error != null){
      sb.append("failed ").append(failureCount).append(" ops").append(", last exception: ").
          append(error == null ? "null" : error);
    } else {
      sb.append("SUCCEEDED");
    }

    sb.append(" on ").append(sn);

    sb.append(", tracking started ").append(startTime);

    if (willRetry) {
      sb.append(", retrying after ").append(backOffTime).append(" ms").
          append(", replay ").append(replaySize).append(" ops.");
    } else if (failureCount > 0) {
      sb.append(" - FAILED, NOT RETRYING ANYMORE");
    }

    return sb.toString();
  }

  /**
   * Waits for another task to finish.
   * @param currentNumberOfTask - the number of task finished when calling the method.
   */
  protected void waitForNextTaskDone(long currentNumberOfTask) throws InterruptedIOException {
    synchronized (this.tasksDone) {
      while (currentNumberOfTask == tasksDone.get()) {
        try {
          this.tasksDone.wait(100);
        } catch (InterruptedException e) {
          throw new InterruptedIOException("#" + id + ", interrupted." +
              " currentNumberOfTask=" + currentNumberOfTask +
              ",  tableName=" + tableName + ", tasksDone=" + tasksDone.get());
        }
      }
    }
  }

  /**
   * Wait until the async does not have more than max tasks in progress.
   */
  private void waitForMaximumCurrentTasks(int max) throws InterruptedIOException {
    long lastLog = EnvironmentEdgeManager.currentTimeMillis();
    long currentTasksDone = this.tasksDone.get();

    while ((tasksSent.get() - currentTasksDone) > max) {
      long now = EnvironmentEdgeManager.currentTimeMillis();
      if (now > lastLog + 10000) {
        lastLog = now;
        LOG.info("#" + id + ", waiting for some tasks to finish. Expected max="
            + max + ", tasksSent=" + tasksSent.get() + ", tasksDone=" + tasksDone.get() +
            ", currentTasksDone=" + currentTasksDone + ", retries=" + retriesCnt.get() +
            " hasError=" + hasError() + ", tableName=" + tableName);
      }
      waitForNextTaskDone(currentTasksDone);
      currentTasksDone = this.tasksDone.get();
    }
  }

  private long getCurrentTasksCount(){
    return  tasksSent.get() - tasksDone.get();
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
    hasError.set(false);
  }

  public RetriesExhaustedWithDetailsException getErrors() {
    return errors.makeException();
  }

  /**
   * increment the tasks counters for a given set of regions. MT safe.
   */
  protected void incTaskCounters(Collection<byte[]> regions, ServerName sn) {
    tasksSent.incrementAndGet();

    AtomicInteger serverCnt = taskCounterPerServer.get(sn);
    if (serverCnt == null) {
      taskCounterPerServer.putIfAbsent(sn, new AtomicInteger());
      serverCnt = taskCounterPerServer.get(sn);
    }
    serverCnt.incrementAndGet();

    for (byte[] regBytes : regions) {
      AtomicInteger regionCnt = taskCounterPerRegion.get(regBytes);
      if (regionCnt == null) {
        regionCnt = new AtomicInteger();
        AtomicInteger oldCnt = taskCounterPerRegion.putIfAbsent(regBytes, regionCnt);
        if (oldCnt != null) {
          regionCnt = oldCnt;
        }
      }
      regionCnt.incrementAndGet();
    }
  }

  /**
   * Decrements the counters for a given region and the region server. MT Safe.
   */
  protected void decTaskCounters(Collection<byte[]> regions, ServerName sn) {
    for (byte[] regBytes : regions) {
      AtomicInteger regionCnt = taskCounterPerRegion.get(regBytes);
      regionCnt.decrementAndGet();
    }

    taskCounterPerServer.get(sn).decrementAndGet();

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
    return new HConnectionManager.ServerErrorTracker(this.serverTrackerTimeout, this.numTries);
  }
}
