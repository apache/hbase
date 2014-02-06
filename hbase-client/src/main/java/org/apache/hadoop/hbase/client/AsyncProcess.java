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
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.htrace.Trace;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class  allows a continuous flow of requests. It's written to be compatible with a
 * synchronous caller such as HTable.
 * <p>
 * The caller sends a buffer of operation, by calling submit. This class extract from this list
 * the operations it can send, i.e. the operations that are on region that are not considered
 * as busy. The process is asynchronous, i.e. it returns immediately when if has finished to
 * iterate on the list. If, and only if, the maximum number of current task is reached, the call
 * to submit will block. Alternatively, the caller can call submitAll, in which case all the
 * operations will be sent. Each call to submit returns a future-like object that can be used
 * to track operation progress.
 * </p>
 * <p>
 * The class manages internally the retries.
 * </p>
 * <p>
 * The class can be constructed in regular mode, or "global error" mode. In global error mode,
 * AP tracks errors across all calls (each "future" also has global view of all errors). That
 * mode is necessary for backward compat with HTable behavior, where multiple submissions are
 * made and the errors can propagate using any put/flush call, from previous calls.
 * In "regular" mode, the errors are tracked inside the Future object that is returned.
 * The results are always tracked inside the Future object and can be retrieved when the call
 * has finished. Partial results can also be retrieved if some part of multi-request failed.
 * </p>
 * <p>
 * This class is thread safe in regular mode; in global error code, submitting operations and
 * retrieving errors from different threads may be not thread safe.
 * Internally, the class is thread safe enough to manage simultaneously new submission and results
 * arising from older operations.
 * </p>
 * <p>
 * Internally, this class works with {@link Row}, this mean it could be theoretically used for
 * gets as well.
 * </p>
 */
class AsyncProcess {
  private static final Log LOG = LogFactory.getLog(AsyncProcess.class);
  protected static final AtomicLong COUNTER = new AtomicLong();

  /**
   * The context used to wait for results from one submit call.
   * 1) If AsyncProcess is set to track errors globally, and not per call (for HTable puts),
   *    then errors and failed operations in this object will reflect global errors.
   * 2) If submit call is made with needResults false, results will not be saved.
   *  */
  public static interface AsyncRequestFuture {
    public boolean hasError();
    public RetriesExhaustedWithDetailsException getErrors();
    public List<? extends Row> getFailedOperations();
    public Object[] getResults();
    /** Wait until all tasks are executed, successfully or not. */
    public void waitUntilDone() throws InterruptedIOException;
  }

  /** Return value from a submit that didn't contain any requests. */
  private static final AsyncRequestFuture NO_REQS_RESULT = new AsyncRequestFuture() {
    public final Object[] result = new Object[0];
    @Override
    public boolean hasError() { return false; }
    @Override
    public RetriesExhaustedWithDetailsException getErrors() { return null; }
    @Override
    public List<? extends Row> getFailedOperations() { return null; }
    @Override
    public Object[] getResults() { return result; }
    @Override
    public void waitUntilDone() throws InterruptedIOException {}
  };

  protected final long id;

  protected final ClusterConnection hConnection;
  protected final RpcRetryingCallerFactory rpcCallerFactory;
  protected final RpcControllerFactory rpcFactory;
  protected final BatchErrors globalErrors;
  protected final ExecutorService pool;

  protected final AtomicLong tasksInProgress = new AtomicLong(0);
  protected final ConcurrentMap<byte[], AtomicInteger> taskCounterPerRegion =
      new ConcurrentSkipListMap<byte[], AtomicInteger>(Bytes.BYTES_COMPARATOR);
  protected final ConcurrentMap<ServerName, AtomicInteger> taskCounterPerServer =
      new ConcurrentHashMap<ServerName, AtomicInteger>();

  // Start configuration settings.
  private final int startLogErrorsCnt;

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
  protected int timeout;
  // End configuration settings.

  protected static class BatchErrors {
    private final List<Throwable> throwables = new ArrayList<Throwable>();
    private final List<Row> actions = new ArrayList<Row>();
    private final List<String> addresses = new ArrayList<String>();

    public synchronized void add(Throwable ex, Row row, ServerName serverName) {
      if (row == null){
        throw new IllegalArgumentException("row cannot be null. location=" + serverName);
      }

      throwables.add(ex);
      actions.add(row);
      addresses.add(serverName != null ? serverName.toString() : "null");
    }

    public boolean hasErrors() {
      return !throwables.isEmpty();
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

  public AsyncProcess(ClusterConnection hc, Configuration conf, ExecutorService pool,
      RpcRetryingCallerFactory rpcCaller, boolean useGlobalErrors, RpcControllerFactory rpcFactory) {
    if (hc == null) {
      throw new IllegalArgumentException("HConnection cannot be null.");
    }

    this.hConnection = hc;
    this.pool = pool;
    this.globalErrors = useGlobalErrors ? new BatchErrors() : null;

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

    // A few failure is fine: region moved, then is not opened, then is overloaded. We try
    //  to have an acceptable heuristic for the number of errors we don't log.
    //  9 was chosen because we wait for 1s at this stage.
    this.startLogErrorsCnt = conf.getInt("hbase.client.start.log.errors.counter", 9);

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
    this.rpcFactory = rpcFactory;
  }

  private ExecutorService getPool(ExecutorService pool) {
    if (pool != null) return pool;
    if (this.pool != null) return this.pool;
    throw new RuntimeException("Neither AsyncProcess nor request have ExecutorService");
  }

  /**
   * See {@link #submit(ExecutorService, TableName, List, boolean, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback, boolean)}.
   * Uses default ExecutorService for this AP (must have been created with one).
   */
  public <CResult> AsyncRequestFuture submit(TableName tableName, List<? extends Row> rows,
      boolean atLeastOne, Batch.Callback<CResult> callback, boolean needResults) throws InterruptedIOException {
    return submit(null, tableName, rows, atLeastOne, callback, needResults);
  }

  /**
   * Extract from the rows list what we can submit. The rows we can not submit are kept in the
   * list.
   *
   * @param pool ExecutorService to use.
   * @param tableName The table for which this request is needed.
   * @param callback Batch callback. Only called on success (94 behavior).
   * @param needResults Whether results are needed, or can be discarded.
   * @param rows - the submitted row. Modified by the method: we remove the rows we took.
   * @param atLeastOne true if we should submit at least a subset.
   */
  public <CResult> AsyncRequestFuture submit(ExecutorService pool, TableName tableName,
      List<? extends Row> rows, boolean atLeastOne, Batch.Callback<CResult> callback,
      boolean needResults) throws InterruptedIOException {
    if (rows.isEmpty()) {
      return NO_REQS_RESULT;
    }

    Map<ServerName, MultiAction<Row>> actionsByServer =
        new HashMap<ServerName, MultiAction<Row>>();
    List<Action<Row>> retainedActions = new ArrayList<Action<Row>>(rows.size());

    NonceGenerator ng = this.hConnection.getNonceGenerator();
    long nonceGroup = ng.getNonceGroup(); // Currently, nonce group is per entire client.

    // Location errors that happen before we decide what requests to take.
    List<Exception> locationErrors = null;
    List<Integer> locationErrorRows = null;
    do {
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
        HRegionLocation loc;
        try {
          loc = findDestLocation(tableName, r);
        } catch (IOException ex) {
          locationErrors = new ArrayList<Exception>();
          locationErrorRows = new ArrayList<Integer>();
          LOG.error("Failed to get region location ", ex);
          // This action failed before creating ars. Add it to retained but do not add to submit list.
          // We will then add it to ars in an already-failed state.
          retainedActions.add(new Action<Row>(r, ++posInList));
          locationErrors.add(ex);
          locationErrorRows.add(posInList);
          it.remove();
          break; // Backward compat: we stop considering actions on location error.
        }

        if (canTakeOperation(loc, regionIncluded, serverIncluded)) {
          Action<Row> action = new Action<Row>(r, ++posInList);
          setNonce(ng, r, action);
          retainedActions.add(action);
          addAction(loc, action, actionsByServer, nonceGroup);
          it.remove();
        }
      }
    } while (retainedActions.isEmpty() && atLeastOne && (locationErrors == null));

    if (retainedActions.isEmpty()) return NO_REQS_RESULT;

    AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(
        tableName, retainedActions, nonceGroup, pool, callback, null, needResults);
    // Add location errors if any
    if (locationErrors != null) {
      for (int i = 0; i < locationErrors.size(); ++i) {
        int originalIndex = locationErrorRows.get(i);
        Row row = retainedActions.get(originalIndex).getAction();
        ars.manageError(originalIndex, row, false, locationErrors.get(i), null);
      }
    }
    ars.sendMultiAction(actionsByServer, 1);
    return ars;
  }

  /**
   * Helper that is used when grouping the actions per region server.
   *
   * @param loc - the destination. Must not be null.
   * @param action - the action to add to the multiaction
   * @param actionsByServer the multiaction per server
   * @param nonceGroup Nonce group.
   */
  private void addAction(HRegionLocation loc, Action<Row> action,
      Map<ServerName, MultiAction<Row>> actionsByServer, long nonceGroup) {
    final byte[] regionName = loc.getRegionInfo().getRegionName();
    MultiAction<Row> multiAction = actionsByServer.get(loc.getServerName());
    if (multiAction == null) {
      multiAction = new MultiAction<Row>();
      actionsByServer.put(loc.getServerName(), multiAction);
    }
    if (action.hasNonce() && !multiAction.hasNonceGroup()) {
      multiAction.setNonceGroup(nonceGroup);
    }

    multiAction.add(regionName, action);
  }

  /**
   * Find the destination.
   * @param tableName the requisite table.
   * @param row the row
   * @return the destination.
   */
  private HRegionLocation findDestLocation(TableName tableName, Row row) throws IOException {
    if (row == null) throw new IllegalArgumentException("#" + id + ", row cannot be null");
    HRegionLocation loc = hConnection.locateRegion(tableName, row.getRow());
    if (loc == null) {
      throw new IOException("#" + id + ", no location found, aborting submit for" +
          " tableName=" + tableName + " rowkey=" + Arrays.toString(row.getRow()));
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
      boolean ok = (newServers + tasksInProgress.get()) < maxTotalConcurrentTasks;

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
   * See {@link #submitAll(ExecutorService, TableName, List, org.apache.hadoop.hbase.client.coprocessor.Batch.Callback, Object[])}.
   * Uses default ExecutorService for this AP (must have been created with one).
   */
  public <CResult> AsyncRequestFuture submitAll(TableName tableName,
      List<? extends Row> rows, Batch.Callback<CResult> callback, Object[] results) {
    return submitAll(null, tableName, rows, callback, results);
  }

  /**
   * Submit immediately the list of rows, whatever the server status. Kept for backward
   * compatibility: it allows to be used with the batch interface that return an array of objects.
   *
   * @param pool ExecutorService to use.
   * @param tableName name of the table for which the submission is made.
   * @param rows the list of rows.
   * @param callback the callback.
   * @param results Optional array to return the results thru; backward compat.
   */
  public <CResult> AsyncRequestFuture submitAll(ExecutorService pool, TableName tableName,
      List<? extends Row> rows, Batch.Callback<CResult> callback, Object[] results) {
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
    AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(
        tableName, actions, ng.getNonceGroup(), getPool(pool), callback, results, results != null);
    ars.groupAndSendMultiAction(actions, 1);
    return ars;
  }

  private void setNonce(NonceGenerator ng, Row r, Action<Row> action) {
    if (!(r instanceof Append) && !(r instanceof Increment)) return;
    action.setNonce(ng.newNonce()); // Action handles NO_NONCE, so it's ok if ng is disabled.
  }

  /**
   * The context, and return value, for a single submit/submitAll call.
   * Note on how this class (one AP submit) works. Initially, all requests are split into groups
   * by server; request is sent to each server in parallel; the RPC calls are not async so a
   * thread per server is used. Every time some actions fail, regions/locations might have
   * changed, so we re-group them by server and region again and send these groups in parallel
   * too. The result, in case of retries, is a "tree" of threads, with parent exiting after
   * scheduling children. This is why lots of code doesn't require any synchronization.
   */
  protected class AsyncRequestFutureImpl<CResult> implements AsyncRequestFuture {
    private final Batch.Callback<CResult> callback;
    private final BatchErrors errors;
    private final ConnectionManager.ServerErrorTracker errorsByServer;
    private final ExecutorService pool;


    private final TableName tableName;
    private final AtomicLong actionsInProgress = new AtomicLong(-1);
    private final Object[] results;
    private final long nonceGroup;

    public AsyncRequestFutureImpl(TableName tableName, List<Action<Row>> actions, long nonceGroup,
        ExecutorService pool, boolean needResults, Object[] results,
        Batch.Callback<CResult> callback) {
      this.pool = pool;
      this.callback = callback;
      this.nonceGroup = nonceGroup;
      this.tableName = tableName;
      this.actionsInProgress.set(actions.size());
      if (results != null) {
        assert needResults;
        if (results.length != actions.size()) throw new AssertionError("results.length");
        this.results = results;
        for (int i = 0; i != this.results.length; ++i) {
          results[i] = null;
        }
      } else {
        this.results = needResults ? new Object[actions.size()] : null;
      }
      this.errorsByServer = createServerErrorTracker();
      this.errors = (globalErrors != null) ? globalErrors : new BatchErrors();
    }

    /**
     * Group a list of actions per region servers, and send them.
     *
     * @param currentActions - the list of row to submit
     * @param numAttempt - the current numAttempt (first attempt is 1)
     */
    private void groupAndSendMultiAction(List<Action<Row>> currentActions, int numAttempt) {
      // group per location => regions server
      final Map<ServerName, MultiAction<Row>> actionsByServer =
          new HashMap<ServerName, MultiAction<Row>>();

      HRegionLocation loc;
      for (Action<Row> action : currentActions) {
        try {
          loc = findDestLocation(tableName, action.getAction());
        } catch (IOException ex) {
          // There are multiple retries in locateRegion already. No need to add new.
          // We can't continue with this row, hence it's the last retry.
          manageError(action.getOriginalIndex(), action.getAction(), false, ex, null);
          continue;
        }
        addAction(loc, action, actionsByServer, nonceGroup);
      }

      if (!actionsByServer.isEmpty()) {
        sendMultiAction(actionsByServer, numAttempt);
      }
    }

    /**
     * Send a multi action structure to the servers, after a delay depending on the attempt
     * number. Asynchronous.
     *
     * @param actionsByServer the actions structured by regions
     * @param numAttempt      the attempt number.
     */
    private void sendMultiAction(
        Map<ServerName, MultiAction<Row>> actionsByServer, final int numAttempt) {
      // Run the last item on the same thread if we are already on a send thread.
      // We hope most of the time it will be the only item, so we can cut down on threads.
      int reuseThreadCountdown = (numAttempt > 1) ? actionsByServer.size() : Integer.MAX_VALUE;
      for (Map.Entry<ServerName, MultiAction<Row>> e : actionsByServer.entrySet()) {
        final ServerName server = e.getKey();
        final MultiAction<Row> multiAction = e.getValue();
        incTaskCounters(multiAction.getRegions(), server);
        Runnable runnable = Trace.wrap("AsyncProcess.sendMultiAction", new Runnable() {
          @Override
          public void run() {
            MultiResponse res;
            try {
              MultiServerCallable<Row> callable = createCallable(server, tableName, multiAction);
              try {
                res = createCaller(callable).callWithoutRetries(callable, timeout);
              } catch (IOException e) {
                // The service itself failed . It may be an error coming from the communication
                //   layer, but, as well, a functional error raised by the server.
                receiveGlobalFailure(multiAction, server, numAttempt, e);
                return;
              } catch (Throwable t) {
                // This should not happen. Let's log & retry anyway.
                LOG.error("#" + id + ", Caught throwable while calling. This is unexpected." +
                    " Retrying. Server is " + server.getServerName() + ", tableName=" + tableName, t);
                receiveGlobalFailure(multiAction, server, numAttempt, t);
                return;
              }

              // Normal case: we received an answer from the server, and it's not an exception.
              receiveMultiAction(multiAction, server, res, numAttempt);
            } catch (Throwable t) {
              // Something really bad happened. We are on the send thread that will now die.
              LOG.error("Internal AsyncProcess #" + id + " error for "
                  + tableName + " processing for " + server, t);
              throw new RuntimeException(t);
            } finally {
              decTaskCounters(multiAction.getRegions(), server);
            }
          }
        });
        --reuseThreadCountdown;
        if (reuseThreadCountdown == 0) {
          runnable.run();
        } else {
          try {
            pool.submit(runnable);
          } catch (RejectedExecutionException ree) {
            // This should never happen. But as the pool is provided by the end user, let's secure
            //  this a little.
            decTaskCounters(multiAction.getRegions(), server);
            LOG.warn("#" + id + ", the task was rejected by the pool. This is unexpected." +
                " Server is " + server.getServerName(), ree);
            // We're likely to fail again, but this will increment the attempt counter, so it will
            //  finish.
            receiveGlobalFailure(multiAction, server, numAttempt, ree);
          }
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
    public boolean manageError(int originalIndex, Row row, boolean canRetry,
                                Throwable throwable, ServerName server) {
      if (canRetry && throwable != null && throwable instanceof DoNotRetryIOException) {
        canRetry = false;
      }

      if (!canRetry) {
        // Batch.Callback<Res> was not called on failure in 0.94. We keep this.
        errors.add(throwable, row, server);
        if (results != null) {
          setResult(originalIndex, row, throwable);
        }
        decActionCounter();
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
        MultiAction<Row> rsActions, ServerName server, int numAttempt, Throwable t) {
      // Do not use the exception for updating cache because it might be coming from
      // any of the regions in the MultiAction.
      byte[] row = rsActions.actions.values().iterator().next().get(0).getAction().getRow();
      hConnection.updateCachedLocations(tableName, null, row, null, server);
      errorsByServer.reportServerError(server);
      boolean canRetry = errorsByServer.canRetryMore(numAttempt);

      List<Action<Row>> toReplay = new ArrayList<Action<Row>>();
      for (Map.Entry<byte[], List<Action<Row>>> e : rsActions.actions.entrySet()) {
        for (Action<Row> action : e.getValue()) {
          if (manageError(action.getOriginalIndex(), action.getAction(), canRetry, t, server)) {
            toReplay.add(action);
          }
        }
      }

      logAndResubmit(server, toReplay, numAttempt, rsActions.size(), t);
    }

    /**
     * Log as much info as possible, and, if there is something to replay,
     * submit it again after a back off sleep.
     */
    private void logAndResubmit(ServerName oldServer, List<Action<Row>> toReplay,
        int numAttempt, int failureCount, Throwable throwable) {
      if (toReplay.isEmpty()) {
        // it's either a success or a last failure
        if (failureCount != 0) {
          // We have a failure but nothing to retry. We're done, it's a final failure..
          LOG.warn(createLog(numAttempt, failureCount, toReplay.size(),
              oldServer, throwable, -1, false, errorsByServer.getStartTrackingTime()));
        } else if (numAttempt > startLogErrorsCnt + 1) {
          // The operation was successful, but needed several attempts. Let's log this.
          LOG.info(createLog(numAttempt, failureCount, 0,
              oldServer, throwable, -1, false, errorsByServer.getStartTrackingTime()));
        }
        return;
      }

      // We have something to replay. We're going to sleep a little before.

      // We have two contradicting needs here:
      //  1) We want to get the new location after having slept, as it may change.
      //  2) We want to take into account the location when calculating the sleep time.
      // It should be possible to have some heuristics to take the right decision. Short term,
      //  we go for one.
      long backOffTime = errorsByServer.calculateBackoffTime(oldServer, pause);
      if (numAttempt > startLogErrorsCnt) {
        // We use this value to have some logs when we have multiple failures, but not too many
        //  logs, as errors are to be expected when a region moves, splits and so on
        LOG.info(createLog(numAttempt, failureCount, toReplay.size(),
            oldServer, throwable, backOffTime, true, errorsByServer.getStartTrackingTime()));
      }

      try {
        Thread.sleep(backOffTime);
      } catch (InterruptedException e) {
        LOG.warn("#" + id + ", not sent: " + toReplay.size() + " operations, " + oldServer, e);
        Thread.currentThread().interrupt();
        return;
      }

      groupAndSendMultiAction(toReplay, numAttempt + 1);
    }

    /**
     * Called when we receive the result of a server query.
     *
     * @param multiAction    - the multiAction we sent
     * @param server       - the location. It's used as a server name.
     * @param responses      - the response, if any
     * @param numAttempt     - the attempt
     */
    private void receiveMultiAction(MultiAction<Row> multiAction,
        ServerName server, MultiResponse responses, int numAttempt) {
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

      // Go by original action.
      for (Map.Entry<byte[], List<Action<Row>>> regionEntry : multiAction.actions.entrySet()) {
        byte[] regionName = regionEntry.getKey();
        Map<Integer, Object> regionResults = responses.getResults().get(regionName);
        if (regionResults == null) {
          if (!responses.getExceptions().containsKey(regionName)) {
            LOG.error("Server sent us neither results nor exceptions for "
                + Bytes.toStringBinary(regionName));
            responses.getExceptions().put(regionName, new RuntimeException("Invalid response"));
          }
          continue;
        }
        boolean regionFailureRegistered = false;
        for (Action<Row> sentAction : regionEntry.getValue()) {
          Object result = regionResults.get(sentAction.getOriginalIndex());
          // Failure: retry if it's make sense else update the errors lists
          if (result == null || result instanceof Throwable) {
            Row row = sentAction.getAction();
            if (!regionFailureRegistered) { // We're doing this once per location.
              regionFailureRegistered = true;
              // The location here is used as a server name.
              hConnection.updateCachedLocations(tableName, regionName, row.getRow(), result, server);
              if (failureCount == 0) {
                errorsByServer.reportServerError(server);
                canRetry = errorsByServer.canRetryMore(numAttempt);
              }
            }
            ++failureCount;
            if (manageError(
                sentAction.getOriginalIndex(), row, canRetry, (Throwable)result, server)) {
              toReplay.add(sentAction);
            }
          } else {
            if (callback != null) {
              try {
                //noinspection unchecked
                this.callback.update(regionName, sentAction.getAction().getRow(), (CResult)result);
              } catch (Throwable t) {
                LOG.error("User callback threw an exception for "
                    + Bytes.toStringBinary(regionName) + ", ignoring", t);
              }
            }
            if (results != null) {
              setResult(sentAction.getOriginalIndex(), sentAction.getAction(), result);
            }
            decActionCounter();
          }
        }
      }

      // The failures global to a region. We will use for multiAction we sent previously to find the
      //   actions to replay.
      for (Map.Entry<byte[], Throwable> throwableEntry : responses.getExceptions().entrySet()) {
        throwable = throwableEntry.getValue();
        byte[] region = throwableEntry.getKey();
        List<Action<Row>> actions = multiAction.actions.get(region);
        if (actions == null || actions.isEmpty()) {
          throw new IllegalStateException("Wrong response for the region: " +
              HRegionInfo.encodeRegionName(region));
        }

        if (failureCount == 0) {
          errorsByServer.reportServerError(server);
          canRetry = errorsByServer.canRetryMore(numAttempt);
        }
        hConnection.updateCachedLocations(
            tableName, region, actions.get(0).getAction().getRow(), throwable, server);
        failureCount += actions.size();

        for (Action<Row> action : actions) {
          Row row = action.getAction();
          if (manageError(action.getOriginalIndex(), row, canRetry, throwable, server)) {
            toReplay.add(action);
          }
        }
      }

      logAndResubmit(server, toReplay, numAttempt, failureCount, throwable);
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

    private void setResult(int index, Row row, Object result) {
      if (result == null) throw new RuntimeException("Result cannot be set to null");
      if (results[index] != null) throw new RuntimeException("Result was already set");
      results[index] = result;
    }

    private void decActionCounter() {
      actionsInProgress.decrementAndGet();
      synchronized (actionsInProgress) {
        actionsInProgress.notifyAll();
      }
    }

    @Override
    public void waitUntilDone() throws InterruptedIOException {
      long lastLog = EnvironmentEdgeManager.currentTimeMillis();
      long currentInProgress;
      try {
        while (0 != (currentInProgress = actionsInProgress.get())) {
          long now = EnvironmentEdgeManager.currentTimeMillis();
          if (now > lastLog + 10000) {
            lastLog = now;
            LOG.info("#" + id + ", waiting for " + currentInProgress + "  actions to finish");
          }
          synchronized (actionsInProgress) {
            if (actionsInProgress.get() == 0) break;
            actionsInProgress.wait(100);
          }
        }
      } catch (InterruptedException iex) {
        throw new InterruptedIOException(iex.getMessage());
      }
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
      return errors.makeException();
    }

    @Override
    public Object[] getResults() {
      return results;
    }
  }


  @VisibleForTesting
  /** Create AsyncRequestFuture. Isolated to be easily overridden in the tests. */
  protected <CResult> AsyncRequestFutureImpl<CResult> createAsyncRequestFuture(
      TableName tableName, List<Action<Row>> actions, long nonceGroup, ExecutorService pool,
      Batch.Callback<CResult> callback, Object[] results, boolean needResults) {
    return new AsyncRequestFutureImpl<CResult>(
        tableName, actions, nonceGroup, getPool(pool), needResults, results, callback);
  }

  /**
   * Create a callable. Isolated to be easily overridden in the tests.
   */
  @VisibleForTesting
  protected MultiServerCallable<Row> createCallable(final ServerName server,
      TableName tableName, final MultiAction<Row> multi) {
    return new MultiServerCallable<Row>(hConnection, tableName, server, this.rpcFactory, multi);
  }

  /**
   * Create a caller. Isolated to be easily overridden in the tests.
   */
  @VisibleForTesting
  protected RpcRetryingCaller<MultiResponse> createCaller(MultiServerCallable<Row> callable) {
    return rpcCallerFactory.<MultiResponse> newCaller();
  }

  @VisibleForTesting
  /** Waits until all outstanding tasks are done. Used in tests. */
  void waitUntilDone() throws InterruptedIOException {
    waitForMaximumCurrentTasks(0);
  }

  /** Wait until the async does not have more than max tasks in progress. */
  private void waitForMaximumCurrentTasks(int max) throws InterruptedIOException {
    long lastLog = EnvironmentEdgeManager.currentTimeMillis();
    long currentInProgress, oldInProgress = Long.MAX_VALUE;
    while ((currentInProgress = this.tasksInProgress.get()) > max) {
      if (oldInProgress != currentInProgress) { // Wait for in progress to change.
        long now = EnvironmentEdgeManager.currentTimeMillis();
        if (now > lastLog + 10000) {
          lastLog = now;
          LOG.info("#" + id + ", waiting for some tasks to finish. Expected max="
              + max + ", tasksInProgress=" + currentInProgress);
        }
      }
      oldInProgress = currentInProgress;
      try {
        synchronized (this.tasksInProgress) {
          if (tasksInProgress.get() != oldInProgress) break;
          this.tasksInProgress.wait(100);
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException("#" + id + ", interrupted." +
            " currentNumberOfTask=" + currentInProgress);
      }
    }
  }

  /**
   * Only used w/useGlobalErrors ctor argument, for HTable backward compat.
   * @return Whether there were any errors in any request since the last time
   *          {@link #waitForAllPreviousOpsAndReset(List)} was called, or AP was created.
   */
  public boolean hasError() {
    return globalErrors.hasErrors();
  }

  /**
   * Only used w/useGlobalErrors ctor argument, for HTable backward compat.
   * Waits for all previous operations to finish, and returns errors and (optionally)
   * failed operations themselves.
   * @param failedRows an optional list into which the rows that failed since the last time
   *        {@link #waitForAllPreviousOpsAndReset(List)} was called, or AP was created, are saved.
   * @return all the errors since the last time {@link #waitForAllPreviousOpsAndReset(List)}
   *          was called, or AP was created.
   */
  public RetriesExhaustedWithDetailsException waitForAllPreviousOpsAndReset(
      List<Row> failedRows) throws InterruptedIOException {
    waitForMaximumCurrentTasks(0);
    if (!globalErrors.hasErrors()) {
      return null;
    }
    if (failedRows != null) {
      failedRows.addAll(globalErrors.actions);
    }
    RetriesExhaustedWithDetailsException result = globalErrors.makeException();
    globalErrors.clear();
    return result;
  }

  /**
   * increment the tasks counters for a given set of regions. MT safe.
   */
  protected void incTaskCounters(Collection<byte[]> regions, ServerName sn) {
    tasksInProgress.incrementAndGet();

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
    tasksInProgress.decrementAndGet();
    synchronized (tasksInProgress) {
      tasksInProgress.notifyAll();
    }
  }

  /**
   * Creates the server error tracker to use inside process.
   * Currently, to preserve the main assumption about current retries, and to work well with
   * the retry-limit-based calculation, the calculation is local per Process object.
   * We may benefit from connection-wide tracking of server errors.
   * @return ServerErrorTracker to use, null if there is no ServerErrorTracker on this connection
   */
  protected ConnectionManager.ServerErrorTracker createServerErrorTracker() {
    return new ConnectionManager.ServerErrorTracker(
        this.serverTrackerTimeout, this.numTries);
  }
}
