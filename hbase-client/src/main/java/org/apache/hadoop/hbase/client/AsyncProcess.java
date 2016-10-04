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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.AsyncProcess.RowChecker.ReturnCode;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

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
@InterfaceAudience.Private
class AsyncProcess {
  private static final Log LOG = LogFactory.getLog(AsyncProcess.class);
  protected static final AtomicLong COUNTER = new AtomicLong();

  public static final String PRIMARY_CALL_TIMEOUT_KEY = "hbase.client.primaryCallTimeout.multiget";

  /**
   * Configure the number of failures after which the client will start logging. A few failures
   * is fine: region moved, then is not opened, then is overloaded. We try to have an acceptable
   * heuristic for the number of errors we don't log. 9 was chosen because we wait for 1s at
   * this stage.
   */
  public static final String START_LOG_ERRORS_AFTER_COUNT_KEY =
      "hbase.client.start.log.errors.counter";
  public static final int DEFAULT_START_LOG_ERRORS_AFTER_COUNT = 9;

  /**
   * Configuration to decide whether to log details for batch error
   */
  public static final String LOG_DETAILS_FOR_BATCH_ERROR = "hbase.client.log.batcherrors.details";

  protected final int thresholdToLogUndoneTaskDetails;
  private static final String THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS =
      "hbase.client.threshold.log.details";
  private static final int DEFAULT_THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS = 10;
  private final int THRESHOLD_TO_LOG_REGION_DETAILS = 2;

  /**
   * The maximum size of single RegionServer.
   */
  public static final String HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE = "hbase.client.max.perrequest.heapsize";

  /**
   * Default value of #HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE
   */
  public static final long DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE = 4194304;

  /**
   * The maximum size of submit.
   */
  public static final String HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE = "hbase.client.max.submit.heapsize";
  /**
   * Default value of #HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE
   */
  public static final long DEFAULT_HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE = DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE;

  /**
   * Return value from a submit that didn't contain any requests.
   */
  private static final AsyncRequestFuture NO_REQS_RESULT = new AsyncRequestFuture() {
    final Object[] result = new Object[0];

    @Override
    public boolean hasError() {
      return false;
    }

    @Override
    public RetriesExhaustedWithDetailsException getErrors() {
      return null;
    }

    @Override
    public List<? extends Row> getFailedOperations() {
      return null;
    }

    @Override
    public Object[] getResults() {
      return result;
    }

    @Override
    public void waitUntilDone() throws InterruptedIOException {
    }
  };

  // TODO: many of the fields should be made private
  protected final long id;

  protected final ClusterConnection connection;
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
  protected final int startLogErrorsCnt;

  /**
   * The number of tasks simultaneously executed on the cluster.
   */
  protected final int maxTotalConcurrentTasks;

  /**
   * The max heap size of all tasks simultaneously executed on a server.
   */
  protected final long maxHeapSizePerRequest;
  protected final long maxHeapSizeSubmit;
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
  protected long primaryCallTimeoutMicroseconds;
  /** Whether to log details for batch errors */
  protected final boolean logBatchErrorDetails;
  // End configuration settings.

  public AsyncProcess(ClusterConnection hc, Configuration conf, ExecutorService pool,
      RpcRetryingCallerFactory rpcCaller, boolean useGlobalErrors,
      RpcControllerFactory rpcFactory, int rpcTimeout) {
    if (hc == null) {
      throw new IllegalArgumentException("ClusterConnection cannot be null.");
    }

    this.connection = hc;
    this.pool = pool;
    this.globalErrors = useGlobalErrors ? new BatchErrors() : null;

    this.id = COUNTER.incrementAndGet();

    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    // how many times we could try in total, one more than retry number
    this.numTries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER) + 1;
    this.timeout = rpcTimeout;
    this.primaryCallTimeoutMicroseconds = conf.getInt(PRIMARY_CALL_TIMEOUT_KEY, 10000);

    this.maxTotalConcurrentTasks = conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
      HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS);
    this.maxConcurrentTasksPerServer = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_PERSERVER_TASKS);
    this.maxConcurrentTasksPerRegion = conf.getInt(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS,
          HConstants.DEFAULT_HBASE_CLIENT_MAX_PERREGION_TASKS);
    this.maxHeapSizePerRequest = conf.getLong(HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE,
          DEFAULT_HBASE_CLIENT_MAX_PERREQUEST_HEAPSIZE);
    this.maxHeapSizeSubmit = conf.getLong(HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE, DEFAULT_HBASE_CLIENT_MAX_SUBMIT_HEAPSIZE);
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
    if (this.maxHeapSizePerRequest <= 0) {
      throw new IllegalArgumentException("maxHeapSizePerServer=" +
          maxHeapSizePerRequest);
    }

    if (this.maxHeapSizeSubmit <= 0) {
      throw new IllegalArgumentException("maxHeapSizeSubmit=" +
          maxHeapSizeSubmit);
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
    this.logBatchErrorDetails = conf.getBoolean(LOG_DETAILS_FOR_BATCH_ERROR, false);

    this.thresholdToLogUndoneTaskDetails =
        conf.getInt(THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS,
          DEFAULT_THRESHOLD_TO_LOG_UNDONE_TASK_DETAILS);
  }

  /**
   * @return pool if non null, otherwise returns this.pool if non null, otherwise throws
   *         RuntimeException
   */
  protected ExecutorService getPool(ExecutorService pool) {
    if (pool != null) {
      return pool;
    }
    if (this.pool != null) {
      return this.pool;
    }
    throw new RuntimeException("Neither AsyncProcess nor request have ExecutorService");
  }

  /**
   * See #submit(ExecutorService, TableName, RowAccess, boolean, Batch.Callback, boolean).
   * Uses default ExecutorService for this AP (must have been created with one).
   */
  public <CResult> AsyncRequestFuture submit(TableName tableName,
      final RowAccess<? extends Row> rows, boolean atLeastOne, Batch.Callback<CResult> callback,
      boolean needResults) throws InterruptedIOException {
    return submit(null, tableName, rows, atLeastOne, callback, needResults);
  }
  /**
   * See {@link #submit(ExecutorService, TableName, RowAccess, boolean, Batch.Callback, boolean)}.
   * Uses the {@link ListRowAccess} to wrap the {@link List}.
   */
  public <CResult> AsyncRequestFuture submit(ExecutorService pool, TableName tableName,
      List<? extends Row> rows, boolean atLeastOne, Batch.Callback<CResult> callback,
      boolean needResults) throws InterruptedIOException {
    return submit(pool, tableName, new ListRowAccess(rows), atLeastOne,
      callback, needResults);
  }

  /**
   * Extract from the rows list what we can submit. The rows we can not submit are kept in the
   * list. Does not send requests to replicas (not currently used for anything other
   * than streaming puts anyway).
   *
   * @param pool ExecutorService to use.
   * @param tableName The table for which this request is needed.
   * @param callback Batch callback. Only called on success (94 behavior).
   * @param needResults Whether results are needed, or can be discarded.
   * @param rows - the submitted row. Modified by the method: we remove the rows we took.
   * @param atLeastOne true if we should submit at least a subset.
   */
  public <CResult> AsyncRequestFuture submit(ExecutorService pool, TableName tableName,
      RowAccess<? extends Row> rows, boolean atLeastOne, Batch.Callback<CResult> callback,
      boolean needResults) throws InterruptedIOException {
    if (rows.isEmpty()) {
      return NO_REQS_RESULT;
    }

    Map<ServerName, MultiAction<Row>> actionsByServer =
        new HashMap<ServerName, MultiAction<Row>>();
    List<Action<Row>> retainedActions = new ArrayList<Action<Row>>(rows.size());

    NonceGenerator ng = this.connection.getNonceGenerator();
    long nonceGroup = ng.getNonceGroup(); // Currently, nonce group is per entire client.

    // Location errors that happen before we decide what requests to take.
    List<Exception> locationErrors = null;
    List<Integer> locationErrorRows = null;
    RowCheckerHost checker = createRowCheckerHost();
    boolean firstIter = true;
    do {
      // Wait until there is at least one slot for a new task.
      waitForMaximumCurrentTasks(maxTotalConcurrentTasks - 1, tableName.getNameAsString());
      int posInList = -1;
      if (!firstIter) {
        checker.reset();
      }
      Iterator<? extends Row> it = rows.iterator();
      while (it.hasNext()) {
        Row r = it.next();
        HRegionLocation loc;
        try {
          if (r == null) {
            throw new IllegalArgumentException("#" + id + ", row cannot be null");
          }
          // Make sure we get 0-s replica.
          RegionLocations locs = connection.locateRegion(
              tableName, r.getRow(), true, true, RegionReplicaUtil.DEFAULT_REPLICA_ID);
          if (locs == null || locs.isEmpty() || locs.getDefaultRegionLocation() == null) {
            throw new IOException("#" + id + ", no location found, aborting submit for"
                + " tableName=" + tableName + " rowkey=" + Bytes.toStringBinary(r.getRow()));
          }
          loc = locs.getDefaultRegionLocation();
        } catch (IOException ex) {
          locationErrors = new ArrayList<Exception>();
          locationErrorRows = new ArrayList<Integer>();
          LOG.error("Failed to get region location ", ex);
          // This action failed before creating ars. Retain it, but do not add to submit list.
          // We will then add it to ars in an already-failed state.
          retainedActions.add(new Action<Row>(r, ++posInList));
          locationErrors.add(ex);
          locationErrorRows.add(posInList);
          it.remove();
          break; // Backward compat: we stop considering actions on location error.
        }
        long rowSize = (r instanceof Mutation) ? ((Mutation) r).heapSize() : 0;
        ReturnCode code = checker.canTakeOperation(loc, rowSize);
        if (code == ReturnCode.END) {
          break;
        }
        if (code == ReturnCode.INCLUDE) {
          Action<Row> action = new Action<Row>(r, ++posInList);
          setNonce(ng, r, action);
          retainedActions.add(action);
          // TODO: replica-get is not supported on this path
          byte[] regionName = loc.getRegionInfo().getRegionName();
          addAction(loc.getServerName(), regionName, action, actionsByServer, nonceGroup);
          it.remove();
        }
      }
      firstIter = false;
    } while (retainedActions.isEmpty() && atLeastOne && (locationErrors == null));

    if (retainedActions.isEmpty()) return NO_REQS_RESULT;

    return submitMultiActions(tableName, retainedActions, nonceGroup, callback, null, needResults,
        locationErrors, locationErrorRows, actionsByServer, pool);
  }

  private RowCheckerHost createRowCheckerHost() {
    return new RowCheckerHost(Arrays.asList(
        new TaskCountChecker(maxTotalConcurrentTasks,
          maxConcurrentTasksPerServer,
          maxConcurrentTasksPerRegion,
          tasksInProgress,
          taskCounterPerServer,
          taskCounterPerRegion)
        , new RequestSizeChecker(maxHeapSizePerRequest)
        , new SubmittedSizeChecker(maxHeapSizeSubmit)
    ));
  }
  <CResult> AsyncRequestFuture submitMultiActions(TableName tableName,
      List<Action<Row>> retainedActions, long nonceGroup, Batch.Callback<CResult> callback,
      Object[] results, boolean needResults, List<Exception> locationErrors,
      List<Integer> locationErrorRows, Map<ServerName, MultiAction<Row>> actionsByServer,
      ExecutorService pool) {
    AsyncRequestFutureImpl<CResult> ars = createAsyncRequestFuture(
      tableName, retainedActions, nonceGroup, pool, callback, results, needResults, null, timeout);
    // Add location errors if any
    if (locationErrors != null) {
      for (int i = 0; i < locationErrors.size(); ++i) {
        int originalIndex = locationErrorRows.get(i);
        Row row = retainedActions.get(originalIndex).getAction();
        ars.manageError(originalIndex, row,
            AsyncRequestFutureImpl.Retry.NO_LOCATION_PROBLEM, locationErrors.get(i), null);
      }
    }
    ars.sendMultiAction(actionsByServer, 1, null, false);
    return ars;
  }

  /**
   * Helper that is used when grouping the actions per region server.
   *
   * @param server - server
   * @param regionName - regionName
   * @param action - the action to add to the multiaction
   * @param actionsByServer the multiaction per server
   * @param nonceGroup Nonce group.
   */
  static void addAction(ServerName server, byte[] regionName, Action<Row> action,
      Map<ServerName, MultiAction<Row>> actionsByServer, long nonceGroup) {
    MultiAction<Row> multiAction = actionsByServer.get(server);
    if (multiAction == null) {
      multiAction = new MultiAction<Row>();
      actionsByServer.put(server, multiAction);
    }
    if (action.hasNonce() && !multiAction.hasNonceGroup()) {
      multiAction.setNonceGroup(nonceGroup);
    }

    multiAction.add(regionName, action);
  }

  public <CResult> AsyncRequestFuture submitAll(ExecutorService pool, TableName tableName,
      List<? extends Row> rows, Batch.Callback<CResult> callback, Object[] results) {
    return submitAll(pool, tableName, rows, callback, results, null, timeout);
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
      List<? extends Row> rows, Batch.Callback<CResult> callback, Object[] results,
      CancellableRegionServerCallable callable, int curTimeout) {
    List<Action<Row>> actions = new ArrayList<Action<Row>>(rows.size());

    // The position will be used by the processBatch to match the object array returned.
    int posInList = -1;
    NonceGenerator ng = this.connection.getNonceGenerator();
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
        tableName, actions, ng.getNonceGroup(), getPool(pool), callback, results, results != null,
        callable, curTimeout);
    ars.groupAndSendMultiAction(actions, 1);
    return ars;
  }

  private void setNonce(NonceGenerator ng, Row r, Action<Row> action) {
    if (!(r instanceof Append) && !(r instanceof Increment)) return;
    action.setNonce(ng.newNonce()); // Action handles NO_NONCE, so it's ok if ng is disabled.
  }

  protected <CResult> AsyncRequestFutureImpl<CResult> createAsyncRequestFuture(
      TableName tableName, List<Action<Row>> actions, long nonceGroup, ExecutorService pool,
      Batch.Callback<CResult> callback, Object[] results, boolean needResults,
      CancellableRegionServerCallable callable, int curTimeout) {
    return new AsyncRequestFutureImpl<CResult>(
        tableName, actions, nonceGroup, getPool(pool), needResults,
        results, callback, callable, curTimeout, this);
  }

  /** Wait until the async does not have more than max tasks in progress. */
  protected void waitForMaximumCurrentTasks(int max, String tableName)
      throws InterruptedIOException {
    waitForMaximumCurrentTasks(max, tasksInProgress, id, tableName);
  }

  // Break out this method so testable
  @VisibleForTesting
  void waitForMaximumCurrentTasks(int max, final AtomicLong tasksInProgress, final long id,
      String tableName) throws InterruptedIOException {
    long lastLog = EnvironmentEdgeManager.currentTime();
    long currentInProgress, oldInProgress = Long.MAX_VALUE;
    while ((currentInProgress = tasksInProgress.get()) > max) {
      if (oldInProgress != currentInProgress) { // Wait for in progress to change.
        long now = EnvironmentEdgeManager.currentTime();
        if (now > lastLog + 10000) {
          lastLog = now;
          LOG.info("#" + id + ", waiting for some tasks to finish. Expected max="
              + max + ", tasksInProgress=" + currentInProgress +
              " hasError=" + hasError() + tableName == null ? "" : ", tableName=" + tableName);
          if (currentInProgress <= thresholdToLogUndoneTaskDetails) {
            logDetailsOfUndoneTasks(currentInProgress);
          }
        }
      }
      oldInProgress = currentInProgress;
      try {
        synchronized (tasksInProgress) {
          if (tasksInProgress.get() == oldInProgress) {
            tasksInProgress.wait(10);
          }
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException("#" + id + ", interrupted." +
            " currentNumberOfTask=" + currentInProgress);
      }
    }
  }

  void logDetailsOfUndoneTasks(long taskInProgress) {
    ArrayList<ServerName> servers = new ArrayList<ServerName>();
    for (Map.Entry<ServerName, AtomicInteger> entry : taskCounterPerServer.entrySet()) {
      if (entry.getValue().get() > 0) {
        servers.add(entry.getKey());
      }
    }
    LOG.info("Left over " + taskInProgress + " task(s) are processed on server(s): " + servers);
    if (taskInProgress <= THRESHOLD_TO_LOG_REGION_DETAILS) {
      ArrayList<String> regions = new ArrayList<String>();
      for (Map.Entry<byte[], AtomicInteger> entry : taskCounterPerRegion.entrySet()) {
        if (entry.getValue().get() > 0) {
          regions.add(Bytes.toString(entry.getKey()));
        }
      }
      LOG.info("Regions against which left over task(s) are processed: " + regions);
    }
  }

  /**
   * Only used w/useGlobalErrors ctor argument, for HTable backward compat.
   * @return Whether there were any errors in any request since the last time
   *          {@link #waitForAllPreviousOpsAndReset(List, String)} was called, or AP was created.
   */
  public boolean hasError() {
    return globalErrors.hasErrors();
  }

  /**
   * Only used w/useGlobalErrors ctor argument, for HTable backward compat.
   * Waits for all previous operations to finish, and returns errors and (optionally)
   * failed operations themselves.
   * @param failedRows an optional list into which the rows that failed since the last time
   *        {@link #waitForAllPreviousOpsAndReset(List, String)} was called, or AP was created, are saved.
   * @param tableName name of the table
   * @return all the errors since the last time {@link #waitForAllPreviousOpsAndReset(List, String)}
   *          was called, or AP was created.
   */
  public RetriesExhaustedWithDetailsException waitForAllPreviousOpsAndReset(
      List<Row> failedRows, String tableName) throws InterruptedIOException {
    waitForMaximumCurrentTasks(0, tableName);
    if (!globalErrors.hasErrors()) {
      return null;
    }
    if (failedRows != null) {
      failedRows.addAll(globalErrors.actions);
    }
    RetriesExhaustedWithDetailsException result = globalErrors.makeException(logBatchErrorDetails);
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
   * Create a caller. Isolated to be easily overridden in the tests.
   */
  @VisibleForTesting
  protected RpcRetryingCaller<AbstractResponse> createCaller(
      CancellableRegionServerCallable callable) {
    return rpcCallerFactory.<AbstractResponse> newCaller();
  }


  /**
   * Creates the server error tracker to use inside process.
   * Currently, to preserve the main assumption about current retries, and to work well with
   * the retry-limit-based calculation, the calculation is local per Process object.
   * We may benefit from connection-wide tracking of server errors.
   * @return ServerErrorTracker to use, null if there is no ServerErrorTracker on this connection
   */
  protected ConnectionImplementation.ServerErrorTracker createServerErrorTracker() {
    return new ConnectionImplementation.ServerErrorTracker(
        this.serverTrackerTimeout, this.numTries);
  }

  static boolean isReplicaGet(Row row) {
    return (row instanceof Get) && (((Get)row).getConsistency() == Consistency.TIMELINE);
  }

  /**
   * Collect all advices from checkers and make the final decision.
   */
  @VisibleForTesting
  static class RowCheckerHost {
    private final List<RowChecker> checkers;
    private boolean isEnd = false;
    RowCheckerHost(final List<RowChecker> checkers) {
      this.checkers = checkers;
    }
    void reset() throws InterruptedIOException {
      isEnd = false;
      InterruptedIOException e = null;
      for (RowChecker checker : checkers) {
        try {
          checker.reset();
        } catch (InterruptedIOException ex) {
          e = ex;
        }
      }
      if (e != null) {
        throw e;
      }
    }
    ReturnCode canTakeOperation(HRegionLocation loc, long rowSize) {
      if (isEnd) {
        return ReturnCode.END;
      }
      ReturnCode code = ReturnCode.INCLUDE;
      for (RowChecker checker : checkers) {
        switch (checker.canTakeOperation(loc, rowSize)) {
          case END:
            isEnd = true;
            code = ReturnCode.END;
            break;
          case SKIP:
            code = ReturnCode.SKIP;
            break;
          case INCLUDE:
          default:
            break;
        }
        if (code == ReturnCode.END) {
          break;
        }
      }
      for (RowChecker checker : checkers) {
        checker.notifyFinal(code, loc, rowSize);
      }
      return code;
    }
  }

  /**
   * Provide a way to control the flow of rows iteration.
   */
  // Visible for Testing. Adding @VisibleForTesting here doesn't work for some reason.
  interface RowChecker {
    enum ReturnCode {
      /**
       * Accept current row.
       */
      INCLUDE,
      /**
       * Skip current row.
       */
      SKIP,
      /**
       * No more row can be included.
       */
      END
    };
    ReturnCode canTakeOperation(HRegionLocation loc, long rowSize);
    /**
     * Add the final ReturnCode to the checker.
     * The ReturnCode may be reversed, so the checker need the final decision to update
     * the inner state.
     */
    void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize);
    /**
     * Reset the inner state.
     */
    void reset() throws InterruptedIOException ;
  }

  /**
   * limit the heapsize of total submitted data.
   * Reduce the limit of heapsize for submitting quickly
   * if there is no running task.
   */
  @VisibleForTesting
  static class SubmittedSizeChecker implements RowChecker {
    private final long maxHeapSizeSubmit;
    private long heapSize = 0;
    SubmittedSizeChecker(final long maxHeapSizeSubmit) {
      this.maxHeapSizeSubmit = maxHeapSizeSubmit;
    }
    @Override
    public ReturnCode canTakeOperation(HRegionLocation loc, long rowSize) {
      if (heapSize >= maxHeapSizeSubmit) {
        return ReturnCode.END;
      }
      return ReturnCode.INCLUDE;
    }

    @Override
    public void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize) {
      if (code == ReturnCode.INCLUDE) {
        heapSize += rowSize;
      }
    }

    @Override
    public void reset() {
      heapSize = 0;
    }
  }
  /**
   * limit the max number of tasks in an AsyncProcess.
   */
  @VisibleForTesting
  static class TaskCountChecker implements RowChecker {
    private static final long MAX_WAITING_TIME = 1000; //ms
    private final Set<HRegionInfo> regionsIncluded = new HashSet<>();
    private final Set<ServerName> serversIncluded = new HashSet<>();
    private final int maxConcurrentTasksPerRegion;
    private final int maxTotalConcurrentTasks;
    private final int maxConcurrentTasksPerServer;
    private final Map<byte[], AtomicInteger> taskCounterPerRegion;
    private final Map<ServerName, AtomicInteger> taskCounterPerServer;
    private final Set<byte[]> busyRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    private final AtomicLong tasksInProgress;
    TaskCountChecker(final int maxTotalConcurrentTasks,
      final int maxConcurrentTasksPerServer,
      final int maxConcurrentTasksPerRegion,
      final AtomicLong tasksInProgress,
      final Map<ServerName, AtomicInteger> taskCounterPerServer,
      final Map<byte[], AtomicInteger> taskCounterPerRegion) {
      this.maxTotalConcurrentTasks = maxTotalConcurrentTasks;
      this.maxConcurrentTasksPerRegion = maxConcurrentTasksPerRegion;
      this.maxConcurrentTasksPerServer = maxConcurrentTasksPerServer;
      this.taskCounterPerRegion = taskCounterPerRegion;
      this.taskCounterPerServer = taskCounterPerServer;
      this.tasksInProgress = tasksInProgress;
    }
    @Override
    public void reset() throws InterruptedIOException {
      // prevent the busy-waiting
      waitForRegion();
      regionsIncluded.clear();
      serversIncluded.clear();
      busyRegions.clear();
    }
    private void waitForRegion() throws InterruptedIOException {
      if (busyRegions.isEmpty()) {
        return;
      }
      EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
      final long start = ee.currentTime();
      while ((ee.currentTime() - start) <= MAX_WAITING_TIME) {
        for (byte[] region : busyRegions) {
          AtomicInteger count = taskCounterPerRegion.get(region);
          if (count == null || count.get() < maxConcurrentTasksPerRegion) {
            return;
          }
        }
        try {
          synchronized (tasksInProgress) {
            tasksInProgress.wait(10);
          }
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted." +
              " tasksInProgress=" + tasksInProgress);
        }
      }
    }
    /**
     * 1) check the regions is allowed.
     * 2) check the concurrent tasks for regions.
     * 3) check the total concurrent tasks.
     * 4) check the concurrent tasks for server.
     * @param loc
     * @param rowSize
     * @return
     */
    @Override
    public ReturnCode canTakeOperation(HRegionLocation loc, long rowSize) {

      HRegionInfo regionInfo = loc.getRegionInfo();
      if (regionsIncluded.contains(regionInfo)) {
        // We already know what to do with this region.
        return ReturnCode.INCLUDE;
      }
      AtomicInteger regionCnt = taskCounterPerRegion.get(loc.getRegionInfo().getRegionName());
      if (regionCnt != null && regionCnt.get() >= maxConcurrentTasksPerRegion) {
        // Too many tasks on this region already.
        return ReturnCode.SKIP;
      }
      int newServers = serversIncluded.size()
        + (serversIncluded.contains(loc.getServerName()) ? 0 : 1);
      if ((newServers + tasksInProgress.get()) > maxTotalConcurrentTasks) {
        // Too many tasks.
        return ReturnCode.SKIP;
      }
      AtomicInteger serverCnt = taskCounterPerServer.get(loc.getServerName());
      if (serverCnt != null && serverCnt.get() >= maxConcurrentTasksPerServer) {
        // Too many tasks for this individual server
        return ReturnCode.SKIP;
      }
      return ReturnCode.INCLUDE;
    }

    @Override
    public void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize) {
      if (code == ReturnCode.INCLUDE) {
        regionsIncluded.add(loc.getRegionInfo());
        serversIncluded.add(loc.getServerName());
      }
      busyRegions.add(loc.getRegionInfo().getRegionName());
    }
  }

  /**
   * limit the request size for each regionserver.
   */
  @VisibleForTesting
  static class RequestSizeChecker implements RowChecker {
    private final long maxHeapSizePerRequest;
    private final Map<ServerName, Long> serverRequestSizes = new HashMap<>();
    RequestSizeChecker(final long maxHeapSizePerRequest) {
      this.maxHeapSizePerRequest = maxHeapSizePerRequest;
    }
    @Override
    public void reset() {
      serverRequestSizes.clear();
    }
    @Override
    public ReturnCode canTakeOperation(HRegionLocation loc, long rowSize) {
      // Is it ok for limit of request size?
      long currentRequestSize = serverRequestSizes.containsKey(loc.getServerName()) ?
        serverRequestSizes.get(loc.getServerName()) : 0L;
      // accept at least one request
      if (currentRequestSize == 0 || currentRequestSize + rowSize <= maxHeapSizePerRequest) {
        return ReturnCode.INCLUDE;
      }
      return ReturnCode.SKIP;
    }

    @Override
    public void notifyFinal(ReturnCode code, HRegionLocation loc, long rowSize) {
      if (code == ReturnCode.INCLUDE) {
        long currentRequestSize = serverRequestSizes.containsKey(loc.getServerName()) ?
          serverRequestSizes.get(loc.getServerName()) : 0L;
        serverRequestSizes.put(loc.getServerName(), currentRequestSize + rowSize);
      }
    }
  }

  public static class ListRowAccess<T> implements RowAccess<T> {
    private final List<T> data;
    ListRowAccess(final List<T> data) {
      this.data = data;
    }

    @Override
    public int size() {
      return data.size();
    }

    @Override
    public boolean isEmpty() {
      return data.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
      return data.iterator();
    }
  }
}
