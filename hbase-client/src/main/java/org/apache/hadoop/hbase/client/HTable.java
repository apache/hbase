/**
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

// DO NOT MAKE USE OF THESE IMPORTS! THEY ARE HERE FOR COPROCESSOR ENDPOINTS ONLY.
// Internally, we use shaded protobuf. This below are part of our public API.
//SEE ABOVE NOTE!
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hbase.client.ConnectionUtils.checkHasFamilies;

/**
 * An implementation of {@link Table}. Used to communicate with a single HBase table.
 * Lightweight. Get as needed and just close when done.
 * Instances of this class SHOULD NOT be constructed directly.
 * Obtain an instance via {@link Connection}. See {@link ConnectionFactory}
 * class comment for an example of how.
 *
 * <p>This class is thread safe since 2.0.0 if not invoking any of the setter methods.
 * All setters are moved into {@link TableBuilder} and reserved here only for keeping
 * backward compatibility, and TODO will be removed soon.
 *
 * <p>HTable is no longer a client API. Use {@link Table} instead. It is marked
 * InterfaceAudience.Private indicating that this is an HBase-internal class as defined in
 * <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/InterfaceClassification.html">Hadoop
 * Interface Classification</a>
 * There are no guarantees for backwards source / binary compatibility and methods or class can
 * change or go away without deprecation.
 *
 * @see Table
 * @see Admin
 * @see Connection
 * @see ConnectionFactory
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class HTable implements Table {
  private static final Logger LOG = LoggerFactory.getLogger(HTable.class);
  private static final Consistency DEFAULT_CONSISTENCY = Consistency.STRONG;
  private final ClusterConnection connection;
  private final TableName tableName;
  private final Configuration configuration;
  private final ConnectionConfiguration connConfiguration;
  private boolean closed = false;
  private final int scannerCaching;
  private final long scannerMaxResultSize;
  private final ExecutorService pool;  // For Multi & Scan
  private int operationTimeoutMs; // global timeout for each blocking method with retrying rpc
  private final int rpcTimeoutMs; // FIXME we should use this for rpc like batch and checkAndXXX
  private int readRpcTimeoutMs; // timeout for each read rpc request
  private int writeRpcTimeoutMs; // timeout for each write rpc request
  private final boolean cleanupPoolOnClose; // shutdown the pool in close()
  private final HRegionLocator locator;

  /** The Async process for batch */
  AsyncProcess multiAp;
  private final RpcRetryingCallerFactory rpcCallerFactory;
  private final RpcControllerFactory rpcControllerFactory;

  // Marked Private @since 1.0
  @InterfaceAudience.Private
  public static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
    int maxThreads = conf.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    int corePoolSize = conf.getInt("hbase.htable.threads.coresize", 1);
    long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in HCM
    // we only create as many Runnables as there are region servers. It means
    // it also scales when new region servers are added.
    ThreadPoolExecutor pool =
      new ThreadPoolExecutor(corePoolSize, maxThreads, keepAliveTime, TimeUnit.SECONDS,
        new SynchronousQueue<>(), new ThreadFactoryBuilder().setNameFormat("htable-pool-%d")
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Creates an object to access a HBase table.
   * Used by HBase internally.  DO NOT USE. See {@link ConnectionFactory} class comment for how to
   * get a {@link Table} instance (use {@link Table} instead of {@link HTable}).
   * @param connection Connection to be used.
   * @param builder The table builder
   * @param rpcCallerFactory The RPC caller factory
   * @param rpcControllerFactory The RPC controller factory
   * @param pool ExecutorService to be used.
   */
  @InterfaceAudience.Private
  protected HTable(final ConnectionImplementation connection,
      final TableBuilderBase builder,
      final RpcRetryingCallerFactory rpcCallerFactory,
      final RpcControllerFactory rpcControllerFactory,
      final ExecutorService pool) {
    this.connection = Preconditions.checkNotNull(connection, "connection is null");
    this.configuration = connection.getConfiguration();
    this.connConfiguration = connection.getConnectionConfiguration();
    if (pool == null) {
      this.pool = getDefaultExecutor(this.configuration);
      this.cleanupPoolOnClose = true;
    } else {
      this.pool = pool;
      this.cleanupPoolOnClose = false;
    }
    if (rpcCallerFactory == null) {
      this.rpcCallerFactory = connection.getNewRpcRetryingCallerFactory(configuration);
    } else {
      this.rpcCallerFactory = rpcCallerFactory;
    }

    if (rpcControllerFactory == null) {
      this.rpcControllerFactory = RpcControllerFactory.instantiate(configuration);
    } else {
      this.rpcControllerFactory = rpcControllerFactory;
    }

    this.tableName = builder.tableName;
    this.operationTimeoutMs = builder.operationTimeout;
    this.rpcTimeoutMs = builder.rpcTimeout;
    this.readRpcTimeoutMs = builder.readRpcTimeout;
    this.writeRpcTimeoutMs = builder.writeRpcTimeout;
    this.scannerCaching = connConfiguration.getScannerCaching();
    this.scannerMaxResultSize = connConfiguration.getScannerMaxResultSize();

    // puts need to track errors globally due to how the APIs currently work.
    multiAp = this.connection.getAsyncProcess();
    this.locator = new HRegionLocator(tableName, connection);
  }

  /**
   * @return maxKeyValueSize from configuration.
   */
  public static int getMaxKeyValueSize(Configuration conf) {
    return conf.getInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, -1);
  }

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  /**
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return A Connection instance.
   */
  protected Connection getConnection() {
    return this.connection;
  }

  @Override
  @Deprecated
  public HTableDescriptor getTableDescriptor() throws IOException {
    HTableDescriptor htd = HBaseAdmin.getHTableDescriptor(tableName, connection, rpcCallerFactory,
      rpcControllerFactory, operationTimeoutMs, readRpcTimeoutMs);
    if (htd != null) {
      return new ImmutableHTableDescriptor(htd);
    }
    return null;
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return HBaseAdmin.getTableDescriptor(tableName, connection, rpcCallerFactory,
      rpcControllerFactory, operationTimeoutMs, readRpcTimeoutMs);
  }

  /**
   * Get the corresponding start keys and regions for an arbitrary range of
   * keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range
   * @param includeEndKey true if endRow is inclusive, false if exclusive
   * @return A pair of list of start keys and list of HRegionLocations that
   *         contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(
      final byte[] startKey, final byte[] endKey, final boolean includeEndKey)
      throws IOException {
    return getKeysAndRegionsInRange(startKey, endKey, includeEndKey, false);
  }

  /**
   * Get the corresponding start keys and regions for an arbitrary range of
   * keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range
   * @param includeEndKey true if endRow is inclusive, false if exclusive
   * @param reload true to reload information or false to use cached information
   * @return A pair of list of start keys and list of HRegionLocations that
   *         contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(
      final byte[] startKey, final byte[] endKey, final boolean includeEndKey,
      final boolean reload) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey,HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException(
        "Invalid range: " + Bytes.toStringBinary(startKey) +
        " > " + Bytes.toStringBinary(endKey));
    }
    List<byte[]> keysInRange = new ArrayList<>();
    List<HRegionLocation> regionsInRange = new ArrayList<>();
    byte[] currentKey = startKey;
    do {
      HRegionLocation regionLocation = getRegionLocator().getRegionLocation(currentKey, reload);
      keysInRange.add(currentKey);
      regionsInRange.add(regionLocation);
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
        && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0
            || (includeEndKey && Bytes.compareTo(currentKey, endKey) == 0)));
    return new Pair<>(keysInRange, regionsInRange);
  }

  /**
   * The underlying {@link HTable} must not be closed.
   * {@link Table#getScanner(Scan)} has other usage details.
   */
  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    if (scan.getCaching() <= 0) {
      scan.setCaching(scannerCaching);
    }
    if (scan.getMaxResultSize() <= 0) {
      scan.setMaxResultSize(scannerMaxResultSize);
    }
    if (scan.getMvccReadPoint() > 0) {
      // it is not supposed to be set by user, clear
      scan.resetMvccReadPoint();
    }
    Boolean async = scan.isAsyncPrefetch();
    if (async == null) {
      async = connConfiguration.isClientScannerAsyncPrefetch();
    }

    if (scan.isReversed()) {
      return new ReversedClientScanner(getConfiguration(), scan, getName(),
        this.connection, this.rpcCallerFactory, this.rpcControllerFactory,
        pool, connConfiguration.getReplicaCallTimeoutMicroSecondScan());
    } else {
      if (async) {
        return new ClientAsyncPrefetchScanner(getConfiguration(), scan, getName(), this.connection,
            this.rpcCallerFactory, this.rpcControllerFactory,
            pool, connConfiguration.getReplicaCallTimeoutMicroSecondScan());
      } else {
        return new ClientSimpleScanner(getConfiguration(), scan, getName(), this.connection,
            this.rpcCallerFactory, this.rpcControllerFactory,
            pool, connConfiguration.getReplicaCallTimeoutMicroSecondScan());
      }
    }
  }

  /**
   * The underlying {@link HTable} must not be closed.
   * {@link Table#getScanner(byte[])} has other usage details.
   */
  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * The underlying {@link HTable} must not be closed.
   * {@link Table#getScanner(byte[], byte[])} has other usage details.
   */
  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  @Override
  public Result get(final Get get) throws IOException {
    return get(get, get.isCheckExistenceOnly());
  }

  private Result get(Get get, final boolean checkExistenceOnly) throws IOException {
    // if we are changing settings to the get, clone it.
    if (get.isCheckExistenceOnly() != checkExistenceOnly || get.getConsistency() == null) {
      get = ReflectionUtils.newInstance(get.getClass(), get);
      get.setCheckExistenceOnly(checkExistenceOnly);
      if (get.getConsistency() == null){
        get.setConsistency(DEFAULT_CONSISTENCY);
      }
    }

    if (get.getConsistency() == Consistency.STRONG) {
      final Get configuredGet = get;
      ClientServiceCallable<Result> callable = new ClientServiceCallable<Result>(this.connection, getName(),
          get.getRow(), this.rpcControllerFactory.newController(), get.getPriority()) {
        @Override
        protected Result rpcCall() throws Exception {
          ClientProtos.GetRequest request = RequestConverter.buildGetRequest(
              getLocation().getRegionInfo().getRegionName(), configuredGet);
          ClientProtos.GetResponse response = doGet(request);
          return response == null? null:
            ProtobufUtil.toResult(response.getResult(), getRpcControllerCellScanner());
        }
      };
      return rpcCallerFactory.<Result>newCaller(readRpcTimeoutMs).callWithRetries(callable,
          this.operationTimeoutMs);
    }

    // Call that takes into account the replica
    RpcRetryingCallerWithReadReplicas callable = new RpcRetryingCallerWithReadReplicas(
        rpcControllerFactory, tableName, this.connection, get, pool,
        connConfiguration.getRetriesNumber(), operationTimeoutMs, readRpcTimeoutMs,
        connConfiguration.getPrimaryCallTimeoutMicroSecond());
    return callable.call(operationTimeoutMs);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (gets.size() == 1) {
      return new Result[]{get(gets.get(0))};
    }
    try {
      Object[] r1 = new Object[gets.size()];
      batch((List<? extends Row>)gets, r1, readRpcTimeoutMs);
      // Translate.
      Result [] results = new Result[r1.length];
      int i = 0;
      for (Object obj: r1) {
        // Batch ensures if there is a failure we get an exception instead
        results[i++] = (Result)obj;
      }
      return results;
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }
  }

  @Override
  public void batch(final List<? extends Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    int rpcTimeout = writeRpcTimeoutMs;
    boolean hasRead = false;
    boolean hasWrite = false;
    for (Row action : actions) {
      if (action instanceof Mutation) {
        hasWrite = true;
      } else {
        hasRead = true;
      }
      if (hasRead && hasWrite) {
        break;
      }
    }
    if (hasRead && !hasWrite) {
      rpcTimeout = readRpcTimeoutMs;
    }
    batch(actions, results, rpcTimeout);
  }

  public void batch(final List<? extends Row> actions, final Object[] results, int rpcTimeout)
      throws InterruptedException, IOException {
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
            .setPool(pool)
            .setTableName(tableName)
            .setRowAccess(actions)
            .setResults(results)
            .setRpcTimeout(rpcTimeout)
            .setOperationTimeout(operationTimeoutMs)
            .setSubmittedRows(AsyncProcessTask.SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = multiAp.submit(task);
    ars.waitUntilDone();
    if (ars.hasError()) {
      throw ars.getErrors();
    }
  }

  @Override
  public <R> void batchCallback(
    final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
    throws IOException, InterruptedException {
    doBatchWithCallback(actions, results, callback, connection, pool, tableName);
  }

  public static <R> void doBatchWithCallback(List<? extends Row> actions, Object[] results,
    Callback<R> callback, ClusterConnection connection, ExecutorService pool, TableName tableName)
    throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    int operationTimeout = connection.getConnectionConfiguration().getOperationTimeout();
    int writeTimeout = connection.getConfiguration().getInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY,
        connection.getConfiguration().getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
            HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    AsyncProcessTask<R> task = AsyncProcessTask.newBuilder(callback)
            .setPool(pool)
            .setTableName(tableName)
            .setRowAccess(actions)
            .setResults(results)
            .setOperationTimeout(operationTimeout)
            .setRpcTimeout(writeTimeout)
            .setSubmittedRows(AsyncProcessTask.SubmittedRows.ALL)
            .build();
    AsyncRequestFuture ars = connection.getAsyncProcess().submit(task);
    ars.waitUntilDone();
    if (ars.hasError()) {
      throw ars.getErrors();
    }
  }

  @Override
  public void delete(final Delete delete) throws IOException {
    ClientServiceCallable<Void> callable =
        new ClientServiceCallable<Void>(this.connection, getName(), delete.getRow(),
            this.rpcControllerFactory.newController(), delete.getPriority()) {
      @Override
      protected Void rpcCall() throws Exception {
        MutateRequest request = RequestConverter
            .buildMutateRequest(getLocation().getRegionInfo().getRegionName(), delete);
        doMutate(request);
        return null;
      }
    };
    rpcCallerFactory.<Void>newCaller(this.writeRpcTimeoutMs)
        .callWithRetries(callable, this.operationTimeoutMs);
  }

  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    Object[] results = new Object[deletes.size()];
    try {
      batch(deletes, results, writeRpcTimeoutMs);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    } finally {
      // TODO: to be consistent with batch put(), do not modify input list
      // mutate list so that it is empty for complete success, or contains only failed records
      // results are returned in the same order as the requests in list walk the list backwards,
      // so we can remove from list without impacting the indexes of earlier members
      for (int i = results.length - 1; i>=0; i--) {
        // if result is not null, it succeeded
        if (results[i] instanceof Result) {
          deletes.remove(i);
        }
      }
    }
  }

  @Override
  public void put(final Put put) throws IOException {
    validatePut(put);
    ClientServiceCallable<Void> callable =
        new ClientServiceCallable<Void>(this.connection, getName(), put.getRow(),
            this.rpcControllerFactory.newController(), put.getPriority()) {
      @Override
      protected Void rpcCall() throws Exception {
        MutateRequest request =
            RequestConverter.buildMutateRequest(getLocation().getRegionInfo().getRegionName(), put);
        doMutate(request);
        return null;
      }
    };
    rpcCallerFactory.<Void> newCaller(this.writeRpcTimeoutMs).callWithRetries(callable,
        this.operationTimeoutMs);
  }

  @Override
  public void put(final List<Put> puts) throws IOException {
    for (Put put : puts) {
      validatePut(put);
    }
    Object[] results = new Object[puts.size()];
    try {
      batch(puts, results, writeRpcTimeoutMs);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
  }

  @Override
  public Result mutateRow(final RowMutations rm) throws IOException {
    long nonceGroup = getNonceGroup();
    long nonce = getNonce();
    CancellableRegionServerCallable<MultiResponse> callable =
      new CancellableRegionServerCallable<MultiResponse>(this.connection, getName(), rm.getRow(),
          rpcControllerFactory.newController(), writeRpcTimeoutMs,
          new RetryingTimeTracker().start(), rm.getMaxPriority()) {
      @Override
      protected MultiResponse rpcCall() throws Exception {
        MultiRequest request = RequestConverter.buildMultiRequest(
          getLocation().getRegionInfo().getRegionName(), rm, nonceGroup, nonce);
        ClientProtos.MultiResponse response = doMulti(request);
        ClientProtos.RegionActionResult res = response.getRegionActionResultList().get(0);
        if (res.hasException()) {
          Throwable ex = ProtobufUtil.toException(res.getException());
          if (ex instanceof IOException) {
            throw (IOException) ex;
          }
          throw new IOException("Failed to mutate row: " + Bytes.toStringBinary(rm.getRow()), ex);
        }
        return ResponseConverter.getResults(request, response, getRpcControllerCellScanner());
      }
    };
    Object[] results = new Object[rm.getMutations().size()];
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
      .setPool(pool)
      .setTableName(tableName)
      .setRowAccess(rm.getMutations())
      .setCallable(callable)
      .setRpcTimeout(writeRpcTimeoutMs)
      .setOperationTimeout(operationTimeoutMs)
      .setSubmittedRows(AsyncProcessTask.SubmittedRows.ALL)
      .setResults(results)
      .build();
    AsyncRequestFuture ars = multiAp.submit(task);
    ars.waitUntilDone();
    if (ars.hasError()) {
      throw ars.getErrors();
    }
    return (Result) results[0];
  }

  private long getNonceGroup() {
    return ((ClusterConnection) getConnection()).getNonceGenerator().getNonceGroup();
  }

  private long getNonce() {
    return ((ClusterConnection) getConnection()).getNonceGenerator().newNonce();
  }

  @Override
  public Result append(final Append append) throws IOException {
    checkHasFamilies(append);
    NoncedRegionServerCallable<Result> callable =
        new NoncedRegionServerCallable<Result>(this.connection, getName(), append.getRow(),
            this.rpcControllerFactory.newController(), append.getPriority()) {
      @Override
      protected Result rpcCall() throws Exception {
        MutateRequest request = RequestConverter.buildMutateRequest(
          getLocation().getRegionInfo().getRegionName(), append, super.getNonceGroup(),
          super.getNonce());
        MutateResponse response = doMutate(request);
        if (!response.hasResult()) return null;
        return ProtobufUtil.toResult(response.getResult(), getRpcControllerCellScanner());
      }
    };
    return rpcCallerFactory.<Result> newCaller(this.writeRpcTimeoutMs).
        callWithRetries(callable, this.operationTimeoutMs);
  }

  @Override
  public Result increment(final Increment increment) throws IOException {
    checkHasFamilies(increment);
    NoncedRegionServerCallable<Result> callable =
        new NoncedRegionServerCallable<Result>(this.connection, getName(), increment.getRow(),
            this.rpcControllerFactory.newController(), increment.getPriority()) {
      @Override
      protected Result rpcCall() throws Exception {
        MutateRequest request = RequestConverter.buildMutateRequest(
          getLocation().getRegionInfo().getRegionName(), increment, super.getNonceGroup(),
          super.getNonce());
        MutateResponse response = doMutate(request);
        // Should this check for null like append does?
        return ProtobufUtil.toResult(response.getResult(), getRpcControllerCellScanner());
      }
    };
    return rpcCallerFactory.<Result> newCaller(writeRpcTimeoutMs).callWithRetries(callable,
        this.operationTimeoutMs);
  }

  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, Durability.SYNC_WAL);
  }

  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final Durability durability)
  throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("family is null");
    }
    if (npe != null) {
      throw new IOException(
          "Invalid arguments to incrementColumnValue", npe);
    }

    NoncedRegionServerCallable<Long> callable =
        new NoncedRegionServerCallable<Long>(this.connection, getName(), row,
            this.rpcControllerFactory.newController(), HConstants.PRIORITY_UNSET) {
      @Override
      protected Long rpcCall() throws Exception {
        MutateRequest request = RequestConverter.buildIncrementRequest(
          getLocation().getRegionInfo().getRegionName(), row, family,
          qualifier, amount, durability, super.getNonceGroup(), super.getNonce());
        MutateResponse response = doMutate(request);
        Result result = ProtobufUtil.toResult(response.getResult(), getRpcControllerCellScanner());
        return Long.valueOf(Bytes.toLong(result.getValue(family, qualifier)));
      }
    };
    return rpcCallerFactory.<Long> newCaller(this.writeRpcTimeoutMs).
        callWithRetries(callable, this.operationTimeoutMs);
  }

  @Override
  @Deprecated
  public boolean checkAndPut(final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put) throws IOException {
    return doCheckAndMutate(row, family, qualifier, CompareOperator.EQUAL, value, null, null,
      put).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndPut(final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final byte [] value, final Put put) throws IOException {
    return doCheckAndMutate(row, family, qualifier, toCompareOperator(compareOp), value, null,
      null, put).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndPut(final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOperator op, final byte [] value, final Put put) throws IOException {
    // The name of the operators in CompareOperator are intentionally those of the
    // operators in the filter's CompareOp enum.
    return doCheckAndMutate(row, family, qualifier, op, value, null, null, put).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier,
    final byte[] value, final Delete delete) throws IOException {
    return doCheckAndMutate(row, family, qualifier, CompareOperator.EQUAL, value, null,
      null, delete).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier,
    final CompareOp compareOp, final byte[] value, final Delete delete) throws IOException {
    return doCheckAndMutate(row, family, qualifier, toCompareOperator(compareOp), value, null,
      null, delete).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier,
    final CompareOperator op, final byte[] value, final Delete delete) throws IOException {
    return doCheckAndMutate(row, family, qualifier, op, value, null, null, delete).isSuccess();
  }

  @Override
  @Deprecated
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilderImpl(row, family);
  }

  @Override
  @Deprecated
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    return new CheckAndMutateWithFilterBuilderImpl(row, filter);
  }

  private CheckAndMutateResult doCheckAndMutate(final byte[] row, final byte[] family,
    final byte[] qualifier, final CompareOperator op, final byte[] value, final Filter filter,
    final TimeRange timeRange, final RowMutations rm) throws IOException {
    long nonceGroup = getNonceGroup();
    long nonce = getNonce();
    CancellableRegionServerCallable<MultiResponse> callable =
    new CancellableRegionServerCallable<MultiResponse>(connection, getName(), rm.getRow(),
    rpcControllerFactory.newController(), writeRpcTimeoutMs, new RetryingTimeTracker().start(),
        rm.getMaxPriority()) {
      @Override
      protected MultiResponse rpcCall() throws Exception {
        MultiRequest request = RequestConverter
          .buildMultiRequest(getLocation().getRegionInfo().getRegionName(), row, family,
            qualifier, op, value, filter, timeRange, rm, nonceGroup, nonce);
        ClientProtos.MultiResponse response = doMulti(request);
        ClientProtos.RegionActionResult res = response.getRegionActionResultList().get(0);
        if (res.hasException()) {
          Throwable ex = ProtobufUtil.toException(res.getException());
          if (ex instanceof IOException) {
            throw (IOException) ex;
          }
          throw new IOException(
            "Failed to checkAndMutate row: " + Bytes.toStringBinary(rm.getRow()), ex);
        }
        return ResponseConverter.getResults(request, response, getRpcControllerCellScanner());
      }
    };

    /**
     *  Currently, we use one array to store 'processed' flag which is returned by server.
     *  It is excessive to send such a large array, but that is required by the framework right now
     * */
    Object[] results = new Object[rm.getMutations().size()];
    AsyncProcessTask task = AsyncProcessTask.newBuilder()
    .setPool(pool)
    .setTableName(tableName)
    .setRowAccess(rm.getMutations())
    .setResults(results)
    .setCallable(callable)
    // TODO any better timeout?
    .setRpcTimeout(Math.max(readRpcTimeoutMs, writeRpcTimeoutMs))
    .setOperationTimeout(operationTimeoutMs)
    .setSubmittedRows(AsyncProcessTask.SubmittedRows.ALL)
    .build();
    AsyncRequestFuture ars = multiAp.submit(task);
    ars.waitUntilDone();
    if (ars.hasError()) {
      throw ars.getErrors();
    }

    return (CheckAndMutateResult) results[0];
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(final byte [] row, final byte [] family, final byte [] qualifier,
    final CompareOp compareOp, final byte [] value, final RowMutations rm)
  throws IOException {
    return doCheckAndMutate(row, family, qualifier, toCompareOperator(compareOp), value, null,
      null, rm).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOperator op, final byte [] value, final RowMutations rm) throws IOException {
    return doCheckAndMutate(row, family, qualifier, op, value, null, null, rm).isSuccess();
  }

  @Override
  public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
    Row action = checkAndMutate.getAction();
    if (action instanceof Put || action instanceof Delete || action instanceof Increment ||
      action instanceof Append) {
      if (action instanceof Put) {
        validatePut((Put) action);
      }
      return doCheckAndMutate(checkAndMutate.getRow(), checkAndMutate.getFamily(),
        checkAndMutate.getQualifier(), checkAndMutate.getCompareOp(), checkAndMutate.getValue(),
        checkAndMutate.getFilter(), checkAndMutate.getTimeRange(), (Mutation) action);
    } else {
      return doCheckAndMutate(checkAndMutate.getRow(), checkAndMutate.getFamily(),
        checkAndMutate.getQualifier(), checkAndMutate.getCompareOp(), checkAndMutate.getValue(),
        checkAndMutate.getFilter(), checkAndMutate.getTimeRange(), (RowMutations) action);
    }
  }

  private CheckAndMutateResult doCheckAndMutate(final byte[] row, final byte[] family,
    final byte[] qualifier, final CompareOperator op, final byte[] value, final Filter filter,
    final TimeRange timeRange, final Mutation mutation) throws IOException {
    long nonceGroup = getNonceGroup();
    long nonce = getNonce();
    ClientServiceCallable<CheckAndMutateResult> callable =
      new ClientServiceCallable<CheckAndMutateResult>(this.connection, getName(), row,
        this.rpcControllerFactory.newController(), mutation.getPriority()) {
        @Override
        protected CheckAndMutateResult rpcCall() throws Exception {
          MutateRequest request = RequestConverter.buildMutateRequest(
            getLocation().getRegionInfo().getRegionName(), row, family, qualifier, op, value,
            filter, timeRange, mutation, nonceGroup, nonce);
          MutateResponse response = doMutate(request);
          if (response.hasResult()) {
            return new CheckAndMutateResult(response.getProcessed(),
              ProtobufUtil.toResult(response.getResult(), getRpcControllerCellScanner()));
          }
          return new CheckAndMutateResult(response.getProcessed(), null);
        }
      };
    return rpcCallerFactory.<CheckAndMutateResult> newCaller(this.writeRpcTimeoutMs)
      .callWithRetries(callable, this.operationTimeoutMs);
  }

  @Override
  public List<CheckAndMutateResult> checkAndMutate(List<CheckAndMutate> checkAndMutates)
    throws IOException {
    if (checkAndMutates.isEmpty()) {
      return Collections.emptyList();
    }
    if (checkAndMutates.size() == 1) {
      return Collections.singletonList(checkAndMutate(checkAndMutates.get(0)));
    }

    Object[] results = new Object[checkAndMutates.size()];
    try {
      batch(checkAndMutates, results, writeRpcTimeoutMs);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }

    // translate.
    List<CheckAndMutateResult> ret = new ArrayList<>(results.length);
    for (Object r : results) {
      // Batch ensures if there is a failure we get an exception instead
      ret.add((CheckAndMutateResult) r);
    }
    return ret;
  }

  private CompareOperator toCompareOperator(CompareOp compareOp) {
    switch (compareOp) {
      case LESS:
        return CompareOperator.LESS;

      case LESS_OR_EQUAL:
        return CompareOperator.LESS_OR_EQUAL;

      case EQUAL:
        return CompareOperator.EQUAL;

      case NOT_EQUAL:
        return CompareOperator.NOT_EQUAL;

      case GREATER_OR_EQUAL:
        return CompareOperator.GREATER_OR_EQUAL;

      case GREATER:
        return CompareOperator.GREATER;

      case NO_OP:
        return CompareOperator.NO_OP;

      default:
        throw new AssertionError();
    }
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    Result r = get(get, true);
    assert r.getExists() != null;
    return r.getExists();
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    if (gets.isEmpty()) return new boolean[]{};
    if (gets.size() == 1) return new boolean[]{exists(gets.get(0))};

    ArrayList<Get> exists = new ArrayList<>(gets.size());
    for (Get g: gets){
      Get ge = new Get(g);
      ge.setCheckExistenceOnly(true);
      exists.add(ge);
    }

    Object[] r1= new Object[exists.size()];
    try {
      batch(exists, r1, readRpcTimeoutMs);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }

    // translate.
    boolean[] results = new boolean[r1.length];
    int i = 0;
    for (Object o : r1) {
      // batch ensures if there is a failure we get an exception instead
      results[i++] = ((Result)o).getExists();
    }

    return results;
  }

  /**
   * Process a mixed batch of Get, Put and Delete actions. All actions for a
   * RegionServer are forwarded in one RPC call. Queries are executed in parallel.
   *
   * @param list The collection of actions.
   * @param results An empty array, same size as list. If an exception is thrown,
   *   you can test here for partial results, and to determine which actions
   *   processed successfully.
   * @throws IOException if there are problems talking to META. Per-item
   *   exceptions are stored in the results array.
   */
  public <R> void processBatchCallback(
    final List<? extends Row> list, final Object[] results, final Batch.Callback<R> callback)
    throws IOException, InterruptedException {
    this.batchCallback(list, results, callback);
  }

  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    if (cleanupPoolOnClose) {
      this.pool.shutdown();
      try {
        boolean terminated = false;
        do {
          // wait until the pool has terminated
          terminated = this.pool.awaitTermination(60, TimeUnit.SECONDS);
        } while (!terminated);
      } catch (InterruptedException e) {
        this.pool.shutdownNow();
        LOG.warn("waitForTermination interrupted");
      }
    }
    this.closed = true;
  }

  // validate for well-formedness
  private void validatePut(final Put put) throws IllegalArgumentException {
    ConnectionUtils.validatePut(put, connConfiguration.getMaxKeyValueSize());
  }

  /**
   * The pool is used for mutli requests for this HTable
   * @return the pool used for mutli
   */
  ExecutorService getPool() {
    return this.pool;
  }

  /**
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public void clearRegionCache() {
    this.connection.clearRegionLocationCache();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return new RegionCoprocessorRpcChannel(connection, tableName, row);
  }

  @Override
  public <T extends Service, R> Map<byte[],R> coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable)
      throws ServiceException, Throwable {
    final Map<byte[],R> results =  Collections.synchronizedMap(
        new TreeMap<byte[], R>(Bytes.BYTES_COMPARATOR));
    coprocessorService(service, startKey, endKey, callable, new Batch.Callback<R>() {
      @Override
      public void update(byte[] region, byte[] row, R value) {
        if (region != null) {
          results.put(region, value);
        }
      }
    });
    return results;
  }

  @Override
  public <T extends Service, R> void coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable,
      final Batch.Callback<R> callback) throws ServiceException, Throwable {
    // get regions covered by the row range
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);
    Map<byte[],Future<R>> futures = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (final byte[] r : keys) {
      final RegionCoprocessorRpcChannel channel =
          new RegionCoprocessorRpcChannel(connection, tableName, r);
      Future<R> future = pool.submit(new Callable<R>() {
        @Override
        public R call() throws Exception {
          T instance =
              org.apache.hadoop.hbase.protobuf.ProtobufUtil.newServiceStub(service, channel);
          R result = callable.call(instance);
          byte[] region = channel.getLastRegion();
          if (callback != null) {
            callback.update(region, r, result);
          }
          return result;
        }
      });
      futures.put(r, future);
    }
    for (Map.Entry<byte[],Future<R>> e : futures.entrySet()) {
      try {
        e.getValue().get();
      } catch (ExecutionException ee) {
        LOG.warn("Error calling coprocessor service " + service.getName() + " for row "
            + Bytes.toStringBinary(e.getKey()), ee);
        throw ee.getCause();
      } catch (InterruptedException ie) {
        throw new InterruptedIOException("Interrupted calling coprocessor service "
            + service.getName() + " for row " + Bytes.toStringBinary(e.getKey())).initCause(ie);
      }
    }
  }

  private List<byte[]> getStartKeysInRange(byte[] start, byte[] end)
  throws IOException {
    if (start == null) {
      start = HConstants.EMPTY_START_ROW;
    }
    if (end == null) {
      end = HConstants.EMPTY_END_ROW;
    }
    return getKeysAndRegionsInRange(start, end, true).getFirst();
  }

  @Override
  public long getRpcTimeout(TimeUnit unit) {
    return unit.convert(rpcTimeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  @Deprecated
  public int getRpcTimeout() {
    return rpcTimeoutMs;
  }

  @Override
  @Deprecated
  public void setRpcTimeout(int rpcTimeout) {
    setReadRpcTimeout(rpcTimeout);
    setWriteRpcTimeout(rpcTimeout);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return unit.convert(readRpcTimeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  @Deprecated
  public int getReadRpcTimeout() {
    return readRpcTimeoutMs;
  }

  @Override
  @Deprecated
  public void setReadRpcTimeout(int readRpcTimeout) {
    this.readRpcTimeoutMs = readRpcTimeout;
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return unit.convert(writeRpcTimeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  @Deprecated
  public int getWriteRpcTimeout() {
    return writeRpcTimeoutMs;
  }

  @Override
  @Deprecated
  public void setWriteRpcTimeout(int writeRpcTimeout) {
    this.writeRpcTimeoutMs = writeRpcTimeout;
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return unit.convert(operationTimeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  @Deprecated
  public int getOperationTimeout() {
    return operationTimeoutMs;
  }

  @Override
  @Deprecated
  public void setOperationTimeout(int operationTimeout) {
    this.operationTimeoutMs = operationTimeout;
  }

  @Override
  public String toString() {
    return tableName + ";" + connection;
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    final Map<byte[], R> results = Collections.synchronizedMap(new TreeMap<byte[], R>(
        Bytes.BYTES_COMPARATOR));
    batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype,
        new Callback<R>() {
      @Override
      public void update(byte[] region, byte[] row, R result) {
        if (region != null) {
          results.put(region, result);
        }
      }
    });
    return results;
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      final Descriptors.MethodDescriptor methodDescriptor, final Message request,
      byte[] startKey, byte[] endKey, final R responsePrototype, final Callback<R> callback)
      throws ServiceException, Throwable {

    if (startKey == null) {
      startKey = HConstants.EMPTY_START_ROW;
    }
    if (endKey == null) {
      endKey = HConstants.EMPTY_END_ROW;
    }
    // get regions covered by the row range
    Pair<List<byte[]>, List<HRegionLocation>> keysAndRegions =
        getKeysAndRegionsInRange(startKey, endKey, true);
    List<byte[]> keys = keysAndRegions.getFirst();
    List<HRegionLocation> regions = keysAndRegions.getSecond();

    // check if we have any calls to make
    if (keys.isEmpty()) {
      LOG.info("No regions were selected by key range start=" + Bytes.toStringBinary(startKey) +
          ", end=" + Bytes.toStringBinary(endKey));
      return;
    }

    List<RegionCoprocessorServiceExec> execs = new ArrayList<>(keys.size());
    final Map<byte[], RegionCoprocessorServiceExec> execsByRow = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < keys.size(); i++) {
      final byte[] rowKey = keys.get(i);
      final byte[] region = regions.get(i).getRegionInfo().getRegionName();
      RegionCoprocessorServiceExec exec =
          new RegionCoprocessorServiceExec(region, rowKey, methodDescriptor, request);
      execs.add(exec);
      execsByRow.put(rowKey, exec);
    }

    // tracking for any possible deserialization errors on success callback
    // TODO: it would be better to be able to reuse AsyncProcess.BatchErrors here
    final List<Throwable> callbackErrorExceptions = new ArrayList<>();
    final List<Row> callbackErrorActions = new ArrayList<>();
    final List<String> callbackErrorServers = new ArrayList<>();
    Object[] results = new Object[execs.size()];

    AsyncProcess asyncProcess = new AsyncProcess(connection, configuration,
        RpcRetryingCallerFactory.instantiate(configuration, connection.getStatisticsTracker()),
        RpcControllerFactory.instantiate(configuration));

    Callback<ClientProtos.CoprocessorServiceResult> resultsCallback
    = (byte[] region, byte[] row, ClientProtos.CoprocessorServiceResult serviceResult) -> {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Received result for endpoint " + methodDescriptor.getFullName() +
            ": region=" + Bytes.toStringBinary(region) +
            ", row=" + Bytes.toStringBinary(row) +
            ", value=" + serviceResult.getValue().getValue());
      }
      try {
        Message.Builder builder = responsePrototype.newBuilderForType();
        org.apache.hadoop.hbase.protobuf.ProtobufUtil.mergeFrom(builder,
            serviceResult.getValue().getValue().toByteArray());
        callback.update(region, row, (R) builder.build());
      } catch (IOException e) {
        LOG.error("Unexpected response type from endpoint " + methodDescriptor.getFullName(),
            e);
        callbackErrorExceptions.add(e);
        callbackErrorActions.add(execsByRow.get(row));
        callbackErrorServers.add("null");
      }
    };
    AsyncProcessTask<ClientProtos.CoprocessorServiceResult> task =
        AsyncProcessTask.newBuilder(resultsCallback)
            .setPool(pool)
            .setTableName(tableName)
            .setRowAccess(execs)
            .setResults(results)
            .setRpcTimeout(readRpcTimeoutMs)
            .setOperationTimeout(operationTimeoutMs)
            .setSubmittedRows(AsyncProcessTask.SubmittedRows.ALL)
            .build();
    AsyncRequestFuture future = asyncProcess.submit(task);
    future.waitUntilDone();

    if (future.hasError()) {
      throw future.getErrors();
    } else if (!callbackErrorExceptions.isEmpty()) {
      throw new RetriesExhaustedWithDetailsException(callbackErrorExceptions, callbackErrorActions,
          callbackErrorServers);
    }
  }

  @Override
  public RegionLocator getRegionLocator() {
    return this.locator;
  }

  private class CheckAndMutateBuilderImpl implements CheckAndMutateBuilder {

    private final byte[] row;
    private final byte[] family;
    private byte[] qualifier;
    private TimeRange timeRange;
    private CompareOperator op;
    private byte[] value;

    CheckAndMutateBuilderImpl(byte[] row, byte[] family) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.family = Preconditions.checkNotNull(family, "family is null");
    }

    @Override
    public CheckAndMutateBuilder qualifier(byte[] qualifier) {
      this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null. Consider using" +
          " an empty byte array, or just do not call this method if you want a null qualifier");
      return this;
    }

    @Override
    public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    @Override
    public CheckAndMutateBuilder ifNotExists() {
      this.op = CompareOperator.EQUAL;
      this.value = null;
      return this;
    }

    @Override
    public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
      this.op = Preconditions.checkNotNull(compareOp, "compareOp is null");
      this.value = Preconditions.checkNotNull(value, "value is null");
      return this;
    }

    private void preCheck() {
      Preconditions.checkNotNull(op, "condition is null. You need to specify the condition by" +
          " calling ifNotExists/ifEquals/ifMatches before executing the request");
    }

    @Override
    public boolean thenPut(Put put) throws IOException {
      validatePut(put);
      preCheck();
      return doCheckAndMutate(row, family, qualifier, op, value, null, timeRange, put)
        .isSuccess();
    }

    @Override
    public boolean thenDelete(Delete delete) throws IOException {
      preCheck();
      return doCheckAndMutate(row, family, qualifier, op, value, null, timeRange, delete)
        .isSuccess();
    }

    @Override
    public boolean thenMutate(RowMutations mutation) throws IOException {
      preCheck();
      return doCheckAndMutate(row, family, qualifier, op, value, null, timeRange,
        mutation).isSuccess();
    }
  }

  private class CheckAndMutateWithFilterBuilderImpl implements CheckAndMutateWithFilterBuilder {

    private final byte[] row;
    private final Filter filter;
    private TimeRange timeRange;

    CheckAndMutateWithFilterBuilderImpl(byte[] row, Filter filter) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.filter = Preconditions.checkNotNull(filter, "filter is null");
    }

    @Override
    public CheckAndMutateWithFilterBuilder timeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    @Override
    public boolean thenPut(Put put) throws IOException {
      validatePut(put);
      return doCheckAndMutate(row, null, null, null, null, filter, timeRange, put).isSuccess();
    }

    @Override
    public boolean thenDelete(Delete delete) throws IOException {
      return doCheckAndMutate(row, null, null, null, null, filter, timeRange, delete).isSuccess();
    }

    @Override
    public boolean thenMutate(RowMutations mutation) throws IOException {
      return doCheckAndMutate(row, null, null, null, null, filter, timeRange, mutation)
        .isSuccess();
    }
  }
}
