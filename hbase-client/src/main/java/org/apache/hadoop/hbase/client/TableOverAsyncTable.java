/**
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

import static org.apache.hadoop.hbase.client.ConnectionUtils.setCoprocessorError;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.client.trace.TableOperationSpanBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcurrentMapUtils.IOExceptionSupplier;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.primitives.Booleans;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * The table implementation based on {@link AsyncTable}.
 */
@InterfaceAudience.Private
class TableOverAsyncTable implements Table {

  private static final Logger LOG = LoggerFactory.getLogger(TableOverAsyncTable.class);

  private final AsyncConnectionImpl conn;

  private final AsyncTable<?> table;

  private final IOExceptionSupplier<ExecutorService> poolSupplier;

  TableOverAsyncTable(AsyncConnectionImpl conn, AsyncTable<?> table,
      IOExceptionSupplier<ExecutorService> poolSupplier) {
    this.conn = conn;
    this.table = table;
    this.poolSupplier = poolSupplier;
  }

  @Override
  public TableName getName() {
    return table.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return FutureUtils.get(conn.getAdmin().getDescriptor(getName()));
  }

  @Override
  public boolean exists(Get get) throws IOException {
    return FutureUtils.get(table.exists(get));
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    return Booleans.toArray(FutureUtils.get(table.existsAll(gets)));
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException {
    if (ArrayUtils.isEmpty(results)) {
      FutureUtils.get(table.batchAll(actions));
      return;
    }
    List<ThrowableWithExtraContext> errors = new ArrayList<>();
    List<CompletableFuture<Object>> futures = table.batch(actions);
    for (int i = 0, n = results.length; i < n; i++) {
      try {
        results[i] = FutureUtils.get(futures.get(i));
      } catch (IOException e) {
        results[i] = e;
        errors.add(new ThrowableWithExtraContext(e, EnvironmentEdgeManager.currentTime(),
          "Error when processing " + actions.get(i)));
      }
    }
    if (!errors.isEmpty()) {
      throw new RetriesExhaustedException(errors.size(), errors);
    }
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    ConcurrentLinkedQueue<ThrowableWithExtraContext> errors = new ConcurrentLinkedQueue<>();
    CountDownLatch latch = new CountDownLatch(actions.size());
    AsyncTableRegionLocator locator = conn.getRegionLocator(getName());
    List<CompletableFuture<R>> futures = table.<R> batch(actions);
    for (int i = 0, n = futures.size(); i < n; i++) {
      final int index = i;
      FutureUtils.addListener(futures.get(i), (r, e) -> {
        if (e != null) {
          errors.add(new ThrowableWithExtraContext(e, EnvironmentEdgeManager.currentTime(),
            "Error when processing " + actions.get(index)));
          if (!ArrayUtils.isEmpty(results)) {
            results[index] = e;
          }
          latch.countDown();
        } else {
          if (!ArrayUtils.isEmpty(results)) {
            results[index] = r;
          }
          FutureUtils.addListener(locator.getRegionLocation(actions.get(index).getRow()),
            (l, le) -> {
              if (le != null) {
                errors.add(new ThrowableWithExtraContext(le, EnvironmentEdgeManager.currentTime(),
                  "Error when finding the region for row " +
                    Bytes.toStringBinary(actions.get(index).getRow())));
              } else {
                callback.update(l.getRegion().getRegionName(), actions.get(index).getRow(), r);
              }
              latch.countDown();
            });
        }
      });
    }
    latch.await();
    if (!errors.isEmpty()) {
      throw new RetriesExhaustedException(errors.size(),
        errors.stream().collect(Collectors.toList()));
    }
  }

  @Override
  public Result get(Get get) throws IOException {
    return FutureUtils.get(table.get(get));
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return FutureUtils.get(table.getAll(gets)).toArray(new Result[0]);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return table.getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return table.getScanner(family);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return table.getScanner(family, qualifier);
  }

  @Override
  public void put(Put put) throws IOException {
    FutureUtils.get(table.put(put));
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    FutureUtils.get(table.putAll(puts));
  }

  @Override
  public void delete(Delete delete) throws IOException {
    FutureUtils.get(table.delete(delete));
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    FutureUtils.get(table.deleteAll(deletes));
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilder() {

      private final AsyncTable.CheckAndMutateBuilder builder = table.checkAndMutate(row, family);

      @Override
      public CheckAndMutateBuilder qualifier(byte[] qualifier) {
        builder.qualifier(qualifier);
        return this;
      }

      @Override
      public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
        builder.timeRange(timeRange);
        return this;
      }

      @Override
      public CheckAndMutateBuilder ifNotExists() {
        builder.ifNotExists();
        return this;
      }

      @Override
      public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
        builder.ifMatches(compareOp, value);
        return this;
      }

      @Override
      public boolean thenPut(Put put) throws IOException {
        return FutureUtils.get(builder.thenPut(put));
      }

      @Override
      public boolean thenDelete(Delete delete) throws IOException {
        return FutureUtils.get(builder.thenDelete(delete));
      }

      @Override
      public boolean thenMutate(RowMutations mutation) throws IOException {
        return FutureUtils.get(builder.thenMutate(mutation));
      }
    };
  }

  @Override
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    return new CheckAndMutateWithFilterBuilder() {
      private final AsyncTable.CheckAndMutateWithFilterBuilder builder =
        table.checkAndMutate(row, filter);

      @Override
      public CheckAndMutateWithFilterBuilder timeRange(TimeRange timeRange) {
        builder.timeRange(timeRange);
        return this;
      }

      @Override
      public boolean thenPut(Put put) throws IOException {
        return FutureUtils.get(builder.thenPut(put));
      }

      @Override
      public boolean thenDelete(Delete delete) throws IOException {
        return FutureUtils.get(builder.thenDelete(delete));
      }

      @Override
      public boolean thenMutate(RowMutations mutation) throws IOException {
        return FutureUtils.get(builder.thenMutate(mutation));
      }
    };
  }

  @Override
  public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
    return FutureUtils.get(table.checkAndMutate(checkAndMutate));
  }

  @Override
  public List<CheckAndMutateResult> checkAndMutate(List<CheckAndMutate> checkAndMutates)
    throws IOException {
    return FutureUtils.get(table.checkAndMutateAll(checkAndMutates));
  }

  @Override
  public Result mutateRow(RowMutations rm) throws IOException {
    return FutureUtils.get(table.mutateRow(rm));
  }

  @Override
  public Result append(Append append) throws IOException {
    return FutureUtils.get(table.append(append));
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return FutureUtils.get(table.increment(increment));
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    return FutureUtils.get(table.incrementColumnValue(row, family, qualifier, amount));
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    return FutureUtils.get(table.incrementColumnValue(row, family, qualifier, amount, durability));
  }

  @Override
  public void close() {
  }

  @SuppressWarnings("deprecation")
  private static final class RegionCoprocessorRpcChannel extends RegionCoprocessorRpcChannelImpl
      implements CoprocessorRpcChannel {

    RegionCoprocessorRpcChannel(AsyncConnectionImpl conn, TableName tableName, RegionInfo region,
        byte[] row, long rpcTimeoutNs, long operationTimeoutNs) {
      super(conn, tableName, region, row, rpcTimeoutNs, operationTimeoutNs);
    }

    @Override
    public void callMethod(MethodDescriptor method, RpcController controller, Message request,
        Message responsePrototype, RpcCallback<Message> done) {
      ClientCoprocessorRpcController c = new ClientCoprocessorRpcController();
      CoprocessorBlockingRpcCallback<Message> callback = new CoprocessorBlockingRpcCallback<>();
      super.callMethod(method, c, request, responsePrototype, callback);
      Message ret;
      try {
        ret = callback.get();
      } catch (IOException e) {
        setCoprocessorError(controller, e);
        return;
      }
      if (c.failed()) {
        setCoprocessorError(controller, c.getFailed());
      }
      done.run(ret);
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor method, RpcController controller,
        Message request, Message responsePrototype) throws ServiceException {
      ClientCoprocessorRpcController c = new ClientCoprocessorRpcController();
      CoprocessorBlockingRpcCallback<Message> done = new CoprocessorBlockingRpcCallback<>();
      callMethod(method, c, request, responsePrototype, done);
      Message ret;
      try {
        ret = done.get();
      } catch (IOException e) {
        throw new ServiceException(e);
      }
      if (c.failed()) {
        setCoprocessorError(controller, c.getFailed());
        throw new ServiceException(c.getFailed());
      }
      return ret;
    }
  }

  @Override
  public RegionCoprocessorRpcChannel coprocessorService(byte[] row) {
    return new RegionCoprocessorRpcChannel(conn, getName(), null, row,
      getRpcTimeout(TimeUnit.NANOSECONDS), getOperationTimeout(TimeUnit.NANOSECONDS));
  }

  /**
   * Get the corresponding start keys and regions for an arbitrary range of keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range
   * @param includeEndKey true if endRow is inclusive, false if exclusive
   * @return A pair of list of start keys and list of HRegionLocations that contain the specified
   *         range
   * @throws IOException if a remote or network exception occurs
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(final byte[] startKey,
      final byte[] endKey, final boolean includeEndKey) throws IOException {
    return getKeysAndRegionsInRange(startKey, endKey, includeEndKey, false);
  }

  /**
   * Get the corresponding start keys and regions for an arbitrary range of keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range
   * @param includeEndKey true if endRow is inclusive, false if exclusive
   * @param reload true to reload information or false to use cached information
   * @return A pair of list of start keys and list of HRegionLocations that contain the specified
   *         range
   * @throws IOException if a remote or network exception occurs
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(final byte[] startKey,
      final byte[] endKey, final boolean includeEndKey, final boolean reload) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException(
        "Invalid range: " + Bytes.toStringBinary(startKey) + " > " + Bytes.toStringBinary(endKey));
    }
    List<byte[]> keysInRange = new ArrayList<>();
    List<HRegionLocation> regionsInRange = new ArrayList<>();
    byte[] currentKey = startKey;
    do {
      HRegionLocation regionLocation =
        FutureUtils.get(conn.getRegionLocator(getName()).getRegionLocation(currentKey, reload));
      keysInRange.add(currentKey);
      regionsInRange.add(regionLocation);
      currentKey = regionLocation.getRegion().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
      (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0 ||
        (includeEndKey && Bytes.compareTo(currentKey, endKey) == 0)));
    return new Pair<>(keysInRange, regionsInRange);
  }

  private List<byte[]> getStartKeysInRange(byte[] start, byte[] end) throws IOException {
    if (start == null) {
      start = HConstants.EMPTY_START_ROW;
    }
    if (end == null) {
      end = HConstants.EMPTY_END_ROW;
    }
    return getKeysAndRegionsInRange(start, end, true).getFirst();
  }

  @FunctionalInterface
  private interface StubCall<R> {
    R call(RegionCoprocessorRpcChannel channel) throws Exception;
  }

  private <R> void coprocessorService(String serviceName, byte[] startKey, byte[] endKey,
      Callback<R> callback, StubCall<R> call) throws Throwable {
    // get regions covered by the row range
    ExecutorService pool = Context.current().wrap(this.poolSupplier.get());
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);
    Map<byte[], Future<R>> futures = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    try {
      for (byte[] r : keys) {
        RegionCoprocessorRpcChannel channel = coprocessorService(r);
        Future<R> future = pool.submit(() -> {
          R result = call.call(channel);
          byte[] region = channel.getLastRegion();
          if (callback != null) {
            callback.update(region, r, result);
          }
          return result;
        });
        futures.put(r, future);
      }
    } catch (RejectedExecutionException e) {
      // maybe the connection has been closed, let's check
      if (conn.isClosed()) {
        throw new DoNotRetryIOException("Connection is closed", e);
      } else {
        throw new HBaseIOException("Coprocessor operation is rejected", e);
      }
    }
    for (Map.Entry<byte[], Future<R>> e : futures.entrySet()) {
      try {
        e.getValue().get();
      } catch (ExecutionException ee) {
        LOG.warn("Error calling coprocessor service {} for row {}", serviceName,
          Bytes.toStringBinary(e.getKey()), ee);
        throw ee.getCause();
      } catch (InterruptedException ie) {
        throw new InterruptedIOException("Interrupted calling coprocessor service " + serviceName +
          " for row " + Bytes.toStringBinary(e.getKey())).initCause(ie);
      }
    }
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    final Supplier<Span> supplier = new TableOperationSpanBuilder(conn)
      .setTableName(table.getName())
      .setOperation(HBaseSemanticAttributes.Operation.COPROC_EXEC);
    TraceUtil.trace(() -> {
      final Context context = Context.current();
      coprocessorService(service.getName(), startKey, endKey, callback, channel -> {
        try (Scope ignored = context.makeCurrent()) {
          T instance = ProtobufUtil.newServiceStub(service, channel);
          return callable.call(instance);
        }
      });
    }, supplier);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    final Supplier<Span> supplier = new TableOperationSpanBuilder(conn)
      .setTableName(table.getName())
      .setOperation(HBaseSemanticAttributes.Operation.COPROC_EXEC);
    TraceUtil.trace(() -> {
      final Context context = Context.current();
      coprocessorService(methodDescriptor.getFullName(), startKey, endKey, callback, channel -> {
        try (Scope ignored = context.makeCurrent()) {
          return (R) channel.callBlockingMethod(
            methodDescriptor, null, request, responsePrototype);
        }
      });
    }, supplier);
  }

  @Override
  public long getRpcTimeout(TimeUnit unit) {
    return table.getRpcTimeout(unit);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return table.getReadRpcTimeout(unit);
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return table.getWriteRpcTimeout(unit);
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return table.getOperationTimeout(unit);
  }

  @Override
  public RegionLocator getRegionLocator() throws IOException {
    return conn.toConnection().getRegionLocator(getName());
  }
}
