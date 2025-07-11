/*
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

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.client.ConnectionUtils.checkHasFamilies;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.client.ConnectionUtils.timelineConsistentRead;
import static org.apache.hadoop.hbase.client.ConnectionUtils.validatePut;
import static org.apache.hadoop.hbase.client.ConnectionUtils.validatePutsInRowMutations;
import static org.apache.hadoop.hbase.trace.TraceUtil.tracedFuture;
import static org.apache.hadoop.hbase.trace.TraceUtil.tracedFutures;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.SingleRequestCallerBuilder;
import org.apache.hadoop.hbase.client.ConnectionUtils.Converter;
import org.apache.hadoop.hbase.client.RegionCoprocessorRpcChannelImpl.RegionNoLongerExistsException;
import org.apache.hadoop.hbase.client.trace.TableOperationSpanBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.io.netty.util.Timer;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;

/**
 * The implementation of RawAsyncTable.
 * <p/>
 * The word 'Raw' means that this is a low level class. The returned {@link CompletableFuture} will
 * be finished inside the rpc framework thread, which means that the callbacks registered to the
 * {@link CompletableFuture} will also be executed inside the rpc framework thread. So users who use
 * this class should not try to do time consuming tasks in the callbacks.
 * @since 2.0.0
 * @see AsyncTableImpl
 */
@InterfaceAudience.Private
class RawAsyncTableImpl implements AsyncTable<AdvancedScanResultConsumer> {

  private static final Logger LOG = LoggerFactory.getLogger(RawAsyncTableImpl.class);

  private final AsyncConnectionImpl conn;

  private final Timer retryTimer;

  private final TableName tableName;

  private final int defaultScannerCaching;

  private final long defaultScannerMaxResultSize;

  private final long rpcTimeoutNs;

  private final long readRpcTimeoutNs;

  private final long writeRpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long scanTimeoutNs;

  private final long pauseNs;

  private final long pauseNsForServerOverloaded;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private final Map<String, byte[]> requestAttributes;

  RawAsyncTableImpl(AsyncConnectionImpl conn, Timer retryTimer, AsyncTableBuilderBase<?> builder) {
    this.conn = conn;
    this.retryTimer = retryTimer;
    this.tableName = builder.tableName;
    this.rpcTimeoutNs = builder.rpcTimeoutNs;
    this.readRpcTimeoutNs = builder.readRpcTimeoutNs;
    this.writeRpcTimeoutNs = builder.writeRpcTimeoutNs;
    this.operationTimeoutNs = builder.operationTimeoutNs;
    this.scanTimeoutNs = builder.scanTimeoutNs;
    this.pauseNs = builder.pauseNs;
    if (builder.pauseNsForServerOverloaded < builder.pauseNs) {
      LOG.warn(
        "Configured value of pauseNsForServerOverloaded is {} ms, which is less than"
          + " the normal pause value {} ms, use the greater one instead",
        TimeUnit.NANOSECONDS.toMillis(builder.pauseNsForServerOverloaded),
        TimeUnit.NANOSECONDS.toMillis(builder.pauseNs));
      this.pauseNsForServerOverloaded = builder.pauseNs;
    } else {
      this.pauseNsForServerOverloaded = builder.pauseNsForServerOverloaded;
    }
    this.maxAttempts = builder.maxAttempts;
    this.startLogErrorsCnt = builder.startLogErrorsCnt;
    this.defaultScannerCaching = tableName.isSystemTable()
      ? conn.connConf.getMetaScannerCaching()
      : conn.connConf.getScannerCaching();
    this.defaultScannerMaxResultSize = conn.connConf.getScannerMaxResultSize();
    this.requestAttributes = builder.requestAttributes;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public Configuration getConfiguration() {
    return conn.getConfiguration();
  }

  @Override
  public CompletableFuture<TableDescriptor> getDescriptor() {
    return conn.getAdmin().getDescriptor(tableName);
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator() {
    return conn.getRegionLocator(tableName);
  }

  private static <REQ, RESP> CompletableFuture<RESP> mutate(HBaseRpcController controller,
    HRegionLocation loc, ClientService.Interface stub, REQ req,
    Converter<MutateRequest, byte[], REQ> reqConvert,
    Converter<RESP, HBaseRpcController, MutateResponse> respConverter) {
    return ConnectionUtils.call(controller, loc, stub, req, reqConvert,
      (s, c, r, done) -> s.mutate(c, r, done), respConverter);
  }

  private static <REQ> CompletableFuture<Void> voidMutate(HBaseRpcController controller,
    HRegionLocation loc, ClientService.Interface stub, REQ req,
    Converter<MutateRequest, byte[], REQ> reqConvert) {
    return mutate(controller, loc, stub, req, reqConvert, (c, resp) -> {
      return null;
    });
  }

  private static Result toResult(HBaseRpcController controller, MutateResponse resp)
    throws IOException {
    if (!resp.hasResult()) {
      return null;
    }
    return ProtobufUtil.toResult(resp.getResult(), controller.cellScanner());
  }

  @FunctionalInterface
  private interface NoncedConverter<D, I, S> {
    D convert(I info, S src, long nonceGroup, long nonce) throws IOException;
  }

  private <REQ, RESP> CompletableFuture<RESP> noncedMutate(long nonceGroup, long nonce,
    HBaseRpcController controller, HRegionLocation loc, ClientService.Interface stub, REQ req,
    NoncedConverter<MutateRequest, byte[], REQ> reqConvert,
    Converter<RESP, HBaseRpcController, MutateResponse> respConverter) {
    return mutate(controller, loc, stub, req,
      (info, src) -> reqConvert.convert(info, src, nonceGroup, nonce), respConverter);
  }

  private <T> SingleRequestCallerBuilder<T> newCaller(byte[] row, int priority, long rpcTimeoutNs) {
    return conn.callerFactory.<T> single().table(tableName).row(row).priority(priority)
      .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
      .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
      .pause(pauseNs, TimeUnit.NANOSECONDS)
      .pauseForServerOverloaded(pauseNsForServerOverloaded, TimeUnit.NANOSECONDS)
      .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt)
      .setRequestAttributes(requestAttributes);
  }

  private <T, R extends OperationWithAttributes & Row> SingleRequestCallerBuilder<T>
    newCaller(R row, long rpcTimeoutNs) {
    return newCaller(row.getRow(), row.getPriority(), rpcTimeoutNs);
  }

  private CompletableFuture<Result> get(Get get, int replicaId) {
    return this.<Result, Get> newCaller(get, readRpcTimeoutNs)
      .action((controller, loc, stub) -> ConnectionUtils.<Get, GetRequest, GetResponse,
        Result> call(controller, loc, stub, get, RequestConverter::buildGetRequest,
          (s, c, req, done) -> s.get(c, req, done),
          (c, resp) -> ProtobufUtil.toResult(resp.getResult(), c.cellScanner())))
      .replicaId(replicaId).call();
  }

  private TableOperationSpanBuilder newTableOperationSpanBuilder() {
    return new TableOperationSpanBuilder(conn).setTableName(tableName);
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(get);
    return tracedFuture(
      () -> timelineConsistentRead(conn.getLocator(), tableName, get, get.getRow(),
        RegionLocateType.CURRENT, replicaId -> get(get, replicaId), readRpcTimeoutNs,
        conn.connConf.getPrimaryCallTimeoutNs(), retryTimer, conn.getConnectionMetrics()),
      supplier);
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    validatePut(put, conn.connConf.getMaxKeyValueSize());
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(put);
    return tracedFuture(() -> this.<Void, Put> newCaller(put, writeRpcTimeoutNs)
      .action((controller, loc, stub) -> RawAsyncTableImpl.<Put> voidMutate(controller, loc, stub,
        put, RequestConverter::buildMutateRequest))
      .call(), supplier);
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(delete);
    return tracedFuture(() -> this.<Void, Delete> newCaller(delete, writeRpcTimeoutNs)
      .action((controller, loc, stub) -> RawAsyncTableImpl.<Delete> voidMutate(controller, loc,
        stub, delete, RequestConverter::buildMutateRequest))
      .call(), supplier);
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    checkHasFamilies(append);
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(append);
    return tracedFuture(() -> {
      long nonceGroup = conn.getNonceGenerator().getNonceGroup();
      long nonce = conn.getNonceGenerator().newNonce();
      return this.<Result, Append> newCaller(append, rpcTimeoutNs)
        .action((controller, loc, stub) -> this.<Append, Result> noncedMutate(nonceGroup, nonce,
          controller, loc, stub, append, RequestConverter::buildMutateRequest,
          RawAsyncTableImpl::toResult))
        .call();
    }, supplier);
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    checkHasFamilies(increment);
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(increment);
    return tracedFuture(() -> {
      long nonceGroup = conn.getNonceGenerator().getNonceGroup();
      long nonce = conn.getNonceGenerator().newNonce();
      return this.<Result, Increment> newCaller(increment, rpcTimeoutNs)
        .action((controller, loc, stub) -> this.<Increment, Result> noncedMutate(nonceGroup, nonce,
          controller, loc, stub, increment, RequestConverter::buildMutateRequest,
          RawAsyncTableImpl::toResult))
        .call();
    }, supplier);
  }

  private final class CheckAndMutateBuilderImpl implements CheckAndMutateBuilder {

    private final byte[] row;

    private final byte[] family;

    private byte[] qualifier;

    private TimeRange timeRange;

    private CompareOperator op;

    private byte[] value;

    public CheckAndMutateBuilderImpl(byte[] row, byte[] family) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.family = Preconditions.checkNotNull(family, "family is null");
    }

    @Override
    public CheckAndMutateBuilder qualifier(byte[] qualifier) {
      this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null. Consider using"
        + " an empty byte array, or just do not call this method if you want a null qualifier");
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
      Preconditions.checkNotNull(op, "condition is null. You need to specify the condition by"
        + " calling ifNotExists/ifEquals/ifMatches before executing the request");
    }

    @Override
    public CompletableFuture<Boolean> thenPut(Put put) {
      validatePut(put, conn.connConf.getMaxKeyValueSize());
      preCheck();
      final Supplier<Span> supplier = newTableOperationSpanBuilder()
        .setOperation(HBaseSemanticAttributes.Operation.CHECK_AND_MUTATE)
        .setContainerOperations(put);
      return tracedFuture(
        () -> RawAsyncTableImpl.this.<Boolean> newCaller(row, put.getPriority(), rpcTimeoutNs)
          .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller, loc, stub, put,
            (rn, p) -> RequestConverter.buildMutateRequest(rn, row, family, qualifier, op, value,
              null, timeRange, p, HConstants.NO_NONCE, HConstants.NO_NONCE, false),
            (c, r) -> r.getProcessed()))
          .call(),
        supplier);
    }

    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      preCheck();
      final Supplier<Span> supplier = newTableOperationSpanBuilder()
        .setOperation(HBaseSemanticAttributes.Operation.CHECK_AND_MUTATE)
        .setContainerOperations(delete);
      return tracedFuture(
        () -> RawAsyncTableImpl.this.<Boolean> newCaller(row, delete.getPriority(), rpcTimeoutNs)
          .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller, loc, stub, delete,
            (rn, d) -> RequestConverter.buildMutateRequest(rn, row, family, qualifier, op, value,
              null, timeRange, d, HConstants.NO_NONCE, HConstants.NO_NONCE, false),
            (c, r) -> r.getProcessed()))
          .call(),
        supplier);
    }

    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations mutations) {
      preCheck();
      validatePutsInRowMutations(mutations, conn.connConf.getMaxKeyValueSize());
      final Supplier<Span> supplier = newTableOperationSpanBuilder()
        .setOperation(HBaseSemanticAttributes.Operation.CHECK_AND_MUTATE)
        .setContainerOperations(mutations);
      return tracedFuture(() -> RawAsyncTableImpl.this
        .<Boolean> newCaller(row, mutations.getMaxPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.this.mutateRow(controller, loc, stub,
          mutations,
          (rn, rm) -> RequestConverter.buildMultiRequest(rn, row, family, qualifier, op, value,
            null, timeRange, rm, HConstants.NO_NONCE, HConstants.NO_NONCE, false),
          CheckAndMutateResult::isSuccess))
        .call(), supplier);
    }
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilderImpl(row, family);
  }

  private final class CheckAndMutateWithFilterBuilderImpl
    implements CheckAndMutateWithFilterBuilder {

    private final byte[] row;

    private final Filter filter;

    private TimeRange timeRange;

    public CheckAndMutateWithFilterBuilderImpl(byte[] row, Filter filter) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.filter = Preconditions.checkNotNull(filter, "filter is null");
    }

    @Override
    public CheckAndMutateWithFilterBuilder timeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    @Override
    public CompletableFuture<Boolean> thenPut(Put put) {
      validatePut(put, conn.connConf.getMaxKeyValueSize());
      final Supplier<Span> supplier = newTableOperationSpanBuilder()
        .setOperation(HBaseSemanticAttributes.Operation.CHECK_AND_MUTATE)
        .setContainerOperations(put);
      return tracedFuture(
        () -> RawAsyncTableImpl.this.<Boolean> newCaller(row, put.getPriority(), rpcTimeoutNs)
          .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller, loc, stub, put,
            (rn, p) -> RequestConverter.buildMutateRequest(rn, row, null, null, null, null, filter,
              timeRange, p, HConstants.NO_NONCE, HConstants.NO_NONCE, false),
            (c, r) -> r.getProcessed()))
          .call(),
        supplier);
    }

    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      final Supplier<Span> supplier = newTableOperationSpanBuilder()
        .setOperation(HBaseSemanticAttributes.Operation.CHECK_AND_MUTATE)
        .setContainerOperations(delete);
      return tracedFuture(
        () -> RawAsyncTableImpl.this.<Boolean> newCaller(row, delete.getPriority(), rpcTimeoutNs)
          .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller, loc, stub, delete,
            (rn, d) -> RequestConverter.buildMutateRequest(rn, row, null, null, null, null, filter,
              timeRange, d, HConstants.NO_NONCE, HConstants.NO_NONCE, false),
            (c, r) -> r.getProcessed()))
          .call(),
        supplier);
    }

    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations mutations) {
      validatePutsInRowMutations(mutations, conn.connConf.getMaxKeyValueSize());
      final Supplier<Span> supplier = newTableOperationSpanBuilder()
        .setOperation(HBaseSemanticAttributes.Operation.CHECK_AND_MUTATE)
        .setContainerOperations(mutations);
      return tracedFuture(() -> RawAsyncTableImpl.this
        .<Boolean> newCaller(row, mutations.getMaxPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.this.mutateRow(controller, loc, stub,
          mutations,
          (rn, rm) -> RequestConverter.buildMultiRequest(rn, row, null, null, null, null, filter,
            timeRange, rm, HConstants.NO_NONCE, HConstants.NO_NONCE, false),
          CheckAndMutateResult::isSuccess))
        .call(), supplier);
    }
  }

  @Override
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    return new CheckAndMutateWithFilterBuilderImpl(row, filter);
  }

  @Override
  public CompletableFuture<CheckAndMutateResult> checkAndMutate(CheckAndMutate checkAndMutate) {
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(checkAndMutate)
      .setContainerOperations(checkAndMutate.getAction());
    return tracedFuture(() -> {
      if (
        checkAndMutate.getAction() instanceof Put || checkAndMutate.getAction() instanceof Delete
          || checkAndMutate.getAction() instanceof Increment
          || checkAndMutate.getAction() instanceof Append
      ) {
        Mutation mutation = (Mutation) checkAndMutate.getAction();
        if (mutation instanceof Put) {
          validatePut((Put) mutation, conn.connConf.getMaxKeyValueSize());
        }
        long nonceGroup = conn.getNonceGenerator().getNonceGroup();
        long nonce = conn.getNonceGenerator().newNonce();
        return RawAsyncTableImpl.this
          .<CheckAndMutateResult> newCaller(checkAndMutate.getRow(), mutation.getPriority(),
            rpcTimeoutNs)
          .action(
            (controller, loc, stub) -> RawAsyncTableImpl.mutate(controller, loc, stub, mutation,
              (rn, m) -> RequestConverter.buildMutateRequest(rn, checkAndMutate.getRow(),
                checkAndMutate.getFamily(), checkAndMutate.getQualifier(),
                checkAndMutate.getCompareOp(), checkAndMutate.getValue(),
                checkAndMutate.getFilter(), checkAndMutate.getTimeRange(), m, nonceGroup, nonce,
                checkAndMutate.isQueryMetricsEnabled()),
              (c, r) -> ResponseConverter.getCheckAndMutateResult(r, c.cellScanner())))
          .call();
      } else if (checkAndMutate.getAction() instanceof RowMutations) {
        RowMutations rowMutations = (RowMutations) checkAndMutate.getAction();
        validatePutsInRowMutations(rowMutations, conn.connConf.getMaxKeyValueSize());
        long nonceGroup = conn.getNonceGenerator().getNonceGroup();
        long nonce = conn.getNonceGenerator().newNonce();
        return RawAsyncTableImpl.this
          .<CheckAndMutateResult> newCaller(checkAndMutate.getRow(), rowMutations.getMaxPriority(),
            rpcTimeoutNs)
          .action((controller, loc, stub) -> RawAsyncTableImpl.this.<CheckAndMutateResult,
            CheckAndMutateResult> mutateRow(controller, loc, stub, rowMutations,
              (rn, rm) -> RequestConverter.buildMultiRequest(rn, checkAndMutate.getRow(),
                checkAndMutate.getFamily(), checkAndMutate.getQualifier(),
                checkAndMutate.getCompareOp(), checkAndMutate.getValue(),
                checkAndMutate.getFilter(), checkAndMutate.getTimeRange(), rm, nonceGroup, nonce,
                checkAndMutate.isQueryMetricsEnabled()),
              resp -> resp))
          .call();
      } else {
        CompletableFuture<CheckAndMutateResult> future = new CompletableFuture<>();
        future.completeExceptionally(new DoNotRetryIOException(
          "CheckAndMutate doesn't support " + checkAndMutate.getAction().getClass().getName()));
        return future;
      }
    }, supplier);
  }

  @Override
  public List<CompletableFuture<CheckAndMutateResult>>
    checkAndMutate(List<CheckAndMutate> checkAndMutates) {
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(checkAndMutates)
      .setContainerOperations(checkAndMutates);
    return tracedFutures(() -> batch(checkAndMutates, rpcTimeoutNs).stream()
      .map(f -> f.thenApply(r -> (CheckAndMutateResult) r)).collect(toList()), supplier);
  }

  // We need the MultiRequest when constructing the org.apache.hadoop.hbase.client.MultiResponse,
  // so here I write a new method as I do not want to change the abstraction of call method.
  @SuppressWarnings("unchecked")
  private <RES, RESP> CompletableFuture<RESP> mutateRow(HBaseRpcController controller,
    HRegionLocation loc, ClientService.Interface stub, RowMutations mutation,
    Converter<MultiRequest, byte[], RowMutations> reqConvert, Function<RES, RESP> respConverter) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    try {
      byte[] regionName = loc.getRegion().getRegionName();
      MultiRequest req = reqConvert.convert(regionName, mutation);
      stub.multi(controller, req, new RpcCallback<MultiResponse>() {

        @Override
        public void run(MultiResponse resp) {
          if (controller.failed()) {
            future.completeExceptionally(controller.getFailed());
          } else {
            try {
              org.apache.hadoop.hbase.client.MultiResponse multiResp =
                ResponseConverter.getResults(req, resp, controller.cellScanner());
              ConnectionUtils.updateStats(conn.getStatisticsTracker(), conn.getConnectionMetrics(),
                loc.getServerName(), multiResp);
              Throwable ex = multiResp.getException(regionName);
              if (ex != null) {
                future.completeExceptionally(ex instanceof IOException
                  ? ex
                  : new IOException(
                    "Failed to mutate row: " + Bytes.toStringBinary(mutation.getRow()), ex));
              } else {
                future.complete(
                  respConverter.apply((RES) multiResp.getResults().get(regionName).result.get(0)));
              }
            } catch (IOException e) {
              future.completeExceptionally(e);
            }
          }
        }
      });
    } catch (IOException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public CompletableFuture<Result> mutateRow(RowMutations mutations) {
    validatePutsInRowMutations(mutations, conn.connConf.getMaxKeyValueSize());
    long nonceGroup = conn.getNonceGenerator().getNonceGroup();
    long nonce = conn.getNonceGenerator().newNonce();
    final Supplier<Span> supplier =
      newTableOperationSpanBuilder().setOperation(mutations).setContainerOperations(mutations);
    return tracedFuture(
      () -> this
        .<Result> newCaller(mutations.getRow(), mutations.getMaxPriority(), writeRpcTimeoutNs)
        .action((controller, loc, stub) -> this.<Result, Result> mutateRow(controller, loc, stub,
          mutations, (rn, rm) -> RequestConverter.buildMultiRequest(rn, rm, nonceGroup, nonce),
          resp -> resp))
        .call(),
      supplier);
  }

  private Scan setDefaultScanConfig(Scan scan) {
    // always create a new scan object as we may reset the start row later.
    Scan newScan = ReflectionUtils.newInstance(scan.getClass(), scan);
    if (newScan.getCaching() <= 0) {
      newScan.setCaching(defaultScannerCaching);
    }
    if (newScan.getMaxResultSize() <= 0) {
      newScan.setMaxResultSize(defaultScannerMaxResultSize);
    }
    return newScan;
  }

  @Override
  public void scan(Scan scan, AdvancedScanResultConsumer consumer) {
    new AsyncClientScanner(setDefaultScanConfig(scan), consumer, tableName, conn, retryTimer,
      pauseNs, pauseNsForServerOverloaded, maxAttempts, scanTimeoutNs, readRpcTimeoutNs,
      startLogErrorsCnt, requestAttributes).start();
  }

  private long resultSize2CacheSize(long maxResultSize) {
    // * 2 if possible
    return maxResultSize > Long.MAX_VALUE / 2 ? maxResultSize : maxResultSize * 2;
  }

  @Override
  public AsyncTableResultScanner getScanner(Scan scan) {
    final long maxCacheSize = resultSize2CacheSize(
      scan.getMaxResultSize() > 0 ? scan.getMaxResultSize() : defaultScannerMaxResultSize);
    final Scan scanCopy = ReflectionUtils.newInstance(scan.getClass(), scan);
    final AsyncTableResultScanner scanner =
      new AsyncTableResultScanner(tableName, scanCopy, maxCacheSize);
    scan(scan, scanner);
    return scanner;
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    CompletableFuture<List<Result>> future = new CompletableFuture<>();
    List<Result> scanResults = new ArrayList<>();
    scan(scan, new AdvancedScanResultConsumer() {

      @Override
      public void onNext(Result[] results, ScanController controller) {
        scanResults.addAll(Arrays.asList(results));
      }

      @Override
      public void onError(Throwable error) {
        future.completeExceptionally(error);
      }

      @Override
      public void onComplete() {
        future.complete(scanResults);
      }
    });
    return future;
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> gets) {
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(gets)
      .setContainerOperations(HBaseSemanticAttributes.Operation.GET);
    return tracedFutures(() -> batch(gets, readRpcTimeoutNs), supplier);
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> puts) {
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(puts)
      .setContainerOperations(HBaseSemanticAttributes.Operation.PUT);
    return tracedFutures(() -> voidMutate(puts), supplier);
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    final Supplier<Span> supplier = newTableOperationSpanBuilder().setOperation(deletes)
      .setContainerOperations(HBaseSemanticAttributes.Operation.DELETE);
    return tracedFutures(() -> voidMutate(deletes), supplier);
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    final Supplier<Span> supplier =
      newTableOperationSpanBuilder().setOperation(actions).setContainerOperations(actions);
    return tracedFutures(() -> batch(actions, rpcTimeoutNs), supplier);
  }

  private List<CompletableFuture<Void>> voidMutate(List<? extends Row> actions) {
    return this.<Object> batch(actions, writeRpcTimeoutNs).stream()
      .map(f -> f.<Void> thenApply(r -> null)).collect(toList());
  }

  private <T> List<CompletableFuture<T>> batch(List<? extends Row> actions, long rpcTimeoutNs) {
    for (Row action : actions) {
      if (action instanceof Put) {
        validatePut((Put) action, conn.connConf.getMaxKeyValueSize());
      } else if (action instanceof CheckAndMutate) {
        CheckAndMutate checkAndMutate = (CheckAndMutate) action;
        if (checkAndMutate.getAction() instanceof Put) {
          validatePut((Put) checkAndMutate.getAction(), conn.connConf.getMaxKeyValueSize());
        } else if (checkAndMutate.getAction() instanceof RowMutations) {
          validatePutsInRowMutations((RowMutations) checkAndMutate.getAction(),
            conn.connConf.getMaxKeyValueSize());
        }
      } else if (action instanceof RowMutations) {
        validatePutsInRowMutations((RowMutations) action, conn.connConf.getMaxKeyValueSize());
      }
    }
    return conn.callerFactory.batch().table(tableName).actions(actions)
      .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
      .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS).pause(pauseNs, TimeUnit.NANOSECONDS)
      .pauseForServerOverloaded(pauseNsForServerOverloaded, TimeUnit.NANOSECONDS)
      .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt)
      .setRequestAttributes(requestAttributes).call();
  }

  @Override
  public long getRpcTimeout(TimeUnit unit) {
    return unit.convert(rpcTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return unit.convert(readRpcTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return unit.convert(writeRpcTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return unit.convert(operationTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public long getScanTimeout(TimeUnit unit) {
    return unit.convert(scanTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public Map<String, byte[]> getRequestAttributes() {
    return requestAttributes;
  }

  private <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
    ServiceCaller<S, R> callable, RegionInfo region, byte[] row) {
    RegionCoprocessorRpcChannelImpl channel = new RegionCoprocessorRpcChannelImpl(conn, tableName,
      region, row, rpcTimeoutNs, operationTimeoutNs);
    final Span span = Span.current();
    S stub = stubMaker.apply(channel);
    CompletableFuture<R> future = new CompletableFuture<>();
    ClientCoprocessorRpcController controller = new ClientCoprocessorRpcController();
    callable.call(stub, controller, resp -> {
      try (Scope ignored = span.makeCurrent()) {
        if (controller.failed()) {
          final Throwable failure = controller.getFailed();
          future.completeExceptionally(failure);
          TraceUtil.setError(span, failure);
        } else {
          future.complete(resp);
          span.setStatus(StatusCode.OK);
        }
      } finally {
        span.end();
      }
    });
    return future;
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
    ServiceCaller<S, R> callable, byte[] row) {
    return coprocessorService(stubMaker, callable, null, row);
  }

  private boolean locateFinished(RegionInfo region, byte[] endKey, boolean endKeyInclusive) {
    if (isEmptyStopRow(endKey)) {
      if (isEmptyStopRow(region.getEndKey())) {
        return true;
      }
      return false;
    } else {
      if (isEmptyStopRow(region.getEndKey())) {
        return true;
      }
      int c = Bytes.compareTo(endKey, region.getEndKey());
      // 1. if the region contains endKey
      // 2. endKey is equal to the region's endKey and we do not want to include endKey.
      return c < 0 || (c == 0 && !endKeyInclusive);
    }
  }

  private <S, R> void coprocessorServiceUntilComplete(Function<RpcChannel, S> stubMaker,
    ServiceCaller<S, R> callable, PartialResultCoprocessorCallback<S, R> callback,
    MultiRegionCoprocessorServiceProgress<R> progress, RegionInfo region,
    AtomicBoolean claimedRegionAssignment, Span span) {
    addListener(coprocessorService(stubMaker, callable, region, region.getStartKey()), (r, e) -> {
      try (Scope ignored = span.makeCurrent()) {
        if (e instanceof RegionNoLongerExistsException) {
          RegionInfo newRegion = ((RegionNoLongerExistsException) e).getNewRegionInfo();
          if (claimedRegionAssignment.get()) {
            // the region is already in progress but no longer exists, so it can't be finished nor
            // restarted
            callback.onRegionError(region,
              new DoNotRetryIOException(
                ("Region %s has been replaced by another region and can't be finished, likely"
                  + " due to a region merge or split").formatted(region.getEncodedName())));
          } else if (progress.markRegionReplacedExistingAndCheckNeedsToBeRestarted(newRegion)) {
            LOG.debug(
              "Attempted to send a coprocessor service RPC to region {} which no"
                + " longer exists, will attempt to send RPCs to the region(s) that replaced it",
              region.getEncodedName());
            restartCoprocessorServiceForRange(stubMaker, callable, callback, region.getStartKey(),
              region.getEndKey(), newRegion, progress, span);
          }
          progress.markRegionFinished(region);
        } else {
          if (!claimedRegionAssignment.get()) {
            if (!progress.tryClaimRegionAssignment(region)) {
              progress.markRegionFinished(region);
              return;
            } else {
              claimedRegionAssignment.set(true);
            }
          }
          progress.onResponse(region, r, e);

          ServiceCaller<S, R> updatedCallable;
          if (e == null && r != null) {
            updatedCallable = callback.getNextCallable(r, region);
          } else {
            updatedCallable = null;
          }

          // If updatedCallable is non-null, we will be sending another request, so no need to
          // decrement unfinishedRequest (recall that && short-circuits).
          // If updatedCallable is null, and unfinishedRequest decrements to 0, we're done with the
          // requests for this coprocessor call.
          if (updatedCallable == null) {
            progress.markRegionFinished(region);
          } else {
            Duration waitInterval = callback.getWaitInterval(r, region);
            LOG.trace("Coprocessor returned incomplete result. "
              + "Sleeping for {} before making follow-up request.", waitInterval);
            if (!waitInterval.isZero()) {
              AsyncConnectionImpl.RETRY_TIMER.newTimeout(
                (timeout) -> coprocessorServiceUntilComplete(stubMaker, updatedCallable, callback,
                  progress, region, claimedRegionAssignment, span),
                waitInterval.toMillis(), TimeUnit.MILLISECONDS);
            } else {
              coprocessorServiceUntilComplete(stubMaker, updatedCallable, callback, progress,
                region, claimedRegionAssignment, span);
            }
          }
        }
      } catch (Throwable t) {
        if (claimedRegionAssignment.get() || progress.tryClaimRegionAssignment(region)) {
          LOG.error("Error processing coprocessor service response for region {}",
            region.getEncodedName(), t);
          progress.onResponse(region, null, t);
        }
        progress.markRegionFinished(region);
      }
    });
  }

  private <S, R> void restartCoprocessorServiceForRange(Function<RpcChannel, S> stubMaker,
    ServiceCaller<S, R> callable, PartialResultCoprocessorCallback<S, R> callback, byte[] startKey,
    byte[] endKey, RegionInfo newRegion, MultiRegionCoprocessorServiceProgress<R> progress,
    Span span) {
    CoprocessorServiceBuilderImpl<S, R> builder = (CoprocessorServiceBuilderImpl<S,
      R>) coprocessorService(stubMaker, callable, new PartialResultCoprocessorCallback<S, R>() {
        final Set<RegionInfo> assignments = Collections.synchronizedSet(new HashSet<>());

        @Override
        public ServiceCaller<S, R> getNextCallable(R response, RegionInfo region) {
          if (assignments.contains(region)) {
            return callback.getNextCallable(response, region);
          }
          return null;
        }

        @Override
        public Duration getWaitInterval(R response, RegionInfo region) {
          return callback.getWaitInterval(response, region);
        }

        @Override
        public void onRegionComplete(RegionInfo region, R resp) {
          boolean hasAssignment = assignments.contains(region);
          if (!hasAssignment) {
            hasAssignment = progress.tryClaimRegionAssignment(region);
            if (hasAssignment) {
              assignments.add(region);
            }
          }
          if (hasAssignment) {
            progress.onResponse(region, resp, null);
          }
        }

        @Override
        public void onRegionError(RegionInfo region, Throwable error) {
          boolean hasAssignment = assignments.contains(region);
          if (!hasAssignment) {
            hasAssignment = progress.tryClaimRegionAssignment(region);
            if (hasAssignment) {
              assignments.add(region);
            }
          }
          if (hasAssignment) {
            progress.onResponse(region, null, error);
          }
        }

        @Override
        public void onComplete() {
          for (RegionInfo region : assignments) {
            progress.markRegionFinished(region);
          }
          if (!assignments.contains(newRegion)) {
            progress.markRegionFinished(newRegion);
          }
        }

        @Override
        public void onError(Throwable error) {
          callback.onError(error);
        }
      });

    builder.fromRow(startKey, true).toRow(endKey, false).withSpan(span).execute();
  }

  private <S, R> void onLocateComplete(Function<RpcChannel, S> stubMaker,
    ServiceCaller<S, R> callable, PartialResultCoprocessorCallback<S, R> callback, byte[] endKey,
    boolean endKeyInclusive, MultiRegionCoprocessorServiceProgress<R> progress, HRegionLocation loc,
    Throwable error) {
    final Span span = Span.current();
    if (error != null) {
      callback.onError(error);
      TraceUtil.setError(span, error);
      span.end();
      return;
    }
    progress.markRegionLocated(loc.getRegion());
    RegionInfo region = loc.getRegion();
    if (locateFinished(region, endKey, endKeyInclusive)) {
      progress.markLocateFinished();
    } else {
      addListener(conn.getLocator().getRegionLocation(tableName, region.getEndKey(),
        RegionLocateType.CURRENT, operationTimeoutNs), (l, e) -> {
          try (Scope ignored = span.makeCurrent()) {
            onLocateComplete(stubMaker, callable, callback, endKey, endKeyInclusive, progress, l,
              e);
          }
        });
    }
    coprocessorServiceUntilComplete(stubMaker, callable, callback, progress, region,
      new AtomicBoolean(false), span);
  }

  private final class CoprocessorServiceBuilderImpl<S, R>
    implements CoprocessorServiceBuilder<S, R> {

    private final Function<RpcChannel, S> stubMaker;

    private final ServiceCaller<S, R> callable;

    private final PartialResultCoprocessorCallback<S, R> callback;

    private byte[] startKey = HConstants.EMPTY_START_ROW;

    private boolean startKeyInclusive;

    private byte[] endKey = HConstants.EMPTY_END_ROW;

    private boolean endKeyInclusive;

    private Span span;

    public CoprocessorServiceBuilderImpl(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, PartialResultCoprocessorCallback<S, R> callback) {
      this.stubMaker = Preconditions.checkNotNull(stubMaker, "stubMaker is null");
      this.callable = Preconditions.checkNotNull(callable, "callable is null");
      this.callback = Preconditions.checkNotNull(callback, "callback is null");
    }

    @Override
    public CoprocessorServiceBuilderImpl<S, R> fromRow(byte[] startKey, boolean inclusive) {
      this.startKey = Preconditions.checkNotNull(startKey,
        "startKey is null. Consider using"
          + " an empty byte array, or just do not call this method if you want to start selection"
          + " from the first region");
      this.startKeyInclusive = inclusive;
      return this;
    }

    @Override
    public CoprocessorServiceBuilderImpl<S, R> toRow(byte[] endKey, boolean inclusive) {
      this.endKey = Preconditions.checkNotNull(endKey,
        "endKey is null. Consider using"
          + " an empty byte array, or just do not call this method if you want to continue"
          + " selection to the last region");
      this.endKeyInclusive = inclusive;
      return this;
    }

    CoprocessorServiceBuilderImpl<S, R> withSpan(Span span) {
      this.span = span;
      return this;
    }

    @Override
    public void execute() {
      final Span span = this.span == null
        ? newTableOperationSpanBuilder().setOperation(HBaseSemanticAttributes.Operation.COPROC_EXEC)
          .build()
        : this.span;
      try (Scope ignored = span.makeCurrent()) {
        final RegionLocateType regionLocateType =
          startKeyInclusive ? RegionLocateType.CURRENT : RegionLocateType.AFTER;
        final CompletableFuture<HRegionLocation> future = conn.getLocator()
          .getRegionLocation(tableName, startKey, regionLocateType, operationTimeoutNs);
        addListener(future, (loc, error) -> {
          try (Scope ignored1 = span.makeCurrent()) {
            onLocateComplete(stubMaker, callable, callback, endKey, endKeyInclusive,
              new MultiRegionCoprocessorServiceProgress<>(callback), loc, error);
          }
        });
      }
    }
  }

  @Override
  public <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(
    Function<RpcChannel, S> stubMaker, ServiceCaller<S, R> callable,
    CoprocessorCallback<R> callback) {
    return new CoprocessorServiceBuilderImpl<>(stubMaker, callable,
      new NoopPartialResultCoprocessorCallback<>(callback));
  }

  @Override
  public <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(
    Function<RpcChannel, S> stubMaker, ServiceCaller<S, R> callable,
    PartialResultCoprocessorCallback<S, R> callback) {
    return new CoprocessorServiceBuilderImpl<>(stubMaker, callable, callback);
  }

  /**
   * Coordinates coprocessor service responses across multiple regions when executing range queries.
   * <p>
   * When clients send coprocessor RPCs to a range, the process involves locating every region in
   * that range and sending an RPC to a row believed to be in each region. However, the actual
   * region may differ from what's in the meta cache, leading to complex race conditions that this
   * class manages.
   * <p>
   * Race conditions handled:
   * <ul>
   * <li><b>Region merges:</b> Multiple original requests consolidate into a single request; this
   * class provides leader election to ensure only one RPC is sent for the merged range</li>
   * <li><b>Region splits:</b> One original request becomes multiple requests; the service is
   * restarted for the affected range to cover all resulting regions</li>
   * <li><b>Region alignment changes:</b> When multiple original requests cover part of a new
   * region, but it doesn't fall into a simple split or merge case, the response is deduplicated
   * after it finishes executing</li>
   * <li><b>Region movement during execution:</b> If a coprocessor has already started on a region
   * but subsequent calls detect the region has changed, an error is reported as the request cannot
   * be recovered</li>
   * </ul>
   * <p>
   * The class maintains region progress state to prevent duplicate response processing and ensures
   * proper calling of Callback::onComplete when all regions have finished processing.
   */
  private static class MultiRegionCoprocessorServiceProgress<R> {
    private final AtomicBoolean locateFinished = new AtomicBoolean(false);
    private final ConcurrentNavigableMap<RegionInfo, RegionProgress> regions =
      new ConcurrentSkipListMap<>();
    private final CoprocessorCallback<R> callback;

    private MultiRegionCoprocessorServiceProgress(CoprocessorCallback<R> callback) {
      this.callback = callback;
    }

    void markLocateFinished() {
      locateFinished.set(true);
    }

    void markRegionLocated(RegionInfo region) {
      tryUpdateRegionProgress(region, RegionProgress.NONE, RegionProgress.LOCATED);
    }

    /**
     * Try to claim the assignment for the region in order to return the response to the caller.
     * This ensure that there is only ever one set of RPCs returned for a region.
     * @return true if claimed successfully, false otherwise
     */
    boolean tryClaimRegionAssignment(RegionInfo region) {
      boolean isNewlyAssigned = tryUpdateRegionProgress(region,
        EnumSet.of(RegionProgress.NONE, RegionProgress.LOCATED), RegionProgress.ASSIGNED);
      if (!isNewlyAssigned) {
        return false;
      }
      List<Pair<RegionInfo, RegionProgress>> overlappingAssignments =
        getOverlappingRegionAssignments(region);
      if (!overlappingAssignments.isEmpty()) {
        callback.onRegionError(region,
          new DoNotRetryIOException(
            ("Region %s has replaced a region that was already in progress, likely"
              + " due to a region merge or split. Overlapping regions: %s")
              .formatted(region.getEncodedName(),
                overlappingAssignments.stream().map(Pair::getFirst)
                  .map(org.apache.hadoop.hbase.client.RegionInfo::getEncodedName)
                  .collect(Collectors.joining(", ")))));
        tryUpdateRegionProgress(region, RegionProgress.ASSIGNED, RegionProgress.CANCELLED);
        return false;
      }

      return true;
    }

    private List<Pair<RegionInfo, RegionProgress>>
      getOverlappingRegionAssignments(RegionInfo region) {
      boolean isLast = isEmptyStopRow(region.getEndKey());

      List<Pair<RegionInfo, RegionProgress>> overlappingAssignments = new ArrayList<>();

      Map.Entry<RegionInfo,
        RegionProgress> candidate = isLast
          ? regions.lastEntry()
          : regions.ceilingEntry(
            RegionInfoBuilder.newBuilder(region).setStartKey(region.getEndKey()).build());

      // look through the list of all regions, starting from the end key of the region and
      // traversing backwards
      while (candidate != null) {
        boolean isOverlap =
          (Bytes.compareTo(candidate.getKey().getStartKey(), region.getEndKey()) < 0 || isLast)
            && Bytes.compareTo(candidate.getKey().getEndKey(), region.getStartKey()) > 0;
        if (
          isOverlap && EnumSet.of(RegionProgress.ASSIGNED, RegionProgress.FINISHED)
            .contains(candidate.getValue())
        ) {
          if (!candidate.getKey().equals(region)) {
            overlappingAssignments.add(new Pair<>(candidate.getKey(), candidate.getValue()));
          }
          candidate = regions.lowerEntry(candidate.getKey());
        } else {
          candidate = null;
        }
      }

      return overlappingAssignments;
    }

    /**
     * If the region containing the target row has changed, we'll need to backtrack, re-determine
     * the affected region locations, and send RPCs to cover the range that the original region had.
     * In the case that it has merged, multiple requests will be merged into a single request and
     * this serves as a mechanism for leader election to ensure only one set of RPCs are sent for
     * the range.
     * @return true if a new set of RPCs should be sent, false if the region was already handled
     */
    boolean markRegionReplacedExistingAndCheckNeedsToBeRestarted(RegionInfo newRegion) {
      boolean notYetRestarted =
        tryUpdateRegionProgress(newRegion, RegionProgress.NONE, RegionProgress.LOCATED);

      return notYetRestarted;
    }

    void onResponse(RegionInfo region, R response, Throwable failure) {
      RegionProgress progress = regions.getOrDefault(region, RegionProgress.NONE);
      if (progress != RegionProgress.ASSIGNED) {

        callback.onError(
          new DoNotRetryIOException("Received an out-of-order response, expected ASSIGNED but was "
            + progress + ", this should not happen."));
        return;
      }

      if (failure != null) {
        callback.onRegionError(region, failure);
      } else {
        callback.onRegionComplete(region, response);
      }
    }

    void markRegionFinished(RegionInfo region) {
      boolean wasAssigned =
        tryUpdateRegionProgress(region, RegionProgress.ASSIGNED, RegionProgress.FINISHED)
          || tryUpdateRegionProgress(region, RegionProgress.LOCATED, RegionProgress.CANCELLED);
      assert wasAssigned
        : "Region " + region.getEncodedName() + " was marked done but it was in state "
          + regions.get(region) + ". This should not happen.";

      if (!locateFinished.get()) {
        return;
      }
      boolean allRegionsFinished;
      synchronized (regions) {
        allRegionsFinished = EnumSet.of(RegionProgress.FINISHED, RegionProgress.CANCELLED)
          .containsAll(regions.values());
      }
      if (allRegionsFinished) {
        callback.onComplete();
      }
    }

    private boolean tryUpdateRegionProgress(RegionInfo region, RegionProgress prevStatus,
      RegionProgress newStatus) {
      return tryUpdateRegionProgress(region, EnumSet.of(prevStatus), newStatus);
    }

    private boolean tryUpdateRegionProgress(RegionInfo region, EnumSet<RegionProgress> prevStatuses,
      RegionProgress newStatus) {
      synchronized (regions) {
        RegionProgress existingRegionStatus = regions.getOrDefault(region, RegionProgress.NONE);
        if (prevStatuses.contains(existingRegionStatus)) {
          regions.put(region, newStatus);
          return true;
        } else {
          return false;
        }
      }
    }

    enum RegionProgress {
      /**
       * The region is not being tracked.
       */
      NONE,

      /**
       * A region has been identified to send the RPC to, either through the RegionLocator or after
       * receiving a NotServingRegionException (i.e. the actual region is not what was expected, and
       * the newly returned region will be used instead).
       */
      LOCATED,

      /**
       * A region was located, but while attempting to send the RPC it was found that the region no
       * longer exists by an underlying NotServingRegionException. The new region that the RPC
       * should be sent to is tracked and retried.
       */
      CANCELLED,

      /**
       * An RPC call has returned with a response from this region, confirming it is correct.
       * Regions assignments are tracked at the call sites of
       * MultiRegionCoprocessorServiceProgress::onResponse.
       */
      ASSIGNED,

      /**
       * After being assigned, the region has finished processing, either successfully or with an
       * error. No more responses or errors can be accepted from this region.
       */
      FINISHED,
    }
  }
}
