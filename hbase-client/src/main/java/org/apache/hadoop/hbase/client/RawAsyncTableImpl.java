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

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.client.ConnectionUtils.checkHasFamilies;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.client.ConnectionUtils.timelineConsistentRead;
import static org.apache.hadoop.hbase.client.ConnectionUtils.validatePut;
import static org.apache.hadoop.hbase.client.ConnectionUtils.validatePutsInRowMutations;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import com.google.protobuf.RpcChannel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.SingleRequestCallerBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
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

  private final long pauseForCQTBENs;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

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
    if (builder.pauseForCQTBENs < builder.pauseNs) {
      LOG.warn(
        "Configured value of pauseForCQTBENs is {} ms, which is less than" +
          " the normal pause value {} ms, use the greater one instead",
        TimeUnit.NANOSECONDS.toMillis(builder.pauseForCQTBENs),
        TimeUnit.NANOSECONDS.toMillis(builder.pauseNs));
      this.pauseForCQTBENs = builder.pauseNs;
    } else {
      this.pauseForCQTBENs = builder.pauseForCQTBENs;
    }
    this.maxAttempts = builder.maxAttempts;
    this.startLogErrorsCnt = builder.startLogErrorsCnt;
    this.defaultScannerCaching = tableName.isSystemTable() ? conn.connConf.getMetaScannerCaching()
      : conn.connConf.getScannerCaching();
    this.defaultScannerMaxResultSize = conn.connConf.getScannerMaxResultSize();
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

  @FunctionalInterface
  private interface Converter<D, I, S> {
    D convert(I info, S src) throws IOException;
  }

  @FunctionalInterface
  private interface RpcCall<RESP, REQ> {
    void call(ClientService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  private static <REQ, PREQ, PRESP, RESP> CompletableFuture<RESP> call(
      HBaseRpcController controller, HRegionLocation loc, ClientService.Interface stub, REQ req,
      Converter<PREQ, byte[], REQ> reqConvert, RpcCall<PRESP, PREQ> rpcCall,
      Converter<RESP, HBaseRpcController, PRESP> respConverter) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    try {
      rpcCall.call(stub, controller, reqConvert.convert(loc.getRegion().getRegionName(), req),
        new RpcCallback<PRESP>() {

          @Override
          public void run(PRESP resp) {
            if (controller.failed()) {
              future.completeExceptionally(controller.getFailed());
            } else {
              try {
                future.complete(respConverter.convert(controller, resp));
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

  private static <REQ, RESP> CompletableFuture<RESP> mutate(HBaseRpcController controller,
      HRegionLocation loc, ClientService.Interface stub, REQ req,
      Converter<MutateRequest, byte[], REQ> reqConvert,
      Converter<RESP, HBaseRpcController, MutateResponse> respConverter) {
    return call(controller, loc, stub, req, reqConvert, (s, c, r, done) -> s.mutate(c, r, done),
      respConverter);
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
      .pause(pauseNs, TimeUnit.NANOSECONDS).pauseForCQTBE(pauseForCQTBENs, TimeUnit.NANOSECONDS)
      .maxAttempts(maxAttempts).startLogErrorsCnt(startLogErrorsCnt);
  }

  private <T, R extends OperationWithAttributes & Row> SingleRequestCallerBuilder<T> newCaller(
      R row, long rpcTimeoutNs) {
    return newCaller(row.getRow(), row.getPriority(), rpcTimeoutNs);
  }

  private CompletableFuture<Result> get(Get get, int replicaId) {
    return this.<Result, Get> newCaller(get, readRpcTimeoutNs)
      .action((controller, loc, stub) -> RawAsyncTableImpl
        .<Get, GetRequest, GetResponse, Result> call(controller, loc, stub, get,
          RequestConverter::buildGetRequest, (s, c, req, done) -> s.get(c, req, done),
          (c, resp) -> ProtobufUtil.toResult(resp.getResult(), c.cellScanner())))
      .replicaId(replicaId).call();
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    return timelineConsistentRead(conn.getLocator(), tableName, get, get.getRow(),
      RegionLocateType.CURRENT, replicaId -> get(get, replicaId), readRpcTimeoutNs,
      conn.connConf.getPrimaryCallTimeoutNs(), retryTimer, conn.getConnectionMetrics());
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    validatePut(put, conn.connConf.getMaxKeyValueSize());
    return this.<Void, Put> newCaller(put, writeRpcTimeoutNs)
      .action((controller, loc, stub) -> RawAsyncTableImpl.<Put> voidMutate(controller, loc, stub,
        put, RequestConverter::buildMutateRequest))
      .call();
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    return this.<Void, Delete> newCaller(delete, writeRpcTimeoutNs)
      .action((controller, loc, stub) -> RawAsyncTableImpl.<Delete> voidMutate(controller, loc,
        stub, delete, RequestConverter::buildMutateRequest))
      .call();
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    checkHasFamilies(append);
    long nonceGroup = conn.getNonceGenerator().getNonceGroup();
    long nonce = conn.getNonceGenerator().newNonce();
    return this.<Result, Append> newCaller(append, rpcTimeoutNs)
      .action(
        (controller, loc, stub) -> this.<Append, Result> noncedMutate(nonceGroup, nonce, controller,
          loc, stub, append, RequestConverter::buildMutateRequest, RawAsyncTableImpl::toResult))
      .call();
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    checkHasFamilies(increment);
    long nonceGroup = conn.getNonceGenerator().getNonceGroup();
    long nonce = conn.getNonceGenerator().newNonce();
    return this.<Result, Increment> newCaller(increment, rpcTimeoutNs)
      .action((controller, loc, stub) -> this.<Increment, Result> noncedMutate(nonceGroup, nonce,
        controller, loc, stub, increment, RequestConverter::buildMutateRequest,
        RawAsyncTableImpl::toResult))
      .call();
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
    public CompletableFuture<Boolean> thenPut(Put put) {
      validatePut(put, conn.connConf.getMaxKeyValueSize());
      preCheck();
      return RawAsyncTableImpl.this.<Boolean> newCaller(row, put.getPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller, loc,
          stub, put,
          (rn, p) -> RequestConverter.buildMutateRequest(rn, row, family, qualifier, op, value,
            null, timeRange, p, HConstants.NO_NONCE, HConstants.NO_NONCE),
          (c, r) -> r.getProcessed()))
        .call();
    }

    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      preCheck();
      return RawAsyncTableImpl.this.<Boolean> newCaller(row, delete.getPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller,
          loc, stub, delete,
          (rn, d) -> RequestConverter.buildMutateRequest(rn, row, family, qualifier, op, value,
            null, timeRange, d, HConstants.NO_NONCE, HConstants.NO_NONCE),
          (c, r) -> r.getProcessed()))
        .call();
    }

    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations mutation) {
      preCheck();
      validatePutsInRowMutations(mutation, conn.connConf.getMaxKeyValueSize());
      return RawAsyncTableImpl.this.<Boolean> newCaller(row, mutation.getMaxPriority(),
        rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.this.mutateRow(controller,
          loc, stub, mutation,
          (rn, rm) -> RequestConverter.buildMultiRequest(rn, row, family, qualifier, op, value,
            null, timeRange, rm, HConstants.NO_NONCE, HConstants.NO_NONCE),
          CheckAndMutateResult::isSuccess))
        .call();
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
      return RawAsyncTableImpl.this.<Boolean> newCaller(row, put.getPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller, loc,
          stub, put,
          (rn, p) -> RequestConverter.buildMutateRequest(rn, row, null, null, null, null,
            filter, timeRange, p, HConstants.NO_NONCE, HConstants.NO_NONCE),
          (c, r) -> r.getProcessed()))
        .call();
    }

    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      return RawAsyncTableImpl.this.<Boolean> newCaller(row, delete.getPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller,
          loc, stub, delete,
          (rn, d) -> RequestConverter.buildMutateRequest(rn, row, null, null, null, null,
            filter, timeRange, d, HConstants.NO_NONCE, HConstants.NO_NONCE),
          (c, r) -> r.getProcessed()))
        .call();
    }

    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations mutation) {
      validatePutsInRowMutations(mutation, conn.connConf.getMaxKeyValueSize());
      return RawAsyncTableImpl.this.<Boolean> newCaller(row, mutation.getMaxPriority(),
        rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.this.mutateRow(controller,
          loc, stub, mutation,
          (rn, rm) -> RequestConverter.buildMultiRequest(rn, row, null, null, null, null,
            filter, timeRange, rm, HConstants.NO_NONCE, HConstants.NO_NONCE),
          CheckAndMutateResult::isSuccess))
        .call();
    }
  }

  @Override
  public CheckAndMutateWithFilterBuilder checkAndMutate(byte[] row, Filter filter) {
    return new CheckAndMutateWithFilterBuilderImpl(row, filter);
  }

  @Override
  public CompletableFuture<CheckAndMutateResult> checkAndMutate(CheckAndMutate checkAndMutate) {
    if (checkAndMutate.getAction() instanceof Put || checkAndMutate.getAction() instanceof Delete
      || checkAndMutate.getAction() instanceof Increment
      || checkAndMutate.getAction() instanceof Append) {
      Mutation mutation = (Mutation) checkAndMutate.getAction();
      if (mutation instanceof Put) {
        validatePut((Put) mutation, conn.connConf.getMaxKeyValueSize());
      }
      long nonceGroup = conn.getNonceGenerator().getNonceGroup();
      long nonce = conn.getNonceGenerator().newNonce();
      return RawAsyncTableImpl.this.<CheckAndMutateResult> newCaller(checkAndMutate.getRow(),
        mutation.getPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.mutate(controller,
          loc, stub, mutation,
          (rn, m) -> RequestConverter.buildMutateRequest(rn, checkAndMutate.getRow(),
            checkAndMutate.getFamily(), checkAndMutate.getQualifier(),
            checkAndMutate.getCompareOp(), checkAndMutate.getValue(), checkAndMutate.getFilter(),
            checkAndMutate.getTimeRange(), m, nonceGroup, nonce),
          (c, r) -> ResponseConverter.getCheckAndMutateResult(r, c.cellScanner())))
        .call();
    } else if (checkAndMutate.getAction() instanceof RowMutations) {
      RowMutations rowMutations = (RowMutations) checkAndMutate.getAction();
      validatePutsInRowMutations(rowMutations, conn.connConf.getMaxKeyValueSize());
      long nonceGroup = conn.getNonceGenerator().getNonceGroup();
      long nonce = conn.getNonceGenerator().newNonce();
      return RawAsyncTableImpl.this.<CheckAndMutateResult> newCaller(checkAndMutate.getRow(),
        rowMutations.getMaxPriority(), rpcTimeoutNs)
        .action((controller, loc, stub) ->
          RawAsyncTableImpl.this.<CheckAndMutateResult, CheckAndMutateResult> mutateRow(
            controller, loc, stub, rowMutations,
            (rn, rm) -> RequestConverter.buildMultiRequest(rn, checkAndMutate.getRow(),
            checkAndMutate.getFamily(), checkAndMutate.getQualifier(),
            checkAndMutate.getCompareOp(), checkAndMutate.getValue(), checkAndMutate.getFilter(),
            checkAndMutate.getTimeRange(), rm, nonceGroup, nonce),
            resp -> resp))
        .call();
    } else {
      CompletableFuture<CheckAndMutateResult> future = new CompletableFuture<>();
      future.completeExceptionally(new DoNotRetryIOException(
        "CheckAndMutate doesn't support " + checkAndMutate.getAction().getClass().getName()));
      return future;
    }
  }

  @Override
  public List<CompletableFuture<CheckAndMutateResult>> checkAndMutate(
    List<CheckAndMutate> checkAndMutates) {
    return batch(checkAndMutates, rpcTimeoutNs).stream()
      .map(f -> f.thenApply(r -> (CheckAndMutateResult) r)).collect(toList());
  }

  // We need the MultiRequest when constructing the org.apache.hadoop.hbase.client.MultiResponse,
  // so here I write a new method as I do not want to change the abstraction of call method.
  @SuppressWarnings("unchecked")
  private <RES, RESP> CompletableFuture<RESP> mutateRow(HBaseRpcController controller,
      HRegionLocation loc, ClientService.Interface stub, RowMutations mutation,
      Converter<MultiRequest, byte[], RowMutations> reqConvert,
      Function<RES, RESP> respConverter) {
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
                future.completeExceptionally(ex instanceof IOException ? ex
                  : new IOException(
                    "Failed to mutate row: " + Bytes.toStringBinary(mutation.getRow()), ex));
              } else {
                future.complete(respConverter
                  .apply((RES) multiResp.getResults().get(regionName).result.get(0)));
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
    return this.<Result> newCaller(mutations.getRow(), mutations.getMaxPriority(),
      writeRpcTimeoutNs).action((controller, loc, stub) ->
        this.<Result, Result> mutateRow(controller, loc, stub, mutations,
          (rn, rm) -> RequestConverter.buildMultiRequest(rn, rm, nonceGroup, nonce),
          resp -> resp))
      .call();
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
      pauseNs, pauseForCQTBENs, maxAttempts, scanTimeoutNs, readRpcTimeoutNs, startLogErrorsCnt)
        .start();
  }

  private long resultSize2CacheSize(long maxResultSize) {
    // * 2 if possible
    return maxResultSize > Long.MAX_VALUE / 2 ? maxResultSize : maxResultSize * 2;
  }

  @Override
  public ResultScanner getScanner(Scan scan) {
    return new AsyncTableResultScanner(this, ReflectionUtils.newInstance(scan.getClass(), scan),
      resultSize2CacheSize(
        scan.getMaxResultSize() > 0 ? scan.getMaxResultSize() : defaultScannerMaxResultSize));
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
    return batch(gets, readRpcTimeoutNs);
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> puts) {
    return voidMutate(puts);
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    return voidMutate(deletes);
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    return batch(actions, rpcTimeoutNs);
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
      .pauseForCQTBE(pauseForCQTBENs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
      .startLogErrorsCnt(startLogErrorsCnt).call();
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

  private <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, RegionInfo region, byte[] row) {
    RegionCoprocessorRpcChannelImpl channel = new RegionCoprocessorRpcChannelImpl(conn, tableName,
      region, row, rpcTimeoutNs, operationTimeoutNs);
    S stub = stubMaker.apply(channel);
    CompletableFuture<R> future = new CompletableFuture<>();
    ClientCoprocessorRpcController controller = new ClientCoprocessorRpcController();
    callable.call(stub, controller, resp -> {
      if (controller.failed()) {
        future.completeExceptionally(controller.getFailed());
      } else {
        future.complete(resp);
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
      return c < 0 || c == 0 && !endKeyInclusive;
    }
  }

  private <S, R> void onLocateComplete(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, CoprocessorCallback<R> callback, List<HRegionLocation> locs,
      byte[] endKey, boolean endKeyInclusive, AtomicBoolean locateFinished,
      AtomicInteger unfinishedRequest, HRegionLocation loc, Throwable error) {
    if (error != null) {
      callback.onError(error);
      return;
    }
    unfinishedRequest.incrementAndGet();
    RegionInfo region = loc.getRegion();
    if (locateFinished(region, endKey, endKeyInclusive)) {
      locateFinished.set(true);
    } else {
      addListener(
        conn.getLocator().getRegionLocation(tableName, region.getEndKey(), RegionLocateType.CURRENT,
          operationTimeoutNs),
        (l, e) -> onLocateComplete(stubMaker, callable, callback, locs, endKey, endKeyInclusive,
          locateFinished, unfinishedRequest, l, e));
    }
    addListener(coprocessorService(stubMaker, callable, region, region.getStartKey()), (r, e) -> {
      if (e != null) {
        callback.onRegionError(region, e);
      } else {
        callback.onRegionComplete(region, r);
      }
      if (unfinishedRequest.decrementAndGet() == 0 && locateFinished.get()) {
        callback.onComplete();
      }
    });
  }

  private final class CoprocessorServiceBuilderImpl<S, R>
      implements CoprocessorServiceBuilder<S, R> {

    private final Function<RpcChannel, S> stubMaker;

    private final ServiceCaller<S, R> callable;

    private final CoprocessorCallback<R> callback;

    private byte[] startKey = HConstants.EMPTY_START_ROW;

    private boolean startKeyInclusive;

    private byte[] endKey = HConstants.EMPTY_END_ROW;

    private boolean endKeyInclusive;

    public CoprocessorServiceBuilderImpl(Function<RpcChannel, S> stubMaker,
        ServiceCaller<S, R> callable, CoprocessorCallback<R> callback) {
      this.stubMaker = Preconditions.checkNotNull(stubMaker, "stubMaker is null");
      this.callable = Preconditions.checkNotNull(callable, "callable is null");
      this.callback = Preconditions.checkNotNull(callback, "callback is null");
    }

    @Override
    public CoprocessorServiceBuilderImpl<S, R> fromRow(byte[] startKey, boolean inclusive) {
      this.startKey = Preconditions.checkNotNull(startKey,
        "startKey is null. Consider using" +
          " an empty byte array, or just do not call this method if you want to start selection" +
          " from the first region");
      this.startKeyInclusive = inclusive;
      return this;
    }

    @Override
    public CoprocessorServiceBuilderImpl<S, R> toRow(byte[] endKey, boolean inclusive) {
      this.endKey = Preconditions.checkNotNull(endKey,
        "endKey is null. Consider using" +
          " an empty byte array, or just do not call this method if you want to continue" +
          " selection to the last region");
      this.endKeyInclusive = inclusive;
      return this;
    }

    @Override
    public void execute() {
      addListener(conn.getLocator().getRegionLocation(tableName, startKey,
        startKeyInclusive ? RegionLocateType.CURRENT : RegionLocateType.AFTER, operationTimeoutNs),
        (loc, error) -> onLocateComplete(stubMaker, callable, callback, new ArrayList<>(), endKey,
          endKeyInclusive, new AtomicBoolean(false), new AtomicInteger(0), loc, error));
    }
  }

  @Override
  public <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(
      Function<RpcChannel, S> stubMaker, ServiceCaller<S, R> callable,
      CoprocessorCallback<R> callback) {
    return new CoprocessorServiceBuilderImpl<>(stubMaker, callable, callback);
  }
}
