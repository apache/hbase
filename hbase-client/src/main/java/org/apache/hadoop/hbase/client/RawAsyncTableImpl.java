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

import static org.apache.hadoop.hbase.client.ConnectionUtils.checkHasFamilies;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.SingleRequestCallerBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * The implementation of RawAsyncTable.
 */
@InterfaceAudience.Private
class RawAsyncTableImpl implements RawAsyncTable {

  private static final Log LOG = LogFactory.getLog(RawAsyncTableImpl.class);

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  private final int defaultScannerCaching;

  private final long defaultScannerMaxResultSize;

  private long readRpcTimeoutNs;

  private long writeRpcTimeoutNs;

  private long operationTimeoutNs;

  private long scanTimeoutNs;

  public RawAsyncTableImpl(AsyncConnectionImpl conn, TableName tableName) {
    this.conn = conn;
    this.tableName = tableName;
    this.readRpcTimeoutNs = conn.connConf.getReadRpcTimeoutNs();
    this.writeRpcTimeoutNs = conn.connConf.getWriteRpcTimeoutNs();
    this.operationTimeoutNs = tableName.isSystemTable() ? conn.connConf.getMetaOperationTimeoutNs()
        : conn.connConf.getOperationTimeoutNs();
    this.defaultScannerCaching = conn.connConf.getScannerCaching();
    this.defaultScannerMaxResultSize = conn.connConf.getScannerMaxResultSize();
    this.scanTimeoutNs = conn.connConf.getScanTimeoutNs();
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public Configuration getConfiguration() {
    return conn.getConfiguration();
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
      rpcCall.call(stub, controller, reqConvert.convert(loc.getRegionInfo().getRegionName(), req),
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

  private <REQ, RESP> CompletableFuture<RESP> noncedMutate(HBaseRpcController controller,
      HRegionLocation loc, ClientService.Interface stub, REQ req,
      NoncedConverter<MutateRequest, byte[], REQ> reqConvert,
      Converter<RESP, HBaseRpcController, MutateResponse> respConverter) {
    long nonceGroup = conn.getNonceGenerator().getNonceGroup();
    long nonce = conn.getNonceGenerator().newNonce();
    return mutate(controller, loc, stub, req,
      (info, src) -> reqConvert.convert(info, src, nonceGroup, nonce), respConverter);
  }

  private <T> SingleRequestCallerBuilder<T> newCaller(byte[] row, long rpcTimeoutNs) {
    return conn.callerFactory.<T> single().table(tableName).row(row)
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS);
  }

  private <T> SingleRequestCallerBuilder<T> newCaller(Row row, long rpcTimeoutNs) {
    return newCaller(row.getRow(), rpcTimeoutNs);
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    return this.<Result> newCaller(get, readRpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl
            .<Get, GetRequest, GetResponse, Result> call(controller, loc, stub, get,
              RequestConverter::buildGetRequest, (s, c, req, done) -> s.get(c, req, done),
              (c, resp) -> ProtobufUtil.toResult(resp.getResult(), c.cellScanner())))
        .call();
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    return this.<Void> newCaller(put, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.<Put> voidMutate(controller, loc, stub,
          put, RequestConverter::buildMutateRequest))
        .call();
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    return this.<Void> newCaller(delete, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.<Delete> voidMutate(controller, loc,
          stub, delete, RequestConverter::buildMutateRequest))
        .call();
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    checkHasFamilies(append);
    return this.<Result> newCaller(append, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> this.<Append, Result> noncedMutate(controller, loc, stub,
          append, RequestConverter::buildMutateRequest, RawAsyncTableImpl::toResult))
        .call();
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    checkHasFamilies(increment);
    return this.<Result> newCaller(increment, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> this.<Increment, Result> noncedMutate(controller, loc,
          stub, increment, RequestConverter::buildMutateRequest, RawAsyncTableImpl::toResult))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Put put) {
    return this.<Boolean> newCaller(row, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.<Put, Boolean> mutate(controller, loc,
          stub, put,
          (rn, p) -> RequestConverter.buildMutateRequest(rn, row, family, qualifier,
            new BinaryComparator(value), CompareType.valueOf(compareOp.name()), p),
          (c, r) -> r.getProcessed()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Delete delete) {
    return this.<Boolean> newCaller(row, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.<Delete, Boolean> mutate(controller,
          loc, stub, delete,
          (rn, d) -> RequestConverter.buildMutateRequest(rn, row, family, qualifier,
            new BinaryComparator(value), CompareType.valueOf(compareOp.name()), d),
          (c, r) -> r.getProcessed()))
        .call();
  }

  // We need the MultiRequest when constructing the org.apache.hadoop.hbase.client.MultiResponse,
  // so here I write a new method as I do not want to change the abstraction of call method.
  private static <RESP> CompletableFuture<RESP> mutateRow(HBaseRpcController controller,
      HRegionLocation loc, ClientService.Interface stub, RowMutations mutation,
      Converter<MultiRequest, byte[], RowMutations> reqConvert,
      Function<Result, RESP> respConverter) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    try {
      byte[] regionName = loc.getRegionInfo().getRegionName();
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
              Throwable ex = multiResp.getException(regionName);
              if (ex != null) {
                future
                    .completeExceptionally(ex instanceof IOException ? ex
                        : new IOException(
                            "Failed to mutate row: " + Bytes.toStringBinary(mutation.getRow()),
                            ex));
              } else {
                future.complete(respConverter
                    .apply((Result) multiResp.getResults().get(regionName).result.get(0)));
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
  public CompletableFuture<Void> mutateRow(RowMutations mutation) {
    return this.<Void> newCaller(mutation, writeRpcTimeoutNs).action((controller, loc,
        stub) -> RawAsyncTableImpl.<Void> mutateRow(controller, loc, stub, mutation, (rn, rm) -> {
          RegionAction.Builder regionMutationBuilder = RequestConverter.buildRegionAction(rn, rm);
          regionMutationBuilder.setAtomic(true);
          return MultiRequest.newBuilder().addRegionAction(regionMutationBuilder.build()).build();
        }, (resp) -> {
          return null;
        })).call();
  }

  @Override
  public CompletableFuture<Boolean> checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, RowMutations mutation) {
    return this.<Boolean> newCaller(mutation, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> RawAsyncTableImpl.<Boolean> mutateRow(controller, loc,
          stub, mutation,
          (rn, rm) -> RequestConverter.buildMutateRequest(rn, row, family, qualifier,
            new BinaryComparator(value), CompareType.valueOf(compareOp.name()), rm),
          (resp) -> resp.getExists()))
        .call();
  }

  private <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
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
  public CompletableFuture<List<Result>> smallScan(Scan scan, int limit) {
    if (!scan.isSmall()) {
      return failedFuture(new IllegalArgumentException("Only small scan is allowed"));
    }
    if (scan.getBatch() > 0 || scan.getAllowPartialResults()) {
      return failedFuture(
        new IllegalArgumentException("Batch and allowPartial is not allowed for small scan"));
    }
    return conn.callerFactory.smallScan().table(tableName).setScan(setDefaultScanConfig(scan))
        .limit(limit).scanTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS)
        .rpcTimeout(readRpcTimeoutNs, TimeUnit.NANOSECONDS).call();
  }

  public void scan(Scan scan, RawScanResultConsumer consumer) {
    if (scan.isSmall()) {
      if (scan.getBatch() > 0 || scan.getAllowPartialResults()) {
        consumer.onError(
          new IllegalArgumentException("Batch and allowPartial is not allowed for small scan"));
      } else {
        LOG.warn("This is small scan " + scan + ", consider using smallScan directly?");
      }
    }
    scan = setDefaultScanConfig(scan);
    new AsyncClientScanner(scan, consumer, tableName, conn, scanTimeoutNs, readRpcTimeoutNs)
        .start();
  }

  @Override
  public void setReadRpcTimeout(long timeout, TimeUnit unit) {
    this.readRpcTimeoutNs = unit.toNanos(timeout);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return unit.convert(readRpcTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public void setWriteRpcTimeout(long timeout, TimeUnit unit) {
    this.writeRpcTimeoutNs = unit.toNanos(timeout);
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return unit.convert(writeRpcTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public void setOperationTimeout(long timeout, TimeUnit unit) {
    this.operationTimeoutNs = unit.toNanos(timeout);
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return unit.convert(operationTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public void setScanTimeout(long timeout, TimeUnit unit) {
    this.scanTimeoutNs = unit.toNanos(timeout);
  }

  @Override
  public long getScanTimeout(TimeUnit unit) {
    return TimeUnit.NANOSECONDS.convert(scanTimeoutNs, unit);
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> gets) {
    return conn.callerFactory.multiGet().table(tableName).gets(gets)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .rpcTimeout(readRpcTimeoutNs, TimeUnit.NANOSECONDS).call();
  }
}