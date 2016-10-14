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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.SingleRequestCallerBuilder;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * The implementation of AsyncTable.
 */
@InterfaceAudience.Private
class AsyncTableImpl implements AsyncTable {

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  private long readRpcTimeoutNs;

  private long writeRpcTimeoutNs;

  private long operationTimeoutNs;

  public AsyncTableImpl(AsyncConnectionImpl conn, TableName tableName) {
    this.conn = conn;
    this.tableName = tableName;
    this.readRpcTimeoutNs = conn.connConf.getReadRpcTimeoutNs();
    this.writeRpcTimeoutNs = conn.connConf.getWriteRpcTimeoutNs();
    this.operationTimeoutNs = tableName.isSystemTable() ? conn.connConf.getMetaOperationTimeoutNs()
        : conn.connConf.getOperationTimeoutNs();
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

  private <T> SingleRequestCallerBuilder<T> newCaller(Row row, long rpcTimeoutNs) {
    return conn.callerFactory.<T> single().table(tableName).row(row.getRow())
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    if (!get.isCheckExistenceOnly()) {
      get = ReflectionUtils.newInstance(get.getClass(), get);
      get.setCheckExistenceOnly(true);
    }
    return get(get).thenApply(r -> r.getExists());
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    return this.<Result> newCaller(get, readRpcTimeoutNs)
        .action((controller, loc, stub) -> AsyncTableImpl
            .<Get, GetRequest, GetResponse, Result> call(controller, loc, stub, get,
              RequestConverter::buildGetRequest, (s, c, req, done) -> s.get(c, req, done),
              (c, resp) -> ProtobufUtil.toResult(resp.getResult(), c.cellScanner())))
        .call();
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    return this.<Void> newCaller(put, writeRpcTimeoutNs)
        .action(
          (controller, loc, stub) -> AsyncTableImpl.<Put, MutateRequest, MutateResponse, Void> call(
            controller, loc, stub, put, RequestConverter::buildMutateRequest,
            (s, c, req, done) -> s.mutate(c, req, done), (c, resp) -> {
              return null;
            }))
        .call();
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    return this.<Void> newCaller(delete, writeRpcTimeoutNs)
        .action((controller, loc, stub) -> AsyncTableImpl
            .<Delete, MutateRequest, MutateResponse, Void> call(controller, loc, stub, delete,
              RequestConverter::buildMutateRequest, (s, c, req, done) -> s.mutate(c, req, done),
              (c, resp) -> {
                return null;
              }))
        .call();
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
}