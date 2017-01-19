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
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningResponse;

/**
 * The implementation of AsyncAdmin.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncHBaseAdmin implements AsyncAdmin {

  private final AsyncConnectionImpl connection;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long pauseNs;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  AsyncHBaseAdmin(AsyncConnectionImpl connection) {
    this.connection = connection;
    this.rpcTimeoutNs = connection.connConf.getRpcTimeoutNs();
    this.operationTimeoutNs = connection.connConf.getOperationTimeoutNs();
    this.pauseNs = connection.connConf.getPauseNs();
    this.maxAttempts = connection.connConf.getMaxRetries();
    this.startLogErrorsCnt = connection.connConf.getStartLogErrorsCnt();
  }

  private <T> MasterRequestCallerBuilder<T> newCaller() {
    return this.connection.callerFactory.<T> masterRequest()
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .pause(pauseNs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt);
  }

  @FunctionalInterface
  private interface RpcCall<RESP, REQ> {
    void call(MasterService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  @FunctionalInterface
  private interface Converter<D, S> {
    D convert(S src) throws IOException;
  }

  private <PREQ, PRESP, RESP> CompletableFuture<RESP> call(HBaseRpcController controller,
      MasterService.Interface stub, PREQ preq, RpcCall<PRESP, PREQ> rpcCall,
      Converter<RESP, PRESP> respConverter) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    rpcCall.call(stub, controller, preq, new RpcCallback<PRESP>() {

      @Override
      public void run(PRESP resp) {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
        } else {
          try {
            future.complete(respConverter.convert(resp));
          } catch (IOException e) {
            future.completeExceptionally(e);
          }
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> listTables() {
    return listTables((Pattern)null, false);
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> listTables(String regex, boolean includeSysTables) {
    return listTables(Pattern.compile(regex), false);
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> listTables(Pattern pattern, boolean includeSysTables) {
    return this
        .<HTableDescriptor[]> newCaller()
        .action(
          (controller, stub) -> this
              .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, HTableDescriptor[]> call(
                controller, stub, RequestConverter.buildGetTableDescriptorsRequest(pattern,
                  includeSysTables), (s, c, req, done) -> s.getTableDescriptors(c, req, done), (
                    resp) -> ProtobufUtil.getHTableDescriptorArray(resp))).call();
  }

  @Override
  public CompletableFuture<TableName[]> listTableNames() {
    return listTableNames((Pattern)null, false);
  }

  @Override
  public CompletableFuture<TableName[]> listTableNames(String regex, boolean includeSysTables) {
    return listTableNames(Pattern.compile(regex), false);
  }

  @Override
  public CompletableFuture<TableName[]> listTableNames(Pattern pattern, boolean includeSysTables) {
    return this
        .<TableName[]> newCaller()
        .action(
          (controller, stub) -> this
              .<GetTableNamesRequest, GetTableNamesResponse, TableName[]> call(controller, stub,
                RequestConverter.buildGetTableNamesRequest(pattern, includeSysTables), (s, c, req,
                    done) -> s.getTableNames(c, req, done), (resp) -> ProtobufUtil
                    .getTableNameArray(resp.getTableNamesList()))).call();
  }

  @Override
  public CompletableFuture<Boolean> setBalancerRunning(final boolean on) {
    return this
        .<Boolean> newCaller()
        .action(
          (controller, stub) -> this
              .<SetBalancerRunningRequest, SetBalancerRunningResponse, Boolean> call(controller,
                stub, RequestConverter.buildSetBalancerRunningRequest(on, true),
                (s, c, req, done) -> s.setBalancerRunning(c, req, done),
                (resp) -> resp.getPrevBalanceValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> balancer() {
    return balancer(false);
  }

  @Override
  public CompletableFuture<Boolean> balancer(boolean force) {
    return this
        .<Boolean> newCaller()
        .action(
          (controller, stub) -> this.<BalanceRequest, BalanceResponse, Boolean> call(controller,
            stub, RequestConverter.buildBalanceRequest(force),
            (s, c, req, done) -> s.balance(c, req, done), (resp) -> resp.getBalancerRan())).call();
  }

  @Override
  public CompletableFuture<Boolean> isBalancerEnabled() {
    return this
        .<Boolean> newCaller()
        .action(
          (controller, stub) -> this.<IsBalancerEnabledRequest, IsBalancerEnabledResponse, Boolean> call(
            controller, stub, RequestConverter.buildIsBalancerEnabledRequest(),
            (s, c, req, done) -> s.isBalancerEnabled(c, req, done), (resp) -> resp.getEnabled()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return AsyncMetaTableAccessor.tableExists(connection, tableName);
  }
}
