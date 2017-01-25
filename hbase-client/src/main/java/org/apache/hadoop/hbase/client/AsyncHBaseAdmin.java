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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;

/**
 * The implementation of AsyncAdmin.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncHBaseAdmin implements AsyncAdmin {

  private static final Log LOG = LogFactory.getLog(AsyncHBaseAdmin.class);

  private final AsyncConnectionImpl connection;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long pauseNs;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private final NonceGenerator ng;

  AsyncHBaseAdmin(AsyncConnectionImpl connection) {
    this.connection = connection;
    this.rpcTimeoutNs = connection.connConf.getRpcTimeoutNs();
    this.operationTimeoutNs = connection.connConf.getOperationTimeoutNs();
    this.pauseNs = connection.connConf.getPauseNs();
    this.maxAttempts = connection.connConf.getMaxRetries();
    this.startLogErrorsCnt = connection.connConf.getStartLogErrorsCnt();
    this.ng = connection.getNonceGenerator();
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
  public AsyncConnectionImpl getConnection() {
    return this.connection;
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return AsyncMetaTableAccessor.tableExists(connection, tableName);
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
  public CompletableFuture<HTableDescriptor> getTableDescriptor(TableName tableName) {
    CompletableFuture<HTableDescriptor> future = new CompletableFuture<>();
    this.<List<TableSchema>> newCaller()
        .action(
          (controller, stub) -> this
              .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, List<TableSchema>> call(
                controller, stub, RequestConverter.buildGetTableDescriptorsRequest(tableName), (s,
                    c, req, done) -> s.getTableDescriptors(c, req, done), (resp) -> resp
                    .getTableSchemaList())).call().whenComplete((tableSchemas, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
            return;
          }
          if (!tableSchemas.isEmpty()) {
            future.complete(ProtobufUtil.convertToHTableDesc(tableSchemas.get(0)));
          } else {
            future.completeExceptionally(new TableNotFoundException(tableName.getNameAsString()));
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> createTable(HTableDescriptor desc) {
    return createTable(desc, null);
  }

  @Override
  public CompletableFuture<Void> createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    try {
      return createTable(desc, getSplitKeys(startKey, endKey, numRegions));
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> createTable(HTableDescriptor desc, byte[][] splitKeys) {
    if (desc.getTableName() == null) {
      return failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }
    if (splitKeys != null && splitKeys.length > 0) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
          return failedFuture(new IllegalArgumentException(
              "Empty split key must not be passed in the split keys."));
        }
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          return failedFuture(new IllegalArgumentException("All split keys must be unique, "
              + "found duplicate: " + Bytes.toStringBinary(splitKey) + ", "
              + Bytes.toStringBinary(lastKey)));
        }
        lastKey = splitKey;
      }
    }

    CompletableFuture<Long> procFuture = this
        .<Long> newCaller()
        .action(
          (controller, stub) -> this.<CreateTableRequest, CreateTableResponse, Long> call(
            controller,
            stub,
            RequestConverter.buildCreateTableRequest(desc, splitKeys, ng.getNonceGroup(),
              ng.newNonce()), (s, c, req, done) -> s.createTable(c, req, done),
            (resp) -> resp.getProcId())).call();
    return waitProcedureResult(procFuture).whenComplete(
      new CreateTableProcedureBiConsumer(this, desc.getTableName()));
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    CompletableFuture<Long> procFuture = this
        .<Long> newCaller()
        .action(
          (controller, stub) -> this.<DeleteTableRequest, DeleteTableResponse, Long> call(
            controller, stub,
            RequestConverter.buildDeleteTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
            (s, c, req, done) -> s.deleteTable(c, req, done), (resp) -> resp.getProcId())).call();
    return waitProcedureResult(procFuture).whenComplete(
      new DeleteTableProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> deleteTables(String regex) {
    return deleteTables(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> deleteTables(Pattern pattern) {
    CompletableFuture<HTableDescriptor[]> future = new CompletableFuture<>();
    List<HTableDescriptor> failed = new LinkedList<>();
    listTables(pattern, false).whenComplete(
      (tables, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        CompletableFuture[] futures = Arrays.stream(tables)
            .map((table) -> deleteTable(table.getTableName()).whenComplete((v, ex) -> {
              if (ex != null) {
                LOG.info("Failed to delete table " + table.getTableName(), ex);
                failed.add(table);
              }
            })).toArray(size -> new CompletableFuture[size]);
        CompletableFuture.allOf(futures).thenAccept((v) -> {
          future.complete(failed.toArray(new HTableDescriptor[failed.size()]));
        });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits) {
    CompletableFuture<Long> procFuture = this
        .<Long> newCaller()
        .action(
          (controller, stub) -> this.<TruncateTableRequest, TruncateTableResponse, Long> call(
            controller,
            stub,
            RequestConverter.buildTruncateTableRequest(tableName, preserveSplits,
              ng.getNonceGroup(), ng.newNonce()),
            (s, c, req, done) -> s.truncateTable(c, req, done), (resp) -> resp.getProcId())).call();
    return waitProcedureResult(procFuture).whenComplete(
      new TruncateTableProcedureBiConsumer(this, tableName));
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

  private byte[][] getSplitKeys(byte[] startKey, byte[] endKey, int numRegions) {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      return new byte[][] { startKey, endKey };
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if (splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    return splitKeys;
  }

  private abstract class ProcedureBiConsumer implements BiConsumer<Void, Throwable> {
    protected final AsyncAdmin admin;

    ProcedureBiConsumer(AsyncAdmin admin) {
      this.admin = admin;
    }

    abstract void onFinished();

    abstract void onError(Throwable error);

    @Override
    public void accept(Void v, Throwable error) {
      if (error != null) {
        onError(error);
        return;
      }
      onFinished();
    }
  }

  private abstract class TableProcedureBiConsumer extends ProcedureBiConsumer {
    protected final TableName tableName;

    TableProcedureBiConsumer(final AsyncAdmin admin, final TableName tableName) {
      super(admin);
      this.tableName = tableName;
    }

    abstract String getOperationType();

    String getDescription() {
      return "Operation: " + getOperationType() + ", " + "Table Name: "
          + tableName.getNameWithNamespaceInclAsString();
    }

    @Override
    void onFinished() {
      LOG.info(getDescription() + " completed");
    }

    @Override
    void onError(Throwable error) {
      LOG.info(getDescription() + " failed with " + error.getMessage());
    }
  }

  private class CreateTableProcedureBiConsumer extends TableProcedureBiConsumer {

    CreateTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "CREATE";
    }
  }

  private class DeleteTableProcedureBiConsumer extends TableProcedureBiConsumer {

    DeleteTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "DELETE";
    }

    @Override
    void onFinished() {
      this.admin.getConnection().getLocator().clearCache(this.tableName);
      super.onFinished();
    }
  }

  private class TruncateTableProcedureBiConsumer extends TableProcedureBiConsumer {

    TruncateTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "TRUNCATE";
    }
  }

  private CompletableFuture<Void> waitProcedureResult(CompletableFuture<Long> procFuture) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    procFuture.whenComplete((procId, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      getProcedureResult(procId, future);
    });
    return future;
  }

  private void getProcedureResult(final long procId, CompletableFuture<Void> future) {
    this.<GetProcedureResultResponse> newCaller()
        .action(
          (controller, stub) -> this
              .<GetProcedureResultRequest, GetProcedureResultResponse, GetProcedureResultResponse> call(
                controller, stub, GetProcedureResultRequest.newBuilder().setProcId(procId).build(),
                (s, c, req, done) -> s.getProcedureResult(c, req, done), (resp) -> resp))
        .call()
        .whenComplete(
          (response, error) -> {
            if (error != null) {
              LOG.warn("failed to get the procedure result procId=" + procId,
                ConnectionUtils.translateException(error));
              connection.RETRY_TIMER.newTimeout(t -> getProcedureResult(procId, future), pauseNs,
                TimeUnit.NANOSECONDS);
              return;
            }
            if (response.getState() == GetProcedureResultResponse.State.RUNNING) {
              connection.RETRY_TIMER.newTimeout(t -> getProcedureResult(procId, future), pauseNs,
                TimeUnit.NANOSECONDS);
              return;
            }
            if (response.hasException()) {
              IOException ioe = ForeignExceptionUtil.toIOException(response.getException());
              future.completeExceptionally(ioe);
            } else {
              future.complete(null);
            }
          });
  }

  private <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }
}
