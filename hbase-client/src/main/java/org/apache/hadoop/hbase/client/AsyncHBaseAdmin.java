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

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.QueryType;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.AdminRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SplitRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;

/**
 * The implementation of AsyncAdmin.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncHBaseAdmin implements AsyncAdmin {

  private static final Log LOG = LogFactory.getLog(AsyncHBaseAdmin.class);

  private final AsyncConnectionImpl connection;

  private final RawAsyncTable metaTable;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long pauseNs;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private final NonceGenerator ng;

  AsyncHBaseAdmin(AsyncConnectionImpl connection) {
    this.connection = connection;
    this.metaTable = connection.getRawTable(META_TABLE_NAME);
    this.rpcTimeoutNs = connection.connConf.getRpcTimeoutNs();
    this.operationTimeoutNs = connection.connConf.getOperationTimeoutNs();
    this.pauseNs = connection.connConf.getPauseNs();
    this.maxAttempts = connection.connConf.getMaxRetries();
    this.startLogErrorsCnt = connection.connConf.getStartLogErrorsCnt();
    this.ng = connection.getNonceGenerator();
  }

  private <T> MasterRequestCallerBuilder<T> newMasterCaller() {
    return this.connection.callerFactory.<T> masterRequest()
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .pause(pauseNs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt);
  }

  private <T> AdminRequestCallerBuilder<T> newAdminCaller() {
    return this.connection.callerFactory.<T> adminRequest()
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .pause(pauseNs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt);
  }

  @FunctionalInterface
  private interface MasterRpcCall<RESP, REQ> {
    void call(MasterService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  @FunctionalInterface
  private interface AdminRpcCall<RESP, REQ> {
    void call(AdminService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  @FunctionalInterface
  private interface Converter<D, S> {
    D convert(S src) throws IOException;
  }

  private <PREQ, PRESP, RESP> CompletableFuture<RESP> call(HBaseRpcController controller,
      MasterService.Interface stub, PREQ preq, MasterRpcCall<PRESP, PREQ> rpcCall,
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

  //TODO abstract call and adminCall into a single method.
  private <PREQ, PRESP, RESP> CompletableFuture<RESP> adminCall(HBaseRpcController controller,
      AdminService.Interface stub, PREQ preq, AdminRpcCall<PRESP, PREQ> rpcCall,
      Converter<RESP, PRESP> respConverter) {

    CompletableFuture<RESP> future = new CompletableFuture<>();
    rpcCall.call(stub, controller, preq, new RpcCallback<PRESP>() {

      @Override
      public void run(PRESP resp) {
        if (controller.failed()) {
          future.completeExceptionally(new IOException(controller.errorText()));
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

  private <PREQ, PRESP> CompletableFuture<Void> procedureCall(PREQ preq,
      MasterRpcCall<PRESP, PREQ> rpcCall, Converter<Long, PRESP> respConverter,
      ProcedureBiConsumer consumer) {
    CompletableFuture<Long> procFuture = this
        .<Long> newMasterCaller()
        .action(
          (controller, stub) -> this.<PREQ, PRESP, Long> call(controller, stub, preq, rpcCall,
            respConverter)).call();
    return waitProcedureResult(procFuture).whenComplete(consumer);
  }

  @FunctionalInterface
  private interface TableOperator {
    CompletableFuture<Void> operate(TableName table);
  }

  private CompletableFuture<HTableDescriptor[]> batchTableOperations(Pattern pattern,
      TableOperator operator, String operationType) {
    CompletableFuture<HTableDescriptor[]> future = new CompletableFuture<>();
    List<HTableDescriptor> failed = new LinkedList<>();
    listTables(pattern, false).whenComplete(
      (tables, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        CompletableFuture[] futures = Arrays.stream(tables)
            .map((table) -> operator.operate(table.getTableName()).whenComplete((v, ex) -> {
              if (ex != null) {
                LOG.info("Failed to " + operationType + " table " + table.getTableName(), ex);
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
  public AsyncConnectionImpl getConnection() {
    return this.connection;
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return AsyncMetaTableAccessor.tableExists(metaTable, tableName);
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> listTables() {
    return listTables((Pattern) null, false);
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> listTables(String regex, boolean includeSysTables) {
    return listTables(Pattern.compile(regex), false);
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> listTables(Pattern pattern, boolean includeSysTables) {
    return this
        .<HTableDescriptor[]>newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, HTableDescriptor[]> call(
                controller, stub, RequestConverter.buildGetTableDescriptorsRequest(pattern,
                  includeSysTables), (s, c, req, done) -> s.getTableDescriptors(c, req, done), (
                    resp) -> ProtobufUtil.getHTableDescriptorArray(resp))).call();
  }

  @Override
  public CompletableFuture<TableName[]> listTableNames() {
    return listTableNames((Pattern) null, false);
  }

  @Override
  public CompletableFuture<TableName[]> listTableNames(String regex, boolean includeSysTables) {
    return listTableNames(Pattern.compile(regex), false);
  }

  @Override
  public CompletableFuture<TableName[]> listTableNames(Pattern pattern, boolean includeSysTables) {
    return this
        .<TableName[]>newMasterCaller()
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
    this.<List<TableSchema>> newMasterCaller()
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

    return this.<CreateTableRequest, CreateTableResponse> procedureCall(
      RequestConverter.buildCreateTableRequest(desc, splitKeys, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.createTable(c, req, done), (resp) -> resp.getProcId(),
      new CreateTableProcedureBiConsumer(this, desc.getTableName()));
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    return this.<DeleteTableRequest, DeleteTableResponse> procedureCall(RequestConverter
        .buildDeleteTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.deleteTable(c, req, done), (resp) -> resp.getProcId(),
      new DeleteTableProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> deleteTables(String regex) {
    return deleteTables(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> deleteTables(Pattern pattern) {
    return batchTableOperations(pattern, (table) -> deleteTable(table), "DELETE");
  }

  @Override
  public CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits) {
    return this.<TruncateTableRequest, TruncateTableResponse> procedureCall(
      RequestConverter.buildTruncateTableRequest(tableName, preserveSplits, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.truncateTable(c, req, done),
      (resp) -> resp.getProcId(), new TruncateTableProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<Void> enableTable(TableName tableName) {
    return this.<EnableTableRequest, EnableTableResponse> procedureCall(RequestConverter
        .buildEnableTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.enableTable(c, req, done), (resp) -> resp.getProcId(),
      new EnableTableProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> enableTables(String regex) {
    return enableTables(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> enableTables(Pattern pattern) {
    return batchTableOperations(pattern, (table) -> enableTable(table), "ENABLE");
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName tableName) {
    return this.<DisableTableRequest, DisableTableResponse> procedureCall(RequestConverter
        .buildDisableTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.disableTable(c, req, done), (resp) -> resp.getProcId(),
      new DisableTableProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> disableTables(String regex) {
    return disableTables(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<HTableDescriptor[]> disableTables(Pattern pattern) {
    return batchTableOperations(pattern, (table) -> disableTable(table), "DISABLE");
  }


  @Override
  public CompletableFuture<Pair<Integer, Integer>> getAlterStatus(TableName tableName) {
    return this
        .<Pair<Integer, Integer>>newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetSchemaAlterStatusRequest, GetSchemaAlterStatusResponse, Pair<Integer, Integer>> call(
                controller, stub, RequestConverter.buildGetSchemaAlterStatusRequest(tableName), (s,
                    c, req, done) -> s.getSchemaAlterStatus(c, req, done), (resp) -> new Pair<>(
                    resp.getYetToUpdateRegions(), resp.getTotalRegions()))).call();
  }

  @Override
  public CompletableFuture<Void> addColumnFamily(TableName tableName, HColumnDescriptor columnFamily) {
    return this.<AddColumnRequest, AddColumnResponse> procedureCall(
      RequestConverter.buildAddColumnRequest(tableName, columnFamily, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.addColumn(c, req, done), (resp) -> resp.getProcId(),
      new AddColumnFamilyProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<Void> deleteColumnFamily(TableName tableName, byte[] columnFamily) {
    return this.<DeleteColumnRequest, DeleteColumnResponse> procedureCall(
      RequestConverter.buildDeleteColumnRequest(tableName, columnFamily, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.deleteColumn(c, req, done),
      (resp) -> resp.getProcId(), new DeleteColumnFamilyProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(TableName tableName,
      HColumnDescriptor columnFamily) {
    return this.<ModifyColumnRequest, ModifyColumnResponse> procedureCall(
      RequestConverter.buildModifyColumnRequest(tableName, columnFamily, ng.getNonceGroup(),
        ng.newNonce()), (s, c, req, done) -> s.modifyColumn(c, req, done),
      (resp) -> resp.getProcId(), new ModifyColumnFamilyProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<Void> createNamespace(NamespaceDescriptor descriptor) {
    return this.<CreateNamespaceRequest, CreateNamespaceResponse> procedureCall(
      RequestConverter.buildCreateNamespaceRequest(descriptor),
      (s, c, req, done) -> s.createNamespace(c, req, done), (resp) -> resp.getProcId(),
      new CreateNamespaceProcedureBiConsumer(this, descriptor.getName()));
  }

  @Override
  public CompletableFuture<Void> modifyNamespace(NamespaceDescriptor descriptor) {
    return this.<ModifyNamespaceRequest, ModifyNamespaceResponse> procedureCall(
      RequestConverter.buildModifyNamespaceRequest(descriptor),
      (s, c, req, done) -> s.modifyNamespace(c, req, done), (resp) -> resp.getProcId(),
      new ModifyNamespaceProcedureBiConsumer(this, descriptor.getName()));
  }

  @Override
  public CompletableFuture<Void> deleteNamespace(String name) {
    return this.<DeleteNamespaceRequest, DeleteNamespaceResponse> procedureCall(
      RequestConverter.buildDeleteNamespaceRequest(name),
      (s, c, req, done) -> s.deleteNamespace(c, req, done), (resp) -> resp.getProcId(),
      new ModifyNamespaceProcedureBiConsumer(this, name));
  }

  @Override
  public CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(String name) {
    return this
        .<NamespaceDescriptor> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetNamespaceDescriptorRequest, GetNamespaceDescriptorResponse, NamespaceDescriptor> call(
                controller, stub, RequestConverter.buildGetNamespaceDescriptorRequest(name), (s, c,
                    req, done) -> s.getNamespaceDescriptor(c, req, done), (resp) -> ProtobufUtil
                    .toNamespaceDescriptor(resp.getNamespaceDescriptor()))).call();
  }

  @Override
  public CompletableFuture<NamespaceDescriptor[]> listNamespaceDescriptors() {
    return this
        .<NamespaceDescriptor[]> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<ListNamespaceDescriptorsRequest, ListNamespaceDescriptorsResponse, NamespaceDescriptor[]> call(
                controller, stub, ListNamespaceDescriptorsRequest.newBuilder().build(), (s, c, req,
                    done) -> s.listNamespaceDescriptors(c, req, done), (resp) -> ProtobufUtil
                    .getNamespaceDescriptorArray(resp))).call();
  }

  @Override
  public CompletableFuture<Boolean> setBalancerRunning(final boolean on) {
    return this
        .<Boolean>newMasterCaller()
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
        .<Boolean>newMasterCaller()
        .action(
          (controller, stub) -> this.<BalanceRequest, BalanceResponse, Boolean> call(controller,
            stub, RequestConverter.buildBalanceRequest(force),
            (s, c, req, done) -> s.balance(c, req, done), (resp) -> resp.getBalancerRan())).call();
  }

  @Override
  public CompletableFuture<Boolean> isBalancerEnabled() {
    return this
        .<Boolean>newMasterCaller()
        .action(
          (controller, stub) -> this.<IsBalancerEnabledRequest, IsBalancerEnabledResponse, Boolean> call(
            controller, stub, RequestConverter.buildIsBalancerEnabledRequest(),
            (s, c, req, done) -> s.isBalancerEnabled(c, req, done), (resp) -> resp.getEnabled()))
        .call();
  }

  @Override
  public CompletableFuture<Void> closeRegion(String regionname, String serverName) {
    return closeRegion(Bytes.toBytes(regionname), serverName);
  }

  @Override
  public CompletableFuture<Void> closeRegion(byte[] regionName, String serverName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegion(regionName).whenComplete((p, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (p == null || p.getFirst() == null) {
        future.completeExceptionally(new UnknownRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      if (serverName != null) {
        closeRegion(ServerName.valueOf(serverName), p.getFirst()).whenComplete((p2, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          }else{
            future.complete(null);
          }
        });
      } else {
        if (p.getSecond() == null) {
          future.completeExceptionally(new NotServingRegionException(regionName));
        } else {
          closeRegion(p.getSecond(), p.getFirst()).whenComplete((p2, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            }else{
              future.complete(null);
            }
          });
        }
      }
    });
    return future;
  }

  CompletableFuture<Pair<HRegionInfo, ServerName>> getRegion(byte[] regionName) {
    if (regionName == null) {
      return failedFuture(new IllegalArgumentException("Pass region name"));
    }
    CompletableFuture<Pair<HRegionInfo, ServerName>> future = new CompletableFuture<>();
    AsyncMetaTableAccessor.getRegion(metaTable, regionName).whenComplete(
      (p, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else if (p != null) {
          future.complete(p);
        } else {
          metaTable.scanAll(
            new Scan().setReadType(ReadType.PREAD).addFamily(HConstants.CATALOG_FAMILY))
              .whenComplete((results, err2) -> {
                if (err2 != null) {
                  future.completeExceptionally(err2);
                  return;
                }
                String encodedName = Bytes.toString(regionName);
                if (results != null && !results.isEmpty()) {
                  for (Result r : results) {
                    if (r.isEmpty() || MetaTableAccessor.getHRegionInfo(r) == null) continue;
                    RegionLocations rl = MetaTableAccessor.getRegionLocations(r);
                    if (rl != null) {
                      for (HRegionLocation h : rl.getRegionLocations()) {
                        if (h != null && encodedName.equals(h.getRegionInfo().getEncodedName())) {
                          future.complete(new Pair<>(h.getRegionInfo(), h.getServerName()));
                          return;
                        }
                      }
                    }
                  }
                }
                future.complete(null);
              });
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> closeRegionWithEncodedRegionName(String encodedRegionName,
      String serverName) {
    return this
        .<Boolean> newAdminCaller()
        .action(
          (controller, stub) -> this.<CloseRegionRequest, CloseRegionResponse, Boolean> adminCall(
            controller, stub,
            ProtobufUtil.buildCloseRegionRequest(ServerName.valueOf(serverName), encodedRegionName),
            (s, c, req, done) -> s.closeRegion(controller, req, done), (resp) -> resp.getClosed()))
        .serverName(ServerName.valueOf(serverName)).call();
  }

  @Override
  public CompletableFuture<Void> closeRegion(ServerName sn, HRegionInfo hri) {
    return this.<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<CloseRegionRequest, CloseRegionResponse, Void> adminCall(
            controller, stub, ProtobufUtil.buildCloseRegionRequest(sn, hri.getRegionName()),
            (s, c, req, done) -> s.closeRegion(controller, req, done), resp -> null))
        .serverName(sn).call();
  }

  private byte[] toEncodeRegionName(byte[] regionName) {
    try {
      return HRegionInfo.isEncodedRegionName(regionName) ? regionName
          : Bytes.toBytes(HRegionInfo.encodeRegionName(regionName));
    } catch (IOException e) {
      return regionName;
    }
  }

  private void checkAndGetTableName(byte[] encodeRegionName, AtomicReference<TableName> tableName,
      CompletableFuture<TableName> result) {
    getRegion(encodeRegionName).whenComplete((p, err) -> {
      if (err != null) {
        result.completeExceptionally(err);
        return;
      }
      if (p == null) {
        result.completeExceptionally(new UnknownRegionException(
            "Can't invoke merge on unknown region " + Bytes.toStringBinary(encodeRegionName)));
        return;
      }
      if (p.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
        result.completeExceptionally(
          new IllegalArgumentException("Can't invoke merge on non-default regions directly"));
        return;
      }
      if (!tableName.compareAndSet(null, p.getFirst().getTable())) {
        if (!tableName.get().equals(p.getFirst().getTable())) {
          // tables of this two region should be same.
          result.completeExceptionally(
            new IllegalArgumentException("Cannot merge regions from two different tables "
                + tableName.get() + " and " + p.getFirst().getTable()));
        } else {
          result.complete(tableName.get());
        }
      }
    });
  }

  private CompletableFuture<TableName> checkRegionsAndGetTableName(byte[] encodeRegionNameA,
      byte[] encodeRegionNameB) {
    AtomicReference<TableName> tableNameRef = new AtomicReference<>();
    CompletableFuture<TableName> future = new CompletableFuture<>();

    checkAndGetTableName(encodeRegionNameA, tableNameRef, future);
    checkAndGetTableName(encodeRegionNameB, tableNameRef, future);
    return future;
  }

  @Override
  public CompletableFuture<Void> mergeRegions(byte[] nameOfRegionA, byte[] nameOfRegionB,
      boolean forcible) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    final byte[] encodeRegionNameA = toEncodeRegionName(nameOfRegionA);
    final byte[] encodeRegionNameB = toEncodeRegionName(nameOfRegionB);

    checkRegionsAndGetTableName(encodeRegionNameA, encodeRegionNameB)
        .whenComplete((tableName, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }

          MergeTableRegionsRequest request = null;
          try {
            request = RequestConverter.buildMergeTableRegionsRequest(
              new byte[][] { encodeRegionNameA, encodeRegionNameB }, forcible, ng.getNonceGroup(),
              ng.newNonce());
          } catch (DeserializationException e) {
            future.completeExceptionally(e);
            return;
          }

          this.<MergeTableRegionsRequest, MergeTableRegionsResponse> procedureCall(request,
            (s, c, req, done) -> s.mergeTableRegions(c, req, done), (resp) -> resp.getProcId(),
            new MergeTableRegionProcedureBiConsumer(this, tableName)).whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });

        });
    return future;
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    tableExists(tableName).whenComplete((exist, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      if (!exist) {
        future.completeExceptionally(new TableNotFoundException(tableName));
        return;
      }
      metaTable
          .scanAll(new Scan().setReadType(ReadType.PREAD).addFamily(HConstants.CATALOG_FAMILY)
              .withStartRow(MetaTableAccessor.getTableStartRowForMeta(tableName, QueryType.REGION))
              .withStopRow(MetaTableAccessor.getTableStopRowForMeta(tableName, QueryType.REGION)))
          .whenComplete((results, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
              return;
            }
            if (results != null && !results.isEmpty()) {
              List<CompletableFuture<Void>> splitFutures = new ArrayList<>();
              for (Result r : results) {
                if (r.isEmpty() || MetaTableAccessor.getHRegionInfo(r) == null) continue;
                RegionLocations rl = MetaTableAccessor.getRegionLocations(r);
                if (rl != null) {
                  for (HRegionLocation h : rl.getRegionLocations()) {
                    if (h != null && h.getServerName() != null) {
                      HRegionInfo hri = h.getRegionInfo();
                      if (hri == null || hri.isSplitParent()
                          || hri.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID)
                        continue;
                      splitFutures.add(split(h.getServerName(), hri, null));
                    }
                  }
                }
              }
              CompletableFuture
                  .allOf(splitFutures.toArray(new CompletableFuture<?>[splitFutures.size()]))
                  .whenComplete((ret, exception) -> {
                    if (exception != null) {
                      future.completeExceptionally(exception);
                      return;
                    }
                    future.complete(ret);
                  });
            } else {
              future.complete(null);
            }
          });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] regionName) {
    return splitRegion(regionName, null);
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName, byte[] splitPoint) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    if (splitPoint == null) {
      return failedFuture(new IllegalArgumentException("splitPoint can not be null."));
    }
    connection.getRegionLocator(tableName).getRegionLocation(splitPoint)
        .whenComplete((loc, err) -> {
          if (err != null) {
            result.completeExceptionally(err);
          } else if (loc == null || loc.getRegionInfo() == null) {
            result.completeExceptionally(new IllegalArgumentException(
                "Region does not found: rowKey=" + Bytes.toStringBinary(splitPoint)));
          } else {
            splitRegion(loc.getRegionInfo().getRegionName(), splitPoint)
                .whenComplete((ret, err2) -> {
                  if (err2 != null) {
                    result.completeExceptionally(err2);
                  } else {
                    result.complete(ret);
                  }

                });
          }
        });
    return result;
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] regionName, byte[] splitPoint) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegion(regionName).whenComplete((p, err) -> {
      if (p == null) {
        future.completeExceptionally(
          new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName)));
        return;
      }
      if (p.getFirst() != null && p.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
        future.completeExceptionally(new IllegalArgumentException("Can't split replicas directly. "
            + "Replicas are auto-split when their primary is split."));
        return;
      }
      if (p.getSecond() == null) {
        future.completeExceptionally(
          new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      split(p.getSecond(), p.getFirst(), splitPoint).whenComplete((ret, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else {
          future.complete(ret);
        }
      });
    });
    return future;
  }

  @VisibleForTesting
  public CompletableFuture<Void> split(final ServerName sn, final HRegionInfo hri,
      byte[] splitPoint) {
    if (hri.getStartKey() != null && splitPoint != null
        && Bytes.compareTo(hri.getStartKey(), splitPoint) == 0) {
      return failedFuture(
        new IllegalArgumentException("should not give a splitkey which equals to startkey!"));
    }
    return this.<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<SplitRegionRequest, SplitRegionResponse, Void> adminCall(
            controller, stub, ProtobufUtil.buildSplitRegionRequest(hri.getRegionName(), splitPoint),
            (s, c, req, done) -> s.splitRegion(controller, req, done), resp -> null))
        .serverName(sn).call();
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

  private abstract class NamespaceProcedureBiConsumer extends ProcedureBiConsumer {
    protected final String namespaceName;

    NamespaceProcedureBiConsumer(final AsyncAdmin admin, final String namespaceName) {
      super(admin);
      this.namespaceName = namespaceName;
    }

    abstract String getOperationType();

    String getDescription() {
      return "Operation: " + getOperationType() + ", Namespace: " + namespaceName;
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

  private class EnableTableProcedureBiConsumer extends TableProcedureBiConsumer {

    EnableTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "ENABLE";
    }
  }

  private class DisableTableProcedureBiConsumer extends TableProcedureBiConsumer {

    DisableTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "DISABLE";
    }
  }

  private class AddColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    AddColumnFamilyProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "ADD_COLUMN_FAMILY";
    }
  }

  private class DeleteColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    DeleteColumnFamilyProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "DELETE_COLUMN_FAMILY";
    }
  }

  private class ModifyColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    ModifyColumnFamilyProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "MODIFY_COLUMN_FAMILY";
    }
  }

  private class CreateNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    CreateNamespaceProcedureBiConsumer(AsyncAdmin admin, String namespaceName) {
      super(admin, namespaceName);
    }

    String getOperationType() {
      return "CREATE_NAMESPACE";
    }
  }

  private class DeleteNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    DeleteNamespaceProcedureBiConsumer(AsyncAdmin admin, String namespaceName) {
      super(admin, namespaceName);
    }

    String getOperationType() {
      return "DELETE_NAMESPACE";
    }
  }

  private class ModifyNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    ModifyNamespaceProcedureBiConsumer(AsyncAdmin admin, String namespaceName) {
      super(admin, namespaceName);
    }

    String getOperationType() {
      return "MODIFY_NAMESPACE";
    }
  }

  private class MergeTableRegionProcedureBiConsumer extends TableProcedureBiConsumer {

    MergeTableRegionProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    String getOperationType() {
      return "MERGE_REGIONS";
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
    this.<GetProcedureResultResponse> newMasterCaller()
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
