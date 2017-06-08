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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.QueryType;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.AdminRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SplitRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;

/**
 * The implementation of AsyncAdmin.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncHBaseAdmin implements AsyncAdmin {
  public static final String FLUSH_TABLE_PROCEDURE_SIGNATURE = "flush-table-proc";

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

  private CompletableFuture<TableDescriptor[]> batchTableOperations(Pattern pattern,
      TableOperator operator, String operationType) {
    CompletableFuture<TableDescriptor[]> future = new CompletableFuture<>();
    List<TableDescriptor> failed = new LinkedList<>();
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
            })).<CompletableFuture> toArray(size -> new CompletableFuture[size]);
        CompletableFuture.allOf(futures).thenAccept((v) -> {
          future.complete(failed.toArray(new TableDescriptor[failed.size()]));
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
  public CompletableFuture<TableDescriptor[]> listTables() {
    return listTables((Pattern) null, false);
  }

  @Override
  public CompletableFuture<TableDescriptor[]> listTables(String regex, boolean includeSysTables) {
    return listTables(Pattern.compile(regex), false);
  }

  @Override
  public CompletableFuture<TableDescriptor[]> listTables(Pattern pattern, boolean includeSysTables) {
    return this
        .<TableDescriptor[]>newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, TableDescriptor[]> call(
                controller, stub, RequestConverter.buildGetTableDescriptorsRequest(pattern,
                  includeSysTables), (s, c, req, done) -> s.getTableDescriptors(c, req, done), (
                    resp) -> ProtobufUtil.getTableDescriptorArray(resp))).call();
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
  public CompletableFuture<TableDescriptor> getTableDescriptor(TableName tableName) {
    CompletableFuture<TableDescriptor> future = new CompletableFuture<>();
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
            future.complete(ProtobufUtil.convertToTableDesc(tableSchemas.get(0)));
          } else {
            future.completeExceptionally(new TableNotFoundException(tableName.getNameAsString()));
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc) {
    return createTable(desc, null);
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    try {
      return createTable(desc, getSplitKeys(startKey, endKey, numRegions));
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[][] splitKeys) {
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
  public CompletableFuture<TableDescriptor[]> deleteTables(String regex) {
    return deleteTables(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<TableDescriptor[]> deleteTables(Pattern pattern) {
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
  public CompletableFuture<TableDescriptor[]> enableTables(String regex) {
    return enableTables(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<TableDescriptor[]> enableTables(Pattern pattern) {
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
  public CompletableFuture<TableDescriptor[]> disableTables(String regex) {
    return disableTables(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<TableDescriptor[]> disableTables(Pattern pattern) {
    return batchTableOperations(pattern, (table) -> disableTable(table), "DISABLE");
  }


  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    AsyncMetaTableAccessor.getTableState(metaTable, tableName).whenComplete((state, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      if (state.isPresent()) {
        future.complete(state.get().inStates(TableState.State.ENABLED));
      } else {
        future.completeExceptionally(new TableNotFoundException(tableName));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    AsyncMetaTableAccessor.getTableState(metaTable, tableName).whenComplete((state, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      if (state.isPresent()) {
        future.complete(state.get().inStates(TableState.State.DISABLED));
      } else {
        future.completeExceptionally(new TableNotFoundException(tableName));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName) {
    return isTableAvailable(tableName, null);
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName, byte[][] splitKeys) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    isTableEnabled(tableName).whenComplete(
      (enabled, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        if (!enabled) {
          future.complete(false);
        } else {
          AsyncMetaTableAccessor.getTableRegionsAndLocations(metaTable, Optional.of(tableName))
              .whenComplete(
                (locations, error1) -> {
                  if (error1 != null) {
                    future.completeExceptionally(error1);
                    return;
                  }
                  int notDeployed = 0;
                  int regionCount = 0;
                  for (Pair<HRegionInfo, ServerName> pair : locations) {
                    HRegionInfo info = pair.getFirst();
                    if (pair.getSecond() == null) {
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Table " + tableName + " has not deployed region "
                            + pair.getFirst().getEncodedName());
                      }
                      notDeployed++;
                    } else if (splitKeys != null
                        && !Bytes.equals(info.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
                      for (byte[] splitKey : splitKeys) {
                        // Just check if the splitkey is available
                        if (Bytes.equals(info.getStartKey(), splitKey)) {
                          regionCount++;
                          break;
                        }
                      }
                    } else {
                      // Always empty start row should be counted
                      regionCount++;
                    }
                  }
                  if (notDeployed > 0) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Table " + tableName + " has " + notDeployed + " regions");
                    }
                    future.complete(false);
                  } else if (splitKeys != null && regionCount != splitKeys.length + 1) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Table " + tableName + " expected to have "
                          + (splitKeys.length + 1) + " regions, but only " + regionCount
                          + " available");
                    }
                    future.complete(false);
                  } else {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Table " + tableName + " should be available");
                    }
                    future.complete(true);
                  }
                });
        }
      });
    return future;
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
  public CompletableFuture<Void> addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily) {
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
      ColumnFamilyDescriptor columnFamily) {
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
      new DeleteNamespaceProcedureBiConsumer(this, name));
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

  @Override
  public CompletableFuture<List<HRegionInfo>> getOnlineRegions(ServerName sn) {
    return this.<List<HRegionInfo>> newAdminCaller()
        .action((controller, stub) -> this
            .<GetOnlineRegionRequest, GetOnlineRegionResponse, List<HRegionInfo>> adminCall(
              controller, stub, RequestConverter.buildGetOnlineRegionRequest(),
              (s, c, req, done) -> s.getOnlineRegion(c, req, done),
              resp -> ProtobufUtil.getRegionInfos(resp)))
        .serverName(sn).call();
  }

  @Override
  public CompletableFuture<Void> flush(TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    tableExists(tableName).whenComplete((exists, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else if (!exists) {
        future.completeExceptionally(new TableNotFoundException(tableName));
      } else {
        isTableEnabled(tableName).whenComplete((tableEnabled, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else if (!tableEnabled) {
            future.completeExceptionally(new TableNotEnabledException(tableName));
          } else {
            execProcedure(FLUSH_TABLE_PROCEDURE_SIGNATURE, tableName.getNameAsString(),
              new HashMap<>()).whenComplete((ret, err3) -> {
                if (err3 != null) {
                  future.completeExceptionally(err3);
                } else {
                  future.complete(ret);
                }
              });
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegion(regionName).whenComplete((p, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (p == null || p.getFirst() == null) {
        future.completeExceptionally(
          new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName)));
        return;
      }
      if (p.getSecond() == null) {
        future.completeExceptionally(
          new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }

      this.<Void> newAdminCaller().serverName(p.getSecond())
          .action((controller, stub) -> this
              .<FlushRegionRequest, FlushRegionResponse, Void> adminCall(controller, stub,
                RequestConverter.buildFlushRegionRequest(p.getFirst().getRegionName()),
                (s, c, req, done) -> s.flushRegion(c, req, done), resp -> null))
          .call().whenComplete((ret, err2) -> {
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
  public CompletableFuture<Void> compact(TableName tableName) {
    return compact(tableName, null, false, CompactType.NORMAL);
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName, byte[] columnFamily) {
    return compact(tableName, columnFamily, false, CompactType.NORMAL);
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName) {
    return compactRegion(regionName, null, false);
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName, byte[] columnFamily) {
    return compactRegion(regionName, columnFamily, false);
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName) {
    return compact(tableName, null, true, CompactType.NORMAL);
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, byte[] columnFamily) {
    return compact(tableName, columnFamily, true, CompactType.NORMAL);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName) {
    return compactRegion(regionName, null, true);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName, byte[] columnFamily) {
    return compactRegion(regionName, columnFamily, true);
  }

  @Override
  public CompletableFuture<Void> compactRegionServer(ServerName sn) {
    return compactRegionServer(sn, false);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegionServer(ServerName sn) {
    return compactRegionServer(sn, true);
  }

  private CompletableFuture<Void> compactRegionServer(ServerName sn, boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getOnlineRegions(sn).whenComplete((hRegionInfos, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      List<CompletableFuture<Void>> compactFutures = new ArrayList<>();
      if (hRegionInfos != null) {
        hRegionInfos.forEach(region -> compactFutures.add(compact(sn, region, major, null)));
      }
      CompletableFuture
          .allOf(compactFutures.toArray(new CompletableFuture<?>[compactFutures.size()]))
          .whenComplete((ret, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              future.complete(ret);
            }
          });
    });
    return future;
  }

  private CompletableFuture<Void> compactRegion(final byte[] regionName, final byte[] columnFamily,
      final boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegion(regionName).whenComplete((p, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (p == null || p.getFirst() == null) {
        future.completeExceptionally(
          new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName)));
        return;
      }
      if (p.getSecond() == null) {
        // found a region without region server assigned.
        future.completeExceptionally(
          new NoServerForRegionException(Bytes.toStringBinary(regionName)));
        return;
      }
      compact(p.getSecond(), p.getFirst(), major, columnFamily).whenComplete((ret, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else {
          future.complete(ret);
        }
      });
    });
    return future;
  }

  /**
   * List all region locations for the specific table.
   */
  private CompletableFuture<List<HRegionLocation>> getTableHRegionLocations(TableName tableName) {
    CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      // For meta table, we use zk to fetch all locations.
      AsyncRegistry registry = AsyncRegistryFactory.getRegistry(connection.getConfiguration());
      registry.getMetaRegionLocation().whenComplete((metaRegions, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else if (metaRegions == null || metaRegions.isEmpty()
            || metaRegions.getDefaultRegionLocation() == null) {
          future.completeExceptionally(new IOException("meta region does not found"));
        } else {
          future.complete(Collections.singletonList(metaRegions.getDefaultRegionLocation()));
        }
        // close the registry.
        IOUtils.closeQuietly(registry);
      });
    } else {
      // For non-meta table, we fetch all locations by scanning hbase:meta table
      AsyncMetaTableAccessor.getTableRegionsAndLocations(metaTable, Optional.of(tableName))
          .whenComplete((locations, err) -> {
            if (err != null) {
              future.completeExceptionally(err);
            } else if (locations == null || locations.isEmpty()) {
              future.complete(Collections.emptyList());
            } else {
              List<HRegionLocation> regionLocations = locations.stream()
                  .map(loc -> new HRegionLocation(loc.getFirst(), loc.getSecond()))
                  .collect(Collectors.toList());
              future.complete(regionLocations);
            }
          });
    }
    return future;
  }

  /**
   * Compact column family of a table, Asynchronous operation even if CompletableFuture.get()
   */
  private CompletableFuture<Void> compact(final TableName tableName, final byte[] columnFamily,
      final boolean major, CompactType compactType) {
    if (CompactType.MOB.equals(compactType)) {
      // TODO support MOB compact.
      return failedFuture(new UnsupportedOperationException("MOB compact does not support"));
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    getTableHRegionLocations(tableName).whenComplete((locations, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      List<CompletableFuture<Void>> compactFutures = new ArrayList<>();
      for (HRegionLocation location : locations) {
        if (location.getRegionInfo() == null || location.getRegionInfo().isOffline()) continue;
        if (location.getServerName() == null) continue;
        compactFutures
            .add(compact(location.getServerName(), location.getRegionInfo(), major, columnFamily));
      }
      // future complete unless all of the compact futures are completed.
      CompletableFuture
          .allOf(compactFutures.toArray(new CompletableFuture<?>[compactFutures.size()]))
          .whenComplete((ret, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              future.complete(ret);
            }
          });
    });
    return future;
  }

  /**
   * Compact the region at specific region server.
   */
  private CompletableFuture<Void> compact(final ServerName sn, final HRegionInfo hri,
      final boolean major, final byte[] family) {
    return this.<Void> newAdminCaller().serverName(sn)
        .action((controller, stub) -> this
            .<CompactRegionRequest, CompactRegionResponse, Void> adminCall(controller, stub,
              RequestConverter.buildCompactRegionRequest(hri.getRegionName(), major, family),
              (s, c, req, done) -> s.compactRegion(c, req, done), resp -> null))
        .call();
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

  /**
   * Turn regionNameOrEncodedRegionName into regionName, if region does not found, then it'll throw
   * an IllegalArgumentException wrapped by a {@link CompletableFuture}
   * @param regionNameOrEncodedRegionName
   * @return
   */
  CompletableFuture<byte[]> getRegionName(byte[] regionNameOrEncodedRegionName) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    if (Bytes
        .equals(regionNameOrEncodedRegionName, HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
        || Bytes.equals(regionNameOrEncodedRegionName,
          HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      future.complete(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      return future;
    }

    getRegion(regionNameOrEncodedRegionName).whenComplete((p, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      }
      if (p != null && p.getFirst() != null) {
        future.complete(p.getFirst().getRegionName());
      } else {
        future.completeExceptionally(
          new IllegalArgumentException("Invalid region name or encoded region name: "
              + Bytes.toStringBinary(regionNameOrEncodedRegionName)));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> assign(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionName(regionName).whenComplete((fullRegionName, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this.<AssignRegionRequest, AssignRegionResponse, Void> call(
                controller, stub, RequestConverter.buildAssignRegionRequest(fullRegionName),
                (s, c, req, done) -> s.assignRegion(c, req, done), resp -> null)))
            .call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> unassign(byte[] regionName, boolean force) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionName(regionName).whenComplete((fullRegionName, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        this.<Void> newMasterCaller()
            .action(((controller, stub) -> this
                .<UnassignRegionRequest, UnassignRegionResponse, Void> call(controller, stub,
                  RequestConverter.buildUnassignRegionRequest(fullRegionName, force),
                  (s, c, req, done) -> s.unassignRegion(c, req, done), resp -> null)))
            .call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> offline(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionName(regionName).whenComplete((fullRegionName, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this.<OfflineRegionRequest, OfflineRegionResponse, Void> call(
                controller, stub, RequestConverter.buildOfflineRegionRequest(fullRegionName),
                (s, c, req, done) -> s.offlineRegion(c, req, done), resp -> null)))
            .call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> move(byte[] regionName, byte[] destServerName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionName(regionName).whenComplete((fullRegionName, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        final MoveRegionRequest request;
        try {
          request = RequestConverter.buildMoveRegionRequest(
            Bytes.toBytes(HRegionInfo.encodeRegionName(fullRegionName)), destServerName);
        } catch (DeserializationException e) {
          future.completeExceptionally(e);
          return;
        }
        this.<Void> newMasterCaller()
            .action((controller, stub) -> this.<MoveRegionRequest, MoveRegionResponse, Void> call(
              controller, stub, request, (s, c, req, done) -> s.moveRegion(c, req, done),
              resp -> null))
            .call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> setQuota(QuotaSettings quota) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<SetQuotaRequest, SetQuotaResponse, Void> call(controller,
            stub, QuotaSettings.buildSetQuotaRequestProto(quota),
            (s, c, req, done) -> s.setQuota(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<List<QuotaSettings>> getQuota(QuotaFilter filter) {
    CompletableFuture<List<QuotaSettings>> future = new CompletableFuture<>();
    Scan scan = QuotaTableUtil.makeScan(filter);
    this.connection.getRawTableBuilder(QuotaTableUtil.QUOTA_TABLE_NAME).build()
        .scan(scan, new RawScanResultConsumer() {
          List<QuotaSettings> settings = new ArrayList<>();

          @Override
          public void onNext(Result[] results, ScanController controller) {
            for (Result result : results) {
              try {
                QuotaTableUtil.parseResultToCollection(result, settings);
              } catch (IOException e) {
                controller.terminate();
                future.completeExceptionally(e);
              }
            }
          }

          @Override
          public void onError(Throwable error) {
            future.completeExceptionally(error);
          }

          @Override
          public void onComplete() {
            future.complete(settings);
          }
        });
    return future;
  }

  public CompletableFuture<Void> addReplicationPeer(String peerId,
      ReplicationPeerConfig peerConfig) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<AddReplicationPeerRequest, AddReplicationPeerResponse, Void> call(controller, stub,
                RequestConverter.buildAddReplicationPeerRequest(peerId, peerConfig), (s, c, req,
                    done) -> s.addReplicationPeer(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeer(String peerId) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<RemoveReplicationPeerRequest, RemoveReplicationPeerResponse, Void> call(controller,
                stub, RequestConverter.buildRemoveReplicationPeerRequest(peerId),
                (s, c, req, done) -> s.removeReplicationPeer(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> enableReplicationPeer(String peerId) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<EnableReplicationPeerRequest, EnableReplicationPeerResponse, Void> call(controller,
                stub, RequestConverter.buildEnableReplicationPeerRequest(peerId),
                (s, c, req, done) -> s.enableReplicationPeer(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> disableReplicationPeer(String peerId) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<DisableReplicationPeerRequest, DisableReplicationPeerResponse, Void> call(
                controller, stub, RequestConverter.buildDisableReplicationPeerRequest(peerId), (s,
                    c, req, done) -> s.disableReplicationPeer(c, req, done), (resp) -> null))
        .call();
  }

  public CompletableFuture<ReplicationPeerConfig> getReplicationPeerConfig(String peerId) {
    return this
        .<ReplicationPeerConfig> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetReplicationPeerConfigRequest, GetReplicationPeerConfigResponse, ReplicationPeerConfig> call(
                controller, stub, RequestConverter.buildGetReplicationPeerConfigRequest(peerId), (
                    s, c, req, done) -> s.getReplicationPeerConfig(c, req, done),
                (resp) -> ReplicationSerDeHelper.convert(resp.getPeerConfig()))).call();
  }

  @Override
  public CompletableFuture<Void> updateReplicationPeerConfig(String peerId,
      ReplicationPeerConfig peerConfig) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<UpdateReplicationPeerConfigRequest, UpdateReplicationPeerConfigResponse, Void> call(
                controller, stub, RequestConverter.buildUpdateReplicationPeerConfigRequest(peerId,
                  peerConfig), (s, c, req, done) -> s.updateReplicationPeerConfig(c, req, done), (
                    resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> appendReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs) {
    if (tableCfs == null) {
      return failedFuture(new ReplicationException("tableCfs is null"));
    }

    CompletableFuture<Void> future = new CompletableFuture<Void>();
    getReplicationPeerConfig(id).whenComplete((peerConfig, error) -> {
      if (!completeExceptionally(future, error)) {
        ReplicationSerDeHelper.appendTableCFsToReplicationPeerConfig(tableCfs, peerConfig);
        updateReplicationPeerConfig(id, peerConfig).whenComplete((result, err) -> {
          if (!completeExceptionally(future, error)) {
            future.complete(result);
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs) {
    if (tableCfs == null) {
      return failedFuture(new ReplicationException("tableCfs is null"));
    }

    CompletableFuture<Void> future = new CompletableFuture<Void>();
    getReplicationPeerConfig(id).whenComplete((peerConfig, error) -> {
      if (!completeExceptionally(future, error)) {
        try {
          ReplicationSerDeHelper.removeTableCFsFromReplicationPeerConfig(tableCfs, peerConfig, id);
        } catch (ReplicationException e) {
          future.completeExceptionally(e);
          return;
        }
        updateReplicationPeerConfig(id, peerConfig).whenComplete((result, err) -> {
          if (!completeExceptionally(future, error)) {
            future.complete(result);
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers() {
    return listReplicationPeers((Pattern) null);
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(String regex) {
    return listReplicationPeers(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(Pattern pattern) {
    return this
        .<List<ReplicationPeerDescription>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<ListReplicationPeersRequest, ListReplicationPeersResponse, List<ReplicationPeerDescription>> call(
                controller,
                stub,
                RequestConverter.buildListReplicationPeersRequest(pattern),
                (s, c, req, done) -> s.listReplicationPeers(c, req, done),
                (resp) -> resp.getPeerDescList().stream()
                    .map(ReplicationSerDeHelper::toReplicationPeerDescription)
                    .collect(Collectors.toList()))).call();
  }

  @Override
  public CompletableFuture<List<TableCFs>> listReplicatedTableCFs() {
    CompletableFuture<List<TableCFs>> future = new CompletableFuture<List<TableCFs>>();
    listTables().whenComplete(
      (tables, error) -> {
        if (!completeExceptionally(future, error)) {
          List<TableCFs> replicatedTableCFs = new ArrayList<>();
          Arrays.asList(tables).forEach(
            table -> {
              Map<String, Integer> cfs = new HashMap<>();
              Stream.of(table.getColumnFamilies())
                  .filter(column -> column.getScope() != HConstants.REPLICATION_SCOPE_LOCAL)
                  .forEach(column -> {
                    cfs.put(column.getNameAsString(), column.getScope());
                  });
              if (!cfs.isEmpty()) {
                replicatedTableCFs.add(new TableCFs(table.getTableName(), cfs));
              }
            });
          future.complete(replicatedTableCFs);
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> snapshot(String snapshotName, TableName tableName) {
    return snapshot(snapshotName, tableName, SnapshotType.FLUSH);
  }

  @Override
  public CompletableFuture<Void> snapshot(String snapshotName, TableName tableName,
      SnapshotType type) {
    return snapshot(new SnapshotDescription(snapshotName, tableName, type));
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription snapshotDesc) {
    SnapshotProtos.SnapshotDescription snapshot =
        ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc);
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    final SnapshotRequest request = SnapshotRequest.newBuilder().setSnapshot(snapshot).build();
    this.<Long> newMasterCaller()
        .action((controller, stub) -> this.<SnapshotRequest, SnapshotResponse, Long> call(
          controller, stub, request, (s, c, req, done) -> s.snapshot(c, req, done),
          resp -> resp.getExpectedTimeout()))
        .call().whenComplete((expectedTimeout, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          TimerTask pollingTask = new TimerTask() {
            int tries = 0;
            long startTime = EnvironmentEdgeManager.currentTime();
            long endTime = startTime + expectedTimeout;
            long maxPauseTime = expectedTimeout / maxAttempts;

            @Override
            public void run(Timeout timeout) throws Exception {
              if (EnvironmentEdgeManager.currentTime() < endTime) {
                isSnapshotFinished(snapshotDesc).whenComplete((done, err) -> {
                  if (err != null) {
                    future.completeExceptionally(err);
                  } else if (done) {
                    future.complete(null);
                  } else {
                    // retry again after pauseTime.
                    long pauseTime = ConnectionUtils
                        .getPauseTime(TimeUnit.NANOSECONDS.toMillis(pauseNs), ++tries);
                    pauseTime = Math.min(pauseTime, maxPauseTime);
                    AsyncConnectionImpl.RETRY_TIMER.newTimeout(this, pauseTime,
                      TimeUnit.MILLISECONDS);
                  }
                });
              } else {
                future.completeExceptionally(new SnapshotCreationException(
                    "Snapshot '" + snapshot.getName() + "' wasn't completed in expectedTime:"
                        + expectedTimeout + " ms",
                    snapshotDesc));
              }
            }
          };
          AsyncConnectionImpl.RETRY_TIMER.newTimeout(pollingTask, 1, TimeUnit.MILLISECONDS);
        });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription snapshot) {
    return this.<Boolean> newMasterCaller()
        .action((controller, stub) -> this
            .<IsSnapshotDoneRequest, IsSnapshotDoneResponse, Boolean> call(controller, stub,
              IsSnapshotDoneRequest.newBuilder()
                  .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(),
              (s, c, req, done) -> s.isSnapshotDone(c, req, done), resp -> resp.getDone()))
        .call();
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName) {
    boolean takeFailSafeSnapshot = this.connection.getConfiguration().getBoolean(
      HConstants.SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT,
      HConstants.DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT);
    return restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  private CompletableFuture<Void> restoreSnapshotWithFailSafe(String snapshotName,
      TableName tableName, boolean takeFailSafeSnapshot) {
    if (takeFailSafeSnapshot) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      // Step.1 Take a snapshot of the current state
      String failSafeSnapshotSnapshotNameFormat =
          this.connection.getConfiguration().get(HConstants.SNAPSHOT_RESTORE_FAILSAFE_NAME,
            HConstants.DEFAULT_SNAPSHOT_RESTORE_FAILSAFE_NAME);
      final String failSafeSnapshotSnapshotName =
          failSafeSnapshotSnapshotNameFormat.replace("{snapshot.name}", snapshotName)
              .replace("{table.name}", tableName.toString().replace(TableName.NAMESPACE_DELIM, '.'))
              .replace("{restore.timestamp}", String.valueOf(EnvironmentEdgeManager.currentTime()));
      LOG.info("Taking restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
      snapshot(failSafeSnapshotSnapshotName, tableName).whenComplete((ret, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          // Step.2 Restore snapshot
          internalRestoreSnapshot(snapshotName, tableName).whenComplete((void2, err2) -> {
            if (err2 != null) {
              // Step.3.a Something went wrong during the restore and try to rollback.
              internalRestoreSnapshot(failSafeSnapshotSnapshotName, tableName)
                  .whenComplete((void3, err3) -> {
                    if (err3 != null) {
                      future.completeExceptionally(err3);
                    } else {
                      String msg =
                          "Restore snapshot=" + snapshotName + " failed. Rollback to snapshot="
                              + failSafeSnapshotSnapshotName + " succeeded.";
                      future.completeExceptionally(new RestoreSnapshotException(msg));
                    }
                  });
            } else {
              // Step.3.b If the restore is succeeded, delete the pre-restore snapshot.
              LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
              deleteSnapshot(failSafeSnapshotSnapshotName).whenComplete((ret3, err3) -> {
                if (err3 != null) {
                  LOG.error(
                    "Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName,
                    err3);
                  future.completeExceptionally(err3);
                } else {
                  future.complete(ret3);
                }
              });
            }
          });
        }
      });
      return future;
    } else {
      return internalRestoreSnapshot(snapshotName, tableName);
    }
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName,
      boolean takeFailSafeSnapshot) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    listSnapshots(snapshotName).whenComplete((snapshotDescriptions, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      TableName tableName = null;
      if (snapshotDescriptions != null && !snapshotDescriptions.isEmpty()) {
        for (SnapshotDescription snap : snapshotDescriptions) {
          if (snap.getName().equals(snapshotName)) {
            tableName = snap.getTableName();
            break;
          }
        }
      }
      if (tableName == null) {
        future.completeExceptionally(new RestoreSnapshotException(
            "Unable to find the table name for snapshot=" + snapshotName));
        return;
      }
      final TableName finalTableName = tableName;
      tableExists(finalTableName).whenComplete((exists, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
        } else if (!exists) {
          // if table does not exist, then just clone snapshot into new table.
          completeConditionalOnFuture(future,
              internalRestoreSnapshot(snapshotName, finalTableName));
        } else {
          isTableDisabled(finalTableName).whenComplete((disabled, err4) -> {
            if (err4 != null) {
              future.completeExceptionally(err4);
            } else if (!disabled) {
              future.completeExceptionally(new TableNotDisabledException(finalTableName));
            } else {
              completeConditionalOnFuture(future,
                  restoreSnapshotWithFailSafe(snapshotName, finalTableName, takeFailSafeSnapshot));
            }
          });
        }
      });
    });
    return future;
  }

  private <T> void completeConditionalOnFuture(CompletableFuture<T> dependentFuture,
      CompletableFuture<T> parentFuture) {
    parentFuture.whenComplete((res, err) -> {
      if (err != null) {
        dependentFuture.completeExceptionally(err);
      } else {
        dependentFuture.complete(res);
      }
    });
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotName, TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    tableExists(tableName).whenComplete((exists, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else if (exists) {
        future.completeExceptionally(new TableExistsException(tableName));
      } else {
        completeConditionalOnFuture(future, internalRestoreSnapshot(snapshotName, tableName));
      }
    });
    return future;
  }

  private CompletableFuture<Void> internalRestoreSnapshot(String snapshotName,
      TableName tableName) {
    SnapshotProtos.SnapshotDescription snapshot = SnapshotProtos.SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(tableName.getNameAsString()).build();
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    return waitProcedureResult(
        this.<Long> newMasterCaller()
            .action((controller, stub) -> this
                .<RestoreSnapshotRequest, RestoreSnapshotResponse, Long> call(controller, stub,
                    RestoreSnapshotRequest.newBuilder().setSnapshot(snapshot)
                        .setNonceGroup(ng.getNonceGroup()).setNonce(ng.newNonce()).build(),
                    (s, c, req, done) -> s.restoreSnapshot(c, req, done),
                    (resp) -> resp.getProcId()))
            .call());
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots() {
    return this.<List<SnapshotDescription>> newMasterCaller()
        .action((controller, stub) -> this
            .<GetCompletedSnapshotsRequest, GetCompletedSnapshotsResponse, List<SnapshotDescription>> call(
              controller, stub, GetCompletedSnapshotsRequest.newBuilder().build(),
              (s, c, req, done) -> s.getCompletedSnapshots(c, req, done),
              resp -> resp.getSnapshotsList().stream().map(ProtobufUtil::createSnapshotDesc)
                  .collect(Collectors.toList())))
        .call();
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(String regex) {
    return listSnapshots(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Pattern pattern) {
    CompletableFuture<List<SnapshotDescription>> future = new CompletableFuture<>();
    listSnapshots().whenComplete((snapshotDescList, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (snapshotDescList == null || snapshotDescList.isEmpty()) {
        future.complete(Collections.emptyList());
        return;
      }
      future.complete(snapshotDescList.stream()
          .filter(snap -> pattern.matcher(snap.getName()).matches()).collect(Collectors.toList()));
    });
    return future;
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(String tableNameRegex,
      String snapshotNameRegex) {
    return listTableSnapshots(Pattern.compile(tableNameRegex), Pattern.compile(snapshotNameRegex));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    CompletableFuture<List<SnapshotDescription>> future = new CompletableFuture<>();
    listTableNames(tableNamePattern, false).whenComplete((tableNames, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (tableNames == null || tableNames.length <= 0) {
        future.complete(Collections.emptyList());
        return;
      }
      List<TableName> tableNameList = Arrays.asList(tableNames);
      listSnapshots(snapshotNamePattern).whenComplete((snapshotDescList, err2) -> {
        if (err2 != null) {
          future.completeExceptionally(err2);
          return;
        }
        if (snapshotDescList == null || snapshotDescList.isEmpty()) {
          future.complete(Collections.emptyList());
          return;
        }
        future.complete(snapshotDescList.stream()
            .filter(snap -> (snap != null && tableNameList.contains(snap.getTableName())))
            .collect(Collectors.toList()));
      });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String snapshotName) {
    return internalDeleteSnapshot(new SnapshotDescription(snapshotName));
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots(String regex) {
    return deleteSnapshots(Pattern.compile(regex));
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots(Pattern snapshotNamePattern) {
    return deleteTableSnapshots(null, snapshotNamePattern);
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(String tableNameRegex,
      String snapshotNameRegex) {
    return deleteTableSnapshots(Pattern.compile(tableNameRegex),
      Pattern.compile(snapshotNameRegex));
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    listTableSnapshots(tableNamePattern, snapshotNamePattern)
        .whenComplete(((snapshotDescriptions, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          if (snapshotDescriptions == null || snapshotDescriptions.isEmpty()) {
            future.complete(null);
            return;
          }
          List<CompletableFuture<Void>> deleteSnapshotFutures = new ArrayList<>();
          snapshotDescriptions
              .forEach(snapDesc -> deleteSnapshotFutures.add(internalDeleteSnapshot(snapDesc)));
          CompletableFuture
              .allOf(deleteSnapshotFutures
                  .toArray(new CompletableFuture<?>[deleteSnapshotFutures.size()]))
              .thenAccept(v -> future.complete(v));
        }));
    return future;
  }

  @Override
  public CompletableFuture<Void> execProcedure(String signature, String instance,
      Map<String, String> props) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    ProcedureDescription procDesc =
        ProtobufUtil.buildProcedureDescription(signature, instance, props);
    this.<Long> newMasterCaller()
        .action((controller, stub) -> this.<ExecProcedureRequest, ExecProcedureResponse, Long> call(
          controller, stub, ExecProcedureRequest.newBuilder().setProcedure(procDesc).build(),
          (s, c, req, done) -> s.execProcedure(c, req, done), resp -> resp.getExpectedTimeout()))
        .call().whenComplete((expectedTimeout, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          TimerTask pollingTask = new TimerTask() {
            int tries = 0;
            long startTime = EnvironmentEdgeManager.currentTime();
            long endTime = startTime + expectedTimeout;
            long maxPauseTime = expectedTimeout / maxAttempts;

            @Override
            public void run(Timeout timeout) throws Exception {
              if (EnvironmentEdgeManager.currentTime() < endTime) {
                isProcedureFinished(signature, instance, props).whenComplete((done, err) -> {
                  if (err != null) {
                    future.completeExceptionally(err);
                    return;
                  }
                  if (done) {
                    future.complete(null);
                  } else {
                    // retry again after pauseTime.
                    long pauseTime = ConnectionUtils
                        .getPauseTime(TimeUnit.NANOSECONDS.toMillis(pauseNs), ++tries);
                    pauseTime = Math.min(pauseTime, maxPauseTime);
                    AsyncConnectionImpl.RETRY_TIMER.newTimeout(this, pauseTime,
                      TimeUnit.MICROSECONDS);
                  }
                });
              } else {
                future.completeExceptionally(new IOException("Procedure '" + signature + " : "
                    + instance + "' wasn't completed in expectedTime:" + expectedTimeout + " ms"));
              }
            }
          };
          // Queue the polling task into RETRY_TIMER to poll procedure state asynchronously.
          AsyncConnectionImpl.RETRY_TIMER.newTimeout(pollingTask, 1, TimeUnit.MILLISECONDS);
        });
    return future;
  }

  @Override
  public CompletableFuture<byte[]> execProcedureWithRet(String signature, String instance,
      Map<String, String> props) {
    ProcedureDescription proDesc =
        ProtobufUtil.buildProcedureDescription(signature, instance, props);
    return this.<byte[]> newMasterCaller()
        .action(
          (controller, stub) -> this.<ExecProcedureRequest, ExecProcedureResponse, byte[]> call(
            controller, stub, ExecProcedureRequest.newBuilder().setProcedure(proDesc).build(),
            (s, c, req, done) -> s.execProcedureWithRet(c, req, done),
            resp -> resp.hasReturnData() ? resp.getReturnData().toByteArray() : null))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> isProcedureFinished(String signature, String instance,
      Map<String, String> props) {
    ProcedureDescription proDesc =
        ProtobufUtil.buildProcedureDescription(signature, instance, props);
    return this.<Boolean> newMasterCaller()
        .action((controller, stub) -> this
            .<IsProcedureDoneRequest, IsProcedureDoneResponse, Boolean> call(controller, stub,
              IsProcedureDoneRequest.newBuilder().setProcedure(proDesc).build(),
              (s, c, req, done) -> s.isProcedureDone(c, req, done), resp -> resp.getDone()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> abortProcedure(long procId, boolean mayInterruptIfRunning) {
    return this.<Boolean> newMasterCaller().action(
      (controller, stub) -> this.<AbortProcedureRequest, AbortProcedureResponse, Boolean> call(
        controller, stub, AbortProcedureRequest.newBuilder().setProcId(procId).build(),
        (s, c, req, done) -> s.abortProcedure(c, req, done), resp -> resp.getIsProcedureAborted()))
        .call();
  }

  @Override
  public CompletableFuture<ProcedureInfo[]> listProcedures() {
    return this.<ProcedureInfo[]> newMasterCaller()
        .action((controller, stub) -> this
            .<ListProceduresRequest, ListProceduresResponse, ProcedureInfo[]> call(controller, stub,
              ListProceduresRequest.newBuilder().build(),
              (s, c, req, done) -> s.listProcedures(c, req, done), resp -> resp.getProcedureList()
                  .stream().map(ProtobufUtil::toProcedureInfo).toArray(ProcedureInfo[]::new)))
        .call();
  }

  private CompletableFuture<Void> internalDeleteSnapshot(SnapshotDescription snapshot) {
    return this.<Void> newMasterCaller()
        .action((controller, stub) -> this
            .<DeleteSnapshotRequest, DeleteSnapshotResponse, Void> call(controller, stub,
              DeleteSnapshotRequest.newBuilder()
                  .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(),
              (s, c, req, done) -> s.deleteSnapshot(c, req, done), resp -> null))
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

  private <T> boolean completeExceptionally(CompletableFuture<T> future, Throwable error) {
    if (error != null) {
      future.completeExceptionally(error);
      return true;
    }
    return false;
  }
}
