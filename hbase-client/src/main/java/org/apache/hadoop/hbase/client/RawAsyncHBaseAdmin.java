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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.QueryType;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.AdminRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.ServerRequestCallerBuilder;
import org.apache.hadoop.hbase.client.RawAsyncTable.CoprocessorCallable;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.shaded.io.netty.util.Timeout;
import org.apache.hadoop.hbase.shaded.io.netty.util.TimerTask;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DrainRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DrainRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDrainingRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDrainingRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampForRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RemoveDrainFromRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RemoveDrainFromRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterResponse;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.google.protobuf.RpcChannel;

/**
 * The implementation of AsyncAdmin.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class RawAsyncHBaseAdmin implements AsyncAdmin {
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

  RawAsyncHBaseAdmin(AsyncConnectionImpl connection, AsyncAdminBuilderBase builder) {
    this.connection = connection;
    this.metaTable = connection.getRawTable(META_TABLE_NAME);
    this.rpcTimeoutNs = builder.rpcTimeoutNs;
    this.operationTimeoutNs = builder.operationTimeoutNs;
    this.pauseNs = builder.pauseNs;
    this.maxAttempts = builder.maxAttempts;
    this.startLogErrorsCnt = builder.startLogErrorsCnt;
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

  private CompletableFuture<List<TableDescriptor>> batchTableOperations(Pattern pattern,
      TableOperator operator, String operationType) {
    CompletableFuture<List<TableDescriptor>> future = new CompletableFuture<>();
    List<TableDescriptor> failed = new LinkedList<>();
    listTables(Optional.ofNullable(pattern), false).whenComplete(
      (tables, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        CompletableFuture[] futures =
            tables.stream()
                .map((table) -> operator.operate(table.getTableName()).whenComplete((v, ex) -> {
                  if (ex != null) {
                    LOG.info("Failed to " + operationType + " table " + table.getTableName(), ex);
                    failed.add(table);
                  }
                })).<CompletableFuture> toArray(size -> new CompletableFuture[size]);
        CompletableFuture.allOf(futures).thenAccept((v) -> {
          future.complete(failed);
        });
      });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return AsyncMetaTableAccessor.tableExists(metaTable, tableName);
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTables(Optional<Pattern> pattern,
      boolean includeSysTables) {
    return this.<List<TableDescriptor>> newMasterCaller()
        .action((controller, stub) -> this
            .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, List<TableDescriptor>> call(
              controller, stub,
              RequestConverter.buildGetTableDescriptorsRequest(pattern, includeSysTables),
              (s, c, req, done) -> s.getTableDescriptors(c, req, done),
              (resp) -> ProtobufUtil.toTableDescriptorList(resp)))
        .call();
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames(Optional<Pattern> pattern,
      boolean includeSysTables) {
    return this.<List<TableName>> newMasterCaller()
        .action((controller, stub) -> this
            .<GetTableNamesRequest, GetTableNamesResponse, List<TableName>> call(controller, stub,
              RequestConverter.buildGetTableNamesRequest(pattern, includeSysTables),
              (s, c, req, done) -> s.getTableNames(c, req, done),
              (resp) -> ProtobufUtil.toTableNameList(resp.getTableNamesList())))
        .call();
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
            future.complete(ProtobufUtil.toTableDescriptor(tableSchemas.get(0)));
          } else {
            future.completeExceptionally(new TableNotFoundException(tableName.getNameAsString()));
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    try {
      return createTable(desc, Optional.of(getSplitKeys(startKey, endKey, numRegions)));
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, Optional<byte[][]> splitKeys) {
    if (desc.getTableName() == null) {
      return failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }
    try {
      splitKeys.ifPresent(keys -> verifySplitKeys(keys));
      return this.<CreateTableRequest, CreateTableResponse> procedureCall(RequestConverter
          .buildCreateTableRequest(desc, splitKeys, ng.getNonceGroup(), ng.newNonce()), (s, c, req,
          done) -> s.createTable(c, req, done), (resp) -> resp.getProcId(),
        new CreateTableProcedureBiConsumer(this, desc.getTableName()));
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    return this.<DeleteTableRequest, DeleteTableResponse> procedureCall(RequestConverter
        .buildDeleteTableRequest(tableName, ng.getNonceGroup(), ng.newNonce()),
      (s, c, req, done) -> s.deleteTable(c, req, done), (resp) -> resp.getProcId(),
      new DeleteTableProcedureBiConsumer(this, tableName));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> deleteTables(Pattern pattern) {
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
  public CompletableFuture<List<TableDescriptor>> enableTables(Pattern pattern) {
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
  public CompletableFuture<List<TableDescriptor>> disableTables(Pattern pattern) {
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
          AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName))
              .whenComplete(
                (locations, error1) -> {
                  if (error1 != null) {
                    future.completeExceptionally(error1);
                    return;
                  }
                  int notDeployed = 0;
                  int regionCount = 0;
                  for (HRegionLocation location : locations) {
                    HRegionInfo info = location.getRegionInfo();
                    if (location.getServerName() == null) {
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Table " + tableName + " has not deployed region "
                            + info.getEncodedName());
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
  public CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors() {
    return this
        .<List<NamespaceDescriptor>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<ListNamespaceDescriptorsRequest, ListNamespaceDescriptorsResponse, List<NamespaceDescriptor>> call(
                controller, stub, ListNamespaceDescriptorsRequest.newBuilder().build(), (s, c, req,
                    done) -> s.listNamespaceDescriptors(c, req, done), (resp) -> ProtobufUtil
                    .toNamespaceDescriptorList(resp))).call();
  }

  @Override
  public CompletableFuture<Boolean> closeRegion(byte[] regionName, Optional<ServerName> unused) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    unassign(regionName, true).whenComplete((result, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        future.complete(true);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<List<HRegionInfo>> getOnlineRegions(ServerName serverName) {
    return this.<List<HRegionInfo>> newAdminCaller()
        .action((controller, stub) -> this
            .<GetOnlineRegionRequest, GetOnlineRegionResponse, List<HRegionInfo>> adminCall(
              controller, stub, RequestConverter.buildGetOnlineRegionRequest(),
              (s, c, req, done) -> s.getOnlineRegion(c, req, done),
              resp -> ProtobufUtil.getRegionInfos(resp)))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<List<HRegionInfo>> getTableRegions(TableName tableName) {
    if (tableName.equals(META_TABLE_NAME)) {
      return connection.getLocator().getRegionLocation(tableName, null, null, operationTimeoutNs)
          .thenApply(loc -> Arrays.asList(loc.getRegionInfo()));
    } else {
      return AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName))
          .thenApply(
            locs -> locs.stream().map(loc -> loc.getRegionInfo()).collect(Collectors.toList()));
    }
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
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }

        HRegionInfo regionInfo = location.getRegionInfo();
        this.<Void> newAdminCaller()
            .serverName(serverName)
            .action(
              (controller, stub) -> this.<FlushRegionRequest, FlushRegionResponse, Void> adminCall(
                controller, stub, RequestConverter.buildFlushRegionRequest(regionInfo
                    .getRegionName()), (s, c, req, done) -> s.flushRegion(c, req, done),
                resp -> null)).call().whenComplete((ret, err2) -> {
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
  public CompletableFuture<Void> compact(TableName tableName, Optional<byte[]> columnFamily) {
    return compact(tableName, columnFamily, false, CompactType.NORMAL);
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName, Optional<byte[]> columnFamily) {
    return compactRegion(regionName, columnFamily, false);
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, Optional<byte[]> columnFamily) {
    return compact(tableName, columnFamily, true, CompactType.NORMAL);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName, Optional<byte[]> columnFamily) {
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
        hRegionInfos.forEach(region -> compactFutures.add(compact(sn, region, major, Optional.empty())));
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

  private CompletableFuture<Void> compactRegion(byte[] regionName, Optional<byte[]> columnFamily,
      boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }
        compact(location.getServerName(), location.getRegionInfo(), major, columnFamily)
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
   * List all region locations for the specific table.
   */
  private CompletableFuture<List<HRegionLocation>> getTableHRegionLocations(TableName tableName) {
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
      // For meta table, we use zk to fetch all locations.
      AsyncRegistry registry = AsyncRegistryFactory.getRegistry(connection.getConfiguration());
      registry.getMetaRegionLocation().whenComplete(
        (metaRegions, err) -> {
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
      return future;
    } else {
      // For non-meta table, we fetch all locations by scanning hbase:meta table
      return AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName));
    }
  }

  /**
   * Compact column family of a table, Asynchronous operation even if CompletableFuture.get()
   */
  private CompletableFuture<Void> compact(final TableName tableName, Optional<byte[]> columnFamily,
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
      final boolean major, Optional<byte[]> columnFamily) {
    return this
        .<Void> newAdminCaller()
        .serverName(sn)
        .action(
          (controller, stub) -> this.<CompactRegionRequest, CompactRegionResponse, Void> adminCall(
            controller, stub, RequestConverter.buildCompactRegionRequest(hri.getRegionName(),
              major, columnFamily), (s, c, req, done) -> s.compactRegion(c, req, done),
            resp -> null)).call();
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
    getRegionLocation(encodeRegionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          result.completeExceptionally(err);
          return;
        }
        HRegionInfo regionInfo = location.getRegionInfo();
        if (regionInfo.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
          result.completeExceptionally(new IllegalArgumentException(
              "Can't invoke merge on non-default regions directly"));
          return;
        }
        if (!tableName.compareAndSet(null, regionInfo.getTable())) {
          if (!tableName.get().equals(regionInfo.getTable())) {
            // tables of this two region should be same.
            result.completeExceptionally(new IllegalArgumentException(
                "Cannot merge regions from two different tables " + tableName.get() + " and "
                    + regionInfo.getTable()));
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
  public CompletableFuture<Boolean> setMergeOn(boolean on) {
    return setSplitOrMergeOn(on, MasterSwitchType.MERGE);
  }

  @Override
  public CompletableFuture<Boolean> isMergeOn() {
    return isSplitOrMergeOn(MasterSwitchType.MERGE);
  }

  @Override
  public CompletableFuture<Boolean> setSplitOn(boolean on) {
    return setSplitOrMergeOn(on, MasterSwitchType.SPLIT);
  }

  @Override
  public CompletableFuture<Boolean> isSplitOn() {
    return isSplitOrMergeOn(MasterSwitchType.SPLIT);
  }

  private CompletableFuture<Boolean> setSplitOrMergeOn(boolean on, MasterSwitchType switchType) {
    SetSplitOrMergeEnabledRequest request =
        RequestConverter.buildSetSplitOrMergeEnabledRequest(on, false, switchType);
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SetSplitOrMergeEnabledRequest, SetSplitOrMergeEnabledResponse, Boolean> call(
                controller, stub, request, (s, c, req, done) -> s.setSplitOrMergeEnabled(c, req,
                  done), (resp) -> resp.getPrevValueList().get(0))).call();
  }

  private CompletableFuture<Boolean> isSplitOrMergeOn(MasterSwitchType switchType) {
    IsSplitOrMergeEnabledRequest request =
        RequestConverter.buildIsSplitOrMergeEnabledRequest(switchType);
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsSplitOrMergeEnabledRequest, IsSplitOrMergeEnabledResponse, Boolean> call(
                controller, stub, request,
                (s, c, req, done) -> s.isSplitOrMergeEnabled(c, req, done),
                (resp) -> resp.getEnabled())).call();
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
                      splitFutures.add(split(hri, Optional.empty()));
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
            splitRegion(loc.getRegionInfo().getRegionName(), Optional.of(splitPoint))
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
  public CompletableFuture<Void> splitRegion(byte[] regionName, Optional<byte[]> splitPoint) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        HRegionInfo regionInfo = location.getRegionInfo();
        if (regionInfo.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
          future.completeExceptionally(new IllegalArgumentException(
              "Can't split replicas directly. "
                  + "Replicas are auto-split when their primary is split."));
          return;
        }
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }
        split(regionInfo, splitPoint).whenComplete((ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
      });
    return future;
  }

  private CompletableFuture<Void> split(final HRegionInfo hri,
      Optional<byte[]> splitPoint) {
    if (hri.getStartKey() != null && splitPoint.isPresent()
        && Bytes.compareTo(hri.getStartKey(), splitPoint.get()) == 0) {
      return failedFuture(new IllegalArgumentException(
          "should not give a splitkey which equals to startkey!"));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    TableName tableName = hri.getTable();
    SplitTableRegionRequest request = null;
    try {
      request = RequestConverter
          .buildSplitTableRegionRequest(hri, splitPoint.isPresent() ? splitPoint.get() : null,
              ng.getNonceGroup(), ng.newNonce());
    } catch (DeserializationException e) {
      future.completeExceptionally(e);
      return future;
    }

    this.<SplitTableRegionRequest, SplitTableRegionResponse>procedureCall(request,
        (s, c, req, done) -> s.splitRegion(c, req, done), (resp) -> resp.getProcId(),
        new SplitTableRegionProcedureBiConsumer(this, tableName)).whenComplete((ret, err2) -> {
      if (err2 != null) {
        future.completeExceptionally(err2);
      } else {
        future.complete(ret);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> assign(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this.<AssignRegionRequest, AssignRegionResponse, Void> call(
                controller, stub, RequestConverter.buildAssignRegionRequest(regionInfo
                    .getRegionName()), (s, c, req, done) -> s.assignRegion(c, req, done),
                resp -> null))).call().whenComplete((ret, err2) -> {
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
  public CompletableFuture<Void> unassign(byte[] regionName, boolean forcible) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this
                  .<UnassignRegionRequest, UnassignRegionResponse, Void> call(controller, stub,
                    RequestConverter.buildUnassignRegionRequest(regionInfo.getRegionName(), forcible),
                    (s, c, req, done) -> s.unassignRegion(c, req, done), resp -> null))).call()
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

  @Override
  public CompletableFuture<Void> offline(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this.<OfflineRegionRequest, OfflineRegionResponse, Void> call(
                controller, stub, RequestConverter.buildOfflineRegionRequest(regionInfo
                    .getRegionName()), (s, c, req, done) -> s.offlineRegion(c, req, done),
                resp -> null))).call().whenComplete((ret, err2) -> {
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
  public CompletableFuture<Void> move(byte[] regionName, Optional<ServerName> destServerName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              (controller, stub) -> this.<MoveRegionRequest, MoveRegionResponse, Void> call(
                controller, stub, RequestConverter.buildMoveRegionRequest(
                  regionInfo.getEncodedNameAsBytes(), destServerName), (s, c, req, done) -> s
                    .moveRegion(c, req, done), resp -> null)).call().whenComplete((ret, err2) -> {
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

  @Override
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

  @Override
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
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(Optional<Pattern> pattern) {
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
          tables.forEach(table -> {
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
  public CompletableFuture<Void> snapshot(SnapshotDescription snapshotDesc) {
    SnapshotProtos.SnapshotDescription snapshot = ProtobufUtil
        .createHBaseProtosSnapshotDesc(snapshotDesc);
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    final SnapshotRequest request = SnapshotRequest.newBuilder().setSnapshot(snapshot).build();
    this.<Long> newMasterCaller()
        .action(
          (controller, stub) -> this.<SnapshotRequest, SnapshotResponse, Long> call(controller,
            stub, request, (s, c, req, done) -> s.snapshot(c, req, done),
            resp -> resp.getExpectedTimeout())).call().whenComplete((expectedTimeout, err) -> {
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
                isSnapshotFinished(snapshotDesc).whenComplete((done, err2) -> {
                  if (err2 != null) {
                    future.completeExceptionally(err2);
                  } else if (done) {
                    future.complete(null);
                  } else {
                    // retry again after pauseTime.
                  long pauseTime = ConnectionUtils.getPauseTime(
                    TimeUnit.NANOSECONDS.toMillis(pauseNs), ++tries);
                  pauseTime = Math.min(pauseTime, maxPauseTime);
                  AsyncConnectionImpl.RETRY_TIMER
                      .newTimeout(this, pauseTime, TimeUnit.MILLISECONDS);
                }
              } );
              } else {
                future.completeExceptionally(new SnapshotCreationException("Snapshot '"
                    + snapshot.getName() + "' wasn't completed in expectedTime:" + expectedTimeout
                    + " ms", snapshotDesc));
              }
            }
          };
          AsyncConnectionImpl.RETRY_TIMER.newTimeout(pollingTask, 1, TimeUnit.MILLISECONDS);
        });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription snapshot) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this.<IsSnapshotDoneRequest, IsSnapshotDoneResponse, Boolean> call(
            controller,
            stub,
            IsSnapshotDoneRequest.newBuilder()
                .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(), (s, c,
                req, done) -> s.isSnapshotDone(c, req, done), resp -> resp.getDone())).call();
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName) {
    boolean takeFailSafeSnapshot = this.connection.getConfiguration().getBoolean(
      HConstants.SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT,
      HConstants.DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT);
    return restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    listSnapshots(Optional.of(Pattern.compile(snapshotName))).whenComplete(
      (snapshotDescriptions, err) -> {
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
        tableExists(finalTableName)
            .whenComplete((exists, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else if (!exists) {
                // if table does not exist, then just clone snapshot into new table.
              completeConditionalOnFuture(future,
                internalRestoreSnapshot(snapshotName, finalTableName));
            } else {
              isTableDisabled(finalTableName).whenComplete(
                (disabled, err4) -> {
                  if (err4 != null) {
                    future.completeExceptionally(err4);
                  } else if (!disabled) {
                    future.completeExceptionally(new TableNotDisabledException(finalTableName));
                  } else {
                    completeConditionalOnFuture(future,
                      restoreSnapshot(snapshotName, finalTableName, takeFailSafeSnapshot));
                  }
                });
            }
          } );
      });
    return future;
  }

  private CompletableFuture<Void> restoreSnapshot(String snapshotName, TableName tableName,
      boolean takeFailSafeSnapshot) {
    if (takeFailSafeSnapshot) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      // Step.1 Take a snapshot of the current state
      String failSafeSnapshotSnapshotNameFormat = this.connection.getConfiguration().get(
        HConstants.SNAPSHOT_RESTORE_FAILSAFE_NAME,
        HConstants.DEFAULT_SNAPSHOT_RESTORE_FAILSAFE_NAME);
      final String failSafeSnapshotSnapshotName = failSafeSnapshotSnapshotNameFormat
          .replace("{snapshot.name}", snapshotName)
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
          internalRestoreSnapshot(failSafeSnapshotSnapshotName, tableName).whenComplete(
            (void3, err3) -> {
              if (err3 != null) {
                future.completeExceptionally(err3);
              } else {
                String msg = "Restore snapshot=" + snapshotName + " failed. Rollback to snapshot="
                    + failSafeSnapshotSnapshotName + " succeeded.";
                future.completeExceptionally(new RestoreSnapshotException(msg));
              }
            });
        } else {
          // Step.3.b If the restore is succeeded, delete the pre-restore snapshot.
          LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
          deleteSnapshot(failSafeSnapshotSnapshotName).whenComplete(
            (ret3, err3) -> {
              if (err3 != null) {
                LOG.error(
                  "Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName, err3);
                future.completeExceptionally(err3);
              } else {
                future.complete(ret3);
              }
            });
        }
      } );
      }
    } );
      return future;
    } else {
      return internalRestoreSnapshot(snapshotName, tableName);
    }
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

  private CompletableFuture<Void> internalRestoreSnapshot(String snapshotName, TableName tableName) {
    SnapshotProtos.SnapshotDescription snapshot = SnapshotProtos.SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(tableName.getNameAsString()).build();
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    return waitProcedureResult(this
        .<Long> newMasterCaller()
        .action(
          (controller, stub) -> this.<RestoreSnapshotRequest, RestoreSnapshotResponse, Long> call(
            controller, stub, RestoreSnapshotRequest.newBuilder().setSnapshot(snapshot)
                .setNonceGroup(ng.getNonceGroup()).setNonce(ng.newNonce()).build(), (s, c, req,
                done) -> s.restoreSnapshot(c, req, done), (resp) -> resp.getProcId())).call());
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Optional<Pattern> pattern) {
    CompletableFuture<List<SnapshotDescription>> future = new CompletableFuture<>();
    this.<GetCompletedSnapshotsResponse> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetCompletedSnapshotsRequest, GetCompletedSnapshotsResponse, GetCompletedSnapshotsResponse> call(
                controller, stub, GetCompletedSnapshotsRequest.newBuilder().build(), (s, c, req,
                    done) -> s.getCompletedSnapshots(c, req, done), resp -> resp))
        .call()
        .whenComplete(
          (resp, err) -> {
            if (err != null) {
              future.completeExceptionally(err);
              return;
            }
            future.complete(resp
                .getSnapshotsList()
                .stream()
                .map(ProtobufUtil::createSnapshotDesc)
                .filter(
                  snap -> pattern.isPresent() ? pattern.get().matcher(snap.getName()).matches()
                      : true).collect(Collectors.toList()));
          });
    return future;
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    CompletableFuture<List<SnapshotDescription>> future = new CompletableFuture<>();
    listTableNames(Optional.ofNullable(tableNamePattern), false).whenComplete(
      (tableNames, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        if (tableNames == null || tableNames.size() <= 0) {
          future.complete(Collections.emptyList());
          return;
        }
        listSnapshots(Optional.ofNullable(snapshotNamePattern)).whenComplete(
          (snapshotDescList, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
              return;
            }
            if (snapshotDescList == null || snapshotDescList.isEmpty()) {
              future.complete(Collections.emptyList());
              return;
            }
            future.complete(snapshotDescList.stream()
                .filter(snap -> (snap != null && tableNames.contains(snap.getTableName())))
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
  public CompletableFuture<Void> deleteSnapshots(Pattern snapshotNamePattern) {
    return deleteTableSnapshots(null, snapshotNamePattern);
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    listTableSnapshots(tableNamePattern, snapshotNamePattern).whenComplete(
      ((snapshotDescriptions, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        if (snapshotDescriptions == null || snapshotDescriptions.isEmpty()) {
          future.complete(null);
          return;
        }
        List<CompletableFuture<Void>> deleteSnapshotFutures = new ArrayList<>();
        snapshotDescriptions.forEach(snapDesc -> deleteSnapshotFutures
            .add(internalDeleteSnapshot(snapDesc)));
        CompletableFuture.allOf(
          deleteSnapshotFutures.toArray(new CompletableFuture<?>[deleteSnapshotFutures.size()]))
            .thenAccept(v -> future.complete(v));
      }));
    return future;
  }

  private CompletableFuture<Void> internalDeleteSnapshot(SnapshotDescription snapshot) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<DeleteSnapshotRequest, DeleteSnapshotResponse, Void> call(
            controller,
            stub,
            DeleteSnapshotRequest.newBuilder()
                .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(), (s, c,
                req, done) -> s.deleteSnapshot(c, req, done), resp -> null)).call();
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
                isProcedureFinished(signature, instance, props).whenComplete((done, err2) -> {
                  if (err2 != null) {
                    future.completeExceptionally(err2);
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
  public CompletableFuture<String> getProcedures() {
    return this
        .<String> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetProceduresRequest, GetProceduresResponse, String> call(
                controller, stub, GetProceduresRequest.newBuilder().build(),
                (s, c, req, done) -> s.getProcedures(c, req, done),
                resp -> ProtobufUtil.toProcedureJson(resp.getProcedureList()))).call();
  }

  @Override
  public CompletableFuture<String> getLocks() {
    return this
        .<String> newMasterCaller()
        .action(
          (controller, stub) -> this.<GetLocksRequest, GetLocksResponse, String> call(
            controller, stub, GetLocksRequest.newBuilder().build(),
            (s, c, req, done) -> s.getLocks(c, req, done),
            resp -> ProtobufUtil.toLockJson(resp.getLockList()))).call();
  }

  @Override
  public CompletableFuture<Void> drainRegionServers(List<ServerName> servers) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<DrainRegionServersRequest, DrainRegionServersResponse, Void> call(controller, stub,
                RequestConverter.buildDrainRegionServersRequest(servers),
                (s, c, req, done) -> s.drainRegionServers(c, req, done), resp -> null)).call();
  }

  @Override
  public CompletableFuture<List<ServerName>> listDrainingRegionServers() {
    return this
        .<List<ServerName>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<ListDrainingRegionServersRequest, ListDrainingRegionServersResponse, List<ServerName>> call(
                controller,
                stub,
                ListDrainingRegionServersRequest.newBuilder().build(),
                (s, c, req, done) -> s.listDrainingRegionServers(c, req, done),
                resp -> resp.getServerNameList().stream().map(ProtobufUtil::toServerName)
                    .collect(Collectors.toList()))).call();
  }

  @Override
  public CompletableFuture<Void> removeDrainFromRegionServers(List<ServerName> servers) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<RemoveDrainFromRegionServersRequest, RemoveDrainFromRegionServersResponse, Void> call(
                controller, stub, RequestConverter
                    .buildRemoveDrainFromRegionServersRequest(servers), (s, c, req, done) -> s
                    .removeDrainFromRegionServers(c, req, done), resp -> null)).call();
  }

  /**
   * Get the region location for the passed region name. The region name may be a full region name
   * or encoded region name. If the region does not found, then it'll throw an
   * UnknownRegionException wrapped by a {@link CompletableFuture}
   * @param regionNameOrEncodedRegionName
   * @return region location, wrapped by a {@link CompletableFuture}
   */
  @VisibleForTesting
  CompletableFuture<HRegionLocation> getRegionLocation(byte[] regionNameOrEncodedRegionName) {
    if (regionNameOrEncodedRegionName == null) {
      return failedFuture(new IllegalArgumentException("Passed region name can't be null"));
    }
    try {
      CompletableFuture<Optional<HRegionLocation>> future;
      if (HRegionInfo.isEncodedRegionName(regionNameOrEncodedRegionName)) {
        future = AsyncMetaTableAccessor.getRegionLocationWithEncodedName(metaTable,
          regionNameOrEncodedRegionName);
      } else {
        future = AsyncMetaTableAccessor.getRegionLocation(metaTable, regionNameOrEncodedRegionName);
      }

      CompletableFuture<HRegionLocation> returnedFuture = new CompletableFuture<>();
      future.whenComplete((location, err) -> {
        if (err != null) {
          returnedFuture.completeExceptionally(err);
          return;
        }
        LOG.info("location is " + location);
        if (!location.isPresent() || location.get().getRegionInfo() == null) {
          LOG.info("unknown location is " + location);
          returnedFuture.completeExceptionally(new UnknownRegionException(
              "Invalid region name or encoded region name: "
                  + Bytes.toStringBinary(regionNameOrEncodedRegionName)));
        } else {
          returnedFuture.complete(location.get());
        }
      });
      return returnedFuture;
    } catch (IOException e) {
      return failedFuture(e);
    }
  }

  /**
   * Get the region info for the passed region name. The region name may be a full region name or
   * encoded region name. If the region does not found, then it'll throw an UnknownRegionException
   * wrapped by a {@link CompletableFuture}
   * @param regionNameOrEncodedRegionName
   * @return region info, wrapped by a {@link CompletableFuture}
   */
  private CompletableFuture<HRegionInfo> getRegionInfo(byte[] regionNameOrEncodedRegionName) {
    if (regionNameOrEncodedRegionName == null) {
      return failedFuture(new IllegalArgumentException("Passed region name can't be null"));
    }

    if (Bytes.equals(regionNameOrEncodedRegionName,
      HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
        || Bytes.equals(regionNameOrEncodedRegionName,
          HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      return CompletableFuture.completedFuture(HRegionInfo.FIRST_META_REGIONINFO);
    }

    CompletableFuture<HRegionInfo> future = new CompletableFuture<>();
    getRegionLocation(regionNameOrEncodedRegionName).whenComplete((location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        future.complete(location.getRegionInfo());
      }
    });
    return future;
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

  private void verifySplitKeys(byte[][] splitKeys) {
    Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
    // Verify there are no duplicate split keys
    byte[] lastKey = null;
    for (byte[] splitKey : splitKeys) {
      if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
        throw new IllegalArgumentException("Empty split key must not be passed in the split keys.");
      }
      if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
        throw new IllegalArgumentException("All split keys must be unique, " + "found duplicate: "
            + Bytes.toStringBinary(splitKey) + ", " + Bytes.toStringBinary(lastKey));
      }
      lastKey = splitKey;
    }
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

    @Override
    String getOperationType() {
      return "CREATE";
    }
  }

  private class DeleteTableProcedureBiConsumer extends TableProcedureBiConsumer {

    DeleteTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "DELETE";
    }

    @Override
    void onFinished() {
      connection.getLocator().clearCache(this.tableName);
      super.onFinished();
    }
  }

  private class TruncateTableProcedureBiConsumer extends TableProcedureBiConsumer {

    TruncateTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "TRUNCATE";
    }
  }

  private class EnableTableProcedureBiConsumer extends TableProcedureBiConsumer {

    EnableTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "ENABLE";
    }
  }

  private class DisableTableProcedureBiConsumer extends TableProcedureBiConsumer {

    DisableTableProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "DISABLE";
    }
  }

  private class AddColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    AddColumnFamilyProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "ADD_COLUMN_FAMILY";
    }
  }

  private class DeleteColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    DeleteColumnFamilyProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "DELETE_COLUMN_FAMILY";
    }
  }

  private class ModifyColumnFamilyProcedureBiConsumer extends TableProcedureBiConsumer {

    ModifyColumnFamilyProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "MODIFY_COLUMN_FAMILY";
    }
  }

  private class CreateNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    CreateNamespaceProcedureBiConsumer(AsyncAdmin admin, String namespaceName) {
      super(admin, namespaceName);
    }

    @Override
    String getOperationType() {
      return "CREATE_NAMESPACE";
    }
  }

  private class DeleteNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    DeleteNamespaceProcedureBiConsumer(AsyncAdmin admin, String namespaceName) {
      super(admin, namespaceName);
    }

    @Override
    String getOperationType() {
      return "DELETE_NAMESPACE";
    }
  }

  private class ModifyNamespaceProcedureBiConsumer extends NamespaceProcedureBiConsumer {

    ModifyNamespaceProcedureBiConsumer(AsyncAdmin admin, String namespaceName) {
      super(admin, namespaceName);
    }

    @Override
    String getOperationType() {
      return "MODIFY_NAMESPACE";
    }
  }

  private class MergeTableRegionProcedureBiConsumer extends TableProcedureBiConsumer {

    MergeTableRegionProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "MERGE_REGIONS";
    }
  }

  private class SplitTableRegionProcedureBiConsumer extends  TableProcedureBiConsumer {

    SplitTableRegionProcedureBiConsumer(AsyncAdmin admin, TableName tableName) {
      super(admin, tableName);
    }

    @Override
    String getOperationType() {
      return "SPLIT_REGION";
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

  @Override
  public CompletableFuture<ClusterStatus> getClusterStatus() {
    return getClusterStatus(EnumSet.allOf(Option.class));
  }

  @Override
  public CompletableFuture<ClusterStatus>getClusterStatus(EnumSet<Option> options) {
    return this
        .<ClusterStatus> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetClusterStatusRequest, GetClusterStatusResponse, ClusterStatus> call(controller,
                stub, RequestConverter.buildGetClusterStatusRequest(options),
                (s, c, req, done) -> s.getClusterStatus(c, req, done),
                resp -> ProtobufUtil.convert(resp.getClusterStatus()))).call();
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<ShutdownRequest, ShutdownResponse, Void> call(controller,
            stub, ShutdownRequest.newBuilder().build(),
            (s, c, req, done) -> s.shutdown(c, req, done), resp -> null)).call();
  }

  @Override
  public CompletableFuture<Void> stopMaster() {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<StopMasterRequest, StopMasterResponse, Void> call(controller,
            stub, StopMasterRequest.newBuilder().build(),
            (s, c, req, done) -> s.stopMaster(c, req, done), resp -> null)).call();
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName serverName) {
    StopServerRequest request =
        RequestConverter.buildStopServerRequest("Called by admin client "
            + this.connection.toString());
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<StopServerRequest, StopServerResponse, Void> adminCall(
            controller, stub, request, (s, c, req, done) -> s.stopServer(controller, req, done),
            resp -> null)).serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> updateConfiguration(ServerName serverName) {
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this
              .<UpdateConfigurationRequest, UpdateConfigurationResponse, Void> adminCall(
                controller, stub, UpdateConfigurationRequest.getDefaultInstance(),
                (s, c, req, done) -> s.updateConfiguration(controller, req, done), resp -> null))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> updateConfiguration() {
    CompletableFuture<Void> future = new CompletableFuture<Void>();
    getClusterStatus().whenComplete(
      (status, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          List<CompletableFuture<Void>> futures = new ArrayList<>();
          status.getServers().forEach((server) -> futures.add(updateConfiguration(server)));
          futures.add(updateConfiguration(status.getMaster()));
          status.getBackupMasters().forEach(master -> futures.add(updateConfiguration(master)));
          CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
              .whenComplete((result, err2) -> {
                if (err2 != null) {
                  future.completeExceptionally(err2);
                } else {
                  future.complete(result);
                }
              });
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> rollWALWriter(ServerName serverName) {
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<RollWALWriterRequest, RollWALWriterResponse, Void> adminCall(
            controller, stub, RequestConverter.buildRollWALWriterRequest(),
            (s, c, req, done) -> s.rollWALWriter(controller, req, done), resp -> null))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> clearCompactionQueues(ServerName serverName, Set<String> queues) {
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this
              .<ClearCompactionQueuesRequest, ClearCompactionQueuesResponse, Void> adminCall(
                controller, stub, RequestConverter.buildClearCompactionQueuesRequest(queues), (s,
                    c, req, done) -> s.clearCompactionQueues(controller, req, done), resp -> null))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<List<SecurityCapability>> getSecurityCapabilities() {
    return this
        .<List<SecurityCapability>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SecurityCapabilitiesRequest, SecurityCapabilitiesResponse, List<SecurityCapability>> call(
                controller, stub, SecurityCapabilitiesRequest.newBuilder().build(), (s, c, req,
                    done) -> s.getSecurityCapabilities(c, req, done), (resp) -> ProtobufUtil
                    .toSecurityCapabilityList(resp.getCapabilitiesList()))).call();
  }

  @Override
  public CompletableFuture<List<RegionLoad>> getRegionLoads(ServerName serverName,
      Optional<TableName> tableName) {
    return this
        .<List<RegionLoad>> newAdminCaller()
        .action(
          (controller, stub) -> this
              .<GetRegionLoadRequest, GetRegionLoadResponse, List<RegionLoad>> adminCall(
                controller, stub, RequestConverter.buildGetRegionLoadRequest(tableName), (s, c,
                    req, done) -> s.getRegionLoad(controller, req, done),
                ProtobufUtil::getRegionLoadInfo)).serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Boolean> isMasterInMaintenanceMode() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsInMaintenanceModeRequest, IsInMaintenanceModeResponse, Boolean> call(controller,
                stub, IsInMaintenanceModeRequest.newBuilder().build(),
                (s, c, req, done) -> s.isMasterInMaintenanceMode(c, req, done),
                resp -> resp.getInMaintenanceMode())).call();
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(TableName tableName) {
    CompletableFuture<CompactionState> future = new CompletableFuture<>();
    getTableHRegionLocations(tableName).whenComplete(
      (locations, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        List<CompactionState> regionStates = new ArrayList<>();
        List<CompletableFuture<CompactionState>> futures = new ArrayList<>();
        locations.stream().filter(loc -> loc.getServerName() != null)
            .filter(loc -> loc.getRegionInfo() != null)
            .filter(loc -> !loc.getRegionInfo().isOffline())
            .map(loc -> loc.getRegionInfo().getRegionName()).forEach(region -> {
              futures.add(getCompactionStateForRegion(region).whenComplete((regionState, err2) -> {
                // If any region compaction state is MAJOR_AND_MINOR
                // the table compaction state is MAJOR_AND_MINOR, too.
                if (err2 != null) {
                  future.completeExceptionally(err2);
                } else if (regionState == CompactionState.MAJOR_AND_MINOR) {

                  future.complete(regionState);
                } else {
                  regionStates.add(regionState);
                }
              }));
            });
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
            .whenComplete((ret, err3) -> {
              // If future not completed, check all regions's compaction state
              if (!future.isCompletedExceptionally() && !future.isDone()) {
                CompactionState state = CompactionState.NONE;
                for (CompactionState regionState : regionStates) {
                  switch (regionState) {
                  case MAJOR:
                    if (state == CompactionState.MINOR) {
                      future.complete(CompactionState.MAJOR_AND_MINOR);
                    } else {
                      state = CompactionState.MAJOR;
                    }
                    break;
                  case MINOR:
                    if (state == CompactionState.MAJOR) {
                      future.complete(CompactionState.MAJOR_AND_MINOR);
                    } else {
                      state = CompactionState.MINOR;
                    }
                    break;
                  case NONE:
                  default:
                  }
                  if (!future.isDone()) {
                    future.complete(state);
                  }
                }
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionStateForRegion(byte[] regionName) {
    CompletableFuture<CompactionState> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }
        this.<GetRegionInfoResponse> newAdminCaller()
            .action(
              (controller, stub) -> this
                  .<GetRegionInfoRequest, GetRegionInfoResponse, GetRegionInfoResponse> adminCall(
                    controller, stub, RequestConverter.buildGetRegionInfoRequest(location
                        .getRegionInfo().getRegionName(), true), (s, c, req, done) -> s
                        .getRegionInfo(controller, req, done), resp -> resp))
            .serverName(serverName).call().whenComplete((resp2, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                if (resp2.hasCompactionState()) {
                  future.complete(ProtobufUtil.createCompactionState(resp2.getCompactionState()));
                } else {
                  future.complete(CompactionState.NONE);
                }
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestamp(TableName tableName) {
    MajorCompactionTimestampRequest request =
        MajorCompactionTimestampRequest.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
    return this
        .<Optional<Long>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<MajorCompactionTimestampRequest, MajorCompactionTimestampResponse, Optional<Long>> call(
                controller, stub, request,
                (s, c, req, done) -> s.getLastMajorCompactionTimestamp(c, req, done),
                ProtobufUtil::toOptionalTimestamp)).call();
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestampForRegion(
      byte[] regionName) {
    CompletableFuture<Optional<Long>> future = new CompletableFuture<>();
    // regionName may be a full region name or encoded region name, so getRegionInfo(byte[]) first
    getRegionInfo(regionName)
        .whenComplete(
          (region, err) -> {
            if (err != null) {
              future.completeExceptionally(err);
              return;
            }
            MajorCompactionTimestampForRegionRequest.Builder builder =
                MajorCompactionTimestampForRegionRequest.newBuilder();
            builder.setRegion(RequestConverter.buildRegionSpecifier(
              RegionSpecifierType.REGION_NAME, regionName));
            this.<Optional<Long>> newMasterCaller()
                .action(
                  (controller, stub) -> this
                      .<MajorCompactionTimestampForRegionRequest, MajorCompactionTimestampResponse, Optional<Long>> call(
                        controller, stub, builder.build(), (s, c, req, done) -> s
                            .getLastMajorCompactionTimestampForRegion(c, req, done),
                        ProtobufUtil::toOptionalTimestamp)).call()
                .whenComplete((timestamp, err2) -> {
                  if (err2 != null) {
                    future.completeExceptionally(err2);
                  } else {
                    future.complete(timestamp);
                  }
                });
          });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> setBalancerOn(final boolean on) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SetBalancerRunningRequest, SetBalancerRunningResponse, Boolean> call(controller,
                stub, RequestConverter.buildSetBalancerRunningRequest(on, true),
                (s, c, req, done) -> s.setBalancerRunning(c, req, done),
                (resp) -> resp.getPrevBalanceValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> balance(boolean forcible) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this.<BalanceRequest, BalanceResponse, Boolean> call(controller,
            stub, RequestConverter.buildBalanceRequest(forcible),
            (s, c, req, done) -> s.balance(c, req, done), (resp) -> resp.getBalancerRan())).call();
  }

  @Override
  public CompletableFuture<Boolean> isBalancerOn() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this.<IsBalancerEnabledRequest, IsBalancerEnabledResponse, Boolean> call(
            controller, stub, RequestConverter.buildIsBalancerEnabledRequest(),
            (s, c, req, done) -> s.isBalancerEnabled(c, req, done), (resp) -> resp.getEnabled()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> setNormalizerOn(boolean on) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SetNormalizerRunningRequest, SetNormalizerRunningResponse, Boolean> call(
                controller, stub, RequestConverter.buildSetNormalizerRunningRequest(on), (s, c,
                    req, done) -> s.setNormalizerRunning(c, req, done), (resp) -> resp
                    .getPrevNormalizerValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> isNormalizerOn() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsNormalizerEnabledRequest, IsNormalizerEnabledResponse, Boolean> call(controller,
                stub, RequestConverter.buildIsNormalizerEnabledRequest(),
                (s, c, req, done) -> s.isNormalizerEnabled(c, req, done),
                (resp) -> resp.getEnabled())).call();
  }

  @Override
  public CompletableFuture<Boolean> normalize() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this.<NormalizeRequest, NormalizeResponse, Boolean> call(
            controller, stub, RequestConverter.buildNormalizeRequest(),
            (s, c, req, done) -> s.normalize(c, req, done), (resp) -> resp.getNormalizerRan()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> setCleanerChoreOn(boolean enabled) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SetCleanerChoreRunningRequest, SetCleanerChoreRunningResponse, Boolean> call(
                controller, stub, RequestConverter.buildSetCleanerChoreRunningRequest(enabled), (s,
                    c, req, done) -> s.setCleanerChoreRunning(c, req, done), (resp) -> resp
                    .getPrevValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> isCleanerChoreOn() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsCleanerChoreEnabledRequest, IsCleanerChoreEnabledResponse, Boolean> call(
                controller, stub, RequestConverter.buildIsCleanerChoreEnabledRequest(), (s, c, req,
                    done) -> s.isCleanerChoreEnabled(c, req, done), (resp) -> resp.getValue()))
        .call();
  }

  @Override
  public CompletableFuture<Boolean> runCleanerChore() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<RunCleanerChoreRequest, RunCleanerChoreResponse, Boolean> call(controller, stub,
                RequestConverter.buildRunCleanerChoreRequest(),
                (s, c, req, done) -> s.runCleanerChore(c, req, done),
                (resp) -> resp.getCleanerChoreRan())).call();
  }

  @Override
  public CompletableFuture<Boolean> setCatalogJanitorOn(boolean enabled) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<EnableCatalogJanitorRequest, EnableCatalogJanitorResponse, Boolean> call(
                controller, stub, RequestConverter.buildEnableCatalogJanitorRequest(enabled), (s,
                    c, req, done) -> s.enableCatalogJanitor(c, req, done), (resp) -> resp
                    .getPrevValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorOn() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsCatalogJanitorEnabledRequest, IsCatalogJanitorEnabledResponse, Boolean> call(
                controller, stub, RequestConverter.buildIsCatalogJanitorEnabledRequest(), (s, c,
                    req, done) -> s.isCatalogJanitorEnabled(c, req, done), (resp) -> resp
                    .getValue())).call();
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    return this
        .<Integer> newMasterCaller()
        .action(
          (controller, stub) -> this.<RunCatalogScanRequest, RunCatalogScanResponse, Integer> call(
            controller, stub, RequestConverter.buildCatalogScanRequest(),
            (s, c, req, done) -> s.runCatalogScan(c, req, done), (resp) -> resp.getScanResult()))
        .call();
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      CoprocessorCallable<S, R> callable) {
    MasterCoprocessorRpcChannelImpl channel =
        new MasterCoprocessorRpcChannelImpl(this.<Message> newMasterCaller());
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
      CoprocessorCallable<S, R> callable, ServerName serverName) {
    RegionServerCoprocessorRpcChannelImpl channel =
        new RegionServerCoprocessorRpcChannelImpl(this.<Message> newServerCaller().serverName(
          serverName));
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

  private <T> ServerRequestCallerBuilder<T> newServerCaller() {
    return this.connection.callerFactory.<T> serverRequest()
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .pause(pauseNs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt);
  }
}
