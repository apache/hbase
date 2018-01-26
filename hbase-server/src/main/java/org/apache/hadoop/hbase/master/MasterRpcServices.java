/**
 *
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil.NonceProcedureRunnable;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse.NamespaceQuotaSnapshot;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse.TableQuotaSnapshot;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse.RegionSizes;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.FileArchiveNotificationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.FileArchiveNotificationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUseReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUseReportResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RemoteProcedureResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ReplicationState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.TransitReplicationPeerSyncReplicationStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.TransitReplicationPeerSyncReplicationStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * Implements the master RPC services.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class MasterRpcServices extends RSRpcServices
      implements MasterService.BlockingInterface, RegionServerStatusService.BlockingInterface,
        LockService.BlockingInterface {
  private static final Logger LOG = LoggerFactory.getLogger(MasterRpcServices.class.getName());

  private final HMaster master;

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  private RegionServerStartupResponse.Builder createConfigurationSubset() {
    RegionServerStartupResponse.Builder resp = addConfig(
      RegionServerStartupResponse.newBuilder(), HConstants.HBASE_DIR);
    resp = addConfig(resp, "fs.defaultFS");
    return addConfig(resp, "hbase.master.info.port");
  }

  private RegionServerStartupResponse.Builder addConfig(
      final RegionServerStartupResponse.Builder resp, final String key) {
    NameStringPair.Builder entry = NameStringPair.newBuilder()
      .setName(key)
      .setValue(master.getConfiguration().get(key));
    resp.addMapEntries(entry.build());
    return resp;
  }

  public MasterRpcServices(HMaster m) throws IOException {
    super(m);
    master = m;
  }

  @Override
  protected RpcServerInterface createRpcServer(Server server, Configuration conf,
      RpcSchedulerFactory rpcSchedulerFactory, InetSocketAddress bindAddress, String name)
      throws IOException {
    // RpcServer at HM by default enable ByteBufferPool iff HM having user table region in it
    boolean reservoirEnabled = conf.getBoolean(RESERVOIR_ENABLED_KEY,
        (LoadBalancer.isTablesOnMaster(conf) && !LoadBalancer.isSystemTablesOnlyOnMaster(conf)));
    try {
      return RpcServerFactory.createRpcServer(server, name, getServices(),
          bindAddress, // use final bindAddress for this server.
          conf, rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(be.getMessage() + ". To switch ports use the '"
          + HConstants.MASTER_PORT + "' configuration property.",
          be.getCause() != null ? be.getCause() : be);
    }
  }

  @Override
  protected PriorityFunction createPriority() {
    return new MasterAnnotationReadingPriorityFunction(this);
  }

  /**
   * Checks for the following pre-checks in order:
   * <ol>
   *   <li>Master is initialized</li>
   *   <li>Rpc caller has admin permissions</li>
   * </ol>
   * @param requestName name of rpc request. Used in reporting failures to provide context.
   * @throws ServiceException If any of the above listed pre-check fails.
   */
  private void rpcPreCheck(String requestName) throws ServiceException {
    try {
      master.checkInitialized();
      requirePermission(requestName, Permission.Action.ADMIN);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  enum BalanceSwitchMode {
    SYNC,
    ASYNC
  }

  /**
   * Assigns balancer switch according to BalanceSwitchMode
   * @param b new balancer switch
   * @param mode BalanceSwitchMode
   * @return old balancer switch
   */
  boolean switchBalancer(final boolean b, BalanceSwitchMode mode) throws IOException {
    boolean oldValue = master.loadBalancerTracker.isBalancerOn();
    boolean newValue = b;
    try {
      if (master.cpHost != null) {
        master.cpHost.preBalanceSwitch(newValue);
      }
      try {
        if (mode == BalanceSwitchMode.SYNC) {
          synchronized (master.getLoadBalancer()) {
            master.loadBalancerTracker.setBalancerOn(newValue);
          }
        } else {
          master.loadBalancerTracker.setBalancerOn(newValue);
        }
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
      LOG.info(master.getClientIdAuditPrefix() + " set balanceSwitch=" + newValue);
      if (master.cpHost != null) {
        master.cpHost.postBalanceSwitch(oldValue, newValue);
      }
    } catch (IOException ioe) {
      LOG.warn("Error flipping balance switch", ioe);
    }
    return oldValue;
  }

  boolean synchronousBalanceSwitch(final boolean b) throws IOException {
    return switchBalancer(b, BalanceSwitchMode.SYNC);
  }

  /**
   * @return list of blocking services and their security info classes that this server supports
   */
  @Override
  protected List<BlockingServiceAndInterface> getServices() {
    List<BlockingServiceAndInterface> bssi = new ArrayList<>(5);
    bssi.add(new BlockingServiceAndInterface(
      MasterService.newReflectiveBlockingService(this),
      MasterService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(
      RegionServerStatusService.newReflectiveBlockingService(this),
      RegionServerStatusService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(LockService.newReflectiveBlockingService(this),
        LockService.BlockingInterface.class));
    bssi.addAll(super.getServices());
    return bssi;
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public GetLastFlushedSequenceIdResponse getLastFlushedSequenceId(RpcController controller,
      GetLastFlushedSequenceIdRequest request) throws ServiceException {
    try {
      master.checkServiceStarted();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    byte[] encodedRegionName = request.getRegionName().toByteArray();
    RegionStoreSequenceIds ids = master.getServerManager()
      .getLastFlushedSequenceId(encodedRegionName);
    return ResponseConverter.buildGetLastFlushedSequenceIdResponse(ids);
  }

  @Override
  public RegionServerReportResponse regionServerReport(
      RpcController controller, RegionServerReportRequest request) throws ServiceException {
    try {
      master.checkServiceStarted();
      int version = VersionInfoUtil.getCurrentClientVersionNumber();
      ClusterStatusProtos.ServerLoad sl = request.getLoad();
      ServerName serverName = ProtobufUtil.toServerName(request.getServer());
      ServerMetrics oldLoad = master.getServerManager().getLoad(serverName);
      ServerMetrics newLoad = ServerMetricsBuilder.toServerMetrics(serverName, version, sl);
      master.getServerManager().regionServerReport(serverName, newLoad);
      master.getAssignmentManager()
          .reportOnlineRegions(serverName, newLoad.getRegionMetrics().keySet());
      if (sl != null && master.metricsMaster != null) {
        // Up our metrics.
        master.metricsMaster.incrementRequests(sl.getTotalNumberOfRequests()
            - (oldLoad != null ? oldLoad.getRequestCount() : 0));
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return RegionServerReportResponse.newBuilder().build();
  }

  @Override
  public RegionServerStartupResponse regionServerStartup(
      RpcController controller, RegionServerStartupRequest request) throws ServiceException {
    // Register with server manager
    try {
      master.checkServiceStarted();
      int version = VersionInfoUtil.getCurrentClientVersionNumber();
      InetAddress ia = master.getRemoteInetAddress(
        request.getPort(), request.getServerStartCode());
      // if regionserver passed hostname to use,
      // then use it instead of doing a reverse DNS lookup
      ServerName rs = master.getServerManager().regionServerStartup(request, version, ia);

      // Send back some config info
      RegionServerStartupResponse.Builder resp = createConfigurationSubset();
      NameStringPair.Builder entry = NameStringPair.newBuilder()
        .setName(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)
        .setValue(rs.getHostname());
      resp.addMapEntries(entry.build());

      return resp.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ReportRSFatalErrorResponse reportRSFatalError(
      RpcController controller, ReportRSFatalErrorRequest request) throws ServiceException {
    String errorText = request.getErrorMessage();
    ServerName sn = ProtobufUtil.toServerName(request.getServer());
    String msg = "Region server " + sn
      + " reported a fatal error:\n" + errorText;
    LOG.error(msg);
    master.rsFatals.add(msg);
    return ReportRSFatalErrorResponse.newBuilder().build();
  }

  @Override
  public AddColumnResponse addColumn(RpcController controller,
      AddColumnRequest req) throws ServiceException {
    try {
      long procId = master.addColumn(
          ProtobufUtil.toTableName(req.getTableName()),
          ProtobufUtil.toColumnFamilyDescriptor(req.getColumnFamilies()),
          req.getNonceGroup(),
          req.getNonce());
      if (procId == -1) {
        // This mean operation was not performed in server, so do not set any procId
        return AddColumnResponse.newBuilder().build();
      } else {
        return AddColumnResponse.newBuilder().setProcId(procId).build();
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public AssignRegionResponse assignRegion(RpcController controller,
      AssignRegionRequest req) throws ServiceException {
    try {
      master.checkInitialized();

      final RegionSpecifierType type = req.getRegion().getType();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("assignRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }

      final byte[] regionName = req.getRegion().getValue().toByteArray();
      final RegionInfo regionInfo = master.getAssignmentManager().getRegionInfo(regionName);
      if (regionInfo == null) throw new UnknownRegionException(Bytes.toStringBinary(regionName));

      final AssignRegionResponse arr = AssignRegionResponse.newBuilder().build();
      if (master.cpHost != null) {
        master.cpHost.preAssign(regionInfo);
      }
      LOG.info(master.getClientIdAuditPrefix() + " assign " + regionInfo.getRegionNameAsString());
      master.getAssignmentManager().assign(regionInfo);
      if (master.cpHost != null) {
        master.cpHost.postAssign(regionInfo);
      }
      return arr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }


  @Override
  public BalanceResponse balance(RpcController controller,
      BalanceRequest request) throws ServiceException {
    try {
      return BalanceResponse.newBuilder().setBalancerRan(master.balance(
        request.hasForce() ? request.getForce() : false)).build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public CreateNamespaceResponse createNamespace(RpcController controller,
     CreateNamespaceRequest request) throws ServiceException {
    try {
      long procId = master.createNamespace(
        ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()),
        request.getNonceGroup(),
        request.getNonce());
      return CreateNamespaceResponse.newBuilder().setProcId(procId).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CreateTableResponse createTable(RpcController controller, CreateTableRequest req)
  throws ServiceException {
    TableDescriptor tableDescriptor = ProtobufUtil.toTableDescriptor(req.getTableSchema());
    byte [][] splitKeys = ProtobufUtil.getSplitKeysArray(req);
    try {
      long procId =
          master.createTable(tableDescriptor, splitKeys, req.getNonceGroup(), req.getNonce());
      return CreateTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public DeleteColumnResponse deleteColumn(RpcController controller,
      DeleteColumnRequest req) throws ServiceException {
    try {
      long procId = master.deleteColumn(
        ProtobufUtil.toTableName(req.getTableName()),
        req.getColumnName().toByteArray(),
        req.getNonceGroup(),
        req.getNonce());
      if (procId == -1) {
        // This mean operation was not performed in server, so do not set any procId
        return DeleteColumnResponse.newBuilder().build();
      } else {
        return DeleteColumnResponse.newBuilder().setProcId(procId).build();
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public DeleteNamespaceResponse deleteNamespace(RpcController controller,
      DeleteNamespaceRequest request) throws ServiceException {
    try {
      long procId = master.deleteNamespace(
        request.getNamespaceName(),
        request.getNonceGroup(),
        request.getNonce());
      return DeleteNamespaceResponse.newBuilder().setProcId(procId).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Execute Delete Snapshot operation.
   * @return DeleteSnapshotResponse (a protobuf wrapped void) if the snapshot existed and was
   *    deleted properly.
   * @throws ServiceException wrapping SnapshotDoesNotExistException if specified snapshot did not
   *    exist.
   */
  @Override
  public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
      DeleteSnapshotRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      master.snapshotManager.checkSnapshotSupport();

      LOG.info(master.getClientIdAuditPrefix() + " delete " + request.getSnapshot());
      master.snapshotManager.deleteSnapshot(request.getSnapshot());
      return DeleteSnapshotResponse.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteTableResponse deleteTable(RpcController controller,
      DeleteTableRequest request) throws ServiceException {
    try {
      long procId = master.deleteTable(ProtobufUtil.toTableName(
          request.getTableName()), request.getNonceGroup(), request.getNonce());
      return DeleteTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public TruncateTableResponse truncateTable(RpcController controller, TruncateTableRequest request)
      throws ServiceException {
    try {
      long procId = master.truncateTable(
        ProtobufUtil.toTableName(request.getTableName()),
        request.getPreserveSplits(),
        request.getNonceGroup(),
        request.getNonce());
      return TruncateTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public DisableTableResponse disableTable(RpcController controller,
      DisableTableRequest request) throws ServiceException {
    try {
      long procId = master.disableTable(
        ProtobufUtil.toTableName(request.getTableName()),
        request.getNonceGroup(),
        request.getNonce());
      return DisableTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController c,
      EnableCatalogJanitorRequest req) throws ServiceException {
    rpcPreCheck("enableCatalogJanitor");
    return EnableCatalogJanitorResponse.newBuilder().setPrevValue(
      master.catalogJanitorChore.setEnabled(req.getEnable())).build();
  }

  @Override
  public SetCleanerChoreRunningResponse setCleanerChoreRunning(
    RpcController c, SetCleanerChoreRunningRequest req) throws ServiceException {
    rpcPreCheck("setCleanerChoreRunning");

    boolean prevValue =
      master.getLogCleaner().getEnabled() && master.getHFileCleaner().getEnabled();
    master.getLogCleaner().setEnabled(req.getOn());
    master.getHFileCleaner().setEnabled(req.getOn());
    return SetCleanerChoreRunningResponse.newBuilder().setPrevValue(prevValue).build();
  }

  @Override
  public EnableTableResponse enableTable(RpcController controller,
      EnableTableRequest request) throws ServiceException {
    try {
      long procId = master.enableTable(
        ProtobufUtil.toTableName(request.getTableName()),
        request.getNonceGroup(),
        request.getNonce());
      return EnableTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public MergeTableRegionsResponse mergeTableRegions(
      RpcController c, MergeTableRegionsRequest request) throws ServiceException {
    try {
      master.checkInitialized();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    RegionStates regionStates = master.getAssignmentManager().getRegionStates();

    assert(request.getRegionCount() == 2);
    RegionInfo[] regionsToMerge = new RegionInfo[request.getRegionCount()];
    for (int i = 0; i < request.getRegionCount(); i++) {
      final byte[] encodedNameOfRegion = request.getRegion(i).getValue().toByteArray();
      if (request.getRegion(i).getType() != RegionSpecifierType.ENCODED_REGION_NAME) {
        LOG.warn("MergeRegions specifier type: expected: "
          + RegionSpecifierType.ENCODED_REGION_NAME + " actual: region " + i + " ="
          + request.getRegion(i).getType());
      }
      RegionState regionState = regionStates.getRegionState(Bytes.toString(encodedNameOfRegion));
      if (regionState == null) {
        throw new ServiceException(
          new UnknownRegionException(Bytes.toStringBinary(encodedNameOfRegion)));
      }
      regionsToMerge[i] = regionState.getRegion();
    }

    try {
      long procId = master.mergeRegions(
        regionsToMerge,
        request.getForcible(),
        request.getNonceGroup(),
        request.getNonce());
      return MergeTableRegionsResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public SplitTableRegionResponse splitRegion(final RpcController controller,
      final SplitTableRegionRequest request) throws ServiceException {
    try {
      long procId = master.splitRegion(
        ProtobufUtil.toRegionInfo(request.getRegionInfo()),
        request.hasSplitRow() ? request.getSplitRow().toByteArray() : null,
        request.getNonceGroup(),
        request.getNonce());
      return SplitTableRegionResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public ClientProtos.CoprocessorServiceResponse execMasterService(final RpcController controller,
      final ClientProtos.CoprocessorServiceRequest request) throws ServiceException {
    rpcPreCheck("execMasterService");
    try {
      ServerRpcController execController = new ServerRpcController();
      ClientProtos.CoprocessorServiceCall call = request.getCall();
      String serviceName = call.getServiceName();
      String methodName = call.getMethodName();
      if (!master.coprocessorServiceHandlers.containsKey(serviceName)) {
        throw new UnknownProtocolException(null,
          "No registered Master Coprocessor Endpoint found for " + serviceName +
          ". Has it been enabled?");
      }

      com.google.protobuf.Service service = master.coprocessorServiceHandlers.get(serviceName);
      com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();
      com.google.protobuf.Descriptors.MethodDescriptor methodDesc =
          CoprocessorRpcUtils.getMethodDescriptor(methodName, serviceDesc);

      com.google.protobuf.Message execRequest =
          CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());
      final com.google.protobuf.Message.Builder responseBuilder =
          service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, execController, execRequest,
        (message) -> {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
        });
      com.google.protobuf.Message execResult = responseBuilder.build();
      if (execController.getFailedOn() != null) {
        throw execController.getFailedOn();
      }
      return CoprocessorRpcUtils.getResponse(execResult, HConstants.EMPTY_BYTE_ARRAY);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Triggers an asynchronous attempt to run a distributed procedure.
   * {@inheritDoc}
   */
  @Override
  public ExecProcedureResponse execProcedure(RpcController controller,
      ExecProcedureRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm = master.getMasterProcedureManagerHost().getProcedureManager(
        desc.getSignature());
      if (mpm == null) {
        throw new ServiceException(new DoNotRetryIOException("The procedure is not registered: "
          + desc.getSignature()));
      }
      LOG.info(master.getClientIdAuditPrefix() + " procedure request for: " + desc.getSignature());
      mpm.checkPermissions(desc, accessChecker, RpcServer.getRequestUser().orElse(null));
      mpm.execProcedure(desc);
      // send back the max amount of time the client should wait for the procedure
      // to complete
      long waitTime = SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME;
      return ExecProcedureResponse.newBuilder().setExpectedTimeout(
        waitTime).build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Triggers a synchronous attempt to run a distributed procedure and sets
   * return data in response.
   * {@inheritDoc}
   */
  @Override
  public ExecProcedureResponse execProcedureWithRet(RpcController controller,
      ExecProcedureRequest request) throws ServiceException {
    rpcPreCheck("execProcedureWithRet");
    try {
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm =
        master.getMasterProcedureManagerHost().getProcedureManager(desc.getSignature());
      if (mpm == null) {
        throw new ServiceException("The procedure is not registered: " + desc.getSignature());
      }
      LOG.info(master.getClientIdAuditPrefix() + " procedure request for: " + desc.getSignature());
      byte[] data = mpm.execProcedureWithRet(desc);
      ExecProcedureResponse.Builder builder = ExecProcedureResponse.newBuilder();
      // set return data if available
      if (data != null) {
        builder.setReturnData(UnsafeByteOperations.unsafeWrap(data));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller,
      GetClusterStatusRequest req) throws ServiceException {
    GetClusterStatusResponse.Builder response = GetClusterStatusResponse.newBuilder();
    try {
      master.checkInitialized();
      response.setClusterStatus(ClusterMetricsBuilder.toClusterStatus(
        master.getClusterMetrics(ClusterMetricsBuilder.toOptions(req.getOptionsList()))));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  /**
   * List the currently available/stored snapshots. Any in-progress snapshots are ignored
   */
  @Override
  public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller,
      GetCompletedSnapshotsRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      GetCompletedSnapshotsResponse.Builder builder = GetCompletedSnapshotsResponse.newBuilder();
      List<SnapshotDescription> snapshots = master.snapshotManager.getCompletedSnapshots();

      // convert to protobuf
      for (SnapshotDescription snapshot : snapshots) {
        builder.addSnapshots(snapshot);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetNamespaceDescriptorResponse getNamespaceDescriptor(
      RpcController controller, GetNamespaceDescriptorRequest request)
      throws ServiceException {
    try {
      return GetNamespaceDescriptorResponse.newBuilder()
        .setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(
            master.getNamespace(request.getNamespaceName())))
        .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Get the number of regions of the table that have been updated by the alter.
   *
   * @return Pair indicating the number of regions updated Pair.getFirst is the
   *         regions that are yet to be updated Pair.getSecond is the total number
   *         of regions of the table
   * @throws ServiceException
   */
  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(
      RpcController controller, GetSchemaAlterStatusRequest req) throws ServiceException {
    // TODO: currently, we query using the table name on the client side. this
    // may overlap with other table operations or the table operation may
    // have completed before querying this API. We need to refactor to a
    // transaction system in the future to avoid these ambiguities.
    TableName tableName = ProtobufUtil.toTableName(req.getTableName());

    try {
      master.checkInitialized();
      Pair<Integer,Integer> pair = master.getAssignmentManager().getReopenStatus(tableName);
      GetSchemaAlterStatusResponse.Builder ret = GetSchemaAlterStatusResponse.newBuilder();
      ret.setYetToUpdateRegions(pair.getFirst());
      ret.setTotalRegions(pair.getSecond());
      return ret.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * Get list of TableDescriptors for requested tables.
   * @param c Unused (set to null).
   * @param req GetTableDescriptorsRequest that contains:
   * - tableNames: requested tables, or if empty, all are requested
   * @return GetTableDescriptorsResponse
   * @throws ServiceException
   */
  @Override
  public GetTableDescriptorsResponse getTableDescriptors(RpcController c,
      GetTableDescriptorsRequest req) throws ServiceException {
    try {
      master.checkInitialized();

      final String regex = req.hasRegex() ? req.getRegex() : null;
      final String namespace = req.hasNamespace() ? req.getNamespace() : null;
      List<TableName> tableNameList = null;
      if (req.getTableNamesCount() > 0) {
        tableNameList = new ArrayList<TableName>(req.getTableNamesCount());
        for (HBaseProtos.TableName tableNamePB: req.getTableNamesList()) {
          tableNameList.add(ProtobufUtil.toTableName(tableNamePB));
        }
      }

      List<TableDescriptor> descriptors = master.listTableDescriptors(namespace, regex,
          tableNameList, req.getIncludeSysTables());

      GetTableDescriptorsResponse.Builder builder = GetTableDescriptorsResponse.newBuilder();
      if (descriptors != null && descriptors.size() > 0) {
        // Add the table descriptors to the response
        for (TableDescriptor htd: descriptors) {
          builder.addTableSchema(ProtobufUtil.toTableSchema(htd));
        }
      }
      return builder.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * Get list of userspace table names
   * @param controller Unused (set to null).
   * @param req GetTableNamesRequest
   * @return GetTableNamesResponse
   * @throws ServiceException
   */
  @Override
  public GetTableNamesResponse getTableNames(RpcController controller,
      GetTableNamesRequest req) throws ServiceException {
    try {
      master.checkServiceStarted();

      final String regex = req.hasRegex() ? req.getRegex() : null;
      final String namespace = req.hasNamespace() ? req.getNamespace() : null;
      List<TableName> tableNames = master.listTableNames(namespace, regex,
          req.getIncludeSysTables());

      GetTableNamesResponse.Builder builder = GetTableNamesResponse.newBuilder();
      if (tableNames != null && tableNames.size() > 0) {
        // Add the table names to the response
        for (TableName table: tableNames) {
          builder.addTableNames(ProtobufUtil.toProtoTableName(table));
        }
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTableStateResponse getTableState(RpcController controller,
      GetTableStateRequest request) throws ServiceException {
    try {
      master.checkServiceStarted();
      TableName tableName = ProtobufUtil.toTableName(request.getTableName());
      TableState ts = master.getTableStateManager().getTableState(tableName);
      GetTableStateResponse.Builder builder = GetTableStateResponse.newBuilder();
      builder.setTableState(ts.convert());
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController c,
      IsCatalogJanitorEnabledRequest req) throws ServiceException {
    return IsCatalogJanitorEnabledResponse.newBuilder().setValue(
      master.isCatalogJanitorEnabled()).build();
  }

  @Override
  public IsCleanerChoreEnabledResponse isCleanerChoreEnabled(RpcController c,
                                                             IsCleanerChoreEnabledRequest req)
    throws ServiceException {
    return IsCleanerChoreEnabledResponse.newBuilder().setValue(master.isCleanerChoreEnabled())
                                        .build();
  }

  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController c,
      IsMasterRunningRequest req) throws ServiceException {
    try {
      master.checkServiceStarted();
      return IsMasterRunningResponse.newBuilder().setIsMasterRunning(
        !master.isStopped()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Checks if the specified procedure is done.
   * @return true if the procedure is done, false if the procedure is in the process of completing
   * @throws ServiceException if invalid procedure or failed procedure with progress failure reason.
   */
  @Override
  public IsProcedureDoneResponse isProcedureDone(RpcController controller,
      IsProcedureDoneRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm = master.getMasterProcedureManagerHost().getProcedureManager(
        desc.getSignature());
      if (mpm == null) {
        throw new ServiceException("The procedure is not registered: "
          + desc.getSignature());
      }
      LOG.debug("Checking to see if procedure from request:"
        + desc.getSignature() + " is done");

      IsProcedureDoneResponse.Builder builder =
        IsProcedureDoneResponse.newBuilder();
      boolean done = mpm.isProcedureDone(desc);
      builder.setDone(done);
      return builder.build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Checks if the specified snapshot is done.
   * @return true if the snapshot is in file system ready to use,
   *   false if the snapshot is in the process of completing
   * @throws ServiceException wrapping UnknownSnapshotException if invalid snapshot, or
   *  a wrapped HBaseSnapshotException with progress failure reason.
   */
  @Override
  public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
      IsSnapshotDoneRequest request) throws ServiceException {
    LOG.debug("Checking to see if snapshot from request:" +
      ClientSnapshotDescriptionUtils.toString(request.getSnapshot()) + " is done");
    try {
      master.checkInitialized();
      IsSnapshotDoneResponse.Builder builder = IsSnapshotDoneResponse.newBuilder();
      boolean done = master.snapshotManager.isSnapshotDone(request.getSnapshot());
      builder.setDone(done);
      return builder.build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetProcedureResultResponse getProcedureResult(RpcController controller,
      GetProcedureResultRequest request) throws ServiceException {
    LOG.debug("Checking to see if procedure is done pid=" + request.getProcId());
    try {
      master.checkInitialized();
      GetProcedureResultResponse.Builder builder = GetProcedureResultResponse.newBuilder();
      long procId = request.getProcId();
      ProcedureExecutor<?> executor = master.getMasterProcedureExecutor();
      Procedure<?> result = executor.getResultOrProcedure(procId);
      if (result != null) {
        builder.setSubmittedTime(result.getSubmittedTime());
        builder.setLastUpdate(result.getLastUpdate());
        if (executor.isFinished(procId)) {
          builder.setState(GetProcedureResultResponse.State.FINISHED);
          if (result.isFailed()) {
            IOException exception = result.getException().unwrapRemoteIOException();
            builder.setException(ForeignExceptionUtil.toProtoForeignException(exception));
          }
          byte[] resultData = result.getResult();
          if (resultData != null) {
            builder.setResult(UnsafeByteOperations.unsafeWrap(resultData));
          }
          master.getMasterProcedureExecutor().removeResult(request.getProcId());
        } else {
          builder.setState(GetProcedureResultResponse.State.RUNNING);
        }
      } else {
        builder.setState(GetProcedureResultResponse.State.NOT_FOUND);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AbortProcedureResponse abortProcedure(
      RpcController rpcController, AbortProcedureRequest request) throws ServiceException {
    try {
      AbortProcedureResponse.Builder response = AbortProcedureResponse.newBuilder();
      boolean abortResult =
          master.abortProcedure(request.getProcId(), request.getMayInterruptIfRunning());
      response.setIsProcedureAborted(abortResult);
      return response.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController c,
      ListNamespaceDescriptorsRequest request) throws ServiceException {
    try {
      ListNamespaceDescriptorsResponse.Builder response =
        ListNamespaceDescriptorsResponse.newBuilder();
      for(NamespaceDescriptor ns: master.getNamespaces()) {
        response.addNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(ns));
      }
      return response.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetProceduresResponse getProcedures(
      RpcController rpcController,
      GetProceduresRequest request) throws ServiceException {
    try {
      final GetProceduresResponse.Builder response = GetProceduresResponse.newBuilder();
      for (Procedure<?> p: master.getProcedures()) {
        response.addProcedure(ProcedureUtil.convertToProtoProcedure(p));
      }
      return response.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetLocksResponse getLocks(
      RpcController controller,
      GetLocksRequest request) throws ServiceException {
    try {
      final GetLocksResponse.Builder builder = GetLocksResponse.newBuilder();

      for (LockedResource lockedResource: master.getLocks()) {
        builder.addLock(ProcedureUtil.convertToProtoLockedResource(lockedResource));
      }

      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(RpcController c,
      ListTableDescriptorsByNamespaceRequest request) throws ServiceException {
    try {
      ListTableDescriptorsByNamespaceResponse.Builder b =
          ListTableDescriptorsByNamespaceResponse.newBuilder();
      for (TableDescriptor htd : master
          .listTableDescriptorsByNamespace(request.getNamespaceName())) {
        b.addTableSchema(ProtobufUtil.toTableSchema(htd));
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListTableNamesByNamespaceResponse listTableNamesByNamespace(RpcController c,
      ListTableNamesByNamespaceRequest request) throws ServiceException {
    try {
      ListTableNamesByNamespaceResponse.Builder b =
        ListTableNamesByNamespaceResponse.newBuilder();
      for (TableName tableName: master.listTableNamesByNamespace(request.getNamespaceName())) {
        b.addTableName(ProtobufUtil.toProtoTableName(tableName));
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyColumnResponse modifyColumn(RpcController controller,
      ModifyColumnRequest req) throws ServiceException {
    try {
      long procId = master.modifyColumn(
        ProtobufUtil.toTableName(req.getTableName()),
        ProtobufUtil.toColumnFamilyDescriptor(req.getColumnFamilies()),
        req.getNonceGroup(),
        req.getNonce());
      if (procId == -1) {
        // This mean operation was not performed in server, so do not set any procId
        return ModifyColumnResponse.newBuilder().build();
      } else {
        return ModifyColumnResponse.newBuilder().setProcId(procId).build();
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ModifyNamespaceResponse modifyNamespace(RpcController controller,
      ModifyNamespaceRequest request) throws ServiceException {
    try {
      long procId = master.modifyNamespace(
        ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()),
        request.getNonceGroup(),
        request.getNonce());
      return ModifyNamespaceResponse.newBuilder().setProcId(procId).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyTableResponse modifyTable(RpcController controller,
      ModifyTableRequest req) throws ServiceException {
    try {
      long procId = master.modifyTable(
        ProtobufUtil.toTableName(req.getTableName()),
        ProtobufUtil.toTableDescriptor(req.getTableSchema()),
        req.getNonceGroup(),
        req.getNonce());
      return ModifyTableResponse.newBuilder().setProcId(procId).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public MoveRegionResponse moveRegion(RpcController controller,
      MoveRegionRequest req) throws ServiceException {
    final byte [] encodedRegionName = req.getRegion().getValue().toByteArray();
    RegionSpecifierType type = req.getRegion().getType();
    final byte [] destServerName = (req.hasDestServerName())?
      Bytes.toBytes(ProtobufUtil.toServerName(req.getDestServerName()).getServerName()):null;
    MoveRegionResponse mrr = MoveRegionResponse.newBuilder().build();

    if (type != RegionSpecifierType.ENCODED_REGION_NAME) {
      LOG.warn("moveRegion specifier type: expected: " + RegionSpecifierType.ENCODED_REGION_NAME
        + " actual: " + type);
    }

    try {
      master.checkInitialized();
      master.move(encodedRegionName, destServerName);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return mrr;
  }

  /**
   * Offline specified region from master's in-memory state. It will not attempt to
   * reassign the region as in unassign.
   *
   * This is a special method that should be used by experts or hbck.
   *
   */
  @Override
  public OfflineRegionResponse offlineRegion(RpcController controller,
      OfflineRegionRequest request) throws ServiceException {
    try {
      master.checkInitialized();

      final RegionSpecifierType type = request.getRegion().getType();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("moveRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }

      final byte[] regionName = request.getRegion().getValue().toByteArray();
      final RegionInfo hri = master.getAssignmentManager().getRegionInfo(regionName);
      if (hri == null) throw new UnknownRegionException(Bytes.toStringBinary(regionName));

      if (master.cpHost != null) {
        master.cpHost.preRegionOffline(hri);
      }
      LOG.info(master.getClientIdAuditPrefix() + " offline " + hri.getRegionNameAsString());
      master.getAssignmentManager().offlineRegion(hri);
      if (master.cpHost != null) {
        master.cpHost.postRegionOffline(hri);
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return OfflineRegionResponse.newBuilder().build();
  }

  /**
   * Execute Restore/Clone snapshot operation.
   *
   * <p>If the specified table exists a "Restore" is executed, replacing the table
   * schema and directory data with the content of the snapshot.
   * The table must be disabled, or a UnsupportedOperationException will be thrown.
   *
   * <p>If the table doesn't exist a "Clone" is executed, a new table is created
   * using the schema at the time of the snapshot, and the content of the snapshot.
   *
   * <p>The restore/clone operation does not require copying HFiles. Since HFiles
   * are immutable the table can point to and use the same files as the original one.
   */
  @Override
  public RestoreSnapshotResponse restoreSnapshot(RpcController controller,
      RestoreSnapshotRequest request) throws ServiceException {
    try {
      long procId = master.restoreSnapshot(request.getSnapshot(), request.getNonceGroup(),
        request.getNonce(), request.getRestoreACL());
      return RestoreSnapshotResponse.newBuilder().setProcId(procId).build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RunCatalogScanResponse runCatalogScan(RpcController c,
      RunCatalogScanRequest req) throws ServiceException {
    rpcPreCheck("runCatalogScan");
    try {
      return ResponseConverter.buildRunCatalogScanResponse(master.catalogJanitorChore.scan());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public RunCleanerChoreResponse runCleanerChore(RpcController c, RunCleanerChoreRequest req)
    throws ServiceException {
    rpcPreCheck("runCleanerChore");
    boolean result = master.getHFileCleaner().runCleaner() && master.getLogCleaner().runCleaner();
    return ResponseConverter.buildRunCleanerChoreResponse(result);
  }

  @Override
  public SetBalancerRunningResponse setBalancerRunning(RpcController c,
      SetBalancerRunningRequest req) throws ServiceException {
    try {
      master.checkInitialized();
      boolean prevValue = (req.getSynchronous())?
        synchronousBalanceSwitch(req.getOn()) : master.balanceSwitch(req.getOn());
      return SetBalancerRunningResponse.newBuilder().setPrevBalanceValue(prevValue).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ShutdownResponse shutdown(RpcController controller,
      ShutdownRequest request) throws ServiceException {
    LOG.info(master.getClientIdAuditPrefix() + " shutdown");
    try {
      master.shutdown();
    } catch (IOException e) {
      LOG.error("Exception occurred in HMaster.shutdown()", e);
      throw new ServiceException(e);
    }
    return ShutdownResponse.newBuilder().build();
  }

  /**
   * Triggers an asynchronous attempt to take a snapshot.
   * {@inheritDoc}
   */
  @Override
  public SnapshotResponse snapshot(RpcController controller,
      SnapshotRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      master.snapshotManager.checkSnapshotSupport();

      LOG.info(master.getClientIdAuditPrefix() + " snapshot request for:" +
        ClientSnapshotDescriptionUtils.toString(request.getSnapshot()));
      // get the snapshot information
      SnapshotDescription snapshot = SnapshotDescriptionUtils.validate(
        request.getSnapshot(), master.getConfiguration());
      master.snapshotManager.takeSnapshot(snapshot);

      // send back the max amount of time the client should wait for the snapshot to complete
      long waitTime = SnapshotDescriptionUtils.getMaxMasterTimeout(master.getConfiguration(),
        snapshot.getType(), SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);
      return SnapshotResponse.newBuilder().setExpectedTimeout(waitTime).build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StopMasterResponse stopMaster(RpcController controller,
      StopMasterRequest request) throws ServiceException {
    LOG.info(master.getClientIdAuditPrefix() + " stop");
    try {
      master.stopMaster();
    } catch (IOException e) {
      LOG.error("Exception occurred while stopping master", e);
      throw new ServiceException(e);
    }
    return StopMasterResponse.newBuilder().build();
  }

  @Override
  public IsInMaintenanceModeResponse isMasterInMaintenanceMode(
      final RpcController controller,
      final IsInMaintenanceModeRequest request) throws ServiceException {
    IsInMaintenanceModeResponse.Builder response = IsInMaintenanceModeResponse.newBuilder();
    try {
      response.setInMaintenanceMode(master.isInMaintenanceMode());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public UnassignRegionResponse unassignRegion(RpcController controller,
      UnassignRegionRequest req) throws ServiceException {
    try {
      final byte [] regionName = req.getRegion().getValue().toByteArray();
      RegionSpecifierType type = req.getRegion().getType();
      final boolean force = req.getForce();
      UnassignRegionResponse urr = UnassignRegionResponse.newBuilder().build();

      master.checkInitialized();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("unassignRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }
      Pair<RegionInfo, ServerName> pair =
        MetaTableAccessor.getRegion(master.getConnection(), regionName);
      if (Bytes.equals(RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName(),regionName)) {
        pair = new Pair<>(RegionInfoBuilder.FIRST_META_REGIONINFO,
            master.getMetaTableLocator().getMetaRegionLocation(master.getZooKeeper()));
      }
      if (pair == null) {
        throw new UnknownRegionException(Bytes.toString(regionName));
      }

      RegionInfo hri = pair.getFirst();
      if (master.cpHost != null) {
        master.cpHost.preUnassign(hri, force);
      }
      LOG.debug(master.getClientIdAuditPrefix() + " unassign " + hri.getRegionNameAsString()
          + " in current location if it is online and reassign.force=" + force);
      master.getAssignmentManager().unassign(hri);
      if (master.cpHost != null) {
        master.cpHost.postUnassign(hri, force);
      }

      return urr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public ReportRegionStateTransitionResponse reportRegionStateTransition(RpcController c,
      ReportRegionStateTransitionRequest req) throws ServiceException {
    try {
      master.checkServiceStarted();
      return master.getAssignmentManager().reportRegionStateTransition(req);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public SetQuotaResponse setQuota(RpcController c, SetQuotaRequest req)
      throws ServiceException {
    try {
      master.checkInitialized();
      return master.getMasterQuotaManager().setQuota(req);
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestamp(RpcController controller,
      MajorCompactionTimestampRequest request) throws ServiceException {
    MajorCompactionTimestampResponse.Builder response =
        MajorCompactionTimestampResponse.newBuilder();
    try {
      master.checkInitialized();
      response.setCompactionTimestamp(master.getLastMajorCompactionTimestamp(ProtobufUtil
          .toTableName(request.getTableName())));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestampForRegion(
      RpcController controller, MajorCompactionTimestampForRegionRequest request)
      throws ServiceException {
    MajorCompactionTimestampResponse.Builder response =
        MajorCompactionTimestampResponse.newBuilder();
    try {
      master.checkInitialized();
      response.setCompactionTimestamp(master.getLastMajorCompactionTimestampForRegion(request
          .getRegion().getValue().toByteArray()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  /**
   * Compact a region on the master.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public CompactRegionResponse compactRegion(final RpcController controller,
    final CompactRegionRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      byte[] regionName = request.getRegion().getValue().toByteArray();
      TableName tableName = RegionInfo.getTable(regionName);
      // if the region is a mob region, do the mob file compaction.
      if (MobUtils.isMobRegionName(tableName, regionName)) {
        checkHFileFormatVersionForMob();
        return compactMob(request, tableName);
      } else {
        return super.compactRegion(controller, request);
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * check configured hfile format version before to do compaction
   * @throws IOException throw IOException
   */
  private void checkHFileFormatVersionForMob() throws IOException {
    if (HFile.getFormatVersion(master.getConfiguration()) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
      LOG.error("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
          + " is required for MOB compaction. Compaction will not run.");
      throw new IOException("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
          + " is required for MOB feature. Consider setting " + HFile.FORMAT_VERSION_KEY
          + " accordingly.");
    }
  }

  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
    final GetRegionInfoRequest request) throws ServiceException {
    byte[] regionName = request.getRegion().getValue().toByteArray();
    TableName tableName = RegionInfo.getTable(regionName);
    if (MobUtils.isMobRegionName(tableName, regionName)) {
      // a dummy region info contains the compaction state.
      RegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(tableName);
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(ProtobufUtil.toRegionInfo(mobRegionInfo));
      if (request.hasCompactionState() && request.getCompactionState()) {
        builder.setCompactionState(master.getMobCompactionState(tableName));
      }
      return builder.build();
    } else {
      return super.getRegionInfo(controller, request);
    }
  }

  /**
   * Compacts the mob files in the current table.
   * @param request the request.
   * @param tableName the current table name.
   * @return The response of the mob file compaction.
   * @throws IOException
   */
  private CompactRegionResponse compactMob(final CompactRegionRequest request,
    TableName tableName) throws IOException {
    if (!master.getTableStateManager().isTableState(tableName, TableState.State.ENABLED)) {
      throw new DoNotRetryIOException("Table " + tableName + " is not enabled");
    }
    boolean allFiles = false;
    List<ColumnFamilyDescriptor> compactedColumns = new ArrayList<>();
    ColumnFamilyDescriptor[] hcds = master.getTableDescriptors().get(tableName).getColumnFamilies();
    byte[] family = null;
    if (request.hasFamily()) {
      family = request.getFamily().toByteArray();
      for (ColumnFamilyDescriptor hcd : hcds) {
        if (Bytes.equals(family, hcd.getName())) {
          if (!hcd.isMobEnabled()) {
            LOG.error("Column family " + hcd.getNameAsString() + " is not a mob column family");
            throw new DoNotRetryIOException("Column family " + hcd.getNameAsString()
                    + " is not a mob column family");
          }
          compactedColumns.add(hcd);
        }
      }
    } else {
      for (ColumnFamilyDescriptor hcd : hcds) {
        if (hcd.isMobEnabled()) {
          compactedColumns.add(hcd);
        }
      }
    }
    if (compactedColumns.isEmpty()) {
      LOG.error("No mob column families are assigned in the mob compaction");
      throw new DoNotRetryIOException(
              "No mob column families are assigned in the mob compaction");
    }
    if (request.hasMajor() && request.getMajor()) {
      allFiles = true;
    }
    String familyLogMsg = (family != null) ? Bytes.toString(family) : "";
    if (LOG.isTraceEnabled()) {
      LOG.trace("User-triggered mob compaction requested for table: "
              + tableName.getNameAsString() + " for column family: " + familyLogMsg);
    }
    master.requestMobCompaction(tableName, compactedColumns, allFiles);
    return CompactRegionResponse.newBuilder().build();
  }

  @Override
  public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller,
      IsBalancerEnabledRequest request) throws ServiceException {
    IsBalancerEnabledResponse.Builder response = IsBalancerEnabledResponse.newBuilder();
    response.setEnabled(master.isBalancerOn());
    return response.build();
  }

  @Override
  public SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(RpcController controller,
    SetSplitOrMergeEnabledRequest request) throws ServiceException {
    SetSplitOrMergeEnabledResponse.Builder response = SetSplitOrMergeEnabledResponse.newBuilder();
    try {
      master.checkInitialized();
      boolean newValue = request.getEnabled();
      for (MasterProtos.MasterSwitchType masterSwitchType: request.getSwitchTypesList()) {
        MasterSwitchType switchType = convert(masterSwitchType);
        boolean oldValue = master.isSplitOrMergeEnabled(switchType);
        response.addPrevValue(oldValue);
        if (master.cpHost != null) {
          master.cpHost.preSetSplitOrMergeEnabled(newValue, switchType);
        }
        master.getSplitOrMergeTracker().setSplitOrMergeEnabled(newValue, switchType);
        if (master.cpHost != null) {
          master.cpHost.postSetSplitOrMergeEnabled(newValue, switchType);
        }
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    } catch (KeeperException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(RpcController controller,
    IsSplitOrMergeEnabledRequest request) throws ServiceException {
    IsSplitOrMergeEnabledResponse.Builder response = IsSplitOrMergeEnabledResponse.newBuilder();
    response.setEnabled(master.isSplitOrMergeEnabled(convert(request.getSwitchType())));
    return response.build();
  }

  @Override
  public NormalizeResponse normalize(RpcController controller,
      NormalizeRequest request) throws ServiceException {
    rpcPreCheck("normalize");
    try {
      return NormalizeResponse.newBuilder().setNormalizerRan(master.normalizeRegions()).build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public SetNormalizerRunningResponse setNormalizerRunning(RpcController controller,
      SetNormalizerRunningRequest request) throws ServiceException {
    rpcPreCheck("setNormalizerRunning");

    // Sets normalizer on/off flag in ZK.
    boolean prevValue = master.getRegionNormalizerTracker().isNormalizerOn();
    boolean newValue = request.getOn();
    try {
      master.getRegionNormalizerTracker().setNormalizerOn(newValue);
    } catch (KeeperException ke) {
      LOG.warn("Error flipping normalizer switch", ke);
    }
    LOG.info("{} set normalizerSwitch={}", master.getClientIdAuditPrefix(), newValue);
    return SetNormalizerRunningResponse.newBuilder().setPrevNormalizerValue(prevValue).build();
  }

  @Override
  public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller,
      IsNormalizerEnabledRequest request) throws ServiceException {
    IsNormalizerEnabledResponse.Builder response = IsNormalizerEnabledResponse.newBuilder();
    response.setEnabled(master.isNormalizerOn());
    return response.build();
  }

  /**
   * Returns the security capabilities in effect on the cluster
   */
  @Override
  public SecurityCapabilitiesResponse getSecurityCapabilities(RpcController controller,
      SecurityCapabilitiesRequest request) throws ServiceException {
    SecurityCapabilitiesResponse.Builder response = SecurityCapabilitiesResponse.newBuilder();
    try {
      master.checkInitialized();
      Set<SecurityCapabilitiesResponse.Capability> capabilities = new HashSet<>();
      // Authentication
      if (User.isHBaseSecurityEnabled(master.getConfiguration())) {
        capabilities.add(SecurityCapabilitiesResponse.Capability.SECURE_AUTHENTICATION);
      } else {
        capabilities.add(SecurityCapabilitiesResponse.Capability.SIMPLE_AUTHENTICATION);
      }
      // A coprocessor that implements AccessControlService can provide AUTHORIZATION and
      // CELL_AUTHORIZATION
      if (master.cpHost != null && hasAccessControlServiceCoprocessor(master.cpHost)) {
        if (AccessChecker.isAuthorizationSupported(master.getConfiguration())) {
          capabilities.add(SecurityCapabilitiesResponse.Capability.AUTHORIZATION);
        }
        if (AccessController.isCellAuthorizationSupported(master.getConfiguration())) {
          capabilities.add(SecurityCapabilitiesResponse.Capability.CELL_AUTHORIZATION);
        }
      }
      // A coprocessor that implements VisibilityLabelsService can provide CELL_VISIBILITY.
      if (master.cpHost != null && hasVisibilityLabelsServiceCoprocessor(master.cpHost)) {
        if (VisibilityController.isCellAuthorizationSupported(master.getConfiguration())) {
          capabilities.add(SecurityCapabilitiesResponse.Capability.CELL_VISIBILITY);
        }
      }
      response.addAllCapabilities(capabilities);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  /**
   * Determines if there is a MasterCoprocessor deployed which implements
   * {@link org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService.Interface}.
   */
  boolean hasAccessControlServiceCoprocessor(MasterCoprocessorHost cpHost) {
    return checkCoprocessorWithService(
        cpHost.findCoprocessors(MasterCoprocessor.class), AccessControlService.Interface.class);
  }

  /**
   * Determines if there is a MasterCoprocessor deployed which implements
   * {@link org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService.Interface}.
   */
  boolean hasVisibilityLabelsServiceCoprocessor(MasterCoprocessorHost cpHost) {
    return checkCoprocessorWithService(
        cpHost.findCoprocessors(MasterCoprocessor.class),
        VisibilityLabelsService.Interface.class);
  }

  /**
   * Determines if there is a coprocessor implementation in the provided argument which extends
   * or implements the provided {@code service}.
   */
  boolean checkCoprocessorWithService(
      List<MasterCoprocessor> coprocessorsToCheck, Class<?> service) {
    if (coprocessorsToCheck == null || coprocessorsToCheck.isEmpty()) {
      return false;
    }
    for (MasterCoprocessor cp : coprocessorsToCheck) {
      if (service.isAssignableFrom(cp.getClass())) {
        return true;
      }
    }
    return false;
  }

  private MasterSwitchType convert(MasterProtos.MasterSwitchType switchType) {
    switch (switchType) {
      case SPLIT:
        return MasterSwitchType.SPLIT;
      case MERGE:
        return MasterSwitchType.MERGE;
      default:
        break;
    }
    return null;
  }

  @Override
  public AddReplicationPeerResponse addReplicationPeer(RpcController controller,
      AddReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = master.addReplicationPeer(request.getPeerId(),
        ReplicationPeerConfigUtil.convert(request.getPeerConfig()),
        request.getPeerState().getState().equals(ReplicationState.State.ENABLED));
      return AddReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
      RemoveReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = master.removeReplicationPeer(request.getPeerId());
      return RemoveReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller,
      EnableReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = master.enableReplicationPeer(request.getPeerId());
      return EnableReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller,
      DisableReplicationPeerRequest request) throws ServiceException {
    try {
      long procId = master.disableReplicationPeer(request.getPeerId());
      return DisableReplicationPeerResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetReplicationPeerConfigResponse getReplicationPeerConfig(RpcController controller,
      GetReplicationPeerConfigRequest request) throws ServiceException {
    GetReplicationPeerConfigResponse.Builder response = GetReplicationPeerConfigResponse
        .newBuilder();
    try {
      String peerId = request.getPeerId();
      ReplicationPeerConfig peerConfig = master.getReplicationPeerConfig(peerId);
      response.setPeerId(peerId);
      response.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(RpcController controller,
      UpdateReplicationPeerConfigRequest request) throws ServiceException {
    try {
      long procId = master.updateReplicationPeerConfig(request.getPeerId(),
        ReplicationPeerConfigUtil.convert(request.getPeerConfig()));
      return UpdateReplicationPeerConfigResponse.newBuilder().setProcId(procId).build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public TransitReplicationPeerSyncReplicationStateResponse
      transitReplicationPeerSyncReplicationState(RpcController controller,
          TransitReplicationPeerSyncReplicationStateRequest request) throws ServiceException {
    try {
      long procId = master.transitReplicationPeerSyncReplicationState(request.getPeerId(),
        ReplicationPeerConfigUtil.toSyncReplicationState(request.getSyncReplicationState()));
      return TransitReplicationPeerSyncReplicationStateResponse.newBuilder().setProcId(procId)
          .build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListReplicationPeersResponse listReplicationPeers(RpcController controller,
      ListReplicationPeersRequest request) throws ServiceException {
    ListReplicationPeersResponse.Builder response = ListReplicationPeersResponse.newBuilder();
    try {
      List<ReplicationPeerDescription> peers = master
          .listReplicationPeers(request.hasRegex() ? request.getRegex() : null);
      for (ReplicationPeerDescription peer : peers) {
        response.addPeerDesc(ReplicationPeerConfigUtil.toProtoReplicationPeerDescription(peer));
      }
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public ListDecommissionedRegionServersResponse listDecommissionedRegionServers(
      RpcController controller, ListDecommissionedRegionServersRequest request)
      throws ServiceException {
    ListDecommissionedRegionServersResponse.Builder response =
        ListDecommissionedRegionServersResponse.newBuilder();
    try {
      master.checkInitialized();
      if (master.cpHost != null) {
        master.cpHost.preListDecommissionedRegionServers();
      }
      List<ServerName> servers = master.listDecommissionedRegionServers();
      response.addAllServerName((servers.stream().map(server -> ProtobufUtil.toServerName(server)))
          .collect(Collectors.toList()));
      if (master.cpHost != null) {
        master.cpHost.postListDecommissionedRegionServers();
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return response.build();
  }

  @Override
  public DecommissionRegionServersResponse decommissionRegionServers(RpcController controller,
      DecommissionRegionServersRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      List<ServerName> servers = request.getServerNameList().stream()
          .map(pbServer -> ProtobufUtil.toServerName(pbServer)).collect(Collectors.toList());
      boolean offload = request.getOffload();
      if (master.cpHost != null) {
        master.cpHost.preDecommissionRegionServers(servers, offload);
      }
      master.decommissionRegionServers(servers, offload);
      if (master.cpHost != null) {
        master.cpHost.postDecommissionRegionServers(servers, offload);
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return DecommissionRegionServersResponse.newBuilder().build();
  }

  @Override
  public RecommissionRegionServerResponse recommissionRegionServer(RpcController controller,
      RecommissionRegionServerRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      ServerName server = ProtobufUtil.toServerName(request.getServerName());
      List<byte[]> encodedRegionNames = request.getRegionList().stream()
          .map(regionSpecifier -> regionSpecifier.getValue().toByteArray())
          .collect(Collectors.toList());
      if (master.cpHost != null) {
        master.cpHost.preRecommissionRegionServer(server, encodedRegionNames);
      }
      master.recommissionRegionServer(server, encodedRegionNames);
      if (master.cpHost != null) {
        master.cpHost.postRecommissionRegionServer(server, encodedRegionNames);
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return RecommissionRegionServerResponse.newBuilder().build();
  }

  @Override
  public LockResponse requestLock(RpcController controller, final LockRequest request)
      throws ServiceException {
    try {
      if (request.getDescription().isEmpty()) {
        throw new IllegalArgumentException("Empty description");
      }
      NonceProcedureRunnable npr;
      LockType type = LockType.valueOf(request.getLockType().name());
      if (request.getRegionInfoCount() > 0) {
        final RegionInfo[] regionInfos = new RegionInfo[request.getRegionInfoCount()];
        for (int i = 0; i < request.getRegionInfoCount(); ++i) {
          regionInfos[i] = ProtobufUtil.toRegionInfo(request.getRegionInfo(i));
        }
        npr = new NonceProcedureRunnable(master, request.getNonceGroup(), request.getNonce()) {
          @Override
          protected void run() throws IOException {
            setProcId(master.getLockManager().remoteLocks().requestRegionsLock(regionInfos,
                request.getDescription(), getNonceKey()));
          }

          @Override
          protected String getDescription() {
            return "RequestLock";
          }
        };
      } else if (request.hasTableName()) {
        final TableName tableName = ProtobufUtil.toTableName(request.getTableName());
        npr = new NonceProcedureRunnable(master, request.getNonceGroup(), request.getNonce()) {
          @Override
          protected void run() throws IOException {
            setProcId(master.getLockManager().remoteLocks().requestTableLock(tableName, type,
                request.getDescription(), getNonceKey()));
          }

          @Override
          protected String getDescription() {
            return "RequestLock";
          }
        };
      } else if (request.hasNamespace()) {
        npr = new NonceProcedureRunnable(master, request.getNonceGroup(), request.getNonce()) {
          @Override
          protected void run() throws IOException {
            setProcId(master.getLockManager().remoteLocks().requestNamespaceLock(
                request.getNamespace(), type, request.getDescription(), getNonceKey()));
          }

          @Override
          protected String getDescription() {
            return "RequestLock";
          }
        };
      } else {
        throw new IllegalArgumentException("one of table/namespace/region should be specified");
      }
      long procId = MasterProcedureUtil.submitProcedure(npr);
      return LockResponse.newBuilder().setProcId(procId).build();
    } catch (IllegalArgumentException e) {
      LOG.warn("Exception when queuing lock", e);
      throw new ServiceException(new DoNotRetryIOException(e));
    } catch (IOException e) {
      LOG.warn("Exception when queuing lock", e);
      throw new ServiceException(e);
    }
  }

  /**
   * @return LOCKED, if procedure is found and it has the lock; else UNLOCKED.
   * @throws ServiceException if given proc id is found but it is not a LockProcedure.
   */
  @Override
  public LockHeartbeatResponse lockHeartbeat(RpcController controller, LockHeartbeatRequest request)
      throws ServiceException {
    try {
      if (master.getLockManager().remoteLocks().lockHeartbeat(request.getProcId(),
          request.getKeepAlive())) {
        return LockHeartbeatResponse.newBuilder().setTimeoutMs(
            master.getConfiguration().getInt(LockProcedure.REMOTE_LOCKS_TIMEOUT_MS_CONF,
                LockProcedure.DEFAULT_REMOTE_LOCKS_TIMEOUT_MS))
            .setLockStatus(LockHeartbeatResponse.LockStatus.LOCKED).build();
      } else {
        return LockHeartbeatResponse.newBuilder()
            .setLockStatus(LockHeartbeatResponse.LockStatus.UNLOCKED).build();
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RegionSpaceUseReportResponse reportRegionSpaceUse(RpcController controller,
      RegionSpaceUseReportRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      if (!QuotaUtil.isQuotaEnabled(master.getConfiguration())) {
        return RegionSpaceUseReportResponse.newBuilder().build();
      }
      MasterQuotaManager quotaManager = this.master.getMasterQuotaManager();
      final long now = EnvironmentEdgeManager.currentTime();
      for (RegionSpaceUse report : request.getSpaceUseList()) {
        quotaManager.addRegionSize(ProtobufUtil.toRegionInfo(
            report.getRegionInfo()), report.getRegionSize(), now);
      }
      return RegionSpaceUseReportResponse.newBuilder().build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetSpaceQuotaRegionSizesResponse getSpaceQuotaRegionSizes(
      RpcController controller, GetSpaceQuotaRegionSizesRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      MasterQuotaManager quotaManager = this.master.getMasterQuotaManager();
      GetSpaceQuotaRegionSizesResponse.Builder builder =
          GetSpaceQuotaRegionSizesResponse.newBuilder();
      if (quotaManager != null) {
        Map<RegionInfo,Long> regionSizes = quotaManager.snapshotRegionSizes();
        Map<TableName,Long> regionSizesByTable = new HashMap<>();
        // Translate hregioninfo+long -> tablename+long
        for (Entry<RegionInfo,Long> entry : regionSizes.entrySet()) {
          final TableName tableName = entry.getKey().getTable();
          Long prevSize = regionSizesByTable.get(tableName);
          if (prevSize == null) {
            prevSize = 0L;
          }
          regionSizesByTable.put(tableName, prevSize + entry.getValue());
        }
        // Serialize them into the protobuf
        for (Entry<TableName,Long> tableSize : regionSizesByTable.entrySet()) {
          builder.addSizes(RegionSizes.newBuilder()
              .setTableName(ProtobufUtil.toProtoTableName(tableSize.getKey()))
              .setSize(tableSize.getValue()).build());
        }
        return builder.build();
      }
      return builder.build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetQuotaStatesResponse getQuotaStates(
      RpcController controller, GetQuotaStatesRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      QuotaObserverChore quotaChore = this.master.getQuotaObserverChore();
      GetQuotaStatesResponse.Builder builder = GetQuotaStatesResponse.newBuilder();
      if (quotaChore != null) {
        // The "current" view of all tables with quotas
        Map<TableName, SpaceQuotaSnapshot> tableSnapshots = quotaChore.getTableQuotaSnapshots();
        for (Entry<TableName, SpaceQuotaSnapshot> entry : tableSnapshots.entrySet()) {
          builder.addTableSnapshots(
              TableQuotaSnapshot.newBuilder()
                  .setTableName(ProtobufUtil.toProtoTableName(entry.getKey()))
                  .setSnapshot(SpaceQuotaSnapshot.toProtoSnapshot(entry.getValue())).build());
        }
        // The "current" view of all namespaces with quotas
        Map<String, SpaceQuotaSnapshot> nsSnapshots = quotaChore.getNamespaceQuotaSnapshots();
        for (Entry<String, SpaceQuotaSnapshot> entry : nsSnapshots.entrySet()) {
          builder.addNsSnapshots(
              NamespaceQuotaSnapshot.newBuilder()
                  .setNamespace(entry.getKey())
                  .setSnapshot(SpaceQuotaSnapshot.toProtoSnapshot(entry.getValue())).build());
        }
        return builder.build();
      }
      return builder.build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClearDeadServersResponse clearDeadServers(RpcController controller,
      ClearDeadServersRequest request) throws ServiceException {
    LOG.debug(master.getClientIdAuditPrefix() + " clear dead region servers.");
    ClearDeadServersResponse.Builder response = ClearDeadServersResponse.newBuilder();
    try {
      master.checkInitialized();
      if (master.cpHost != null) {
        master.cpHost.preClearDeadServers();
      }

      if (master.getServerManager().areDeadServersInProgress()) {
        LOG.debug("Some dead server is still under processing, won't clear the dead server list");
        response.addAllServerName(request.getServerNameList());
      } else {
        for (HBaseProtos.ServerName pbServer : request.getServerNameList()) {
          if (!master.getServerManager().getDeadServers()
                  .removeDeadServer(ProtobufUtil.toServerName(pbServer))) {
            response.addServerName(pbServer);
          }
        }
      }

      if (master.cpHost != null) {
        master.cpHost.postClearDeadServers(
            ProtobufUtil.toServerNameList(request.getServerNameList()),
            ProtobufUtil.toServerNameList(response.getServerNameList()));
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }
    return response.build();
  }

  @Override
  public ReportProcedureDoneResponse reportProcedureDone(RpcController controller,
      ReportProcedureDoneRequest request) throws ServiceException {
    request.getResultList().forEach(result -> {
      if (result.getStatus() == RemoteProcedureResult.Status.SUCCESS) {
        master.remoteProcedureCompleted(result.getProcId());
      } else {
        master.remoteProcedureFailed(result.getProcId(),
          RemoteProcedureException.fromProto(result.getError()));
      }
    });
    return ReportProcedureDoneResponse.getDefaultInstance();
  }

  @Override
  public FileArchiveNotificationResponse reportFileArchival(RpcController controller,
      FileArchiveNotificationRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      if (!QuotaUtil.isQuotaEnabled(master.getConfiguration())) {
        return FileArchiveNotificationResponse.newBuilder().build();
      }
      master.getMasterQuotaManager().processFileArchivals(request, master.getConnection(),
          master.getConfiguration(), master.getFileSystem());
      return FileArchiveNotificationResponse.newBuilder().build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }
}
