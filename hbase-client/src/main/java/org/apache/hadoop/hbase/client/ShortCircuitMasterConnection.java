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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos
  .IsSnapshotCleanupEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos
  .IsSnapshotCleanupEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespacesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespacesResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
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

/**
 * A short-circuit connection that can bypass the RPC layer (serialization, deserialization,
 * networking, etc..) when talking to a local master
 */
@InterfaceAudience.Private
public class ShortCircuitMasterConnection implements MasterKeepAliveConnection {

  private final MasterService.BlockingInterface stub;

  public ShortCircuitMasterConnection(MasterService.BlockingInterface stub) {
    this.stub = stub;
  }

  @Override
  public UnassignRegionResponse unassignRegion(RpcController controller,
      UnassignRegionRequest request) throws ServiceException {
    return stub.unassignRegion(controller, request);
  }

  @Override
  public TruncateTableResponse truncateTable(RpcController controller, TruncateTableRequest request)
      throws ServiceException {
    return stub.truncateTable(controller, request);
  }

  @Override
  public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request)
      throws ServiceException {
    return stub.stopMaster(controller, request);
  }

  @Override
  public SnapshotResponse snapshot(RpcController controller, SnapshotRequest request)
      throws ServiceException {
    return stub.snapshot(controller, request);
  }

  @Override
  public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request)
      throws ServiceException {
    return stub.shutdown(controller, request);
  }

  @Override
  public SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(RpcController controller,
      SetSplitOrMergeEnabledRequest request) throws ServiceException {
    return stub.setSplitOrMergeEnabled(controller, request);
  }

  @Override
  public SetQuotaResponse setQuota(RpcController controller, SetQuotaRequest request)
      throws ServiceException {
    return stub.setQuota(controller, request);
  }

  @Override
  public SetNormalizerRunningResponse setNormalizerRunning(RpcController controller,
      SetNormalizerRunningRequest request) throws ServiceException {
    return stub.setNormalizerRunning(controller, request);
  }

  @Override
  public SetBalancerRunningResponse setBalancerRunning(RpcController controller,
      SetBalancerRunningRequest request) throws ServiceException {
    return stub.setBalancerRunning(controller, request);
  }

  @Override
  public RunCatalogScanResponse runCatalogScan(RpcController controller,
      RunCatalogScanRequest request) throws ServiceException {
    return stub.runCatalogScan(controller, request);
  }

  @Override
  public RestoreSnapshotResponse restoreSnapshot(RpcController controller,
      RestoreSnapshotRequest request) throws ServiceException {
    return stub.restoreSnapshot(controller, request);
  }

  @Override
  public SetSnapshotCleanupResponse switchSnapshotCleanup(RpcController controller,
      SetSnapshotCleanupRequest request) throws ServiceException {
    return stub.switchSnapshotCleanup(controller, request);
  }

  @Override
  public IsSnapshotCleanupEnabledResponse isSnapshotCleanupEnabled(
      RpcController controller, IsSnapshotCleanupEnabledRequest request)
      throws ServiceException {
    return stub.isSnapshotCleanupEnabled(controller, request);
  }

  @Override
  public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
      RemoveReplicationPeerRequest request) throws ServiceException {
    return stub.removeReplicationPeer(controller, request);
  }

  @Override
  public RecommissionRegionServerResponse recommissionRegionServer(RpcController controller,
      RecommissionRegionServerRequest request) throws ServiceException {
    return stub.recommissionRegionServer(controller, request);
  }

  @Override
  public OfflineRegionResponse offlineRegion(RpcController controller, OfflineRegionRequest request)
      throws ServiceException {
    return stub.offlineRegion(controller, request);
  }

  @Override
  public NormalizeResponse normalize(RpcController controller, NormalizeRequest request)
      throws ServiceException {
    return stub.normalize(controller, request);
  }

  @Override
  public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest request)
      throws ServiceException {
    return stub.moveRegion(controller, request);
  }

  @Override
  public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest request)
      throws ServiceException {
    return stub.modifyTable(controller, request);
  }

  @Override
  public ModifyNamespaceResponse modifyNamespace(RpcController controller,
      ModifyNamespaceRequest request) throws ServiceException {
    return stub.modifyNamespace(controller, request);
  }

  @Override
  public ModifyColumnResponse modifyColumn(RpcController controller, ModifyColumnRequest request)
      throws ServiceException {
    return stub.modifyColumn(controller, request);
  }

  @Override
  public MergeTableRegionsResponse mergeTableRegions(RpcController controller,
      MergeTableRegionsRequest request) throws ServiceException {
    return stub.mergeTableRegions(controller, request);
  }

  @Override
  public ListTableNamesByNamespaceResponse listTableNamesByNamespace(RpcController controller,
      ListTableNamesByNamespaceRequest request) throws ServiceException {
    return stub.listTableNamesByNamespace(controller, request);
  }

  @Override
  public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(
      RpcController controller, ListTableDescriptorsByNamespaceRequest request)
      throws ServiceException {
    return stub.listTableDescriptorsByNamespace(controller, request);
  }

  @Override
  public GetProceduresResponse getProcedures(RpcController controller,
      GetProceduresRequest request) throws ServiceException {
    return stub.getProcedures(controller, request);
  }

  @Override
  public GetLocksResponse getLocks(RpcController controller,
      GetLocksRequest request) throws ServiceException {
    return stub.getLocks(controller, request);
  }

  @Override
  public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController controller,
      ListNamespaceDescriptorsRequest request) throws ServiceException {
    return stub.listNamespaceDescriptors(controller, request);
  }

  @Override
  public ListDecommissionedRegionServersResponse listDecommissionedRegionServers(RpcController controller,
      ListDecommissionedRegionServersRequest request) throws ServiceException {
    return stub.listDecommissionedRegionServers(controller, request);
  }

  @Override
  public IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(RpcController controller,
      IsSplitOrMergeEnabledRequest request) throws ServiceException {
    return stub.isSplitOrMergeEnabled(controller, request);
  }

  @Override
  public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
      IsSnapshotDoneRequest request) throws ServiceException {
    return stub.isSnapshotDone(controller, request);
  }

  @Override
  public IsProcedureDoneResponse isProcedureDone(RpcController controller,
      IsProcedureDoneRequest request) throws ServiceException {
    return stub.isProcedureDone(controller, request);
  }

  @Override
  public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller,
      IsNormalizerEnabledRequest request) throws ServiceException {
    return stub.isNormalizerEnabled(controller, request);
  }

  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController controller,
      IsMasterRunningRequest request) throws ServiceException {
    return stub.isMasterRunning(controller, request);
  }

  @Override
  public IsInMaintenanceModeResponse isMasterInMaintenanceMode(RpcController controller,
      IsInMaintenanceModeRequest request) throws ServiceException {
    return stub.isMasterInMaintenanceMode(controller, request);
  }

  @Override
  public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController controller,
      IsCatalogJanitorEnabledRequest request) throws ServiceException {
    return stub.isCatalogJanitorEnabled(controller, request);
  }

  @Override
  public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller,
      IsBalancerEnabledRequest request) throws ServiceException {
    return stub.isBalancerEnabled(controller, request);
  }

  @Override
  public GetTableStateResponse getTableState(RpcController controller, GetTableStateRequest request)
      throws ServiceException {
    return stub.getTableState(controller, request);
  }

  @Override
  public GetTableNamesResponse getTableNames(RpcController controller, GetTableNamesRequest request)
      throws ServiceException {
    return stub.getTableNames(controller, request);
  }

  @Override
  public GetTableDescriptorsResponse getTableDescriptors(RpcController controller,
      GetTableDescriptorsRequest request) throws ServiceException {
    return stub.getTableDescriptors(controller, request);
  }

  @Override
  public SecurityCapabilitiesResponse getSecurityCapabilities(RpcController controller,
      SecurityCapabilitiesRequest request) throws ServiceException {
    return stub.getSecurityCapabilities(controller, request);
  }

  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(RpcController controller,
      GetSchemaAlterStatusRequest request) throws ServiceException {
    return stub.getSchemaAlterStatus(controller, request);
  }

  @Override
  public GetProcedureResultResponse getProcedureResult(RpcController controller,
      GetProcedureResultRequest request) throws ServiceException {
    return stub.getProcedureResult(controller, request);
  }

  @Override
  public GetNamespaceDescriptorResponse getNamespaceDescriptor(RpcController controller,
      GetNamespaceDescriptorRequest request) throws ServiceException {
    return stub.getNamespaceDescriptor(controller, request);
  }

  @Override
  public ListNamespacesResponse listNamespaces(RpcController controller,
      ListNamespacesRequest request) throws ServiceException {
    return stub.listNamespaces(controller, request);
  }

  @Override
  public HBaseProtos.LogEntry getLogEntries(RpcController controller,
      HBaseProtos.LogRequest request) throws ServiceException {
    return stub.getLogEntries(controller, request);
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestampForRegion(
      RpcController controller, MajorCompactionTimestampForRegionRequest request)
      throws ServiceException {
    return stub.getLastMajorCompactionTimestampForRegion(controller, request);
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestamp(RpcController controller,
      MajorCompactionTimestampRequest request) throws ServiceException {
    return stub.getLastMajorCompactionTimestamp(controller, request);
  }

  @Override
  public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller,
      GetCompletedSnapshotsRequest request) throws ServiceException {
    return stub.getCompletedSnapshots(controller, request);
  }

  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller,
      GetClusterStatusRequest request) throws ServiceException {
    return stub.getClusterStatus(controller, request);
  }

  @Override
  public ExecProcedureResponse execProcedureWithRet(RpcController controller,
      ExecProcedureRequest request) throws ServiceException {
    return stub.execProcedureWithRet(controller, request);
  }

  @Override
  public ExecProcedureResponse execProcedure(RpcController controller, ExecProcedureRequest request)
      throws ServiceException {
    return stub.execProcedure(controller, request);
  }

  @Override
  public CoprocessorServiceResponse execMasterService(RpcController controller,
      CoprocessorServiceRequest request) throws ServiceException {
    return stub.execMasterService(controller, request);
  }

  @Override
  public EnableTableResponse enableTable(RpcController controller, EnableTableRequest request)
      throws ServiceException {
    return stub.enableTable(controller, request);
  }

  @Override
  public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller,
      EnableReplicationPeerRequest request) throws ServiceException {
    return stub.enableReplicationPeer(controller, request);
  }

  @Override
  public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController controller,
      EnableCatalogJanitorRequest request) throws ServiceException {
    return stub.enableCatalogJanitor(controller, request);
  }

  @Override
  public DecommissionRegionServersResponse decommissionRegionServers(RpcController controller,
      DecommissionRegionServersRequest request) throws ServiceException {
    return stub.decommissionRegionServers(controller, request);
  }

  @Override
  public DisableTableResponse disableTable(RpcController controller, DisableTableRequest request)
      throws ServiceException {
    return stub.disableTable(controller, request);
  }

  @Override
  public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller,
      DisableReplicationPeerRequest request) throws ServiceException {
    return stub.disableReplicationPeer(controller, request);
  }

  @Override
  public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest request)
      throws ServiceException {
    return stub.deleteTable(controller, request);
  }

  @Override
  public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
      DeleteSnapshotRequest request) throws ServiceException {
    return stub.deleteSnapshot(controller, request);
  }

  @Override
  public DeleteNamespaceResponse deleteNamespace(RpcController controller,
      DeleteNamespaceRequest request) throws ServiceException {
    return stub.deleteNamespace(controller, request);
  }

  @Override
  public DeleteColumnResponse deleteColumn(RpcController controller, DeleteColumnRequest request)
      throws ServiceException {
    return stub.deleteColumn(controller, request);
  }

  @Override
  public CreateTableResponse createTable(RpcController controller, CreateTableRequest request)
      throws ServiceException {
    return stub.createTable(controller, request);
  }

  @Override
  public CreateNamespaceResponse createNamespace(RpcController controller,
      CreateNamespaceRequest request) throws ServiceException {
    return stub.createNamespace(controller, request);
  }

  @Override
  public BalanceResponse balance(RpcController controller, BalanceRequest request)
      throws ServiceException {
    return stub.balance(controller, request);
  }

  @Override
  public AssignRegionResponse assignRegion(RpcController controller, AssignRegionRequest request)
      throws ServiceException {
    return stub.assignRegion(controller, request);
  }

  @Override
  public AddReplicationPeerResponse addReplicationPeer(RpcController controller,
      AddReplicationPeerRequest request) throws ServiceException {
    return stub.addReplicationPeer(controller, request);
  }

  @Override
  public AddColumnResponse addColumn(RpcController controller, AddColumnRequest request)
      throws ServiceException {
    return stub.addColumn(controller, request);
  }

  @Override
  public AbortProcedureResponse abortProcedure(RpcController controller,
      AbortProcedureRequest request) throws ServiceException {
    return stub.abortProcedure(controller, request);
  }

  @Override
  public void close() {
    // nothing to do here
  }

  @Override
  public RunCleanerChoreResponse runCleanerChore(RpcController controller,
      RunCleanerChoreRequest request) throws ServiceException {
    return stub.runCleanerChore(controller, request);
  }

  @Override
  public SetCleanerChoreRunningResponse setCleanerChoreRunning(RpcController controller,
      SetCleanerChoreRunningRequest request) throws ServiceException {
    return stub.setCleanerChoreRunning(controller, request);
  }

  @Override
  public IsCleanerChoreEnabledResponse isCleanerChoreEnabled(RpcController controller,
      IsCleanerChoreEnabledRequest request) throws ServiceException {
    return stub.isCleanerChoreEnabled(controller, request);
  }

  @Override
  public GetReplicationPeerConfigResponse getReplicationPeerConfig(RpcController controller,
      GetReplicationPeerConfigRequest request) throws ServiceException {
    return stub.getReplicationPeerConfig(controller, request);
  }

  @Override
  public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(RpcController controller,
      UpdateReplicationPeerConfigRequest request) throws ServiceException {
    return stub.updateReplicationPeerConfig(controller, request);
  }

  @Override
  public ListReplicationPeersResponse listReplicationPeers(RpcController controller,
      ListReplicationPeersRequest request) throws ServiceException {
    return stub.listReplicationPeers(controller, request);
  }

  @Override
  public GetSpaceQuotaRegionSizesResponse getSpaceQuotaRegionSizes(RpcController controller,
      GetSpaceQuotaRegionSizesRequest request) throws ServiceException {
    return stub.getSpaceQuotaRegionSizes(controller, request);
  }

  @Override
  public GetQuotaStatesResponse getQuotaStates(RpcController controller,
      GetQuotaStatesRequest request) throws ServiceException {
    return stub.getQuotaStates(controller, request);
  }

  @Override
  public ClearDeadServersResponse clearDeadServers(RpcController controller,
      ClearDeadServersRequest request) throws ServiceException {
    return stub.clearDeadServers(controller, request);
  }

  @Override
  public SplitTableRegionResponse splitRegion(RpcController controller, SplitTableRegionRequest request)
      throws ServiceException {
    return stub.splitRegion(controller, request);
  }

  @Override
  public SwitchRpcThrottleResponse switchRpcThrottle(RpcController controller,
      SwitchRpcThrottleRequest request) throws ServiceException {
    return stub.switchRpcThrottle(controller, request);
  }

  @Override
  public IsRpcThrottleEnabledResponse isRpcThrottleEnabled(RpcController controller,
      IsRpcThrottleEnabledRequest request) throws ServiceException {
    return stub.isRpcThrottleEnabled(controller, request);
  }

  @Override
  public SwitchExceedThrottleQuotaResponse switchExceedThrottleQuota(RpcController controller,
      SwitchExceedThrottleQuotaRequest request) throws ServiceException {
    return stub.switchExceedThrottleQuota(controller, request);
  }

  @Override
  public GrantResponse grant(RpcController controller, GrantRequest request)
      throws ServiceException {
    return stub.grant(controller, request);
  }

  @Override
  public RevokeResponse revoke(RpcController controller, RevokeRequest request)
      throws ServiceException {
    return stub.revoke(controller, request);
  }

  @Override
  public GetUserPermissionsResponse getUserPermissions(RpcController controller,
      GetUserPermissionsRequest request) throws ServiceException {
    return stub.getUserPermissions(controller, request);
  }

  @Override
  public HasUserPermissionsResponse hasUserPermissions(RpcController controller,
      HasUserPermissionsRequest request) throws ServiceException {
    return stub.hasUserPermissions(controller, request);
  }
}
