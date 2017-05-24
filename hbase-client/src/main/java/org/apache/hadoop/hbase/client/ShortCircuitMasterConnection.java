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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.*;
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
@InterfaceAudience.Public
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
  public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
      RemoveReplicationPeerRequest request) throws ServiceException {
    return stub.removeReplicationPeer(controller, request);
  }

  @Override
  public RemoveDrainFromRegionServersResponse removeDrainFromRegionServers(RpcController controller,
      RemoveDrainFromRegionServersRequest request) throws ServiceException {
    return stub.removeDrainFromRegionServers(controller, request);
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
  public ListProceduresResponse listProcedures(RpcController controller,
      ListProceduresRequest request) throws ServiceException {
    return stub.listProcedures(controller, request);
  }

  @Override
  public ListLocksResponse listLocks(RpcController controller,
      ListLocksRequest request) throws ServiceException {
    return stub.listLocks(controller, request);
  }

  @Override
  public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController controller,
      ListNamespaceDescriptorsRequest request) throws ServiceException {
    return stub.listNamespaceDescriptors(controller, request);
  }

  @Override
  public ListDrainingRegionServersResponse listDrainingRegionServers(RpcController controller,
      ListDrainingRegionServersRequest request) throws ServiceException {
    return stub.listDrainingRegionServers(controller, request);
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
  public DrainRegionServersResponse drainRegionServers(RpcController controller,
      DrainRegionServersRequest request) throws ServiceException {
    return stub.drainRegionServers(controller, request);
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
  public SplitTableRegionResponse splitRegion(RpcController controller, SplitTableRegionRequest request)
      throws ServiceException {
    return stub.splitRegion(controller, request);
  }

  @Override
  public DispatchMergingRegionsResponse dispatchMergingRegions(RpcController controller,
      DispatchMergingRegionsRequest request) throws ServiceException {
    return stub.dispatchMergingRegions(controller, request);
  }
}
