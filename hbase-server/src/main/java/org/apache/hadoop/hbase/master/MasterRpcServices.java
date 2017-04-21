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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil.NonceProcedureRunnable;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.*;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDrainingRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDrainingRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListProceduresResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse.Capability;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.SplitTableRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.SplitTableRegionResponse;
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
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

/**
 * Implements the master RPC services.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class MasterRpcServices extends RSRpcServices
      implements MasterService.BlockingInterface, RegionServerStatusService.BlockingInterface,
        LockService.BlockingInterface {
  private static final Log LOG = LogFactory.getLog(MasterRpcServices.class.getName());

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
  protected PriorityFunction createPriority() {
    return new MasterAnnotationReadingPriorityFunction(this);
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
        newValue = master.cpHost.preBalanceSwitch(newValue);
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
   * Sets normalizer on/off flag in ZK.
   */
  public boolean normalizerSwitch(boolean on) {
    boolean oldValue = master.getRegionNormalizerTracker().isNormalizerOn();
    boolean newValue = on;
    try {
      try {
        master.getRegionNormalizerTracker().setNormalizerOn(newValue);
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
      LOG.info(master.getClientIdAuditPrefix() + " set normalizerSwitch=" + newValue);
    } catch (IOException ioe) {
      LOG.warn("Error flipping normalizer switch", ioe);
    }
    return oldValue;
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
      ClusterStatusProtos.ServerLoad sl = request.getLoad();
      ServerName serverName = ProtobufUtil.toServerName(request.getServer());
      ServerLoad oldLoad = master.getServerManager().getLoad(serverName);
      master.getServerManager().regionServerReport(serverName, new ServerLoad(sl));
      if (sl != null && master.metricsMaster != null) {
        // Up our metrics.
        master.metricsMaster.incrementRequests(sl.getTotalNumberOfRequests()
            - (oldLoad != null ? oldLoad.getTotalNumberOfRequests() : 0));
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
      InetAddress ia = master.getRemoteInetAddress(
        request.getPort(), request.getServerStartCode());
      // if regionserver passed hostname to use,
      // then use it instead of doing a reverse DNS lookup
      ServerName rs = master.getServerManager().regionServerStartup(request, ia);

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
          ProtobufUtil.convertToHColumnDesc(req.getColumnFamilies()),
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
      final byte [] regionName = req.getRegion().getValue().toByteArray();
      RegionSpecifierType type = req.getRegion().getType();
      AssignRegionResponse arr = AssignRegionResponse.newBuilder().build();

      master.checkInitialized();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("assignRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      HRegionInfo regionInfo = regionStates.getRegionInfo(regionName);
      if (regionInfo == null) throw new UnknownRegionException(Bytes.toString(regionName));
      if (master.cpHost != null) {
        if (master.cpHost.preAssign(regionInfo)) {
          return arr;
        }
      }
      LOG.info(master.getClientIdAuditPrefix()
        + " assign " + regionInfo.getRegionNameAsString());
      master.getAssignmentManager().assign(regionInfo, true);
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
    HTableDescriptor hTableDescriptor = ProtobufUtil.convertToHTableDesc(req.getTableSchema());
    byte [][] splitKeys = ProtobufUtil.getSplitKeysArray(req);
    try {
      long procId =
          master.createTable(hTableDescriptor, splitKeys, req.getNonceGroup(), req.getNonce());
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
    try {
      master.checkInitialized();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return EnableCatalogJanitorResponse.newBuilder().setPrevValue(
      master.catalogJanitorChore.setEnabled(req.getEnable())).build();
  }

  @Override
  public SetCleanerChoreRunningResponse setCleanerChoreRunning(RpcController c,
                                                               SetCleanerChoreRunningRequest req)
    throws ServiceException {
    try {
      master.checkInitialized();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
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
    HRegionInfo[] regionsToMerge = new HRegionInfo[request.getRegionCount()];
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
  public SplitTableRegionResponse splitRegion(
      final RpcController controller,
      final SplitTableRegionRequest request) throws ServiceException {
    try {
      long procId = master.splitRegion(
        HRegionInfo.convert(request.getRegionInfo()),
        request.getSplitRow().toByteArray(),
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
    try {
      master.checkInitialized();
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
          new com.google.protobuf.RpcCallback<com.google.protobuf.Message>() {
        @Override
        public void run(com.google.protobuf.Message message) {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
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
        throw new ServiceException("The procedure is not registered: "
          + desc.getSignature());
      }

      LOG.info(master.getClientIdAuditPrefix() + " procedure request for: "
        + desc.getSignature());

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
    try {
      master.checkInitialized();
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm = master.getMasterProcedureManagerHost().getProcedureManager(
        desc.getSignature());
      if (mpm == null) {
        throw new ServiceException("The procedure is not registered: "
          + desc.getSignature());
      }

      LOG.info(master.getClientIdAuditPrefix() + " procedure request for: "
        + desc.getSignature());

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
      response.setClusterStatus(ProtobufUtil.convert(master.getClusterStatus()));
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

      List<HTableDescriptor> descriptors = master.listTableDescriptors(namespace, regex,
          tableNameList, req.getIncludeSysTables());

      GetTableDescriptorsResponse.Builder builder = GetTableDescriptorsResponse.newBuilder();
      if (descriptors != null && descriptors.size() > 0) {
        // Add the table descriptors to the response
        for (HTableDescriptor htd: descriptors) {
          builder.addTableSchema(ProtobufUtil.convertToTableSchema(htd));
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
      TableState.State state = master.getTableStateManager()
              .getTableState(tableName);
      GetTableStateResponse.Builder builder = GetTableStateResponse.newBuilder();
      builder.setTableState(new TableState(tableName, state).convert());
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
    LOG.debug("Checking to see if procedure is done procId=" + request.getProcId());
    try {
      master.checkInitialized();
      GetProcedureResultResponse.Builder builder = GetProcedureResultResponse.newBuilder();

      Pair<ProcedureInfo, Procedure> v = master.getMasterProcedureExecutor()
          .getResultOrProcedure(request.getProcId());
      if (v.getFirst() != null) {
        ProcedureInfo result = v.getFirst();
        builder.setState(GetProcedureResultResponse.State.FINISHED);
        builder.setSubmittedTime(result.getSubmittedTime());
        builder.setLastUpdate(result.getLastUpdate());
        if (result.isFailed()) {
          builder.setException(ForeignExceptionUtil.toProtoForeignException(result.getException()));
        }
        if (result.hasResultData()) {
          builder.setResult(UnsafeByteOperations.unsafeWrap(result.getResult()));
        }
        master.getMasterProcedureExecutor().removeResult(request.getProcId());
      } else {
        Procedure proc = v.getSecond();
        if (proc == null) {
          builder.setState(GetProcedureResultResponse.State.NOT_FOUND);
        } else {
          builder.setState(GetProcedureResultResponse.State.RUNNING);
          builder.setSubmittedTime(proc.getSubmittedTime());
          builder.setLastUpdate(proc.getLastUpdate());
        }
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AbortProcedureResponse abortProcedure(
      RpcController rpcController,
      AbortProcedureRequest request) throws ServiceException {
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
  public ListProceduresResponse listProcedures(
      RpcController rpcController,
      ListProceduresRequest request) throws ServiceException {
    try {
      final ListProceduresResponse.Builder response = ListProceduresResponse.newBuilder();
      for (ProcedureInfo p: master.listProcedures()) {
        response.addProcedure(ProcedureUtil.convertToProtoProcedure(p));
      }
      return response.build();
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
      for (HTableDescriptor htd : master
          .listTableDescriptorsByNamespace(request.getNamespaceName())) {
        b.addTableSchema(ProtobufUtil.convertToTableSchema(htd));
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
        ProtobufUtil.convertToHColumnDesc(req.getColumnFamilies()),
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
        ProtobufUtil.convertToHTableDesc(req.getTableSchema()),
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
    final byte [] regionName = request.getRegion().getValue().toByteArray();
    RegionSpecifierType type = request.getRegion().getType();
    if (type != RegionSpecifierType.REGION_NAME) {
      LOG.warn("moveRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
        + " actual: " + type);
    }

    try {
      master.checkInitialized();
      Pair<HRegionInfo, ServerName> pair =
        MetaTableAccessor.getRegion(master.getConnection(), regionName);
      if (pair == null) throw new UnknownRegionException(Bytes.toStringBinary(regionName));
      HRegionInfo hri = pair.getFirst();
      if (master.cpHost != null) {
        master.cpHost.preRegionOffline(hri);
      }
      LOG.info(master.getClientIdAuditPrefix() + " offline " + hri.getRegionNameAsString());
      master.getAssignmentManager().regionOffline(hri);
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
      long procId = master.restoreSnapshot(request.getSnapshot(),
          request.getNonceGroup(), request.getNonce());
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
    try {
      master.checkInitialized();
      return ResponseConverter.buildRunCatalogScanResponse(master.catalogJanitorChore.scan());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public RunCleanerChoreResponse runCleanerChore(RpcController c, RunCleanerChoreRequest req)
    throws ServiceException {
    try {
      master.checkInitialized();
      Boolean result = master.getHFileCleaner().runCleaner() && master.getLogCleaner().runCleaner();
      return ResponseConverter.buildRunCleanerChoreResponse(result);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
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
    response.setInMaintenanceMode(master.isInMaintenanceMode());
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
      Pair<HRegionInfo, ServerName> pair =
        MetaTableAccessor.getRegion(master.getConnection(), regionName);
      if (Bytes.equals(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(),regionName)) {
        pair = new Pair<>(HRegionInfo.FIRST_META_REGIONINFO,
            master.getMetaTableLocator().getMetaRegionLocation(master.getZooKeeper()));
      }
      if (pair == null) {
        throw new UnknownRegionException(Bytes.toString(regionName));
      }

      if (pair == null) throw new UnknownRegionException(Bytes.toString(regionName));
      HRegionInfo hri = pair.getFirst();
      if (master.cpHost != null) {
        if (master.cpHost.preUnassign(hri, force)) {
          return urr;
        }
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
      RegionStateTransition rt = req.getTransition(0);
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      for (RegionInfo ri : rt.getRegionInfoList())  {
        TableName tableName = ProtobufUtil.toTableName(ri.getTableName());
        if (!(TableName.META_TABLE_NAME.equals(tableName)
            && regionStates.getRegionState(HRegionInfo.FIRST_META_REGIONINFO) != null)
              && !master.getAssignmentManager().isFailoverCleanupDone()) {
          // Meta region is assigned before master finishes the
          // failover cleanup. So no need this check for it
          throw new PleaseHoldException("Master is rebuilding user regions");
        }
      }
      ServerName sn = ProtobufUtil.toServerName(req.getServer());
      String error = master.getAssignmentManager().onRegionTransition(sn, rt);
      ReportRegionStateTransitionResponse.Builder rrtr =
        ReportRegionStateTransitionResponse.newBuilder();
      if (error != null) {
        rrtr.setErrorMessage(error);
      }
      return rrtr.build();
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
      TableName tableName = HRegionInfo.getTable(regionName);
      // if the region is a mob region, do the mob file compaction.
      if (MobUtils.isMobRegionName(tableName, regionName)) {
        return compactMob(request, tableName);
      } else {
        return super.compactRegion(controller, request);
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority=HConstants.ADMIN_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
    final GetRegionInfoRequest request) throws ServiceException {
    byte[] regionName = request.getRegion().getValue().toByteArray();
    TableName tableName = HRegionInfo.getTable(regionName);
    if (MobUtils.isMobRegionName(tableName, regionName)) {
      // a dummy region info contains the compaction state.
      HRegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(tableName);
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(HRegionInfo.convert(mobRegionInfo));
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
    List<HColumnDescriptor> compactedColumns = new ArrayList<>();
    HColumnDescriptor[] hcds = master.getTableDescriptors().get(tableName).getColumnFamilies();
    byte[] family = null;
    if (request.hasFamily()) {
      family = request.getFamily().toByteArray();
      for (HColumnDescriptor hcd : hcds) {
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
      for (HColumnDescriptor hcd : hcds) {
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
        boolean bypass = false;
        if (master.cpHost != null) {
          bypass = master.cpHost.preSetSplitOrMergeEnabled(newValue, switchType);
        }
        if (!bypass) {
          master.getSplitOrMergeTracker().setSplitOrMergeEnabled(newValue, switchType);
        }
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
    try {
      return NormalizeResponse.newBuilder().setNormalizerRan(master.normalizeRegions()).build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public SetNormalizerRunningResponse setNormalizerRunning(RpcController controller,
      SetNormalizerRunningRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      boolean prevValue = normalizerSwitch(request.getOn());
      return SetNormalizerRunningResponse.newBuilder().setPrevNormalizerValue(prevValue).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
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
      Set<Capability> capabilities = new HashSet<>();
      // Authentication
      if (User.isHBaseSecurityEnabled(master.getConfiguration())) {
        capabilities.add(Capability.SECURE_AUTHENTICATION);
      } else {
        capabilities.add(Capability.SIMPLE_AUTHENTICATION);
      }
      // The AccessController can provide AUTHORIZATION and CELL_AUTHORIZATION
      if (master.cpHost != null &&
            master.cpHost.findCoprocessor(AccessController.class.getName()) != null) {
        if (AccessController.isAuthorizationSupported(master.getConfiguration())) {
          capabilities.add(Capability.AUTHORIZATION);
        }
        if (AccessController.isCellAuthorizationSupported(master.getConfiguration())) {
          capabilities.add(Capability.CELL_AUTHORIZATION);
        }
      }
      // The VisibilityController can provide CELL_VISIBILITY
      if (master.cpHost != null &&
            master.cpHost.findCoprocessor(VisibilityController.class.getName()) != null) {
        if (VisibilityController.isCellAuthorizationSupported(master.getConfiguration())) {
          capabilities.add(Capability.CELL_VISIBILITY);
        }
      }
      response.addAllCapabilities(capabilities);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
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
      master.addReplicationPeer(request.getPeerId(),
        ReplicationSerDeHelper.convert(request.getPeerConfig()));
      return AddReplicationPeerResponse.newBuilder().build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
      RemoveReplicationPeerRequest request) throws ServiceException {
    try {
      master.removeReplicationPeer(request.getPeerId());
      return RemoveReplicationPeerResponse.newBuilder().build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller,
      EnableReplicationPeerRequest request) throws ServiceException {
    try {
      master.enableReplicationPeer(request.getPeerId());
      return EnableReplicationPeerResponse.newBuilder().build();
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller,
      DisableReplicationPeerRequest request) throws ServiceException {
    try {
      master.disableReplicationPeer(request.getPeerId());
      return DisableReplicationPeerResponse.newBuilder().build();
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
      response.setPeerConfig(ReplicationSerDeHelper.convert(peerConfig));
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(RpcController controller,
      UpdateReplicationPeerConfigRequest request) throws ServiceException {
    try {
      master.updateReplicationPeerConfig(request.getPeerId(),
        ReplicationSerDeHelper.convert(request.getPeerConfig()));
      return UpdateReplicationPeerConfigResponse.newBuilder().build();
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
        response.addPeerDesc(ReplicationSerDeHelper.toProtoReplicationPeerDescription(peer));
      }
    } catch (ReplicationException | IOException e) {
      throw new ServiceException(e);
    }
    return response.build();
  }

  @Override
  public ListDrainingRegionServersResponse listDrainingRegionServers(RpcController controller,
      ListDrainingRegionServersRequest request) throws ServiceException {
    ListDrainingRegionServersResponse.Builder response =
        ListDrainingRegionServersResponse.newBuilder();
    try {
      master.checkInitialized();
      List<ServerName> servers = master.listDrainingRegionServers();
      for (ServerName server : servers) {
        response.addServerName(ProtobufUtil.toServerName(server));
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return response.build();
  }

  @Override
  public DrainRegionServersResponse drainRegionServers(RpcController controller,
      DrainRegionServersRequest request) throws ServiceException {
    DrainRegionServersResponse.Builder response = DrainRegionServersResponse.newBuilder();
    try {
      master.checkInitialized();
      for (HBaseProtos.ServerName pbServer : request.getServerNameList()) {
        master.drainRegionServer(ProtobufUtil.toServerName(pbServer));
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return response.build();
  }

  @Override
  public RemoveDrainFromRegionServersResponse removeDrainFromRegionServers(RpcController controller,
      RemoveDrainFromRegionServersRequest request) throws ServiceException {
    RemoveDrainFromRegionServersResponse.Builder response =
        RemoveDrainFromRegionServersResponse.newBuilder();
    try {
      master.checkInitialized();
      for (HBaseProtos.ServerName pbServer : request.getServerNameList()) {
        master.removeDrainFromRegionServer(ProtobufUtil.toServerName(pbServer));
      }
    } catch (IOException io) {
      throw new ServiceException(io);
    }

    return response.build();
  }

  @Override
  public LockResponse requestLock(RpcController controller, final LockRequest request)
      throws ServiceException {
    try {
      if (request.getDescription().isEmpty()) {
        throw new IllegalArgumentException("Empty description");
      }
      NonceProcedureRunnable npr;
      LockProcedure.LockType type = LockProcedure.LockType.valueOf(request.getLockType().name());
      if (request.getRegionInfoCount() > 0) {
        final HRegionInfo[] regionInfos = new HRegionInfo[request.getRegionInfoCount()];
        for (int i = 0; i < request.getRegionInfoCount(); ++i) {
          regionInfos[i] = HRegionInfo.convert(request.getRegionInfo(i));
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
}
