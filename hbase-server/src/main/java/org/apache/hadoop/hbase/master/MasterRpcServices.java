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
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.*;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AbortProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AbortProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ClearDeadServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ClearDeadServersResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCleanerChoreEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotCleanupEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotCleanupEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespacesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespacesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListProceduresRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListProceduresResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MajorCompactionTimestampForRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MajorCompactionTimestampRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MajorCompactionTimestampResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.NormalizeResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCleanerChoreRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCleanerChoreResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse.Capability;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetCleanerChoreRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetCleanerChoreRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetNormalizerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetSnapshotCleanupRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetSnapshotCleanupResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.VisibilityController;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * Implements the master RPC services.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class MasterRpcServices extends RSRpcServices
    implements MasterService.BlockingInterface, RegionServerStatusService.BlockingInterface {
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
        newValue = master.cpHost.preBalanceSwitch(newValue);
      }
      try {
        if (mode == BalanceSwitchMode.SYNC) {
          synchronized (master.balancer) {
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
      master.getLoadBalancer().updateBalancerStatus(newValue);
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
    List<BlockingServiceAndInterface> bssi = new ArrayList<BlockingServiceAndInterface>(4);
    bssi.add(new BlockingServiceAndInterface(
      MasterService.newReflectiveBlockingService(this),
      MasterService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(
      RegionServerStatusService.newReflectiveBlockingService(this),
      RegionServerStatusService.BlockingInterface.class));
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
    RegionStoreSequenceIds ids = master.serverManager.getLastFlushedSequenceId(encodedRegionName);
    return ResponseConverter.buildGetLastFlushedSequenceIdResponse(ids);
  }

  @Override
  public RegionServerReportResponse regionServerReport(
      RpcController controller, RegionServerReportRequest request) throws ServiceException {
    try {
      master.checkServiceStarted();
      ClusterStatusProtos.ServerLoad sl = request.getLoad();
      ServerName serverName = ProtobufUtil.toServerName(request.getServer());
      ServerLoad oldLoad = master.serverManager.getLoad(serverName);
      master.serverManager.regionServerReport(serverName, new ServerLoad(sl));
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
      ServerName rs = master.serverManager.regionServerStartup(request, ia);

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
      master.addColumn(
          ProtobufUtil.toTableName(req.getTableName()),
          HColumnDescriptor.convert(req.getColumnFamilies()),
          req.getNonceGroup(),
          req.getNonce());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return AddColumnResponse.newBuilder().build();
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
      RegionStates regionStates = master.assignmentManager.getRegionStates();
      HRegionInfo regionInfo = regionStates.getRegionInfo(regionName);
      if (regionInfo == null) throw new UnknownRegionException(Bytes.toString(regionName));
      if (master.cpHost != null) {
        if (master.cpHost.preAssign(regionInfo)) {
          return arr;
        }
      }
      LOG.info(master.getClientIdAuditPrefix()
        + " assign " + regionInfo.getRegionNameAsString());
      master.assignmentManager.assign(regionInfo, true, true);
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
      master.createNamespace(
        ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()),
        request.getNonceGroup(),
        request.getNonce());
      return CreateNamespaceResponse.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CreateTableResponse createTable(RpcController controller, CreateTableRequest req)
  throws ServiceException {
    HTableDescriptor hTableDescriptor = HTableDescriptor.convert(req.getTableSchema());
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
      master.deleteColumn(
        ProtobufUtil.toTableName(req.getTableName()),
        req.getColumnName().toByteArray(),
        req.getNonceGroup(),
        req.getNonce());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DeleteColumnResponse.newBuilder().build();
  }

  @Override
  public DeleteNamespaceResponse deleteNamespace(RpcController controller,
      DeleteNamespaceRequest request) throws ServiceException {
    try {
      master.deleteNamespace(
        request.getNamespaceName(),
        request.getNonceGroup(),
        request.getNonce());
      return DeleteNamespaceResponse.getDefaultInstance();
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
      master.truncateTable(
        ProtobufUtil.toTableName(request.getTableName()),
        request.getPreserveSplits(),
        request.getNonceGroup(),
        request.getNonce());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return TruncateTableResponse.newBuilder().build();
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
  public DispatchMergingRegionsResponse dispatchMergingRegions(RpcController c,
      DispatchMergingRegionsRequest request) throws ServiceException {
    try {
      master.checkInitialized();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    final byte[] encodedNameOfRegionA = request.getRegionA().getValue()
      .toByteArray();
    final byte[] encodedNameOfRegionB = request.getRegionB().getValue()
      .toByteArray();
    final boolean forcible = request.getForcible();
    if (request.getRegionA().getType() != RegionSpecifierType.ENCODED_REGION_NAME
        || request.getRegionB().getType() != RegionSpecifierType.ENCODED_REGION_NAME) {
      LOG.warn("mergeRegions specifier type: expected: "
        + RegionSpecifierType.ENCODED_REGION_NAME + " actual: region_a="
        + request.getRegionA().getType() + ", region_b="
        + request.getRegionB().getType());
    }
    RegionStates regionStates = master.assignmentManager.getRegionStates();
    RegionState regionStateA = regionStates.getRegionState(Bytes.toString(encodedNameOfRegionA));
    RegionState regionStateB = regionStates.getRegionState(Bytes.toString(encodedNameOfRegionB));
    if (regionStateA == null || regionStateB == null) {
      throw new ServiceException(new UnknownRegionException(
          Bytes.toStringBinary(regionStateA == null ? encodedNameOfRegionA
              : encodedNameOfRegionB)));
    }

    if (!regionStateA.isOpened() || !regionStateB.isOpened()) {
      throw new ServiceException(new MergeRegionException(
        "Unable to merge regions not online " + regionStateA + ", " + regionStateB));
    }

    final HRegionInfo regionInfoA = regionStateA.getRegion();
    final HRegionInfo regionInfoB = regionStateB.getRegion();
    if (regionInfoA.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID ||
        regionInfoB.getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
      throw new ServiceException(new MergeRegionException("Can't merge non-default replicas"));
    }
    if (regionInfoA.compareTo(regionInfoB) == 0) {
      throw new ServiceException(new MergeRegionException(
        "Unable to merge a region to itself " + regionInfoA + ", " + regionInfoB));
    }
    try {
      master.cpHost.preDispatchMerge(regionInfoA, regionInfoB);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    if (!forcible && !HRegionInfo.areAdjacent(regionInfoA, regionInfoB)) {
      throw new ServiceException(new MergeRegionException(
        "Unable to merge not adjacent regions "
          + regionInfoA.getRegionNameAsString() + ", "
          + regionInfoB.getRegionNameAsString()
          + " where forcible = " + forcible));
    }

    try {
      master.dispatchMergingRegions(regionInfoA, regionInfoB, forcible, RpcServer.getRequestUser());
      master.cpHost.postDispatchMerge(regionInfoA, regionInfoB);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    return DispatchMergingRegionsResponse.newBuilder().build();
  }

  @Override
  public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController c,
      EnableCatalogJanitorRequest req) throws ServiceException {
    rpcPreCheck("enableCatalogJanitor");
    return EnableCatalogJanitorResponse.newBuilder().setPrevValue(
      master.catalogJanitorChore.setEnabled(req.getEnable())).build();
  }

  @Override
  public SetCleanerChoreRunningResponse setCleanerChoreRunning(RpcController c,
      SetCleanerChoreRunningRequest req) throws ServiceException {
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
          "No registered master coprocessor service found for name "+serviceName);
      }

      Service service = master.coprocessorServiceHandlers.get(serviceName);
      Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();
      Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
      if (methodDesc == null) {
        throw new UnknownProtocolException(service.getClass(),
          "Unknown method "+methodName+" called on master service "+serviceName);
      }

      //invoke the method
      Message.Builder builderForType = service.getRequestPrototype(methodDesc).newBuilderForType();
      ProtobufUtil.mergeFrom(builderForType, call.getRequest());
      Message execRequest = builderForType.build();
      final Message.Builder responseBuilder =
          service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, execController, execRequest, new RpcCallback<Message>() {
        @Override
        public void run(Message message) {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
        }
      });
      Message execResult = responseBuilder.build();

      if (execController.getFailedOn() != null) {
        throw execController.getFailedOn();
      }
      ClientProtos.CoprocessorServiceResponse.Builder builder =
        ClientProtos.CoprocessorServiceResponse.newBuilder();
      builder.setRegion(RequestConverter.buildRegionSpecifier(
        RegionSpecifierType.REGION_NAME, HConstants.EMPTY_BYTE_ARRAY));
      builder.setValue(
        builder.getValueBuilder().setName(execResult.getClass().getName())
          .setValue(execResult.toByteString()));
      return builder.build();
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
    rpcPreCheck("execProcedure");
    try {
      ProcedureDescription desc = request.getProcedure();
      MasterProcedureManager mpm = master.getMasterProcedureManagerHost().getProcedureManager(
        desc.getSignature());
      if (mpm == null) {
        throw new ServiceException("The procedure is not registered: "
          + desc.getSignature());
      }
      LOG.info(master.getClientIdAuditPrefix() + " procedure request for: " + desc.getSignature());
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
        builder.setReturnData(ByteString.copyFrom(data));
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
      response.setClusterStatus(master.getClusterStatus().convert());
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
  public ListNamespacesResponse listNamespaces(
      RpcController controller, ListNamespacesRequest request)
      throws ServiceException {
    try {
      return ListNamespacesResponse.newBuilder()
        .addAllNamespaceName(master.listNamespaces())
        .build();
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
            master.getNamespaceDescriptor(request.getNamespaceName())))
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
      Pair<Integer,Integer> pair = master.assignmentManager.getReopenStatus(tableName);
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
        for (TableProtos.TableName tableNamePB: req.getTableNamesList()) {
          tableNameList.add(ProtobufUtil.toTableName(tableNamePB));
        }
      }

      List<HTableDescriptor> descriptors = master.listTableDescriptors(namespace, regex,
          tableNameList, req.getIncludeSysTables());

      GetTableDescriptorsResponse.Builder builder = GetTableDescriptorsResponse.newBuilder();
      if (descriptors != null && descriptors.size() > 0) {
        // Add the table descriptors to the response
        for (HTableDescriptor htd: descriptors) {
          builder.addTableSchema(htd.convert());
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
      master.checkInitialized();

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
  public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController c,
      IsCatalogJanitorEnabledRequest req) throws ServiceException {
    return IsCatalogJanitorEnabledResponse.newBuilder().setValue(
      master.isCatalogJanitorEnabled()).build();
  }

  @Override
  public IsCleanerChoreEnabledResponse isCleanerChoreEnabled(RpcController c,
      IsCleanerChoreEnabledRequest req) throws ServiceException {
    return IsCleanerChoreEnabledResponse.newBuilder()
        .setValue(master.isCleanerChoreEnabled()).build();
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
   * @return true if the procedure is done,
   *   false if the procedure is in the process of completing
   * @throws ServiceException if invalid procedure, or
   *  a failed procedure with progress failure reason.
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
   * Returns the status of the requested snapshot restore/clone operation.
   * This method is not exposed to the user, it is just used internally by HBaseAdmin
   * to verify if the restore is completed.
   *
   * No exceptions are thrown if the restore is not running, the result will be "done".
   *
   * @return done <tt>true</tt> if the restore/clone operation is completed.
   * @throws ServiceException if the operation failed.
   */
  @Override
  public IsRestoreSnapshotDoneResponse isRestoreSnapshotDone(RpcController controller,
      IsRestoreSnapshotDoneRequest request) throws ServiceException {
    try {
      master.checkInitialized();
      SnapshotDescription snapshot = request.getSnapshot();
      IsRestoreSnapshotDoneResponse.Builder builder = IsRestoreSnapshotDoneResponse.newBuilder();
      boolean done = master.snapshotManager.isRestoreDone(snapshot);
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
        builder.setStartTime(result.getStartTime());
        builder.setLastUpdate(result.getLastUpdate());
        if (result.isFailed()) {
          builder.setException(result.getForeignExceptionMessage());
        }
        if (result.hasResultData()) {
          builder.setResult(ByteStringer.wrap(result.getResult()));
        }
        master.getMasterProcedureExecutor().removeResult(request.getProcId());
      } else {
        Procedure proc = v.getSecond();
        if (proc == null) {
          builder.setState(GetProcedureResultResponse.State.NOT_FOUND);
        } else {
          builder.setState(GetProcedureResultResponse.State.RUNNING);
          builder.setStartTime(proc.getStartTime());
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
  public ListProceduresResponse listProcedures(
      RpcController rpcController,
      ListProceduresRequest request) throws ServiceException {
    try {
      ListProceduresResponse.Builder response =
          ListProceduresResponse.newBuilder();
      for(ProcedureInfo p: master.listProcedures()) {
        response.addProcedure(ProcedureInfo.convertToProcedureProto(p));
      }
      return response.build();
    } catch (IOException e) {
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
  public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController c,
      ListNamespaceDescriptorsRequest request) throws ServiceException {
    try {
      ListNamespaceDescriptorsResponse.Builder response =
        ListNamespaceDescriptorsResponse.newBuilder();
      for(NamespaceDescriptor ns: master.listNamespaceDescriptors()) {
        response.addNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(ns));
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
        b.addTableSchema(htd.convert());
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
      master.modifyColumn(
        ProtobufUtil.toTableName(req.getTableName()),
        HColumnDescriptor.convert(req.getColumnFamilies()),
        req.getNonceGroup(),
        req.getNonce());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ModifyColumnResponse.newBuilder().build();
  }

  @Override
  public ModifyNamespaceResponse modifyNamespace(RpcController controller,
      ModifyNamespaceRequest request) throws ServiceException {
    try {
      master.modifyNamespace(
        ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()),
        request.getNonceGroup(),
        request.getNonce());
      return ModifyNamespaceResponse.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyTableResponse modifyTable(RpcController controller,
      ModifyTableRequest req) throws ServiceException {
    try {
      master.modifyTable(
        ProtobufUtil.toTableName(req.getTableName()),
        HTableDescriptor.convert(req.getTableSchema()),
        req.getNonceGroup(),
        req.getNonce());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ModifyTableResponse.newBuilder().build();
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
      master.assignmentManager.regionOffline(hri);
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
      master.checkInitialized();
      master.snapshotManager.checkSnapshotSupport();

      // ensure namespace exists
      TableName dstTable = TableName.valueOf(request.getSnapshot().getTable());
      master.ensureNamespaceExists(dstTable.getNamespaceAsString());

      SnapshotDescription reqSnapshot = request.getSnapshot();
      master.snapshotManager.restoreSnapshot(reqSnapshot,
        request.hasRestoreACL() && request.getRestoreACL());
      return RestoreSnapshotResponse.newBuilder().build();
    } catch (ForeignException e) {
      throw new ServiceException(e.getCause());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetSnapshotCleanupResponse switchSnapshotCleanup(
      RpcController controller, SetSnapshotCleanupRequest request)
      throws ServiceException {
    try {
      master.checkInitialized();
      final boolean enabled = request.getEnabled();
      final boolean isSynchronous = request.hasSynchronous() && request.getSynchronous();
      final boolean prevSnapshotCleanupRunning = this.switchSnapshotCleanup(enabled, isSynchronous);
      return SetSnapshotCleanupResponse.newBuilder()
          .setPrevSnapshotCleanup(prevSnapshotCleanupRunning).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public IsSnapshotCleanupEnabledResponse isSnapshotCleanupEnabled(
      RpcController controller, IsSnapshotCleanupEnabledRequest request)
      throws ServiceException {
    try {
      master.checkInitialized();
      final boolean isSnapshotCleanupEnabled = master.snapshotCleanupTracker
          .isSnapshotCleanupEnabled();
      return IsSnapshotCleanupEnabledResponse.newBuilder()
          .setEnabled(isSnapshotCleanupEnabled).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Turn on/off snapshot auto-cleanup based on TTL
   *
   * @param enabledNewVal Set to <code>true</code> to enable, <code>false</code> to disable
   * @param synchronous If <code>true</code>, it waits until current snapshot cleanup is completed,
   *   if outstanding
   * @return previous snapshot auto-cleanup mode
   */
  private synchronized boolean switchSnapshotCleanup(final boolean enabledNewVal,
      final boolean synchronous) {
    final boolean oldValue = master.snapshotCleanupTracker.isSnapshotCleanupEnabled();
    master.switchSnapshotCleanup(enabledNewVal, synchronous);
    LOG.info(master.getClientIdAuditPrefix() + " Successfully set snapshot cleanup to {}" +
      enabledNewVal);
    return oldValue;
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
    Boolean result = master.getHFileCleaner().runCleaner() && master.getLogCleaner().runCleaner();
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
      Pair<HRegionInfo, ServerName> pair =
        MetaTableAccessor.getRegion(master.getConnection(), regionName);
      if (pair == null) throw new UnknownRegionException(Bytes.toString(regionName));
      HRegionInfo hri = pair.getFirst();
      if (master.cpHost != null) {
        if (master.cpHost.preUnassign(hri, force)) {
          return urr;
        }
      }
      LOG.debug(master.getClientIdAuditPrefix() + " unassign " + hri.getRegionNameAsString()
          + " in current location if it is online and reassign.force=" + force);
      master.assignmentManager.unassign(hri, force);
      if (master.assignmentManager.getRegionStates().isRegionOffline(hri)) {
        LOG.debug("Region " + hri.getRegionNameAsString()
            + " is not online on any region server, reassigning it.");
        master.assignmentManager.assign(hri, true);
      }
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
      for (HBaseProtos.RegionInfo ri : rt.getRegionInfoList())  {
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
      String error = master.assignmentManager.onRegionTransition(sn, rt);
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

  @Override
  public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller,
      IsBalancerEnabledRequest request) throws ServiceException {
    IsBalancerEnabledResponse.Builder response = IsBalancerEnabledResponse.newBuilder();
    response.setEnabled(master.isBalancerOn());
    return response.build();
  }

  @Override
  public MasterProtos.SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(
    RpcController controller,
    MasterProtos.SetSplitOrMergeEnabledRequest request) throws ServiceException {
    MasterProtos.SetSplitOrMergeEnabledResponse.Builder response =
            MasterProtos.SetSplitOrMergeEnabledResponse.newBuilder();
    try {
      master.checkInitialized();
      boolean newValue = request.getEnabled();
      for (MasterProtos.MasterSwitchType masterSwitchType : request.getSwitchTypesList()) {
        Admin.MasterSwitchType switchType = convert(masterSwitchType);
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
  public MasterProtos.IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(RpcController controller,
    MasterProtos.IsSplitOrMergeEnabledRequest request) throws ServiceException {
    MasterProtos.IsSplitOrMergeEnabledResponse.Builder response =
            MasterProtos.IsSplitOrMergeEnabledResponse.newBuilder();
    response.setEnabled(master.isSplitOrMergeEnabled(convert(request.getSwitchType())));
    return response.build();
  }

  @Override
  public NormalizeResponse normalize(RpcController controller,
      NormalizeRequest request) throws ServiceException {
    rpcPreCheck("normalize");
    try {
      return NormalizeResponse.newBuilder().setNormalizerRan(master.normalizeRegions()).build();
    } catch (IOException | CoordinatedStateException ex) {
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
    LOG.info(master.getClientIdAuditPrefix() + " set normalizerSwitch=" + newValue);
    return SetNormalizerRunningResponse.newBuilder().setPrevNormalizerValue(prevValue).build();
  }

  @Override
  public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller,
      IsNormalizerEnabledRequest request) throws ServiceException {
    IsNormalizerEnabledResponse.Builder response = IsNormalizerEnabledResponse.newBuilder();
    response.setEnabled(master.isNormalizerOn());
    return response.build();
  }

  @Override
  public SetQuotaResponse setQuota(RpcController c, SetQuotaRequest req) throws ServiceException {
    try {
      master.checkInitialized();
      return master.getMasterQuotaManager().setQuota(req);
    } catch (Exception e) {
      throw new ServiceException(e);
    }
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
        if (AccessChecker.isAuthorizationSupported(master.getConfiguration())) {
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

  private Admin.MasterSwitchType convert(MasterProtos.MasterSwitchType switchType) {
    switch (switchType) {
      case SPLIT:
        return Admin.MasterSwitchType.SPLIT;
      case MERGE:
        return Admin.MasterSwitchType.MERGE;
      default:
        break;
    }
    return null;
  }
}
