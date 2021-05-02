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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CacheEvictionStatsBuilder;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.RegionMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.ShadedAccessControlUtil;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos
  .IsSnapshotCleanupEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespacesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampForRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse.RegionSizes;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse.TableQuotaSnapshot;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBaseAdmin is no longer a client API. It is marked InterfaceAudience.Private indicating that
 * this is an HBase-internal class as defined in
 * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/InterfaceClassification.html
 * There are no guarantees for backwards source / binary compatibility and methods or class can
 * change or go away without deprecation.
 * Use {@link Connection#getAdmin()} to obtain an instance of {@link Admin} instead of constructing
 * an HBaseAdmin directly.
 *
 * <p>Connection should be an <i>unmanaged</i> connection obtained via
 * {@link ConnectionFactory#createConnection(Configuration)}
 *
 * @see ConnectionFactory
 * @see Connection
 * @see Admin
 */
@InterfaceAudience.Private
public class HBaseAdmin implements Admin {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseAdmin.class);

  private ClusterConnection connection;

  private final Configuration conf;
  private final long pause;
  private final int numRetries;
  private final int syncWaitTimeout;
  private boolean aborted;
  private int operationTimeout;
  private int rpcTimeout;
  private int getProcedureTimeout;

  private RpcRetryingCallerFactory rpcCallerFactory;
  private RpcControllerFactory rpcControllerFactory;

  private NonceGenerator ng;

  @Override
  public int getOperationTimeout() {
    return operationTimeout;
  }

  @Override
  public int getSyncWaitTimeout() {
    return syncWaitTimeout;
  }

  HBaseAdmin(ClusterConnection connection) throws IOException {
    this.conf = connection.getConfiguration();
    this.connection = connection;

    // TODO: receive ConnectionConfiguration here rather than re-parsing these configs every time.
    this.pause = this.conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.operationTimeout = this.conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.rpcTimeout = this.conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    this.syncWaitTimeout = this.conf.getInt(
      "hbase.client.sync.wait.timeout.msec", 10 * 60000); // 10min
    this.getProcedureTimeout =
        this.conf.getInt("hbase.client.procedure.future.get.timeout.msec", 10 * 60000); // 10min

    this.rpcCallerFactory = connection.getRpcRetryingCallerFactory();
    this.rpcControllerFactory = connection.getRpcControllerFactory();

    this.ng = this.connection.getNonceGenerator();
  }

  @Override
  public void abort(String why, Throwable e) {
    // Currently does nothing but throw the passed message and exception
    this.aborted = true;
    throw new RuntimeException(why, e);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  @Override
  public boolean abortProcedure(final long procId, final boolean mayInterruptIfRunning)
  throws IOException {
    return get(abortProcedureAsync(procId, mayInterruptIfRunning), this.syncWaitTimeout,
      TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<Boolean> abortProcedureAsync(final long procId, final boolean mayInterruptIfRunning)
      throws IOException {
    Boolean abortProcResponse =
        executeCallable(new MasterCallable<AbortProcedureResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected AbortProcedureResponse rpcCall() throws Exception {
        AbortProcedureRequest abortProcRequest =
            AbortProcedureRequest.newBuilder().setProcId(procId).build();
        return master.abortProcedure(getRpcController(), abortProcRequest);
      }
    }).getIsProcedureAborted();
    return new AbortProcedureFuture(this, procId, abortProcResponse);
  }

  @Override
  public List<TableDescriptor> listTableDescriptors() throws IOException {
    return listTableDescriptors((Pattern)null, false);
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
      throws IOException {
    return executeCallable(new MasterCallable<List<TableDescriptor>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<TableDescriptor> rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(pattern, includeSysTables);
        return ProtobufUtil.toTableDescriptorList(master.getTableDescriptors(getRpcController(),
            req));
      }
    });
  }

  @Override
  public TableDescriptor getDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    return getTableDescriptor(tableName, getConnection(), rpcCallerFactory, rpcControllerFactory,
       operationTimeout, rpcTimeout);
  }

  @Override
  public Future<Void> modifyTableAsync(TableDescriptor td) throws IOException {
    ModifyTableResponse response = executeCallable(
      new MasterCallable<ModifyTableResponse>(getConnection(), getRpcControllerFactory()) {
        Long nonceGroup = ng.getNonceGroup();
        Long nonce = ng.newNonce();
        @Override
        protected ModifyTableResponse rpcCall() throws Exception {
          setPriority(td.getTableName());
          ModifyTableRequest request = RequestConverter.buildModifyTableRequest(
            td.getTableName(), td, nonceGroup, nonce);
          return master.modifyTable(getRpcController(), request);
        }
      });
    return new ModifyTableFuture(this, td.getTableName(), response);
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] name) throws IOException {
    return executeCallable(new MasterCallable<List<TableDescriptor>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<TableDescriptor> rpcCall() throws Exception {
        return master.listTableDescriptorsByNamespace(getRpcController(),
                ListTableDescriptorsByNamespaceRequest.newBuilder()
                  .setNamespaceName(Bytes.toString(name)).build())
                .getTableSchemaList()
                .stream()
                .map(ProtobufUtil::toTableDescriptor)
                .collect(Collectors.toList());
      }
    });
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(List<TableName> tableNames) throws IOException {
    return executeCallable(new MasterCallable<List<TableDescriptor>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<TableDescriptor> rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(tableNames);
          return ProtobufUtil.toTableDescriptorList(master.getTableDescriptors(getRpcController(),
              req));
      }
    });
  }

  @Override
  public List<RegionInfo> getRegions(final ServerName sn) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    return ProtobufUtil.getOnlineRegions(controller, admin);
  }

  @Override
  public List<RegionInfo> getRegions(TableName tableName) throws IOException {
    if (TableName.isMetaTableName(tableName)) {
      return Arrays.asList(RegionInfoBuilder.FIRST_META_REGIONINFO);
    } else {
      return MetaTableAccessor.getTableRegions(connection, tableName, true);
    }
  }

  private static class AbortProcedureFuture extends ProcedureFuture<Boolean> {
    private boolean isAbortInProgress;

    public AbortProcedureFuture(
        final HBaseAdmin admin,
        final Long procId,
        final Boolean abortProcResponse) {
      super(admin, procId);
      this.isAbortInProgress = abortProcResponse;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!this.isAbortInProgress) {
        return false;
      }
      super.get(timeout, unit);
      return true;
    }
  }

  /** @return Connection used by this object. */
  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean tableExists(final TableName tableName) throws IOException {
    return executeCallable(new RpcRetryingCallable<Boolean>() {
      @Override
      protected Boolean rpcCall(int callTimeout) throws Exception {
        return MetaTableAccessor.getTableState(getConnection(), tableName) != null;
      }
    });
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return listTables((Pattern)null, false);
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    return listTables(pattern, false);
  }

  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex), false);
  }

  @Override
  public HTableDescriptor[] listTables(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected HTableDescriptor[] rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(pattern, includeSysTables);
        return ProtobufUtil.toTableDescriptorList(master.getTableDescriptors(getRpcController(),
                req)).stream().map(ImmutableHTableDescriptor::new).toArray(HTableDescriptor[]::new);
      }
    });
  }

  @Override
  public HTableDescriptor[] listTables(String regex, boolean includeSysTables)
      throws IOException {
    return listTables(Pattern.compile(regex), includeSysTables);
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return listTableNames((Pattern)null, false);
  }

  @Override
  public TableName[] listTableNames(String regex) throws IOException {
    return listTableNames(Pattern.compile(regex), false);
  }

  @Override
  public TableName[] listTableNames(final Pattern pattern, final boolean includeSysTables)
      throws IOException {
    return executeCallable(new MasterCallable<TableName[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected TableName[] rpcCall() throws Exception {
        GetTableNamesRequest req =
            RequestConverter.buildGetTableNamesRequest(pattern, includeSysTables);
        return ProtobufUtil.getTableNameArray(master.getTableNames(getRpcController(), req)
            .getTableNamesList());
      }
    });
  }

  @Override
  public TableName[] listTableNames(final String regex, final boolean includeSysTables)
      throws IOException {
    return listTableNames(Pattern.compile(regex), includeSysTables);
  }

  @Override
  public HTableDescriptor getTableDescriptor(final TableName tableName) throws IOException {
    return getHTableDescriptor(tableName, getConnection(), rpcCallerFactory, rpcControllerFactory,
       operationTimeout, rpcTimeout);
  }

  static TableDescriptor getTableDescriptor(final TableName tableName, Connection connection,
      RpcRetryingCallerFactory rpcCallerFactory, final RpcControllerFactory rpcControllerFactory,
      int operationTimeout, int rpcTimeout) throws IOException {
    if (tableName == null) return null;
    TableDescriptor td =
        executeCallable(new MasterCallable<TableDescriptor>(connection, rpcControllerFactory) {
      @Override
      protected TableDescriptor rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(tableName);
        GetTableDescriptorsResponse htds = master.getTableDescriptors(getRpcController(), req);
        if (!htds.getTableSchemaList().isEmpty()) {
          return ProtobufUtil.toTableDescriptor(htds.getTableSchemaList().get(0));
        }
        return null;
      }
    }, rpcCallerFactory, operationTimeout, rpcTimeout);
    if (td != null) {
      return td;
    }
    throw new TableNotFoundException(tableName.getNameAsString());
  }

  /**
   * @deprecated since 2.0 version and will be removed in 3.0 version.
   *             use {@link #getTableDescriptor(TableName,
   *             Connection, RpcRetryingCallerFactory,RpcControllerFactory,int,int)}
   */
  @Deprecated
  static HTableDescriptor getHTableDescriptor(final TableName tableName, Connection connection,
      RpcRetryingCallerFactory rpcCallerFactory, final RpcControllerFactory rpcControllerFactory,
      int operationTimeout, int rpcTimeout) throws IOException {
    if (tableName == null) {
      return null;
    }
    HTableDescriptor htd =
        executeCallable(new MasterCallable<HTableDescriptor>(connection, rpcControllerFactory) {
          @Override
          protected HTableDescriptor rpcCall() throws Exception {
            GetTableDescriptorsRequest req =
                RequestConverter.buildGetTableDescriptorsRequest(tableName);
            GetTableDescriptorsResponse htds = master.getTableDescriptors(getRpcController(), req);
            if (!htds.getTableSchemaList().isEmpty()) {
              return new ImmutableHTableDescriptor(
                  ProtobufUtil.toTableDescriptor(htds.getTableSchemaList().get(0)));
            }
            return null;
          }
        }, rpcCallerFactory, operationTimeout, rpcTimeout);
    if (htd != null) {
      return new ImmutableHTableDescriptor(htd);
    }
    throw new TableNotFoundException(tableName.getNameAsString());
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  @Override
  public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      createTable(desc, new byte[][] { startKey, endKey });
      return;
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if (splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys);
  }

  @Override
  public Future<Void> createTableAsync(final TableDescriptor desc, final byte[][] splitKeys)
      throws IOException {
    if (desc.getTableName() == null) {
      throw new IllegalArgumentException("TableName cannot be null");
    }
    if (splitKeys != null && splitKeys.length > 0) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
          throw new IllegalArgumentException(
              "Empty split key must not be passed in the split keys.");
        }
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, " +
            "found duplicate: " + Bytes.toStringBinary(splitKey) +
            ", " + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
      }
    }

    CreateTableResponse response = executeCallable(
      new MasterCallable<CreateTableResponse>(getConnection(), getRpcControllerFactory()) {
        Long nonceGroup = ng.getNonceGroup();
        Long nonce = ng.newNonce();
        @Override
        protected CreateTableResponse rpcCall() throws Exception {
          setPriority(desc.getTableName());
          CreateTableRequest request = RequestConverter.buildCreateTableRequest(
            desc, splitKeys, nonceGroup, nonce);
          return master.createTable(getRpcController(), request);
        }
      });
    return new CreateTableFuture(this, desc, splitKeys, response);
  }

  private static class CreateTableFuture extends TableFuture<Void> {
    private final TableDescriptor desc;
    private final byte[][] splitKeys;

    public CreateTableFuture(final HBaseAdmin admin, final TableDescriptor desc,
        final byte[][] splitKeys, final CreateTableResponse response) {
      super(admin, desc.getTableName(),
              (response != null && response.hasProcId()) ? response.getProcId() : null);
      this.splitKeys = splitKeys;
      this.desc = desc;
    }

    @Override
    protected TableDescriptor getTableDescriptor() {
      return desc;
    }

    @Override
    public String getOperationType() {
      return "CREATE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      waitForAllRegionsOnline(deadlineTs, splitKeys);
      return null;
    }
  }

  @Override
  public Future<Void> deleteTableAsync(final TableName tableName) throws IOException {
    DeleteTableResponse response = executeCallable(
      new MasterCallable<DeleteTableResponse>(getConnection(), getRpcControllerFactory()) {
        Long nonceGroup = ng.getNonceGroup();
        Long nonce = ng.newNonce();
        @Override
        protected DeleteTableResponse rpcCall() throws Exception {
          setPriority(tableName);
          DeleteTableRequest req =
              RequestConverter.buildDeleteTableRequest(tableName, nonceGroup,nonce);
          return master.deleteTable(getRpcController(), req);
        }
      });
    return new DeleteTableFuture(this, tableName, response);
  }

  private static class DeleteTableFuture extends TableFuture<Void> {
    public DeleteTableFuture(final HBaseAdmin admin, final TableName tableName,
        final DeleteTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    public String getOperationType() {
      return "DELETE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      waitTableNotFound(deadlineTs);
      return null;
    }

    @Override
    protected Void postOperationResult(final Void result, final long deadlineTs)
        throws IOException, TimeoutException {
      // Delete cached information to prevent clients from using old locations
      ((ClusterConnection) getAdmin().getConnection()).clearRegionCache(getTableName());
      return super.postOperationResult(result, deadlineTs);
    }
  }

  @Override
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  /**
   * Delete tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #deleteTable(TableName)}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<>();
    for (HTableDescriptor table : listTables(pattern)) {
      try {
        deleteTable(table.getTableName());
      } catch (IOException ex) {
        LOG.info("Failed to delete table " + table.getTableName(), ex);
        failed.add(table);
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  @Override
  public Future<Void> truncateTableAsync(final TableName tableName, final boolean preserveSplits)
      throws IOException {
    TruncateTableResponse response =
        executeCallable(new MasterCallable<TruncateTableResponse>(getConnection(),
            getRpcControllerFactory()) {
          Long nonceGroup = ng.getNonceGroup();
          Long nonce = ng.newNonce();
          @Override
          protected TruncateTableResponse rpcCall() throws Exception {
            setPriority(tableName);
            LOG.info("Started truncating " + tableName);
            TruncateTableRequest req = RequestConverter.buildTruncateTableRequest(
              tableName, preserveSplits, nonceGroup, nonce);
            return master.truncateTable(getRpcController(), req);
          }
        });
    return new TruncateTableFuture(this, tableName, preserveSplits, response);
  }

  private static class TruncateTableFuture extends TableFuture<Void> {
    private final boolean preserveSplits;

    public TruncateTableFuture(final HBaseAdmin admin, final TableName tableName,
        final boolean preserveSplits, final TruncateTableResponse response) {
      super(admin, tableName,
             (response != null && response.hasProcId()) ? response.getProcId() : null);
      this.preserveSplits = preserveSplits;
    }

    @Override
    public String getOperationType() {
      return "TRUNCATE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      // once the table is enabled, we know the operation is done. so we can fetch the splitKeys
      byte[][] splitKeys = preserveSplits ? getAdmin().getTableSplits(getTableName()) : null;
      waitForAllRegionsOnline(deadlineTs, splitKeys);
      return null;
    }
  }

  private byte[][] getTableSplits(final TableName tableName) throws IOException {
    byte[][] splits = null;
    try (RegionLocator locator = getConnection().getRegionLocator(tableName)) {
      byte[][] startKeys = locator.getStartKeys();
      if (startKeys.length == 1) {
        return splits;
      }
      splits = new byte[startKeys.length - 1][];
      for (int i = 1; i < startKeys.length; i++) {
        splits[i - 1] = startKeys[i];
      }
    }
    return splits;
  }

  @Override
  public Future<Void> enableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    EnableTableResponse response = executeCallable(
      new MasterCallable<EnableTableResponse>(getConnection(), getRpcControllerFactory()) {
        Long nonceGroup = ng.getNonceGroup();
        Long nonce = ng.newNonce();
        @Override
        protected EnableTableResponse rpcCall() throws Exception {
          setPriority(tableName);
          LOG.info("Started enable of " + tableName);
          EnableTableRequest req =
              RequestConverter.buildEnableTableRequest(tableName, nonceGroup, nonce);
          return master.enableTable(getRpcController(),req);
        }
      });
    return new EnableTableFuture(this, tableName, response);
  }

  private static class EnableTableFuture extends TableFuture<Void> {
    public EnableTableFuture(final HBaseAdmin admin, final TableName tableName,
        final EnableTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    public String getOperationType() {
      return "ENABLE";
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs) throws IOException, TimeoutException {
      waitForTableEnabled(deadlineTs);
      return null;
    }
  }

  @Override
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    return enableTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableDisabled(table.getTableName())) {
        try {
          enableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to enable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  @Override
  public Future<Void> disableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    DisableTableResponse response = executeCallable(
      new MasterCallable<DisableTableResponse>(getConnection(), getRpcControllerFactory()) {
        Long nonceGroup = ng.getNonceGroup();
        Long nonce = ng.newNonce();
        @Override
        protected DisableTableResponse rpcCall() throws Exception {
          setPriority(tableName);
          LOG.info("Started disable of " + tableName);
          DisableTableRequest req =
              RequestConverter.buildDisableTableRequest(
                tableName, nonceGroup, nonce);
          return master.disableTable(getRpcController(), req);
        }
      });
    return new DisableTableFuture(this, tableName, response);
  }

  private static class DisableTableFuture extends TableFuture<Void> {
    public DisableTableFuture(final HBaseAdmin admin, final TableName tableName,
        final DisableTableResponse response) {
      super(admin, tableName,
              (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    @Override
    public String getOperationType() {
      return "DISABLE";
    }

    @Override
    protected Void waitOperationResult(long deadlineTs) throws IOException, TimeoutException {
      waitForTableDisabled(deadlineTs);
      return null;
    }
  }

  @Override
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    return disableTables(Pattern.compile(regex));
  }

  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableEnabled(table.getTableName())) {
        try {
          disableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to disable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  @Override
  public boolean isTableEnabled(final TableName tableName) throws IOException {
    checkTableExists(tableName);
    return executeCallable(new RpcRetryingCallable<Boolean>() {
      @Override
      protected Boolean rpcCall(int callTimeout) throws Exception {
        TableState tableState = MetaTableAccessor.getTableState(getConnection(), tableName);
        if (tableState == null) {
          throw new TableNotFoundException(tableName);
        }
        return tableState.inStates(TableState.State.ENABLED);
      }
    });
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    checkTableExists(tableName);
    return connection.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return connection.isTableAvailable(tableName, null);
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    return connection.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(final TableName tableName) throws IOException {
    return executeCallable(new MasterCallable<Pair<Integer, Integer>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected Pair<Integer, Integer> rpcCall() throws Exception {
        setPriority(tableName);
        GetSchemaAlterStatusRequest req = RequestConverter
            .buildGetSchemaAlterStatusRequest(tableName);
        GetSchemaAlterStatusResponse ret = master.getSchemaAlterStatus(getRpcController(), req);
        Pair<Integer, Integer> pair = new Pair<>(ret.getYetToUpdateRegions(),
            ret.getTotalRegions());
        return pair;
      }
    });
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName) throws IOException {
    return getAlterStatus(TableName.valueOf(tableName));
  }

  @Override
  public Future<Void> addColumnFamilyAsync(final TableName tableName,
      final ColumnFamilyDescriptor columnFamily) throws IOException {
    AddColumnResponse response =
        executeCallable(new MasterCallable<AddColumnResponse>(getConnection(),
            getRpcControllerFactory()) {
          Long nonceGroup = ng.getNonceGroup();
          Long nonce = ng.newNonce();
          @Override
          protected AddColumnResponse rpcCall() throws Exception {
            setPriority(tableName);
            AddColumnRequest req =
                RequestConverter.buildAddColumnRequest(tableName, columnFamily, nonceGroup, nonce);
            return master.addColumn(getRpcController(), req);
          }
        });
    return new AddColumnFamilyFuture(this, tableName, response);
  }

  private static class AddColumnFamilyFuture extends ModifyTableFuture {
    public AddColumnFamilyFuture(final HBaseAdmin admin, final TableName tableName,
        final AddColumnResponse response) {
      super(admin, tableName, (response != null && response.hasProcId()) ? response.getProcId()
          : null);
    }

    @Override
    public String getOperationType() {
      return "ADD_COLUMN_FAMILY";
    }
  }

  /**
   * {@inheritDoc}
   * @deprecated Since 2.0. Will be removed in 3.0. Use
   *     {@link #deleteColumnFamily(TableName, byte[])} instead.
   */
  @Override
  @Deprecated
  public void deleteColumn(final TableName tableName, final byte[] columnFamily)
      throws IOException {
    deleteColumnFamily(tableName, columnFamily);
  }

  @Override
  public Future<Void> deleteColumnFamilyAsync(final TableName tableName, final byte[] columnFamily)
      throws IOException {
    DeleteColumnResponse response =
        executeCallable(new MasterCallable<DeleteColumnResponse>(getConnection(),
            getRpcControllerFactory()) {
          Long nonceGroup = ng.getNonceGroup();
          Long nonce = ng.newNonce();
          @Override
          protected DeleteColumnResponse rpcCall() throws Exception {
            setPriority(tableName);
            DeleteColumnRequest req =
                RequestConverter.buildDeleteColumnRequest(tableName, columnFamily,
                  nonceGroup, nonce);
            return master.deleteColumn(getRpcController(), req);
          }
        });
    return new DeleteColumnFamilyFuture(this, tableName, response);
  }

  private static class DeleteColumnFamilyFuture extends ModifyTableFuture {
    public DeleteColumnFamilyFuture(final HBaseAdmin admin, final TableName tableName,
        final DeleteColumnResponse response) {
      super(admin, tableName, (response != null && response.hasProcId()) ? response.getProcId()
          : null);
    }

    @Override
    public String getOperationType() {
      return "DELETE_COLUMN_FAMILY";
    }
  }

  @Override
  public Future<Void> modifyColumnFamilyAsync(final TableName tableName,
      final ColumnFamilyDescriptor columnFamily) throws IOException {
    ModifyColumnResponse response =
        executeCallable(new MasterCallable<ModifyColumnResponse>(getConnection(),
            getRpcControllerFactory()) {
          Long nonceGroup = ng.getNonceGroup();
          Long nonce = ng.newNonce();
          @Override
          protected ModifyColumnResponse rpcCall() throws Exception {
            setPriority(tableName);
            ModifyColumnRequest req =
                RequestConverter.buildModifyColumnRequest(tableName, columnFamily,
                  nonceGroup, nonce);
            return master.modifyColumn(getRpcController(), req);
          }
        });
    return new ModifyColumnFamilyFuture(this, tableName, response);
  }

  private static class ModifyColumnFamilyFuture extends ModifyTableFuture {
    public ModifyColumnFamilyFuture(final HBaseAdmin admin, final TableName tableName,
        final ModifyColumnResponse response) {
      super(admin, tableName, (response != null && response.hasProcId()) ? response.getProcId()
          : null);
    }

    @Override
    public String getOperationType() {
      return "MODIFY_COLUMN_FAMILY";
    }
  }

  @Deprecated
  @Override
  public void closeRegion(final String regionName, final String unused) throws IOException {
    unassign(Bytes.toBytes(regionName), true);
  }

  @Deprecated
  @Override
  public void closeRegion(final byte [] regionName, final String unused) throws IOException {
    unassign(regionName, true);
  }

  @Deprecated
  @Override
  public boolean closeRegionWithEncodedRegionName(final String encodedRegionName,
      final String unused) throws IOException {
    unassign(Bytes.toBytes(encodedRegionName), true);
    return true;
  }

  @Deprecated
  @Override
  public void closeRegion(final ServerName unused, final HRegionInfo hri) throws IOException {
    unassign(hri.getRegionName(), true);
  }

  /**
   * @param sn
   * @return List of {@link HRegionInfo}.
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegions(ServerName)}.
   */
  @Deprecated
  @Override
  public List<HRegionInfo> getOnlineRegions(final ServerName sn) throws IOException {
    return getRegions(sn).stream().map(ImmutableHRegionInfo::new).collect(Collectors.toList());
  }

  @Override
  public void flush(final TableName tableName) throws IOException {
    flush(tableName, null);
  }

  @Override
  public void flush(final TableName tableName, byte[] columnFamily) throws IOException {
    checkTableExists(tableName);
    if (isTableDisabled(tableName)) {
      LOG.info("Table is disabled: " + tableName.getNameAsString());
      return;
    }
    Map<String, String> props = new HashMap<>();
    if (columnFamily != null) {
      props.put(HConstants.FAMILY_KEY_STR, Bytes.toString(columnFamily));
    }
    execProcedure("flush-table-proc", tableName.getNameAsString(), props);
  }

  @Override
  public void flushRegion(final byte[] regionName) throws IOException {
    flushRegion(regionName, null);
  }

  @Override
  public void flushRegion(final byte[] regionName, byte[] columnFamily) throws IOException {
    Pair<RegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Unknown regionname: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    final RegionInfo regionInfo = regionServerPair.getFirst();
    ServerName serverName = regionServerPair.getSecond();
    flush(this.connection.getAdmin(serverName), regionInfo, columnFamily);
  }

  private void flush(AdminService.BlockingInterface admin, final RegionInfo info,
      byte[] columnFamily)
    throws IOException {
    ProtobufUtil.call(() -> {
      // TODO: There is no timeout on this controller. Set one!
      HBaseRpcController controller = rpcControllerFactory.newController();
      FlushRegionRequest request =
        RequestConverter.buildFlushRegionRequest(info.getRegionName(), columnFamily, false);
      admin.flushRegion(controller, request);
      return null;
    });
  }

  @Override
  public void flushRegionServer(ServerName serverName) throws IOException {
    for (RegionInfo region : getRegions(serverName)) {
      flush(this.connection.getAdmin(serverName), region, null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName)
    throws IOException {
    compact(tableName, null, false, CompactType.NORMAL);
  }

  @Override
  public void compactRegion(final byte[] regionName)
    throws IOException {
    compactRegion(regionName, null, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName, final byte[] columnFamily)
    throws IOException {
    compact(tableName, columnFamily, false, CompactType.NORMAL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compactRegion(final byte[] regionName, final byte[] columnFamily)
    throws IOException {
    compactRegion(regionName, columnFamily, false);
  }

  @Override
  public Map<ServerName, Boolean> compactionSwitch(boolean switchState, List<String>
      serverNamesList) throws IOException {
    List<ServerName> serverList = new ArrayList<>();
    if (serverNamesList.isEmpty()) {
      ClusterMetrics status = getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
      serverList.addAll(status.getLiveServerMetrics().keySet());
    } else {
      for (String regionServerName: serverNamesList) {
        ServerName serverName = null;
        try {
          serverName = ServerName.valueOf(regionServerName);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("Invalid ServerName format: %s",
              regionServerName));
        }
        if (serverName == null) {
          throw new IllegalArgumentException(String.format("Null ServerName: %s",
              regionServerName));
        }
        serverList.add(serverName);
      }
    }
    Map<ServerName, Boolean> res = new HashMap<>(serverList.size());
    for (ServerName serverName: serverList) {
      boolean prev_state = switchCompact(this.connection.getAdmin(serverName), switchState);
      res.put(serverName, prev_state);
    }
    return res;
  }

  private Boolean switchCompact(AdminService.BlockingInterface admin, boolean onOrOff)
      throws IOException {
    return executeCallable(new RpcRetryingCallable<Boolean>() {
      @Override protected Boolean rpcCall(int callTimeout) throws Exception {
        HBaseRpcController controller = rpcControllerFactory.newController();
        CompactionSwitchRequest request =
            CompactionSwitchRequest.newBuilder().setEnabled(onOrOff).build();
        CompactionSwitchResponse compactionSwitchResponse =
            admin.compactionSwitch(controller, request);
        return compactionSwitchResponse.getPrevState();
      }
    });
  }

  @Override
  public void compactRegionServer(final ServerName serverName) throws IOException {
    for (RegionInfo region : getRegions(serverName)) {
      compact(this.connection.getAdmin(serverName), region, false, null);
    }
  }

  @Override
  public void majorCompactRegionServer(final ServerName serverName) throws IOException {
    for (RegionInfo region : getRegions(serverName)) {
      compact(this.connection.getAdmin(serverName), region, true, null);
    }
  }

  @Override
  public void majorCompact(final TableName tableName)
  throws IOException {
    compact(tableName, null, true, CompactType.NORMAL);
  }

  @Override
  public void majorCompactRegion(final byte[] regionName)
  throws IOException {
    compactRegion(regionName, null, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName, final byte[] columnFamily)
  throws IOException {
    compact(tableName, columnFamily, true, CompactType.NORMAL);
  }

  @Override
  public void majorCompactRegion(final byte[] regionName, final byte[] columnFamily)
  throws IOException {
    compactRegion(regionName, columnFamily, true);
  }

  /**
   * Compact a table.
   * Asynchronous operation.
   *
   * @param tableName table or region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @throws IOException if a remote or network exception occurs
   */
  private void compact(final TableName tableName, final byte[] columnFamily,final boolean major,
                       CompactType compactType) throws IOException {
    switch (compactType) {
      case MOB:
        compact(this.connection.getAdminForMaster(), RegionInfo.createMobRegionInfo(tableName),
            major, columnFamily);
        break;
      case NORMAL:
        checkTableExists(tableName);
        for (HRegionLocation loc :connection.locateRegions(tableName, false, false)) {
          ServerName sn = loc.getServerName();
          if (sn == null) {
            continue;
          }
          try {
            compact(this.connection.getAdmin(sn), loc.getRegion(), major, columnFamily);
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to" + (major ? " major" : "") + " compact " + loc.getRegion() +
                  ": " + StringUtils.stringifyException(e));
            }
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown compactType: " + compactType);
    }
  }

  /**
   * Compact an individual region.
   * Asynchronous operation.
   *
   * @param regionName region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  private void compactRegion(final byte[] regionName, final byte[] columnFamily,
      final boolean major) throws IOException {
    Pair<RegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    compact(this.connection.getAdmin(regionServerPair.getSecond()), regionServerPair.getFirst(),
      major, columnFamily);
  }

  private void compact(AdminService.BlockingInterface admin, RegionInfo hri, boolean major,
      byte[] family) throws IOException {
    Callable<Void> callable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO: There is no timeout on this controller. Set one!
        HBaseRpcController controller = rpcControllerFactory.newController();
        CompactRegionRequest request =
            RequestConverter.buildCompactRegionRequest(hri.getRegionName(), major, family);
        admin.compactRegion(controller, request);
        return null;
      }
    };
    ProtobufUtil.call(callable);
  }

  @Override
  public void move(byte[] encodedRegionName) throws IOException {
    move(encodedRegionName, (ServerName) null);
  }

  public void move(final byte[] encodedRegionName, ServerName destServerName) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(encodedRegionName);
        MoveRegionRequest request =
          RequestConverter.buildMoveRegionRequest(encodedRegionName, destServerName);
        master.moveRegion(getRpcController(), request);
        return null;
      }
    });
  }

  @Override
  public void assign(final byte [] regionName) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(regionName);
        AssignRegionRequest request =
            RequestConverter.buildAssignRegionRequest(getRegionName(regionName));
        master.assignRegion(getRpcController(), request);
        return null;
      }
    });
  }

  @Override
  public void unassign(final byte [] regionName) throws IOException {
    final byte[] toBeUnassigned = getRegionName(regionName);
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(regionName);
        UnassignRegionRequest request =
            RequestConverter.buildUnassignRegionRequest(toBeUnassigned);
        master.unassignRegion(getRpcController(), request);
        return null;
      }
    });
  }

  @Override
  public void offline(final byte [] regionName)
  throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(regionName);
        master.offlineRegion(getRpcController(),
            RequestConverter.buildOfflineRegionRequest(regionName));
        return null;
      }
    });
  }

  @Override
  public boolean balancerSwitch(final boolean on, final boolean synchronous)
  throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        SetBalancerRunningRequest req =
            RequestConverter.buildSetBalancerRunningRequest(on, synchronous);
        return master.setBalancerRunning(getRpcController(), req).getPrevBalanceValue();
      }
    });
  }

  @Override
  public boolean balance() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.balance(getRpcController(),
            RequestConverter.buildBalanceRequest(false)).getBalancerRan();
      }
    });
  }

  @Override
  public boolean balance(final boolean force) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.balance(getRpcController(),
            RequestConverter.buildBalanceRequest(force)).getBalancerRan();
      }
    });
  }

  @Override
  public boolean isBalancerEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isBalancerEnabled(getRpcController(),
          RequestConverter.buildIsBalancerEnabledRequest()).getEnabled();
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheEvictionStats clearBlockCache(final TableName tableName) throws IOException {
    checkTableExists(tableName);
    CacheEvictionStatsBuilder cacheEvictionStats = CacheEvictionStats.builder();
    List<Pair<RegionInfo, ServerName>> pairs =
      MetaTableAccessor.getTableRegionsAndLocations(connection, tableName);
    Map<ServerName, List<RegionInfo>> regionInfoByServerName =
        pairs.stream()
            .filter(pair -> !(pair.getFirst().isOffline()))
            .filter(pair -> pair.getSecond() != null)
            .collect(Collectors.groupingBy(pair -> pair.getSecond(),
                Collectors.mapping(pair -> pair.getFirst(), Collectors.toList())));

    for (Map.Entry<ServerName, List<RegionInfo>> entry : regionInfoByServerName.entrySet()) {
      CacheEvictionStats stats = clearBlockCache(entry.getKey(), entry.getValue());
      cacheEvictionStats = cacheEvictionStats.append(stats);
      if (stats.getExceptionCount() > 0) {
        for (Map.Entry<byte[], Throwable> exception : stats.getExceptions().entrySet()) {
          LOG.debug("Failed to clear block cache for "
              + Bytes.toStringBinary(exception.getKey())
              + " on " + entry.getKey() + ": ", exception.getValue());
        }
      }
    }
    return cacheEvictionStats.build();
  }

  private CacheEvictionStats clearBlockCache(final ServerName sn, final List<RegionInfo> hris)
      throws IOException {
    HBaseRpcController controller = rpcControllerFactory.newController();
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    ClearRegionBlockCacheRequest request =
      RequestConverter.buildClearRegionBlockCacheRequest(hris);
    ClearRegionBlockCacheResponse response;
    try {
      response = admin.clearRegionBlockCache(controller, request);
      return ProtobufUtil.toCacheEvictionStats(response.getStats());
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  @Override
  public boolean normalize(NormalizeTableFilterParams ntfp) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.normalize(getRpcController(),
            RequestConverter.buildNormalizeRequest(ntfp)).getNormalizerRan();
      }
    });
  }

  @Override
  public boolean isNormalizerEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isNormalizerEnabled(getRpcController(),
          RequestConverter.buildIsNormalizerEnabledRequest()).getEnabled();
      }
    });
  }

  @Override
  public boolean normalizerSwitch(final boolean on) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        SetNormalizerRunningRequest req =
          RequestConverter.buildSetNormalizerRunningRequest(on);
        return master.setNormalizerRunning(getRpcController(), req).getPrevNormalizerValue();
      }
    });
  }

  @Override
  public boolean catalogJanitorSwitch(final boolean enable) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.enableCatalogJanitor(getRpcController(),
          RequestConverter.buildEnableCatalogJanitorRequest(enable)).getPrevValue();
      }
    });
  }

  @Override
  public int runCatalogJanitor() throws IOException {
    return executeCallable(new MasterCallable<Integer>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Integer rpcCall() throws Exception {
        return master.runCatalogScan(getRpcController(),
          RequestConverter.buildCatalogScanRequest()).getScanResult();
      }
    });
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isCatalogJanitorEnabled(getRpcController(),
          RequestConverter.buildIsCatalogJanitorEnabledRequest()).getValue();
      }
    });
  }

  @Override
  public boolean cleanerChoreSwitch(final boolean on) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override public Boolean rpcCall() throws Exception {
        return master.setCleanerChoreRunning(getRpcController(),
            RequestConverter.buildSetCleanerChoreRunningRequest(on)).getPrevValue();
      }
    });
  }

  @Override
  public boolean runCleanerChore() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override public Boolean rpcCall() throws Exception {
        return master.runCleanerChore(getRpcController(),
            RequestConverter.buildRunCleanerChoreRequest()).getCleanerChoreRan();
      }
    });
  }

  @Override
  public boolean isCleanerChoreEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override public Boolean rpcCall() throws Exception {
        return master.isCleanerChoreEnabled(getRpcController(),
            RequestConverter.buildIsCleanerChoreEnabledRequest()).getValue();
      }
    });
  }

  /**
   * Merge two regions. Synchronous operation.
   * Note: It is not feasible to predict the length of merge.
   *   Therefore, this is for internal testing only.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException if a remote or network exception occurs
   */
  public void mergeRegionsSync(
      final byte[] nameOfRegionA,
      final byte[] nameOfRegionB,
      final boolean forcible) throws IOException {
    get(
      mergeRegionsAsync(nameOfRegionA, nameOfRegionB, forcible),
      syncWaitTimeout,
      TimeUnit.MILLISECONDS);
  }

  /**
   * Merge two regions. Asynchronous operation.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          two adjacent regions
   * @throws IOException if a remote or network exception occurs
   * @deprecated Since 2.0. Will be removed in 3.0. Use
   *     {@link #mergeRegionsAsync(byte[], byte[], boolean)} instead.
   */
  @Deprecated
  @Override
  public void mergeRegions(final byte[] nameOfRegionA,
      final byte[] nameOfRegionB, final boolean forcible)
      throws IOException {
    mergeRegionsAsync(nameOfRegionA, nameOfRegionB, forcible);
  }

  /**
   * Merge two regions. Asynchronous operation.
   * @param nameofRegionsToMerge encoded or full name of daughter regions
   * @param forcible true if do a compulsory merge, otherwise we will only merge
   *          adjacent regions
   */
  @Override
  public Future<Void> mergeRegionsAsync(final byte[][] nameofRegionsToMerge, final boolean forcible)
      throws IOException {
    Preconditions.checkArgument(nameofRegionsToMerge.length >= 2, "Can not merge only %s region",
      nameofRegionsToMerge.length);
    byte[][] encodedNameofRegionsToMerge = new byte[nameofRegionsToMerge.length][];
    for (int i = 0; i < nameofRegionsToMerge.length; i++) {
      encodedNameofRegionsToMerge[i] =
        RegionInfo.isEncodedRegionName(nameofRegionsToMerge[i]) ? nameofRegionsToMerge[i]
          : Bytes.toBytes(RegionInfo.encodeRegionName(nameofRegionsToMerge[i]));
    }

    TableName tableName = null;
    Pair<RegionInfo, ServerName> pair;

    for(int i = 0; i < nameofRegionsToMerge.length; i++) {
      pair = getRegion(nameofRegionsToMerge[i]);

      if (pair != null) {
        if (pair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
          throw new IllegalArgumentException ("Can't invoke merge on non-default regions directly");
        }
        if (tableName == null) {
          tableName = pair.getFirst().getTable();
        } else  if (!tableName.equals(pair.getFirst().getTable())) {
          throw new IllegalArgumentException ("Cannot merge regions from two different tables " +
              tableName + " and " + pair.getFirst().getTable());
        }
      } else {
        throw new UnknownRegionException (
          "Can't invoke merge on unknown region "
          + Bytes.toStringBinary(encodedNameofRegionsToMerge[i]));
      }
    }

    MergeTableRegionsResponse response =
        executeCallable(new MasterCallable<MergeTableRegionsResponse>(getConnection(),
            getRpcControllerFactory()) {
          Long nonceGroup = ng.getNonceGroup();
          Long nonce = ng.newNonce();
      @Override
      protected MergeTableRegionsResponse rpcCall() throws Exception {
        MergeTableRegionsRequest request = RequestConverter
            .buildMergeTableRegionsRequest(
                encodedNameofRegionsToMerge,
                forcible,
                nonceGroup,
                nonce);
        return master.mergeTableRegions(getRpcController(), request);
      }
    });
    return new MergeTableRegionsFuture(this, tableName, response);
  }

  private static class MergeTableRegionsFuture extends TableFuture<Void> {
    public MergeTableRegionsFuture(
        final HBaseAdmin admin,
        final TableName tableName,
        final MergeTableRegionsResponse response) {
      super(admin, tableName,
          (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    public MergeTableRegionsFuture(
        final HBaseAdmin admin,
        final TableName tableName,
        final Long procId) {
      super(admin, tableName, procId);
    }

    @Override
    public String getOperationType() {
      return "MERGE_REGIONS";
    }
  }
  /**
   * Split one region. Synchronous operation.
   * Note: It is not feasible to predict the length of split.
   *   Therefore, this is for internal testing only.
   * @param regionName encoded or full name of region
   * @param splitPoint key where region splits
   * @throws IOException if a remote or network exception occurs
   */
  public void splitRegionSync(byte[] regionName, byte[] splitPoint) throws IOException {
    splitRegionSync(regionName, splitPoint, syncWaitTimeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Split one region. Synchronous operation.
   * @param regionName region to be split
   * @param splitPoint split point
   * @param timeout how long to wait on split
   * @param units time units
   * @throws IOException if a remote or network exception occurs
   */
  public void splitRegionSync(byte[] regionName, byte[] splitPoint, final long timeout,
      final TimeUnit units) throws IOException {
    get(splitRegionAsync(regionName, splitPoint), timeout, units);
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName, byte[] splitPoint)
      throws IOException {
    byte[] encodedNameofRegionToSplit = HRegionInfo.isEncodedRegionName(regionName) ?
        regionName : Bytes.toBytes(HRegionInfo.encodeRegionName(regionName));
    Pair<RegionInfo, ServerName> pair = getRegion(regionName);
    if (pair != null) {
      if (pair.getFirst() != null &&
          pair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
        throw new IllegalArgumentException ("Can't invoke split on non-default regions directly");
      }
    } else {
      throw new UnknownRegionException (
          "Can't invoke merge on unknown region "
              + Bytes.toStringBinary(encodedNameofRegionToSplit));
    }

    return splitRegionAsync(pair.getFirst(), splitPoint);
  }

  Future<Void> splitRegionAsync(RegionInfo hri, byte[] splitPoint) throws IOException {
    TableName tableName = hri.getTable();
    if (hri.getStartKey() != null && splitPoint != null &&
        Bytes.compareTo(hri.getStartKey(), splitPoint) == 0) {
      throw new IOException("should not give a splitkey which equals to startkey!");
    }

    SplitTableRegionResponse response = executeCallable(
        new MasterCallable<SplitTableRegionResponse>(getConnection(), getRpcControllerFactory()) {
          Long nonceGroup = ng.getNonceGroup();
          Long nonce = ng.newNonce();
          @Override
          protected SplitTableRegionResponse rpcCall() throws Exception {
            setPriority(tableName);
            SplitTableRegionRequest request = RequestConverter
                .buildSplitTableRegionRequest(hri, splitPoint, nonceGroup, nonce);
            return master.splitRegion(getRpcController(), request);
          }
        });
    return new SplitTableRegionFuture(this, tableName, response);
  }

  private static class SplitTableRegionFuture extends TableFuture<Void> {
    public SplitTableRegionFuture(final HBaseAdmin admin,
        final TableName tableName,
        final SplitTableRegionResponse response) {
      super(admin, tableName,
          (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    public SplitTableRegionFuture(
        final HBaseAdmin admin,
        final TableName tableName,
        final Long procId) {
      super(admin, tableName, procId);
    }

    @Override
    public String getOperationType() {
      return "SPLIT_REGION";
    }
  }

  @Override
  public void split(final TableName tableName) throws IOException {
    split(tableName, null);
  }

  @Override
  public void splitRegion(final byte[] regionName) throws IOException {
    splitRegion(regionName, null);
  }

  @Override
  public void split(final TableName tableName, final byte[] splitPoint) throws IOException {
    checkTableExists(tableName);
    for (HRegionLocation loc : connection.locateRegions(tableName, false, false)) {
      ServerName sn = loc.getServerName();
      if (sn == null) {
        continue;
      }
      RegionInfo r = loc.getRegion();
      // check for parents
      if (r.isSplitParent()) {
        continue;
      }
      // if a split point given, only split that particular region
      if (r.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID ||
          (splitPoint != null && !r.containsRow(splitPoint))) {
        continue;
      }
      // call out to master to do split now
      splitRegionAsync(r, splitPoint);
    }
  }

  @Override
  public void splitRegion(final byte[] regionName, final byte [] splitPoint) throws IOException {
    Pair<RegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getFirst() != null &&
        regionServerPair.getFirst().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException("Can't split replicas directly. "
          + "Replicas are auto-split when their primary is split.");
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    splitRegionAsync(regionServerPair.getFirst(), splitPoint);
  }

  private static class ModifyTableFuture extends TableFuture<Void> {
    public ModifyTableFuture(final HBaseAdmin admin, final TableName tableName,
        final ModifyTableResponse response) {
      super(admin, tableName,
          (response != null && response.hasProcId()) ? response.getProcId() : null);
    }

    public ModifyTableFuture(final HBaseAdmin admin, final TableName tableName, final Long procId) {
      super(admin, tableName, procId);
    }

    @Override
    public String getOperationType() {
      return "MODIFY";
    }

    @Override
    protected Void postOperationResult(final Void result, final long deadlineTs)
        throws IOException, TimeoutException {
      // The modify operation on the table is asynchronous on the server side irrespective
      // of whether Procedure V2 is supported or not. So, we wait in the client till
      // all regions get updated.
      waitForSchemaUpdate(deadlineTs);
      return result;
    }
  }

  /**
   * @param regionName Name of a region.
   * @return a pair of HRegionInfo and ServerName if <code>regionName</code> is
   *  a verified region name (we call {@link
   *  MetaTableAccessor#getRegionLocation(Connection, byte[])}
   *  else null.
   * Throw IllegalArgumentException if <code>regionName</code> is null.
   * @throws IOException if a remote or network exception occurs
   */
  Pair<RegionInfo, ServerName> getRegion(final byte[] regionName) throws IOException {
    if (regionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    Pair<RegionInfo, ServerName> pair = MetaTableAccessor.getRegion(connection, regionName);
    if (pair == null) {
      final String encodedName = Bytes.toString(regionName);
      // When it is not a valid regionName, it is possible that it could be an encoded regionName.
      // To match the encoded regionName, it has to scan the meta table and compare entry by entry.
      // Since it scans meta table, so it has to be the MD5 hash, it can filter out
      // most of invalid cases.
      if (!RegionInfo.isMD5Hash(encodedName)) {
        return null;
      }
      final AtomicReference<Pair<RegionInfo, ServerName>> result = new AtomicReference<>(null);
      MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
        @Override
        public boolean visit(Result data) throws IOException {
          RegionInfo info = MetaTableAccessor.getRegionInfo(data);
          if (info == null) {
            LOG.warn("No serialized HRegionInfo in " + data);
            return true;
          }
          RegionLocations rl = MetaTableAccessor.getRegionLocations(data);
          boolean matched = false;
          ServerName sn = null;
          if (rl != null) {
            for (HRegionLocation h : rl.getRegionLocations()) {
              if (h != null && encodedName.equals(h.getRegionInfo().getEncodedName())) {
                sn = h.getServerName();
                info = h.getRegionInfo();
                matched = true;
              }
            }
          }
          if (!matched) return true;
          result.set(new Pair<>(info, sn));
          return false; // found the region, stop
        }
      };

      MetaTableAccessor.fullScanRegions(connection, visitor);
      pair = result.get();
    }
    return pair;
  }

  /**
   * If the input is a region name, it is returned as is. If it's an
   * encoded region name, the corresponding region is found from meta
   * and its region name is returned. If we can't find any region in
   * meta matching the input as either region name or encoded region
   * name, the input is returned as is. We don't throw unknown
   * region exception.
   */
  private byte[] getRegionName(
      final byte[] regionNameOrEncodedRegionName) throws IOException {
    if (Bytes.equals(regionNameOrEncodedRegionName,
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
          || Bytes.equals(regionNameOrEncodedRegionName,
            HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      return HRegionInfo.FIRST_META_REGIONINFO.getRegionName();
    }
    byte[] tmp = regionNameOrEncodedRegionName;
    Pair<RegionInfo, ServerName> regionServerPair = getRegion(regionNameOrEncodedRegionName);
    if (regionServerPair != null && regionServerPair.getFirst() != null) {
      tmp = regionServerPair.getFirst().getRegionName();
    }
    return tmp;
  }

  /**
   * Check if table exists or not
   * @param tableName Name of a table.
   * @return tableName instance
   * @throws IOException if a remote or network exception occurs.
   * @throws TableNotFoundException if table does not exist.
   */
  private TableName checkTableExists(final TableName tableName)
      throws IOException {
    return executeCallable(new RpcRetryingCallable<TableName>() {
      @Override
      protected TableName rpcCall(int callTimeout) throws Exception {
        if (MetaTableAccessor.getTableState(getConnection(), tableName) == null) {
          throw new TableNotFoundException(tableName);
        }
        return tableName;
      }
    });
  }

  @Override
  public synchronized void shutdown() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(HConstants.HIGH_QOS);
        master.shutdown(getRpcController(), ShutdownRequest.newBuilder().build());
        return null;
      }
    });
  }

  @Override
  public synchronized void stopMaster() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        setPriority(HConstants.HIGH_QOS);
        master.stopMaster(getRpcController(), StopMasterRequest.newBuilder().build());
        return null;
      }
    });
  }

  @Override
  public synchronized void stopRegionServer(final String hostnamePort)
  throws IOException {
    String hostname = Addressing.parseHostname(hostnamePort);
    int port = Addressing.parsePort(hostnamePort);
    final AdminService.BlockingInterface admin =
      this.connection.getAdmin(ServerName.valueOf(hostname, port, 0));
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    controller.setPriority(HConstants.HIGH_QOS);
    StopServerRequest request = RequestConverter.buildStopServerRequest(
        "Called by admin client " + this.connection.toString());
    try {
      admin.stopServer(controller, request);
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public boolean isMasterInMaintenanceMode() throws IOException {
    return executeCallable(new MasterCallable<IsInMaintenanceModeResponse>(getConnection(),
        this.rpcControllerFactory) {
      @Override
      protected IsInMaintenanceModeResponse rpcCall() throws Exception {
        return master.isMasterInMaintenanceMode(getRpcController(),
            IsInMaintenanceModeRequest.newBuilder().build());
      }
    }).getInMaintenanceMode();
  }

  @Override
  public ClusterMetrics getClusterMetrics(EnumSet<Option> options) throws IOException {
    return executeCallable(new MasterCallable<ClusterMetrics>(getConnection(),
        this.rpcControllerFactory) {
      @Override
      protected ClusterMetrics rpcCall() throws Exception {
        GetClusterStatusRequest req = RequestConverter.buildGetClusterStatusRequest(options);
        return ClusterMetricsBuilder.toClusterMetrics(
          master.getClusterStatus(getRpcController(), req).getClusterStatus());
      }
    });
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName serverName, TableName tableName)
      throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(serverName);
    HBaseRpcController controller = rpcControllerFactory.newController();
    AdminProtos.GetRegionLoadRequest request =
      RequestConverter.buildGetRegionLoadRequest(tableName);
    try {
      return admin.getRegionLoad(controller, request).getRegionLoadsList().stream()
        .map(RegionMetricsBuilder::toRegionMetrics).collect(Collectors.toList());
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Do a get with a timeout against the passed in <code>future</code>.
   */
  private static <T> T get(final Future<T> future, final long timeout, final TimeUnit units)
  throws IOException {
    try {
      // TODO: how long should we wait? Spin forever?
      return future.get(timeout, units);
    } catch (InterruptedException e) {
      IOException ioe = new InterruptedIOException("Interrupt while waiting on " + future);
      ioe.initCause(e);
      throw ioe;
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  @Override
  public Future<Void> createNamespaceAsync(final NamespaceDescriptor descriptor)
      throws IOException {
    CreateNamespaceResponse response =
        executeCallable(new MasterCallable<CreateNamespaceResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected CreateNamespaceResponse rpcCall() throws Exception {
        return master.createNamespace(getRpcController(),
          CreateNamespaceRequest.newBuilder().setNamespaceDescriptor(ProtobufUtil.
              toProtoNamespaceDescriptor(descriptor)).build());
      }
    });
    return new NamespaceFuture(this, descriptor.getName(), response.getProcId()) {
      @Override
      public String getOperationType() {
        return "CREATE_NAMESPACE";
      }
    };
  }

  @Override
  public Future<Void> modifyNamespaceAsync(final NamespaceDescriptor descriptor)
      throws IOException {
    ModifyNamespaceResponse response =
        executeCallable(new MasterCallable<ModifyNamespaceResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected ModifyNamespaceResponse rpcCall() throws Exception {
        // TODO: set priority based on NS?
        return master.modifyNamespace(getRpcController(), ModifyNamespaceRequest.newBuilder().
          setNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(descriptor)).build());
       }
    });
    return new NamespaceFuture(this, descriptor.getName(), response.getProcId()) {
      @Override
      public String getOperationType() {
        return "MODIFY_NAMESPACE";
      }
    };
  }

  @Override
  public Future<Void> deleteNamespaceAsync(final String name)
      throws IOException {
    DeleteNamespaceResponse response =
        executeCallable(new MasterCallable<DeleteNamespaceResponse>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected DeleteNamespaceResponse rpcCall() throws Exception {
        // TODO: set priority based on NS?
        return master.deleteNamespace(getRpcController(), DeleteNamespaceRequest.newBuilder().
          setNamespaceName(name).build());
        }
      });
    return new NamespaceFuture(this, name, response.getProcId()) {
      @Override
      public String getOperationType() {
        return "DELETE_NAMESPACE";
      }
    };
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(final String name)
      throws NamespaceNotFoundException, IOException {
    return executeCallable(new MasterCallable<NamespaceDescriptor>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected NamespaceDescriptor rpcCall() throws Exception {
        return ProtobufUtil.toNamespaceDescriptor(
            master.getNamespaceDescriptor(getRpcController(),
                GetNamespaceDescriptorRequest.newBuilder().
                  setNamespaceName(name).build()).getNamespaceDescriptor());
      }
    });
  }

  /**
   * List available namespaces
   * @return List of namespace names
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public String[] listNamespaces() throws IOException {
    return executeCallable(new MasterCallable<String[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected String[] rpcCall() throws Exception {
        List<String> list = master.listNamespaces(getRpcController(),
          ListNamespacesRequest.newBuilder().build()).getNamespaceNameList();
        return list.toArray(new String[list.size()]);
      }
    });
  }

  /**
   * List available namespace descriptors
   * @return List of descriptors
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return executeCallable(new MasterCallable<NamespaceDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected NamespaceDescriptor[] rpcCall() throws Exception {
        List<HBaseProtos.NamespaceDescriptor> list =
            master.listNamespaceDescriptors(getRpcController(),
              ListNamespaceDescriptorsRequest.newBuilder().build()).getNamespaceDescriptorList();
        NamespaceDescriptor[] res = new NamespaceDescriptor[list.size()];
        for(int i = 0; i < list.size(); i++) {
          res[i] = ProtobufUtil.toNamespaceDescriptor(list.get(i));
        }
        return res;
      }
    });
  }

  @Override
  public String getProcedures() throws IOException {
    return executeCallable(new MasterCallable<String>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected String rpcCall() throws Exception {
        GetProceduresRequest request = GetProceduresRequest.newBuilder().build();
        GetProceduresResponse response = master.getProcedures(getRpcController(), request);
        return ProtobufUtil.toProcedureJson(response.getProcedureList());
      }
    });
  }

  @Override
  public String getLocks() throws IOException {
    return executeCallable(new MasterCallable<String>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected String rpcCall() throws Exception {
        GetLocksRequest request = GetLocksRequest.newBuilder().build();
        GetLocksResponse response = master.getLocks(getRpcController(), request);
        return ProtobufUtil.toLockJson(response.getLockList());
      }
    });
  }

  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(final String name) throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected HTableDescriptor[] rpcCall() throws Exception {
        List<TableSchema> list =
            master.listTableDescriptorsByNamespace(getRpcController(),
                ListTableDescriptorsByNamespaceRequest.newBuilder().setNamespaceName(name)
                .build()).getTableSchemaList();
        HTableDescriptor[] res = new HTableDescriptor[list.size()];
        for(int i=0; i < list.size(); i++) {
          res[i] = new ImmutableHTableDescriptor(ProtobufUtil.toTableDescriptor(list.get(i)));
        }
        return res;
      }
    });
  }

  @Override
  public TableName[] listTableNamesByNamespace(final String name) throws IOException {
    return executeCallable(new MasterCallable<TableName[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected TableName[] rpcCall() throws Exception {
        List<HBaseProtos.TableName> tableNames =
            master.listTableNamesByNamespace(getRpcController(), ListTableNamesByNamespaceRequest.
                newBuilder().setNamespaceName(name).build())
            .getTableNameList();
        TableName[] result = new TableName[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
          result[i] = ProtobufUtil.toTableName(tableNames.get(i));
        }
        return result;
      }
    });
  }

  /**
   * Is HBase available? Throw an exception if not.
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running.
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper. // TODO do not expose
   *           ZKConnectionException.
   */
  public static void available(final Configuration conf)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);
    // We set it to make it fail as soon as possible if HBase is not available
    copyOfConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    copyOfConf.setInt("zookeeper.recovery.retry", 0);

    // Check ZK first.
    // If the connection exists, we may have a connection to ZK that does not work anymore
    try (ClusterConnection connection =
        (ClusterConnection) ConnectionFactory.createConnection(copyOfConf)) {
      // can throw MasterNotRunningException
      connection.isMasterRunning();
    }
  }

  /**
   *
   * @param tableName
   * @return List of {@link HRegionInfo}.
   * @throws IOException if a remote or network exception occurs
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegions(TableName)}.
   */
  @Deprecated
  @Override
  public List<HRegionInfo> getTableRegions(final TableName tableName)
    throws IOException {
    return getRegions(tableName).stream()
        .map(ImmutableHRegionInfo::new)
        .collect(Collectors.toList());
  }

  @Override
  public synchronized void close() throws IOException {
  }

  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(final List<TableName> tableNames)
  throws IOException {
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected HTableDescriptor[] rpcCall() throws Exception {
        GetTableDescriptorsRequest req =
            RequestConverter.buildGetTableDescriptorsRequest(tableNames);
        return ProtobufUtil
            .toTableDescriptorList(master.getTableDescriptors(getRpcController(), req)).stream()
            .map(ImmutableHTableDescriptor::new).toArray(HTableDescriptor[]::new);
      }
    });
  }

  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> names)
  throws IOException {
    List<TableName> tableNames = new ArrayList<>(names.size());
    for(String name : names) {
      tableNames.add(TableName.valueOf(name));
    }
    return getTableDescriptorsByTableName(tableNames);
  }

  private RollWALWriterResponse rollWALWriterImpl(final ServerName sn) throws IOException,
      FailedLogCloseException {
    final AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    RollWALWriterRequest request = RequestConverter.buildRollWALWriterRequest();
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    try {
      return admin.rollWALWriter(controller, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public synchronized void rollWALWriter(ServerName serverName)
      throws IOException, FailedLogCloseException {
    rollWALWriterImpl(serverName);
  }

  @Override
  public CompactionState getCompactionState(final TableName tableName)
  throws IOException {
    return getCompactionState(tableName, CompactType.NORMAL);
  }

  @Override
  public CompactionState getCompactionStateForRegion(final byte[] regionName)
  throws IOException {
    final Pair<RegionInfo, ServerName> regionServerPair = getRegion(regionName);
    if (regionServerPair == null) {
      throw new IllegalArgumentException("Invalid region: " + Bytes.toStringBinary(regionName));
    }
    if (regionServerPair.getSecond() == null) {
      throw new NoServerForRegionException(Bytes.toStringBinary(regionName));
    }
    ServerName sn = regionServerPair.getSecond();
    final AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller = rpcControllerFactory.newController();
    GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(
      regionServerPair.getFirst().getRegionName(), true);
    GetRegionInfoResponse response;
    try {
      response = admin.getRegionInfo(controller, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
    if (response.getCompactionState() != null) {
      return ProtobufUtil.createCompactionState(response.getCompactionState());
    }
    return null;
  }

  @Override
  public void snapshot(SnapshotDescription snapshotDesc)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    // actually take the snapshot
    SnapshotProtos.SnapshotDescription snapshot =
      ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc);
    SnapshotResponse response = asyncSnapshot(snapshot);
    final IsSnapshotDoneRequest request =
        IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot).build();
    IsSnapshotDoneResponse done = null;
    long start = EnvironmentEdgeManager.currentTime();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for snapshot '" +
        ClientSnapshotDescriptionUtils.toString(snapshot) + "'' to complete. (max " +
        maxPauseTime + " ms per retry)");
    while (tries == 0
        || ((EnvironmentEdgeManager.currentTime() - start) < max && !done.getDone())) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug("(#" + tries + ") Sleeping: " + sleep +
          "ms while waiting for snapshot completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of snapshot from master...");
      done = executeCallable(new MasterCallable<IsSnapshotDoneResponse>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected IsSnapshotDoneResponse rpcCall() throws Exception {
          return master.isSnapshotDone(getRpcController(), request);
        }
      });
    }
    if (!done.getDone()) {
      throw new SnapshotCreationException("Snapshot '" + snapshot.getName()
          + "' wasn't completed in expectedTime:" + max + " ms", snapshotDesc);
    }
  }

  @Override
  public Future<Void> snapshotAsync(SnapshotDescription snapshotDesc)
      throws IOException, SnapshotCreationException {
    asyncSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc));
    return new ProcedureFuture<Void>(this, null) {

      @Override
      protected Void waitOperationResult(long deadlineTs) throws IOException, TimeoutException {
        waitForState(deadlineTs, new WaitForStateCallable() {

          @Override
          public void throwInterruptedException() throws InterruptedIOException {
            throw new InterruptedIOException(
              "Interrupted while waiting for taking snapshot" + snapshotDesc);
          }

          @Override
          public void throwTimeoutException(long elapsedTime) throws TimeoutException {
            throw new TimeoutException("Snapshot '" + snapshotDesc.getName() +
              "' wasn't completed in expectedTime:" + elapsedTime + " ms");
          }

          @Override
          public boolean checkState(int tries) throws IOException {
            return isSnapshotFinished(snapshotDesc);
          }
        });
        return null;
      }
    };
  }

  private SnapshotResponse asyncSnapshot(SnapshotProtos.SnapshotDescription snapshot)
      throws IOException {
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    final SnapshotRequest request = SnapshotRequest.newBuilder().setSnapshot(snapshot)
        .build();
    // run the snapshot on the master
    return executeCallable(new MasterCallable<SnapshotResponse>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected SnapshotResponse rpcCall() throws Exception {
        return master.snapshot(getRpcController(), request);
      }
    });
  }

  @Override
  public boolean isSnapshotFinished(final SnapshotDescription snapshotDesc)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    final SnapshotProtos.SnapshotDescription snapshot =
        ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotDesc);
    return executeCallable(new MasterCallable<IsSnapshotDoneResponse>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected IsSnapshotDoneResponse rpcCall() throws Exception {
        return master.isSnapshotDone(getRpcController(),
          IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot).build());
      }
    }).getDone();
  }

  @Override
  public void restoreSnapshot(final byte[] snapshotName)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName));
  }

  @Override
  public void restoreSnapshot(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    boolean takeFailSafeSnapshot =
        conf.getBoolean(HConstants.SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT,
          HConstants.DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT);
    restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  @Override
  public void restoreSnapshot(final byte[] snapshotName, final boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName), takeFailSafeSnapshot);
  }

  /**
   * Check whether the snapshot exists and contains disabled table
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if no valid snapshot is found
   */
  private TableName getTableNameBeforeRestoreSnapshot(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    TableName tableName = null;
    for (SnapshotDescription snapshotInfo: listSnapshots()) {
      if (snapshotInfo.getName().equals(snapshotName)) {
        tableName = snapshotInfo.getTableName();
        break;
      }
    }

    if (tableName == null) {
      throw new RestoreSnapshotException(
        "Unable to find the table name for snapshot=" + snapshotName);
    }
    return tableName;
  }

  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(snapshotName, takeFailSafeSnapshot, false);
  }

  @Override
  public void restoreSnapshot(final String snapshotName, final boolean takeFailSafeSnapshot,
      final boolean restoreAcl) throws IOException, RestoreSnapshotException {
    TableName tableName = getTableNameBeforeRestoreSnapshot(snapshotName);

    // The table does not exists, switch to clone.
    if (!tableExists(tableName)) {
      cloneSnapshot(snapshotName, tableName, restoreAcl);
      return;
    }

    // Check if the table is disabled
    if (!isTableDisabled(tableName)) {
      throw new TableNotDisabledException(tableName);
    }

    // Take a snapshot of the current state
    String failSafeSnapshotSnapshotName = null;
    if (takeFailSafeSnapshot) {
      failSafeSnapshotSnapshotName = conf.get("hbase.snapshot.restore.failsafe.name",
        "hbase-failsafe-{snapshot.name}-{restore.timestamp}");
      failSafeSnapshotSnapshotName = failSafeSnapshotSnapshotName
        .replace("{snapshot.name}", snapshotName)
        .replace("{table.name}", tableName.toString().replace(TableName.NAMESPACE_DELIM, '.'))
        .replace("{restore.timestamp}", String.valueOf(EnvironmentEdgeManager.currentTime()));
      LOG.info("Taking restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
      snapshot(failSafeSnapshotSnapshotName, tableName);
    }

    try {
      // Restore snapshot
      get(
        internalRestoreSnapshotAsync(snapshotName, tableName, restoreAcl),
        syncWaitTimeout,
        TimeUnit.MILLISECONDS);
    } catch (IOException e) {
      // Something went wrong during the restore...
      // if the pre-restore snapshot is available try to rollback
      if (takeFailSafeSnapshot) {
        try {
          get(
            internalRestoreSnapshotAsync(failSafeSnapshotSnapshotName, tableName, restoreAcl),
            syncWaitTimeout,
            TimeUnit.MILLISECONDS);
          String msg = "Restore snapshot=" + snapshotName +
            " failed. Rollback to snapshot=" + failSafeSnapshotSnapshotName + " succeeded.";
          LOG.error(msg, e);
          throw new RestoreSnapshotException(msg, e);
        } catch (IOException ex) {
          String msg = "Failed to restore and rollback to snapshot=" + failSafeSnapshotSnapshotName;
          LOG.error(msg, ex);
          throw new RestoreSnapshotException(msg, e);
        }
      } else {
        throw new RestoreSnapshotException("Failed to restore snapshot=" + snapshotName, e);
      }
    }

    // If the restore is succeeded, delete the pre-restore snapshot
    if (takeFailSafeSnapshot) {
      try {
        LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
        deleteSnapshot(failSafeSnapshotSnapshotName);
      } catch (IOException e) {
        LOG.error("Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName, e);
      }
    }
  }

  @Override
  public Future<Void> restoreSnapshotAsync(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    TableName tableName = getTableNameBeforeRestoreSnapshot(snapshotName);

    // The table does not exists, switch to clone.
    if (!tableExists(tableName)) {
      return cloneSnapshotAsync(snapshotName, tableName);
    }

    // Check if the table is disabled
    if (!isTableDisabled(tableName)) {
      throw new TableNotDisabledException(tableName);
    }

    return internalRestoreSnapshotAsync(snapshotName, tableName, false);
  }

  @Override
  public Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName,
      boolean restoreAcl) throws IOException, TableExistsException, RestoreSnapshotException {
    if (tableExists(tableName)) {
      throw new TableExistsException(tableName);
    }
    return internalRestoreSnapshotAsync(snapshotName, tableName, restoreAcl);
  }

  @Override
  public byte[] execProcedureWithReturn(String signature, String instance, Map<String,
      String> props) throws IOException {
    ProcedureDescription desc = ProtobufUtil.buildProcedureDescription(signature, instance, props);
    final ExecProcedureRequest request =
        ExecProcedureRequest.newBuilder().setProcedure(desc).build();
    // run the procedure on the master
    ExecProcedureResponse response = executeCallable(
      new MasterCallable<ExecProcedureResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected ExecProcedureResponse rpcCall() throws Exception {
          return master.execProcedureWithRet(getRpcController(), request);
        }
      });

    return response.hasReturnData() ? response.getReturnData().toByteArray() : null;
  }

  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {
    ProcedureDescription desc = ProtobufUtil.buildProcedureDescription(signature, instance, props);
    final ExecProcedureRequest request =
        ExecProcedureRequest.newBuilder().setProcedure(desc).build();
    // run the procedure on the master
    ExecProcedureResponse response = executeCallable(new MasterCallable<ExecProcedureResponse>(
        getConnection(), getRpcControllerFactory()) {
      @Override
      protected ExecProcedureResponse rpcCall() throws Exception {
        return master.execProcedure(getRpcController(), request);
      }
    });

    long start = EnvironmentEdgeManager.currentTime();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for procedure '" +
        signature + " : " + instance + "'' to complete. (max " + maxPauseTime + " ms per retry)");
    boolean done = false;
    while (tries == 0
        || ((EnvironmentEdgeManager.currentTime() - start) < max && !done)) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug("(#" + tries + ") Sleeping: " + sleep +
          "ms while waiting for procedure completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of procedure from master...");
      done = isProcedureFinished(signature, instance, props);
    }
    if (!done) {
      throw new IOException("Procedure '" + signature + " : " + instance
          + "' wasn't completed in expectedTime:" + max + " ms");
    }
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    ProcedureDescription desc = ProtobufUtil.buildProcedureDescription(signature, instance, props);
    return executeCallable(
      new MasterCallable<IsProcedureDoneResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected IsProcedureDoneResponse rpcCall() throws Exception {
          return master.isProcedureDone(getRpcController(),
            IsProcedureDoneRequest.newBuilder().setProcedure(desc).build());
        }
      }).getDone();
  }

  /**
   * Execute Restore/Clone snapshot and wait for the server to complete (blocking).
   * To check if the cloned table exists, use {@link #isTableAvailable} -- it is not safe to
   * create an HTable instance to this table before it is available.
   * @param snapshotName snapshot to restore
   * @param tableName table name to restore the snapshot on
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  private Future<Void> internalRestoreSnapshotAsync(final String snapshotName,
      final TableName tableName, final boolean restoreAcl)
      throws IOException, RestoreSnapshotException {
    final SnapshotProtos.SnapshotDescription snapshot =
        SnapshotProtos.SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(tableName.getNameAsString()).build();

    // actually restore the snapshot
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);

    RestoreSnapshotResponse response = executeCallable(
        new MasterCallable<RestoreSnapshotResponse>(getConnection(), getRpcControllerFactory()) {
          Long nonceGroup = ng.getNonceGroup();
          Long nonce = ng.newNonce();
      @Override
      protected RestoreSnapshotResponse rpcCall() throws Exception {
        final RestoreSnapshotRequest request = RestoreSnapshotRequest.newBuilder()
            .setSnapshot(snapshot)
            .setNonceGroup(nonceGroup)
            .setNonce(nonce)
            .setRestoreACL(restoreAcl)
            .build();
        return master.restoreSnapshot(getRpcController(), request);
      }
    });

    return new RestoreSnapshotFuture(this, snapshot, tableName, response);
  }

  private static class RestoreSnapshotFuture extends TableFuture<Void> {
    public RestoreSnapshotFuture(
        final HBaseAdmin admin,
        final SnapshotProtos.SnapshotDescription snapshot,
        final TableName tableName,
        final RestoreSnapshotResponse response) {
      super(admin, tableName,
          (response != null && response.hasProcId()) ? response.getProcId() : null);

      if (response != null && !response.hasProcId()) {
        throw new UnsupportedOperationException("Client could not call old version of Server");
      }
    }

    public RestoreSnapshotFuture(
        final HBaseAdmin admin,
        final TableName tableName,
        final Long procId) {
      super(admin, tableName, procId);
    }

    @Override
    public String getOperationType() {
      return "MODIFY";
    }
  }

  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    return executeCallable(new MasterCallable<List<SnapshotDescription>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<SnapshotDescription> rpcCall() throws Exception {
        List<SnapshotProtos.SnapshotDescription> snapshotsList = master
            .getCompletedSnapshots(getRpcController(),
                GetCompletedSnapshotsRequest.newBuilder().build())
            .getSnapshotsList();
        List<SnapshotDescription> result = new ArrayList<>(snapshotsList.size());
        for (SnapshotProtos.SnapshotDescription snapshot : snapshotsList) {
          result.add(ProtobufUtil.createSnapshotDesc(snapshot));
        }
        return result;
      }
    });
  }

  @Override
  public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    List<SnapshotDescription> matched = new LinkedList<>();
    List<SnapshotDescription> snapshots = listSnapshots();
    for (SnapshotDescription snapshot : snapshots) {
      if (pattern.matcher(snapshot.getName()).matches()) {
        matched.add(snapshot);
      }
    }
    return matched;
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(String tableNameRegex,
      String snapshotNameRegex) throws IOException {
    return listTableSnapshots(Pattern.compile(tableNameRegex), Pattern.compile(snapshotNameRegex));
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException {
    TableName[] tableNames = listTableNames(tableNamePattern);

    List<SnapshotDescription> tableSnapshots = new LinkedList<>();
    List<SnapshotDescription> snapshots = listSnapshots(snapshotNamePattern);

    List<TableName> listOfTableNames = Arrays.asList(tableNames);
    for (SnapshotDescription snapshot : snapshots) {
      if (listOfTableNames.contains(snapshot.getTableName())) {
        tableSnapshots.add(snapshot);
      }
    }
    return tableSnapshots;
  }

  @Override
  public void deleteSnapshot(final byte[] snapshotName) throws IOException {
    deleteSnapshot(Bytes.toString(snapshotName));
  }

  @Override
  public void deleteSnapshot(final String snapshotName) throws IOException {
    // make sure the snapshot is possibly valid
    TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(snapshotName));
    // do the delete
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        master.deleteSnapshot(getRpcController(),
          DeleteSnapshotRequest.newBuilder().setSnapshot(
                SnapshotProtos.SnapshotDescription.newBuilder().setName(snapshotName).build())
              .build()
        );
        return null;
      }
    });
  }

  @Override
  public void deleteSnapshots(final String regex) throws IOException {
    deleteSnapshots(Pattern.compile(regex));
  }

  @Override
  public void deleteSnapshots(final Pattern pattern) throws IOException {
    List<SnapshotDescription> snapshots = listSnapshots(pattern);
    for (final SnapshotDescription snapshot : snapshots) {
      try {
        internalDeleteSnapshot(snapshot);
      } catch (IOException ex) {
        LOG.info("Failed to delete snapshot " + snapshot.getName() + " for table "
                + snapshot.getTableNameAsString(), ex);
      }
    }
  }

  private void internalDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        this.master.deleteSnapshot(getRpcController(), DeleteSnapshotRequest.newBuilder()
          .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build());
        return null;
      }
    });
  }

  @Override
  public void deleteTableSnapshots(String tableNameRegex, String snapshotNameRegex)
      throws IOException {
    deleteTableSnapshots(Pattern.compile(tableNameRegex), Pattern.compile(snapshotNameRegex));
  }

  @Override
  public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
      throws IOException {
    List<SnapshotDescription> snapshots = listTableSnapshots(tableNamePattern, snapshotNamePattern);
    for (SnapshotDescription snapshot : snapshots) {
      try {
        internalDeleteSnapshot(snapshot);
        LOG.debug("Successfully deleted snapshot: " + snapshot.getName());
      } catch (IOException e) {
        LOG.error("Failed to delete snapshot: " + snapshot.getName(), e);
      }
    }
  }

  @Override
  public void setQuota(final QuotaSettings quota) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        this.master.setQuota(getRpcController(), QuotaSettings.buildSetQuotaRequestProto(quota));
        return null;
      }
    });
  }

  @Override
  public QuotaRetriever getQuotaRetriever(final QuotaFilter filter) throws IOException {
    return QuotaRetriever.open(conf, filter);
  }

  @Override
  public List<QuotaSettings> getQuota(QuotaFilter filter) throws IOException {
    List<QuotaSettings> quotas = new LinkedList<>();
    try (QuotaRetriever retriever = QuotaRetriever.open(conf, filter)) {
      Iterator<QuotaSettings> iterator = retriever.iterator();
      while (iterator.hasNext()) {
        quotas.add(iterator.next());
      }
    }
    return quotas;
  }

  private <C extends RetryingCallable<V> & Closeable, V> V executeCallable(C callable)
      throws IOException {
    return executeCallable(callable, rpcCallerFactory, operationTimeout, rpcTimeout);
  }

  static private <C extends RetryingCallable<V> & Closeable, V> V executeCallable(C callable,
             RpcRetryingCallerFactory rpcCallerFactory, int operationTimeout, int rpcTimeout)
  throws IOException {
    RpcRetryingCaller<V> caller = rpcCallerFactory.newCaller(rpcTimeout);
    try {
      return caller.callWithRetries(callable, operationTimeout);
    } finally {
      callable.close();
    }
  }

  @Override
  // Coprocessor Endpoint against the Master.
  public CoprocessorRpcChannel coprocessorService() {
    return new SyncCoprocessorRpcChannel() {
      @Override
      protected Message callExecService(final RpcController controller,
          final Descriptors.MethodDescriptor method, final Message request,
          final Message responsePrototype)
      throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Call: " + method.getName() + ", " + request.toString());
        }
        // Try-with-resources so close gets called when we are done.
        try (MasterCallable<CoprocessorServiceResponse> callable =
            new MasterCallable<CoprocessorServiceResponse>(connection,
                connection.getRpcControllerFactory()) {
          @Override
          protected CoprocessorServiceResponse rpcCall() throws Exception {
            CoprocessorServiceRequest csr =
                CoprocessorRpcUtils.getCoprocessorServiceRequest(method, request);
            return this.master.execMasterService(getRpcController(), csr);
          }
        }) {
          // TODO: Are we retrying here? Does not seem so. We should use RetryingRpcCaller
          callable.prepare(false);
          int operationTimeout = connection.getConnectionConfiguration().getOperationTimeout();
          CoprocessorServiceResponse result = callable.call(operationTimeout);
          return CoprocessorRpcUtils.getResponse(result, responsePrototype);
        }
      }
    };
  }

  /**
   * Simple {@link Abortable}, throwing RuntimeException on abort.
   */
  private static class ThrowableAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
      throw new RuntimeException(why, e);
    }

    @Override
    public boolean isAborted() {
      return true;
    }
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(final ServerName serverName) {
    return new SyncCoprocessorRpcChannel() {
      @Override
      protected Message callExecService(RpcController controller,
          Descriptors.MethodDescriptor method, Message request, Message responsePrototype)
      throws IOException {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Call: " + method.getName() + ", " + request.toString());
        }
        CoprocessorServiceRequest csr =
            CoprocessorRpcUtils.getCoprocessorServiceRequest(method, request);
        // TODO: Are we retrying here? Does not seem so. We should use RetryingRpcCaller
        // TODO: Make this same as RegionCoprocessorRpcChannel and MasterCoprocessorRpcChannel. They
        // are all different though should do same thing; e.g. RpcChannel setup.
        ClientProtos.ClientService.BlockingInterface stub = connection.getClient(serverName);
        CoprocessorServiceResponse result;
        try {
          result = stub.
              execRegionServerService(connection.getRpcControllerFactory().newController(), csr);
          return CoprocessorRpcUtils.getResponse(result, responsePrototype);
        } catch (ServiceException e) {
          throw ProtobufUtil.handleRemoteException(e);
        }
      }
    };
  }

  @Override
  public void updateConfiguration(final ServerName server) throws IOException {
    final AdminService.BlockingInterface admin = this.connection.getAdmin(server);
    Callable<Void> callable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.updateConfiguration(null, UpdateConfigurationRequest.getDefaultInstance());
        return null;
      }
    };
    ProtobufUtil.call(callable);
  }

  @Override
  public void updateConfiguration() throws IOException {
    ClusterMetrics status = getClusterMetrics(
      EnumSet.of(Option.LIVE_SERVERS, Option.MASTER, Option.BACKUP_MASTERS));
    for (ServerName server : status.getLiveServerMetrics().keySet()) {
      updateConfiguration(server);
    }

    updateConfiguration(status.getMasterName());

    for (ServerName server : status.getBackupMasterNames()) {
      updateConfiguration(server);
    }
  }

  @Override
  public long getLastMajorCompactionTimestamp(final TableName tableName) throws IOException {
    return executeCallable(new MasterCallable<Long>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Long rpcCall() throws Exception {
        MajorCompactionTimestampRequest req =
            MajorCompactionTimestampRequest.newBuilder()
                .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
        return master.getLastMajorCompactionTimestamp(getRpcController(), req).
            getCompactionTimestamp();
      }
    });
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(final byte[] regionName) throws IOException {
    return executeCallable(new MasterCallable<Long>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Long rpcCall() throws Exception {
        MajorCompactionTimestampForRegionRequest req =
            MajorCompactionTimestampForRegionRequest.newBuilder().setRegion(RequestConverter
                      .buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName)).build();
        return master.getLastMajorCompactionTimestampForRegion(getRpcController(), req)
            .getCompactionTimestamp();
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName, final byte[] columnFamily, CompactType compactType)
    throws IOException, InterruptedException {
    compact(tableName, columnFamily, false, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(final TableName tableName, CompactType compactType)
    throws IOException, InterruptedException {
    compact(tableName, null, false, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName, final byte[] columnFamily,
    CompactType compactType) throws IOException, InterruptedException {
    compact(tableName, columnFamily, true, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void majorCompact(final TableName tableName, CompactType compactType)
          throws IOException, InterruptedException {
    compact(tableName, null, true, compactType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompactionState getCompactionState(final TableName tableName, CompactType compactType)
      throws IOException {
    checkTableExists(tableName);
    if (!isTableEnabled(tableName)) {
      // If the table is disabled, the compaction state of the table should always be NONE
      return ProtobufUtil.createCompactionState(
        AdminProtos.GetRegionInfoResponse.CompactionState.NONE);
    }

    AdminProtos.GetRegionInfoResponse.CompactionState state =
      AdminProtos.GetRegionInfoResponse.CompactionState.NONE;

    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController rpcController = rpcControllerFactory.newController();
    switch (compactType) {
      case MOB:
        final AdminProtos.AdminService.BlockingInterface masterAdmin =
          this.connection.getAdminForMaster();
        Callable<AdminProtos.GetRegionInfoResponse.CompactionState> callable =
          new Callable<AdminProtos.GetRegionInfoResponse.CompactionState>() {
            @Override
            public AdminProtos.GetRegionInfoResponse.CompactionState call() throws Exception {
              RegionInfo info = RegionInfo.createMobRegionInfo(tableName);
              GetRegionInfoRequest request =
                RequestConverter.buildGetRegionInfoRequest(info.getRegionName(), true);
              GetRegionInfoResponse response = masterAdmin.getRegionInfo(rpcController, request);
              return response.getCompactionState();
            }
          };
        state = ProtobufUtil.call(callable);
        break;
      case NORMAL:
        for (HRegionLocation loc : connection.locateRegions(tableName, false, false)) {
          ServerName sn = loc.getServerName();
          if (sn == null) {
            continue;
          }
          byte[] regionName = loc.getRegion().getRegionName();
          AdminService.BlockingInterface snAdmin = this.connection.getAdmin(sn);
          try {
            Callable<GetRegionInfoResponse> regionInfoCallable =
              new Callable<GetRegionInfoResponse>() {
                @Override
                public GetRegionInfoResponse call() throws Exception {
                  GetRegionInfoRequest request =
                    RequestConverter.buildGetRegionInfoRequest(regionName, true);
                  return snAdmin.getRegionInfo(rpcController, request);
                }
              };
            GetRegionInfoResponse response = ProtobufUtil.call(regionInfoCallable);
            switch (response.getCompactionState()) {
              case MAJOR_AND_MINOR:
                return CompactionState.MAJOR_AND_MINOR;
              case MAJOR:
                if (state == AdminProtos.GetRegionInfoResponse.CompactionState.MINOR) {
                  return CompactionState.MAJOR_AND_MINOR;
                }
                state = AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR;
                break;
              case MINOR:
                if (state == AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR) {
                  return CompactionState.MAJOR_AND_MINOR;
                }
                state = AdminProtos.GetRegionInfoResponse.CompactionState.MINOR;
                break;
              case NONE:
              default: // nothing, continue
            }
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to get compaction state of " + loc.getRegion() + ": " +
                StringUtils.stringifyException(e));
            }
          } catch (RemoteException e) {
            if (e.getMessage().indexOf(NotServingRegionException.class.getName()) >= 0) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to get compaction state of " + loc.getRegion() + ": " +
                  StringUtils.stringifyException(e));
              }
            } else {
              throw e;
            }
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown compactType: " + compactType);
    }
    if (state != null) {
      return ProtobufUtil.createCompactionState(state);
    }
    return null;
  }

  /**
   * Future that waits on a procedure result.
   * Returned by the async version of the Admin calls,
   * and used internally by the sync calls to wait on the result of the procedure.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static class ProcedureFuture<V> implements Future<V> {
    private ExecutionException exception = null;
    private boolean procResultFound = false;
    private boolean done = false;
    private boolean cancelled = false;
    private V result = null;

    private final HBaseAdmin admin;
    protected final Long procId;

    public ProcedureFuture(final HBaseAdmin admin, final Long procId) {
      this.admin = admin;
      this.procId = procId;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      AbortProcedureRequest abortProcRequest = AbortProcedureRequest.newBuilder()
          .setProcId(procId).setMayInterruptIfRunning(mayInterruptIfRunning).build();
      try {
        cancelled = abortProcedureResult(abortProcRequest).getIsProcedureAborted();
        if (cancelled) {
          done = true;
        }
      } catch (IOException e) {
        // Cancell thrown exception for some reason. At this time, we are not sure whether
        // the cancell succeeds or fails. We assume that it is failed, but print out a warning
        // for debugging purpose.
        LOG.warn(
          "Cancelling the procedure with procId=" + procId + " throws exception " + e.getMessage(),
          e);
        cancelled = false;
      }
      return cancelled;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    protected AbortProcedureResponse abortProcedureResult(
        final AbortProcedureRequest request) throws IOException {
      return admin.executeCallable(new MasterCallable<AbortProcedureResponse>(
          admin.getConnection(), admin.getRpcControllerFactory()) {
        @Override
        protected AbortProcedureResponse rpcCall() throws Exception {
          return master.abortProcedure(getRpcController(), request);
        }
      });
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      // TODO: should we ever spin forever?
      // fix HBASE-21715. TODO: If the function call get() without timeout limit is not allowed,
      // is it possible to compose instead of inheriting from the class Future for this class?
      try {
        return get(admin.getProcedureTimeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        LOG.warn("Failed to get the procedure with procId=" + procId + " throws exception " + e
            .getMessage(), e);
        return null;
      }
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!done) {
        long deadlineTs = EnvironmentEdgeManager.currentTime() + unit.toMillis(timeout);
        try {
          try {
            // if the master support procedures, try to wait the result
            if (procId != null) {
              result = waitProcedureResult(procId, deadlineTs);
            }
            // if we don't have a proc result, try the compatibility wait
            if (!procResultFound) {
              result = waitOperationResult(deadlineTs);
            }
            result = postOperationResult(result, deadlineTs);
            done = true;
          } catch (IOException e) {
            result = postOperationFailure(e, deadlineTs);
            done = true;
          }
        } catch (IOException e) {
          exception = new ExecutionException(e);
          done = true;
        }
      }
      if (exception != null) {
        throw exception;
      }
      return result;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    protected HBaseAdmin getAdmin() {
      return admin;
    }

    private V waitProcedureResult(long procId, long deadlineTs)
        throws IOException, TimeoutException, InterruptedException {
      GetProcedureResultRequest request = GetProcedureResultRequest.newBuilder()
          .setProcId(procId)
          .build();

      int tries = 0;
      IOException serviceEx = null;
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        GetProcedureResultResponse response = null;
        try {
          // Try to fetch the result
          response = getProcedureResult(request);
        } catch (IOException e) {
          serviceEx = unwrapException(e);

          // the master may be down
          LOG.warn("failed to get the procedure result procId=" + procId, serviceEx);

          // Not much to do, if we have a DoNotRetryIOException
          if (serviceEx instanceof DoNotRetryIOException) {
            // TODO: looks like there is no way to unwrap this exception and get the proper
            // UnsupportedOperationException aside from looking at the message.
            // anyway, if we fail here we just failover to the compatibility side
            // and that is always a valid solution.
            LOG.warn("Proc-v2 is unsupported on this master: " + serviceEx.getMessage(), serviceEx);
            procResultFound = false;
            return null;
          }
        }

        // If the procedure is no longer running, we should have a result
        if (response != null && response.getState() != GetProcedureResultResponse.State.RUNNING) {
          procResultFound = response.getState() != GetProcedureResultResponse.State.NOT_FOUND;
          return convertResult(response);
        }

        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          throw new InterruptedException(
            "Interrupted while waiting for the result of proc " + procId);
        }
      }
      if (serviceEx != null) {
        throw serviceEx;
      } else {
        throw new TimeoutException("The procedure " + procId + " is still running");
      }
    }

    private static IOException unwrapException(IOException e) {
      if (e instanceof RemoteException) {
        return ((RemoteException)e).unwrapRemoteException();
      }
      return e;
    }

    protected GetProcedureResultResponse getProcedureResult(final GetProcedureResultRequest request)
        throws IOException {
      return admin.executeCallable(new MasterCallable<GetProcedureResultResponse>(
          admin.getConnection(), admin.getRpcControllerFactory()) {
        @Override
        protected GetProcedureResultResponse rpcCall() throws Exception {
          return master.getProcedureResult(getRpcController(), request);
        }
      });
    }

    /**
     * Convert the procedure result response to a specified type.
     * @param response the procedure result object to parse
     * @return the result data of the procedure.
     */
    protected V convertResult(final GetProcedureResultResponse response) throws IOException {
      if (response.hasException()) {
        throw ForeignExceptionUtil.toIOException(response.getException());
      }
      return null;
    }

    /**
     * Fallback implementation in case the procedure is not supported by the server.
     * It should try to wait until the operation is completed.
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result data of the operation
     */
    protected V waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      return null;
    }

    /**
     * Called after the operation is completed and the result fetched. this allows to perform extra
     * steps after the procedure is completed. it allows to apply transformations to the result that
     * will be returned by get().
     * @param result the result of the procedure
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result of the procedure, which may be the same as the passed one
     */
    protected V postOperationResult(final V result, final long deadlineTs)
        throws IOException, TimeoutException {
      return result;
    }

    /**
     * Called after the operation is terminated with a failure.
     * this allows to perform extra steps after the procedure is terminated.
     * it allows to apply transformations to the result that will be returned by get().
     * The default implementation will rethrow the exception
     * @param exception the exception got from fetching the result
     * @param deadlineTs the timestamp after which this method should throw a TimeoutException
     * @return the result of the procedure, which may be the same as the passed one
     */
    protected V postOperationFailure(final IOException exception, final long deadlineTs)
        throws IOException, TimeoutException {
      throw exception;
    }

    protected interface WaitForStateCallable {
      boolean checkState(int tries) throws IOException;
      void throwInterruptedException() throws InterruptedIOException;
      void throwTimeoutException(long elapsed) throws TimeoutException;
    }

    protected void waitForState(final long deadlineTs, final WaitForStateCallable callable)
        throws IOException, TimeoutException {
      int tries = 0;
      IOException serverEx = null;
      long startTime = EnvironmentEdgeManager.currentTime();
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        serverEx = null;
        try {
          if (callable.checkState(tries)) {
            return;
          }
        } catch (IOException e) {
          serverEx = e;
        }
        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          callable.throwInterruptedException();
        }
      }
      if (serverEx != null) {
        throw unwrapException(serverEx);
      } else {
        callable.throwTimeoutException(EnvironmentEdgeManager.currentTime() - startTime);
      }
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static abstract class TableFuture<V> extends ProcedureFuture<V> {
    private final TableName tableName;

    public TableFuture(final HBaseAdmin admin, final TableName tableName, final Long procId) {
      super(admin, procId);
      this.tableName = tableName;
    }

    @Override
    public String toString() {
      return getDescription();
    }

    /**
     * @return the table name
     */
    protected TableName getTableName() {
      return tableName;
    }

    /**
     * @return the table descriptor
     */
    protected TableDescriptor getTableDescriptor() throws IOException {
      return getAdmin().getDescriptor(getTableName());
    }

    /**
     * @return the operation type like CREATE, DELETE, DISABLE etc.
     */
    public abstract String getOperationType();

    /**
     * @return a description of the operation
     */
    protected String getDescription() {
      return "Operation: " + getOperationType() + ", " + "Table Name: " +
        tableName.getNameWithNamespaceInclAsString() + ", procId: " + procId;
    }

    protected abstract class TableWaitForStateCallable implements WaitForStateCallable {
      @Override
      public void throwInterruptedException() throws InterruptedIOException {
        throw new InterruptedIOException("Interrupted while waiting for " + getDescription());
      }

      @Override
      public void throwTimeoutException(long elapsedTime) throws TimeoutException {
        throw new TimeoutException(
          getDescription() + " has not completed after " + elapsedTime + "ms");
      }
    }

    @Override
    protected V postOperationResult(final V result, final long deadlineTs)
        throws IOException, TimeoutException {
      LOG.info(getDescription() + " completed");
      return super.postOperationResult(result, deadlineTs);
    }

    @Override
    protected V postOperationFailure(final IOException exception, final long deadlineTs)
        throws IOException, TimeoutException {
      LOG.info(getDescription() + " failed with " + exception.getMessage());
      return super.postOperationFailure(exception, deadlineTs);
    }

    protected void waitForTableEnabled(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          try {
            if (getAdmin().isTableAvailable(tableName)) {
              return true;
            }
          } catch (TableNotFoundException tnfe) {
            LOG.debug("Table " + tableName.getNameWithNamespaceInclAsString()
                + " was not enabled, sleeping. tries=" + tries);
          }
          return false;
        }
      });
    }

    protected void waitForTableDisabled(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return getAdmin().isTableDisabled(tableName);
        }
      });
    }

    protected void waitTableNotFound(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return !getAdmin().tableExists(tableName);
        }
      });
    }

    protected void waitForSchemaUpdate(final long deadlineTs)
        throws IOException, TimeoutException {
      waitForState(deadlineTs, new TableWaitForStateCallable() {
        @Override
        public boolean checkState(int tries) throws IOException {
          return getAdmin().getAlterStatus(tableName).getFirst() == 0;
        }
      });
    }

    protected void waitForAllRegionsOnline(final long deadlineTs, final byte[][] splitKeys)
        throws IOException, TimeoutException {
      final TableDescriptor desc = getTableDescriptor();
      final AtomicInteger actualRegCount = new AtomicInteger(0);
      final MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
        @Override
        public boolean visit(Result rowResult) throws IOException {
          RegionLocations list = MetaTableAccessor.getRegionLocations(rowResult);
          if (list == null) {
            LOG.warn("No serialized HRegionInfo in " + rowResult);
            return true;
          }
          HRegionLocation l = list.getRegionLocation();
          if (l == null) {
            return true;
          }
          if (!l.getRegionInfo().getTable().equals(desc.getTableName())) {
            return false;
          }
          if (l.getRegionInfo().isOffline() || l.getRegionInfo().isSplit()) return true;
          HRegionLocation[] locations = list.getRegionLocations();
          for (HRegionLocation location : locations) {
            if (location == null) continue;
            ServerName serverName = location.getServerName();
            // Make sure that regions are assigned to server
            if (serverName != null && serverName.getAddress() != null) {
              actualRegCount.incrementAndGet();
            }
          }
          return true;
        }
      };

      int tries = 0;
      int numRegs = (splitKeys == null ? 1 : splitKeys.length + 1) * desc.getRegionReplication();
      while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
        actualRegCount.set(0);
        MetaTableAccessor.scanMetaForTableRegions(getAdmin().getConnection(), visitor,
          desc.getTableName());
        if (actualRegCount.get() == numRegs) {
          // all the regions are online
          return;
        }

        try {
          Thread.sleep(getAdmin().getPauseTime(tries++));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when opening" + " regions; "
              + actualRegCount.get() + " of " + numRegs + " regions processed so far");
        }
      }
      throw new TimeoutException("Only " + actualRegCount.get() + " of " + numRegs
          + " regions are online; retries exhausted.");
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  protected static abstract class NamespaceFuture extends ProcedureFuture<Void> {
    private final String namespaceName;

    public NamespaceFuture(final HBaseAdmin admin, final String namespaceName, final Long procId) {
      super(admin, procId);
      this.namespaceName = namespaceName;
    }

    /**
     * @return the namespace name
     */
    protected String getNamespaceName() {
      return namespaceName;
    }

    /**
     * @return the operation type like CREATE_NAMESPACE, DELETE_NAMESPACE, etc.
     */
    public abstract String getOperationType();

    @Override
    public String toString() {
      return "Operation: " + getOperationType() + ", Namespace: " + getNamespaceName();
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  private static class ReplicationFuture extends ProcedureFuture<Void> {
    private final String peerId;
    private final Supplier<String> getOperation;

    public ReplicationFuture(HBaseAdmin admin, String peerId, Long procId,
        Supplier<String> getOperation) {
      super(admin, procId);
      this.peerId = peerId;
      this.getOperation = getOperation;
    }

    @Override
    public String toString() {
      return "Operation: " + getOperation.get() + ", peerId: " + peerId;
    }
  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    try {
      return executeCallable(new MasterCallable<List<SecurityCapability>>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected List<SecurityCapability> rpcCall() throws Exception {
          SecurityCapabilitiesRequest req = SecurityCapabilitiesRequest.newBuilder().build();
          return ProtobufUtil.toSecurityCapabilityList(
            master.getSecurityCapabilities(getRpcController(), req).getCapabilitiesList());
        }
      });
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        e = ((RemoteException)e).unwrapRemoteException();
      }
      throw e;
    }
  }

  @Override
  public boolean splitSwitch(boolean enabled, boolean synchronous) throws IOException {
    return splitOrMergeSwitch(enabled, synchronous, MasterSwitchType.SPLIT);
  }

  @Override
  public boolean mergeSwitch(boolean enabled, boolean synchronous) throws IOException {
    return splitOrMergeSwitch(enabled, synchronous, MasterSwitchType.MERGE);
  }

  private boolean splitOrMergeSwitch(boolean enabled, boolean synchronous,
      MasterSwitchType switchType) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        MasterProtos.SetSplitOrMergeEnabledResponse response = master.setSplitOrMergeEnabled(
          getRpcController(),
          RequestConverter.buildSetSplitOrMergeEnabledRequest(enabled, synchronous, switchType));
        return response.getPrevValueList().get(0);
      }
    });
  }

  @Override
  public boolean isSplitEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isSplitOrMergeEnabled(getRpcController(),
          RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.SPLIT)).getEnabled();
      }
    });
  }

  @Override
  public boolean isMergeEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return master.isSplitOrMergeEnabled(getRpcController(),
          RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.MERGE)).getEnabled();
      }
    });
  }

  private RpcControllerFactory getRpcControllerFactory() {
    return this.rpcControllerFactory;
  }

  @Override
  public Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig,
      boolean enabled) throws IOException {
    AddReplicationPeerResponse response = executeCallable(
      new MasterCallable<AddReplicationPeerResponse>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected AddReplicationPeerResponse rpcCall() throws Exception {
          return master.addReplicationPeer(getRpcController(),
            RequestConverter.buildAddReplicationPeerRequest(peerId, peerConfig, enabled));
        }
      });
    return new ReplicationFuture(this, peerId, response.getProcId(), () -> "ADD_REPLICATION_PEER");
  }

  @Override
  public Future<Void> removeReplicationPeerAsync(String peerId) throws IOException {
    RemoveReplicationPeerResponse response =
      executeCallable(new MasterCallable<RemoveReplicationPeerResponse>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected RemoveReplicationPeerResponse rpcCall() throws Exception {
          return master.removeReplicationPeer(getRpcController(),
            RequestConverter.buildRemoveReplicationPeerRequest(peerId));
        }
      });
    return new ReplicationFuture(this, peerId, response.getProcId(),
      () -> "REMOVE_REPLICATION_PEER");
  }

  @Override
  public Future<Void> enableReplicationPeerAsync(final String peerId) throws IOException {
    EnableReplicationPeerResponse response =
      executeCallable(new MasterCallable<EnableReplicationPeerResponse>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected EnableReplicationPeerResponse rpcCall() throws Exception {
          return master.enableReplicationPeer(getRpcController(),
            RequestConverter.buildEnableReplicationPeerRequest(peerId));
        }
      });
    return new ReplicationFuture(this, peerId, response.getProcId(),
      () -> "ENABLE_REPLICATION_PEER");
  }

  @Override
  public Future<Void> disableReplicationPeerAsync(final String peerId) throws IOException {
    DisableReplicationPeerResponse response =
      executeCallable(new MasterCallable<DisableReplicationPeerResponse>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected DisableReplicationPeerResponse rpcCall() throws Exception {
          return master.disableReplicationPeer(getRpcController(),
            RequestConverter.buildDisableReplicationPeerRequest(peerId));
        }
      });
    return new ReplicationFuture(this, peerId, response.getProcId(),
      () -> "DISABLE_REPLICATION_PEER");
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(final String peerId) throws IOException {
    return executeCallable(new MasterCallable<ReplicationPeerConfig>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected ReplicationPeerConfig rpcCall() throws Exception {
        GetReplicationPeerConfigResponse response = master.getReplicationPeerConfig(
          getRpcController(), RequestConverter.buildGetReplicationPeerConfigRequest(peerId));
        return ReplicationPeerConfigUtil.convert(response.getPeerConfig());
      }
    });
  }

  @Override
  public Future<Void> updateReplicationPeerConfigAsync(final String peerId,
      final ReplicationPeerConfig peerConfig) throws IOException {
    UpdateReplicationPeerConfigResponse response =
      executeCallable(new MasterCallable<UpdateReplicationPeerConfigResponse>(getConnection(),
          getRpcControllerFactory()) {
        @Override
        protected UpdateReplicationPeerConfigResponse rpcCall() throws Exception {
          return master.updateReplicationPeerConfig(getRpcController(),
            RequestConverter.buildUpdateReplicationPeerConfigRequest(peerId, peerConfig));
        }
      });
    return new ReplicationFuture(this, peerId, response.getProcId(),
      () -> "UPDATE_REPLICATION_PEER_CONFIG");
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    return listReplicationPeers((Pattern)null);
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern)
      throws IOException {
    return executeCallable(new MasterCallable<List<ReplicationPeerDescription>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<ReplicationPeerDescription> rpcCall() throws Exception {
        List<ReplicationProtos.ReplicationPeerDescription> peersList = master.listReplicationPeers(
          getRpcController(), RequestConverter.buildListReplicationPeersRequest(pattern))
            .getPeerDescList();
        List<ReplicationPeerDescription> result = new ArrayList<>(peersList.size());
        for (ReplicationProtos.ReplicationPeerDescription peer : peersList) {
          result.add(ReplicationPeerConfigUtil.toReplicationPeerDescription(peer));
        }
        return result;
      }
    });
  }

  @Override
  public void decommissionRegionServers(List<ServerName> servers, boolean offload)
      throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      public Void rpcCall() throws ServiceException {
        master.decommissionRegionServers(getRpcController(),
          RequestConverter.buildDecommissionRegionServersRequest(servers, offload));
        return null;
      }
    });
  }

  @Override
  public List<ServerName> listDecommissionedRegionServers() throws IOException {
    return executeCallable(new MasterCallable<List<ServerName>>(getConnection(),
              getRpcControllerFactory()) {
      @Override
      public List<ServerName> rpcCall() throws ServiceException {
        ListDecommissionedRegionServersRequest req =
            ListDecommissionedRegionServersRequest.newBuilder().build();
        List<ServerName> servers = new ArrayList<>();
        for (HBaseProtos.ServerName server : master
            .listDecommissionedRegionServers(getRpcController(), req).getServerNameList()) {
          servers.add(ProtobufUtil.toServerName(server));
        }
        return servers;
      }
    });
  }

  @Override
  public void recommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames)
      throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      public Void rpcCall() throws ServiceException {
        master.recommissionRegionServer(getRpcController(),
          RequestConverter.buildRecommissionRegionServerRequest(server, encodedRegionNames));
        return null;
      }
    });
  }

  @Override
  public List<TableCFs> listReplicatedTableCFs() throws IOException {
    List<TableCFs> replicatedTableCFs = new ArrayList<>();
    List<TableDescriptor> tables = listTableDescriptors();
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
    return replicatedTableCFs;
  }

  @Override
  public void enableTableReplication(final TableName tableName) throws IOException {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name cannot be null");
    }
    if (!tableExists(tableName)) {
      throw new TableNotFoundException("Table '" + tableName.getNameAsString()
          + "' does not exists.");
    }
    byte[][] splits = getTableSplits(tableName);
    checkAndSyncTableDescToPeers(tableName, splits);
    setTableRep(tableName, true);
  }

  @Override
  public void disableTableReplication(final TableName tableName) throws IOException {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name is null");
    }
    if (!tableExists(tableName)) {
      throw new TableNotFoundException("Table '" + tableName.getNameAsString()
          + "' does not exists.");
    }
    setTableRep(tableName, false);
  }

  /**
   * Connect to peer and check the table descriptor on peer:
   * <ol>
   * <li>Create the same table on peer when not exist.</li>
   * <li>Throw an exception if the table already has replication enabled on any of the column
   * families.</li>
   * <li>Throw an exception if the table exists on peer cluster but descriptors are not same.</li>
   * </ol>
   * @param tableName name of the table to sync to the peer
   * @param splits table split keys
   * @throws IOException if a remote or network exception occurs
   */
  private void checkAndSyncTableDescToPeers(final TableName tableName, final byte[][] splits)
      throws IOException {
    List<ReplicationPeerDescription> peers = listReplicationPeers();
    if (peers == null || peers.size() <= 0) {
      throw new IllegalArgumentException("Found no peer cluster for replication.");
    }

    for (ReplicationPeerDescription peerDesc : peers) {
      if (peerDesc.getPeerConfig().needToReplicate(tableName)) {
        Configuration peerConf =
            ReplicationPeerConfigUtil.getPeerClusterConfiguration(this.conf, peerDesc);
        try (Connection conn = ConnectionFactory.createConnection(peerConf);
            Admin repHBaseAdmin = conn.getAdmin()) {
          TableDescriptor tableDesc = getDescriptor(tableName);
          TableDescriptor peerTableDesc = null;
          if (!repHBaseAdmin.tableExists(tableName)) {
            repHBaseAdmin.createTable(tableDesc, splits);
          } else {
            peerTableDesc = repHBaseAdmin.getDescriptor(tableName);
            if (peerTableDesc == null) {
              throw new IllegalArgumentException("Failed to get table descriptor for table "
                  + tableName.getNameAsString() + " from peer cluster " + peerDesc.getPeerId());
            }
            if (TableDescriptor.COMPARATOR_IGNORE_REPLICATION.compare(peerTableDesc,
              tableDesc) != 0) {
              throw new IllegalArgumentException("Table " + tableName.getNameAsString()
                  + " exists in peer cluster " + peerDesc.getPeerId()
                  + ", but the table descriptors are not same when compared with source cluster."
                  + " Thus can not enable the table's replication switch.");
            }
          }
        }
      }
    }
  }

  /**
   * Set the table's replication switch if the table's replication switch is already not set.
   * @param tableName name of the table
   * @param enableRep is replication switch enable or disable
   * @throws IOException if a remote or network exception occurs
   */
  private void setTableRep(final TableName tableName, boolean enableRep) throws IOException {
    TableDescriptor tableDesc = getDescriptor(tableName);
    if (!tableDesc.matchReplicationScope(enableRep)) {
      int scope =
          enableRep ? HConstants.REPLICATION_SCOPE_GLOBAL : HConstants.REPLICATION_SCOPE_LOCAL;
      modifyTable(TableDescriptorBuilder.newBuilder(tableDesc).setReplicationScope(scope).build());
    }
  }

  @Override
  public void clearCompactionQueues(final ServerName sn, final Set<String> queues)
    throws IOException, InterruptedException {
    if (queues == null || queues.size() == 0) {
      throw new IllegalArgumentException("queues cannot be null or empty");
    }
    final AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    Callable<Void> callable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO: There is no timeout on this controller. Set one!
        HBaseRpcController controller = rpcControllerFactory.newController();
        ClearCompactionQueuesRequest request =
                RequestConverter.buildClearCompactionQueuesRequest(queues);
        admin.clearCompactionQueues(controller, request);
        return null;
      }
    };
    ProtobufUtil.call(callable);
  }

  @Override
  public List<ServerName> clearDeadServers(List<ServerName> servers) throws IOException {
    return executeCallable(new MasterCallable<List<ServerName>>(getConnection(),
            getRpcControllerFactory()) {
      @Override
      protected List<ServerName> rpcCall() throws Exception {
        ClearDeadServersRequest req = RequestConverter.
          buildClearDeadServersRequest(servers == null? Collections.EMPTY_LIST: servers);
        return ProtobufUtil.toServerNameList(
                master.clearDeadServers(getRpcController(), req).getServerNameList());
      }
    });
  }

  @Override
  public void cloneTableSchema(final TableName tableName, final TableName newTableName,
      final boolean preserveSplits) throws IOException {
    checkTableExists(tableName);
    if (tableExists(newTableName)) {
      throw new TableExistsException(newTableName);
    }
    TableDescriptor htd = TableDescriptorBuilder.copy(newTableName, getTableDescriptor(tableName));
    if (preserveSplits) {
      createTable(htd, getTableSplits(tableName));
    } else {
      createTable(htd);
    }
  }

  @Override
  public boolean switchRpcThrottle(final boolean enable) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return this.master
            .switchRpcThrottle(getRpcController(), MasterProtos.SwitchRpcThrottleRequest
                .newBuilder().setRpcThrottleEnabled(enable).build())
            .getPreviousRpcThrottleEnabled();
      }
    });
  }

  @Override
  public boolean isRpcThrottleEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return this.master.isRpcThrottleEnabled(getRpcController(),
          IsRpcThrottleEnabledRequest.newBuilder().build()).getRpcThrottleEnabled();
      }
    });
  }

  @Override
  public boolean exceedThrottleQuotaSwitch(final boolean enable) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Boolean rpcCall() throws Exception {
        return this.master
            .switchExceedThrottleQuota(getRpcController(),
              MasterProtos.SwitchExceedThrottleQuotaRequest.newBuilder()
                  .setExceedThrottleQuotaEnabled(enable).build())
            .getPreviousExceedThrottleQuotaEnabled();
      }
    });
  }

  @Override
  public Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException {
    return executeCallable(
      new MasterCallable<Map<TableName, Long>>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected Map<TableName, Long> rpcCall() throws Exception {
          GetSpaceQuotaRegionSizesResponse resp = master.getSpaceQuotaRegionSizes(
            getRpcController(), RequestConverter.buildGetSpaceQuotaRegionSizesRequest());
          Map<TableName, Long> tableSizes = new HashMap<>();
          for (RegionSizes sizes : resp.getSizesList()) {
            TableName tn = ProtobufUtil.toTableName(sizes.getTableName());
            tableSizes.put(tn, sizes.getSize());
          }
          return tableSizes;
        }
      });
  }

  @Override
  public Map<TableName, SpaceQuotaSnapshot> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) throws IOException {
    final AdminService.BlockingInterface admin = this.connection.getAdmin(serverName);
    Callable<GetSpaceQuotaSnapshotsResponse> callable =
      new Callable<GetSpaceQuotaSnapshotsResponse>() {
        @Override
        public GetSpaceQuotaSnapshotsResponse call() throws Exception {
          return admin.getSpaceQuotaSnapshots(rpcControllerFactory.newController(),
            RequestConverter.buildGetSpaceQuotaSnapshotsRequest());
        }
      };
    GetSpaceQuotaSnapshotsResponse resp = ProtobufUtil.call(callable);
    Map<TableName, SpaceQuotaSnapshot> snapshots = new HashMap<>();
    for (TableQuotaSnapshot snapshot : resp.getSnapshotsList()) {
      snapshots.put(ProtobufUtil.toTableName(snapshot.getTableName()),
        SpaceQuotaSnapshot.toSpaceQuotaSnapshot(snapshot.getSnapshot()));
    }
    return snapshots;
  }

  @Override
  public SpaceQuotaSnapshot getCurrentSpaceQuotaSnapshot(String namespace) throws IOException {
    return executeCallable(
      new MasterCallable<SpaceQuotaSnapshot>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected SpaceQuotaSnapshot rpcCall() throws Exception {
          GetQuotaStatesResponse resp = master.getQuotaStates(getRpcController(),
            RequestConverter.buildGetQuotaStatesRequest());
          for (GetQuotaStatesResponse.NamespaceQuotaSnapshot nsSnapshot : resp
            .getNsSnapshotsList()) {
            if (namespace.equals(nsSnapshot.getNamespace())) {
              return SpaceQuotaSnapshot.toSpaceQuotaSnapshot(nsSnapshot.getSnapshot());
            }
          }
          return null;
        }
      });
  }

  @Override
  public SpaceQuotaSnapshot getCurrentSpaceQuotaSnapshot(TableName tableName) throws IOException {
    return executeCallable(
      new MasterCallable<SpaceQuotaSnapshot>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected SpaceQuotaSnapshot rpcCall() throws Exception {
          GetQuotaStatesResponse resp = master.getQuotaStates(getRpcController(),
            RequestConverter.buildGetQuotaStatesRequest());
          HBaseProtos.TableName protoTableName = ProtobufUtil.toProtoTableName(tableName);
          for (GetQuotaStatesResponse.TableQuotaSnapshot tableSnapshot : resp
            .getTableSnapshotsList()) {
            if (protoTableName.equals(tableSnapshot.getTableName())) {
              return SpaceQuotaSnapshot.toSpaceQuotaSnapshot(tableSnapshot.getSnapshot());
            }
          }
          return null;
        }
      });
  }

  @Override
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
      throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        GrantRequest req =
            ShadedAccessControlUtil.buildGrantRequest(userPermission, mergeExistingPermissions);
        this.master.grant(getRpcController(), req);
        return null;
      }
    });
  }

  @Override
  public void revoke(UserPermission userPermission) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection(), getRpcControllerFactory()) {
      @Override
      protected Void rpcCall() throws Exception {
        RevokeRequest req = ShadedAccessControlUtil.buildRevokeRequest(userPermission);
        this.master.revoke(getRpcController(), req);
        return null;
      }
    });
  }

  @Override
  public List<UserPermission>
      getUserPermissions(GetUserPermissionsRequest getUserPermissionsRequest) throws IOException {
    return executeCallable(
      new MasterCallable<List<UserPermission>>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected List<UserPermission> rpcCall() throws Exception {
          AccessControlProtos.GetUserPermissionsRequest req =
              ShadedAccessControlUtil.buildGetUserPermissionsRequest(getUserPermissionsRequest);
          AccessControlProtos.GetUserPermissionsResponse response =
              this.master.getUserPermissions(getRpcController(), req);
          return response.getUserPermissionList().stream()
              .map(userPermission -> ShadedAccessControlUtil.toUserPermission(userPermission))
              .collect(Collectors.toList());
        }
      });
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName) throws IOException {
    return splitRegionAsync(regionName, null);
  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor desc) throws IOException {
    return createTableAsync(desc, null);
  }

  @Override
  public List<Boolean> hasUserPermissions(String userName, List<Permission> permissions)
      throws IOException {
    return executeCallable(
      new MasterCallable<List<Boolean>>(getConnection(), getRpcControllerFactory()) {
        @Override
        protected List<Boolean> rpcCall() throws Exception {
          HasUserPermissionsRequest request =
              ShadedAccessControlUtil.buildHasUserPermissionsRequest(userName, permissions);
          return this.master.hasUserPermissions(getRpcController(), request)
              .getHasUserPermissionList();
        }
      });
  }

  @Override
  public boolean snapshotCleanupSwitch(boolean on, boolean synchronous) throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(),
        getRpcControllerFactory()) {

      @Override
      protected Boolean rpcCall() throws Exception {
        SetSnapshotCleanupRequest req =
          RequestConverter.buildSetSnapshotCleanupRequest(on, synchronous);
        return master.switchSnapshotCleanup(getRpcController(), req).getPrevSnapshotCleanup();
      }
    });

  }

  @Override
  public boolean isSnapshotCleanupEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection(),
        getRpcControllerFactory()) {

      @Override
      protected Boolean rpcCall() throws Exception {
        IsSnapshotCleanupEnabledRequest req =
          RequestConverter.buildIsSnapshotCleanupEnabledRequest();
        return master.isSnapshotCleanupEnabled(getRpcController(), req).getEnabled();
      }
    });

  }

  private List<LogEntry> getSlowLogResponses(
      final Map<String, Object> filterParams, final Set<ServerName> serverNames, final int limit,
      final String logType) {
    if (CollectionUtils.isEmpty(serverNames)) {
      return Collections.emptyList();
    }
    return serverNames.stream().map(serverName -> {
        try {
          return getSlowLogResponseFromServer(serverName, filterParams, limit, logType);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    ).flatMap(List::stream).collect(Collectors.toList());
  }

  private List<LogEntry> getSlowLogResponseFromServer(ServerName serverName,
      Map<String, Object> filterParams, int limit, String logType) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(serverName);
    return executeCallable(new RpcRetryingCallable<List<LogEntry>>() {
      @Override
      protected List<LogEntry> rpcCall(int callTimeout) throws Exception {
        HBaseRpcController controller = rpcControllerFactory.newController();
        HBaseProtos.LogRequest logRequest =
          RequestConverter.buildSlowLogResponseRequest(filterParams, limit, logType);
        HBaseProtos.LogEntry logEntry = admin.getLogEntries(controller, logRequest);
        return ProtobufUtil.toSlowLogPayloads(logEntry);
      }
    });
  }

  @Override
  public List<Boolean> clearSlowLogResponses(@Nullable final Set<ServerName> serverNames)
      throws IOException {
    if (CollectionUtils.isEmpty(serverNames)) {
      return Collections.emptyList();
    }
    return serverNames.stream().map(serverName -> {
      try {
        return clearSlowLogsResponses(serverName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  @Override
  public List<LogEntry> getLogEntries(Set<ServerName> serverNames, String logType,
      ServerType serverType, int limit, Map<String, Object> filterParams) throws IOException {
    if (logType == null || serverType == null) {
      throw new IllegalArgumentException("logType and/or serverType cannot be empty");
    }
    if (logType.equals("SLOW_LOG") || logType.equals("LARGE_LOG")) {
      if (ServerType.MASTER.equals(serverType)) {
        throw new IllegalArgumentException("Slow/Large logs are not maintained by HMaster");
      }
      return getSlowLogResponses(filterParams, serverNames, limit, logType);
    } else if (logType.equals("BALANCER_DECISION")) {
      if (ServerType.REGION_SERVER.equals(serverType)) {
        throw new IllegalArgumentException(
          "Balancer Decision logs are not maintained by HRegionServer");
      }
      return getBalancerDecisions(limit);
    } else if (logType.equals("BALANCER_REJECTION")) {
      if (ServerType.REGION_SERVER.equals(serverType)) {
        throw new IllegalArgumentException(
          "Balancer Rejection logs are not maintained by HRegionServer");
      }
      return getBalancerRejections(limit);
    }
    return Collections.emptyList();
  }

  private List<LogEntry> getBalancerDecisions(final int limit) throws IOException {
    return executeCallable(new MasterCallable<List<LogEntry>>(getConnection(),
        getRpcControllerFactory()) {
      @Override
      protected List<LogEntry> rpcCall() throws Exception {
        HBaseProtos.LogEntry logEntry =
          master.getLogEntries(getRpcController(), ProtobufUtil.toBalancerDecisionRequest(limit));
        return ProtobufUtil.toBalancerDecisionResponse(logEntry);
      }
    });
  }

  private List<LogEntry> getBalancerRejections(final int limit) throws IOException {
    return executeCallable(new MasterCallable<List<LogEntry>>(getConnection(),
      getRpcControllerFactory()) {
      @Override
      protected List<LogEntry> rpcCall() throws Exception {
        HBaseProtos.LogEntry logEntry =
          master.getLogEntries(getRpcController(), ProtobufUtil.toBalancerRejectionRequest(limit));
        return ProtobufUtil.toBalancerRejectionResponse(logEntry);
      }
    });
  }

  private Boolean clearSlowLogsResponses(final ServerName serverName) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(serverName);
    return executeCallable(new RpcRetryingCallable<Boolean>() {
      @Override
      protected Boolean rpcCall(int callTimeout) throws Exception {
        HBaseRpcController controller = rpcControllerFactory.newController();
        AdminProtos.ClearSlowLogResponses clearSlowLogResponses =
          admin.clearSlowLogsResponses(controller,
            RequestConverter.buildClearSlowLogResponseRequest());
        return ProtobufUtil.toClearSlowLogPayload(clearSlowLogResponses);
      }
    });
  }

}
