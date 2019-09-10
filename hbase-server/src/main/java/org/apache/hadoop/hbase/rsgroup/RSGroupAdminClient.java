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
package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.AddRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersAndTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Client used for managing region server group information.
 */
@InterfaceAudience.Private
public class RSGroupAdminClient implements RSGroupAdmin, Admin {
  private RSGroupAdminService.BlockingInterface stub;
  private Admin admin;

  public RSGroupAdminClient(Connection conn) throws IOException {
    admin = conn.getAdmin();
    stub = RSGroupAdminService.newBlockingStub(admin.coprocessorService());
  }

  // for writing UTs
  @VisibleForTesting
  protected RSGroupAdminClient() {
  }

  @Override
  public int getOperationTimeout() {
    return 0;
  }

  @Override
  public int getSyncWaitTimeout() {
    return 0;
  }

  @Override
  public void abort(String why, Throwable e) {

  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public Connection getConnection() {
    return null;
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    return false;
  }

  @Override
  public List<TableDescriptor> listTableDescriptors() throws IOException {
    return null;
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
      throws IOException {
    return null;
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return new TableName[0];
  }

  @Override
  public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
    return new TableName[0];
  }

  @Override
  public TableDescriptor getDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    return null;
  }

  @Override
  public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {

  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor desc) throws IOException {
    return null;
  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys)
      throws IOException {
    return null;
  }

  @Override
  public Future<Void> deleteTableAsync(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public Future<Void> truncateTableAsync(TableName tableName, boolean preserveSplits)
      throws IOException {
    return null;
  }

  @Override
  public Future<Void> enableTableAsync(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public Future<Void> disableTableAsync(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return false;
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return false;
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return false;
  }

  @Override
  public Future<Void> addColumnFamilyAsync(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    return null;
  }

  @Override
  public Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] columnFamily)
      throws IOException {
    return null;
  }

  @Override
  public Future<Void> modifyColumnFamilyAsync(TableName tableName,
      ColumnFamilyDescriptor columnFamily) throws IOException {
    return null;
  }

  @Override
  public List<RegionInfo> getRegions(ServerName serverName) throws IOException {
    return null;
  }

  @Override
  public void flush(TableName tableName) throws IOException {

  }

  @Override
  public void flushRegion(byte[] regionName) throws IOException {

  }

  @Override
  public void flushRegionServer(ServerName serverName) throws IOException {

  }

  @Override
  public void compact(TableName tableName) throws IOException {

  }

  @Override
  public void compactRegion(byte[] regionName) throws IOException {

  }

  @Override
  public void compact(TableName tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void compactRegion(byte[] regionName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void compact(TableName tableName, CompactType compactType)
      throws IOException, InterruptedException {

  }

  @Override
  public void compact(TableName tableName, byte[] columnFamily, CompactType compactType)
      throws IOException, InterruptedException {

  }

  @Override
  public void majorCompact(TableName tableName) throws IOException {

  }

  @Override
  public void majorCompactRegion(byte[] regionName) throws IOException {

  }

  @Override
  public void majorCompact(TableName tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void majorCompactRegion(byte[] regionName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void majorCompact(TableName tableName, CompactType compactType)
      throws IOException, InterruptedException {

  }

  @Override
  public void majorCompact(TableName tableName, byte[] columnFamily, CompactType compactType)
      throws IOException, InterruptedException {

  }

  @Override
  public Map<ServerName, Boolean> compactionSwitch(boolean switchState,
      List<String> serverNamesList) throws IOException {
    return null;
  }

  @Override
  public void compactRegionServer(ServerName serverName) throws IOException {

  }

  @Override
  public void majorCompactRegionServer(ServerName serverName) throws IOException {

  }

  @Override
  public void move(byte[] encodedRegionName) throws IOException {

  }

  @Override
  public void move(byte[] encodedRegionName, ServerName destServerName) throws IOException {

  }

  @Override
  public void assign(byte[] regionName) throws IOException {

  }

  @Override
  public void unassign(byte[] regionName, boolean force) throws IOException {

  }

  @Override
  public void offline(byte[] regionName) throws IOException {

  }

  @Override
  public boolean balancerSwitch(boolean onOrOff, boolean synchronous) throws IOException {
    return false;
  }

  @Override
  public boolean balance() throws IOException {
    return false;
  }

  @Override
  public boolean balance(boolean force) throws IOException {
    return false;
  }

  @Override
  public boolean isBalancerEnabled() throws IOException {
    return false;
  }

  @Override
  public CacheEvictionStats clearBlockCache(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public boolean normalize() throws IOException {
    return false;
  }

  @Override
  public boolean isNormalizerEnabled() throws IOException {
    return false;
  }

  @Override
  public boolean normalizerSwitch(boolean on) throws IOException {
    return false;
  }

  @Override
  public boolean catalogJanitorSwitch(boolean onOrOff) throws IOException {
    return false;
  }

  @Override
  public int runCatalogJanitor() throws IOException {
    return 0;
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws IOException {
    return false;
  }

  @Override
  public boolean cleanerChoreSwitch(boolean onOrOff) throws IOException {
    return false;
  }

  @Override
  public boolean runCleanerChore() throws IOException {
    return false;
  }

  @Override
  public boolean isCleanerChoreEnabled() throws IOException {
    return false;
  }

  @Override
  public Future<Void> mergeRegionsAsync(byte[][] nameofRegionsToMerge, boolean forcible)
      throws IOException {
    return null;
  }

  @Override
  public void split(TableName tableName) throws IOException {

  }

  @Override
  public void split(TableName tableName, byte[] splitPoint) throws IOException {

  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName) throws IOException {
    return null;
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName, byte[] splitPoint) throws IOException {
    return null;
  }

  @Override
  public Future<Void> modifyTableAsync(TableDescriptor td) throws IOException {
    return null;
  }

  @Override
  public void shutdown() throws IOException {

  }

  @Override
  public void stopMaster() throws IOException {

  }

  @Override
  public boolean isMasterInMaintenanceMode() throws IOException {
    return false;
  }

  @Override
  public void stopRegionServer(String hostnamePort) throws IOException {

  }

  @Override
  public ClusterMetrics getClusterMetrics(EnumSet<ClusterMetrics.Option> options)
      throws IOException {
    return null;
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName serverName) throws IOException {
    return null;
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName serverName, TableName tableName)
      throws IOException {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public Future<Void> createNamespaceAsync(NamespaceDescriptor descriptor) throws IOException {
    return null;
  }

  @Override
  public Future<Void> modifyNamespaceAsync(NamespaceDescriptor descriptor) throws IOException {
    return null;
  }

  @Override
  public Future<Void> deleteNamespaceAsync(String name) throws IOException {
    return null;
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name)
      throws NamespaceNotFoundException, IOException {
    return null;
  }

  @Override
  public String[] listNamespaces() throws IOException {
    return new String[0];
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return new NamespaceDescriptor[0];
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] name) throws IOException {
    return null;
  }

  @Override
  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    return new TableName[0];
  }

  @Override
  public List<RegionInfo> getRegions(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public List<TableDescriptor> listTableDescriptors(List<TableName> tableNames) throws IOException {
    return null;
  }

  @Override
  public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning)
      throws IOException {
    return null;
  }

  @Override
  public String getProcedures() throws IOException {
    return null;
  }

  @Override
  public String getLocks() throws IOException {
    return null;
  }

  @Override
  public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {

  }

  @Override
  public CompactionState getCompactionState(TableName tableName) throws IOException {
    return null;
  }

  @Override
  public CompactionState getCompactionState(TableName tableName, CompactType compactType)
      throws IOException {
    return null;
  }

  @Override
  public CompactionState getCompactionStateForRegion(byte[] regionName) throws IOException {
    return null;
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
    return 0;
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return 0;
  }

  @Override
  public void snapshot(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {

  }

  @Override
  public Future<Void> snapshotAsync(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    return null;
  }

  @Override
  public boolean isSnapshotFinished(SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    return false;
  }

  @Override
  public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {

  }

  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot, boolean restoreAcl)
      throws IOException, RestoreSnapshotException {

  }

  @Override
  public Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName,
      boolean restoreAcl) throws IOException, TableExistsException, RestoreSnapshotException {
    return null;
  }

  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {

  }

  @Override
  public byte[] execProcedureWithReturn(String signature, String instance,
      Map<String, String> props) throws IOException {
    return new byte[0];
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    return false;
  }

  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    return null;
  }

  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    return null;
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException {
    return null;
  }

  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {

  }

  @Override
  public void deleteSnapshots(Pattern pattern) throws IOException {

  }

  @Override
  public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
      throws IOException {

  }

  @Override
  public void setQuota(QuotaSettings quota) throws IOException {

  }

  @Override
  public List<QuotaSettings> getQuota(QuotaFilter filter) throws IOException {
    return null;
  }

  @Override
  public CoprocessorRpcChannel coprocessorService() {
    return null;
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
    return null;
  }

  @Override
  public void updateConfiguration(ServerName server) throws IOException {

  }

  @Override
  public void updateConfiguration() throws IOException {

  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    return null;
  }

  @Override
  public boolean splitSwitch(boolean enabled, boolean synchronous) throws IOException {
    return false;
  }

  @Override
  public boolean mergeSwitch(boolean enabled, boolean synchronous) throws IOException {
    return false;
  }

  @Override
  public boolean isSplitEnabled() throws IOException {
    return false;
  }

  @Override
  public boolean isMergeEnabled() throws IOException {
    return false;
  }

  @Override
  public Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig,
      boolean enabled) throws IOException {
    return null;
  }

  @Override
  public Future<Void> removeReplicationPeerAsync(String peerId) throws IOException {
    return null;
  }

  @Override
  public Future<Void> enableReplicationPeerAsync(String peerId) throws IOException {
    return null;
  }

  @Override
  public Future<Void> disableReplicationPeerAsync(String peerId) throws IOException {
    return null;
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId) throws IOException {
    return null;
  }

  @Override
  public Future<Void> updateReplicationPeerConfigAsync(String peerId,
      ReplicationPeerConfig peerConfig) throws IOException {
    return null;
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    return null;
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern) throws IOException {
    return null;
  }

  @Override
  public Future<Void> transitReplicationPeerSyncReplicationStateAsync(String peerId,
      SyncReplicationState state) throws IOException {
    return null;
  }

  @Override
  public void decommissionRegionServers(List<ServerName> servers, boolean offload)
      throws IOException {

  }

  @Override
  public List<ServerName> listDecommissionedRegionServers() throws IOException {
    return null;
  }

  @Override
  public void recommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames)
      throws IOException {

  }

  @Override
  public List<TableCFs> listReplicatedTableCFs() throws IOException {
    return null;
  }

  @Override
  public void enableTableReplication(TableName tableName) throws IOException {

  }

  @Override
  public void disableTableReplication(TableName tableName) throws IOException {

  }

  @Override
  public void clearCompactionQueues(ServerName serverName, Set<String> queues)
      throws IOException, InterruptedException {

  }

  @Override
  public List<ServerName> clearDeadServers(List<ServerName> servers) throws IOException {
    return null;
  }

  @Override
  public void cloneTableSchema(TableName tableName, TableName newTableName, boolean preserveSplits)
      throws IOException {

  }

  @Override
  public boolean switchRpcThrottle(boolean enable) throws IOException {
    return false;
  }

  @Override
  public boolean isRpcThrottleEnabled() throws IOException {
    return false;
  }

  @Override
  public boolean exceedThrottleQuotaSwitch(boolean enable) throws IOException {
    return false;
  }

  @Override
  public Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException {
    return null;
  }

  @Override
  public Map<TableName, ? extends SpaceQuotaSnapshotView> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) throws IOException {
    return null;
  }

  @Override
  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(String namespace) throws IOException {
    return null;
  }

  @Override
  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(TableName tableName)
      throws IOException {
    return null;
  }

  @Override
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
      throws IOException {

  }

  @Override
  public void revoke(UserPermission userPermission) throws IOException {

  }

  @Override
  public List<UserPermission> getUserPermissions(
      GetUserPermissionsRequest getUserPermissionsRequest) throws IOException {
    return null;
  }

  @Override
  public List<Boolean> hasUserPermissions(String userName, List<Permission> permissions)
      throws IOException {
    return null;
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    try {
      GetRSGroupInfoResponse resp = stub.getRSGroupInfo(null,
        GetRSGroupInfoRequest.newBuilder().setRSGroupName(groupName).build());
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    GetRSGroupInfoOfTableRequest request = GetRSGroupInfoOfTableRequest.newBuilder().setTableName(
        ProtobufUtil.toProtoTableName(tableName)).build();
    try {
      GetRSGroupInfoOfTableResponse resp = stub.getRSGroupInfoOfTable(null, request);
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void setRSGroupForTables(Set<TableName> tables, String groupName) throws IOException {

  }

  @Override
  public void moveServers(Set<Address> servers, String targetGroup) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    for(Address el: servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder()
        .setHostName(el.getHostname())
        .setPort(el.getPort())
        .build());
    }
    MoveServersRequest request = MoveServersRequest.newBuilder()
            .setTargetGroup(targetGroup)
            .addAllServers(hostPorts)
            .build();
    try {
      stub.moveServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    MoveTablesRequest.Builder builder = MoveTablesRequest.newBuilder().setTargetGroup(targetGroup);
    for(TableName tableName: tables) {
      builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
      if (!admin.tableExists(tableName)) {
        throw new TableNotFoundException(tableName);
      }
    }
    try {
      stub.moveTables(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void addRSGroup(String groupName) throws IOException {
    AddRSGroupRequest request = AddRSGroupRequest.newBuilder().setRSGroupName(groupName).build();
    try {
      stub.addRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    RemoveRSGroupRequest request = RemoveRSGroupRequest.newBuilder().setRSGroupName(name).build();
    try {
      stub.removeRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    BalanceRSGroupRequest request = BalanceRSGroupRequest.newBuilder()
        .setRSGroupName(groupName).build();
    try {
      return stub.balanceRSGroup(null, request).getBalanceRan();
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    try {
      List<RSGroupProtos.RSGroupInfo> resp = stub.listRSGroupInfos(null,
          ListRSGroupInfosRequest.getDefaultInstance()).getRSGroupInfoList();
      List<RSGroupInfo> result = new ArrayList<>(resp.size());
      for(RSGroupProtos.RSGroupInfo entry : resp) {
        result.add(ProtobufUtil.toGroupInfo(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    GetRSGroupInfoOfServerRequest request = GetRSGroupInfoOfServerRequest.newBuilder()
            .setServer(HBaseProtos.ServerName.newBuilder()
                .setHostName(hostPort.getHostname())
                .setPort(hostPort.getPort())
                .build())
            .build();
    try {
      GetRSGroupInfoOfServerResponse resp = stub.getRSGroupInfoOfServer(null, request);
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables, String targetGroup)
      throws IOException {
    MoveServersAndTablesRequest.Builder builder =
            MoveServersAndTablesRequest.newBuilder().setTargetGroup(targetGroup);
    for(Address el: servers) {
      builder.addServers(HBaseProtos.ServerName.newBuilder()
              .setHostName(el.getHostname())
              .setPort(el.getPort())
              .build());
    }
    for(TableName tableName: tables) {
      builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
      if (!admin.tableExists(tableName)) {
        throw new TableNotFoundException(tableName);
      }
    }
    try {
      stub.moveServersAndTables(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void removeServers(Set<Address> servers) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    for(Address el: servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder()
          .setHostName(el.getHostname())
          .setPort(el.getPort())
          .build());
    }
    RemoveServersRequest request = RemoveServersRequest.newBuilder()
        .addAllServers(hostPorts)
        .build();
    try {
      stub.removeServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }
}
