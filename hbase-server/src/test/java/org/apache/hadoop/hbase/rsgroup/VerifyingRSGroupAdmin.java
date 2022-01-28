/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.LogEntry;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerType;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.net.Address;
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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupProtos;

@InterfaceAudience.Private
public class VerifyingRSGroupAdmin implements Admin, Closeable {

  private final Connection conn;

  private final Admin admin;

  private final ZKWatcher zkw;

  public VerifyingRSGroupAdmin(Configuration conf) throws IOException {
    conn = ConnectionFactory.createConnection(conf);
    admin = conn.getAdmin();
    zkw = new ZKWatcher(conf, this.getClass().getSimpleName(), null);
  }

  public int getOperationTimeout() {
    return admin.getOperationTimeout();
  }

  public int getSyncWaitTimeout() {
    return admin.getSyncWaitTimeout();
  }

  public void abort(String why, Throwable e) {
    admin.abort(why, e);
  }

  public boolean isAborted() {
    return admin.isAborted();
  }

  public Connection getConnection() {
    return admin.getConnection();
  }

  public boolean tableExists(TableName tableName) throws IOException {
    return admin.tableExists(tableName);
  }

  public List<TableDescriptor> listTableDescriptors() throws IOException {
    return admin.listTableDescriptors();
  }

  public List<TableDescriptor> listTableDescriptors(boolean includeSysTables) throws IOException {
    return admin.listTableDescriptors(includeSysTables);
  }

  public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
    throws IOException {
    return admin.listTableDescriptors(pattern, includeSysTables);
  }

  public TableName[] listTableNames() throws IOException {
    return admin.listTableNames();
  }

  public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
    return admin.listTableNames(pattern, includeSysTables);
  }

  public TableDescriptor getDescriptor(TableName tableName)
    throws TableNotFoundException, IOException {
    return admin.getDescriptor(tableName);
  }

  public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
    throws IOException {
    admin.createTable(desc, startKey, endKey, numRegions);
  }

  public Future<Void> createTableAsync(TableDescriptor desc) throws IOException {
    return admin.createTableAsync(desc);
  }

  public Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys)
    throws IOException {
    return admin.createTableAsync(desc, splitKeys);
  }

  public Future<Void> deleteTableAsync(TableName tableName) throws IOException {
    return admin.deleteTableAsync(tableName);
  }

  public Future<Void> truncateTableAsync(TableName tableName, boolean preserveSplits)
    throws IOException {
    return admin.truncateTableAsync(tableName, preserveSplits);
  }

  public Future<Void> enableTableAsync(TableName tableName) throws IOException {
    return admin.enableTableAsync(tableName);
  }

  public Future<Void> disableTableAsync(TableName tableName) throws IOException {
    return admin.disableTableAsync(tableName);
  }

  public boolean isTableEnabled(TableName tableName) throws IOException {
    return admin.isTableEnabled(tableName);
  }

  public boolean isTableDisabled(TableName tableName) throws IOException {
    return admin.isTableDisabled(tableName);
  }

  public boolean isTableAvailable(TableName tableName) throws IOException {
    return admin.isTableAvailable(tableName);
  }

  public Future<Void> addColumnFamilyAsync(TableName tableName, ColumnFamilyDescriptor columnFamily)
    throws IOException {
    return admin.addColumnFamilyAsync(tableName, columnFamily);
  }

  public Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] columnFamily)
    throws IOException {
    return admin.deleteColumnFamilyAsync(tableName, columnFamily);
  }

  public Future<Void> modifyColumnFamilyAsync(TableName tableName,
    ColumnFamilyDescriptor columnFamily) throws IOException {
    return admin.modifyColumnFamilyAsync(tableName, columnFamily);
  }

  public List<RegionInfo> getRegions(ServerName serverName) throws IOException {
    return admin.getRegions(serverName);
  }

  public void flush(TableName tableName) throws IOException {
    admin.flush(tableName);
  }

  public void flush(TableName tableName, byte[] columnFamily) throws IOException {
    admin.flush(tableName, columnFamily);
  }

  public void flushRegion(byte[] regionName) throws IOException {
    admin.flushRegion(regionName);
  }

  public void flushRegion(byte[] regionName, byte[] columnFamily) throws IOException {
    admin.flushRegion(regionName, columnFamily);
  }

  public void flushRegionServer(ServerName serverName) throws IOException {
    admin.flushRegionServer(serverName);
  }

  public void compact(TableName tableName) throws IOException {
    admin.compact(tableName);
  }

  public void compactRegion(byte[] regionName) throws IOException {
    admin.compactRegion(regionName);
  }

  public void compact(TableName tableName, byte[] columnFamily) throws IOException {
    admin.compact(tableName, columnFamily);
  }

  public void compactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
    admin.compactRegion(regionName, columnFamily);
  }

  public void compact(TableName tableName, CompactType compactType)
    throws IOException, InterruptedException {
    admin.compact(tableName, compactType);
  }

  public void compact(TableName tableName, byte[] columnFamily, CompactType compactType)
    throws IOException, InterruptedException {
    admin.compact(tableName, columnFamily, compactType);
  }

  public void majorCompact(TableName tableName) throws IOException {
    admin.majorCompact(tableName);
  }

  public void majorCompactRegion(byte[] regionName) throws IOException {
    admin.majorCompactRegion(regionName);
  }

  public void majorCompact(TableName tableName, byte[] columnFamily) throws IOException {
    admin.majorCompact(tableName, columnFamily);
  }

  public void majorCompactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
    admin.majorCompactRegion(regionName, columnFamily);
  }

  public void majorCompact(TableName tableName, CompactType compactType)
    throws IOException, InterruptedException {
    admin.majorCompact(tableName, compactType);
  }

  public void majorCompact(TableName tableName, byte[] columnFamily, CompactType compactType)
    throws IOException, InterruptedException {
    admin.majorCompact(tableName, columnFamily, compactType);
  }

  public Map<ServerName, Boolean> compactionSwitch(boolean switchState,
    List<String> serverNamesList) throws IOException {
    return admin.compactionSwitch(switchState, serverNamesList);
  }

  public void compactRegionServer(ServerName serverName) throws IOException {
    admin.compactRegionServer(serverName);
  }

  public void majorCompactRegionServer(ServerName serverName) throws IOException {
    admin.majorCompactRegionServer(serverName);
  }

  public void move(byte[] encodedRegionName) throws IOException {
    admin.move(encodedRegionName);
  }

  public void move(byte[] encodedRegionName, ServerName destServerName) throws IOException {
    admin.move(encodedRegionName, destServerName);
  }

  public void assign(byte[] regionName) throws IOException {
    admin.assign(regionName);
  }

  public void unassign(byte[] regionName) throws IOException {
    admin.unassign(regionName);
  }

  public void offline(byte[] regionName) throws IOException {
    admin.offline(regionName);
  }

  public boolean balancerSwitch(boolean onOrOff, boolean synchronous) throws IOException {
    return admin.balancerSwitch(onOrOff, synchronous);
  }

  public BalanceResponse balance(BalanceRequest request) throws IOException {
    return admin.balance(request);
  }

  public boolean isBalancerEnabled() throws IOException {
    return admin.isBalancerEnabled();
  }

  public CacheEvictionStats clearBlockCache(TableName tableName) throws IOException {
    return admin.clearBlockCache(tableName);
  }

  @Override
  public boolean normalize(NormalizeTableFilterParams ntfp) throws IOException {
    return admin.normalize(ntfp);
  }

  public boolean isNormalizerEnabled() throws IOException {
    return admin.isNormalizerEnabled();
  }

  public boolean normalizerSwitch(boolean on) throws IOException {
    return admin.normalizerSwitch(on);
  }

  public boolean catalogJanitorSwitch(boolean onOrOff) throws IOException {
    return admin.catalogJanitorSwitch(onOrOff);
  }

  public int runCatalogJanitor() throws IOException {
    return admin.runCatalogJanitor();
  }

  public boolean isCatalogJanitorEnabled() throws IOException {
    return admin.isCatalogJanitorEnabled();
  }

  public boolean cleanerChoreSwitch(boolean onOrOff) throws IOException {
    return admin.cleanerChoreSwitch(onOrOff);
  }

  public boolean runCleanerChore() throws IOException {
    return admin.runCleanerChore();
  }

  public boolean isCleanerChoreEnabled() throws IOException {
    return admin.isCleanerChoreEnabled();
  }

  public Future<Void> mergeRegionsAsync(byte[][] nameofRegionsToMerge, boolean forcible)
    throws IOException {
    return admin.mergeRegionsAsync(nameofRegionsToMerge, forcible);
  }

  public void split(TableName tableName) throws IOException {
    admin.split(tableName);
  }

  public void split(TableName tableName, byte[] splitPoint) throws IOException {
    admin.split(tableName, splitPoint);
  }

  public Future<Void> splitRegionAsync(byte[] regionName) throws IOException {
    return admin.splitRegionAsync(regionName);
  }

  public Future<Void> splitRegionAsync(byte[] regionName, byte[] splitPoint) throws IOException {
    return admin.splitRegionAsync(regionName, splitPoint);
  }

  public Future<Void> modifyTableAsync(TableDescriptor td) throws IOException {
    return admin.modifyTableAsync(td);
  }

  public void shutdown() throws IOException {
    admin.shutdown();
  }

  public void stopMaster() throws IOException {
    admin.stopMaster();
  }

  public boolean isMasterInMaintenanceMode() throws IOException {
    return admin.isMasterInMaintenanceMode();
  }

  public void stopRegionServer(String hostnamePort) throws IOException {
    admin.stopRegionServer(hostnamePort);
  }

  public ClusterMetrics getClusterMetrics(EnumSet<Option> options) throws IOException {
    return admin.getClusterMetrics(options);
  }

  public List<RegionMetrics> getRegionMetrics(ServerName serverName) throws IOException {
    return admin.getRegionMetrics(serverName);
  }

  public List<RegionMetrics> getRegionMetrics(ServerName serverName, TableName tableName)
    throws IOException {
    return admin.getRegionMetrics(serverName, tableName);
  }

  public Configuration getConfiguration() {
    return admin.getConfiguration();
  }

  public Future<Void> createNamespaceAsync(NamespaceDescriptor descriptor) throws IOException {
    return admin.createNamespaceAsync(descriptor);
  }

  public Future<Void> modifyNamespaceAsync(NamespaceDescriptor descriptor) throws IOException {
    return admin.modifyNamespaceAsync(descriptor);
  }

  public Future<Void> deleteNamespaceAsync(String name) throws IOException {
    return admin.deleteNamespaceAsync(name);
  }

  public NamespaceDescriptor getNamespaceDescriptor(String name)
    throws NamespaceNotFoundException, IOException {
    return admin.getNamespaceDescriptor(name);
  }

  public String[] listNamespaces() throws IOException {
    return admin.listNamespaces();
  }

  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return admin.listNamespaceDescriptors();
  }

  public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] name) throws IOException {
    return admin.listTableDescriptorsByNamespace(name);
  }

  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    return admin.listTableNamesByNamespace(name);
  }

  public List<RegionInfo> getRegions(TableName tableName) throws IOException {
    return admin.getRegions(tableName);
  }

  public void close() {
    admin.close();
  }

  public List<TableDescriptor> listTableDescriptors(List<TableName> tableNames) throws IOException {
    return admin.listTableDescriptors(tableNames);
  }

  public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning)
    throws IOException {
    return admin.abortProcedureAsync(procId, mayInterruptIfRunning);
  }

  public String getProcedures() throws IOException {
    return admin.getProcedures();
  }

  public String getLocks() throws IOException {
    return admin.getLocks();
  }

  public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {
    admin.rollWALWriter(serverName);
  }

  public CompactionState getCompactionState(TableName tableName) throws IOException {
    return admin.getCompactionState(tableName);
  }

  public CompactionState getCompactionState(TableName tableName, CompactType compactType)
    throws IOException {
    return admin.getCompactionState(tableName, compactType);
  }

  public CompactionState getCompactionStateForRegion(byte[] regionName) throws IOException {
    return admin.getCompactionStateForRegion(regionName);
  }

  public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
    return admin.getLastMajorCompactionTimestamp(tableName);
  }

  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return admin.getLastMajorCompactionTimestampForRegion(regionName);
  }

  public void snapshot(SnapshotDescription snapshot)
    throws IOException, SnapshotCreationException, IllegalArgumentException {
    admin.snapshot(snapshot);
  }

  public Future<Void> snapshotAsync(SnapshotDescription snapshot)
    throws IOException, SnapshotCreationException {
    return admin.snapshotAsync(snapshot);
  }

  public boolean isSnapshotFinished(SnapshotDescription snapshot)
    throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    return admin.isSnapshotFinished(snapshot);
  }

  public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
    admin.restoreSnapshot(snapshotName);
  }

  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot, boolean restoreAcl)
    throws IOException, RestoreSnapshotException {
    admin.restoreSnapshot(snapshotName, takeFailSafeSnapshot, restoreAcl);
  }

  public Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName,
    boolean restoreAcl, String customSFT)
    throws IOException, TableExistsException, RestoreSnapshotException {
    return admin.cloneSnapshotAsync(snapshotName, tableName, restoreAcl, customSFT);
  }

  public void execProcedure(String signature, String instance, Map<String, String> props)
    throws IOException {
    admin.execProcedure(signature, instance, props);
  }

  public byte[] execProcedureWithReturn(String signature, String instance,
    Map<String, String> props) throws IOException {
    return admin.execProcedureWithReturn(signature, instance, props);
  }

  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
    throws IOException {
    return admin.isProcedureFinished(signature, instance, props);
  }

  public List<SnapshotDescription> listSnapshots() throws IOException {
    return admin.listSnapshots();
  }

  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    return admin.listSnapshots(pattern);
  }

  public List<SnapshotDescription> listTableSnapshots(Pattern tableNamePattern,
    Pattern snapshotNamePattern) throws IOException {
    return admin.listTableSnapshots(tableNamePattern, snapshotNamePattern);
  }

  public void deleteSnapshot(String snapshotName) throws IOException {
    admin.deleteSnapshot(snapshotName);
  }

  public void deleteSnapshots(Pattern pattern) throws IOException {
    admin.deleteSnapshots(pattern);
  }

  public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
    throws IOException {
    admin.deleteTableSnapshots(tableNamePattern, snapshotNamePattern);
  }

  public void setQuota(QuotaSettings quota) throws IOException {
    admin.setQuota(quota);
  }

  public List<QuotaSettings> getQuota(QuotaFilter filter) throws IOException {
    return admin.getQuota(filter);
  }

  public CoprocessorRpcChannel coprocessorService() {
    return admin.coprocessorService();
  }

  public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
    return admin.coprocessorService(serverName);
  }

  public void updateConfiguration(ServerName server) throws IOException {
    admin.updateConfiguration(server);
  }

  public void updateConfiguration() throws IOException {
    admin.updateConfiguration();
  }

  public void updateConfiguration(String groupName) throws IOException {
    admin.updateConfiguration(groupName);
  }

  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    return admin.getSecurityCapabilities();
  }

  public boolean splitSwitch(boolean enabled, boolean synchronous) throws IOException {
    return admin.splitSwitch(enabled, synchronous);
  }

  public boolean mergeSwitch(boolean enabled, boolean synchronous) throws IOException {
    return admin.mergeSwitch(enabled, synchronous);
  }

  public boolean isSplitEnabled() throws IOException {
    return admin.isSplitEnabled();
  }

  public boolean isMergeEnabled() throws IOException {
    return admin.isMergeEnabled();
  }

  public Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig,
    boolean enabled) throws IOException {
    return admin.addReplicationPeerAsync(peerId, peerConfig, enabled);
  }

  public Future<Void> removeReplicationPeerAsync(String peerId) throws IOException {
    return admin.removeReplicationPeerAsync(peerId);
  }

  public Future<Void> enableReplicationPeerAsync(String peerId) throws IOException {
    return admin.enableReplicationPeerAsync(peerId);
  }

  public Future<Void> disableReplicationPeerAsync(String peerId) throws IOException {
    return admin.disableReplicationPeerAsync(peerId);
  }

  public ReplicationPeerConfig getReplicationPeerConfig(String peerId) throws IOException {
    return admin.getReplicationPeerConfig(peerId);
  }

  public Future<Void> updateReplicationPeerConfigAsync(String peerId,
    ReplicationPeerConfig peerConfig) throws IOException {
    return admin.updateReplicationPeerConfigAsync(peerId, peerConfig);
  }

  public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    return admin.listReplicationPeers();
  }

  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern) throws IOException {
    return admin.listReplicationPeers(pattern);
  }

  public Future<Void> transitReplicationPeerSyncReplicationStateAsync(String peerId,
    SyncReplicationState state) throws IOException {
    return admin.transitReplicationPeerSyncReplicationStateAsync(peerId, state);
  }

  public void decommissionRegionServers(List<ServerName> servers, boolean offload)
    throws IOException {
    admin.decommissionRegionServers(servers, offload);
  }

  public List<ServerName> listDecommissionedRegionServers() throws IOException {
    return admin.listDecommissionedRegionServers();
  }

  public void recommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames)
    throws IOException {
    admin.recommissionRegionServer(server, encodedRegionNames);
  }

  public List<TableCFs> listReplicatedTableCFs() throws IOException {
    return admin.listReplicatedTableCFs();
  }

  public void enableTableReplication(TableName tableName) throws IOException {
    admin.enableTableReplication(tableName);
  }

  public void disableTableReplication(TableName tableName) throws IOException {
    admin.disableTableReplication(tableName);
  }

  public void clearCompactionQueues(ServerName serverName, Set<String> queues)
    throws IOException, InterruptedException {
    admin.clearCompactionQueues(serverName, queues);
  }

  public List<ServerName> clearDeadServers(List<ServerName> servers) throws IOException {
    return admin.clearDeadServers(servers);
  }

  public void cloneTableSchema(TableName tableName, TableName newTableName, boolean preserveSplits)
    throws IOException {
    admin.cloneTableSchema(tableName, newTableName, preserveSplits);
  }

  public boolean switchRpcThrottle(boolean enable) throws IOException {
    return admin.switchRpcThrottle(enable);
  }

  public boolean isRpcThrottleEnabled() throws IOException {
    return admin.isRpcThrottleEnabled();
  }

  public boolean exceedThrottleQuotaSwitch(boolean enable) throws IOException {
    return admin.exceedThrottleQuotaSwitch(enable);
  }

  public Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException {
    return admin.getSpaceQuotaTableSizes();
  }

  public Map<TableName, ? extends SpaceQuotaSnapshotView>
    getRegionServerSpaceQuotaSnapshots(ServerName serverName) throws IOException {
    return admin.getRegionServerSpaceQuotaSnapshots(serverName);
  }

  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(String namespace) throws IOException {
    return admin.getCurrentSpaceQuotaSnapshot(namespace);
  }

  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(TableName tableName)
    throws IOException {
    return admin.getCurrentSpaceQuotaSnapshot(tableName);
  }

  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
    throws IOException {
    admin.grant(userPermission, mergeExistingPermissions);
  }

  public void revoke(UserPermission userPermission) throws IOException {
    admin.revoke(userPermission);
  }

  public List<UserPermission>
    getUserPermissions(GetUserPermissionsRequest getUserPermissionsRequest) throws IOException {
    return admin.getUserPermissions(getUserPermissionsRequest);
  }

  public List<Boolean> hasUserPermissions(String userName, List<Permission> permissions)
    throws IOException {
    return admin.hasUserPermissions(userName, permissions);
  }

  public boolean snapshotCleanupSwitch(boolean on, boolean synchronous) throws IOException {
    return admin.snapshotCleanupSwitch(on, synchronous);
  }

  public boolean isSnapshotCleanupEnabled() throws IOException {
    return admin.isSnapshotCleanupEnabled();
  }

  public void addRSGroup(String groupName) throws IOException {
    admin.addRSGroup(groupName);
    verify();
  }

  public RSGroupInfo getRSGroup(String groupName) throws IOException {
    return admin.getRSGroup(groupName);
  }

  public RSGroupInfo getRSGroup(Address hostPort) throws IOException {
    return admin.getRSGroup(hostPort);
  }

  public RSGroupInfo getRSGroup(TableName tableName) throws IOException {
    return admin.getRSGroup(tableName);
  }

  public List<RSGroupInfo> listRSGroups() throws IOException {
    return admin.listRSGroups();
  }

  @Override
  public List<TableName> listTablesInRSGroup(String groupName) throws IOException {
    return admin.listTablesInRSGroup(groupName);
  }

  @Override
  public Pair<List<String>, List<TableName>>
    getConfiguredNamespacesAndTablesInRSGroup(String groupName) throws IOException {
    return admin.getConfiguredNamespacesAndTablesInRSGroup(groupName);
  }

  public void removeRSGroup(String groupName) throws IOException {
    admin.removeRSGroup(groupName);
    verify();
  }

  public void removeServersFromRSGroup(Set<Address> servers) throws IOException {
    admin.removeServersFromRSGroup(servers);
    verify();
  }

  public void moveServersToRSGroup(Set<Address> servers, String targetGroup) throws IOException {
    admin.moveServersToRSGroup(servers, targetGroup);
    verify();
  }

  public void setRSGroup(Set<TableName> tables, String groupName) throws IOException {
    admin.setRSGroup(tables, groupName);
    verify();
  }

  public BalanceResponse balanceRSGroup(String groupName, BalanceRequest request) throws IOException {
    return admin.balanceRSGroup(groupName, request);
  }

  @Override
  public void renameRSGroup(String oldName, String newName) throws IOException {
    admin.renameRSGroup(oldName, newName);
    verify();
  }

  @Override
  public void updateRSGroupConfig(String groupName, Map<String, String> configuration)
      throws IOException {
    admin.updateRSGroupConfig(groupName, configuration);
    verify();
  }

  @Override
  public List<LogEntry> getLogEntries(Set<ServerName> serverNames, String logType,
    ServerType serverType, int limit, Map<String, Object> filterParams) throws IOException {
    return admin.getLogEntries(serverNames, logType, serverType, limit, filterParams);
  }

  private void verify() throws IOException {
    Map<String, RSGroupInfo> groupMap = Maps.newHashMap();
    Set<RSGroupInfo> zList = Sets.newHashSet();
    List<TableDescriptor> tds = new ArrayList<>();
    try (Admin admin = conn.getAdmin()) {
      tds.addAll(admin.listTableDescriptors());
      tds.addAll(admin.listTableDescriptorsByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME));
    }
    SortedSet<Address> lives = Sets.newTreeSet();
    for (ServerName sn : conn.getAdmin().getClusterMetrics().getLiveServerMetrics().keySet()) {
      lives.add(sn.getAddress());
    }
    for (ServerName sn : conn.getAdmin().listDecommissionedRegionServers()) {
      lives.remove(sn.getAddress());
    }
    try (Table table = conn.getTable(RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME);
      ResultScanner scanner = table.getScanner(new Scan())) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        RSGroupProtos.RSGroupInfo proto = RSGroupProtos.RSGroupInfo.parseFrom(result.getValue(
          RSGroupInfoManagerImpl.META_FAMILY_BYTES, RSGroupInfoManagerImpl.META_QUALIFIER_BYTES));
        RSGroupInfo rsGroupInfo = ProtobufUtil.toGroupInfo(proto);
        groupMap.put(proto.getName(), RSGroupUtil.fillTables(rsGroupInfo, tds));
        for (Address address : rsGroupInfo.getServers()) {
          lives.remove(address);
        }
      }
    }
    SortedSet<TableName> tables = Sets.newTreeSet();
    for (TableDescriptor td : conn.getAdmin().listTableDescriptors(Pattern.compile(".*"), true)) {
      String groupName = td.getRegionServerGroup().orElse(RSGroupInfo.DEFAULT_GROUP);
      if (groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
        tables.add(td.getTableName());
      }
    }

    groupMap.put(RSGroupInfo.DEFAULT_GROUP,
      new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, lives, tables));
    assertEquals(Sets.newHashSet(groupMap.values()), Sets.newHashSet(admin.listRSGroups()));
    try {
      String groupBasePath = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "rsgroup");
      for (String znode : ZKUtil.listChildrenNoWatch(zkw, groupBasePath)) {
        byte[] data = ZKUtil.getData(zkw, ZNodePaths.joinZNode(groupBasePath, znode));
        if (data.length > 0) {
          ProtobufUtil.expectPBMagicPrefix(data);
          ByteArrayInputStream bis =
            new ByteArrayInputStream(data, ProtobufUtil.lengthOfPBMagic(), data.length);
          RSGroupInfo rsGroupInfo =
            ProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis));
          zList.add(RSGroupUtil.fillTables(rsGroupInfo, tds));
        }
      }
      groupMap.remove(RSGroupInfo.DEFAULT_GROUP);
      assertEquals(zList.size(), groupMap.size());
      for (RSGroupInfo rsGroupInfo : zList) {
        assertTrue(groupMap.get(rsGroupInfo.getName()).equals(rsGroupInfo));
      }
    } catch (KeeperException e) {
      throw new IOException("ZK verification failed", e);
    } catch (DeserializationException e) {
      throw new IOException("ZK verification failed", e);
    } catch (InterruptedException e) {
      throw new IOException("ZK verification failed", e);
    }
  }

  @Override
  public List<Boolean> clearSlowLogResponses(Set<ServerName> serverNames) throws IOException {
    return admin.clearSlowLogResponses(serverNames);
  }

  @Override
  public Future<Void> modifyColumnFamilyStoreFileTrackerAsync(TableName tableName, byte[] family,
    String dstSFT) throws IOException {
    return admin.modifyColumnFamilyStoreFileTrackerAsync(tableName, family, dstSFT);
  }

  @Override
  public Future<Void> modifyTableStoreFileTrackerAsync(TableName tableName, String dstSFT)
    throws IOException {
    return admin.modifyTableStoreFileTrackerAsync(tableName, dstSFT);
  }
}
