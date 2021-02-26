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
package org.apache.hadoop.hbase.client;

import com.google.protobuf.RpcChannel;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Just a wrapper of {@link RawAsyncHBaseAdmin}. The difference is that users need to provide a
 * thread pool when constructing this class, and the callback methods registered to the returned
 * {@link CompletableFuture} will be executed in this thread pool. So usually it is safe for users
 * to do anything they want in the callbacks without breaking the rpc framework.
 * @since 2.0.0
 * @see RawAsyncHBaseAdmin
 * @see AsyncConnection#getAdmin(ExecutorService)
 * @see AsyncConnection#getAdminBuilder(ExecutorService)
 */
@InterfaceAudience.Private
class AsyncHBaseAdmin implements AsyncAdmin {

  private final RawAsyncHBaseAdmin rawAdmin;

  private final ExecutorService pool;

  AsyncHBaseAdmin(RawAsyncHBaseAdmin rawAdmin, ExecutorService pool) {
    this.rawAdmin = rawAdmin;
    this.pool = pool;
  }

  private <T> CompletableFuture<T> wrap(CompletableFuture<T> future) {
    return FutureUtils.wrapFuture(future, pool);
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return wrap(rawAdmin.tableExists(tableName));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(boolean includeSysTables) {
    return wrap(rawAdmin.listTableDescriptors(includeSysTables));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(Pattern pattern,
      boolean includeSysTables) {
    return wrap(rawAdmin.listTableDescriptors(pattern, includeSysTables));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptors(List<TableName> tableNames) {
    return wrap(rawAdmin.listTableDescriptors(tableNames));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTableDescriptorsByNamespace(String name) {
    return wrap(rawAdmin.listTableDescriptorsByNamespace(name));
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames(boolean includeSysTables) {
    return wrap(rawAdmin.listTableNames(includeSysTables));
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames(Pattern pattern,
      boolean includeSysTables) {
    return wrap(rawAdmin.listTableNames(pattern, includeSysTables));
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNamesByNamespace(String name) {
    return wrap(rawAdmin.listTableNamesByNamespace(name));
  }

  @Override
  public CompletableFuture<TableDescriptor> getDescriptor(TableName tableName) {
    return wrap(rawAdmin.getDescriptor(tableName));
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc) {
    return wrap(rawAdmin.createTable(desc));
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    return wrap(rawAdmin.createTable(desc, startKey, endKey, numRegions));
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[][] splitKeys) {
    return wrap(rawAdmin.createTable(desc, splitKeys));
  }

  @Override
  public CompletableFuture<Void> modifyTable(TableDescriptor desc) {
    return wrap(rawAdmin.modifyTable(desc));
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    return wrap(rawAdmin.deleteTable(tableName));
  }

  @Override
  public CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits) {
    return wrap(rawAdmin.truncateTable(tableName, preserveSplits));
  }

  @Override
  public CompletableFuture<Void> enableTable(TableName tableName) {
    return wrap(rawAdmin.enableTable(tableName));
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName tableName) {
    return wrap(rawAdmin.disableTable(tableName));
  }

  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    return wrap(rawAdmin.isTableEnabled(tableName));
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    return wrap(rawAdmin.isTableDisabled(tableName));
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName) {
    return wrap(rawAdmin.isTableAvailable(tableName));
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName, byte[][] splitKeys) {
    return wrap(rawAdmin.isTableAvailable(tableName, splitKeys));
  }

  @Override
  public CompletableFuture<Void> addColumnFamily(TableName tableName,
      ColumnFamilyDescriptor columnFamily) {
    return wrap(rawAdmin.addColumnFamily(tableName, columnFamily));
  }

  @Override
  public CompletableFuture<Void> deleteColumnFamily(TableName tableName, byte[] columnFamily) {
    return wrap(rawAdmin.deleteColumnFamily(tableName, columnFamily));
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(TableName tableName,
      ColumnFamilyDescriptor columnFamily) {
    return wrap(rawAdmin.modifyColumnFamily(tableName, columnFamily));
  }

  @Override
  public CompletableFuture<Void> createNamespace(NamespaceDescriptor descriptor) {
    return wrap(rawAdmin.createNamespace(descriptor));
  }

  @Override
  public CompletableFuture<Void> modifyNamespace(NamespaceDescriptor descriptor) {
    return wrap(rawAdmin.modifyNamespace(descriptor));
  }

  @Override
  public CompletableFuture<Void> deleteNamespace(String name) {
    return wrap(rawAdmin.deleteNamespace(name));
  }

  @Override
  public CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(String name) {
    return wrap(rawAdmin.getNamespaceDescriptor(name));
  }

  @Override
  public CompletableFuture<List<String>> listNamespaces() {
    return wrap(rawAdmin.listNamespaces());
  }

  @Override
  public CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors() {
    return wrap(rawAdmin.listNamespaceDescriptors());
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getRegions(ServerName serverName) {
    return wrap(rawAdmin.getRegions(serverName));
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getRegions(TableName tableName) {
    return wrap(rawAdmin.getRegions(tableName));
  }

  @Override
  public CompletableFuture<Void> flush(TableName tableName) {
    return wrap(rawAdmin.flush(tableName));
  }

  @Override
  public CompletableFuture<Void> flush(TableName tableName, byte[] columnFamily) {
    return wrap(rawAdmin.flush(tableName, columnFamily));
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] regionName) {
    return wrap(rawAdmin.flushRegion(regionName));
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] regionName, byte[] columnFamily) {
    return wrap(rawAdmin.flushRegion(regionName, columnFamily));
  }

  @Override
  public CompletableFuture<Void> flushRegionServer(ServerName sn) {
    return wrap(rawAdmin.flushRegionServer(sn));
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName,
      CompactType compactType) {
    return wrap(rawAdmin.compact(tableName, compactType));
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName,
      byte[] columnFamily, CompactType compactType) {
    return wrap(rawAdmin.compact(tableName, columnFamily, compactType));
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName) {
    return wrap(rawAdmin.compactRegion(regionName));
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName, byte[] columnFamily) {
    return wrap(rawAdmin.compactRegion(regionName, columnFamily));
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, CompactType compactType) {
    return wrap(rawAdmin.majorCompact(tableName, compactType));
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, byte[] columnFamily,
      CompactType compactType) {
    return wrap(rawAdmin.majorCompact(tableName, columnFamily, compactType));
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName) {
    return wrap(rawAdmin.majorCompactRegion(regionName));
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName, byte[] columnFamily) {
    return wrap(rawAdmin.majorCompactRegion(regionName, columnFamily));
  }

  @Override
  public CompletableFuture<Void> compactRegionServer(ServerName serverName) {
    return wrap(rawAdmin.compactRegionServer(serverName));
  }

  @Override
  public CompletableFuture<Void> majorCompactRegionServer(ServerName serverName) {
    return wrap(rawAdmin.majorCompactRegionServer(serverName));
  }

  @Override
  public CompletableFuture<Boolean> mergeSwitch(boolean enabled, boolean drainMerges) {
    return wrap(rawAdmin.mergeSwitch(enabled, drainMerges));
  }

  @Override
  public CompletableFuture<Boolean> isMergeEnabled() {
    return wrap(rawAdmin.isMergeEnabled());
  }

  @Override
  public CompletableFuture<Boolean> splitSwitch(boolean enabled, boolean drainSplits) {
    return wrap(rawAdmin.splitSwitch(enabled, drainSplits));
  }

  @Override
  public CompletableFuture<Boolean> isSplitEnabled() {
    return wrap(rawAdmin.isSplitEnabled());
  }

  @Override
  public CompletableFuture<Void> mergeRegions(List<byte[]> nameOfRegionsToMerge, boolean forcible) {
    return wrap(rawAdmin.mergeRegions(nameOfRegionsToMerge, forcible));
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName) {
    return wrap(rawAdmin.split(tableName));
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName, byte[] splitPoint) {
    return wrap(rawAdmin.split(tableName, splitPoint));
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] regionName) {
    return wrap(rawAdmin.splitRegion(regionName));
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] regionName, byte[] splitPoint) {
    return wrap(rawAdmin.splitRegion(regionName, splitPoint));
  }

  @Override
  public CompletableFuture<Void> assign(byte[] regionName) {
    return wrap(rawAdmin.assign(regionName));
  }

  @Override
  public CompletableFuture<Void> unassign(byte[] regionName) {
    return wrap(rawAdmin.unassign(regionName));
  }

  @Override
  public CompletableFuture<Void> offline(byte[] regionName) {
    return wrap(rawAdmin.offline(regionName));
  }

  @Override
  public CompletableFuture<Void> move(byte[] regionName) {
    return wrap(rawAdmin.move(regionName));
  }

  @Override
  public CompletableFuture<Void> move(byte[] regionName, ServerName destServerName) {
    return wrap(rawAdmin.move(regionName, destServerName));
  }

  @Override
  public CompletableFuture<Void> setQuota(QuotaSettings quota) {
    return wrap(rawAdmin.setQuota(quota));
  }

  @Override
  public CompletableFuture<List<QuotaSettings>> getQuota(QuotaFilter filter) {
    return wrap(rawAdmin.getQuota(filter));
  }

  @Override
  public CompletableFuture<Void> addReplicationPeer(String peerId,
      ReplicationPeerConfig peerConfig, boolean enabled) {
    return wrap(rawAdmin.addReplicationPeer(peerId, peerConfig, enabled));
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeer(String peerId) {
    return wrap(rawAdmin.removeReplicationPeer(peerId));
  }

  @Override
  public CompletableFuture<Void> enableReplicationPeer(String peerId) {
    return wrap(rawAdmin.enableReplicationPeer(peerId));
  }

  @Override
  public CompletableFuture<Void> disableReplicationPeer(String peerId) {
    return wrap(rawAdmin.disableReplicationPeer(peerId));
  }

  @Override
  public CompletableFuture<ReplicationPeerConfig> getReplicationPeerConfig(String peerId) {
    return wrap(rawAdmin.getReplicationPeerConfig(peerId));
  }

  @Override
  public CompletableFuture<Void> updateReplicationPeerConfig(String peerId,
      ReplicationPeerConfig peerConfig) {
    return wrap(rawAdmin.updateReplicationPeerConfig(peerId, peerConfig));
  }

  @Override
  public CompletableFuture<Void> appendReplicationPeerTableCFs(String peerId,
      Map<TableName, List<String>> tableCfs) {
    return wrap(rawAdmin.appendReplicationPeerTableCFs(peerId, tableCfs));
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeerTableCFs(String peerId,
      Map<TableName, List<String>> tableCfs) {
    return wrap(rawAdmin.removeReplicationPeerTableCFs(peerId, tableCfs));
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers() {
    return wrap(rawAdmin.listReplicationPeers());
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(Pattern pattern) {
    return wrap(rawAdmin.listReplicationPeers(pattern));
  }

  @Override
  public CompletableFuture<List<TableCFs>> listReplicatedTableCFs() {
    return wrap(rawAdmin.listReplicatedTableCFs());
  }

  @Override
  public CompletableFuture<Void> enableTableReplication(TableName tableName) {
    return wrap(rawAdmin.enableTableReplication(tableName));
  }

  @Override
  public CompletableFuture<Void> disableTableReplication(TableName tableName) {
    return wrap(rawAdmin.disableTableReplication(tableName));
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription snapshot) {
    return wrap(rawAdmin.snapshot(snapshot));
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription snapshot) {
    return wrap(rawAdmin.isSnapshotFinished(snapshot));
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName) {
    return wrap(rawAdmin.restoreSnapshot(snapshotName));
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot,
      boolean restoreAcl) {
    return wrap(rawAdmin.restoreSnapshot(snapshotName, takeFailSafeSnapshot, restoreAcl));
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotName, TableName tableName,
      boolean restoreAcl) {
    return wrap(rawAdmin.cloneSnapshot(snapshotName, tableName, restoreAcl));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots() {
    return wrap(rawAdmin.listSnapshots());
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Pattern pattern) {
    return wrap(rawAdmin.listSnapshots(pattern));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern) {
    return wrap(rawAdmin.listTableSnapshots(tableNamePattern));
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    return wrap(rawAdmin.listTableSnapshots(tableNamePattern, snapshotNamePattern));
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String snapshotName) {
    return wrap(rawAdmin.deleteSnapshot(snapshotName));
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots() {
    return wrap(rawAdmin.deleteSnapshots());
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots(Pattern pattern) {
    return wrap(rawAdmin.deleteSnapshots(pattern));
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern) {
    return wrap(rawAdmin.deleteTableSnapshots(tableNamePattern));
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    return wrap(rawAdmin.deleteTableSnapshots(tableNamePattern, snapshotNamePattern));
  }

  @Override
  public CompletableFuture<Void> execProcedure(String signature, String instance,
      Map<String, String> props) {
    return wrap(rawAdmin.execProcedure(signature, instance, props));
  }

  @Override
  public CompletableFuture<byte[]> execProcedureWithReturn(String signature, String instance,
      Map<String, String> props) {
    return wrap(rawAdmin.execProcedureWithReturn(signature, instance, props));
  }

  @Override
  public CompletableFuture<Boolean> isProcedureFinished(String signature, String instance,
      Map<String, String> props) {
    return wrap(rawAdmin.isProcedureFinished(signature, instance, props));
  }

  @Override
  public CompletableFuture<Boolean> abortProcedure(long procId, boolean mayInterruptIfRunning) {
    return wrap(rawAdmin.abortProcedure(procId, mayInterruptIfRunning));
  }

  @Override
  public CompletableFuture<String> getProcedures() {
    return wrap(rawAdmin.getProcedures());
  }

  @Override
  public CompletableFuture<String> getLocks() {
    return wrap(rawAdmin.getLocks());
  }

  @Override
  public CompletableFuture<Void> decommissionRegionServers(List<ServerName> servers,
      boolean offload) {
    return wrap(rawAdmin.decommissionRegionServers(servers, offload));
  }

  @Override
  public CompletableFuture<List<ServerName>> listDecommissionedRegionServers() {
    return wrap(rawAdmin.listDecommissionedRegionServers());
  }

  @Override
  public CompletableFuture<Void> recommissionRegionServer(ServerName server,
      List<byte[]> encodedRegionNames) {
    return wrap(rawAdmin.recommissionRegionServer(server, encodedRegionNames));
  }

  @Override
  public CompletableFuture<ClusterMetrics> getClusterMetrics() {
    return getClusterMetrics(EnumSet.allOf(Option.class));
  }

  @Override
  public CompletableFuture<ClusterMetrics> getClusterMetrics(EnumSet<Option> options) {
    return wrap(rawAdmin.getClusterMetrics(options));
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return wrap(rawAdmin.shutdown());
  }

  @Override
  public CompletableFuture<Void> stopMaster() {
    return wrap(rawAdmin.stopMaster());
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName serverName) {
    return wrap(rawAdmin.stopRegionServer(serverName));
  }

  @Override
  public CompletableFuture<Void> updateConfiguration(ServerName serverName) {
    return wrap(rawAdmin.updateConfiguration(serverName));
  }

  @Override
  public CompletableFuture<Void> updateConfiguration() {
    return wrap(rawAdmin.updateConfiguration());
  }

  @Override
  public CompletableFuture<Void> rollWALWriter(ServerName serverName) {
    return wrap(rawAdmin.rollWALWriter(serverName));
  }

  @Override
  public CompletableFuture<Void> clearCompactionQueues(ServerName serverName, Set<String> queues) {
    return wrap(rawAdmin.clearCompactionQueues(serverName, queues));
  }

  @Override
  public CompletableFuture<List<SecurityCapability>> getSecurityCapabilities() {
    return wrap(rawAdmin.getSecurityCapabilities());
  }

  @Override
  public CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName serverName) {
    return wrap(rawAdmin.getRegionMetrics(serverName));
  }

  @Override
  public CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName serverName,
      TableName tableName) {
    return wrap(rawAdmin.getRegionMetrics(serverName, tableName));
  }

  @Override
  public CompletableFuture<Boolean> isMasterInMaintenanceMode() {
    return wrap(rawAdmin.isMasterInMaintenanceMode());
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(
      TableName tableName, CompactType compactType) {
    return wrap(rawAdmin.getCompactionState(tableName, compactType));
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionStateForRegion(byte[] regionName) {
    return wrap(rawAdmin.getCompactionStateForRegion(regionName));
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestamp(TableName tableName) {
    return wrap(rawAdmin.getLastMajorCompactionTimestamp(tableName));
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestampForRegion(
      byte[] regionName) {
    return wrap(rawAdmin.getLastMajorCompactionTimestampForRegion(regionName));
  }

  @Override
  public CompletableFuture<Boolean> balancerSwitch(boolean on, boolean drainRITs) {
    return wrap(rawAdmin.balancerSwitch(on, drainRITs));
  }

  @Override
  public CompletableFuture<Boolean> balance(boolean forcible) {
    return wrap(rawAdmin.balance(forcible));
  }

  @Override
  public CompletableFuture<Boolean> isBalancerEnabled() {
    return wrap(rawAdmin.isBalancerEnabled());
  }

  @Override
  public CompletableFuture<Boolean> normalizerSwitch(boolean on) {
    return wrap(rawAdmin.normalizerSwitch(on));
  }

  @Override
  public CompletableFuture<Boolean> isNormalizerEnabled() {
    return wrap(rawAdmin.isNormalizerEnabled());
  }

  @Override
  public CompletableFuture<Boolean> normalize(NormalizeTableFilterParams ntfp) {
    return wrap(rawAdmin.normalize(ntfp));
  }

  @Override
  public CompletableFuture<Boolean> cleanerChoreSwitch(boolean enabled) {
    return wrap(rawAdmin.cleanerChoreSwitch(enabled));
  }

  @Override
  public CompletableFuture<Boolean> isCleanerChoreEnabled() {
    return wrap(rawAdmin.isCleanerChoreEnabled());
  }

  @Override
  public CompletableFuture<Boolean> runCleanerChore() {
    return wrap(rawAdmin.runCleanerChore());
  }

  @Override
  public CompletableFuture<Boolean> catalogJanitorSwitch(boolean enabled) {
    return wrap(rawAdmin.catalogJanitorSwitch(enabled));
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorEnabled() {
    return wrap(rawAdmin.isCatalogJanitorEnabled());
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    return wrap(rawAdmin.runCatalogJanitor());
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable) {
    return wrap(rawAdmin.coprocessorService(stubMaker, callable));
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable, ServerName serverName) {
    return wrap(rawAdmin.coprocessorService(stubMaker, callable, serverName));
  }

  @Override
  public CompletableFuture<List<ServerName>> listDeadServers() {
    return wrap(rawAdmin.listDeadServers());
  }

  @Override
  public CompletableFuture<List<ServerName>> clearDeadServers(List<ServerName> servers) {
    return wrap(rawAdmin.clearDeadServers(servers));
  }

  @Override
  public CompletableFuture<CacheEvictionStats> clearBlockCache(TableName tableName) {
    return wrap(rawAdmin.clearBlockCache(tableName));
  }

  @Override
  public CompletableFuture<Void> cloneTableSchema(TableName tableName, TableName newTableName,
      boolean preserveSplits) {
    return wrap(rawAdmin.cloneTableSchema(tableName, newTableName, preserveSplits));
  }

  @Override
  public CompletableFuture<Map<ServerName, Boolean>> compactionSwitch(boolean switchState,
      List<String> serverNamesList) {
    return wrap(rawAdmin.compactionSwitch(switchState, serverNamesList));
  }

  @Override
  public CompletableFuture<Boolean> switchRpcThrottle(boolean enable) {
    return wrap(rawAdmin.switchRpcThrottle(enable));
  }

  @Override
  public CompletableFuture<Boolean> isRpcThrottleEnabled() {
    return wrap(rawAdmin.isRpcThrottleEnabled());
  }

  @Override
  public CompletableFuture<Boolean> exceedThrottleQuotaSwitch(boolean enable) {
    return wrap(rawAdmin.exceedThrottleQuotaSwitch(enable));
  }

  @Override
  public CompletableFuture<Map<TableName, Long>> getSpaceQuotaTableSizes() {
    return wrap(rawAdmin.getSpaceQuotaTableSizes());
  }

  @Override
  public CompletableFuture<Map<TableName, SpaceQuotaSnapshot>> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) {
    return wrap(rawAdmin.getRegionServerSpaceQuotaSnapshots(serverName));
  }

  @Override
  public CompletableFuture<SpaceQuotaSnapshot> getCurrentSpaceQuotaSnapshot(String namespace) {
    return wrap(rawAdmin.getCurrentSpaceQuotaSnapshot(namespace));
  }

  @Override
  public CompletableFuture<SpaceQuotaSnapshot> getCurrentSpaceQuotaSnapshot(TableName tableName) {
    return wrap(rawAdmin.getCurrentSpaceQuotaSnapshot(tableName));
  }

  @Override
  public CompletableFuture<Void> grant(UserPermission userPermission,
      boolean mergeExistingPermissions) {
    return wrap(rawAdmin.grant(userPermission, mergeExistingPermissions));
  }

  @Override
  public CompletableFuture<Void> revoke(UserPermission userPermission) {
    return wrap(rawAdmin.revoke(userPermission));
  }

  @Override
  public CompletableFuture<List<UserPermission>>
      getUserPermissions(GetUserPermissionsRequest getUserPermissionsRequest) {
    return wrap(rawAdmin.getUserPermissions(getUserPermissionsRequest));
  }

  @Override
  public CompletableFuture<List<Boolean>> hasUserPermissions(String userName,
      List<Permission> permissions) {
    return wrap(rawAdmin.hasUserPermissions(userName, permissions));
  }

  @Override
  public CompletableFuture<Boolean> snapshotCleanupSwitch(final boolean on,
      final boolean sync) {
    return wrap(rawAdmin.snapshotCleanupSwitch(on, sync));
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotCleanupEnabled() {
    return wrap(rawAdmin.isSnapshotCleanupEnabled());
  }

  @Override
  public CompletableFuture<List<Boolean>> clearSlowLogResponses(Set<ServerName> serverNames) {
    return wrap(rawAdmin.clearSlowLogResponses(serverNames));
  }

  @Override
  public CompletableFuture<List<LogEntry>> getLogEntries(Set<ServerName> serverNames,
      String logType, ServerType serverType, int limit,
      Map<String, Object> filterParams) {
    return wrap(rawAdmin.getLogEntries(serverNames, logType, serverType, limit, filterParams));
  }
}
