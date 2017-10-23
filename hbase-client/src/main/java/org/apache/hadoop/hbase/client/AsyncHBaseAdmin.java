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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RawAsyncTable.CoprocessorCallable;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.yetus.audience.InterfaceAudience;

import com.google.protobuf.RpcChannel;

/**
 * The implementation of AsyncAdmin.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class AsyncHBaseAdmin implements AsyncAdmin {

  private static final Log LOG = LogFactory.getLog(AsyncHBaseAdmin.class);

  private final RawAsyncHBaseAdmin rawAdmin;

  private final ExecutorService pool;

  AsyncHBaseAdmin(RawAsyncHBaseAdmin rawAdmin, ExecutorService pool) {
    this.rawAdmin = rawAdmin;
    this.pool = pool;
  }

  private <T> CompletableFuture<T> wrap(CompletableFuture<T> future) {
    CompletableFuture<T> asyncFuture = new CompletableFuture<>();
    future.whenCompleteAsync((r, e) -> {
      if (e != null) {
        asyncFuture.completeExceptionally(e);
      } else {
        asyncFuture.complete(r);
      }
    }, pool);
    return asyncFuture;
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return wrap(rawAdmin.tableExists(tableName));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTables(boolean includeSysTables) {
    return wrap(rawAdmin.listTables(includeSysTables));
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTables(Pattern pattern,
      boolean includeSysTables) {
    return wrap(rawAdmin.listTables(pattern, includeSysTables));
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
  public CompletableFuture<TableDescriptor> getTableDescriptor(TableName tableName) {
    return wrap(rawAdmin.getTableDescriptor(tableName));
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
  public CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors() {
    return wrap(rawAdmin.listNamespaceDescriptors());
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getOnlineRegions(ServerName serverName) {
    return wrap(rawAdmin.getOnlineRegions(serverName));
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getTableRegions(TableName tableName) {
    return wrap(rawAdmin.getTableRegions(tableName));
  }

  @Override
  public CompletableFuture<Void> flush(TableName tableName) {
    return wrap(rawAdmin.flush(tableName));
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] regionName) {
    return wrap(rawAdmin.flushRegion(regionName));
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName) {
    return wrap(rawAdmin.compact(tableName));
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName, byte[] columnFamily) {
    return wrap(rawAdmin.compact(tableName, columnFamily));
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
  public CompletableFuture<Void> majorCompact(TableName tableName) {
    return wrap(rawAdmin.majorCompact(tableName));
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, byte[] columnFamily) {
    return wrap(rawAdmin.majorCompact(tableName, columnFamily));
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
  public CompletableFuture<Boolean> setMergeOn(boolean on) {
    return wrap(rawAdmin.setMergeOn(on));
  }

  @Override
  public CompletableFuture<Boolean> isMergeOn() {
    return wrap(rawAdmin.isMergeOn());
  }

  @Override
  public CompletableFuture<Boolean> setSplitOn(boolean on) {
    return wrap(rawAdmin.setSplitOn(on));
  }

  @Override
  public CompletableFuture<Boolean> isSplitOn() {
    return wrap(rawAdmin.isSplitOn());
  }

  @Override
  public CompletableFuture<Void> mergeRegions(byte[] nameOfRegionA, byte[] nameOfRegionB,
      boolean forcible) {
    return wrap(rawAdmin.mergeRegions(nameOfRegionA, nameOfRegionB, forcible));
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
  public CompletableFuture<Void> unassign(byte[] regionName, boolean forcible) {
    return wrap(rawAdmin.unassign(regionName, forcible));
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
  public CompletableFuture<Void>
      addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig) {
    return wrap(rawAdmin.addReplicationPeer(peerId, peerConfig));
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
      Map<TableName, ? extends Collection<String>> tableCfs) {
    return wrap(rawAdmin.appendReplicationPeerTableCFs(peerId, tableCfs));
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeerTableCFs(String peerId,
      Map<TableName, ? extends Collection<String>> tableCfs) {
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
  public CompletableFuture<Void> restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot) {
    return wrap(rawAdmin.restoreSnapshot(snapshotName, takeFailSafeSnapshot));
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotName, TableName tableName) {
    return wrap(rawAdmin.cloneSnapshot(snapshotName, tableName));
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
  public CompletableFuture<byte[]> execProcedureWithRet(String signature, String instance,
      Map<String, String> props) {
    return wrap(rawAdmin.execProcedureWithRet(signature, instance, props));
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
  public CompletableFuture<ClusterStatus> getClusterStatus() {
    return getClusterStatus(EnumSet.allOf(Option.class));
  }

  @Override
  public CompletableFuture<ClusterStatus> getClusterStatus(EnumSet<Option> options) {
    return wrap(rawAdmin.getClusterStatus(options));
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
  public CompletableFuture<List<RegionLoad>> getRegionLoads(ServerName serverName) {
    return wrap(rawAdmin.getRegionLoads(serverName));
  }

  @Override
  public CompletableFuture<List<RegionLoad>> getRegionLoads(ServerName serverName,
      TableName tableName) {
    return wrap(rawAdmin.getRegionLoads(serverName, tableName));
  }

  @Override
  public CompletableFuture<Boolean> isMasterInMaintenanceMode() {
    return wrap(rawAdmin.isMasterInMaintenanceMode());
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(TableName tableName) {
    return wrap(rawAdmin.getCompactionState(tableName));
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
  public CompletableFuture<Boolean> setBalancerOn(boolean on) {
    return wrap(rawAdmin.setBalancerOn(on));
  }

  @Override
  public CompletableFuture<Boolean> balance(boolean forcible) {
    return wrap(rawAdmin.balance(forcible));
  }

  @Override
  public CompletableFuture<Boolean> isBalancerOn() {
    return wrap(rawAdmin.isBalancerOn());
  }

  @Override
  public CompletableFuture<Boolean> setNormalizerOn(boolean on) {
    return wrap(rawAdmin.setNormalizerOn(on));
  }

  @Override
  public CompletableFuture<Boolean> isNormalizerOn() {
    return wrap(rawAdmin.isNormalizerOn());
  }

  @Override
  public CompletableFuture<Boolean> normalize() {
    return wrap(rawAdmin.normalize());
  }

  @Override
  public CompletableFuture<Boolean> setCleanerChoreOn(boolean enabled) {
    return wrap(rawAdmin.setCleanerChoreOn(enabled));
  }

  @Override
  public CompletableFuture<Boolean> isCleanerChoreOn() {
    return wrap(rawAdmin.isCleanerChoreOn());
  }

  @Override
  public CompletableFuture<Boolean> runCleanerChore() {
    return wrap(rawAdmin.runCleanerChore());
  }

  @Override
  public CompletableFuture<Boolean> setCatalogJanitorOn(boolean enabled) {
    return wrap(rawAdmin.setCatalogJanitorOn(enabled));
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorOn() {
    return wrap(rawAdmin.isCatalogJanitorOn());
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    return wrap(rawAdmin.runCatalogJanitor());
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      CoprocessorCallable<S, R> callable) {
    return wrap(rawAdmin.coprocessorService(stubMaker, callable));
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      CoprocessorCallable<S, R> callable, ServerName serverName) {
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
}