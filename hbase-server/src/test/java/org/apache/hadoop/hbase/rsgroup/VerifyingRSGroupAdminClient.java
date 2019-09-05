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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
public class VerifyingRSGroupAdminClient implements Admin {
  private Connection conn;
  private ZKWatcher zkw;
  private Admin wrapped;

  public VerifyingRSGroupAdminClient(Admin admin, Configuration conf)
      throws IOException {
    wrapped = admin;
    conn = ConnectionFactory.createConnection(conf);
    zkw = new ZKWatcher(conf, this.getClass().getSimpleName(), null);
  }

  @Override
  public void addRSGroup(String groupName) throws IOException {
    wrapped.addRSGroup(groupName);
    verify();
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
  public org.apache.hadoop.hbase.client.Connection getConnection() {
    return null;
  }

  @Override
  public boolean tableExists(org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {
    return false;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.TableDescriptor> listTableDescriptors()
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.TableDescriptor> listTableDescriptors(
      java.util.regex.Pattern pattern, boolean includeSysTables) throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.TableName[] listTableNames() throws java.io.IOException {
    return new org.apache.hadoop.hbase.TableName[0];
  }

  @Override
  public org.apache.hadoop.hbase.TableName[] listTableNames(java.util.regex.Pattern pattern,
      boolean includeSysTables) throws java.io.IOException {
    return new org.apache.hadoop.hbase.TableName[0];
  }

  @Override
  public org.apache.hadoop.hbase.client.TableDescriptor getDescriptor(
      org.apache.hadoop.hbase.TableName tableName)
      throws org.apache.hadoop.hbase.TableNotFoundException, java.io.IOException {
    return null;
  }

  @Override
  public void createTable(org.apache.hadoop.hbase.client.TableDescriptor desc, byte[] startKey,
      byte[] endKey, int numRegions) throws java.io.IOException {

  }

  @Override
  public java.util.concurrent.Future<Void> createTableAsync(
      org.apache.hadoop.hbase.client.TableDescriptor desc) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> createTableAsync(
      org.apache.hadoop.hbase.client.TableDescriptor desc, byte[][] splitKeys)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> deleteTableAsync(
      org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> truncateTableAsync(
      org.apache.hadoop.hbase.TableName tableName, boolean preserveSplits)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> enableTableAsync(
      org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> disableTableAsync(
      org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {
    return null;
  }

  @Override
  public boolean isTableEnabled(org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isTableDisabled(org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isTableAvailable(org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {
    return false;
  }

  @Override
  public java.util.concurrent.Future<Void> addColumnFamilyAsync(
      org.apache.hadoop.hbase.TableName tableName,
      org.apache.hadoop.hbase.client.ColumnFamilyDescriptor columnFamily)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> deleteColumnFamilyAsync(
      org.apache.hadoop.hbase.TableName tableName, byte[] columnFamily) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> modifyColumnFamilyAsync(
      org.apache.hadoop.hbase.TableName tableName,
      org.apache.hadoop.hbase.client.ColumnFamilyDescriptor columnFamily)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.RegionInfo> getRegions(
      org.apache.hadoop.hbase.ServerName serverName) throws java.io.IOException {
    return null;
  }

  @Override
  public void flush(org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {

  }

  @Override
  public void flushRegion(byte[] regionName) throws java.io.IOException {

  }

  @Override
  public void flushRegionServer(org.apache.hadoop.hbase.ServerName serverName)
      throws java.io.IOException {

  }

  @Override
  public void compact(org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {

  }

  @Override
  public void compactRegion(byte[] regionName) throws java.io.IOException {

  }

  @Override
  public void compact(org.apache.hadoop.hbase.TableName tableName, byte[] columnFamily)
      throws java.io.IOException {

  }

  @Override
  public void compactRegion(byte[] regionName, byte[] columnFamily) throws java.io.IOException {

  }

  @Override
  public void compact(org.apache.hadoop.hbase.TableName tableName,
      org.apache.hadoop.hbase.client.CompactType compactType)
      throws java.io.IOException, InterruptedException {

  }

  @Override
  public void compact(org.apache.hadoop.hbase.TableName tableName, byte[] columnFamily,
      org.apache.hadoop.hbase.client.CompactType compactType)
      throws java.io.IOException, InterruptedException {

  }

  @Override
  public void majorCompact(org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {

  }

  @Override
  public void majorCompactRegion(byte[] regionName) throws java.io.IOException {

  }

  @Override
  public void majorCompact(org.apache.hadoop.hbase.TableName tableName, byte[] columnFamily)
      throws java.io.IOException {

  }

  @Override
  public void majorCompactRegion(byte[] regionName, byte[] columnFamily)
      throws java.io.IOException {

  }

  @Override
  public void majorCompact(org.apache.hadoop.hbase.TableName tableName,
      org.apache.hadoop.hbase.client.CompactType compactType)
      throws java.io.IOException, InterruptedException {

  }

  @Override
  public void majorCompact(org.apache.hadoop.hbase.TableName tableName, byte[] columnFamily,
      org.apache.hadoop.hbase.client.CompactType compactType)
      throws java.io.IOException, InterruptedException {

  }

  @Override
  public java.util.Map<org.apache.hadoop.hbase.ServerName, Boolean> compactionSwitch(
      boolean switchState, java.util.List<String> serverNamesList) throws java.io.IOException {
    return null;
  }

  @Override
  public void compactRegionServer(org.apache.hadoop.hbase.ServerName serverName)
      throws java.io.IOException {

  }

  @Override
  public void majorCompactRegionServer(org.apache.hadoop.hbase.ServerName serverName)
      throws java.io.IOException {

  }

  @Override
  public void move(byte[] encodedRegionName) throws java.io.IOException {

  }

  @Override
  public void move(byte[] encodedRegionName, org.apache.hadoop.hbase.ServerName destServerName)
      throws java.io.IOException {

  }

  @Override
  public void assign(byte[] regionName) throws java.io.IOException {

  }

  @Override
  public void unassign(byte[] regionName, boolean force) throws java.io.IOException {

  }

  @Override
  public void offline(byte[] regionName) throws java.io.IOException {

  }

  @Override
  public boolean balancerSwitch(boolean onOrOff, boolean synchronous) throws java.io.IOException {
    return false;
  }

  @Override
  public boolean balance() throws java.io.IOException {
    return false;
  }

  @Override
  public boolean balance(boolean force) throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isBalancerEnabled() throws java.io.IOException {
    return false;
  }

  @Override
  public org.apache.hadoop.hbase.CacheEvictionStats clearBlockCache(
      org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {
    return null;
  }

  @Override
  public boolean normalize() throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isNormalizerEnabled() throws java.io.IOException {
    return false;
  }

  @Override
  public boolean normalizerSwitch(boolean on) throws java.io.IOException {
    return false;
  }

  @Override
  public boolean catalogJanitorSwitch(boolean onOrOff) throws java.io.IOException {
    return false;
  }

  @Override
  public int runCatalogJanitor() throws java.io.IOException {
    return 0;
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws java.io.IOException {
    return false;
  }

  @Override
  public boolean cleanerChoreSwitch(boolean onOrOff) throws java.io.IOException {
    return false;
  }

  @Override
  public boolean runCleanerChore() throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isCleanerChoreEnabled() throws java.io.IOException {
    return false;
  }

  @Override
  public java.util.concurrent.Future<Void> mergeRegionsAsync(byte[][] nameofRegionsToMerge,
      boolean forcible) throws java.io.IOException {
    return null;
  }

  @Override
  public void split(org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {

  }

  @Override
  public void split(org.apache.hadoop.hbase.TableName tableName, byte[] splitPoint)
      throws java.io.IOException {

  }

  @Override
  public java.util.concurrent.Future<Void> splitRegionAsync(byte[] regionName)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> splitRegionAsync(byte[] regionName, byte[] splitPoint)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> modifyTableAsync(
      org.apache.hadoop.hbase.client.TableDescriptor td) throws java.io.IOException {
    return null;
  }

  @Override
  public void shutdown() throws java.io.IOException {

  }

  @Override
  public void stopMaster() throws java.io.IOException {

  }

  @Override
  public boolean isMasterInMaintenanceMode() throws java.io.IOException {
    return false;
  }

  @Override
  public void stopRegionServer(String hostnamePort) throws java.io.IOException {

  }

  @Override
  public org.apache.hadoop.hbase.ClusterMetrics getClusterMetrics(
      java.util.EnumSet<org.apache.hadoop.hbase.ClusterMetrics.Option> options)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.RegionMetrics> getRegionMetrics(
      org.apache.hadoop.hbase.ServerName serverName) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.RegionMetrics> getRegionMetrics(
      org.apache.hadoop.hbase.ServerName serverName, org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.conf.Configuration getConfiguration() {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> createNamespaceAsync(
      org.apache.hadoop.hbase.NamespaceDescriptor descriptor) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> modifyNamespaceAsync(
      org.apache.hadoop.hbase.NamespaceDescriptor descriptor) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> deleteNamespaceAsync(String name)
      throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.NamespaceDescriptor getNamespaceDescriptor(String name)
      throws org.apache.hadoop.hbase.NamespaceNotFoundException, java.io.IOException {
    return null;
  }

  @Override
  public String[] listNamespaces() throws java.io.IOException {
    return new String[0];
  }

  @Override
  public org.apache.hadoop.hbase.NamespaceDescriptor[] listNamespaceDescriptors()
      throws java.io.IOException {
    return new org.apache.hadoop.hbase.NamespaceDescriptor[0];
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.TableDescriptor> listTableDescriptorsByNamespace(
      byte[] name) throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.TableName[] listTableNamesByNamespace(String name)
      throws java.io.IOException {
    return new org.apache.hadoop.hbase.TableName[0];
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.RegionInfo> getRegions(
      org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.TableDescriptor> listTableDescriptors(
      java.util.List<org.apache.hadoop.hbase.TableName> tableNames) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Boolean> abortProcedureAsync(long procId,
      boolean mayInterruptIfRunning) throws java.io.IOException {
    return null;
  }

  @Override
  public String getProcedures() throws java.io.IOException {
    return null;
  }

  @Override
  public String getLocks() throws java.io.IOException {
    return null;
  }

  @Override
  public void rollWALWriter(org.apache.hadoop.hbase.ServerName serverName)
      throws java.io.IOException, org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException {

  }

  @Override
  public org.apache.hadoop.hbase.client.CompactionState getCompactionState(
      org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.client.CompactionState getCompactionState(
      org.apache.hadoop.hbase.TableName tableName,
      org.apache.hadoop.hbase.client.CompactType compactType) throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.client.CompactionState getCompactionStateForRegion(
      byte[] regionName) throws java.io.IOException {
    return null;
  }

  @Override
  public long getLastMajorCompactionTimestamp(org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {
    return 0;
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName)
      throws java.io.IOException {
    return 0;
  }

  @Override
  public void snapshot(org.apache.hadoop.hbase.client.SnapshotDescription snapshot)
      throws java.io.IOException, org.apache.hadoop.hbase.snapshot.SnapshotCreationException,
      IllegalArgumentException {

  }

  @Override
  public java.util.concurrent.Future<Void> snapshotAsync(
      org.apache.hadoop.hbase.client.SnapshotDescription snapshot)
      throws java.io.IOException, org.apache.hadoop.hbase.snapshot.SnapshotCreationException {
    return null;
  }

  @Override
  public boolean isSnapshotFinished(org.apache.hadoop.hbase.client.SnapshotDescription snapshot)
      throws java.io.IOException, org.apache.hadoop.hbase.snapshot.HBaseSnapshotException,
      org.apache.hadoop.hbase.snapshot.UnknownSnapshotException {
    return false;
  }

  @Override
  public void restoreSnapshot(String snapshotName)
      throws java.io.IOException, org.apache.hadoop.hbase.snapshot.RestoreSnapshotException {

  }

  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot, boolean restoreAcl)
      throws java.io.IOException, org.apache.hadoop.hbase.snapshot.RestoreSnapshotException {

  }

  @Override
  public java.util.concurrent.Future<Void> cloneSnapshotAsync(String snapshotName,
      org.apache.hadoop.hbase.TableName tableName, boolean restoreAcl)
      throws java.io.IOException, org.apache.hadoop.hbase.TableExistsException,
      org.apache.hadoop.hbase.snapshot.RestoreSnapshotException {
    return null;
  }

  @Override
  public void execProcedure(String signature, String instance, java.util.Map<String, String> props)
      throws java.io.IOException {

  }

  @Override
  public byte[] execProcedureWithReturn(String signature, String instance,
      java.util.Map<String, String> props) throws java.io.IOException {
    return new byte[0];
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance,
      java.util.Map<String, String> props) throws java.io.IOException {
    return false;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.SnapshotDescription> listSnapshots()
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.SnapshotDescription> listSnapshots(
      java.util.regex.Pattern pattern) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.SnapshotDescription> listTableSnapshots(
      java.util.regex.Pattern tableNamePattern, java.util.regex.Pattern snapshotNamePattern)
      throws java.io.IOException {
    return null;
  }

  @Override
  public void deleteSnapshot(String snapshotName) throws java.io.IOException {

  }

  @Override
  public void deleteSnapshots(java.util.regex.Pattern pattern) throws java.io.IOException {

  }

  @Override
  public void deleteTableSnapshots(java.util.regex.Pattern tableNamePattern,
      java.util.regex.Pattern snapshotNamePattern) throws java.io.IOException {

  }

  @Override
  public void setQuota(org.apache.hadoop.hbase.quotas.QuotaSettings quota)
      throws java.io.IOException {

  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.quotas.QuotaSettings> getQuota(
      org.apache.hadoop.hbase.quotas.QuotaFilter filter) throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel coprocessorService() {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel coprocessorService(
      org.apache.hadoop.hbase.ServerName serverName) {
    return null;
  }

  @Override
  public void updateConfiguration(org.apache.hadoop.hbase.ServerName server)
      throws java.io.IOException {

  }

  @Override
  public void updateConfiguration() throws java.io.IOException {

  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.security.SecurityCapability> getSecurityCapabilities()
      throws java.io.IOException {
    return null;
  }

  @Override
  public boolean splitSwitch(boolean enabled, boolean synchronous) throws java.io.IOException {
    return false;
  }

  @Override
  public boolean mergeSwitch(boolean enabled, boolean synchronous) throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isSplitEnabled() throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isMergeEnabled() throws java.io.IOException {
    return false;
  }

  @Override
  public java.util.concurrent.Future<Void> addReplicationPeerAsync(String peerId,
      org.apache.hadoop.hbase.replication.ReplicationPeerConfig peerConfig, boolean enabled)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> removeReplicationPeerAsync(String peerId)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> enableReplicationPeerAsync(String peerId)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> disableReplicationPeerAsync(String peerId)
      throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.replication.ReplicationPeerConfig getReplicationPeerConfig(
      String peerId) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> updateReplicationPeerConfigAsync(String peerId,
      org.apache.hadoop.hbase.replication.ReplicationPeerConfig peerConfig)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.replication.ReplicationPeerDescription> listReplicationPeers()
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.replication.ReplicationPeerDescription> listReplicationPeers(
      java.util.regex.Pattern pattern) throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.concurrent.Future<Void> transitReplicationPeerSyncReplicationStateAsync(
      String peerId, org.apache.hadoop.hbase.replication.SyncReplicationState state)
      throws java.io.IOException {
    return null;
  }

  @Override
  public void decommissionRegionServers(java.util.List<org.apache.hadoop.hbase.ServerName> servers,
      boolean offload) throws java.io.IOException {

  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.ServerName> listDecommissionedRegionServers()
      throws java.io.IOException {
    return null;
  }

  @Override
  public void recommissionRegionServer(org.apache.hadoop.hbase.ServerName server,
      java.util.List<byte[]> encodedRegionNames) throws java.io.IOException {

  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.client.replication.TableCFs> listReplicatedTableCFs()
      throws java.io.IOException {
    return null;
  }

  @Override
  public void enableTableReplication(org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {

  }

  @Override
  public void disableTableReplication(org.apache.hadoop.hbase.TableName tableName)
      throws java.io.IOException {

  }

  @Override
  public void clearCompactionQueues(org.apache.hadoop.hbase.ServerName serverName,
      java.util.Set<String> queues) throws java.io.IOException, InterruptedException {

  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.ServerName> clearDeadServers(
      java.util.List<org.apache.hadoop.hbase.ServerName> servers) throws java.io.IOException {
    return null;
  }

  @Override
  public void cloneTableSchema(org.apache.hadoop.hbase.TableName tableName,
      org.apache.hadoop.hbase.TableName newTableName, boolean preserveSplits)
      throws java.io.IOException {

  }

  @Override
  public boolean switchRpcThrottle(boolean enable) throws java.io.IOException {
    return false;
  }

  @Override
  public boolean isRpcThrottleEnabled() throws java.io.IOException {
    return false;
  }

  @Override
  public boolean exceedThrottleQuotaSwitch(boolean enable) throws java.io.IOException {
    return false;
  }

  @Override
  public java.util.Map<org.apache.hadoop.hbase.TableName, Long> getSpaceQuotaTableSizes()
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.Map<org.apache.hadoop.hbase.TableName, ? extends org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView> getRegionServerSpaceQuotaSnapshots(
      org.apache.hadoop.hbase.ServerName serverName) throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(
      String namespace) throws java.io.IOException {
    return null;
  }

  @Override
  public org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(
      org.apache.hadoop.hbase.TableName tableName) throws java.io.IOException {
    return null;
  }

  @Override
  public void grant(org.apache.hadoop.hbase.security.access.UserPermission userPermission,
      boolean mergeExistingPermissions) throws java.io.IOException {

  }

  @Override
  public void revoke(org.apache.hadoop.hbase.security.access.UserPermission userPermission)
      throws java.io.IOException {

  }

  @Override
  public java.util.List<org.apache.hadoop.hbase.security.access.UserPermission> getUserPermissions(
      org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest getUserPermissionsRequest)
      throws java.io.IOException {
    return null;
  }

  @Override
  public java.util.List<Boolean> hasUserPermissions(String userName,
      java.util.List<org.apache.hadoop.hbase.security.access.Permission> permissions)
      throws java.io.IOException {
    return null;
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    return wrapped.getRSGroupInfo(groupName);
  }

  @Override
  public void moveServers(Set<Address> servers, String targetGroup) throws IOException {
    wrapped.moveServers(servers, targetGroup);
    verify();
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    wrapped.removeRSGroup(name);
    verify();
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    return wrapped.balanceRSGroup(groupName);
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return wrapped.listRSGroups();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    return wrapped.getRSGroupOfServer(hostPort);
  }

  @Override
  public void removeServers(Set<Address> servers) throws IOException {
    wrapped.removeServers(servers);
    verify();
  }

  public void verify() throws IOException {
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
        for(Address address : rsGroupInfo.getServers()){
          lives.remove(address);
        }
      }
    }
    SortedSet<TableName> tables = Sets.newTreeSet();
    for (TableDescriptor td : conn.getAdmin().listTableDescriptors(Pattern.compile(".*"),
        true)){
      String groupName = td.getRegionServerGroup().orElse(RSGroupInfo.DEFAULT_GROUP);
      if (groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
        tables.add(td.getTableName());
      }
    }

    groupMap.put(RSGroupInfo.DEFAULT_GROUP,
        new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, lives, tables));
    assertEquals(Sets.newHashSet(groupMap.values()), Sets.newHashSet(wrapped.listRSGroups()));
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
}
