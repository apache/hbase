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

import static org.apache.hadoop.hbase.client.ConnectionUtils.setCoprocessorError;
import static org.apache.hadoop.hbase.util.FutureUtils.get;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

/**
 * The {@link Admin} implementation which is based on an {@link AsyncAdmin}.
 */
@InterfaceAudience.Private
class AdminOverAsyncAdmin implements Admin {

  private static final Logger LOG = LoggerFactory.getLogger(AdminOverAsyncAdmin.class);

  private volatile boolean aborted = false;

  private final Connection conn;

  private final RawAsyncHBaseAdmin admin;

  private final int operationTimeout;

  private final int syncWaitTimeout;

  public AdminOverAsyncAdmin(Connection conn, RawAsyncHBaseAdmin admin) {
    this.conn = conn;
    this.admin = admin;
    this.operationTimeout = conn.getConfiguration().getInt(
      HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.syncWaitTimeout =
      conn.getConfiguration().getInt("hbase.client.sync.wait.timeout.msec", 10 * 60000); // 10min
  }

  @Override
  public int getOperationTimeout() {
    return operationTimeout;
  }

  @Override
  public int getSyncWaitTimeout() {
    return syncWaitTimeout;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.warn("Aborting becasue of {}", why, e);
    this.aborted = true;
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }

  @Override
  public Connection getConnection() {
    return conn;
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    return get(admin.tableExists(tableName));
  }

  @Override
  public List<TableDescriptor> listTableDescriptors() throws IOException {
    return get(admin.listTableDescriptors());
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(boolean includeSysTables)
      throws IOException {
    return get(admin.listTableDescriptors(includeSysTables));
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
      throws IOException {
    return get(admin.listTableDescriptors(pattern, includeSysTables));
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return get(admin.listTableNames()).toArray(new TableName[0]);
  }

  @Override
  public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
    return get(admin.listTableNames(pattern, includeSysTables)).toArray(new TableName[0]);
  }

  @Override
  public TableDescriptor getDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    return get(admin.getDescriptor(tableName));
  }

  @Override
  public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    get(admin.createTable(desc, startKey, endKey, numRegions));
  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor desc) throws IOException {
    return admin.createTable(desc);
  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys)
      throws IOException {
    return admin.createTable(desc, splitKeys);
  }

  @Override
  public Future<Void> deleteTableAsync(TableName tableName) throws IOException {
    return admin.deleteTable(tableName);
  }

  @Override
  public Future<Void> truncateTableAsync(TableName tableName, boolean preserveSplits)
      throws IOException {
    return admin.truncateTable(tableName, preserveSplits);
  }

  @Override
  public Future<Void> enableTableAsync(TableName tableName) throws IOException {
    return admin.enableTable(tableName);
  }

  @Override
  public Future<Void> disableTableAsync(TableName tableName) throws IOException {
    return admin.disableTable(tableName);
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return get(admin.isTableEnabled(tableName));
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return get(admin.isTableDisabled(tableName));
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return get(admin.isTableAvailable(tableName));
  }

  @Override
  public Future<Void> addColumnFamilyAsync(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    return admin.addColumnFamily(tableName, columnFamily);
  }

  @Override
  public Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] columnFamily)
      throws IOException {
    return admin.deleteColumnFamily(tableName, columnFamily);
  }

  @Override
  public Future<Void> modifyColumnFamilyAsync(TableName tableName,
    ColumnFamilyDescriptor columnFamily) throws IOException {
    return admin.modifyColumnFamily(tableName, columnFamily);
  }

  @Override
  public Future<Void> modifyColumnFamilyStoreFileTrackerAsync(TableName tableName, byte[] family,
    String dstSFT) throws IOException {
    return admin.modifyColumnFamilyStoreFileTracker(tableName, family, dstSFT);
  }

  @Override
  public List<RegionInfo> getRegions(ServerName serverName) throws IOException {
    return get(admin.getRegions(serverName));
  }

  @Override
  public void flush(TableName tableName) throws IOException {
    get(admin.flush(tableName));
  }

  @Override
  public void flush(TableName tableName, byte[] columnFamily) throws IOException {
    get(admin.flush(tableName, columnFamily));
  }

  @Override
  public void flushRegion(byte[] regionName) throws IOException {
    get(admin.flushRegion(regionName));
  }

  @Override
  public void flushRegion(byte[] regionName, byte[] columnFamily) throws IOException {
    get(admin.flushRegion(regionName, columnFamily));
  }

  @Override
  public void flushRegionServer(ServerName serverName) throws IOException {
    get(admin.flushRegionServer(serverName));
  }

  @Override
  public void compact(TableName tableName) throws IOException {
    get(admin.compact(tableName));
  }

  @Override
  public void compactRegion(byte[] regionName) throws IOException {
    get(admin.compactRegion(regionName));
  }

  @Override
  public void compact(TableName tableName, byte[] columnFamily) throws IOException {
    get(admin.compact(tableName, columnFamily));
  }

  @Override
  public void compactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
    get(admin.compactRegion(regionName, columnFamily));
  }

  @Override
  public void compact(TableName tableName, CompactType compactType)
      throws IOException, InterruptedException {
    get(admin.compact(tableName, compactType));
  }

  @Override
  public void compact(TableName tableName, byte[] columnFamily, CompactType compactType)
      throws IOException, InterruptedException {
    get(admin.compact(tableName, columnFamily, compactType));
  }

  @Override
  public void majorCompact(TableName tableName) throws IOException {
    get(admin.majorCompact(tableName));
  }

  @Override
  public void majorCompactRegion(byte[] regionName) throws IOException {
    get(admin.majorCompactRegion(regionName));
  }

  @Override
  public void majorCompact(TableName tableName, byte[] columnFamily) throws IOException {
    get(admin.majorCompact(tableName, columnFamily));
  }

  @Override
  public void majorCompactRegion(byte[] regionName, byte[] columnFamily) throws IOException {
    get(admin.majorCompactRegion(regionName, columnFamily));
  }

  @Override
  public void majorCompact(TableName tableName, CompactType compactType)
      throws IOException, InterruptedException {
    get(admin.majorCompact(tableName, compactType));
  }

  @Override
  public void majorCompact(TableName tableName, byte[] columnFamily, CompactType compactType)
      throws IOException, InterruptedException {
    get(admin.majorCompact(tableName, columnFamily, compactType));
  }

  @Override
  public Map<ServerName, Boolean> compactionSwitch(boolean switchState,
      List<String> serverNamesList) throws IOException {
    return get(admin.compactionSwitch(switchState, serverNamesList));
  }

  @Override
  public void compactRegionServer(ServerName serverName) throws IOException {
    get(admin.compactRegionServer(serverName));
  }

  @Override
  public void majorCompactRegionServer(ServerName serverName) throws IOException {
    get(admin.majorCompactRegionServer(serverName));
  }

  @Override
  public void move(byte[] encodedRegionName) throws IOException {
    get(admin.move(encodedRegionName));
  }

  @Override
  public void move(byte[] encodedRegionName, ServerName destServerName) throws IOException {
    get(admin.move(encodedRegionName, destServerName));
  }

  @Override
  public void assign(byte[] regionName) throws IOException {
    get(admin.assign(regionName));
  }

  @Override
  public void unassign(byte[] regionName) throws IOException {
    get(admin.unassign(regionName));
  }

  @Override
  public void offline(byte[] regionName) throws IOException {
    get(admin.offline(regionName));
  }

  @Override
  public boolean balancerSwitch(boolean onOrOff, boolean synchronous) throws IOException {
    return get(admin.balancerSwitch(onOrOff, synchronous));
  }


  public BalanceResponse balance(BalanceRequest request) throws IOException {
    return get(admin.balance(request));
  }

  @Override
  public boolean balance() throws IOException {
    return get(admin.balance());
  }

  @Override
  public boolean balance(boolean force) throws IOException {
    return get(admin.balance(force));
  }

  @Override
  public boolean isBalancerEnabled() throws IOException {
    return get(admin.isBalancerEnabled());
  }

  @Override
  public CacheEvictionStats clearBlockCache(TableName tableName) throws IOException {
    return get(admin.clearBlockCache(tableName));
  }

  @Override
  public boolean normalize(NormalizeTableFilterParams ntfp) throws IOException {
    return get(admin.normalize(ntfp));
  }

  @Override
  public boolean isNormalizerEnabled() throws IOException {
    return get(admin.isNormalizerEnabled());
  }

  @Override
  public boolean normalizerSwitch(boolean on) throws IOException {
    return get(admin.normalizerSwitch(on));
  }

  @Override
  public boolean catalogJanitorSwitch(boolean onOrOff) throws IOException {
    return get(admin.catalogJanitorSwitch(onOrOff));
  }

  @Override
  public int runCatalogJanitor() throws IOException {
    return get(admin.runCatalogJanitor());
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws IOException {
    return get(admin.isCatalogJanitorEnabled());
  }

  @Override
  public boolean cleanerChoreSwitch(boolean onOrOff) throws IOException {
    return get(admin.cleanerChoreSwitch(onOrOff));
  }

  @Override
  public boolean runCleanerChore() throws IOException {
    return get(admin.runCleanerChore());
  }

  @Override
  public boolean isCleanerChoreEnabled() throws IOException {
    return get(admin.isCleanerChoreEnabled());
  }

  @Override
  public Future<Void> mergeRegionsAsync(byte[][] nameOfRegionsToMerge, boolean forcible)
      throws IOException {
    return admin.mergeRegions(Arrays.asList(nameOfRegionsToMerge), forcible);
  }

  @Override
  public void split(TableName tableName) throws IOException {
    get(admin.split(tableName));
  }

  @Override
  public void split(TableName tableName, byte[] splitPoint) throws IOException {
    get(admin.split(tableName, splitPoint));
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName) throws IOException {
    return admin.splitRegion(regionName);
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName, byte[] splitPoint) throws IOException {
    return admin.splitRegion(regionName, splitPoint);
  }

  @Override
  public Future<Void> modifyTableAsync(TableDescriptor td) throws IOException {
    return admin.modifyTable(td);
  }

  @Override
  public Future<Void> modifyTableStoreFileTrackerAsync(TableName tableName, String dstSFT)
    throws IOException {
    return admin.modifyTableStoreFileTracker(tableName, dstSFT);
  }

  @Override
  public void shutdown() throws IOException {
    get(admin.shutdown());
  }

  @Override
  public void stopMaster() throws IOException {
    get(admin.stopMaster());
  }

  @Override
  public boolean isMasterInMaintenanceMode() throws IOException {
    return get(admin.isMasterInMaintenanceMode());
  }

  @Override
  public void stopRegionServer(String hostnamePort) throws IOException {
    get(admin.stopRegionServer(ServerName.valueOf(hostnamePort, 0)));
  }

  @Override
  public ClusterMetrics getClusterMetrics(EnumSet<Option> options) throws IOException {
    return get(admin.getClusterMetrics(options));
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName serverName) throws IOException {
    return get(admin.getRegionMetrics(serverName));
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName serverName, TableName tableName)
      throws IOException {
    return get(admin.getRegionMetrics(serverName, tableName));
  }

  @Override
  public Configuration getConfiguration() {
    return conn.getConfiguration();
  }

  @Override
  public Future<Void> createNamespaceAsync(NamespaceDescriptor descriptor) throws IOException {
    return admin.createNamespace(descriptor);
  }

  @Override
  public Future<Void> modifyNamespaceAsync(NamespaceDescriptor descriptor) throws IOException {
    return admin.modifyNamespace(descriptor);
  }

  @Override
  public Future<Void> deleteNamespaceAsync(String name) throws IOException {
    return admin.deleteNamespace(name);
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name)
      throws NamespaceNotFoundException, IOException {
    return get(admin.getNamespaceDescriptor(name));
  }

  @Override
  public String[] listNamespaces() throws IOException {
    return get(admin.listNamespaces()).toArray(new String[0]);
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return get(admin.listNamespaceDescriptors()).toArray(new NamespaceDescriptor[0]);
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] name) throws IOException {
    return get(admin.listTableDescriptorsByNamespace(Bytes.toString(name)));
  }

  @Override
  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    return get(admin.listTableNamesByNamespace(name)).toArray(new TableName[0]);
  }

  @Override
  public List<RegionInfo> getRegions(TableName tableName) throws IOException {
    return get(admin.getRegions(tableName));
  }

  @Override
  public void close() {
    // do nothing, AsyncAdmin is not a Closeable.
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(List<TableName> tableNames) throws IOException {
    return get(admin.listTableDescriptors(tableNames));
  }

  @Override
  public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning)
      throws IOException {
    return admin.abortProcedure(procId, mayInterruptIfRunning);
  }

  @Override
  public String getProcedures() throws IOException {
    return get(admin.getProcedures());
  }

  @Override
  public String getLocks() throws IOException {
    return get(admin.getLocks());
  }

  @Override
  public void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException {
    get(admin.rollWALWriter(serverName));
  }

  @Override
  public CompactionState getCompactionState(TableName tableName) throws IOException {
    return get(admin.getCompactionState(tableName));
  }

  @Override
  public CompactionState getCompactionState(TableName tableName, CompactType compactType)
      throws IOException {
    return get(admin.getCompactionState(tableName, compactType));
  }

  @Override
  public CompactionState getCompactionStateForRegion(byte[] regionName) throws IOException {
    return get(admin.getCompactionStateForRegion(regionName));
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
    return get(admin.getLastMajorCompactionTimestamp(tableName)).orElse(0L);
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return get(admin.getLastMajorCompactionTimestampForRegion(regionName)).orElse(0L);
  }

  @Override
  public void snapshot(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    get(admin.snapshot(snapshot));
  }

  @Override
  public Future<Void> snapshotAsync(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    return admin.snapshot(snapshot);
  }

  @Override
  public boolean isSnapshotFinished(SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    return get(admin.isSnapshotFinished(snapshot));
  }

  @Override
  public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
    get(admin.restoreSnapshot(snapshotName));
  }

  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot, boolean restoreAcl)
    throws IOException, RestoreSnapshotException {
    get(admin.restoreSnapshot(snapshotName, takeFailSafeSnapshot, restoreAcl));
  }

  @Override
  public Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName,
    boolean restoreAcl, String customSFT)
    throws IOException, TableExistsException, RestoreSnapshotException {
    return admin.cloneSnapshot(snapshotName, tableName, restoreAcl, customSFT);
  }

  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {
    get(admin.execProcedure(signature, instance, props));
  }

  @Override
  public byte[] execProcedureWithReturn(String signature, String instance,
      Map<String, String> props) throws IOException {
    return get(admin.execProcedureWithReturn(signature, instance, props));
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    return get(admin.isProcedureFinished(signature, instance, props));
  }

  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    return get(admin.listSnapshots());
  }

  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    return get(admin.listSnapshots(pattern));
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException {
    return get(admin.listTableSnapshots(tableNamePattern, snapshotNamePattern));
  }

  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {
    get(admin.deleteSnapshot(snapshotName));
  }

  @Override
  public void deleteSnapshots(Pattern pattern) throws IOException {
    get(admin.deleteSnapshots(pattern));
  }

  @Override
  public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
      throws IOException {
    get(admin.deleteTableSnapshots(tableNamePattern, snapshotNamePattern));
  }

  @Override
  public void setQuota(QuotaSettings quota) throws IOException {
    get(admin.setQuota(quota));
  }

  @Override
  public List<QuotaSettings> getQuota(QuotaFilter filter) throws IOException {
    return get(admin.getQuota(filter));
  }

  @SuppressWarnings("deprecation")
  private static final class SyncCoprocessorRpcChannelOverAsync implements CoprocessorRpcChannel {

    private final RpcChannel delegate;

    public SyncCoprocessorRpcChannelOverAsync(RpcChannel delegate) {
      this.delegate = delegate;
    }

    @Override
    public void callMethod(MethodDescriptor method, RpcController controller, Message request,
        Message responsePrototype, RpcCallback<Message> done) {
      ClientCoprocessorRpcController c = new ClientCoprocessorRpcController();
      CoprocessorBlockingRpcCallback<Message> callback = new CoprocessorBlockingRpcCallback<>();
      delegate.callMethod(method, c, request, responsePrototype, callback);
      Message ret;
      try {
        ret = callback.get();
      } catch (IOException e) {
        setCoprocessorError(controller, e);
        return;
      }
      if (c.failed()) {
        setCoprocessorError(controller, c.getFailed());
      }
      done.run(ret);
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor method, RpcController controller,
        Message request, Message responsePrototype) throws ServiceException {
      ClientCoprocessorRpcController c = new ClientCoprocessorRpcController();
      CoprocessorBlockingRpcCallback<Message> done = new CoprocessorBlockingRpcCallback<>();
      callMethod(method, c, request, responsePrototype, done);
      Message ret;
      try {
        ret = done.get();
      } catch (IOException e) {
        throw new ServiceException(e);
      }
      if (c.failed()) {
        setCoprocessorError(controller, c.getFailed());
        throw new ServiceException(c.getFailed());
      }
      return ret;
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public CoprocessorRpcChannel coprocessorService() {
    return new SyncCoprocessorRpcChannelOverAsync(
      new MasterCoprocessorRpcChannelImpl(admin.<Message> newMasterCaller()));
  }

  @SuppressWarnings("deprecation")
  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
    return new SyncCoprocessorRpcChannelOverAsync(new RegionServerCoprocessorRpcChannelImpl(
      admin.<Message> newServerCaller().serverName(serverName)));
  }

  @Override
  public void updateConfiguration(ServerName server) throws IOException {
    get(admin.updateConfiguration(server));
  }

  @Override
  public void updateConfiguration() throws IOException {
    get(admin.updateConfiguration());
  }

  @Override
  public void updateConfiguration(String groupName) throws IOException {
    get(admin.updateConfiguration(groupName));
  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    return get(admin.getSecurityCapabilities());
  }

  @Override
  public boolean splitSwitch(boolean enabled, boolean synchronous) throws IOException {
    return get(admin.splitSwitch(enabled, synchronous));
  }

  @Override
  public boolean mergeSwitch(boolean enabled, boolean synchronous) throws IOException {
    return get(admin.mergeSwitch(enabled, synchronous));
  }

  @Override
  public boolean isSplitEnabled() throws IOException {
    return get(admin.isSplitEnabled());
  }

  @Override
  public boolean isMergeEnabled() throws IOException {
    return get(admin.isMergeEnabled());
  }

  @Override
  public Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig,
      boolean enabled) throws IOException {
    return admin.addReplicationPeer(peerId, peerConfig, enabled);
  }

  @Override
  public Future<Void> removeReplicationPeerAsync(String peerId) throws IOException {
    return admin.removeReplicationPeer(peerId);
  }

  @Override
  public Future<Void> enableReplicationPeerAsync(String peerId) throws IOException {
    return admin.enableReplicationPeer(peerId);
  }

  @Override
  public Future<Void> disableReplicationPeerAsync(String peerId) throws IOException {
    return admin.disableReplicationPeer(peerId);
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId) throws IOException {
    return get(admin.getReplicationPeerConfig(peerId));
  }

  @Override
  public Future<Void> updateReplicationPeerConfigAsync(String peerId,
      ReplicationPeerConfig peerConfig) throws IOException {
    return admin.updateReplicationPeerConfig(peerId, peerConfig);
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    return get(admin.listReplicationPeers());
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern) throws IOException {
    return get(admin.listReplicationPeers(pattern));
  }

  @Override
  public Future<Void> transitReplicationPeerSyncReplicationStateAsync(String peerId,
      SyncReplicationState state) throws IOException {
    return admin.transitReplicationPeerSyncReplicationState(peerId, state);
  }

  @Override
  public void decommissionRegionServers(List<ServerName> servers, boolean offload)
      throws IOException {
    get(admin.decommissionRegionServers(servers, offload));
  }

  @Override
  public List<ServerName> listDecommissionedRegionServers() throws IOException {
    return get(admin.listDecommissionedRegionServers());
  }

  @Override
  public void recommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames)
      throws IOException {
    get(admin.recommissionRegionServer(server, encodedRegionNames));
  }

  @Override
  public List<TableCFs> listReplicatedTableCFs() throws IOException {
    return get(admin.listReplicatedTableCFs());
  }

  @Override
  public void enableTableReplication(TableName tableName) throws IOException {
    get(admin.enableTableReplication(tableName));
  }

  @Override
  public void disableTableReplication(TableName tableName) throws IOException {
    get(admin.disableTableReplication(tableName));
  }

  @Override
  public void clearCompactionQueues(ServerName serverName, Set<String> queues)
      throws IOException, InterruptedException {
    get(admin.clearCompactionQueues(serverName, queues));
  }

  @Override
  public List<ServerName> clearDeadServers(List<ServerName> servers) throws IOException {
    return get(admin.clearDeadServers(servers));
  }

  @Override
  public void cloneTableSchema(TableName tableName, TableName newTableName, boolean preserveSplits)
      throws IOException {
    get(admin.cloneTableSchema(tableName, newTableName, preserveSplits));
  }

  @Override
  public boolean switchRpcThrottle(boolean enable) throws IOException {
    return get(admin.switchRpcThrottle(enable));
  }

  @Override
  public boolean isRpcThrottleEnabled() throws IOException {
    return get(admin.isRpcThrottleEnabled());
  }

  @Override
  public boolean exceedThrottleQuotaSwitch(boolean enable) throws IOException {
    return get(admin.exceedThrottleQuotaSwitch(enable));
  }

  @Override
  public Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException {
    return get(admin.getSpaceQuotaTableSizes());
  }

  @Override
  public Map<TableName, ? extends SpaceQuotaSnapshotView> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) throws IOException {
    return get(admin.getRegionServerSpaceQuotaSnapshots(serverName));
  }

  @Override
  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(String namespace) throws IOException {
    return get(admin.getCurrentSpaceQuotaSnapshot(namespace));
  }

  @Override
  public SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(TableName tableName)
      throws IOException {
    return get(admin.getCurrentSpaceQuotaSnapshot(tableName));
  }

  @Override
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
      throws IOException {
    get(admin.grant(userPermission, mergeExistingPermissions));
  }

  @Override
  public void revoke(UserPermission userPermission) throws IOException {
    get(admin.revoke(userPermission));
  }

  @Override
  public List<UserPermission> getUserPermissions(
      GetUserPermissionsRequest getUserPermissionsRequest) throws IOException {
    return get(admin.getUserPermissions(getUserPermissionsRequest));
  }

  @Override
  public List<Boolean> hasUserPermissions(String userName, List<Permission> permissions)
      throws IOException {
    return get(admin.hasUserPermissions(userName, permissions));
  }

  @Override
  public boolean snapshotCleanupSwitch(final boolean on, final boolean synchronous)
      throws IOException {
    return get(admin.snapshotCleanupSwitch(on, synchronous));
  }

  @Override
  public boolean isSnapshotCleanupEnabled() throws IOException {
    return get(admin.isSnapshotCleanupEnabled());
  }

  @Override
  public List<Boolean> clearSlowLogResponses(final Set<ServerName> serverNames)
      throws IOException {
    return get(admin.clearSlowLogResponses(serverNames));
  }

  @Override
  public RSGroupInfo getRSGroup(String groupName) throws IOException {
    return get(admin.getRSGroup(groupName));
  }

  @Override
  public void moveServersToRSGroup(Set<Address> servers, String groupName) throws IOException {
    get(admin.moveServersToRSGroup(servers, groupName));
  }

  @Override
  public void addRSGroup(String groupName) throws IOException {
    get(admin.addRSGroup(groupName));
  }

  @Override
  public void removeRSGroup(String groupName) throws IOException {
    get(admin.removeRSGroup(groupName));
  }

  @Override
  public BalanceResponse balanceRSGroup(String groupName, BalanceRequest request) throws IOException {
    return get(admin.balanceRSGroup(groupName, request));
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return get(admin.listRSGroups());
  }

  @Override
  public List<TableName> listTablesInRSGroup(String groupName) throws IOException {
    return get(admin.listTablesInRSGroup(groupName));
  }

  @Override
  public Pair<List<String>, List<TableName>>
    getConfiguredNamespacesAndTablesInRSGroup(String groupName) throws IOException {
    return get(admin.getConfiguredNamespacesAndTablesInRSGroup(groupName));
  }

  @Override
  public RSGroupInfo getRSGroup(Address hostPort) throws IOException {
    return get(admin.getRSGroup(hostPort));
  }

  @Override
  public void removeServersFromRSGroup(Set<Address> servers) throws IOException {
    get(admin.removeServersFromRSGroup(servers));
  }

  @Override
  public RSGroupInfo getRSGroup(TableName tableName) throws IOException {
    return get(admin.getRSGroup(tableName));
  }

  @Override
  public void setRSGroup(Set<TableName> tables, String groupName) throws IOException {
    get(admin.setRSGroup(tables, groupName));
  }

  @Override
  public void renameRSGroup(String oldName, String newName) throws IOException {
    get(admin.renameRSGroup(oldName, newName));
  }

  @Override
  public void updateRSGroupConfig(String groupName, Map<String, String> configuration)
      throws IOException {
    get(admin.updateRSGroupConfig(groupName, configuration));
  }

  @Override
  public List<LogEntry> getLogEntries(Set<ServerName> serverNames, String logType,
      ServerType serverType, int limit, Map<String, Object> filterParams)
      throws IOException {
    return get(admin.getLogEntries(serverNames, logType, serverType, limit, filterParams));
  }
}
