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
package org.apache.hadoop.hbase.master;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.master.replication.SyncReplicationReplayWALManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

import org.apache.hbase.thirdparty.com.google.protobuf.Service;

public class MockNoopMasterServices implements MasterServices {
  private final Configuration conf;
  private final MetricsMaster metricsMaster;

  public MockNoopMasterServices() {
    this(null);
  }

  public MockNoopMasterServices(final Configuration conf) {
    this.conf = conf;
    this.metricsMaster = new MetricsMaster(new MetricsMasterWrapperImpl(null));
  }

  @Override
  public void checkTableModifiable(TableName tableName) throws IOException {
    //no-op
  }

  @Override
  public long createTable(
      final TableDescriptor desc,
      final byte[][] splitKeys,
      final long nonceGroup,
      final long nonce) throws IOException {
    // no-op
    return -1;
  }

  @Override
  public long createSystemTable(final TableDescriptor tableDescriptor) throws IOException {
    return -1;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return null;
  }

  @Override
  public ExecutorService getExecutorService() {
    return null;
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }

  @Override
  public RegionNormalizer getRegionNormalizer() {
    return null;
  }

  @Override
  public CatalogJanitor getCatalogJanitor() {
    return null;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return null;
  }

  @Override
  public MasterWalManager getMasterWalManager() {
    return null;
  }

  @Override
  public MasterCoprocessorHost getMasterCoprocessorHost() {
    return null;
  }

  @Override
  public MasterQuotaManager getMasterQuotaManager() {
    return null;
  }

  @Override
  public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return null;
  }

  @Override
  public MetricsMaster getMasterMetrics() {
    return metricsMaster;
  }

  @Override
  public ServerManager getServerManager() {
    return null;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return null;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public Connection getConnection() {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ServerName getServerName() {
    return ServerName.valueOf("mock.master", 12345, 1);
  }

  @Override
  public void abort(String why, Throwable e) {
    //no-op
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  private boolean stopped = false;

  @Override
  public void stop(String why) {
    stopped = true;
  }

  @Override
  public boolean isStopping() {
    return stopped;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return null;
  }

  @Override
  public boolean registerService(Service instance) {
    return false;
  }

  @Override
  public boolean abortProcedure(final long procId, final boolean mayInterruptIfRunning)
      throws IOException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<Procedure<?>> getProcedures() throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<LockedResource> getLocks() throws IOException {
    return null;
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<TableName> listTableNamesByNamespace(String name) throws IOException {
    return null;
  }

  @Override
  public long deleteTable(
      final TableName tableName,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long truncateTable(
      final TableName tableName,
      final boolean preserveSplits,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
  }


  @Override
  public long modifyTable(
      final TableName tableName,
      final TableDescriptor descriptor,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long enableTable(
      final TableName tableName,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long disableTable(
      TableName tableName,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long addColumn(final TableName tableName, final ColumnFamilyDescriptor columnDescriptor,
      final long nonceGroup, final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long modifyColumn(final TableName tableName, final ColumnFamilyDescriptor descriptor,
      final long nonceGroup, final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long deleteColumn(final TableName tableName, final byte[] columnName,
      final long nonceGroup, final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long mergeRegions(
      final RegionInfo[] regionsToMerge,
      final boolean forcible,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long splitRegion(
      final RegionInfo regionInfo,
      final byte[] splitRow,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
  }

  @Override
  public TableStateManager getTableStateManager() {
    return mock(TableStateManager.class);
  }

  @Override
  public boolean isActiveMaster() {
    return true;
  }

  @Override
  public boolean isInitialized() {
    return false;
  }

  @Override
  public boolean isInMaintenanceMode() {
    return false;
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName table) throws IOException {
    return 0;
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return 0;
  }

  @Override
  public ClusterSchema getClusterSchema() {
    return null;
  }

  @Override
  public LoadBalancer getLoadBalancer() {
    return null;
  }

  @Override
  public FavoredNodesManager getFavoredNodesManager() {
    return null;
  }

  @Override
  public SnapshotManager getSnapshotManager() {
    return null;
  }

  @Override
  public MasterProcedureManagerHost getMasterProcedureManagerHost() {
    return null;
  }

  @Override
  public boolean isSplitOrMergeEnabled(MasterSwitchType switchType) {
    return false;
  }

  @Override
  public long addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException {
    return 0;
  }

  @Override
  public long removeReplicationPeer(String peerId) throws ReplicationException {
    return 0;
  }

  @Override
  public long enableReplicationPeer(String peerId) throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public long disableReplicationPeer(String peerId) throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId) throws ReplicationException,
      IOException {
    return null;
  }

  @Override
  public long updateReplicationPeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(String regex)
      throws ReplicationException, IOException {
    return null;
  }

  @Override
  public LockManager getLockManager() {
    return null;
  }

  @Override
  public String getRegionServerVersion(ServerName sn) {
    return "0.0.0";
  }

  @Override
  public void checkIfShouldMoveSystemRegionAsync() {
  }

  @Override
  public String getClientIdAuditPrefix() {
    return null;
  }

  @Override
  public ProcedureEvent<?> getInitializedEvent() {
    return null;
  }

  @Override
  public FileSystem getFileSystem() {
    return null;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    return null;
  }

  @Override
  public ReplicationPeerManager getReplicationPeerManager() {
    return null;
  }

  @Override
  public boolean isClusterUp() {
    return true;
  }

  public long transitReplicationPeerSyncReplicationState(String peerId,
    SyncReplicationState clusterState) throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public SyncReplicationReplayWALManager getSyncReplicationReplayWALManager() {
    return null;
  }

  @Override
  public AccessChecker getAccessChecker() {
    return null;
  }

  @Override
  public ZKPermissionWatcher getZKPermissionWatcher() {
    return null;
  }

  @Override
  public List<RegionPlan> executeRegionPlansWithThrottling(List<RegionPlan> plans) {
    return null;
  }

  @Override
  public AsyncClusterConnection getAsyncClusterConnection() {
    return null;
  }

  @Override
  public void runReplicationBarrierCleaner() {
  }

  @Override
  public RSGroupInfoManager getRSGroupInfoManager() {
    return null;
  }

  @Override
  public boolean isBalancerOn() {
    return false;
  }
}
