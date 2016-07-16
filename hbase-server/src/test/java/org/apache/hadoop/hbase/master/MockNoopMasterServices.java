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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.mockito.Mockito;

import com.google.protobuf.Service;

public class MockNoopMasterServices implements MasterServices, Server {
  private final Configuration conf;

  public MockNoopMasterServices() {
    this(null);
  }

  public MockNoopMasterServices(final Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void checkTableModifiable(TableName tableName) throws IOException {
    //no-op
  }

  @Override
  public long createTable(
      final HTableDescriptor desc,
      final byte[][] splitKeys,
      final long nonceGroup,
      final long nonce) throws IOException {
    // no-op
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
  public ServerManager getServerManager() {
    return null;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return null;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return null;
  }

  @Override
  public ClusterConnection getConnection() {
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
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return null;
  }

  @Override
  public boolean isServerCrashProcessingEnabled() {
    return true;
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
  public List<ProcedureInfo> listProcedures() throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<HTableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
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
      final HTableDescriptor descriptor,
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
  public long addColumn(final TableName tableName, final HColumnDescriptor columnDescriptor,
      final long nonceGroup, final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long modifyColumn(final TableName tableName, final HColumnDescriptor descriptor,
      final long nonceGroup, final long nonce) throws IOException {
    return -1;
  }

  @Override
  public long deleteColumn(final TableName tableName, final byte[] columnName,
      final long nonceGroup, final long nonce) throws IOException {
    return -1;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return null;
  }

  @Override
  public TableStateManager getTableStateManager() {
    return null;
  }

  @Override
  public long dispatchMergingRegions(
      final HRegionInfo region_a,
      final HRegionInfo region_b,
      final boolean forcible,
      final long nonceGroup,
      final long nonce) throws IOException {
    return -1;
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
  public ClusterConnection getClusterConnection() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LoadBalancer getLoadBalancer() {
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
}
