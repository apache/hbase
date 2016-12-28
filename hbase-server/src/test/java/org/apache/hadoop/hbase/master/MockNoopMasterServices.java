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

import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import java.io.IOException;
import java.util.List;

public class MockNoopMasterServices implements MasterServices, Server {
  @Override
  public SnapshotManager getSnapshotManager() {
    return null;
  }

  @Override
  public MasterProcedureManagerHost getMasterProcedureManagerHost() {
    return null;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return null;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return null;
  }

  @Override
  public ServerManager getServerManager() {
    return null;
  }

  @Override
  public ExecutorService getExecutorService() {
    return null;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return null;
  }

  @Override
  public MasterCoprocessorHost getMasterCoprocessorHost() {
    return null;
  }

  @Override
  public TableNamespaceManager getTableNamespaceManager() {
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
  public void checkTableModifiable(TableName tableName) throws IOException, TableNotFoundException, TableNotDisabledException {

  }

  @Override
  public long createTable(HTableDescriptor desc, byte[][] splitKeys, long nonceGroup, long nonce) throws IOException {
    return 0;
  }

  @Override
  public long createSystemTable(HTableDescriptor hTableDescriptor) throws IOException {
    return 0;
  }

  @Override
  public long deleteTable(TableName tableName, long nonceGroup, long nonce) throws IOException {
    return 0;
  }

  @Override
  public void truncateTable(TableName tableName, boolean preserveSplits, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public void modifyTable(TableName tableName, HTableDescriptor descriptor, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public long enableTable(TableName tableName, long nonceGroup, long nonce) throws IOException {
    return 0;
  }

  @Override
  public long disableTable(TableName tableName, long nonceGroup, long nonce) throws IOException {
    return 0;
  }

  @Override
  public void addColumn(TableName tableName, HColumnDescriptor column, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public void modifyColumn(TableName tableName, HColumnDescriptor descriptor, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public void deleteColumn(TableName tableName, byte[] columnName, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return null;
  }

  @Override
  public boolean isServerCrashProcessingEnabled() {
    return false;
  }

  @Override
  public boolean registerService(Service instance) {
    return false;
  }

  @Override
  public void dispatchMergingRegions(HRegionInfo region_a, HRegionInfo region_b, boolean forcible, User user) throws IOException {

  }

  @Override
  public boolean isInitialized() {
    return false;
  }

  @Override
  public void createNamespace(NamespaceDescriptor descriptor, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public void createNamespaceSync(
      final NamespaceDescriptor descriptor,
      final long nonceGroup,
      final long nonce,
      final boolean executeCoprocessor) throws IOException {

  }

  @Override
  public void modifyNamespace(NamespaceDescriptor descriptor, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public void deleteNamespace(String name, long nonceGroup, long nonce) throws IOException {

  }

  @Override
  public boolean isInMaintenanceMode() {
    return false;
  }

  @Override
  public boolean abortProcedure(long procId, boolean mayInterruptIfRunning) throws IOException {
    return false;
  }

  @Override
  public List<ProcedureInfo> listProcedures() throws IOException {
    return null;
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
    return null;
  }

  @Override
  public List<NamespaceDescriptor> listNamespaceDescriptors() throws IOException {
    return null;
  }

  @Override
  public List<HTableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
    return null;
  }

  @Override
  public List<TableName> listTableNamesByNamespace(String name) throws IOException {
    return null;
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
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return null;
  }

  @Override
  public ClusterConnection getConnection() {
    return null;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return null;
  }

  @Override
  public ServerName getServerName() {
    return null;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }

  @Override
  public void abort(String why, Throwable e) {

  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public void stop(String why) {

  }

  @Override
  public boolean isStopped() {
    return false;
  }
}