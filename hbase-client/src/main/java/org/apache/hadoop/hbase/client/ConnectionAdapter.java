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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;

/**
 * An internal class that delegates to an {@link HConnection} instance.
 * A convenience to override when customizing method implementations.
 *
 *
 * @see ConnectionUtils#createShortCircuitHConnection(HConnection, ServerName,
 * AdminService.BlockingInterface, ClientService.BlockingInterface) for case where we make
 * Connections skip RPC if request is to local server.
 */
@InterfaceAudience.Private
@Deprecated
//NOTE: DO NOT make this class public. It was made package-private on purpose.
abstract class ConnectionAdapter implements ClusterConnection {

  private final ClusterConnection wrappedConnection;

  public ConnectionAdapter(Connection c) {
    wrappedConnection = (ClusterConnection)c;
  }

  @Override
  public void abort(String why, Throwable e) {
    wrappedConnection.abort(why, e);
  }

  @Override
  public boolean isAborted() {
    return wrappedConnection.isAborted();
  }

  @Override
  public void close() throws IOException {
    wrappedConnection.close();
  }

  @Override
  public Configuration getConfiguration() {
    return wrappedConnection.getConfiguration();
  }

  @Override
  public HTableInterface getTable(String tableName) throws IOException {
    return wrappedConnection.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(byte[] tableName) throws IOException {
    return wrappedConnection.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(TableName tableName) throws IOException {
    return wrappedConnection.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(String tableName, ExecutorService pool)
      throws IOException {
    return wrappedConnection.getTable(tableName, pool);
  }

  @Override
  public HTableInterface getTable(byte[] tableName, ExecutorService pool)
      throws IOException {
    return wrappedConnection.getTable(tableName, pool);
  }

  @Override
  public HTableInterface getTable(TableName tableName, ExecutorService pool)
      throws IOException {
    return wrappedConnection.getTable(tableName, pool);
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params)
      throws IOException {
    return wrappedConnection.getBufferedMutator(params);
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return wrappedConnection.getBufferedMutator(tableName);
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return wrappedConnection.getRegionLocator(tableName);
  }

  @Override
  public Admin getAdmin() throws IOException {
    return wrappedConnection.getAdmin();
  }

  @Override
  public MetricsConnection getConnectionMetrics() {
    return wrappedConnection.getConnectionMetrics();
  }

  @Override
  public boolean isMasterRunning() throws MasterNotRunningException,
      ZooKeeperConnectionException {
    return wrappedConnection.isMasterRunning();
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return wrappedConnection.isTableEnabled(tableName);
  }

  @Override
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return wrappedConnection.isTableEnabled(tableName);
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return wrappedConnection.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return wrappedConnection.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return wrappedConnection.isTableAvailable(tableName);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return wrappedConnection.isTableAvailable(tableName);
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys)
      throws IOException {
    return wrappedConnection.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName, byte[][] splitKeys)
      throws IOException {
    return wrappedConnection.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return wrappedConnection.listTables();
  }

  @Override
  public String[] getTableNames() throws IOException {
    return wrappedConnection.getTableNames();
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return wrappedConnection.listTableNames();
  }

  @Override
  public HTableDescriptor getHTableDescriptor(TableName tableName)
      throws IOException {
    return wrappedConnection.getHTableDescriptor(tableName);
  }

  @Override
  public HTableDescriptor getHTableDescriptor(byte[] tableName)
      throws IOException {
    return wrappedConnection.getHTableDescriptor(tableName);
  }

  @Override
  public HRegionLocation locateRegion(TableName tableName, byte[] row)
      throws IOException {
    return wrappedConnection.locateRegion(tableName, row);
  }

  @Override
  public HRegionLocation locateRegion(byte[] tableName, byte[] row)
      throws IOException {
    return wrappedConnection.locateRegion(tableName, row);
  }

  @Override
  public RegionLocations locateRegion(TableName tableName, byte[] row, boolean useCache,
      boolean retry) throws IOException {
    return wrappedConnection.locateRegion(tableName, row, useCache, retry);
  }

  @Override
  public void clearRegionCache() {
    wrappedConnection.clearRegionCache();
  }

  @Override
  public void clearRegionCache(TableName tableName) {
    wrappedConnection.clearRegionCache(tableName);
  }

  @Override
  public void clearRegionCache(byte[] tableName) {
    wrappedConnection.clearRegionCache(tableName);
  }

  @Override
  public void deleteCachedRegionLocation(HRegionLocation location) {
    wrappedConnection.deleteCachedRegionLocation(location);
  }

  @Override
  public HRegionLocation relocateRegion(TableName tableName, byte[] row)
      throws IOException {
    return wrappedConnection.relocateRegion(tableName, row);
  }

  @Override
  public HRegionLocation relocateRegion(byte[] tableName, byte[] row)
      throws IOException {
    return wrappedConnection.relocateRegion(tableName, row);
  }

  @Override
  public void updateCachedLocations(TableName tableName, byte[] rowkey,
      Object exception, HRegionLocation source) {
    wrappedConnection.updateCachedLocations(tableName, rowkey, exception, source);
  }

  @Override
  public void updateCachedLocations(TableName tableName, byte[] regionName, byte[] rowkey,
      Object exception, ServerName source) {
    wrappedConnection.updateCachedLocations(tableName, regionName, rowkey, exception, source);
  }

  @Override
  public void updateCachedLocations(byte[] tableName, byte[] rowkey,
      Object exception, HRegionLocation source) {
    wrappedConnection.updateCachedLocations(tableName, rowkey, exception, source);
  }

  @Override
  public HRegionLocation locateRegion(byte[] regionName) throws IOException {
    return wrappedConnection.locateRegion(regionName);
  }

  @Override
  public List<HRegionLocation> locateRegions(TableName tableName)
      throws IOException {
    return wrappedConnection.locateRegions(tableName);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName)
      throws IOException {
    return wrappedConnection.locateRegions(tableName);
  }

  @Override
  public List<HRegionLocation> locateRegions(TableName tableName,
      boolean useCache, boolean offlined) throws IOException {
    return wrappedConnection.locateRegions(tableName, useCache, offlined);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName,
      boolean useCache, boolean offlined) throws IOException {
    return wrappedConnection.locateRegions(tableName, useCache, offlined);
  }

  @Override
  public RegionLocations locateRegion(TableName tableName, byte[] row, boolean useCache,
      boolean retry, int replicaId) throws IOException {
    return wrappedConnection.locateRegion(tableName, row, useCache, retry, replicaId);
  }

  @Override
  public RegionLocations relocateRegion(TableName tableName, byte[] row, int replicaId)
      throws IOException {
    return wrappedConnection.relocateRegion(tableName, row, replicaId);
  }

  @Override
  public MasterService.BlockingInterface getMaster() throws IOException {
    return wrappedConnection.getMaster();
  }

  @Override
  public AdminService.BlockingInterface getAdmin(
      ServerName serverName) throws IOException {
    return wrappedConnection.getAdmin(serverName);
  }

  @Override
  public ClientService.BlockingInterface getClient(
      ServerName serverName) throws IOException {
    return wrappedConnection.getClient(serverName);
  }

  @Override
  public AdminService.BlockingInterface getAdmin(
      ServerName serverName, boolean getMaster) throws IOException {
    return wrappedConnection.getAdmin(serverName, getMaster);
  }

  @Override
  public HRegionLocation getRegionLocation(TableName tableName, byte[] row,
      boolean reload) throws IOException {
    return wrappedConnection.getRegionLocation(tableName, row, reload);
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] tableName, byte[] row,
      boolean reload) throws IOException {
    return wrappedConnection.getRegionLocation(tableName, row, reload);
  }

  @Override
  public void processBatch(List<? extends Row> actions, TableName tableName,
      ExecutorService pool, Object[] results) throws IOException,
      InterruptedException {
    wrappedConnection.processBatch(actions, tableName, pool, results);
  }

  @Override
  public void processBatch(List<? extends Row> actions, byte[] tableName,
      ExecutorService pool, Object[] results) throws IOException,
      InterruptedException {
    wrappedConnection.processBatch(actions, tableName, pool, results);
  }

  @Override
  public <R> void processBatchCallback(List<? extends Row> list,
      TableName tableName, ExecutorService pool, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    wrappedConnection.processBatchCallback(list, tableName, pool, results, callback);
  }

  @Override
  public <R> void processBatchCallback(List<? extends Row> list,
      byte[] tableName, ExecutorService pool, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    wrappedConnection.processBatchCallback(list, tableName, pool, results, callback);
  }

  @Override
  public void setRegionCachePrefetch(TableName tableName, boolean enable) {
    wrappedConnection.setRegionCachePrefetch(tableName, enable);
  }

  @Override
  public void setRegionCachePrefetch(byte[] tableName, boolean enable) {
    wrappedConnection.setRegionCachePrefetch(tableName, enable);
  }

  @Override
  public boolean getRegionCachePrefetch(TableName tableName) {
    return wrappedConnection.getRegionCachePrefetch(tableName);
  }

  @Override
  public boolean getRegionCachePrefetch(byte[] tableName) {
     return wrappedConnection.getRegionCachePrefetch(tableName);
  }

  @Override
  public int getCurrentNrHRS() throws IOException {
    return wrappedConnection.getCurrentNrHRS();
  }

  @Override
  public HTableDescriptor[] getHTableDescriptorsByTableName(
      List<TableName> tableNames) throws IOException {
    return wrappedConnection.getHTableDescriptorsByTableName(tableNames);
  }

  @Override
  public HTableDescriptor[] getHTableDescriptors(List<String> tableNames)
      throws IOException {
    return wrappedConnection.getHTableDescriptors(tableNames);
  }

  @Override
  public boolean isClosed() {
    return wrappedConnection.isClosed();
  }

  @Override
  public void clearCaches(ServerName sn) {
    wrappedConnection.clearCaches(sn);
  }

  @Override
  public MasterKeepAliveConnection getKeepAliveMasterService()
      throws MasterNotRunningException {
    return wrappedConnection.getKeepAliveMasterService();
  }

  @Override
  public boolean isDeadServer(ServerName serverName) {
    return wrappedConnection.isDeadServer(serverName);
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return wrappedConnection.getNonceGenerator();
  }

  @Override
  public AsyncProcess getAsyncProcess() {
    return wrappedConnection.getAsyncProcess();
  }

  @Override
  public RpcRetryingCallerFactory getNewRpcRetryingCallerFactory(Configuration conf) {
    return wrappedConnection.getNewRpcRetryingCallerFactory(conf);
  }
  
  @Override
  public boolean isManaged() {
    return wrappedConnection.isManaged();
  }

  @Override
  public ServerStatisticTracker getStatisticsTracker() {
    return wrappedConnection.getStatisticsTracker();
  }

  @Override
  public ClientBackoffPolicy getBackoffPolicy() {
    return wrappedConnection.getBackoffPolicy();
  }

  @Override
  public boolean hasCellBlockSupport() {
    return wrappedConnection.hasCellBlockSupport();
  }
}
