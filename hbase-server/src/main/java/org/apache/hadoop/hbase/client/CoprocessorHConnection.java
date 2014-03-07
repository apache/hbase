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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * Connection to an HTable from within a Coprocessor. We can do some nice tricks since we know we
 * are on a regionserver, for instance skipping the full serialization/deserialization of objects
 * when talking to the server.
 * <p>
 * You should not use this class from any client - its an internal class meant for use by the
 * coprocessor framework.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class CoprocessorHConnection implements ClusterConnection {
  private static final NonceGenerator ng = new ConnectionManager.NoNonceGenerator();

  /**
   * Create an unmanaged {@link HConnection} based on the environment in which we are running the
   * coprocessor. The {@link HConnection} must be externally cleaned up (we bypass the usual HTable
   * cleanup mechanisms since we own everything).
   * @param env environment hosting the {@link HConnection}
   * @return an unmanaged {@link HConnection}.
   * @throws IOException if we cannot create the basic connection
   */
  static ClusterConnection getConnectionForEnvironment(CoprocessorEnvironment env)
      throws IOException {
    ClusterConnection connection =
        ConnectionManager.createConnectionInternal(env.getConfiguration());
    // this bit is a little hacky - just trying to get it going for the moment
    if (env instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment e = (RegionCoprocessorEnvironment) env;
      RegionServerServices services = e.getRegionServerServices();
      if (services instanceof HRegionServer) {
        return new CoprocessorHConnection(connection, (HRegionServer) services);
      }
    }
    return connection;
  }

  private ClusterConnection delegate;
  private ServerName serverName;
  private HRegionServer server;

  public CoprocessorHConnection(ClusterConnection delegate, HRegionServer server) {
    this.server = server;
    this.serverName = server.getServerName();
    this.delegate = delegate;
  }

  @Override
  public org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface
      getClient(ServerName serverName) throws IOException {
    // client is trying to reach off-server, so we can't do anything special
    if (!this.serverName.equals(serverName)) {
      return delegate.getClient(serverName);
    }
    // the client is attempting to write to the same regionserver, we can short-circuit to our
    // local regionserver
    return server.getRSRpcServices();
  }

  @Override
  public void abort(String why, Throwable e) {
    delegate.abort(why, e);
  }

  @Override
  public boolean isAborted() {
    return delegate.isAborted();
  }

  @Override
  public Configuration getConfiguration() {
    return delegate.getConfiguration();
  }

  @Override
  public HTableInterface getTable(String tableName) throws IOException {
    return delegate.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(byte[] tableName) throws IOException {
    return delegate.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(TableName tableName) throws IOException {
    return delegate.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(String tableName, ExecutorService pool) throws IOException {
    return delegate.getTable(tableName, pool);
  }

  @Override
  public HTableInterface getTable(byte[] tableName, ExecutorService pool) throws IOException {
    return delegate.getTable(tableName, pool);
  }

  @Override
  public HTableInterface getTable(TableName tableName, ExecutorService pool) throws IOException {
    return delegate.getTable(tableName, pool);
  }

  @Override
  public Admin getAdmin() throws IOException { return delegate.getAdmin(); }

  @Override
  public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
    return delegate.isMasterRunning();
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return delegate.isTableEnabled(tableName);
  }

  @Override
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return delegate.isTableEnabled(tableName);
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return delegate.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return delegate.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return delegate.isTableAvailable(tableName);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return delegate.isTableAvailable(tableName);
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    return delegate.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName, byte[][] splitKeys) throws IOException {
    return delegate.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return delegate.listTables();
  }

  @Override
  public String[] getTableNames() throws IOException {
    return delegate.getTableNames();
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return delegate.listTableNames();
  }

  @Override
  public HTableDescriptor getHTableDescriptor(TableName tableName) throws IOException {
    return delegate.getHTableDescriptor(tableName);
  }

  @Override
  public HTableDescriptor getHTableDescriptor(byte[] tableName) throws IOException {
    return delegate.getHTableDescriptor(tableName);
  }

  @Override
  public HRegionLocation locateRegion(TableName tableName, byte[] row) throws IOException {
    return delegate.locateRegion(tableName, row);
  }

  @Override
  public HRegionLocation locateRegion(byte[] tableName, byte[] row) throws IOException {
    return delegate.locateRegion(tableName, row);
  }

  @Override
  public void clearRegionCache() {
    delegate.clearRegionCache();
  }

  @Override
  public void clearRegionCache(TableName tableName) {
    delegate.clearRegionCache(tableName);
  }

  @Override
  public void clearRegionCache(byte[] tableName) {
    delegate.clearRegionCache(tableName);
  }

  @Override
  public HRegionLocation relocateRegion(TableName tableName, byte[] row) throws IOException {
    return delegate.relocateRegion(tableName, row);
  }

  @Override
  public HRegionLocation relocateRegion(byte[] tableName, byte[] row) throws IOException {
    return delegate.relocateRegion(tableName, row);
  }

  @Override
  public void updateCachedLocations(TableName tableName, byte[] regionName, byte[] rowkey,
      Object exception, ServerName source) {
    delegate.updateCachedLocations(tableName, regionName, rowkey, exception, source);
  }

  @Override
  public void updateCachedLocations(TableName tableName, byte[] rowkey, Object exception,
      HRegionLocation source) {
    delegate.updateCachedLocations(tableName, rowkey, exception, source);
  }

  @Override
  public void updateCachedLocations(byte[] tableName, byte[] rowkey, Object exception,
      HRegionLocation source) {
    delegate.updateCachedLocations(tableName, rowkey, exception, source);
  }

  @Override
  public HRegionLocation locateRegion(byte[] regionName) throws IOException {
    return delegate.locateRegion(regionName);
  }

  @Override
  public List<HRegionLocation> locateRegions(TableName tableName) throws IOException {
    return delegate.locateRegions(tableName);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName) throws IOException {
    return delegate.locateRegions(tableName);
  }

  @Override
  public List<HRegionLocation>
      locateRegions(TableName tableName, boolean useCache, boolean offlined) throws IOException {
    return delegate.locateRegions(tableName, useCache, offlined);
  }

  @Override
  public RegionLocations locateRegion(TableName tableName, byte[] row,
                                      boolean useCache, boolean retry) throws IOException {
    return delegate.locateRegion(tableName, row, useCache, retry);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName, boolean useCache, boolean offlined)
      throws IOException {
    return delegate.locateRegions(tableName, useCache, offlined);
  }

  @Override
  public org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService.BlockingInterface getMaster()
  throws IOException {
    return delegate.getMaster();
  }

  @Override
  public org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface
      getAdmin(ServerName serverName) throws IOException {
    return delegate.getAdmin(serverName);
  }

  @Override
  public org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface
      getAdmin(ServerName serverName, boolean getMaster) throws IOException {
    return delegate.getAdmin(serverName, getMaster);
  }

  @Override
  public HRegionLocation getRegionLocation(TableName tableName, byte[] row, boolean reload)
      throws IOException {
    return delegate.getRegionLocation(tableName, row, reload);
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] tableName, byte[] row, boolean reload)
      throws IOException {
    return delegate.getRegionLocation(tableName, row, reload);
  }

  @Override
  public void processBatch(List<? extends Row> actions, TableName tableName, ExecutorService pool,
      Object[] results) throws IOException, InterruptedException {
    delegate.processBatch(actions, tableName, pool, results);
  }

  @Override
  public void processBatch(List<? extends Row> actions, byte[] tableName, ExecutorService pool,
      Object[] results) throws IOException, InterruptedException {
    delegate.processBatch(actions, tableName, pool, results);
  }

  @Override
  public <R> void processBatchCallback(List<? extends Row> list, TableName tableName,
      ExecutorService pool, Object[] results, Callback<R> callback) throws IOException,
      InterruptedException {
    delegate.processBatchCallback(list, tableName, pool, results, callback);
  }

  @Override
  public <R> void processBatchCallback(List<? extends Row> list, byte[] tableName,
      ExecutorService pool, Object[] results, Callback<R> callback) throws IOException,
      InterruptedException {
    delegate.processBatchCallback(list, tableName, pool, results, callback);
  }

  @Override
  public void setRegionCachePrefetch(TableName tableName, boolean enable) {
    delegate.setRegionCachePrefetch(tableName, enable);
  }

  @Override
  public void setRegionCachePrefetch(byte[] tableName, boolean enable) {
    delegate.setRegionCachePrefetch(tableName, enable);
  }

  @Override
  public boolean getRegionCachePrefetch(TableName tableName) {
    return delegate.getRegionCachePrefetch(tableName);
  }

  @Override
  public boolean getRegionCachePrefetch(byte[] tableName) {
    return delegate.getRegionCachePrefetch(tableName);
  }

  @Override
  public int getCurrentNrHRS() throws IOException {
    return delegate.getCurrentNrHRS();
  }

  @Override
  public HTableDescriptor[] getHTableDescriptorsByTableName(List<TableName> tableNames)
      throws IOException {
    return delegate.getHTableDescriptorsByTableName(tableNames);
  }

  @Override
  public HTableDescriptor[] getHTableDescriptors(List<String> tableNames) throws IOException {
    return delegate.getHTableDescriptors(tableNames);
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public void clearCaches(ServerName sn) {
    delegate.clearCaches(sn);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void deleteCachedRegionLocation(HRegionLocation location) {
    delegate.deleteCachedRegionLocation(location);
  }

  @Override
  public MasterKeepAliveConnection getKeepAliveMasterService()
      throws MasterNotRunningException {
    return delegate.getKeepAliveMasterService();
  }

  @Override
  public boolean isDeadServer(ServerName serverName) {
    return delegate.isDeadServer(serverName);
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return ng; // don't use nonces for coprocessor connection
  }

  @Override
  public AsyncProcess getAsyncProcess() {
    return delegate.getAsyncProcess();
  }

  @Override
  public RegionLocations locateRegionAll(TableName tableName, byte[] row) throws IOException {
    return delegate.locateRegionAll(tableName, row);
  }
}