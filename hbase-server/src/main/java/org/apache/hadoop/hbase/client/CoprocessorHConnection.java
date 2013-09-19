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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.MasterAdminKeepAliveConnection;
import org.apache.hadoop.hbase.client.MasterMonitorKeepAliveConnection;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

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
public class CoprocessorHConnection implements HConnection {

  /**
   * Create an unmanaged {@link HConnection} based on the environment in which we are running the
   * coprocessor. The {@link HConnection} must be externally cleaned up (we bypass the usual HTable
   * cleanup mechanisms since we own everything).
   * @param env environment hosting the {@link HConnection}
   * @return an unmanaged {@link HConnection}.
   * @throws IOException if we cannot create the basic connection
   */
  public static HConnection getConnectionForEnvironment(CoprocessorEnvironment env)
      throws IOException {
    HConnection connection = HConnectionManager.createConnection(env.getConfiguration());
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

  private HConnection delegate;
  private ServerName serverName;
  private HRegionServer server;

  public CoprocessorHConnection(HConnection delegate, HRegionServer server) {
    this.server = server;
    this.serverName = server.getServerName();
    this.delegate = delegate;
  }

  public org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface
      getClient(ServerName serverName) throws IOException {
    // client is trying to reach off-server, so we can't do anything special
    if (!this.serverName.equals(serverName)) {
      return delegate.getClient(serverName);
    }
    // the client is attempting to write to the same regionserver, we can short-circuit to our
    // local regionserver
    final BlockingService blocking = ClientService.newReflectiveBlockingService(this.server);
    final RpcServerInterface rpc = this.server.getRpcServer();

    final MonitoredRPCHandler status =
        TaskMonitor.get().createRPCStatus(Thread.currentThread().getName());
    status.pause("Setting up server-local call");

    final long timestamp = EnvironmentEdgeManager.currentTimeMillis();
    BlockingRpcChannel channel = new BlockingRpcChannel() {

      @Override
      public Message callBlockingMethod(MethodDescriptor method, RpcController controller,
          Message request, Message responsePrototype) throws ServiceException {
        try {
          // we never need a cell-scanner - everything is already fully formed
          return rpc.call(blocking, method, request, null, timestamp, status).getFirst();
        } catch (IOException e) {
          throw new ServiceException(e);
        }
      }
    };
    return ClientService.newBlockingStub(channel);
  }

  public void abort(String why, Throwable e) {
    delegate.abort(why, e);
  }

  public boolean isAborted() {
    return delegate.isAborted();
  }

  public Configuration getConfiguration() {
    return delegate.getConfiguration();
  }

  public HTableInterface getTable(String tableName) throws IOException {
    return delegate.getTable(tableName);
  }

  public HTableInterface getTable(byte[] tableName) throws IOException {
    return delegate.getTable(tableName);
  }

  public HTableInterface getTable(TableName tableName) throws IOException {
    return delegate.getTable(tableName);
  }

  public HTableInterface getTable(String tableName, ExecutorService pool) throws IOException {
    return delegate.getTable(tableName, pool);
  }

  public HTableInterface getTable(byte[] tableName, ExecutorService pool) throws IOException {
    return delegate.getTable(tableName, pool);
  }

  public HTableInterface getTable(TableName tableName, ExecutorService pool) throws IOException {
    return delegate.getTable(tableName, pool);
  }

  public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
    return delegate.isMasterRunning();
  }

  public boolean isTableEnabled(TableName tableName) throws IOException {
    return delegate.isTableEnabled(tableName);
  }

  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return delegate.isTableEnabled(tableName);
  }

  public boolean isTableDisabled(TableName tableName) throws IOException {
    return delegate.isTableDisabled(tableName);
  }

  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return delegate.isTableDisabled(tableName);
  }

  public boolean isTableAvailable(TableName tableName) throws IOException {
    return delegate.isTableAvailable(tableName);
  }

  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return delegate.isTableAvailable(tableName);
  }

  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    return delegate.isTableAvailable(tableName, splitKeys);
  }

  public boolean isTableAvailable(byte[] tableName, byte[][] splitKeys) throws IOException {
    return delegate.isTableAvailable(tableName, splitKeys);
  }

  public HTableDescriptor[] listTables() throws IOException {
    return delegate.listTables();
  }

  public String[] getTableNames() throws IOException {
    return delegate.getTableNames();
  }

  public TableName[] listTableNames() throws IOException {
    return delegate.listTableNames();
  }

  public HTableDescriptor getHTableDescriptor(TableName tableName) throws IOException {
    return delegate.getHTableDescriptor(tableName);
  }

  public HTableDescriptor getHTableDescriptor(byte[] tableName) throws IOException {
    return delegate.getHTableDescriptor(tableName);
  }

  public HRegionLocation locateRegion(TableName tableName, byte[] row) throws IOException {
    return delegate.locateRegion(tableName, row);
  }

  public HRegionLocation locateRegion(byte[] tableName, byte[] row) throws IOException {
    return delegate.locateRegion(tableName, row);
  }

  public void clearRegionCache() {
    delegate.clearRegionCache();
  }

  public void clearRegionCache(TableName tableName) {
    delegate.clearRegionCache(tableName);
  }

  public void clearRegionCache(byte[] tableName) {
    delegate.clearRegionCache(tableName);
  }

  public HRegionLocation relocateRegion(TableName tableName, byte[] row) throws IOException {
    return delegate.relocateRegion(tableName, row);
  }

  public HRegionLocation relocateRegion(byte[] tableName, byte[] row) throws IOException {
    return delegate.relocateRegion(tableName, row);
  }

  public void updateCachedLocations(TableName tableName, byte[] rowkey, Object exception,
      HRegionLocation source) {
    delegate.updateCachedLocations(tableName, rowkey, exception, source);
  }

  public void updateCachedLocations(byte[] tableName, byte[] rowkey, Object exception,
      HRegionLocation source) {
    delegate.updateCachedLocations(tableName, rowkey, exception, source);
  }

  public HRegionLocation locateRegion(byte[] regionName) throws IOException {
    return delegate.locateRegion(regionName);
  }

  public List<HRegionLocation> locateRegions(TableName tableName) throws IOException {
    return delegate.locateRegions(tableName);
  }

  public List<HRegionLocation> locateRegions(byte[] tableName) throws IOException {
    return delegate.locateRegions(tableName);
  }

  public List<HRegionLocation>
      locateRegions(TableName tableName, boolean useCache, boolean offlined) throws IOException {
    return delegate.locateRegions(tableName, useCache, offlined);
  }

  public List<HRegionLocation> locateRegions(byte[] tableName, boolean useCache, boolean offlined)
      throws IOException {
    return delegate.locateRegions(tableName, useCache, offlined);
  }

  public BlockingInterface getMasterAdmin() throws IOException {
    return delegate.getMasterAdmin();
  }

  public
      org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.MasterMonitorService.BlockingInterface
      getMasterMonitor() throws IOException {
    return delegate.getMasterMonitor();
  }

  public org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface
      getAdmin(ServerName serverName) throws IOException {
    return delegate.getAdmin(serverName);
  }

  public org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface
      getAdmin(ServerName serverName, boolean getMaster) throws IOException {
    return delegate.getAdmin(serverName, getMaster);
  }

  public HRegionLocation getRegionLocation(TableName tableName, byte[] row, boolean reload)
      throws IOException {
    return delegate.getRegionLocation(tableName, row, reload);
  }

  public HRegionLocation getRegionLocation(byte[] tableName, byte[] row, boolean reload)
      throws IOException {
    return delegate.getRegionLocation(tableName, row, reload);
  }

  public void processBatch(List<? extends Row> actions, TableName tableName, ExecutorService pool,
      Object[] results) throws IOException, InterruptedException {
    delegate.processBatch(actions, tableName, pool, results);
  }

  public void processBatch(List<? extends Row> actions, byte[] tableName, ExecutorService pool,
      Object[] results) throws IOException, InterruptedException {
    delegate.processBatch(actions, tableName, pool, results);
  }

  public <R> void processBatchCallback(List<? extends Row> list, TableName tableName,
      ExecutorService pool, Object[] results, Callback<R> callback) throws IOException,
      InterruptedException {
    delegate.processBatchCallback(list, tableName, pool, results, callback);
  }

  public <R> void processBatchCallback(List<? extends Row> list, byte[] tableName,
      ExecutorService pool, Object[] results, Callback<R> callback) throws IOException,
      InterruptedException {
    delegate.processBatchCallback(list, tableName, pool, results, callback);
  }

  public void setRegionCachePrefetch(TableName tableName, boolean enable) {
    delegate.setRegionCachePrefetch(tableName, enable);
  }

  public void setRegionCachePrefetch(byte[] tableName, boolean enable) {
    delegate.setRegionCachePrefetch(tableName, enable);
  }

  public boolean getRegionCachePrefetch(TableName tableName) {
    return delegate.getRegionCachePrefetch(tableName);
  }

  public boolean getRegionCachePrefetch(byte[] tableName) {
    return delegate.getRegionCachePrefetch(tableName);
  }

  public int getCurrentNrHRS() throws IOException {
    return delegate.getCurrentNrHRS();
  }

  public HTableDescriptor[] getHTableDescriptorsByTableName(List<TableName> tableNames)
      throws IOException {
    return delegate.getHTableDescriptorsByTableName(tableNames);
  }

  public HTableDescriptor[] getHTableDescriptors(List<String> tableNames) throws IOException {
    return delegate.getHTableDescriptors(tableNames);
  }

  public boolean isClosed() {
    return delegate.isClosed();
  }

  public void clearCaches(ServerName sn) {
    delegate.clearCaches(sn);
  }

  public void close() throws IOException {
    delegate.close();
  }

  public void deleteCachedRegionLocation(HRegionLocation location) {
    delegate.deleteCachedRegionLocation(location);
  }

  public MasterMonitorKeepAliveConnection getKeepAliveMasterMonitorService()
      throws MasterNotRunningException {
    return delegate.getKeepAliveMasterMonitorService();
  }

  public MasterAdminKeepAliveConnection getKeepAliveMasterAdminService()
      throws MasterNotRunningException {
    return delegate.getKeepAliveMasterAdminService();
  }

  public boolean isDeadServer(ServerName serverName) {
    return delegate.isDeadServer(serverName);
  }
}