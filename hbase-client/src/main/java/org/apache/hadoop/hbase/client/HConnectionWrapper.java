/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.exceptions.MasterNotRunningException;
import org.apache.hadoop.hbase.exceptions.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MasterAdminService;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.MasterMonitorService;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This is a HConnection wrapper. Whenever a RPC connection is created,
 * this class makes sure the specific user is used as the ticket. We assume
 * just these methods will create any RPC connection based on the default
 * HConnection implementation: getClient, getAdmin, getKeepAliveMasterMonitorService,
 * and getKeepAliveMasterAdminService.
 *
 * This class is put here only because the HConnection interface exposes
 * packaged private class MasterMonitorKeepAliveConnection and
 * MasterAddKeepAliveConnection.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class HConnectionWrapper implements HConnection {
  private final UserGroupInformation ugi;
  private final HConnection hconnection;

  public HConnectionWrapper(final UserGroupInformation ugi,
      final HConnection hconnection) {
    this.hconnection = hconnection;
    this.ugi = ugi;
  }

  @Override
  public void abort(String why, Throwable e) {
    hconnection.abort(why, e);
  }

  @Override
  public boolean isAborted() {
    return hconnection.isAborted();
  }

  @Override
  public void close() throws IOException {
    hconnection.close();
  }

  @Override
  public Configuration getConfiguration() {
    return hconnection.getConfiguration();
  }

  @Override
  public boolean isMasterRunning() throws MasterNotRunningException,
      ZooKeeperConnectionException {
    return hconnection.isMasterRunning();
  }

  @Override
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return hconnection.isTableEnabled(tableName);
  }

  @Override
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return hconnection.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return hconnection.isTableAvailable(tableName);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName, byte[][] splitKeys)
      throws IOException {
    return hconnection.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return hconnection.listTables();
  }

  @Override
  public HTableDescriptor getHTableDescriptor(byte[] tableName)
      throws IOException {
    return hconnection.getHTableDescriptor(tableName);
  }

  @Override
  public HRegionLocation locateRegion(byte[] tableName, byte[] row)
      throws IOException {
    return hconnection.locateRegion(tableName, row);
  }

  @Override
  public void clearRegionCache() {
    hconnection.clearRegionCache();
  }

  @Override
  public void clearRegionCache(byte[] tableName) {
    hconnection.clearRegionCache(tableName);
  }

  @Override
  public void deleteCachedRegionLocation(HRegionLocation location) {
    hconnection.deleteCachedRegionLocation(location);
  }

  @Override
  public HRegionLocation relocateRegion(byte[] tableName, byte[] row)
      throws IOException {
    return hconnection.relocateRegion(tableName, row);
  }

  @Override
  public void updateCachedLocations(byte[] tableName, byte[] rowkey,
      Object exception, HRegionLocation source) {
    hconnection.updateCachedLocations(tableName, rowkey, exception, source);
  }

  @Override
  public HRegionLocation locateRegion(byte[] regionName) throws IOException {
    return hconnection.locateRegion(regionName);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName)
      throws IOException {
    return hconnection.locateRegions(tableName);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName,
      boolean useCache, boolean offlined) throws IOException {
    return hconnection.locateRegions(tableName, useCache, offlined);
  }

  @Override
  public MasterAdminService.BlockingInterface getMasterAdmin() throws IOException {
    return hconnection.getMasterAdmin();
  }

  @Override
  public MasterMonitorService.BlockingInterface getMasterMonitor()
      throws IOException {
    return hconnection.getMasterMonitor();
  }

  @Override
  public AdminService.BlockingInterface getAdmin(
      ServerName serverName) throws IOException {
    return hconnection.getAdmin(serverName);
  }

  @Override
  public ClientService.BlockingInterface getClient(
      final ServerName serverName) throws IOException {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<ClientService.BlockingInterface>() {
        @Override
        public ClientService.BlockingInterface run() throws IOException {
          return hconnection.getClient(serverName);
         }
       });
     } catch (InterruptedException e) {
       Thread.currentThread().interrupt();
       throw new IOException(e);
     }
  }

  @Override
  public AdminService.BlockingInterface getAdmin(
      final ServerName serverName, final boolean getMaster) throws IOException {
    try {
     return ugi.doAs(new PrivilegedExceptionAction<AdminService.BlockingInterface>() {
       @Override
       public AdminService.BlockingInterface run() throws IOException {
         return hconnection.getAdmin(serverName, getMaster);
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] tableName, byte[] row,
      boolean reload) throws IOException {
    return hconnection.getRegionLocation(tableName, row, reload);
  }

  @Override
  public void processBatch(List<? extends Row> actions, byte[] tableName,
      ExecutorService pool, Object[] results) throws IOException,
      InterruptedException {
    hconnection.processBatch(actions, tableName, pool, results);
  }

  @Override
  public <R> void processBatchCallback(List<? extends Row> list,
      byte[] tableName, ExecutorService pool, Object[] results,
      Callback<R> callback) throws IOException, InterruptedException {
    hconnection.processBatchCallback(list, tableName, pool, results, callback);
  }

  @Override
  public void setRegionCachePrefetch(byte[] tableName, boolean enable) {
    hconnection.setRegionCachePrefetch(tableName, enable);
  }

  @Override
  public boolean getRegionCachePrefetch(byte[] tableName) {
    return hconnection.getRegionCachePrefetch(tableName);
  }

  @Override
  public int getCurrentNrHRS() throws IOException {
    return hconnection.getCurrentNrHRS();
  }

  @Override
  public HTableDescriptor[] getHTableDescriptors(List<String> tableNames)
      throws IOException {
    return hconnection.getHTableDescriptors(tableNames);
  }

  @Override
  public boolean isClosed() {
    return hconnection.isClosed();
  }

  @Override
  public void clearCaches(ServerName sn) {
    hconnection.clearCaches(sn);
  }

  @Override
  public MasterMonitorKeepAliveConnection getKeepAliveMasterMonitorService()
      throws MasterNotRunningException {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<MasterMonitorKeepAliveConnection>() {
        @Override
        public MasterMonitorKeepAliveConnection run() throws MasterNotRunningException {
          return hconnection.getKeepAliveMasterMonitorService();
         }
       });
     } catch (IOException ie) {
       throw new MasterNotRunningException(ie);
     } catch (InterruptedException e) {
       Thread.currentThread().interrupt();
       throw new MasterNotRunningException(e);
     }
  }

  @Override
  public MasterAdminKeepAliveConnection getKeepAliveMasterAdminService()
      throws MasterNotRunningException {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<MasterAdminKeepAliveConnection>() {
        @Override
        public MasterAdminKeepAliveConnection run() throws MasterNotRunningException {
          return hconnection.getKeepAliveMasterAdminService();
         }
       });
    } catch (IOException ie) {
      throw new MasterNotRunningException(ie);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MasterNotRunningException(e);
    }
  }

  @Override
  public boolean isDeadServer(ServerName serverName) {
    return hconnection.isDeadServer(serverName);
  }
}
