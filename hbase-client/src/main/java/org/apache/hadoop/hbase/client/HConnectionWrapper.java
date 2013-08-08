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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
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
  public HTableInterface getTable(String tableName) throws IOException {
    return hconnection.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(byte[] tableName) throws IOException {
    return hconnection.getTable(tableName);
  }

  @Override
  public HTableInterface getTable(String tableName, ExecutorService pool)  throws IOException {
    return hconnection.getTable(tableName, pool);
  }

  @Override
  public HTableInterface getTable(byte[] tableName, ExecutorService pool)  throws IOException {
    return hconnection.getTable(tableName, pool);
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
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return hconnection.isTableEnabled(tableName);
  }

  @Override
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return hconnection.isTableDisabled(tableName);
  }

  @Override
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return hconnection.isTableAvailable(tableName);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    return hconnection.isTableAvailable(tableName, splitKeys);
  }

  @Override
  public boolean isTableAvailable(byte[] tableName, byte[][] splitKeys) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName), splitKeys);
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    return hconnection.listTables();
  }

  @Override
  public HTableDescriptor getHTableDescriptor(TableName tableName) throws IOException {
    return hconnection.getHTableDescriptor(tableName);
  }

  @Override
  public HTableDescriptor getHTableDescriptor(byte[] tableName) throws IOException {
    return getHTableDescriptor(TableName.valueOf(tableName));
  }

  @Override
  public HRegionLocation locateRegion(TableName tableName, byte[] row) throws IOException {
    return hconnection.locateRegion(tableName, row);
  }

  @Override
  public HRegionLocation locateRegion(byte[] tableName, byte[] row) throws IOException {
    return locateRegion(TableName.valueOf(tableName), row);
  }

  @Override
  public void clearRegionCache() {
    hconnection.clearRegionCache();
  }

  @Override
  public void clearRegionCache(TableName tableName) {
    hconnection.clearRegionCache(tableName);
  }

  @Override
  public void clearRegionCache(byte[] tableName) {
    clearRegionCache(TableName.valueOf(tableName));
  }

  @Override
  public void deleteCachedRegionLocation(HRegionLocation location) {
    hconnection.deleteCachedRegionLocation(location);
  }

  @Override
  public HRegionLocation relocateRegion(TableName tableName, byte[] row) throws IOException {
    return hconnection.relocateRegion(tableName, row);
  }

  @Override
  public HRegionLocation relocateRegion(byte[] tableName, byte[] row) throws IOException {
    return relocateRegion(TableName.valueOf(tableName), row);
  }

  @Override
  public void updateCachedLocations(TableName tableName,
                                    byte[] rowkey,
                                    Object exception,
                                    HRegionLocation source) {
    hconnection.updateCachedLocations(tableName, rowkey, exception, source);
  }

  @Override
  public void updateCachedLocations(byte[] tableName,
                                    byte[] rowkey,
                                    Object exception,
                                    HRegionLocation source) {
    updateCachedLocations(TableName.valueOf(tableName), rowkey, exception, source);
  }

  @Override
  public HRegionLocation locateRegion(byte[] regionName) throws IOException {
    return hconnection.locateRegion(regionName);
  }

  @Override
  public List<HRegionLocation> locateRegions(TableName tableName) throws IOException {
    return hconnection.locateRegions(tableName);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName) throws IOException {
    return locateRegions(TableName.valueOf(tableName));
  }

  @Override
  public List<HRegionLocation> locateRegions(TableName tableName,
                                             boolean useCache,
                                             boolean offlined) throws IOException {
    return hconnection.locateRegions(tableName, useCache, offlined);
  }

  @Override
  public List<HRegionLocation> locateRegions(byte[] tableName,
                                             boolean useCache,
                                             boolean offlined) throws IOException {
    return locateRegions(TableName.valueOf(tableName));
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
  public HRegionLocation getRegionLocation(TableName tableName,
                                           byte[] row, boolean reload) throws IOException {
    return hconnection.getRegionLocation(tableName, row, reload);
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] tableName,
                                           byte[] row, boolean reload) throws IOException {
    return getRegionLocation(TableName.valueOf(tableName), row, reload);
  }

  @Override
  public void processBatch(List<? extends Row> actions, TableName tableName, ExecutorService pool,
                           Object[] results) throws IOException, InterruptedException {
    hconnection.processBatch(actions, tableName, pool, results);
  }

  @Override
  public void processBatch(List<? extends Row> actions, byte[] tableName, ExecutorService pool,
                           Object[] results) throws IOException, InterruptedException {
    processBatch(actions, TableName.valueOf(tableName), pool, results);
  }

  @Override
  public <R> void processBatchCallback(List<? extends Row> list, TableName tableName,
                                       ExecutorService pool,
                                       Object[] results,
                                       Callback<R> callback)
      throws IOException, InterruptedException {
    hconnection.processBatchCallback(list, tableName, pool, results, callback);
  }

  @Override
  public <R> void processBatchCallback(List<? extends Row> list, byte[] tableName,
                                       ExecutorService pool,
                                       Object[] results,
                                       Callback<R> callback)
      throws IOException, InterruptedException {
    processBatchCallback(list, TableName.valueOf(tableName), pool, results, callback);
  }

  @Override
  public void setRegionCachePrefetch(TableName tableName, boolean enable) {
    hconnection.setRegionCachePrefetch(tableName, enable);
  }

  @Override
  public void setRegionCachePrefetch(byte[] tableName, boolean enable) {
    setRegionCachePrefetch(TableName.valueOf(tableName), enable);
  }

  @Override
  public boolean getRegionCachePrefetch(TableName tableName) {
    return hconnection.getRegionCachePrefetch(tableName);
  }

  @Override
  public boolean getRegionCachePrefetch(byte[] tableName) {
    return getRegionCachePrefetch(TableName.valueOf(tableName));
  }

  @Override
  public int getCurrentNrHRS() throws IOException {
    return hconnection.getCurrentNrHRS();
  }

  @Override
  public HTableDescriptor[] getHTableDescriptorsByTableName(
      List<TableName> tableNames) throws IOException {
    return hconnection.getHTableDescriptorsByTableName(tableNames);
  }

  @Override
  public HTableDescriptor[] getHTableDescriptors(
      List<String> names) throws IOException {
    List<TableName> tableNames = new ArrayList<TableName>(names.size());
    for(String name : names) {
      tableNames.add(TableName.valueOf(name));
    }
    return getHTableDescriptorsByTableName(tableNames);
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
