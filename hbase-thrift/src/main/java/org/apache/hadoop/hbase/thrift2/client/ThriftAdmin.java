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
package org.apache.hadoop.hbase.thrift2.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ServerType;
import org.apache.hadoop.hbase.client.LogEntry;
import org.apache.hadoop.hbase.client.LogQueryFilter;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.client.OnlineLogRecord;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.thrift2.ThriftUtilities;
import org.apache.hadoop.hbase.thrift2.generated.TColumnFamilyDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TLogQueryFilter;
import org.apache.hadoop.hbase.thrift2.generated.TNamespaceDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TOnlineLogRecord;
import org.apache.hadoop.hbase.thrift2.generated.TServerName;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TTableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ThriftAdmin implements Admin {

  private THBaseService.Client client;
  private TTransport transport;
  private int operationTimeout;
  private int syncWaitTimeout;
  private Configuration conf;


  public ThriftAdmin(THBaseService.Client client, TTransport tTransport, Configuration conf) {
    this.client = client;
    this.transport = tTransport;
    this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.syncWaitTimeout = conf.getInt("hbase.client.sync.wait.timeout.msec", 10 * 60000); // 10min
    this.conf = conf;
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
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public void close() {
    transport.close();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      return client.tableExists(tTableName);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Connection getConnection() {
    throw new NotImplementedException("getConnection not supported in ThriftAdmin");
  }

  @Override
  public List<TableDescriptor> listTableDescriptors() throws IOException {
    return listTableDescriptors((Pattern) null);
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(boolean includeSysTables) throws IOException {
    return listTableDescriptors(null, includeSysTables);
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern pattern) throws IOException {
    return listTableDescriptors(pattern, false);
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
      throws IOException {
    try {
      String regex = (pattern == null ? null : pattern.toString());
      List<TTableDescriptor> tTableDescriptors = client
          .getTableDescriptorsByPattern(regex, includeSysTables);
      return ThriftUtilities.tableDescriptorsFromThrift(tTableDescriptors);

    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    return listTableNames(null);
  }

  @Override
  public TableName[] listTableNames(Pattern pattern) throws IOException {
    return listTableNames(pattern, false);
  }

  @Override
  public TableName[] listTableNames(Pattern pattern, boolean includeSysTables) throws IOException {
    String regex = (pattern == null ? null : pattern.toString());
    try {
      List<TTableName> tTableNames = client.getTableNamesByPattern(regex, includeSysTables);
      return ThriftUtilities.tableNamesArrayFromThrift(tTableNames);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public TableDescriptor getDescriptor(TableName tableName)
      throws TableNotFoundException, IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      TTableDescriptor tTableDescriptor = client.getTableDescriptor(tTableName);
      return ThriftUtilities.tableDescriptorFromThrift(tTableDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(byte[] name) throws IOException {
    try {
      List<TTableDescriptor> tTableDescriptors = client
          .getTableDescriptorsByNamespace(Bytes.toString(name));
      return ThriftUtilities.tableDescriptorsFromThrift(tTableDescriptors);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    try {
      List<TTableName> tTableNames = client.getTableNamesByNamespace(name);
      return ThriftUtilities.tableNamesArrayFromThrift(tTableNames);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createTable(TableDescriptor desc) throws IOException {
    createTable(desc, null);
  }

  @Override
  public void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    if(numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if(Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      createTable(desc, new byte[][]{startKey, endKey});
      return;
    }
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if(splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys);
  }

  @Override
  public void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException {
    TTableDescriptor tTableDescriptor = ThriftUtilities.tableDescriptorFromHBase(desc);
    List<ByteBuffer> splitKeyInBuffer = ThriftUtilities.splitKeyFromHBase(splitKeys);
    try {
      client.createTable(tTableDescriptor, splitKeyInBuffer);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteTable(TableName tableName) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      client.deleteTable(tTableName);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      client.truncateTable(tTableName, preserveSplits);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void enableTable(TableName tableName) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      client.enableTable(tTableName);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void disableTable(TableName tableName) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      client.disableTable(tTableName);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      return client.isTableEnabled(tTableName);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      return client.isTableDisabled(tTableName);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      return client.isTableAvailable(tTableName);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    TColumnFamilyDescriptor tColumnFamilyDescriptor = ThriftUtilities
        .columnFamilyDescriptorFromHBase(columnFamily);
    try {
      client.addColumnFamily(tTableName, tColumnFamilyDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteColumnFamily(TableName tableName, byte[] columnFamily) throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    try {
      client.deleteColumnFamily(tTableName, ByteBuffer.wrap(columnFamily));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void modifyColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    TTableName tTableName = ThriftUtilities.tableNameFromHBase(tableName);
    TColumnFamilyDescriptor tColumnFamilyDescriptor = ThriftUtilities
        .columnFamilyDescriptorFromHBase(columnFamily);
    try {
      client.modifyColumnFamily(tTableName, tColumnFamilyDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void modifyTable(TableDescriptor td) throws IOException {
    TTableDescriptor tTableDescriptor = ThriftUtilities
        .tableDescriptorFromHBase(td);
    try {
      client.modifyTable(tTableDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
    TNamespaceDescriptor tNamespaceDescriptor = ThriftUtilities
        .namespaceDescriptorFromHBase(descriptor);
    try {
      client.modifyNamespace(tNamespaceDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteNamespace(String name) throws IOException {
    try {
      client.deleteNamespace(name);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name)
      throws NamespaceNotFoundException, IOException {
    try {
      TNamespaceDescriptor tNamespaceDescriptor = client.getNamespaceDescriptor(name);
      return ThriftUtilities.namespaceDescriptorFromThrift(tNamespaceDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String[] listNamespaces() throws IOException {
    try {
      List<String> tNamespaces = client.listNamespaces();
      return tNamespaces.toArray(new String[tNamespaces.size()]);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    try {
      List<TNamespaceDescriptor> tNamespaceDescriptors = client.listNamespaceDescriptors();
      return ThriftUtilities.namespaceDescriptorsFromThrift(tNamespaceDescriptors);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    TNamespaceDescriptor tNamespaceDescriptor = ThriftUtilities
        .namespaceDescriptorFromHBase(descriptor);
    try {
      client.createNamespace(tNamespaceDescriptor);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean switchRpcThrottle(boolean enable) throws IOException {
    throw new NotImplementedException("switchRpcThrottle by pattern not supported in ThriftAdmin");
  }

  @Override
  public boolean isRpcThrottleEnabled() throws IOException {
    throw new NotImplementedException(
        "isRpcThrottleEnabled by pattern not supported in ThriftAdmin");
  }

  @Override
  public boolean exceedThrottleQuotaSwitch(boolean enable) throws IOException {
    throw new NotImplementedException(
        "exceedThrottleQuotaSwitch by pattern not supported in ThriftAdmin");
  }

  @Override
  public List<TableDescriptor> listTableDescriptors(List<TableName> tableNames) throws IOException {
    throw new NotImplementedException("listTableDescriptors not supported in ThriftAdmin"
        + ", use getDescriptor to get descriptors one by one");
  }

  @Override
  public List<RegionInfo> getRegions(ServerName serverName) {
    throw new NotImplementedException("getRegions not supported in ThriftAdmin");
  }

  @Override
  public void flush(TableName tableName) {
    throw new NotImplementedException("flush not supported in ThriftAdmin");

  }

  @Override
  public void flush(TableName tableName, byte[] columnFamily) {
    throw new NotImplementedException("flush not supported in ThriftAdmin");
  }

  @Override
  public void flushRegion(byte[] regionName) {
    throw new NotImplementedException("flushRegion not supported in ThriftAdmin");

  }

  @Override
  public void flushRegion(byte[] regionName, byte[] columnFamily) {
    throw new NotImplementedException("flushRegion not supported in ThriftAdmin");
  }

  @Override
  public void flushRegionServer(ServerName serverName) {
    throw new NotImplementedException("flushRegionServer not supported in ThriftAdmin");

  }

  @Override
  public void compact(TableName tableName) {
    throw new NotImplementedException("compact not supported in ThriftAdmin");

  }

  @Override
  public void compactRegion(byte[] regionName) {
    throw new NotImplementedException("compactRegion not supported in ThriftAdmin");

  }

  @Override
  public void compact(TableName tableName, byte[] columnFamily) {
    throw new NotImplementedException("compact not supported in ThriftAdmin");

  }

  @Override
  public void compactRegion(byte[] regionName, byte[] columnFamily) {
    throw new NotImplementedException("compactRegion not supported in ThriftAdmin");

  }

  @Override
  public void compact(TableName tableName, CompactType compactType) {
    throw new NotImplementedException("compact not supported in ThriftAdmin");

  }

  @Override
  public void compact(TableName tableName, byte[] columnFamily, CompactType compactType) {
    throw new NotImplementedException("compact not supported in ThriftAdmin");

  }

  @Override
  public void majorCompact(TableName tableName) {
    throw new NotImplementedException("majorCompact not supported in ThriftAdmin");

  }

  @Override
  public void majorCompactRegion(byte[] regionName) {
    throw new NotImplementedException("majorCompactRegion not supported in ThriftAdmin");

  }

  @Override
  public void majorCompact(TableName tableName, byte[] columnFamily) {
    throw new NotImplementedException("majorCompact not supported in ThriftAdmin");

  }

  @Override
  public void majorCompactRegion(byte[] regionName, byte[] columnFamily) {
    throw new NotImplementedException("majorCompactRegion not supported in ThriftAdmin");

  }

  @Override
  public void majorCompact(TableName tableName, CompactType compactType) {
    throw new NotImplementedException("majorCompact not supported in ThriftAdmin");

  }

  @Override
  public void majorCompact(TableName tableName, byte[] columnFamily, CompactType compactType) {
    throw new NotImplementedException("majorCompact not supported in ThriftAdmin");

  }

  @Override
  public Map<ServerName, Boolean> compactionSwitch(boolean switchState,
      List<String> serverNamesList) {
    throw new NotImplementedException("compactionSwitch not supported in ThriftAdmin");
  }

  @Override
  public void compactRegionServer(ServerName serverName) {
    throw new NotImplementedException("compactRegionServer not supported in ThriftAdmin");

  }

  @Override
  public void majorCompactRegionServer(ServerName serverName) {
    throw new NotImplementedException("majorCompactRegionServer not supported in ThriftAdmin");

  }

  @Override
  public void move(byte[] encodedRegionName) {
    throw new NotImplementedException("move not supported in ThriftAdmin");
  }

  @Override
  public void move(byte[] encodedRegionName, ServerName destServerName) {
    throw new NotImplementedException("move not supported in ThriftAdmin");
  }

  @Override
  public void assign(byte[] regionName) {
    throw new NotImplementedException("assign not supported in ThriftAdmin");

  }

  @Override
  public void unassign(byte[] regionName) {
    throw new NotImplementedException("unassign not supported in ThriftAdmin");
  }

  @Override
  public void offline(byte[] regionName) {
    throw new NotImplementedException("offline not supported in ThriftAdmin");

  }

  @Override
  public boolean balancerSwitch(boolean onOrOff, boolean synchronous) {
    throw new NotImplementedException("balancerSwitch not supported in ThriftAdmin");
  }

  @Override
  public boolean balance() {
    throw new NotImplementedException("balance not supported in ThriftAdmin");
  }

  @Override
  public boolean balance(boolean force) {
    throw new NotImplementedException("balance not supported in ThriftAdmin");
  }

  @Override
  public BalanceResponse balance(BalanceRequest request) throws IOException {
    throw new NotImplementedException("balance not supported in ThriftAdmin");
  }

  @Override
  public boolean isBalancerEnabled() {
    throw new NotImplementedException("isBalancerEnabled not supported in ThriftAdmin");
  }

  @Override
  public CacheEvictionStats clearBlockCache(TableName tableName) {
    throw new NotImplementedException("clearBlockCache not supported in ThriftAdmin");
  }

  @Override
  public boolean normalize(NormalizeTableFilterParams ntfp) {
    throw new NotImplementedException("normalize not supported in ThriftAdmin");
  }

  @Override
  public boolean isNormalizerEnabled() {
    throw new NotImplementedException("isNormalizerEnabled not supported in ThriftAdmin");
  }

  @Override
  public boolean normalizerSwitch(boolean on) {
    throw new NotImplementedException("normalizerSwitch not supported in ThriftAdmin");
  }

  @Override
  public boolean catalogJanitorSwitch(boolean onOrOff) {
    throw new NotImplementedException("catalogJanitorSwitch not supported in ThriftAdmin");
  }

  @Override
  public int runCatalogJanitor() {
    throw new NotImplementedException("runCatalogJanitor not supported in ThriftAdmin");
  }

  @Override
  public boolean isCatalogJanitorEnabled() {
    throw new NotImplementedException("isCatalogJanitorEnabled not supported in ThriftAdmin");
  }

  @Override
  public boolean cleanerChoreSwitch(boolean onOrOff) {
    throw new NotImplementedException("cleanerChoreSwitch not supported in ThriftAdmin");
  }

  @Override
  public boolean runCleanerChore() {
    throw new NotImplementedException("runCleanerChore not supported in ThriftAdmin");
  }

  @Override
  public boolean isCleanerChoreEnabled() {
    throw new NotImplementedException("isCleanerChoreEnabled not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> mergeRegionsAsync(byte[] nameOfRegionA, byte[] nameOfRegionB,
      boolean forcible) {
    throw new NotImplementedException("mergeRegionsAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> mergeRegionsAsync(byte[][] nameofRegionsToMerge, boolean forcible) {
    throw new NotImplementedException("mergeRegionsAsync not supported in ThriftAdmin");
  }

  @Override
  public void split(TableName tableName) {
    throw new NotImplementedException("split not supported in ThriftAdmin");
  }

  @Override
  public void split(TableName tableName, byte[] splitPoint) {
    throw new NotImplementedException("split not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName, byte[] splitPoint) {
    throw new NotImplementedException("splitRegionAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> modifyTableAsync(TableDescriptor td) {
    throw new NotImplementedException("modifyTableAsync not supported in ThriftAdmin");
  }

  @Override
  public void shutdown() {
    throw new NotImplementedException("shutdown not supported in ThriftAdmin");

  }

  @Override
  public void stopMaster() {
    throw new NotImplementedException("stopMaster not supported in ThriftAdmin");

  }

  @Override
  public boolean isMasterInMaintenanceMode() {
    throw new NotImplementedException("isMasterInMaintenanceMode not supported in ThriftAdmin");
  }

  @Override
  public void stopRegionServer(String hostnamePort) {
    throw new NotImplementedException("stopRegionServer not supported in ThriftAdmin");

  }

  @Override
  public ClusterMetrics getClusterMetrics(EnumSet<ClusterMetrics.Option> options) {
    throw new NotImplementedException("getClusterMetrics not supported in ThriftAdmin");
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName serverName) {
    throw new NotImplementedException("getRegionMetrics not supported in ThriftAdmin");
  }

  @Override
  public List<RegionMetrics> getRegionMetrics(ServerName serverName, TableName tableName) {
    throw new NotImplementedException("getRegionMetrics not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> createNamespaceAsync(NamespaceDescriptor descriptor) {
    throw new NotImplementedException("createNamespaceAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> modifyNamespaceAsync(NamespaceDescriptor descriptor) {
    throw new NotImplementedException("modifyNamespaceAsync not supported in ThriftAdmin");
  }

  @Override
  public List<RegionInfo> getRegions(TableName tableName) {
    throw new NotImplementedException("getRegions not supported in ThriftAdmin");
  }

  @Override
  public boolean abortProcedure(long procId, boolean mayInterruptIfRunning) {
    throw new NotImplementedException("abortProcedure not supported in ThriftAdmin");
  }

  @Override
  public Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning) {
    throw new NotImplementedException("abortProcedureAsync not supported in ThriftAdmin");
  }

  @Override
  public String getProcedures() {
    throw new NotImplementedException("getProcedures not supported in ThriftAdmin");
  }

  @Override
  public String getLocks() {
    throw new NotImplementedException("getLocks not supported in ThriftAdmin");
  }

  @Override
  public void rollWALWriter(ServerName serverName) {
    throw new NotImplementedException("rollWALWriter not supported in ThriftAdmin");

  }

  @Override
  public CompactionState getCompactionState(TableName tableName) {
    throw new NotImplementedException("getCompactionState not supported in ThriftAdmin");
  }

  @Override
  public CompactionState getCompactionState(TableName tableName, CompactType compactType) {
    throw new NotImplementedException("getCompactionState not supported in ThriftAdmin");
  }

  @Override
  public CompactionState getCompactionStateForRegion(byte[] regionName) {
    throw new NotImplementedException("getCompactionStateForRegion not supported in ThriftAdmin");
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName tableName) {
    throw new NotImplementedException(
        "getLastMajorCompactionTimestamp not supported in ThriftAdmin");
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) {
    throw new NotImplementedException(
        "getLastMajorCompactionTimestampForRegion not supported in ThriftAdmin");
  }

  @Override
  public void snapshot(String snapshotName, TableName tableName) {
    throw new NotImplementedException("snapshot not supported in ThriftAdmin");

  }

  @Override
  public void snapshot(String snapshotName, TableName tableName, SnapshotType type) {
    throw new NotImplementedException("snapshot not supported in ThriftAdmin");

  }

  @Override
  public void snapshot(SnapshotDescription snapshot) {
    throw new NotImplementedException("snapshot not supported in ThriftAdmin");

  }

  @Override
  public Future<Void> snapshotAsync(SnapshotDescription snapshot) {
    throw new NotImplementedException("snapshotAsync not supported in ThriftAdmin");

  }

  @Override
  public boolean isSnapshotFinished(SnapshotDescription snapshot) {
    throw new NotImplementedException("isSnapshotFinished not supported in ThriftAdmin");
  }

  @Override
  public void restoreSnapshot(String snapshotName) {
    throw new NotImplementedException("restoreSnapshot not supported in ThriftAdmin");

  }
  @Override
  public void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot,
      boolean restoreAcl) {
    throw new NotImplementedException("restoreSnapshot not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName, boolean cloneAcl,
    String customSFT) throws IOException, TableExistsException, RestoreSnapshotException {
    throw new NotImplementedException("cloneSnapshotAsync not supported in ThriftAdmin");
  }

  @Override
  public void execProcedure(String signature, String instance, Map<String, String> props) {
    throw new NotImplementedException("execProcedure not supported in ThriftAdmin");

  }

  @Override
  public byte[] execProcedureWithReturn(String signature, String instance,
      Map<String, String> props) {
    throw new NotImplementedException("execProcedureWithReturn not supported in ThriftAdmin");
  }

  @Override
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props) {
    throw new NotImplementedException("isProcedureFinished not supported in ThriftAdmin");
  }

  @Override
  public List<SnapshotDescription> listSnapshots() {
    throw new NotImplementedException("listSnapshots not supported in ThriftAdmin");
  }
  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) {
    throw new NotImplementedException("listSnapshots not supported in ThriftAdmin");
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    throw new NotImplementedException("listTableSnapshots not supported in ThriftAdmin");
  }

  @Override
  public void deleteSnapshot(String snapshotName) {
    throw new NotImplementedException("deleteSnapshot not supported in ThriftAdmin");
  }

  @Override
  public void deleteSnapshots(Pattern pattern) {
    throw new NotImplementedException("deleteSnapshots not supported in ThriftAdmin");
  }

  @Override
  public void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern) {
    throw new NotImplementedException("deleteTableSnapshots not supported in ThriftAdmin");
  }

  @Override
  public void setQuota(QuotaSettings quota) {
    throw new NotImplementedException("setQuota not supported in ThriftAdmin");
  }

  @Override
  public List<QuotaSettings> getQuota(QuotaFilter filter) {
    throw new NotImplementedException("getQuota not supported in ThriftAdmin");
  }

  @Override
  public CoprocessorRpcChannel coprocessorService() {
    throw new NotImplementedException("coprocessorService not supported in ThriftAdmin");
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
    throw new NotImplementedException("coprocessorService not supported in ThriftAdmin");
  }

  @Override
  public void updateConfiguration(ServerName server) {
    throw new NotImplementedException("updateConfiguration not supported in ThriftAdmin");
  }

  @Override
  public void updateConfiguration() {
    throw new NotImplementedException("updateConfiguration not supported in ThriftAdmin");
  }

  @Override
  public void updateConfiguration(String groupName) {
    throw new NotImplementedException("updateConfiguration not supported in ThriftAdmin");
  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() {
    throw new NotImplementedException("getSecurityCapabilities not supported in ThriftAdmin");
  }

  @Override
  public boolean splitSwitch(boolean enabled, boolean synchronous) {
    throw new NotImplementedException("splitSwitch not supported in ThriftAdmin");
  }

  @Override
  public boolean mergeSwitch(boolean enabled, boolean synchronous) {
    throw new NotImplementedException("mergeSwitch not supported in ThriftAdmin");
  }

  @Override
  public boolean isSplitEnabled() {
    throw new NotImplementedException("isSplitEnabled not supported in ThriftAdmin");
  }

  @Override
  public boolean isMergeEnabled() {
    throw new NotImplementedException("isMergeEnabled not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig,
      boolean enabled) {
    throw new NotImplementedException("addReplicationPeerAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> removeReplicationPeerAsync(String peerId) {
    throw new NotImplementedException("removeReplicationPeerAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> enableReplicationPeerAsync(String peerId) {
    throw new NotImplementedException("enableReplicationPeerAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> disableReplicationPeerAsync(String peerId) {
    throw new NotImplementedException("disableReplicationPeerAsync not supported in ThriftAdmin");
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId) {
    throw new NotImplementedException("getReplicationPeerConfig not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> updateReplicationPeerConfigAsync(String peerId,
      ReplicationPeerConfig peerConfig) {
    throw new NotImplementedException(
        "updateReplicationPeerConfigAsync not supported in ThriftAdmin");
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers() {
    throw new NotImplementedException("listReplicationPeers not supported in ThriftAdmin");
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern) {
    throw new NotImplementedException("listReplicationPeers not supported in ThriftAdmin");
  }
  @Override
  public Future<Void> transitReplicationPeerSyncReplicationStateAsync(String peerId,
      SyncReplicationState state) {
    throw new NotImplementedException(
        "transitReplicationPeerSyncReplicationStateAsync not supported in ThriftAdmin");
  }

  @Override
  public void decommissionRegionServers(List<ServerName> servers, boolean offload) {
    throw new NotImplementedException("decommissionRegionServers not supported in ThriftAdmin");

  }

  @Override
  public List<ServerName> listDecommissionedRegionServers() {
    throw new NotImplementedException(
        "listDecommissionedRegionServers not supported in ThriftAdmin");
  }

  @Override
  public void recommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames) {
    throw new NotImplementedException("recommissionRegionServer not supported in ThriftAdmin");

  }

  @Override
  public List<TableCFs> listReplicatedTableCFs() {
    throw new NotImplementedException("listReplicatedTableCFs not supported in ThriftAdmin");
  }

  @Override
  public void enableTableReplication(TableName tableName) {
    throw new NotImplementedException("enableTableReplication not supported in ThriftAdmin");

  }

  @Override
  public void disableTableReplication(TableName tableName) {
    throw new NotImplementedException("disableTableReplication not supported in ThriftAdmin");

  }

  @Override
  public void clearCompactionQueues(ServerName serverName, Set<String> queues) {
    throw new NotImplementedException("clearCompactionQueues not supported in ThriftAdmin");

  }

  @Override
  public List<ServerName> clearDeadServers(List<ServerName> servers) {
    throw new NotImplementedException("clearDeadServers not supported in ThriftAdmin");
  }

  @Override
  public void cloneTableSchema(TableName tableName, TableName newTableName,
      boolean preserveSplits) {
    throw new NotImplementedException("cloneTableSchema not supported in ThriftAdmin");

  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor desc) {
    throw new NotImplementedException("createTableAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys) {
    throw new NotImplementedException("createTableAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> deleteTableAsync(TableName tableName) {
    throw new NotImplementedException("deleteTableAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> truncateTableAsync(TableName tableName, boolean preserveSplits) {
    throw new NotImplementedException("truncateTableAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> enableTableAsync(TableName tableName) {
    throw new NotImplementedException("enableTableAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> disableTableAsync(TableName tableName) {
    throw new NotImplementedException("disableTableAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] columnFamily) {
    throw new NotImplementedException("deleteColumnFamilyAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> addColumnFamilyAsync(TableName tableName,
      ColumnFamilyDescriptor columnFamily) {
    throw new NotImplementedException("addColumnFamilyAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> modifyColumnFamilyAsync(TableName tableName,
      ColumnFamilyDescriptor columnFamily) {
    throw new NotImplementedException("modifyColumnFamilyAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> deleteNamespaceAsync(String name) {
    throw new NotImplementedException("deleteNamespaceAsync not supported in ThriftAdmin");
  }

  @Override
  public Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException {
    throw new NotImplementedException("getSpaceQuotaTableSizes not supported in ThriftAdmin");
  }

  @Override
  public Map<TableName, SpaceQuotaSnapshot> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) throws IOException {
    throw new NotImplementedException(
      "getRegionServerSpaceQuotaSnapshots not supported in ThriftAdmin");
  }

  @Override
  public SpaceQuotaSnapshot getCurrentSpaceQuotaSnapshot(String namespace) throws IOException {
    throw new NotImplementedException("getCurrentSpaceQuotaSnapshot not supported in ThriftAdmin");
  }

  @Override
  public SpaceQuotaSnapshot getCurrentSpaceQuotaSnapshot(TableName tableName) throws IOException {
    throw new NotImplementedException("getCurrentSpaceQuotaSnapshot not supported in ThriftAdmin");
  }

  @Override
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions) {
    throw new NotImplementedException("grant not supported in ThriftAdmin");
  }

  @Override
  public void revoke(UserPermission userPermission) {
    throw new NotImplementedException("revoke not supported in ThriftAdmin");
  }

  @Override
  public List<UserPermission> getUserPermissions(
      GetUserPermissionsRequest getUserPermissionsRequest) {
    throw new NotImplementedException("getUserPermissions not supported in ThriftAdmin");
  }

  @Override
  public List<Boolean> hasUserPermissions(String userName, List<Permission> permissions) {
    throw new NotImplementedException("hasUserPermissions not supported in ThriftAdmin");
  }

  @Override
  public boolean snapshotCleanupSwitch(boolean on, boolean synchronous) {
    throw new NotImplementedException("snapshotCleanupSwitch not supported in ThriftAdmin");
  }

  @Override
  public boolean isSnapshotCleanupEnabled() {
    throw new NotImplementedException("isSnapshotCleanupEnabled not supported in ThriftAdmin");
  }

  @Override
  public List<OnlineLogRecord> getSlowLogResponses(final Set<ServerName> serverNames,
      final LogQueryFilter logQueryFilter) throws IOException {
    Set<TServerName> tServerNames = ThriftUtilities.getServerNamesFromHBase(serverNames);
    TLogQueryFilter tLogQueryFilter =
      ThriftUtilities.getSlowLogQueryFromHBase(logQueryFilter);
    try {
      List<TOnlineLogRecord> tOnlineLogRecords =
        client.getSlowLogResponses(tServerNames, tLogQueryFilter);
      return ThriftUtilities.getSlowLogRecordsFromThrift(tOnlineLogRecords);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<Boolean> clearSlowLogResponses(final Set<ServerName> serverNames)
      throws IOException {
    Set<TServerName> tServerNames = ThriftUtilities.getServerNamesFromHBase(serverNames);
    try {
      return client.clearSlowLogResponses(tServerNames);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroup(String groupName) {
    throw new NotImplementedException("getRSGroup not supported in ThriftAdmin");
  }

  @Override
  public void moveServersToRSGroup(Set<Address> servers, String targetGroup) {
    throw new NotImplementedException("moveToRSGroup not supported in ThriftAdmin");
  }

  @Override
  public void addRSGroup(String groupName) {
    throw new NotImplementedException("addRSGroup not supported in ThriftAdmin");
  }

  @Override
  public void removeRSGroup(String groupName) {
    throw new NotImplementedException("removeRSGroup not supported in ThriftAdmin");
  }

  @Override
  public BalanceResponse balanceRSGroup(String groupName, BalanceRequest request) {
    throw new NotImplementedException("balanceRSGroup not supported in ThriftAdmin");
  }

  @Override
  public List<RSGroupInfo> listRSGroups() {
    throw new NotImplementedException("listRSGroups not supported in ThriftAdmin");
  }

  @Override
  public RSGroupInfo getRSGroup(Address hostPort) {
    throw new NotImplementedException("getRSGroup not supported in ThriftAdmin");
  }

  @Override
  public void removeServersFromRSGroup(Set<Address> servers) {
    throw new NotImplementedException("removeRSGroup not supported in ThriftAdmin");
  }

  @Override
  public RSGroupInfo getRSGroup(TableName tableName) {
    throw new NotImplementedException("getRSGroup not supported in ThriftAdmin");
  }

  @Override
  public void setRSGroup(Set<TableName> tables, String groupName) {
    throw new NotImplementedException("setRSGroup not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> splitRegionAsync(byte[] regionName) throws IOException {
    return splitRegionAsync(regionName, null);
  }

  @Override
  public List<TableName> listTablesInRSGroup(String groupName) throws IOException {
    throw new NotImplementedException("setRSGroup not supported in ThriftAdmin");
  }

  @Override
  public Pair<List<String>, List<TableName>>
    getConfiguredNamespacesAndTablesInRSGroup(String groupName) throws IOException {
    throw new NotImplementedException("setRSGroup not supported in ThriftAdmin");
  }

  @Override
  public void renameRSGroup(String oldName, String newName) throws IOException {
    throw new NotImplementedException("renameRSGroup not supported in ThriftAdmin");
  }

  @Override
  public void updateRSGroupConfig(String groupName, Map<String, String> configuration)
      throws IOException {
    throw new NotImplementedException("updateRSGroupConfig not supported in ThriftAdmin");
  }

  @Override
  public List<LogEntry> getLogEntries(Set<ServerName> serverNames, String logType,
      ServerType serverType, int limit, Map<String, Object> filterParams)
      throws IOException {
    throw new NotImplementedException("getLogEntries not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> modifyColumnFamilyStoreFileTrackerAsync(TableName tableName, byte[] family,
    String dstSFT) throws IOException {
    throw new NotImplementedException(
      "modifyColumnFamilyStoreFileTrackerAsync not supported in ThriftAdmin");
  }

  @Override
  public Future<Void> modifyTableStoreFileTrackerAsync(TableName tableName, String dstSFT)
    throws IOException {
    throw new NotImplementedException(
      "modifyTableStoreFileTrackerAsync not supported in ThriftAdmin");
  }
}
