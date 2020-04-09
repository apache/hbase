/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class BaseMasterAndRegionObserver extends BaseRegionObserver
    implements MasterObserver {
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preDispatchMerge(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionA, HRegionInfo regionB) throws IOException {
  }

  @Override
  public void postDispatchMerge(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionA, HRegionInfo regionB) throws IOException {
  }

  @Override
  public void preGetClusterStatus(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
  }

  @Override
  public void postGetClusterStatus(ObserverContext<MasterCoprocessorEnvironment> ctx,
      ClusterStatus status)
    throws IOException {
  }

  @Override
  public void preClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
  }

  @Override
  public void postClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, List<ServerName> notClearedServers) throws IOException {
  }

  @Override
  public void preCreateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCreateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preDeleteTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException{
  }

  @Override
  public void postDeleteTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preTruncateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postTruncateTableHandler(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preModifyTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
  }

  @Override
  public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preListNamespaces(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<String> namespaces) throws IOException {
  }

  @Override
  public void postListNamespaces(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<String> namespaces) throws IOException {
  }

  @Override
  public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {
  }

  @Override
  public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void preAddColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor column) throws IOException {
  }

  @Override
  public void postAddColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor column) throws IOException {
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preModifyColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void postModifyColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] c) throws IOException {
  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, byte[] c) throws IOException {
  }

  @Override
  public void preDeleteColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      byte[] c) throws IOException {
  }

  @Override
  public void postDeleteColumnHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
      byte[] c) throws IOException {
  }


  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preEnableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postEnableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preDisableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void postDisableTableHandler(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
  }

  @Override
  public void preAbortProcedure(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      final ProcedureExecutor<MasterProcedureEnv> procEnv,
      final long procId) throws IOException {
  }

  @Override
  public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postListProcedures(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ProcedureInfo> procInfoList) throws IOException {
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
    HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
    HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
      throws IOException {
  }

  @Override
  public boolean preSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           boolean newValue,
                                           Admin.MasterSwitchType switchType)
      throws IOException {
    return false;
  }

  @Override
  public void postSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                         boolean newValue,
                                         Admin.MasterSwitchType switchType)
      throws IOException {

  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean b) throws IOException {
    return b;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean oldValue, boolean newValue) throws IOException {
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
  }

  @Override
  public void preMasterInitialization(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
  }

  @Override
  public void stop(CoprocessorEnvironment ctx) throws IOException {
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo region, ServerName srcServer, ServerName destServer)
  throws IOException {
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo region, ServerName srcServer, ServerName destServer)
  throws IOException {
  }

  @Override
  public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void postListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors)
      throws IOException {
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors) throws IOException {
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
      String regex) throws IOException {
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
      String regex) throws IOException {
  }

  @Override
  public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors, String regex) throws IOException {
  }

  @Override
  public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors, String regex) throws IOException {
  }

  @Override
  public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final Quotas quotas) throws IOException {
  }

  @Override
  public void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void postSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final Quotas quotas) throws IOException {
  }

  @Override
  public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void postBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 String groupName, boolean balancerRan) throws IOException {
  }

  @Override
  public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<Address>
      servers, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName>
      tables, String targetGroup) throws IOException {
  }

  @Override
  public void preMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
  }

  @Override
  public void postRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName)
      throws IOException {
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, String targetGroup) throws IOException {
  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<TableName> tables, String targetGroup) throws IOException {
  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preRemoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {
  }

  @Override
  public void postRemoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {
  }


  @Override
  public void preRenameRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               String oldName, String newName) throws IOException {
  }

  @Override
  public void postRenameRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                String oldName, String newName) throws IOException {
  }
}
