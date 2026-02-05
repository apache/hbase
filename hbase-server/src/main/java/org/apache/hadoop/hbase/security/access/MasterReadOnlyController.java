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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class MasterReadOnlyController extends AbstractReadOnlyController
  implements MasterCoprocessor, MasterObserver {

  private static final Logger LOG = LoggerFactory.getLogger(MasterReadOnlyController.class);

  private MasterServices masterServices;

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public TableDescriptor preCreateTableRegionsInfos(
    ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc) throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preCreateTableRegionsInfos(ctx, desc);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCreateTable(ctx, desc, regions);
  }

  @Override
  public void preCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCreateTableAction(ctx, desc, regions);
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteTable(ctx, tableName);
  }

  @Override
  public void preDeleteTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteTableAction(ctx, tableName);
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preTruncateTable(ctx, tableName);
  }

  @Override
  public void preTruncateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preTruncateTableAction(ctx, tableName);
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Override
  public String preModifyTableStoreFileTracker(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, String dstSFT) throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preModifyTableStoreFileTracker(ctx, tableName, dstSFT);
  }

  @Override
  public String preModifyColumnFamilyStoreFileTracker(
    ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] family,
    String dstSFT) throws IOException {
    internalReadOnlyGuard();
    return MasterObserver.super.preModifyColumnFamilyStoreFileTracker(ctx, tableName, family,
      dstSFT);
  }

  @Override
  public void preModifyTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preModifyTableAction(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Override
  public void preSplitRegion(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName,
    byte[] splitRow) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegion(c, tableName, splitRow);
  }

  @Override
  public void preSplitRegionAction(ObserverContext<MasterCoprocessorEnvironment> c,
    TableName tableName, byte[] splitRow) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegionAction(c, tableName, splitRow);
  }

  @Override
  public void preSplitRegionBeforeMETAAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    byte[] splitKey, List<Mutation> metaEntries) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegionBeforeMETAAction(ctx, splitKey, metaEntries);
  }

  @Override
  public void preSplitRegionAfterMETAAction(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSplitRegionAfterMETAAction(ctx);
  }

  @Override
  public void preTruncateRegion(ObserverContext<MasterCoprocessorEnvironment> c,
    RegionInfo regionInfo) {
    try {
      internalReadOnlyGuard();
    } catch (IOException e) {
      LOG.info("Region truncation of region {} not allowed in read-only mode",
        regionInfo.getRegionNameAsString());
    }
    MasterObserver.super.preTruncateRegion(c, regionInfo);
  }

  @Override
  public void preTruncateRegionAction(ObserverContext<MasterCoprocessorEnvironment> c,
    RegionInfo regionInfo) {
    try {
      internalReadOnlyGuard();
    } catch (IOException e) {
      LOG.info("Region truncation of region {} not allowed in read-only mode",
        regionInfo.getRegionNameAsString());
    }
    MasterObserver.super.preTruncateRegionAction(c, regionInfo);
  }

  @Override
  public void preMergeRegionsAction(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final RegionInfo[] regionsToMerge) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMergeRegionsAction(ctx, regionsToMerge);
  }

  @Override
  public void preMergeRegionsCommitAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    RegionInfo[] regionsToMerge, List<Mutation> metaEntries) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMergeRegionsCommitAction(ctx, regionsToMerge, metaEntries);
  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCloneSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Override
  public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot, TableDescriptor tableDescriptor) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRestoreSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Override
  public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
    SnapshotDescription snapshot) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteSnapshot(ctx, snapshot);
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor ns) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preCreateNamespace(ctx, ns);
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor currentNsDescriptor, NamespaceDescriptor newNsDescriptor)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preModifyNamespace(ctx, currentNsDescriptor, newNsDescriptor);
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String namespace) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDeleteNamespace(ctx, namespace);
  }

  @Override
  public void preMasterStoreFlush(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMasterStoreFlush(ctx);
  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
    GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetUserQuota(ctx, userName, quotas);
  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
    TableName tableName, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetUserQuota(ctx, userName, tableName, quotas);
  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
    String namespace, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetUserQuota(ctx, userName, namespace, quotas);
  }

  @Override
  public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetTableQuota(ctx, tableName, quotas);
  }

  @Override
  public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String namespace, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetNamespaceQuota(ctx, namespace, quotas);
  }

  @Override
  public void preSetRegionServerQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String regionServer, GlobalQuotaSettings quotas) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preSetRegionServerQuota(ctx, regionServer, quotas);
  }

  @Override
  public void preMergeRegions(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final RegionInfo[] regionsToMerge) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMergeRegions(ctx, regionsToMerge);
  }

  @Override
  public void preMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMoveServersAndTables(ctx, servers, tables, targetGroup);
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<Address> servers, String targetGroup) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMoveServers(ctx, servers, targetGroup);
  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<TableName> tables, String targetGroup) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preMoveTables(ctx, tables, targetGroup);
  }

  @Override
  public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preAddRSGroup(ctx, name);
  }

  @Override
  public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRemoveRSGroup(ctx, name);
  }

  @Override
  public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName,
    BalanceRequest request) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preBalanceRSGroup(ctx, groupName, request);
  }

  @Override
  public void preRemoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
    Set<Address> servers) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRemoveServers(ctx, servers);
  }

  @Override
  public void preRenameRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String oldName,
    String newName) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRenameRSGroup(ctx, oldName, newName);
  }

  @Override
  public void preUpdateRSGroupConfig(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String groupName, Map<String, String> configuration) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preUpdateRSGroupConfig(ctx, groupName, configuration);
  }

  @Override
  public void preAddReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId, ReplicationPeerConfig peerConfig) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preAddReplicationPeer(ctx, peerId, peerConfig);
  }

  @Override
  public void preRemoveReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRemoveReplicationPeer(ctx, peerId);
  }

  @Override
  public void preEnableReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preEnableReplicationPeer(ctx, peerId);
  }

  @Override
  public void preDisableReplicationPeer(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preDisableReplicationPeer(ctx, peerId);
  }

  @Override
  public void preUpdateReplicationPeerConfig(ObserverContext<MasterCoprocessorEnvironment> ctx,
    String peerId, ReplicationPeerConfig peerConfig) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preUpdateReplicationPeerConfig(ctx, peerId, peerConfig);
  }

  @Override
  public void preTransitReplicationPeerSyncReplicationState(
    ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId, SyncReplicationState state)
    throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preTransitReplicationPeerSyncReplicationState(ctx, peerId, state);
  }

  @Override
  public void preGrant(ObserverContext<MasterCoprocessorEnvironment> ctx,
    UserPermission userPermission, boolean mergeExistingPermissions) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preGrant(ctx, userPermission, mergeExistingPermissions);
  }

  @Override
  public void preRevoke(ObserverContext<MasterCoprocessorEnvironment> ctx,
    UserPermission userPermission) throws IOException {
    internalReadOnlyGuard();
    MasterObserver.super.preRevoke(ctx, userPermission);
  }
}
