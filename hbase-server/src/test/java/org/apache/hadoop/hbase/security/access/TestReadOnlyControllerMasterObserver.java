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

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// Tests methods of Master Observer which are implemented in ReadOnlyController,
// by mocking the coprocessor environment and dependencies

@Category({ SecurityTests.class, SmallTests.class })
public class TestReadOnlyControllerMasterObserver {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyControllerMasterObserver.class);

  ReadOnlyController readOnlyController;

  // Master Coprocessor mocking variables
  ObserverContext<MasterCoprocessorEnvironment> c, ctx;
  TableDescriptor desc;
  RegionInfo[] regions;
  TableName tableName;
  TableDescriptor currentDescriptor, newDescriptor;
  String dstSFT;
  byte[] family;
  byte[] splitRow;
  byte[] splitKey;
  List<Mutation> metaEntries;
  RegionInfo[] regionsToMerge;
  SnapshotDescription snapshot;
  TableDescriptor tableDescriptor;
  NamespaceDescriptor ns;
  NamespaceDescriptor currentNsDescriptor, newNsDescriptor;
  String namespace;
  String userName;
  GlobalQuotaSettings quotas;
  String regionServer;
  Set<Address> servers;
  Set<TableName> tables;
  String targetGroup;
  String name;
  String groupName;
  BalanceRequest request;
  String oldName, newName;
  Map<String, String> configuration;
  String peerId;
  ReplicationPeerConfig peerConfig;
  SyncReplicationState state;
  UserPermission userPermission;
  boolean mergeExistingPermissions;

  @Before
  public void setup() throws Exception {
    readOnlyController = new ReadOnlyController();

    // mocking variables initialization
    c = mock(ObserverContext.class);
    // ctx is created to make naming variable in sync with the Observer interface
    // methods where 'ctx' is used as the ObserverContext variable name instead of 'c'.
    // otherwise both are one and the same
    ctx = c;
    desc = mock(TableDescriptor.class);
    regions = new RegionInfo[] {};
    tableName = TableName.valueOf("testTable");
    currentDescriptor = mock(TableDescriptor.class);
    newDescriptor = mock(TableDescriptor.class);
    dstSFT = "dstSFT";
    family = Bytes.toBytes("testFamily");
    splitRow = Bytes.toBytes("splitRow");
    splitKey = Bytes.toBytes("splitKey");
    metaEntries = List.of();
    regionsToMerge = new RegionInfo[] {};
    snapshot = mock(SnapshotDescription.class);
    tableDescriptor = mock(TableDescriptor.class);
    ns = mock(NamespaceDescriptor.class);
    currentNsDescriptor = mock(NamespaceDescriptor.class);
    newNsDescriptor = mock(NamespaceDescriptor.class);
    namespace = "testNamespace";
    userName = "testUser";
    quotas = mock(GlobalQuotaSettings.class);
    regionServer = "testRegionServer";
    servers = Set.of();
    tables = Set.of();
    targetGroup = "targetGroup";
    name = "testRSGroup";
    groupName = "testGroupName";
    request = BalanceRequest.newBuilder().build();
    oldName = "oldRSGroupName";
    newName = "newRSGroupName";
    configuration = Map.of();
    peerId = "testPeerId";
    peerConfig = mock(ReplicationPeerConfig.class);
    state = SyncReplicationState.NONE;
    userPermission = mock(UserPermission.class);
    mergeExistingPermissions = false;

    // Linking the mocks:

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test(expected = IOException.class)
  public void testPreCreateTableRegionsInfosReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preCreateTableRegionsInfos(ctx, desc);
  }

  @Test
  public void testPreCreateTableRegionsInfosNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preCreateTableRegionsInfos(ctx, desc);
  }

  @Test(expected = IOException.class)
  public void testPreCreateTableReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preCreateTable(ctx, desc, regions);
  }

  @Test
  public void testPreCreateTableNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preCreateTable(ctx, desc, regions);
  }

  @Test(expected = IOException.class)
  public void testPreCreateTableActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preCreateTableAction(ctx, desc, regions);
  }

  @Test
  public void testPreCreateTableActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preCreateTableAction(ctx, desc, regions);
  }

  @Test(expected = IOException.class)
  public void testPreDeleteTableReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preDeleteTable(ctx, tableName);
  }

  @Test
  public void testPreDeleteTableNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preDeleteTable(ctx, tableName);
  }

  @Test(expected = IOException.class)
  public void testPreDeleteTableActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preDeleteTableAction(ctx, tableName);
  }

  @Test
  public void testPreDeleteTableActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preDeleteTableAction(ctx, tableName);
  }

  @Test(expected = IOException.class)
  public void testPreTruncateTableReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preTruncateTable(ctx, tableName);
  }

  @Test
  public void testPreTruncateTableNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preTruncateTable(ctx, tableName);
  }

  @Test(expected = IOException.class)
  public void testPreTruncateTableActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preTruncateTableAction(ctx, tableName);
  }

  @Test
  public void testPreTruncateTableActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preTruncateTableAction(ctx, tableName);
  }

  @Test(expected = IOException.class)
  public void testPreModifyTableReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Test
  public void testPreModifyTableNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Test(expected = IOException.class)
  public void testPreModifyTableStoreFileTrackerReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preModifyTableStoreFileTracker(ctx, tableName, dstSFT);
  }

  @Test
  public void testPreModifyTableStoreFileTrackerNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preModifyTableStoreFileTracker(ctx, tableName, dstSFT);
  }

  @Test(expected = IOException.class)
  public void testPreModifyColumnFamilyStoreFileTrackerReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preModifyColumnFamilyStoreFileTracker(ctx, tableName, family, dstSFT);
  }

  @Test
  public void testPreModifyColumnFamilyStoreFileTrackerNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preModifyColumnFamilyStoreFileTracker(ctx, tableName, family, dstSFT);
  }

  @Test(expected = IOException.class)
  public void testPreModifyTableActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preModifyTableAction(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Test
  public void testPreModifyTableActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preModifyTableAction(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Test(expected = IOException.class)
  public void testPreSplitRegionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSplitRegion(c, tableName, splitRow);
  }

  @Test
  public void testPreSplitRegionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSplitRegion(c, tableName, splitRow);
  }

  @Test(expected = IOException.class)
  public void testPreSplitRegionActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSplitRegionAction(c, tableName, splitRow);
  }

  @Test
  public void testPreSplitRegionActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSplitRegionAction(c, tableName, splitRow);
  }

  @Test(expected = IOException.class)
  public void testPreSplitRegionBeforeMETAActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSplitRegionBeforeMETAAction(ctx, splitKey, metaEntries);
  }

  @Test
  public void testPreSplitRegionBeforeMETAActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSplitRegionBeforeMETAAction(ctx, splitKey, metaEntries);
  }

  @Test(expected = IOException.class)
  public void testPreSplitRegionAfterMETAActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSplitRegionAfterMETAAction(ctx);
  }

  @Test
  public void testPreSplitRegionAfterMETAActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSplitRegionAfterMETAAction(ctx);
  }

  @Test(expected = IOException.class)
  public void testPreMergeRegionsActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preMergeRegionsAction(ctx, regionsToMerge);
  }

  @Test
  public void testPreMergeRegionsActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preMergeRegionsAction(ctx, regionsToMerge);
  }

  @Test(expected = IOException.class)
  public void testPreMergeRegionsCommitActionReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preMergeRegionsCommitAction(ctx, regionsToMerge, metaEntries);
  }

  @Test
  public void testPreMergeRegionsCommitActionNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preMergeRegionsCommitAction(ctx, regionsToMerge, metaEntries);
  }

  @Test(expected = IOException.class)
  public void testPreSnapshotReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test
  public void testPreSnapshotNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test(expected = IOException.class)
  public void testPreCloneSnapshotReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preCloneSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test
  public void testPreCloneSnapshotNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preCloneSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test(expected = IOException.class)
  public void testPreRestoreSnapshotReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preRestoreSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test
  public void testPreRestoreSnapshotNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preRestoreSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test(expected = IOException.class)
  public void testPreDeleteSnapshotReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preDeleteSnapshot(ctx, snapshot);
  }

  @Test
  public void testPreDeleteSnapshotNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preDeleteSnapshot(ctx, snapshot);
  }

  @Test(expected = IOException.class)
  public void testPreCreateNamespaceReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preCreateNamespace(ctx, ns);
  }

  @Test
  public void testPreCreateNamespaceNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preCreateNamespace(ctx, ns);
  }

  @Test(expected = IOException.class)
  public void testPreModifyNamespaceReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preModifyNamespace(ctx, currentNsDescriptor, newNsDescriptor);
  }

  @Test
  public void testPreModifyNamespaceNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preModifyNamespace(ctx, currentNsDescriptor, newNsDescriptor);
  }

  @Test(expected = IOException.class)
  public void testPreDeleteNamespaceReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preDeleteNamespace(ctx, namespace);
  }

  @Test
  public void testPreDeleteNamespaceNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preDeleteNamespace(ctx, namespace);
  }

  @Test(expected = IOException.class)
  public void testPreMasterStoreFlushReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preMasterStoreFlush(ctx);
  }

  @Test
  public void testPreMasterStoreFlushNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preMasterStoreFlush(ctx);
  }

  @Test(expected = IOException.class)
  public void testPreSetUserQuotaReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSetUserQuota(ctx, userName, quotas);
  }

  @Test
  public void testPreSetUserQuotaNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSetUserQuota(ctx, userName, quotas);
  }

  @Test(expected = IOException.class)
  public void testPreSetUserQuotaOnTableReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSetUserQuota(ctx, userName, tableName, quotas);
  }

  @Test
  public void testPreSetUserQuotaOnTableNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSetUserQuota(ctx, userName, tableName, quotas);
  }

  @Test(expected = IOException.class)
  public void testPreSetUserQuotaOnNamespaceReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSetUserQuota(ctx, userName, namespace, quotas);
  }

  @Test
  public void testPreSetUserQuotaOnNamespaceNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSetUserQuota(ctx, userName, namespace, quotas);
  }

  @Test(expected = IOException.class)
  public void testPreSetTableQuotaReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSetTableQuota(ctx, tableName, quotas);
  }

  @Test
  public void testPreSetTableQuotaNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSetTableQuota(ctx, tableName, quotas);
  }

  @Test(expected = IOException.class)
  public void testPreSetNamespaceQuotaReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSetNamespaceQuota(ctx, namespace, quotas);
  }

  @Test
  public void testPreSetNamespaceQuotaNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSetNamespaceQuota(ctx, namespace, quotas);
  }

  @Test(expected = IOException.class)
  public void testPreSetRegionServerQuotaReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preSetRegionServerQuota(ctx, regionServer, quotas);
  }

  @Test
  public void testPreSetRegionServerQuotaNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preSetRegionServerQuota(ctx, regionServer, quotas);
  }

  @Test(expected = IOException.class)
  public void testPreMergeRegionsReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preMergeRegions(ctx, regionsToMerge);
  }

  @Test
  public void testPreMergeRegionsNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preMergeRegions(ctx, regionsToMerge);
  }

  @Test(expected = IOException.class)
  public void testPreMoveServersAndTablesReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preMoveServersAndTables(ctx, servers, tables, targetGroup);
  }

  @Test
  public void testPreMoveServersAndTablesNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preMoveServersAndTables(ctx, servers, tables, targetGroup);
  }

  @Test(expected = IOException.class)
  public void testPreMoveServersReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preMoveServers(ctx, servers, targetGroup);
  }

  @Test
  public void testPreMoveServersNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preMoveServers(ctx, servers, targetGroup);
  }

  @Test(expected = IOException.class)
  public void testPreMoveTablesReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preMoveTables(ctx, tables, targetGroup);
  }

  @Test
  public void testPreMoveTablesNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preMoveTables(ctx, tables, targetGroup);
  }

  @Test(expected = IOException.class)
  public void testPreAddRSGroupReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preAddRSGroup(ctx, name);
  }

  @Test
  public void testPreAddRSGroupNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preAddRSGroup(ctx, name);
  }

  @Test(expected = IOException.class)
  public void testPreRemoveRSGroupReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preRemoveRSGroup(ctx, name);
  }

  @Test
  public void testPreRemoveRSGroupNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preRemoveRSGroup(ctx, name);
  }

  @Test(expected = IOException.class)
  public void testPreBalanceRSGroupReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preBalanceRSGroup(ctx, groupName, request);
  }

  @Test
  public void testPreBalanceRSGroupNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preBalanceRSGroup(ctx, groupName, request);
  }

  @Test(expected = IOException.class)
  public void testPreRemoveServersReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preRemoveServers(ctx, servers);
  }

  @Test
  public void testPreRemoveServersNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preRemoveServers(ctx, servers);
  }

  @Test(expected = IOException.class)
  public void testPreRenameRSGroupReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preRenameRSGroup(ctx, oldName, newName);
  }

  @Test
  public void testPreRenameRSGroupNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preRenameRSGroup(ctx, oldName, newName);
  }

  @Test(expected = IOException.class)
  public void testPreUpdateRSGroupConfigReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preUpdateRSGroupConfig(ctx, groupName, configuration);
  }

  @Test
  public void testPreUpdateRSGroupConfigNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preUpdateRSGroupConfig(ctx, groupName, configuration);
  }

  @Test(expected = IOException.class)
  public void testPreAddReplicationPeerReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preAddReplicationPeer(ctx, peerId, peerConfig);
  }

  @Test
  public void testPreAddReplicationPeerNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preAddReplicationPeer(ctx, peerId, peerConfig);
  }

  @Test(expected = IOException.class)
  public void testPreRemoveReplicationPeerReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preRemoveReplicationPeer(ctx, peerId);
  }

  @Test
  public void testPreRemoveReplicationPeerNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preRemoveReplicationPeer(ctx, peerId);
  }

  @Test(expected = IOException.class)
  public void testPreEnableReplicationPeerReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preEnableReplicationPeer(ctx, peerId);
  }

  @Test
  public void testPreEnableReplicationPeerNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preEnableReplicationPeer(ctx, peerId);
  }

  @Test(expected = IOException.class)
  public void testPreDisableReplicationPeerReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preDisableReplicationPeer(ctx, peerId);
  }

  @Test
  public void testPreDisableReplicationPeerNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preDisableReplicationPeer(ctx, peerId);
  }

  @Test(expected = IOException.class)
  public void testPreUpdateReplicationPeerConfigReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preUpdateReplicationPeerConfig(ctx, peerId, peerConfig);
  }

  @Test
  public void testPreUpdateReplicationPeerConfigNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preUpdateReplicationPeerConfig(ctx, peerId, peerConfig);
  }

  @Test(expected = IOException.class)
  public void testPreTransitReplicationPeerSyncReplicationStateReadOnlyException()
    throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preTransitReplicationPeerSyncReplicationState(ctx, peerId, state);
  }

  @Test
  public void testPreTransitReplicationPeerSyncReplicationStateNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preTransitReplicationPeerSyncReplicationState(ctx, peerId, state);
  }

  @Test(expected = IOException.class)
  public void testPreGrantReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preGrant(ctx, userPermission, mergeExistingPermissions);
  }

  @Test
  public void testPreGrantNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preGrant(ctx, userPermission, mergeExistingPermissions);
  }

  @Test(expected = IOException.class)
  public void testPreRevokeReadOnlyException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(true);
    readOnlyController.preRevoke(ctx, userPermission);
  }

  @Test
  public void testPreRevokeNoException() throws IOException {
    readOnlyController.setGlobalReadOnlyEnabled(false);
    readOnlyController.preRevoke(ctx, userPermission);
  }

  // @Test(expected = IOException.class)
  // public void testReadOnlyException() throws IOException {
  // readOnlyController.setGlobalReadOnlyEnabled(true);
  // readOnlyController.();
  // }
  //
  // @Test
  // public void testNoException() throws IOException {
  // readOnlyController.setGlobalReadOnlyEnabled(false);
  // readOnlyController.();
  // }
}
