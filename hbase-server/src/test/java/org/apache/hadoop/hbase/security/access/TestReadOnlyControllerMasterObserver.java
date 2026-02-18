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
import org.apache.hadoop.hbase.DoNotRetryIOException;
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

  MasterReadOnlyController MasterReadOnlyController;

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
    MasterReadOnlyController = new MasterReadOnlyController();

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

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCreateTableRegionsInfosReadOnlyException() throws IOException {
    MasterReadOnlyController.preCreateTableRegionsInfos(ctx, desc);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCreateTableReadOnlyException() throws IOException {
    MasterReadOnlyController.preCreateTable(ctx, desc, regions);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCreateTableActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preCreateTableAction(ctx, desc, regions);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreDeleteTableReadOnlyException() throws IOException {
    MasterReadOnlyController.preDeleteTable(ctx, tableName);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreDeleteTableActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preDeleteTableAction(ctx, tableName);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreTruncateTableReadOnlyException() throws IOException {
    MasterReadOnlyController.preTruncateTable(ctx, tableName);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreTruncateTableActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preTruncateTableAction(ctx, tableName);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreModifyTableReadOnlyException() throws IOException {
    MasterReadOnlyController.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreModifyTableStoreFileTrackerReadOnlyException() throws IOException {
    MasterReadOnlyController.preModifyTableStoreFileTracker(ctx, tableName, dstSFT);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreModifyColumnFamilyStoreFileTrackerReadOnlyException() throws IOException {
    MasterReadOnlyController.preModifyColumnFamilyStoreFileTracker(ctx, tableName, family, dstSFT);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreModifyTableActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preModifyTableAction(ctx, tableName, currentDescriptor, newDescriptor);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSplitRegionReadOnlyException() throws IOException {
    MasterReadOnlyController.preSplitRegion(c, tableName, splitRow);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSplitRegionActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preSplitRegionAction(c, tableName, splitRow);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSplitRegionBeforeMETAActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preSplitRegionBeforeMETAAction(ctx, splitKey, metaEntries);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSplitRegionAfterMETAActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preSplitRegionAfterMETAAction(ctx);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMergeRegionsActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preMergeRegionsAction(ctx, regionsToMerge);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMergeRegionsCommitActionReadOnlyException() throws IOException {
    MasterReadOnlyController.preMergeRegionsCommitAction(ctx, regionsToMerge, metaEntries);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSnapshotReadOnlyException() throws IOException {
    MasterReadOnlyController.preSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCloneSnapshotReadOnlyException() throws IOException {
    MasterReadOnlyController.preCloneSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreRestoreSnapshotReadOnlyException() throws IOException {
    MasterReadOnlyController.preRestoreSnapshot(ctx, snapshot, tableDescriptor);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreDeleteSnapshotReadOnlyException() throws IOException {
    MasterReadOnlyController.preDeleteSnapshot(ctx, snapshot);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreCreateNamespaceReadOnlyException() throws IOException {
    MasterReadOnlyController.preCreateNamespace(ctx, ns);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreModifyNamespaceReadOnlyException() throws IOException {
    MasterReadOnlyController.preModifyNamespace(ctx, currentNsDescriptor, newNsDescriptor);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreDeleteNamespaceReadOnlyException() throws IOException {
    MasterReadOnlyController.preDeleteNamespace(ctx, namespace);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMasterStoreFlushReadOnlyException() throws IOException {
    MasterReadOnlyController.preMasterStoreFlush(ctx);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSetUserQuotaReadOnlyException() throws IOException {
    MasterReadOnlyController.preSetUserQuota(ctx, userName, quotas);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSetUserQuotaOnTableReadOnlyException() throws IOException {
    MasterReadOnlyController.preSetUserQuota(ctx, userName, tableName, quotas);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSetUserQuotaOnNamespaceReadOnlyException() throws IOException {
    MasterReadOnlyController.preSetUserQuota(ctx, userName, namespace, quotas);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSetTableQuotaReadOnlyException() throws IOException {
    MasterReadOnlyController.preSetTableQuota(ctx, tableName, quotas);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSetNamespaceQuotaReadOnlyException() throws IOException {
    MasterReadOnlyController.preSetNamespaceQuota(ctx, namespace, quotas);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreSetRegionServerQuotaReadOnlyException() throws IOException {
    MasterReadOnlyController.preSetRegionServerQuota(ctx, regionServer, quotas);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMergeRegionsReadOnlyException() throws IOException {
    MasterReadOnlyController.preMergeRegions(ctx, regionsToMerge);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMoveServersAndTablesReadOnlyException() throws IOException {
    MasterReadOnlyController.preMoveServersAndTables(ctx, servers, tables, targetGroup);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMoveServersReadOnlyException() throws IOException {
    MasterReadOnlyController.preMoveServers(ctx, servers, targetGroup);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreMoveTablesReadOnlyException() throws IOException {
    MasterReadOnlyController.preMoveTables(ctx, tables, targetGroup);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreAddRSGroupReadOnlyException() throws IOException {
    MasterReadOnlyController.preAddRSGroup(ctx, name);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreRemoveRSGroupReadOnlyException() throws IOException {
    MasterReadOnlyController.preRemoveRSGroup(ctx, name);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreBalanceRSGroupReadOnlyException() throws IOException {
    MasterReadOnlyController.preBalanceRSGroup(ctx, groupName, request);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreRemoveServersReadOnlyException() throws IOException {
    MasterReadOnlyController.preRemoveServers(ctx, servers);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreRenameRSGroupReadOnlyException() throws IOException {
    MasterReadOnlyController.preRenameRSGroup(ctx, oldName, newName);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreUpdateRSGroupConfigReadOnlyException() throws IOException {
    MasterReadOnlyController.preUpdateRSGroupConfig(ctx, groupName, configuration);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreAddReplicationPeerReadOnlyException() throws IOException {
    MasterReadOnlyController.preAddReplicationPeer(ctx, peerId, peerConfig);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreRemoveReplicationPeerReadOnlyException() throws IOException {
    MasterReadOnlyController.preRemoveReplicationPeer(ctx, peerId);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreEnableReplicationPeerReadOnlyException() throws IOException {
    MasterReadOnlyController.preEnableReplicationPeer(ctx, peerId);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreDisableReplicationPeerReadOnlyException() throws IOException {
    MasterReadOnlyController.preDisableReplicationPeer(ctx, peerId);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreUpdateReplicationPeerConfigReadOnlyException() throws IOException {
    MasterReadOnlyController.preUpdateReplicationPeerConfig(ctx, peerId, peerConfig);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreTransitReplicationPeerSyncReplicationStateReadOnlyException()
    throws IOException {
    MasterReadOnlyController.preTransitReplicationPeerSyncReplicationState(ctx, peerId, state);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreGrantReadOnlyException() throws IOException {
    MasterReadOnlyController.preGrant(ctx, userPermission, mergeExistingPermissions);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreRevokeReadOnlyException() throws IOException {
    MasterReadOnlyController.preRevoke(ctx, userPermission);
  }
}
