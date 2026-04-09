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

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.WriteAttemptedOnReadOnlyClusterException;
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

  @Test
  public void testPreCreateTableRegionsInfosReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preCreateTableRegionsInfos(ctx, desc);
    });
  }

  @Test
  public void testPreCreateTableReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preCreateTable(ctx, desc, regions);
    });
  }

  @Test
  public void testPreCreateTableActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preCreateTableAction(ctx, desc, regions);
    });
  }

  @Test
  public void testPreDeleteTableReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preDeleteTable(ctx, tableName);
    });
  }

  @Test
  public void testPreDeleteTableActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preDeleteTableAction(ctx, tableName);
    });
  }

  @Test
  public void testPreTruncateTableReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preTruncateTable(ctx, tableName);
    });
  }

  @Test
  public void testPreTruncateTableActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preTruncateTableAction(ctx, tableName);
    });
  }

  @Test
  public void testPreModifyTableReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
    });
  }

  @Test
  public void testPreModifyTableStoreFileTrackerReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preModifyTableStoreFileTracker(ctx, tableName, dstSFT);
    });
  }

  @Test
  public void testPreModifyColumnFamilyStoreFileTrackerReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preModifyColumnFamilyStoreFileTracker(ctx, tableName, family,
        dstSFT);
    });
  }

  @Test
  public void testPreModifyTableActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preModifyTableAction(ctx, tableName, currentDescriptor,
        newDescriptor);
    });
  }

  @Test
  public void testPreSplitRegionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSplitRegion(c, tableName, splitRow);
    });
  }

  @Test
  public void testPreSplitRegionActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSplitRegionAction(c, tableName, splitRow);
    });
  }

  @Test
  public void testPreSplitRegionBeforeMETAActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSplitRegionBeforeMETAAction(ctx, splitKey, metaEntries);
    });
  }

  @Test
  public void testPreSplitRegionAfterMETAActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSplitRegionAfterMETAAction(ctx);
    });
  }

  @Test
  public void testPreMergeRegionsActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preMergeRegionsAction(ctx, regionsToMerge);
    });
  }

  @Test
  public void testPreMergeRegionsCommitActionReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preMergeRegionsCommitAction(ctx, regionsToMerge, metaEntries);
    });
  }

  @Test
  public void testPreSnapshotReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSnapshot(ctx, snapshot, tableDescriptor);
    });
  }

  @Test
  public void testPreCloneSnapshotReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preCloneSnapshot(ctx, snapshot, tableDescriptor);
    });
  }

  @Test
  public void testPreRestoreSnapshotReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preRestoreSnapshot(ctx, snapshot, tableDescriptor);
    });
  }

  @Test
  public void testPreDeleteSnapshotReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preDeleteSnapshot(ctx, snapshot);
    });
  }

  @Test
  public void testPreCreateNamespaceReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preCreateNamespace(ctx, ns);
    });
  }

  @Test
  public void testPreModifyNamespaceReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preModifyNamespace(ctx, currentNsDescriptor, newNsDescriptor);
    });
  }

  @Test
  public void testPreDeleteNamespaceReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preDeleteNamespace(ctx, namespace);
    });
  }

  @Test
  public void testPreMasterStoreFlushReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preMasterStoreFlush(ctx);
    });
  }

  @Test
  public void testPreSetUserQuotaReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSetUserQuota(ctx, userName, quotas);
    });
  }

  @Test
  public void testPreSetUserQuotaOnTableReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSetUserQuota(ctx, userName, tableName, quotas);
    });
  }

  @Test
  public void testPreSetUserQuotaOnNamespaceReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSetUserQuota(ctx, userName, namespace, quotas);
    });
  }

  @Test
  public void testPreSetTableQuotaReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSetTableQuota(ctx, tableName, quotas);
    });
  }

  @Test
  public void testPreSetNamespaceQuotaReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSetNamespaceQuota(ctx, namespace, quotas);
    });
  }

  @Test
  public void testPreSetRegionServerQuotaReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preSetRegionServerQuota(ctx, regionServer, quotas);
    });
  }

  @Test
  public void testPreMergeRegionsReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preMergeRegions(ctx, regionsToMerge);
    });
  }

  @Test
  public void testPreMoveServersAndTablesReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preMoveServersAndTables(ctx, servers, tables, targetGroup);
    });
  }

  @Test
  public void testPreMoveServersReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preMoveServers(ctx, servers, targetGroup);
    });
  }

  @Test
  public void testPreMoveTablesReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preMoveTables(ctx, tables, targetGroup);
    });
  }

  @Test
  public void testPreAddRSGroupReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preAddRSGroup(ctx, name);
    });
  }

  @Test
  public void testPreRemoveRSGroupReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preRemoveRSGroup(ctx, name);
    });
  }

  @Test
  public void testPreBalanceRSGroupReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preBalanceRSGroup(ctx, groupName, request);
    });
  }

  @Test
  public void testPreRemoveServersReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preRemoveServers(ctx, servers);
    });
  }

  @Test
  public void testPreRenameRSGroupReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preRenameRSGroup(ctx, oldName, newName);
    });
  }

  @Test
  public void testPreUpdateRSGroupConfigReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preUpdateRSGroupConfig(ctx, groupName, configuration);
    });
  }

  @Test
  public void testPreAddReplicationPeerReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preAddReplicationPeer(ctx, peerId, peerConfig);
    });
  }

  @Test
  public void testPreRemoveReplicationPeerReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preRemoveReplicationPeer(ctx, peerId);
    });
  }

  @Test
  public void testPreEnableReplicationPeerReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preEnableReplicationPeer(ctx, peerId);
    });
  }

  @Test
  public void testPreDisableReplicationPeerReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preDisableReplicationPeer(ctx, peerId);
    });
  }

  @Test
  public void testPreUpdateReplicationPeerConfigReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preUpdateReplicationPeerConfig(ctx, peerId, peerConfig);
    });
  }

  @Test
  public void testPreTransitReplicationPeerSyncReplicationStateReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preTransitReplicationPeerSyncReplicationState(ctx, peerId, state);
    });
  }

  @Test
  public void testPreGrantReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preGrant(ctx, userPermission, mergeExistingPermissions);
    });
  }

  @Test
  public void testPreRevokeReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class, () -> {
      MasterReadOnlyController.preRevoke(ctx, userPermission);
    });
  }
}
