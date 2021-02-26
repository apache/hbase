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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;

/**
 * Tests invocation of the {@link org.apache.hadoop.hbase.coprocessor.MasterObserver}
 * interface hooks at all appropriate times during normal HMaster operations.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestMasterObserver {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterObserver.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterObserver.class);

  public static CountDownLatch tableCreationLatch = new CountDownLatch(1);
  public static CountDownLatch tableDeletionLatch = new CountDownLatch(1);

  public static class CPMasterObserver implements MasterCoprocessor, MasterObserver {

    private boolean preCreateTableRegionInfosCalled;
    private boolean preCreateTableCalled;
    private boolean postCreateTableCalled;
    private boolean preDeleteTableCalled;
    private boolean postDeleteTableCalled;
    private boolean preTruncateTableCalled;
    private boolean postTruncateTableCalled;
    private boolean preModifyTableCalled;
    private boolean postModifyTableCalled;
    private boolean preCreateNamespaceCalled;
    private boolean postCreateNamespaceCalled;
    private boolean preDeleteNamespaceCalled;
    private boolean postDeleteNamespaceCalled;
    private boolean preModifyNamespaceCalled;
    private boolean postModifyNamespaceCalled;
    private boolean preGetNamespaceDescriptorCalled;
    private boolean postGetNamespaceDescriptorCalled;
    private boolean preListNamespacesCalled;
    private boolean postListNamespacesCalled;
    private boolean preListNamespaceDescriptorsCalled;
    private boolean postListNamespaceDescriptorsCalled;
    private boolean preAddColumnCalled;
    private boolean postAddColumnCalled;
    private boolean preModifyColumnCalled;
    private boolean postModifyColumnCalled;
    private boolean preDeleteColumnCalled;
    private boolean postDeleteColumnCalled;
    private boolean preEnableTableCalled;
    private boolean postEnableTableCalled;
    private boolean preDisableTableCalled;
    private boolean postDisableTableCalled;
    private boolean preAbortProcedureCalled;
    private boolean postAbortProcedureCalled;
    private boolean preGetProceduresCalled;
    private boolean postGetProceduresCalled;
    private boolean preGetLocksCalled;
    private boolean postGetLocksCalled;
    private boolean preMoveCalled;
    private boolean postMoveCalled;
    private boolean preAssignCalled;
    private boolean postAssignCalled;
    private boolean preUnassignCalled;
    private boolean postUnassignCalled;
    private boolean preRegionOfflineCalled;
    private boolean postRegionOfflineCalled;
    private boolean preBalanceCalled;
    private boolean postBalanceCalled;
    private boolean preBalanceSwitchCalled;
    private boolean postBalanceSwitchCalled;
    private boolean preShutdownCalled;
    private boolean preStopMasterCalled;
    private boolean preMasterInitializationCalled;
    private boolean postStartMasterCalled;
    private boolean startCalled;
    private boolean stopCalled;
    private boolean preSnapshotCalled;
    private boolean postSnapshotCalled;
    private boolean preListSnapshotCalled;
    private boolean postListSnapshotCalled;
    private boolean preCloneSnapshotCalled;
    private boolean postCloneSnapshotCalled;
    private boolean preRestoreSnapshotCalled;
    private boolean postRestoreSnapshotCalled;
    private boolean preDeleteSnapshotCalled;
    private boolean postDeleteSnapshotCalled;
    private boolean preCreateTableActionCalled;
    private boolean postCompletedCreateTableActionCalled;
    private boolean preDeleteTableActionCalled;
    private boolean postCompletedDeleteTableActionCalled;
    private boolean preTruncateTableActionCalled;
    private boolean postCompletedTruncateTableActionCalled;
    private boolean preAddColumnFamilyActionCalled;
    private boolean postCompletedAddColumnFamilyActionCalled;
    private boolean preModifyColumnFamilyActionCalled;
    private boolean postCompletedModifyColumnFamilyActionCalled;
    private boolean preDeleteColumnFamilyActionCalled;
    private boolean postCompletedDeleteColumnFamilyActionCalled;
    private boolean preEnableTableActionCalled;
    private boolean postCompletedEnableTableActionCalled;
    private boolean preDisableTableActionCalled;
    private boolean postCompletedDisableTableActionCalled;
    private boolean preModifyTableActionCalled;
    private boolean postCompletedModifyTableActionCalled;
    private boolean preGetTableDescriptorsCalled;
    private boolean postGetTableDescriptorsCalled;
    private boolean postGetTableNamesCalled;
    private boolean preGetTableNamesCalled;
    private boolean preMergeRegionsCalled;
    private boolean postMergeRegionsCalled;
    private boolean preRequestLockCalled;
    private boolean postRequestLockCalled;
    private boolean preLockHeartbeatCalled;
    private boolean postLockHeartbeatCalled;

    public void resetStates() {
      preCreateTableRegionInfosCalled = false;
      preCreateTableCalled = false;
      postCreateTableCalled = false;
      preDeleteTableCalled = false;
      postDeleteTableCalled = false;
      preTruncateTableCalled = false;
      postTruncateTableCalled = false;
      preModifyTableCalled = false;
      postModifyTableCalled = false;
      preCreateNamespaceCalled = false;
      postCreateNamespaceCalled = false;
      preDeleteNamespaceCalled = false;
      postDeleteNamespaceCalled = false;
      preModifyNamespaceCalled = false;
      postModifyNamespaceCalled = false;
      preGetNamespaceDescriptorCalled = false;
      postGetNamespaceDescriptorCalled = false;
      preListNamespacesCalled = false;
      postListNamespacesCalled = false;
      preListNamespaceDescriptorsCalled = false;
      postListNamespaceDescriptorsCalled = false;
      preAddColumnCalled = false;
      postAddColumnCalled = false;
      preModifyColumnCalled = false;
      postModifyColumnCalled = false;
      preDeleteColumnCalled = false;
      postDeleteColumnCalled = false;
      preEnableTableCalled = false;
      postEnableTableCalled = false;
      preDisableTableCalled = false;
      postDisableTableCalled = false;
      preAbortProcedureCalled = false;
      postAbortProcedureCalled = false;
      preGetProceduresCalled = false;
      postGetProceduresCalled = false;
      preGetLocksCalled = false;
      postGetLocksCalled = false;
      preMoveCalled= false;
      postMoveCalled = false;
      preAssignCalled = false;
      postAssignCalled = false;
      preUnassignCalled = false;
      postUnassignCalled = false;
      preRegionOfflineCalled = false;
      postRegionOfflineCalled = false;
      preBalanceCalled = false;
      postBalanceCalled = false;
      preBalanceSwitchCalled = false;
      postBalanceSwitchCalled = false;
      preShutdownCalled = false;
      preStopMasterCalled = false;
      preSnapshotCalled = false;
      postSnapshotCalled = false;
      preListSnapshotCalled = false;
      postListSnapshotCalled = false;
      preCloneSnapshotCalled = false;
      postCloneSnapshotCalled = false;
      preRestoreSnapshotCalled = false;
      postRestoreSnapshotCalled = false;
      preDeleteSnapshotCalled = false;
      postDeleteSnapshotCalled = false;
      preCreateTableActionCalled = false;
      postCompletedCreateTableActionCalled = false;
      preDeleteTableActionCalled = false;
      postCompletedDeleteTableActionCalled = false;
      preTruncateTableActionCalled = false;
      postCompletedTruncateTableActionCalled = false;
      preModifyTableActionCalled = false;
      postCompletedModifyTableActionCalled = false;
      preAddColumnFamilyActionCalled = false;
      postCompletedAddColumnFamilyActionCalled = false;
      preModifyColumnFamilyActionCalled = false;
      postCompletedModifyColumnFamilyActionCalled = false;
      preDeleteColumnFamilyActionCalled = false;
      postCompletedDeleteColumnFamilyActionCalled = false;
      preEnableTableActionCalled = false;
      postCompletedEnableTableActionCalled = false;
      preDisableTableActionCalled = false;
      postCompletedDisableTableActionCalled = false;
      preGetTableDescriptorsCalled = false;
      postGetTableDescriptorsCalled = false;
      postGetTableNamesCalled = false;
      preGetTableNamesCalled = false;
      preMergeRegionsCalled = false;
      postMergeRegionsCalled = false;
      preRequestLockCalled = false;
      postRequestLockCalled = false;
      preLockHeartbeatCalled = false;
      postLockHeartbeatCalled = false;
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preMergeRegions(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final RegionInfo[] regionsToMerge) throws IOException {
      preMergeRegionsCalled = true;
    }

    @Override
    public void postMergeRegions(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final RegionInfo[] regionsToMerge) throws IOException {
      postMergeRegionsCalled = true;
    }

    public boolean wasMergeRegionsCalled() {
      return preMergeRegionsCalled && postMergeRegionsCalled;
    }

    @Override
    public TableDescriptor preCreateTableRegionsInfos(
        ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc)
        throws IOException {
      preCreateTableRegionInfosCalled = true;
      return desc;
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableDescriptor desc, RegionInfo[] regions) throws IOException {
      preCreateTableCalled = true;
    }

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableDescriptor desc, RegionInfo[] regions) throws IOException {
      postCreateTableCalled = true;
    }

    public boolean wasCreateTableCalled() {
      return preCreateTableRegionInfosCalled && preCreateTableCalled && postCreateTableCalled;
    }

    public boolean preCreateTableCalledOnly() {
      return preCreateTableRegionInfosCalled && preCreateTableCalled && !postCreateTableCalled;
    }

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      preDeleteTableCalled = true;
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      postDeleteTableCalled = true;
    }

    public boolean wasDeleteTableCalled() {
      return preDeleteTableCalled && postDeleteTableCalled;
    }

    public boolean preDeleteTableCalledOnly() {
      return preDeleteTableCalled && !postDeleteTableCalled;
    }

    @Override
    public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      preTruncateTableCalled = true;
    }

    @Override
    public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      postTruncateTableCalled = true;
    }

    public boolean wasTruncateTableCalled() {
      return preTruncateTableCalled && postTruncateTableCalled;
    }

    public boolean preTruncateTableCalledOnly() {
      return preTruncateTableCalled && !postTruncateTableCalled;
    }

    @Override
    public void postSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final boolean newValue, final MasterSwitchType switchType) throws IOException {
    }

    @Override
    public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
        throws IOException {
      preModifyTableCalled = true;
      return newDescriptor;
    }

    @Override
    public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName, TableDescriptor htd) throws IOException {
      postModifyTableCalled = true;
    }

    public boolean wasModifyTableCalled() {
      return preModifyTableCalled && postModifyTableCalled;
    }

    public boolean preModifyTableCalledOnly() {
      return preModifyTableCalled && !postModifyTableCalled;
    }

    @Override
    public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> env,
        NamespaceDescriptor ns) throws IOException {
      preCreateNamespaceCalled = true;
    }

    @Override
    public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> env,
        NamespaceDescriptor ns) throws IOException {
      postCreateNamespaceCalled = true;
    }

    public boolean wasCreateNamespaceCalled() {
      return preCreateNamespaceCalled && postCreateNamespaceCalled;
    }

    public boolean preCreateNamespaceCalledOnly() {
      return preCreateNamespaceCalled && !postCreateNamespaceCalled;
    }

    @Override
    public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> env,
        String name) throws IOException {
      preDeleteNamespaceCalled = true;
    }

    @Override
    public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> env,
        String name) throws IOException {
      postDeleteNamespaceCalled = true;
    }

    public boolean wasDeleteNamespaceCalled() {
      return preDeleteNamespaceCalled && postDeleteNamespaceCalled;
    }

    public boolean preDeleteNamespaceCalledOnly() {
      return preDeleteNamespaceCalled && !postDeleteNamespaceCalled;
    }

    @Override
    public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> env,
        NamespaceDescriptor ns) throws IOException {
      preModifyNamespaceCalled = true;
    }

    @Override
    public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> env,
        NamespaceDescriptor ns) throws IOException {
      postModifyNamespaceCalled = true;
    }

    public boolean wasModifyNamespaceCalled() {
      return preModifyNamespaceCalled && postModifyNamespaceCalled;
    }

    public boolean preModifyNamespaceCalledOnly() {
      return preModifyNamespaceCalled && !postModifyNamespaceCalled;
    }


    @Override
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String namespace) throws IOException {
      preGetNamespaceDescriptorCalled = true;
    }

    @Override
    public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
        NamespaceDescriptor ns) throws IOException {
      postGetNamespaceDescriptorCalled = true;
    }

    public boolean wasGetNamespaceDescriptorCalled() {
      return preGetNamespaceDescriptorCalled && postGetNamespaceDescriptorCalled;
    }

    @Override
    public void preListNamespaces(ObserverContext<MasterCoprocessorEnvironment> ctx,
        List<String> namespaces) {
      preListNamespacesCalled = true;
    }

    @Override
    public void postListNamespaces(ObserverContext<MasterCoprocessorEnvironment> ctx,
        List<String> namespaces) {
      postListNamespacesCalled = true;
    }

    @Override
    public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> env,
        List<NamespaceDescriptor> descriptors) throws IOException {
      preListNamespaceDescriptorsCalled = true;
    }

    @Override
    public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> env,
        List<NamespaceDescriptor> descriptors) throws IOException {
      postListNamespaceDescriptorsCalled = true;
    }

    public boolean wasListNamespaceDescriptorsCalled() {
      return preListNamespaceDescriptorsCalled && postListNamespaceDescriptorsCalled;
    }

    public boolean preListNamespaceDescriptorsCalledOnly() {
      return preListNamespaceDescriptorsCalled && !postListNamespaceDescriptorsCalled;
    }

    @Override
    public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      preEnableTableCalled = true;
    }

    @Override
    public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      postEnableTableCalled = true;
    }

    public boolean wasEnableTableCalled() {
      return preEnableTableCalled && postEnableTableCalled;
    }

    public boolean preEnableTableCalledOnly() {
      return preEnableTableCalled && !postEnableTableCalled;
    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      preDisableTableCalled = true;
    }

    @Override
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName) throws IOException {
      postDisableTableCalled = true;
    }

    public boolean wasDisableTableCalled() {
      return preDisableTableCalled && postDisableTableCalled;
    }

    public boolean preDisableTableCalledOnly() {
      return preDisableTableCalled && !postDisableTableCalled;
    }

    @Override
    public void preAbortProcedure(
        ObserverContext<MasterCoprocessorEnvironment> ctx, final long procId) throws IOException {
      preAbortProcedureCalled = true;
    }

    @Override
    public void postAbortProcedure(
        ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
      postAbortProcedureCalled = true;
    }

    public boolean wasAbortProcedureCalled() {
      return preAbortProcedureCalled && postAbortProcedureCalled;
    }

    public boolean wasPreAbortProcedureCalledOnly() {
      return preAbortProcedureCalled && !postAbortProcedureCalled;
    }

    @Override
    public void preGetProcedures(
        ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
      preGetProceduresCalled = true;
    }

    @Override
    public void postGetProcedures(
        ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
      postGetProceduresCalled = true;
    }

    public boolean wasGetProceduresCalled() {
      return preGetProceduresCalled && postGetProceduresCalled;
    }

    public boolean wasPreGetProceduresCalledOnly() {
      return preGetProceduresCalled && !postGetProceduresCalled;
    }

    @Override
    public void preGetLocks(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
      preGetLocksCalled = true;
    }

    @Override
    public void postGetLocks(ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      postGetLocksCalled = true;
    }

    public boolean wasGetLocksCalled() {
      return preGetLocksCalled && postGetLocksCalled;
    }

    public boolean wasPreGetLocksCalledOnly() {
      return preGetLocksCalled && !postGetLocksCalled;
    }

    @Override
    public void preMove(ObserverContext<MasterCoprocessorEnvironment> env,
        RegionInfo region, ServerName srcServer, ServerName destServer)
    throws IOException {
      preMoveCalled = true;
    }

    @Override
    public void postMove(ObserverContext<MasterCoprocessorEnvironment> env, RegionInfo region,
        ServerName srcServer, ServerName destServer)
    throws IOException {
      postMoveCalled = true;
    }

    public boolean wasMoveCalled() {
      return preMoveCalled && postMoveCalled;
    }

    public boolean preMoveCalledOnly() {
      return preMoveCalled && !postMoveCalled;
    }

    @Override
    public void preAssign(ObserverContext<MasterCoprocessorEnvironment> env,
        final RegionInfo regionInfo) throws IOException {
      preAssignCalled = true;
    }

    @Override
    public void postAssign(ObserverContext<MasterCoprocessorEnvironment> env,
        final RegionInfo regionInfo) throws IOException {
      postAssignCalled = true;
    }

    public boolean wasAssignCalled() {
      return preAssignCalled && postAssignCalled;
    }

    public boolean preAssignCalledOnly() {
      return preAssignCalled && !postAssignCalled;
    }

    @Override
    public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> env,
        final RegionInfo regionInfo) throws IOException {
      preUnassignCalled = true;
    }

    @Override
    public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> env,
        final RegionInfo regionInfo) throws IOException {
      postUnassignCalled = true;
    }

    public boolean wasUnassignCalled() {
      return preUnassignCalled && postUnassignCalled;
    }

    public boolean preUnassignCalledOnly() {
      return preUnassignCalled && !postUnassignCalled;
    }

    @Override
    public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> env,
        final RegionInfo regionInfo) throws IOException {
      preRegionOfflineCalled = true;
    }

    @Override
    public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> env,
        final RegionInfo regionInfo) throws IOException {
      postRegionOfflineCalled = true;
    }

    public boolean wasRegionOfflineCalled() {
      return preRegionOfflineCalled && postRegionOfflineCalled;
    }

    public boolean preRegionOfflineCalledOnly() {
      return preRegionOfflineCalled && !postRegionOfflineCalled;
    }

    @Override
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> env)
        throws IOException {
      preBalanceCalled = true;
    }

    @Override
    public void postBalance(ObserverContext<MasterCoprocessorEnvironment> env,
        List<RegionPlan> plans) throws IOException {
      postBalanceCalled = true;
    }

    public boolean wasBalanceCalled() {
      return preBalanceCalled && postBalanceCalled;
    }

    public boolean preBalanceCalledOnly() {
      return preBalanceCalled && !postBalanceCalled;
    }

    @Override
    public void preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> env, boolean b)
        throws IOException {
      preBalanceSwitchCalled = true;
    }

    @Override
    public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> env,
        boolean oldValue, boolean newValue) throws IOException {
      postBalanceSwitchCalled = true;
    }

    public boolean wasBalanceSwitchCalled() {
      return preBalanceSwitchCalled && postBalanceSwitchCalled;
    }

    public boolean preBalanceSwitchCalledOnly() {
      return preBalanceSwitchCalled && !postBalanceSwitchCalled;
    }

    @Override
    public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> env)
        throws IOException {
      preShutdownCalled = true;
    }

    public boolean wasShutdownCalled() {
      return preShutdownCalled;
    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> env)
        throws IOException {
      preStopMasterCalled = true;
    }

    public boolean wasStopMasterCalled() {
      return preStopMasterCalled;
    }

    @Override
    public void preMasterInitialization(
        ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
      preMasterInitializationCalled = true;
    }

    public boolean wasMasterInitializationCalled(){
      return preMasterInitializationCalled;
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      postStartMasterCalled = true;
    }

    public boolean wasStartMasterCalled() {
      return postStartMasterCalled;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      startCalled = true;
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
      stopCalled = true;
    }

    public boolean wasStarted() { return startCalled; }

    public boolean wasStopped() { return stopCalled; }

    @Override
    public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
        throws IOException {
      preSnapshotCalled = true;
    }

    @Override
    public void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
        throws IOException {
      postSnapshotCalled = true;
    }

    public boolean wasSnapshotCalled() {
      return preSnapshotCalled && postSnapshotCalled;
    }

    @Override
    public void preListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot) throws IOException {
      preListSnapshotCalled = true;
    }

    @Override
    public void postListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot) throws IOException {
      postListSnapshotCalled = true;
    }

    public boolean wasListSnapshotCalled() {
      return preListSnapshotCalled && postListSnapshotCalled;
    }

    @Override
    public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
        throws IOException {
      preCloneSnapshotCalled = true;
    }

    @Override
    public void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
        throws IOException {
      postCloneSnapshotCalled = true;
    }

    public boolean wasCloneSnapshotCalled() {
      return preCloneSnapshotCalled && postCloneSnapshotCalled;
    }

    @Override
    public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
        throws IOException {
      preRestoreSnapshotCalled = true;
    }

    @Override
    public void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
        throws IOException {
      postRestoreSnapshotCalled = true;
    }

    public boolean wasRestoreSnapshotCalled() {
      return preRestoreSnapshotCalled && postRestoreSnapshotCalled;
    }

    @Override
    public void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot) throws IOException {
      preDeleteSnapshotCalled = true;
    }

    @Override
    public void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final SnapshotDescription snapshot) throws IOException {
      postDeleteSnapshotCalled = true;
    }

    public boolean wasDeleteSnapshotCalled() {
      return preDeleteSnapshotCalled && postDeleteSnapshotCalled;
    }

    @Override
    public void preCreateTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> env,
        final TableDescriptor desc,
        final RegionInfo[] regions) throws IOException {
      preCreateTableActionCalled = true;
    }

    @Override
    public void postCompletedCreateTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final TableDescriptor desc,
        final RegionInfo[] regions) throws IOException {
      postCompletedCreateTableActionCalled = true;
      tableCreationLatch.countDown();
    }

    public boolean wasPreCreateTableActionCalled(){
      return preCreateTableActionCalled;
    }
    public boolean wasCreateTableActionCalled() {
      return preCreateTableActionCalled && postCompletedCreateTableActionCalled;
    }

    public boolean wasCreateTableActionCalledOnly() {
      return preCreateTableActionCalled && !postCompletedCreateTableActionCalled;
    }

    @Override
    public void preDeleteTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> env, final TableName tableName)
        throws IOException {
      preDeleteTableActionCalled = true;
    }

    @Override
    public void postCompletedDeleteTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
        throws IOException {
      postCompletedDeleteTableActionCalled = true;
      tableDeletionLatch.countDown();
    }

    public boolean wasDeleteTableActionCalled() {
      return preDeleteTableActionCalled && postCompletedDeleteTableActionCalled;
    }

    public boolean wasDeleteTableActionCalledOnly() {
      return preDeleteTableActionCalled && !postCompletedDeleteTableActionCalled;
    }

    @Override
    public void preTruncateTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> env, final TableName tableName)
        throws IOException {
      preTruncateTableActionCalled = true;
    }

    @Override
    public void postCompletedTruncateTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
        throws IOException {
      postCompletedTruncateTableActionCalled = true;
    }

    public boolean wasTruncateTableActionCalled() {
      return preTruncateTableActionCalled && postCompletedTruncateTableActionCalled;
    }

    public boolean wasTruncateTableActionCalledOnly() {
      return preTruncateTableActionCalled && !postCompletedTruncateTableActionCalled;
    }

    @Override
    public void preModifyTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> env,
        final TableName tableName,
        final TableDescriptor htd) throws IOException {
      preModifyTableActionCalled = true;
    }

    @Override
    public void postCompletedModifyTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> env,
        final TableName tableName,
        final TableDescriptor htd) throws IOException {
      postCompletedModifyTableActionCalled = true;
    }

    public boolean wasModifyTableActionCalled() {
      return preModifyTableActionCalled && postCompletedModifyTableActionCalled;
    }

    public boolean wasModifyTableActionCalledOnly() {
      return preModifyTableActionCalled && !postCompletedModifyTableActionCalled;
    }

    @Override
    public void preEnableTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
        throws IOException {
      preEnableTableActionCalled = true;
    }

    @Override
    public void postCompletedEnableTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
        throws IOException {
      postCompletedEnableTableActionCalled = true;
    }

    public boolean wasEnableTableActionCalled() {
      return preEnableTableActionCalled && postCompletedEnableTableActionCalled;
    }

    public boolean preEnableTableActionCalledOnly() {
      return preEnableTableActionCalled && !postCompletedEnableTableActionCalled;
    }

    @Override
    public void preDisableTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
        throws IOException {
      preDisableTableActionCalled = true;
    }

    @Override
    public void postCompletedDisableTableAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
        throws IOException {
      postCompletedDisableTableActionCalled = true;
    }

    public boolean wasDisableTableActionCalled() {
      return preDisableTableActionCalled && postCompletedDisableTableActionCalled;
    }

    public boolean preDisableTableActionCalledOnly() {
      return preDisableTableActionCalled && !postCompletedDisableTableActionCalled;
    }

    @Override
    public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
        List<TableName> tableNamesList, List<TableDescriptor> descriptors, String regex)
        throws IOException {
      preGetTableDescriptorsCalled = true;
    }

    @Override
    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
        List<TableName> tableNamesList, List<TableDescriptor> descriptors,
        String regex) throws IOException {
      postGetTableDescriptorsCalled = true;
    }

    public boolean wasGetTableDescriptorsCalled() {
      return preGetTableDescriptorsCalled && postGetTableDescriptorsCalled;
    }

    @Override
    public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
        List<TableDescriptor> descriptors, String regex) throws IOException {
      preGetTableNamesCalled = true;
    }

    @Override
    public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
        List<TableDescriptor> descriptors, String regex) throws IOException {
      postGetTableNamesCalled = true;
    }

    public boolean wasGetTableNamesCalled() {
      return preGetTableNamesCalled && postGetTableNamesCalled;
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
        final String userName, final GlobalQuotaSettings quotas) throws IOException {
    }

    @Override
    public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String userName, final GlobalQuotaSettings quotas) throws IOException {
    }

    @Override
    public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String userName, final TableName tableName, final GlobalQuotaSettings quotas)
            throws IOException {
    }

    @Override
    public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String userName, final TableName tableName, final GlobalQuotaSettings quotas)
            throws IOException {
    }

    @Override
    public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String userName, final String namespace, final GlobalQuotaSettings quotas)
            throws IOException {
    }

    @Override
    public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String userName, final String namespace, final GlobalQuotaSettings quotas)
            throws IOException {
    }

    @Override
    public void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final TableName tableName, final GlobalQuotaSettings quotas) throws IOException {
    }

    @Override
    public void postSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final TableName tableName, final GlobalQuotaSettings quotas) throws IOException {
    }

    @Override
    public void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String namespace, final GlobalQuotaSettings quotas) throws IOException {
    }

    @Override
    public void postSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final String namespace, final GlobalQuotaSettings quotas) throws IOException {
    }

    @Override
    public void preMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
    }

    @Override
    public void postMoveServersAndTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, Set<TableName> tables,String targetGroup) throws IOException {
    }

    @Override
    public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               Set<Address> servers, String targetGroup) throws IOException {
    }

    @Override
    public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                Set<Address> servers, String targetGroup) throws IOException {
    }

    @Override
    public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              Set<TableName> tables, String targetGroupGroup) throws IOException {
    }

    @Override
    public void postMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               Set<TableName> tables, String targetGroup) throws IOException {
    }

    @Override
    public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              String name) throws IOException {
    }

    @Override
    public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               String name) throws IOException {
    }

    @Override
    public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 String name) throws IOException {
    }

    @Override
    public void postRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  String name) throws IOException {
    }

    @Override
    public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  String groupName) throws IOException {
    }

    @Override
    public void postBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String groupName, boolean balancerRan) throws IOException {
    }

    @Override
    public void preRequestLock(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace,
        TableName tableName, RegionInfo[] regionInfos, String description) throws IOException {
      preRequestLockCalled = true;
    }

    @Override
    public void postRequestLock(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace,
        TableName tableName, RegionInfo[] regionInfos, String description) throws IOException {
      postRequestLockCalled = true;
    }

    @Override
    public void preLockHeartbeat(ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tn, String description) throws IOException {
      preLockHeartbeatCalled = true;
    }

    @Override
    public void postLockHeartbeat(ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      postLockHeartbeatCalled = true;
    }

    public boolean preAndPostForQueueLockAndHeartbeatLockCalled() {
      return preRequestLockCalled && postRequestLockCalled && preLockHeartbeatCalled &&
          postLockHeartbeatCalled;
    }

    @Override
    public void preSplitRegion(
        final ObserverContext<MasterCoprocessorEnvironment> c,
        final TableName tableName,
        final byte[] splitRow) throws IOException {
    }

    @Override
    public void preSplitRegionAction(
        final ObserverContext<MasterCoprocessorEnvironment> c,
        final TableName tableName,
        final byte[] splitRow) throws IOException {
    }

    @Override
    public void postCompletedSplitRegionAction(
        final ObserverContext<MasterCoprocessorEnvironment> c,
        final RegionInfo regionInfoA,
        final RegionInfo regionInfoB) throws IOException {
    }

    @Override
    public void preSplitRegionBeforeMETAAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final byte[] splitKey,
        final List<Mutation> metaEntries) throws IOException {
    }

    @Override
    public void preSplitRegionAfterMETAAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    }

    @Override
    public void postRollBackSplitRegionAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    }

    @Override
    public void preMergeRegionsAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final RegionInfo[] regionsToMerge) throws IOException {
    }

    @Override
    public void postCompletedMergeRegionsAction(
        final ObserverContext<MasterCoprocessorEnvironment> c,
        final RegionInfo[] regionsToMerge,
        final RegionInfo mergedRegion) throws IOException {
    }

    @Override
    public void preMergeRegionsCommitAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final RegionInfo[] regionsToMerge,
        final List<Mutation> metaEntries) throws IOException {
    }

    @Override
    public void postMergeRegionsCommitAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final RegionInfo[] regionsToMerge,
        final RegionInfo mergedRegion) throws IOException {
    }

    @Override
    public void postRollBackMergeRegionsAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final RegionInfo[] regionsToMerge) throws IOException {
    }

  }

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static byte[] TEST_SNAPSHOT = Bytes.toBytes("observed_snapshot");
  private static TableName TEST_CLONE = TableName.valueOf("observed_clone");
  private static byte[] TEST_FAMILY = Bytes.toBytes("fam1");
  private static byte[] TEST_FAMILY2 = Bytes.toBytes("fam2");
  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CPMasterObserver.class.getName());
    // We need more than one data server on this test
    UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStarted() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    assertTrue("Master should be active", master.isActiveMaster());
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    assertNotNull("CoprocessorHost should not be null", host);
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    assertNotNull("CPMasterObserver coprocessor not found or not installed!", cp);

    // check basic lifecycle
    assertTrue("MasterObserver should have been started", cp.wasStarted());
    assertTrue("preMasterInitialization() hook should have been called",
        cp.wasMasterInitializationCalled());
    assertTrue("postStartMaster() hook should have been called",
        cp.wasStartMasterCalled());
  }

  @Test
  public void testTableOperations() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();
    assertFalse("No table created yet", cp.wasCreateTableCalled());

    // create a table
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    try(Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
        Admin admin = connection.getAdmin()) {
      tableCreationLatch = new CountDownLatch(1);
      admin.createTable(htd, Arrays.copyOfRange(HBaseTestingUtility.KEYS,
        1, HBaseTestingUtility.KEYS.length));

      assertTrue("Test table should be created", cp.wasCreateTableCalled());
      tableCreationLatch.await();
      assertTrue("Table pre create handler called.", cp
        .wasPreCreateTableActionCalled());
      assertTrue("Table create handler should be called.",
        cp.wasCreateTableActionCalled());

      RegionLocator regionLocator = connection.getRegionLocator(htd.getTableName());
      List<HRegionLocation> regions = regionLocator.getAllRegionLocations();

      admin.mergeRegionsAsync(regions.get(0).getRegionInfo().getEncodedNameAsBytes(),
        regions.get(1).getRegionInfo().getEncodedNameAsBytes(), true);
      assertTrue("Coprocessor should have been called on region merge",
        cp.wasMergeRegionsCalled());

      tableCreationLatch = new CountDownLatch(1);
      admin.disableTable(tableName);
      assertTrue(admin.isTableDisabled(tableName));
      assertTrue("Coprocessor should have been called on table disable",
        cp.wasDisableTableCalled());
      assertTrue("Disable table handler should be called.",
        cp.wasDisableTableActionCalled());

      // enable
      assertFalse(cp.wasEnableTableCalled());
      admin.enableTable(tableName);
      assertTrue(admin.isTableEnabled(tableName));
      assertTrue("Coprocessor should have been called on table enable",
        cp.wasEnableTableCalled());
      assertTrue("Enable table handler should be called.",
        cp.wasEnableTableActionCalled());

      admin.disableTable(tableName);
      assertTrue(admin.isTableDisabled(tableName));

      // modify table
      htd.setMaxFileSize(512 * 1024 * 1024);
      modifyTableSync(admin, tableName, htd);
      assertTrue("Test table should have been modified",
        cp.wasModifyTableCalled());

      // truncate table
      admin.truncateTable(tableName, false);

      // delete table
      admin.disableTable(tableName);
      assertTrue(admin.isTableDisabled(tableName));
      deleteTable(admin, tableName);
      assertFalse("Test table should have been deleted",
        admin.tableExists(tableName));
      assertTrue("Coprocessor should have been called on table delete",
        cp.wasDeleteTableCalled());
      assertTrue("Delete table handler should be called.",
        cp.wasDeleteTableActionCalled());

      // When bypass was supported, we'd turn off bypass and rerun tests. Leaving rerun in place.
      cp.resetStates();

      admin.createTable(htd);
      assertTrue("Test table should be created", cp.wasCreateTableCalled());
      tableCreationLatch.await();
      assertTrue("Table pre create handler called.", cp
        .wasPreCreateTableActionCalled());
      assertTrue("Table create handler should be called.",
        cp.wasCreateTableActionCalled());

      // disable
      assertFalse(cp.wasDisableTableCalled());
      assertFalse(cp.wasDisableTableActionCalled());
      admin.disableTable(tableName);
      assertTrue(admin.isTableDisabled(tableName));
      assertTrue("Coprocessor should have been called on table disable",
        cp.wasDisableTableCalled());
      assertTrue("Disable table handler should be called.",
        cp.wasDisableTableActionCalled());

      // modify table
      htd.setMaxFileSize(512 * 1024 * 1024);
      modifyTableSync(admin, tableName, htd);
      assertTrue("Test table should have been modified",
        cp.wasModifyTableCalled());

      // enable
      assertFalse(cp.wasEnableTableCalled());
      assertFalse(cp.wasEnableTableActionCalled());
      admin.enableTable(tableName);
      assertTrue(admin.isTableEnabled(tableName));
      assertTrue("Coprocessor should have been called on table enable",
        cp.wasEnableTableCalled());
      assertTrue("Enable table handler should be called.",
        cp.wasEnableTableActionCalled());

      // disable again
      admin.disableTable(tableName);
      assertTrue(admin.isTableDisabled(tableName));

      // delete table
      assertFalse("No table deleted yet", cp.wasDeleteTableCalled());
      assertFalse("Delete table handler should not be called.",
        cp.wasDeleteTableActionCalled());
      deleteTable(admin, tableName);
      assertFalse("Test table should have been deleted",
        admin.tableExists(tableName));
      assertTrue("Coprocessor should have been called on table delete",
        cp.wasDeleteTableCalled());
      assertTrue("Delete table handler should be called.",
        cp.wasDeleteTableActionCalled());
    }
  }

  @Test
  public void testSnapshotOperations() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    // create a table
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    Admin admin = UTIL.getAdmin();

    tableCreationLatch = new CountDownLatch(1);
    admin.createTable(htd);
    tableCreationLatch.await();
    tableCreationLatch = new CountDownLatch(1);

    admin.disableTable(tableName);
    assertTrue(admin.isTableDisabled(tableName));

    try {
      // Test snapshot operation
      assertFalse("Coprocessor should not have been called yet",
        cp.wasSnapshotCalled());
      admin.snapshot(TEST_SNAPSHOT, tableName);
      assertTrue("Coprocessor should have been called on snapshot",
        cp.wasSnapshotCalled());

      //Test list operation
      admin.listSnapshots();
      assertTrue("Coprocessor should have been called on snapshot list",
        cp.wasListSnapshotCalled());

      // Test clone operation
      admin.cloneSnapshot(TEST_SNAPSHOT, TEST_CLONE);
      assertTrue("Coprocessor should have been called on snapshot clone",
        cp.wasCloneSnapshotCalled());
      assertFalse("Coprocessor restore should not have been called on snapshot clone",
        cp.wasRestoreSnapshotCalled());
      admin.disableTable(TEST_CLONE);
      assertTrue(admin.isTableDisabled(tableName));
      deleteTable(admin, TEST_CLONE);

      // Test restore operation
      cp.resetStates();
      admin.restoreSnapshot(TEST_SNAPSHOT);
      assertTrue("Coprocessor should have been called on snapshot restore",
        cp.wasRestoreSnapshotCalled());
      assertFalse("Coprocessor clone should not have been called on snapshot restore",
        cp.wasCloneSnapshotCalled());

      admin.deleteSnapshot(TEST_SNAPSHOT);
      assertTrue("Coprocessor should have been called on snapshot delete",
        cp.wasDeleteSnapshotCalled());
    } finally {
      deleteTable(admin, tableName);
    }
  }

  @Test
  public void testNamespaceOperations() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    String testNamespace = "observed_ns";
    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);

    // create a table
    Admin admin = UTIL.getAdmin();

    admin.listNamespaces();
    assertTrue("preListNamespaces should have been called", cp.preListNamespacesCalled);
    assertTrue("postListNamespaces should have been called", cp.postListNamespacesCalled);

    admin.createNamespace(NamespaceDescriptor.create(testNamespace).build());
    assertTrue("Test namespace should be created", cp.wasCreateNamespaceCalled());

    assertNotNull(admin.getNamespaceDescriptor(testNamespace));
    assertTrue("Test namespace descriptor should have been called",
        cp.wasGetNamespaceDescriptorCalled());
    // This test used to do a bunch w/ bypass but bypass of these table and namespace stuff has
    // been removed so the testing code was removed.
  }

  private void modifyTableSync(Admin admin, TableName tableName, HTableDescriptor htd)
      throws IOException {
    admin.modifyTable(tableName, htd);
    //wait until modify table finishes
    for (int t = 0; t < 100; t++) { //10 sec timeout
      HTableDescriptor td = admin.getTableDescriptor(htd.getTableName());
      if (td.equals(htd)) {
        break;
      }
      Threads.sleep(100);
    }
  }

  @Test
  public void testRegionTransitionOperations() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    Table table = UTIL.createMultiRegionTable(tableName, TEST_FAMILY);

    try (RegionLocator r = UTIL.getConnection().getRegionLocator(tableName)) {
      UTIL.waitUntilAllRegionsAssigned(tableName);

      List<HRegionLocation> regions = r.getAllRegionLocations();
      HRegionLocation firstGoodPair = null;
      for (HRegionLocation e: regions) {
        if (e.getServerName() != null) {
          firstGoodPair = e;
          break;
        }
      }
      assertNotNull("Found a non-null entry", firstGoodPair);
      LOG.info("Found " + firstGoodPair.toString());
      // Try to force a move
      Collection<ServerName> servers = master.getClusterMetrics().getLiveServerMetrics().keySet();
      String destName = null;
      String serverNameForFirstRegion = firstGoodPair.getServerName().toString();
      LOG.info("serverNameForFirstRegion=" + serverNameForFirstRegion);
      ServerName masterServerName = master.getServerName();
      boolean found = false;
      // Find server that is NOT carrying the first region
      for (ServerName info : servers) {
        LOG.info("ServerName=" + info);
        if (!serverNameForFirstRegion.equals(info.getServerName())
            && !masterServerName.equals(info)) {
          destName = info.toString();
          found = true;
          break;
        }
      }
      assertTrue("Found server", found);
      LOG.info("Found " + destName);
      master.getMasterRpcServices().moveRegion(null, RequestConverter.buildMoveRegionRequest(
          firstGoodPair.getRegionInfo().getEncodedNameAsBytes(), ServerName.valueOf(destName)));
      assertTrue("Coprocessor should have been called on region move",
        cp.wasMoveCalled());

      // make sure balancer is on
      master.balanceSwitch(true);
      assertTrue("Coprocessor should have been called on balance switch",
          cp.wasBalanceSwitchCalled());

      // turn balancer off
      master.balanceSwitch(false);

      // wait for assignments to finish, if any
      UTIL.waitUntilNoRegionsInTransition();

      // move half the open regions from RS 0 to RS 1
      HRegionServer rs = cluster.getRegionServer(0);
      byte[] destRS = Bytes.toBytes(cluster.getRegionServer(1).getServerName().toString());
      //Make sure no regions are in transition now
      UTIL.waitUntilNoRegionsInTransition();
      List<RegionInfo> openRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      int moveCnt = openRegions.size()/2;
      for (int i = 0; i < moveCnt; i++) {
        RegionInfo info = openRegions.get(i);
        if (!info.isMetaRegion()) {
          master.getMasterRpcServices().moveRegion(null,
            RequestConverter.buildMoveRegionRequest(openRegions.get(i).getEncodedNameAsBytes(),
              ServerName.valueOf(Bytes.toString(destRS))));
        }
      }
      //Make sure no regions are in transition now
      UTIL.waitUntilNoRegionsInTransition();
      // now trigger a balance
      master.balanceSwitch(true);
      boolean balanceRun = master.balance();
      assertTrue("Coprocessor should be called on region rebalancing",
          cp.wasBalanceCalled());
    } finally {
      Admin admin = UTIL.getAdmin();
      admin.disableTable(tableName);
      deleteTable(admin, tableName);
    }
  }

  @Test
  public void testTableDescriptorsEnumeration() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    GetTableDescriptorsRequest req =
        RequestConverter.buildGetTableDescriptorsRequest((List<TableName>)null);
    master.getMasterRpcServices().getTableDescriptors(null, req);

    assertTrue("Coprocessor should be called on table descriptors request",
      cp.wasGetTableDescriptorsCalled());
  }

  @Test
  public void testTableNamesEnumeration() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    master.getMasterRpcServices().getTableNames(null,
        GetTableNamesRequest.newBuilder().build());
    assertTrue("Coprocessor should be called on table names request",
      cp.wasGetTableNamesCalled());
  }

  @Test
  public void testAbortProcedureOperation() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    master.abortProcedure(1, true);
    assertTrue(
      "Coprocessor should be called on abort procedure request",
      cp.wasAbortProcedureCalled());
  }

  @Test
  public void testGetProceduresOperation() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    master.getProcedures();
    assertTrue(
      "Coprocessor should be called on get procedures request",
      cp.wasGetProceduresCalled());
  }

  @Test
  public void testGetLocksOperation() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    CPMasterObserver cp = host.findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    master.getLocks();
    assertTrue(
      "Coprocessor should be called on get locks request",
      cp.wasGetLocksCalled());
  }

  private void deleteTable(Admin admin, TableName tableName) throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    tableDeletionLatch = new CountDownLatch(1);
    admin.deleteTable(tableName);
    tableDeletionLatch.await();
    tableDeletionLatch = new CountDownLatch(1);
  }

  @Test
  public void testQueueLockAndLockHeartbeatOperations() throws Exception {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    CPMasterObserver cp = master.getMasterCoprocessorHost().findCoprocessor(CPMasterObserver.class);
    cp.resetStates();

    final TableName tableName = TableName.valueOf("testLockedTable");
    long procId = master.getLockManager().remoteLocks().requestTableLock(tableName,
          LockType.EXCLUSIVE, "desc", null);
    master.getLockManager().remoteLocks().lockHeartbeat(procId, false);

    assertTrue(cp.preAndPostForQueueLockAndHeartbeatLockCalled());

    ProcedureTestingUtility.waitNoProcedureRunning(master.getMasterProcedureExecutor());
    ProcedureTestingUtility.assertProcNotFailed(master.getMasterProcedureExecutor(), procId);
  }
}
