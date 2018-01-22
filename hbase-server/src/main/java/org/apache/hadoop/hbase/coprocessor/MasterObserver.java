/*
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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.GlobalQuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;


/**
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 * <br><br>
 *
 * Since most implementations will be interested in only a subset of hooks, this class uses
 * 'default' functions to avoid having to add unnecessary overrides. When the functions are
 * non-empty, it's simply to satisfy the compiler by returning value of expected (non-void) type.
 * It is done in a way that these default definitions act as no-op. So our suggestion to
 * implementation would be to not call these 'default' methods from overrides.
 * <br><br>
 *
 * <h3>Exception Handling</h3>
 * For all functions, exception handling is done as follows:
 * <ul>
 *   <li>Exceptions of type {@link IOException} are reported back to client.</li>
 *   <li>For any other kind of exception:
 *     <ul>
 *       <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then
 *         the server aborts.</li>
 *       <li>Otherwise, coprocessor is removed from the server and
 *         {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface MasterObserver {
  /**
   * Called before a new table is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.  Called as part of create
   * table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param desc the TableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void preCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {}

  /**
   * Called after the createTable operation has been requested.  Called as part
   * of create table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param desc the TableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void postCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {}

  /**
   * Called before a new table is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.  Called as part of create
   * table procedure and it is async to the create RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param desc the TableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void preCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableDescriptor desc,
      final RegionInfo[] regions) throws IOException {}

  /**
   * Called after the createTable operation has been requested.  Called as part
   * of create table RPC call.  Called as part of create table procedure and
   * it is async to the create RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param desc the TableDescriptor for the table
   * @param regions the initial regions created for the table
   */
  default void postCompletedCreateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableDescriptor desc,
      final RegionInfo[] regions) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDeleteTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called after the deleteTable operation has been requested.  Called as part
   * of delete table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postDeleteTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table procedure and
   * it is async to the delete RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table.  Called as part of delete table procedure and it is async to the
   * delete RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedDeleteTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called after the truncateTable operation has been requested.  Called as part
   * of truncate table RPC call.
   * The truncate is synchronous, so this method will be called when the
   * truncate operation is terminated.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postTruncateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table procedure and it is async
   * to the truncate RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called after {@link org.apache.hadoop.hbase.master.HMaster} truncates a
   * table.  Called as part of truncate table procedure and it is async to the
   * truncate RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedTruncateTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
      throws IOException {}

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param newDescriptor after modify operation, table will have this descriptor
   * @deprecated Since 2.1. Will be removed in 3.0.
   */
  @Deprecated
  default void preModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final TableName tableName, TableDescriptor newDescriptor) throws IOException {}

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param currentDescriptor current TableDescriptor of the table
   * @param newDescriptor after modify operation, table will have this descriptor
   */
  default void preModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    preModifyTable(ctx, tableName, newDescriptor);
  }
  /**
   * Called after the modifyTable operation has been requested.  Called as part
   * of modify table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param currentDescriptor current TableDescriptor of the table
   * @deprecated Since 2.1. Will be removed in 3.0.
   */
  @Deprecated
  default void postModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final TableName tableName, TableDescriptor currentDescriptor) throws IOException {}

  /**
   * Called after the modifyTable operation has been requested.  Called as part
   * of modify table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param oldDescriptor descriptor of table before modify operation happened
   * @param currentDescriptor current TableDescriptor of the table
   */
  default void postModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, TableDescriptor oldDescriptor, TableDescriptor currentDescriptor)
    throws IOException {
    postModifyTable(ctx, tableName, currentDescriptor);
  }

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table procedure and it is async to the modify table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param newDescriptor after modify operation, table will have this descriptor
   * @deprecated Since 2.1. Will be removed in 3.0.
   */
  @Deprecated
  default void preModifyTableAction(
    final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final TableName tableName,
    final TableDescriptor newDescriptor) throws IOException {}

  /**
   * Called prior to modifying a table's properties.  Called as part of modify
   * table procedure and it is async to the modify table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param currentDescriptor current TableDescriptor of the table
   * @param newDescriptor after modify operation, table will have this descriptor
   */
  default void preModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final TableDescriptor currentDescriptor,
      final TableDescriptor newDescriptor) throws IOException {
    preModifyTableAction(ctx, tableName, newDescriptor);
  }

  /**
   * Called after to modifying a table's properties.  Called as part of modify
   * table procedure and it is async to the modify table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param currentDescriptor current TableDescriptor of the table
   * @deprecated Since 2.1. Will be removed in 3.0.
   */
  @Deprecated
  default void postCompletedModifyTableAction(
    final ObserverContext<MasterCoprocessorEnvironment> ctx,
    final TableName tableName,
    final TableDescriptor currentDescriptor) throws IOException {}

  /**
   * Called after to modifying a table's properties.  Called as part of modify
   * table procedure and it is async to the modify table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param oldDescriptor descriptor of table before modify operation happened
   * @param currentDescriptor current TableDescriptor of the table
   */
  default void postCompletedModifyTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName,
      final TableDescriptor oldDescriptor,
      final TableDescriptor currentDescriptor) throws IOException {
    postCompletedModifyTableAction(ctx, tableName, currentDescriptor);
  }

  /**
   * Called prior to enabling a table.  Called as part of enable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preEnableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the enableTable operation has been requested.  Called as part
   * of enable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postEnableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to enabling a table.  Called as part of enable table procedure
   * and it is async to the enable table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preEnableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the enableTable operation has been requested.  Called as part
   * of enable table procedure and it is async to the enable table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedEnableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to disabling a table.  Called as part of disable table RPC
   * call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDisableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the disableTable operation has been requested.  Called as part
   * of disable table RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postDisableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called prior to disabling a table.  Called as part of disable table procedure
   * and it is asyn to the disable table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preDisableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the disableTable operation has been requested.  Called as part
   * of disable table procedure and it is asyn to the disable table RPC call.
   *
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postCompletedDisableTableAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called before a abortProcedure request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param procId the Id of the procedure
   */
  default void preAbortProcedure(
      ObserverContext<MasterCoprocessorEnvironment> ctx, final long procId) throws IOException {}

  /**
   * Called after a abortProcedure request has been processed.
   * @param ctx the environment to interact with the framework and master
   */
  default void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before a getProcedures request has been processed.
   * @param ctx the environment to interact with the framework and master
   */
  default void preGetProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after a getProcedures request has been processed.
   * @param ctx the environment to interact with the framework and master
   */
  default void postGetProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before a getLocks request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @throws IOException if something went wrong
   */
  default void preGetLocks(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after a getLocks request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @throws IOException if something went wrong
   */
  default void postGetLocks(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {}

  /**
   * Called prior to moving a given region from one region server to another.
   * @param ctx the environment to interact with the framework and master
   * @param region the RegionInfo
   * @param srcServer the source ServerName
   * @param destServer the destination ServerName
   */
  default void preMove(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo region, final ServerName srcServer,
      final ServerName destServer)
    throws IOException {}

  /**
   * Called after the region move has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param region the RegionInfo
   * @param srcServer the source ServerName
   * @param destServer the destination ServerName
   */
  default void postMove(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo region, final ServerName srcServer,
      final ServerName destServer)
    throws IOException {}

  /**
   * Called prior to assigning a specific region.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo the regionInfo of the region
   */
  default void preAssign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo regionInfo) throws IOException {}

  /**
   * Called after the region assignment has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo the regionInfo of the region
   */
  default void postAssign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo regionInfo) throws IOException {}

  /**
   * Called prior to unassigning a given region.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   * @param force whether to force unassignment or not
   */
  default void preUnassign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo regionInfo, final boolean force) throws IOException {}

  /**
   * Called after the region unassignment has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   * @param force whether to force unassignment or not
   */
  default void postUnassign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo regionInfo, final boolean force) throws IOException {}

  /**
   * Called prior to marking a given region as offline.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   */
  default void preRegionOffline(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo regionInfo) throws IOException {}

  /**
   * Called after the region has been marked offline.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   */
  default void postRegionOffline(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo regionInfo) throws IOException {}

  /**
   * Called prior to requesting rebalancing of the cluster regions, though after
   * the initial checks for regions in transition and the balance switch flag.
   * @param ctx the environment to interact with the framework and master
   */
  default void preBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after the balancing plan has been submitted.
   * @param ctx the environment to interact with the framework and master
   * @param plans the RegionPlans which master has executed. RegionPlan serves as hint
   * as for the final destination for the underlying region but may not represent the
   * final state of assignment
   */
  default void postBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
      throws IOException {}

  /**
   * Called prior to setting split / merge switch
   * Supports Coprocessor 'bypass'.
   * @param ctx the coprocessor instance's environment
   * @param newValue the new value submitted in the call
   * @param switchType type of switch
   */
  default void preSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {}

  /**
   * Called after setting split / merge switch
   * @param ctx the coprocessor instance's environment
   * @param newValue the new value submitted in the call
   * @param switchType type of switch
   */
  default void postSetSplitOrMergeEnabled(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue, final MasterSwitchType switchType) throws IOException {}

  /**
   * Called before the split region procedure is called.
   * @param c the environment to interact with the framework and master
   * @param tableName the table where the region belongs to
   * @param splitRow split point
   */
  default void preSplitRegion(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName,
      final byte[] splitRow)
      throws IOException {}

  /**
   * Called before the region is split.
   * @param c the environment to interact with the framework and master
   * @param tableName the table where the region belongs to
   * @param splitRow split point
   */
  default void preSplitRegionAction(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final TableName tableName,
      final byte[] splitRow)
      throws IOException {}

  /**
   * Called after the region is split.
   * @param c the environment to interact with the framework and master
   * @param regionInfoA the left daughter region
   * @param regionInfoB the right daughter region
   */
  default void postCompletedSplitRegionAction(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final RegionInfo regionInfoA,
      final RegionInfo regionInfoB) throws IOException {}

  /**
   * This will be called before update META step as part of split transaction.
   * @param ctx the environment to interact with the framework and master
   * @param splitKey
   * @param metaEntries
   */
  default void preSplitRegionBeforeMETAAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] splitKey,
      final List<Mutation> metaEntries) throws IOException {}


  /**
   * This will be called after update META step as part of split transaction
   * @param ctx the environment to interact with the framework and master
   */
  default void preSplitRegionAfterMETAAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * This will be called after the roll back of the split region is completed
   * @param ctx the environment to interact with the framework and master
   */
  default void postRollBackSplitRegionAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before the regions merge.
   * @param ctx the environment to interact with the framework and master
   */
  default void preMergeRegionsAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo[] regionsToMerge) throws IOException {}

  /**
   * called after the regions merge.
   * @param ctx the environment to interact with the framework and master
   */
  default void postCompletedMergeRegionsAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo[] regionsToMerge,
      final RegionInfo mergedRegion) throws IOException {}

  /**
   * This will be called before update META step as part of regions merge transaction.
   * @param ctx the environment to interact with the framework and master
   * @param metaEntries mutations to execute on hbase:meta atomically with regions merge updates.
   *        Any puts or deletes to execute on hbase:meta can be added to the mutations.
   */
  default void preMergeRegionsCommitAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo[] regionsToMerge,
      @MetaMutationAnnotation List<Mutation> metaEntries) throws IOException {}

  /**
   * This will be called after META step as part of regions merge transaction.
   * @param ctx the environment to interact with the framework and master
   */
  default void postMergeRegionsCommitAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo[] regionsToMerge,
      final RegionInfo mergedRegion) throws IOException {}

  /**
   * This will be called after the roll back of the regions merge.
   * @param ctx the environment to interact with the framework and master
   */
  default void postRollBackMergeRegionsAction(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo[] regionsToMerge) throws IOException {}

  /**
   * Called prior to modifying the flag used to enable/disable region balancing.
   * @param ctx the coprocessor instance's environment
   */
  default void preBalanceSwitch(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue) throws IOException {}

  /**
   * Called after the flag to enable/disable balancing has changed.
   * @param ctx the coprocessor instance's environment
   * @param oldValue the previously set balanceSwitch value
   * @param newValue the newly set balanceSwitch value
   */
  default void postBalanceSwitch(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean oldValue, final boolean newValue) throws IOException {}

  /**
   * Called prior to shutting down the full HBase cluster, including this
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  default void preShutdown(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}


  /**
   * Called immediately prior to stopping this
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  default void preStopMaster(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called immediately after an active master instance has completed
   * initialization.  Will not be called on standby master instances unless
   * they take over the active role.
   */
  default void postStartMaster(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Call before the master initialization is set to true.
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  default void preMasterInitialization(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before a new snapshot is taken.
   * Called as part of snapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param tableDescriptor the TableDescriptor of the table to snapshot
   */
  default void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor tableDescriptor)
      throws IOException {}

  /**
   * Called after the snapshot operation has been requested.
   * Called as part of snapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param tableDescriptor the TableDescriptor of the table to snapshot
   */
  default void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor tableDescriptor)
      throws IOException {}

  /**
   * Called before listSnapshots request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to list
   */
  default void preListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called after listSnapshots request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to list
   */
  default void postListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called before a snapshot is cloned.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param tableDescriptor the TableDescriptor of the table to create
   */
  default void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor tableDescriptor)
      throws IOException {}

  /**
   * Called after a snapshot clone operation has been requested.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param tableDescriptor the v of the table to create
   */
  default void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor tableDescriptor)
      throws IOException {}

  /**
   * Called before a snapshot is restored.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param tableDescriptor the TableDescriptor of the table to restore
   */
  default void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor tableDescriptor)
      throws IOException {}

  /**
   * Called after a snapshot restore operation has been requested.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param tableDescriptor the TableDescriptor of the table to restore
   */
  default void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor tableDescriptor)
      throws IOException {}

  /**
   * Called before a snapshot is deleted.
   * Called as part of deleteSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to delete
   */
  default void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called after the delete snapshot operation has been requested.
   * Called as part of deleteSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to delete
   */
  default void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException {}

  /**
   * Called before a getTableDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param tableNamesList the list of table names, or null if querying for all
   * @param descriptors an empty list, can be filled with what to return in coprocessor
   * @param regex regular expression used for filtering the table names
   */
  default void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<TableDescriptor> descriptors,
      String regex) throws IOException {}

  /**
   * Called after a getTableDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param tableNamesList the list of table names, or null if querying for all
   * @param descriptors the list of descriptors about to be returned
   * @param regex regular expression used for filtering the table names
   */
  default void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<TableDescriptor> descriptors,
      String regex) throws IOException {}

  /**
   * Called before a getTableNames request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors an empty list, can be filled with what to return by coprocessor
   * @param regex regular expression used for filtering the table names
   */
  default void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableDescriptor> descriptors, String regex) throws IOException {}

  /**
   * Called after a getTableNames request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors the list of descriptors about to be returned
   * @param regex regular expression used for filtering the table names
   */
  default void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableDescriptor> descriptors, String regex) throws IOException {}



  /**
   * Called before a new namespace is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor for the table
   */
  default void preCreateNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {}
  /**
   * Called after the createNamespace operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor for the table
   */
  default void postCreateNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
       NamespaceDescriptor ns) throws IOException {}

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * namespace
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   */
  default void preDeleteNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {}

  /**
   * Called after the deleteNamespace operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   */
  default void postDeleteNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {}

  /**
   * Called prior to modifying a namespace's properties.
   * @param ctx the environment to interact with the framework and master
   * @param newNsDescriptor after modify operation, namespace will have this descriptor
   * @deprecated Since 2.1. Will be removed in 3.0.
   */
  @Deprecated
  default void preModifyNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor newNsDescriptor) throws IOException {}

  /**
   * Called prior to modifying a namespace's properties.
   * @param ctx the environment to interact with the framework and master
   * @param currentNsDescriptor current NamespaceDescriptor of the namespace
   * @param newNsDescriptor after modify operation, namespace will have this descriptor
   */
  default void preModifyNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor currentNsDescriptor, NamespaceDescriptor newNsDescriptor)
    throws IOException {
    preModifyNamespace(ctx, newNsDescriptor);
  }

  /**
   * Called after the modifyNamespace operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param currentNsDescriptor current NamespaceDescriptor of the namespace
   * @deprecated Since 2.1. Will be removed in 3.0.
   */
  @Deprecated
  default void postModifyNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor currentNsDescriptor) throws IOException {}

  /**
   * Called after the modifyNamespace operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param oldNsDescriptor descriptor of namespace before modify operation happened
   * @param currentNsDescriptor current NamespaceDescriptor of the namespace
   */
  default void postModifyNamespace(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor oldNsDescriptor, NamespaceDescriptor currentNsDescriptor)
    throws IOException {
    postModifyNamespace(ctx, currentNsDescriptor);
  }

  /**
   * Called before a getNamespaceDescriptor request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   */
  default void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {}

  /**
   * Called after a getNamespaceDescriptor request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param ns the NamespaceDescriptor
   */
  default void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {}

  /**
   * Called before a listNamespaceDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors an empty list, can be filled with what to return by coprocessor
   */
  default void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {}

  /**
   * Called after a listNamespaceDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors the list of descriptors about to be returned
   */
  default void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<NamespaceDescriptor> descriptors) throws IOException {}


  /**
   * Called before the table memstore is flushed to disk.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void preTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after the table memstore is flushed to disk.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  default void postTableFlush(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called before the quota for the user is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param quotas the current quota for the user
   */
  default void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called after the quota for the user is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param quotas the resulting quota for the user
   */
  default void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String userName, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called before the quota for the user on the specified table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param tableName the name of the table
   * @param quotas the current quota for the user on the table
   */
  default void preSetUserQuota(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final String userName,
      final TableName tableName, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called after the quota for the user on the specified table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param tableName the name of the table
   * @param quotas the resulting quota for the user on the table
   */
  default void postSetUserQuota(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final String userName,
      final TableName tableName, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called before the quota for the user on the specified namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param namespace the name of the namespace
   * @param quotas the current quota for the user on the namespace
   */
  default void preSetUserQuota(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final String userName,
      final String namespace, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called after the quota for the user on the specified namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param userName the name of user
   * @param namespace the name of the namespace
   * @param quotas the resulting quota for the user on the namespace
   */
  default void postSetUserQuota(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, final String userName,
      final String namespace, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called before the quota for the table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param quotas the current quota for the table
   */
  default void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called after the quota for the table is stored.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param quotas the resulting quota for the table
   */
  default void postSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called before the quota for the namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   * @param quotas the current quota for the namespace
   */
  default void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called after the quota for the namespace is stored.
   * @param ctx the environment to interact with the framework and master
   * @param namespace the name of the namespace
   * @param quotas the resulting quota for the namespace
   */
  default void postSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String namespace, final GlobalQuotaSettings quotas) throws IOException {}

  /**
   * Called before merge regions request.
   * @param ctx coprocessor environment
   * @param regionsToMerge regions to be merged
   */
  default void preMergeRegions(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final RegionInfo[] regionsToMerge) throws IOException {}

  /**
   * called after merge regions request.
   * @param c coprocessor environment
   * @param regionsToMerge regions to be merged
   */
  default void postMergeRegions(
      final ObserverContext<MasterCoprocessorEnvironment> c,
      final RegionInfo[] regionsToMerge) throws IOException {}

  /**
   * Called before servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup destination group
   */
  default void preMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called after servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup name of group
   */
  default void postMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called before servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup destination group
   */
  default void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      Set<Address> servers, String targetGroup) throws IOException {}

  /**
   * Called after servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to move
   * @param targetGroup name of group
   */
  default void postMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                       Set<Address> servers, String targetGroup) throws IOException {}

  /**
   * Called before tables are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param tables set of tables to move
   * @param targetGroup name of group
   */
  default void preMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                     Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called after servers are moved to target region server group
   * @param ctx the environment to interact with the framework and master
   * @param tables set of tables to move
   * @param targetGroup name of group
   */
  default void postMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      Set<TableName> tables, String targetGroup) throws IOException {}

  /**
   * Called before a new region server group is added
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void preAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                     String name) throws IOException {}

  /**
   * Called after a new region server group is added
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void postAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                      String name) throws IOException {}

  /**
   * Called before a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void preRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                        String name) throws IOException {}

  /**
   * Called after a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param name group name
   */
  default void postRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                         String name) throws IOException {}

  /**
   * Called before a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param groupName group name
   */
  default void preBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                         String groupName) throws IOException {}

  /**
   * Called after a region server group is removed
   * @param ctx the environment to interact with the framework and master
   * @param groupName group name
   */
  default void postBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                          String groupName, boolean balancerRan) throws IOException {}

  /**
   * Called before servers are removed from rsgroup
   * @param ctx the environment to interact with the framework and master
   * @param servers set of decommissioned servers to remove
   */
  default void preRemoveServers(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {}

  /**
   * Called after servers are removed from rsgroup
   * @param ctx the environment to interact with the framework and master
   * @param servers set of servers to remove
   */
  default void postRemoveServers(
      final ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {}

  /**
   * Called before getting region server group info of the passed groupName.
   * @param ctx the environment to interact with the framework and master
   * @param groupName name of the group to get RSGroupInfo for
   */
  default void preGetRSGroupInfo(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String groupName) throws IOException {}

  /**
   * Called after getting region server group info of the passed groupName.
   * @param ctx the environment to interact with the framework and master
   * @param groupName name of the group to get RSGroupInfo for
   */
  default void postGetRSGroupInfo(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String groupName) throws IOException {}

  /**
   * Called before getting region server group info of the passed tableName.
   * @param ctx the environment to interact with the framework and master
   * @param tableName name of the table to get RSGroupInfo for
   */
  default void preGetRSGroupInfoOfTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called after getting region server group info of the passed tableName.
   * @param ctx the environment to interact with the framework and master
   * @param tableName name of the table to get RSGroupInfo for
   */
  default void postGetRSGroupInfoOfTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {}

  /**
   * Called before listing region server group information.
   * @param ctx the environment to interact with the framework and master
   */
  default void preListRSGroups(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after listing region server group information.
   * @param ctx the environment to interact with the framework and master
   */
  default void postListRSGroups(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before getting region server group info of the passed server.
   * @param ctx the environment to interact with the framework and master
   * @param server server to get RSGroupInfo for
   */
  default void preGetRSGroupInfoOfServer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final Address server) throws IOException {}

  /**
   * Called after getting region server group info of the passed server.
   * @param ctx the environment to interact with the framework and master
   * @param server server to get RSGroupInfo for
   */
  default void postGetRSGroupInfoOfServer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final Address server) throws IOException {}

  /**
   * Called before add a replication peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   */
  default void preAddReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId, ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called after add a replication peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   */
  default void postAddReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId, ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called before remove a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preRemoveReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after remove a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postRemoveReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called before enable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preEnableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after enable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postEnableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called before disable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preDisableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after disable a replication peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postDisableReplicationPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called before get the configured ReplicationPeerConfig for the specified peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preGetReplicationPeerConfig(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String peerId) throws IOException {}

  /**
   * Called after get the configured ReplicationPeerConfig for the specified peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void postGetReplicationPeerConfig(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId) throws IOException {}

  /**
   * Called before update peerConfig for the specified peer
   * @param ctx
   * @param peerId a short name that identifies the peer
   */
  default void preUpdateReplicationPeerConfig(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId,
      ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called after update peerConfig for the specified peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   */
  default void postUpdateReplicationPeerConfig(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId,
      ReplicationPeerConfig peerConfig) throws IOException {}

  /**
   * Called before list replication peers.
   * @param ctx the environment to interact with the framework and master
   * @param regex The regular expression to match peer id
   */
  default void preListReplicationPeers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String regex) throws IOException {}

  /**
   * Called after list replication peers.
   * @param ctx the environment to interact with the framework and master
   * @param regex The regular expression to match peer id
   */
  default void postListReplicationPeers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      String regex) throws IOException {}

  /**
   * Called before transit current cluster state for the specified synchronous replication peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   * @param state a new state
   */
  default void preTransitReplicationPeerSyncReplicationState(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId,
      SyncReplicationState state) throws IOException {
  }

  /**
   * Called after transit current cluster state for the specified synchronous replication peer
   * @param ctx the environment to interact with the framework and master
   * @param peerId a short name that identifies the peer
   * @param state a new state
   */
  default void postTransitReplicationPeerSyncReplicationState(
      final ObserverContext<MasterCoprocessorEnvironment> ctx, String peerId,
      SyncReplicationState state) throws IOException {
  }

  /**
   * Called before new LockProcedure is queued.
   * @param ctx the environment to interact with the framework and master
   */
  default void preRequestLock(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace,
      TableName tableName, RegionInfo[] regionInfos, String description) throws IOException {}

  /**
   * Called after new LockProcedure is queued.
   * @param ctx the environment to interact with the framework and master
   */
  default void postRequestLock(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace,
      TableName tableName, RegionInfo[] regionInfos, String description) throws IOException {}

  /**
   * Called before heartbeat to a lock.
   * @param ctx the environment to interact with the framework and master
   */
  default void preLockHeartbeat(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tn, String description) throws IOException {}

  /**
   * Called after heartbeat to a lock.
   * @param ctx the environment to interact with the framework and master
   */
  default void postLockHeartbeat(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before get cluster status.
   */
  default void preGetClusterMetrics(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after get cluster status.
   */
  default void postGetClusterMetrics(ObserverContext<MasterCoprocessorEnvironment> ctx,
    ClusterMetrics status) throws IOException {}

  /**
   * Called before clear dead region servers.
   */
  default void preClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after clear dead region servers.
   */
  default void postClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, List<ServerName> notClearedServers)
      throws IOException {}

  /**
   * Called before decommission region servers.
   */
  default void preDecommissionRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, boolean offload) throws IOException {}

  /**
   * Called after decommission region servers.
   */
  default void postDecommissionRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<ServerName> servers, boolean offload) throws IOException {}

  /**
   * Called before list decommissioned region servers.
   */
  default void preListDecommissionedRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called after list decommissioned region servers.
   */
  default void postListDecommissionedRegionServers(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {}

  /**
   * Called before recommission region server.
   */
  default void preRecommissionRegionServer(ObserverContext<MasterCoprocessorEnvironment> ctx,
      ServerName server, List<byte[]> encodedRegionNames) throws IOException {}

  /**
   * Called after recommission region server.
   */
  default void postRecommissionRegionServer(ObserverContext<MasterCoprocessorEnvironment> ctx,
      ServerName server, List<byte[]> encodedRegionNames) throws IOException {}
}
