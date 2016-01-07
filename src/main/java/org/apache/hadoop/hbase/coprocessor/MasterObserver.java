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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

import java.io.IOException;
import java.util.List;

/**
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 */
public interface MasterObserver extends Coprocessor {

  /**
   * Called before a new table is created by
   * {@link org.apache.hadoop.hbase.master.HMaster}.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   * @throws IOException
   */
  void preCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException;

  /**
   * Called after the createTable operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param desc the HTableDescriptor for the table
   * @param regions the initial regions created for the table
   * @throws IOException
   */
  void postCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException;

  /**
   * Called before {@link org.apache.hadoop.hbase.master.HMaster} deletes a
   * table
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  void preDeleteTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException;

  /**
   * Called after the deleteTable operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  void postDeleteTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException;

  /**
   * Called prior to modifying a table's properties.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   */
  void preModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] tableName, HTableDescriptor htd) throws IOException;

  /**
   * Called after the modifyTable operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param htd the HTableDescriptor
   */
  void postModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] tableName, HTableDescriptor htd) throws IOException;

  /**
   * Called prior to adding a new column family to the table.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param column the HColumnDescriptor
   */
  void preAddColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor column) throws IOException;

  /**
   * Called after the new column family has been created.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param column the HColumnDescriptor
   */
  void postAddColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor column) throws IOException;

  /**
   * Called prior to modifying a column family's attributes.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param descriptor the HColumnDescriptor
   */
  void preModifyColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte [] tableName, HColumnDescriptor descriptor) throws IOException;

  /**
   * Called after the column family has been updated.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param descriptor the HColumnDescriptor
   */
  void postModifyColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException;

  /**
   * Called prior to deleting the entire column family.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param c the column
   */
  void preDeleteColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte [] tableName, final byte[] c) throws IOException;

  /**
   * Called after the column family has been deleted.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   * @param c the column
   */
  void postDeleteColumn(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte [] tableName, final byte[] c) throws IOException;

  /**
   * Called prior to enabling a table.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  void preEnableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] tableName) throws IOException;

  /**
   * Called after the enableTable operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  void postEnableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] tableName) throws IOException;

  /**
   * Called prior to disabling a table.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  void preDisableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] tableName) throws IOException;

  /**
   * Called after the disableTable operation has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param tableName the name of the table
   */
  void postDisableTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final byte[] tableName) throws IOException;

  /**
   * Called prior to moving a given region from one region server to another.
   * @param ctx the environment to interact with the framework and master
   * @param region the HRegionInfo
   * @param srcServer the source ServerName
   * @param destServer the destination ServerName
   */
  void preMove(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer)
    throws IOException;

  /**
   * Called after the region move has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param region the HRegionInfo
   * @param srcServer the source ServerName
   * @param destServer the destination ServerName
   */
  void postMove(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer)
    throws IOException;

  /**
   * Called prior to assigning a specific region.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo the regionInfo of the region
   */
  void preAssign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo) throws IOException;
  
  /**
   * Called after the region assignment has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo the regionInfo of the region
   */
  void postAssign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo) throws IOException;
  
  /**
   * Called prior to unassigning a given region.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   * @param force whether to force unassignment or not
   */
  void preUnassign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo, final boolean force) throws IOException;

  /**
   * Called after the region unassignment has been requested.
   * @param ctx the environment to interact with the framework and master
   * @param regionInfo
   * @param force whether to force unassignment or not
   */
  void postUnassign(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final HRegionInfo regionInfo, final boolean force) throws IOException;

  /**
   * Called prior to requesting rebalancing of the cluster regions, though after
   * the initial checks for regions in transition and the balance switch flag.
   * @param ctx the environment to interact with the framework and master
   */
  void preBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException;

  /**
   * Called after the balancing plan has been submitted.
   * @param ctx the environment to interact with the framework and master
   */
  void postBalance(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException;

  /**
   * Called prior to modifying the flag used to enable/disable region balancing.
   * @param ctx the coprocessor instance's environment
   * @param newValue the new flag value submitted in the call
   */
  boolean preBalanceSwitch(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean newValue) throws IOException;

  /**
   * Called after the flag to enable/disable balancing has changed.
   * @param ctx the coprocessor instance's environment
   * @param oldValue the previously set balanceSwitch value
   * @param newValue the newly set balanceSwitch value
   */
  void postBalanceSwitch(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final boolean oldValue, final boolean newValue) throws IOException;

  /**
   * Called prior to shutting down the full HBase cluster, including this
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  void preShutdown(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException;


  /**
   * Called immediately prior to stopping this
   * {@link org.apache.hadoop.hbase.master.HMaster} process.
   */
  void preStopMaster(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException;

  /**
   * Called immediately after an active master instance has completed
   * initialization.  Will not be called on standby master instances unless
   * they take over the active role.
   */
  void postStartMaster(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException;

  /**
   * Called before a new snapshot is taken.
   * Called as part of snapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to snapshot
   * @throws IOException
   */
  void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException;

  /**
   * Called after the snapshot operation has been requested.
   * Called as part of snapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to snapshot
   * @throws IOException
   */
  void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException;

  /**
   * Called before a snapshot is cloned.
   * Called as part of restoreSnapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to create
   * @throws IOException
   */
  void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException;

  /**
   * Called after a snapshot clone operation has been requested.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to create
   * @throws IOException
   */
  void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException;

  /**
   * Called before a snapshot is restored.
   * Called as part of restoreSnapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to restore
   * @throws IOException
   */
  void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException;

  /**
   * Called after a snapshot restore operation has been requested.
   * Called as part of restoreSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor for the snapshot
   * @param hTableDescriptor the hTableDescriptor of the table to restore
   * @throws IOException
   */
  void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws IOException;

  /**
   * Called before a snapshot is deleted.
   * Called as part of deleteSnapshot RPC call.
   * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to delete
   * @throws IOException
   */
  void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException;

  /**
   * Called after the delete snapshot operation has been requested.
   * Called as part of deleteSnapshot RPC call.
   * @param ctx the environment to interact with the framework and master
   * @param snapshot the SnapshotDescriptor of the snapshot to delete
   * @throws IOException
   */
  void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot) throws IOException;

  /**
   * Called before a getTableDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param tableNamesList the list of table names, or null if querying for all
   * @param descriptors an empty list, can be filled with what to return if bypassing
   * @throws IOException
   */
  void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<String> tableNamesList, List<HTableDescriptor> descriptors) throws IOException;

  /**
   * Called after a getTableDescriptors request has been processed.
   * @param ctx the environment to interact with the framework and master
   * @param descriptors the list of descriptors about to be returned
   * @throws IOException
   */
  void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors) throws IOException;
}
