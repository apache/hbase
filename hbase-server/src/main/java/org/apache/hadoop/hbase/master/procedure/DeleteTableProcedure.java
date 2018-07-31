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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DeleteTableState;

@InterfaceAudience.Private
public class DeleteTableProcedure
    extends AbstractStateMachineTableProcedure<DeleteTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteTableProcedure.class);

  private List<RegionInfo> regions;
  private TableName tableName;

  public DeleteTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public DeleteTableProcedure(final MasterProcedureEnv env, final TableName tableName) {
    this(env, tableName, null);
  }

  public DeleteTableProcedure(final MasterProcedureEnv env, final TableName tableName,
      final ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.tableName = tableName;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, DeleteTableState state)
      throws InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case DELETE_TABLE_PRE_OPERATION:
          // Verify if we can delete the table
          boolean deletable = prepareDelete(env);
          releaseSyncLatch();
          if (!deletable) {
            assert isFailed() : "the delete should have an exception here";
            return Flow.NO_MORE_STATE;
          }

          // TODO: Move out... in the acquireLock()
          LOG.debug("Waiting for RIT for {}", this);
          regions = env.getAssignmentManager().getRegionStates().getRegionsOfTable(getTableName());
          assert regions != null && !regions.isEmpty() : "unexpected 0 regions";
          ProcedureSyncWait.waitRegionInTransition(env, regions);

          // Call coprocessors
          preDelete(env);

          setNextState(DeleteTableState.DELETE_TABLE_REMOVE_FROM_META);
          break;
        case DELETE_TABLE_REMOVE_FROM_META:
          LOG.debug("Deleting regions from META for {}", this);
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), regions);
          setNextState(DeleteTableState.DELETE_TABLE_CLEAR_FS_LAYOUT);
          break;
        case DELETE_TABLE_CLEAR_FS_LAYOUT:
          LOG.debug("Deleting regions from filesystem for {}", this);
          DeleteTableProcedure.deleteFromFs(env, getTableName(), regions, true);
          setNextState(DeleteTableState.DELETE_TABLE_UPDATE_DESC_CACHE);
          regions = null;
          break;
        case DELETE_TABLE_UPDATE_DESC_CACHE:
          LOG.debug("Deleting descriptor for {}", this);
          DeleteTableProcedure.deleteTableDescriptorCache(env, getTableName());
          setNextState(DeleteTableState.DELETE_TABLE_UNASSIGN_REGIONS);
          break;
        case DELETE_TABLE_UNASSIGN_REGIONS:
          LOG.debug("Deleting assignment state for {}", this);
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          setNextState(DeleteTableState.DELETE_TABLE_POST_OPERATION);
          break;
        case DELETE_TABLE_POST_OPERATION:
          postDelete(env);
          LOG.debug("Finished {}", this);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-delete-table", e);
      } else {
        LOG.warn("Retriable error trying to delete table=" + getTableName() + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // TODO: Current behavior is: with no rollback and no abort support, procedure may get stuck
    // looping in retrying failing a step forever. Default behavior of abort is changed to support
    // aborting all procedures. Override the default wisely. Following code retains the current
    // behavior. Revisit it later.
    return isRollbackSupported(getCurrentState()) ? super.abort(env) : false;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DeleteTableState state) {
    if (state == DeleteTableState.DELETE_TABLE_PRE_OPERATION) {
      // nothing to rollback, pre-delete is just table-state checks.
      // We can fail if the table does not exist or is not disabled.
      // TODO: coprocessor rollback semantic is still undefined.
      releaseSyncLatch();
      return;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final DeleteTableState state) {
    switch (state) {
      case DELETE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DeleteTableState getState(final int stateId) {
    return DeleteTableState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final DeleteTableState state) {
    return state.getNumber();
  }

  @Override
  protected DeleteTableState getInitialState() {
    return DeleteTableState.DELETE_TABLE_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.DELETE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.DeleteTableStateData.Builder state =
      MasterProcedureProtos.DeleteTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName));
    if (regions != null) {
      for (RegionInfo hri: regions) {
        state.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
      }
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.DeleteTableStateData state =
        serializer.deserialize(MasterProcedureProtos.DeleteTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    tableName = ProtobufUtil.toTableName(state.getTableName());
    if (state.getRegionInfoCount() == 0) {
      regions = null;
    } else {
      regions = new ArrayList<>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: state.getRegionInfoList()) {
        regions.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
  }

  private boolean prepareDelete(final MasterProcedureEnv env) throws IOException {
    try {
      env.getMasterServices().checkTableModifiable(tableName);
    } catch (TableNotFoundException|TableNotDisabledException e) {
      setFailure("master-delete-table", e);
      return false;
    }
    return true;
  }

  private boolean preDelete(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = this.tableName;
      cpHost.preDeleteTableAction(tableName, getUser());
    }
    return true;
  }

  private void postDelete(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    deleteTableStates(env, tableName);

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = this.tableName;
      cpHost.postCompletedDeleteTableAction(tableName, getUser());
    }
  }

  protected static void deleteFromFs(final MasterProcedureEnv env,
      final TableName tableName, final List<RegionInfo> regions,
      final boolean archive) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final FileSystem fs = mfs.getFileSystem();
    final Path tempdir = mfs.getTempDir();

    final Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), tableName);
    final Path tempTableDir = FSUtils.getTableDir(tempdir, tableName);

    if (fs.exists(tableDir)) {
      // Ensure temp exists
      if (!fs.exists(tempdir) && !fs.mkdirs(tempdir)) {
        throw new IOException("HBase temp directory '" + tempdir + "' creation failure.");
      }

      // Ensure parent exists
      if (!fs.exists(tempTableDir.getParent()) && !fs.mkdirs(tempTableDir.getParent())) {
        throw new IOException("HBase temp directory '" + tempdir + "' creation failure.");
      }

      // Move the table in /hbase/.tmp
      if (!fs.rename(tableDir, tempTableDir)) {
        if (fs.exists(tempTableDir)) {
          // TODO
          // what's in this dir? something old? probably something manual from the user...
          // let's get rid of this stuff...
          FileStatus[] files = fs.listStatus(tempdir);
          if (files != null && files.length > 0) {
            for (int i = 0; i < files.length; ++i) {
              if (!files[i].isDir()) continue;
              HFileArchiver.archiveRegion(fs, mfs.getRootDir(), tempTableDir, files[i].getPath());
            }
          }
          fs.delete(tempdir, true);
        }
        throw new IOException("Unable to move '" + tableDir + "' to temp '" + tempTableDir + "'");
      }
    }

    // Archive regions from FS (temp directory)
    if (archive) {
      for (RegionInfo hri : regions) {
        LOG.debug("Archiving region " + hri.getRegionNameAsString() + " from FS");
        HFileArchiver.archiveRegion(fs, mfs.getRootDir(),
            tempTableDir, HRegion.getRegionDir(tempTableDir, hri.getEncodedName()));
      }
      LOG.debug("Table '" + tableName + "' archived!");
    }

    // Archive mob data
    Path mobTableDir = FSUtils.getTableDir(new Path(mfs.getRootDir(), MobConstants.MOB_DIR_NAME),
            tableName);
    Path regionDir =
            new Path(mobTableDir, MobUtils.getMobRegionInfo(tableName).getEncodedName());
    if (fs.exists(regionDir)) {
      HFileArchiver.archiveRegion(fs, mfs.getRootDir(), mobTableDir, regionDir);
    }

    // Delete table directory from FS (temp directory)
    if (!fs.delete(tempTableDir, true) && fs.exists(tempTableDir)) {
      throw new IOException("Couldn't delete " + tempTableDir);
    }

    // Delete the table directory where the mob files are saved
    if (mobTableDir != null && fs.exists(mobTableDir)) {
      if (!fs.delete(mobTableDir, true)) {
        throw new IOException("Couldn't delete mob dir " + mobTableDir);
      }
    }
  }

  /**
   * There may be items for this table still up in hbase:meta in the case where the
   * info:regioninfo column was empty because of some write error. Remove ALL rows from hbase:meta
   * that have to do with this table. See HBASE-12980.
   * @throws IOException
   */
  private static void cleanAnyRemainingRows(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    Connection connection = env.getMasterServices().getConnection();
    Scan tableScan = MetaTableAccessor.getScanForTableName(connection, tableName);
    try (Table metaTable = connection.getTable(TableName.META_TABLE_NAME)) {
      List<Delete> deletes = new ArrayList<>();
      try (ResultScanner resScanner = metaTable.getScanner(tableScan)) {
        for (Result result : resScanner) {
          deletes.add(new Delete(result.getRow()));
        }
      }
      if (!deletes.isEmpty()) {
        LOG.warn("Deleting some vestigial " + deletes.size() + " rows of " + tableName +
          " from " + TableName.META_TABLE_NAME);
        metaTable.delete(deletes);
      }
    }
  }

  protected static void deleteFromMeta(final MasterProcedureEnv env,
      final TableName tableName, List<RegionInfo> regions) throws IOException {
    MetaTableAccessor.deleteRegions(env.getMasterServices().getConnection(), regions);

    // Clean any remaining rows for this table.
    cleanAnyRemainingRows(env, tableName);

    // clean region references from the server manager
    env.getMasterServices().getServerManager().removeRegions(regions);

    // Clear Favored Nodes for this table
    FavoredNodesManager fnm = env.getMasterServices().getFavoredNodesManager();
    if (fnm != null) {
      fnm.deleteFavoredNodesForRegions(regions);
    }
  }

  protected static void deleteAssignmentState(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    // Clean up regions of the table in RegionStates.
    LOG.debug("Removing '" + tableName + "' from region states.");
    env.getMasterServices().getAssignmentManager().deleteTable(tableName);

    // If entry for this table states, remove it.
    LOG.debug("Marking '" + tableName + "' as deleted.");
    env.getMasterServices().getTableStateManager().setDeletedTable(tableName);
  }

  protected static void deleteTableDescriptorCache(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    LOG.debug("Removing '" + tableName + "' descriptor.");
    env.getMasterServices().getTableDescriptors().remove(tableName);
  }

  protected static void deleteTableStates(final MasterProcedureEnv env, final TableName tableName)
      throws IOException {
    if (!tableName.isSystemTable()) {
      ProcedureSyncWait.getMasterQuotaManager(env).removeTableFromNamespaceQuota(tableName);
    }
  }
}
