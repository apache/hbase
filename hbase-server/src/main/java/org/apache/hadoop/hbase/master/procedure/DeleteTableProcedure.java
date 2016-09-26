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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DeleteTableState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSUtils;

@InterfaceAudience.Private
public class DeleteTableProcedure
    extends AbstractStateMachineTableProcedure<DeleteTableState> {
  private static final Log LOG = LogFactory.getLog(DeleteTableProcedure.class);

  private List<HRegionInfo> regions;
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
          LOG.debug("waiting for '" + getTableName() + "' regions in transition");
          regions = ProcedureSyncWait.getRegionsFromMeta(env, getTableName());
          assert regions != null && !regions.isEmpty() : "unexpected 0 regions";
          ProcedureSyncWait.waitRegionInTransition(env, regions);

          // Call coprocessors
          preDelete(env);

          setNextState(DeleteTableState.DELETE_TABLE_REMOVE_FROM_META);
          break;
        case DELETE_TABLE_REMOVE_FROM_META:
          LOG.debug("delete '" + getTableName() + "' regions from META");
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), regions);
          setNextState(DeleteTableState.DELETE_TABLE_CLEAR_FS_LAYOUT);
          break;
        case DELETE_TABLE_CLEAR_FS_LAYOUT:
          LOG.debug("delete '" + getTableName() + "' from filesystem");
          DeleteTableProcedure.deleteFromFs(env, getTableName(), regions, true);
          setNextState(DeleteTableState.DELETE_TABLE_UPDATE_DESC_CACHE);
          regions = null;
          break;
        case DELETE_TABLE_UPDATE_DESC_CACHE:
          LOG.debug("delete '" + getTableName() + "' descriptor");
          DeleteTableProcedure.deleteTableDescriptorCache(env, getTableName());
          setNextState(DeleteTableState.DELETE_TABLE_UNASSIGN_REGIONS);
          break;
        case DELETE_TABLE_UNASSIGN_REGIONS:
          LOG.debug("delete '" + getTableName() + "' assignment state");
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          setNextState(DeleteTableState.DELETE_TABLE_POST_OPERATION);
          break;
        case DELETE_TABLE_POST_OPERATION:
          postDelete(env);
          LOG.debug("delete '" + getTableName() + "' completed");
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (HBaseException|IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-delete-table", e);
      } else {
        LOG.warn("Retriable error trying to delete table=" + getTableName() + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
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
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.DeleteTableStateData.Builder state =
      MasterProcedureProtos.DeleteTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName));
    if (regions != null) {
      for (HRegionInfo hri: regions) {
        state.addRegionInfo(HRegionInfo.convert(hri));
      }
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.DeleteTableStateData state =
      MasterProcedureProtos.DeleteTableStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    tableName = ProtobufUtil.toTableName(state.getTableName());
    if (state.getRegionInfoCount() == 0) {
      regions = null;
    } else {
      regions = new ArrayList<HRegionInfo>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: state.getRegionInfoList()) {
        regions.add(HRegionInfo.convert(hri));
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
      final TableName tableName, final List<HRegionInfo> regions,
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
      for (HRegionInfo hri : regions) {
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
    try (Table metaTable =
        connection.getTable(TableName.META_TABLE_NAME)) {
      List<Delete> deletes = new ArrayList<Delete>();
      try (ResultScanner resScanner = metaTable.getScanner(tableScan)) {
        for (Result result : resScanner) {
          deletes.add(new Delete(result.getRow()));
        }
      }
      if (!deletes.isEmpty()) {
        LOG.warn("Deleting some vestigal " + deletes.size() + " rows of " + tableName +
          " from " + TableName.META_TABLE_NAME);
        metaTable.delete(deletes);
      }
    }
  }

  protected static void deleteFromMeta(final MasterProcedureEnv env,
      final TableName tableName, List<HRegionInfo> regions) throws IOException {
    MetaTableAccessor.deleteRegions(env.getMasterServices().getConnection(), regions);

    // Clean any remaining rows for this table.
    cleanAnyRemainingRows(env, tableName);

    // clean region references from the server manager
    env.getMasterServices().getServerManager().removeRegions(regions);
  }

  protected static void deleteAssignmentState(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    final AssignmentManager am = env.getMasterServices().getAssignmentManager();

    // Clean up regions of the table in RegionStates.
    LOG.debug("Removing '" + tableName + "' from region states.");
    am.getRegionStates().tableDeleted(tableName);

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
