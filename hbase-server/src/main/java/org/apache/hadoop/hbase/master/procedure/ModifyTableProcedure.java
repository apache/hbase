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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.ModifyTableState;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Private
public class ModifyTableProcedure
    extends StateMachineProcedure<MasterProcedureEnv, ModifyTableState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(ModifyTableProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  private HTableDescriptor unmodifiedHTableDescriptor = null;
  private HTableDescriptor modifiedHTableDescriptor;
  private UserGroupInformation user;
  private boolean deleteColumnFamilyInModify;

  private List<HRegionInfo> regionInfoList;
  private Boolean traceEnabled = null;

  public ModifyTableProcedure() {
    initilize();
  }

  public ModifyTableProcedure(
    final MasterProcedureEnv env,
    final HTableDescriptor htd) throws IOException {
    initilize();
    this.modifiedHTableDescriptor = htd;
    this.user = env.getRequestUser().getUGI();
    this.setOwner(this.user.getShortUserName());
  }

  private void initilize() {
    this.unmodifiedHTableDescriptor = null;
    this.regionInfoList = null;
    this.traceEnabled = null;
    this.deleteColumnFamilyInModify = false;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final ModifyTableState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case MODIFY_TABLE_PREPARE:
        prepareModify(env);
        setNextState(ModifyTableState.MODIFY_TABLE_PRE_OPERATION);
        break;
      case MODIFY_TABLE_PRE_OPERATION:
        preModify(env, state);
        setNextState(ModifyTableState.MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR);
        break;
      case MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR:
        updateTableDescriptor(env);
        setNextState(ModifyTableState.MODIFY_TABLE_REMOVE_REPLICA_COLUMN);
        break;
      case MODIFY_TABLE_REMOVE_REPLICA_COLUMN:
        updateReplicaColumnsIfNeeded(env, unmodifiedHTableDescriptor, modifiedHTableDescriptor);
        if (deleteColumnFamilyInModify) {
          setNextState(ModifyTableState.MODIFY_TABLE_DELETE_FS_LAYOUT);
        } else {
          setNextState(ModifyTableState.MODIFY_TABLE_POST_OPERATION);
        }
        break;
      case MODIFY_TABLE_DELETE_FS_LAYOUT:
        deleteFromFs(env, unmodifiedHTableDescriptor, modifiedHTableDescriptor);
        setNextState(ModifyTableState.MODIFY_TABLE_POST_OPERATION);
        break;
      case MODIFY_TABLE_POST_OPERATION:
        postModify(env, state);
        setNextState(ModifyTableState.MODIFY_TABLE_REOPEN_ALL_REGIONS);
        break;
      case MODIFY_TABLE_REOPEN_ALL_REGIONS:
        reOpenAllRegionsIfTableIsOnline(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retry.
        LOG.warn("Error trying to modify table=" + getTableName() + " state=" + state, e);
      } else {
        LOG.error("Error trying to modify table=" + getTableName() + " state=" + state, e);
        setFailure("master-modify-table", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final ModifyTableState state)
      throws IOException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }
    try {
      switch (state) {
      case MODIFY_TABLE_REOPEN_ALL_REGIONS:
        break; // Nothing to undo.
      case MODIFY_TABLE_POST_OPERATION:
        // TODO-MAYBE: call the coprocessor event to un-modify?
        break;
      case MODIFY_TABLE_DELETE_FS_LAYOUT:
        // Once we reach to this state - we could NOT rollback - as it is tricky to undelete
        // the deleted files. We are not suppose to reach here, throw exception so that we know
        // there is a code bug to investigate.
        assert deleteColumnFamilyInModify;
        throw new UnsupportedOperationException(this + " rollback of state=" + state
            + " is unsupported.");
      case MODIFY_TABLE_REMOVE_REPLICA_COLUMN:
        // Undo the replica column update.
        updateReplicaColumnsIfNeeded(env, modifiedHTableDescriptor, unmodifiedHTableDescriptor);
        break;
      case MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR:
        restoreTableDescriptor(env);
        break;
      case MODIFY_TABLE_PRE_OPERATION:
        // TODO-MAYBE: call the coprocessor event to un-modify?
        break;
      case MODIFY_TABLE_PREPARE:
        break; // Nothing to undo.
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.warn("Fail trying to rollback modify table=" + getTableName() + " state=" + state, e);
      throw e;
    }
  }

  @Override
  protected ModifyTableState getState(final int stateId) {
    return ModifyTableState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final ModifyTableState state) {
    return state.getNumber();
  }

  @Override
  protected ModifyTableState getInitialState() {
    return ModifyTableState.MODIFY_TABLE_PREPARE;
  }

  @Override
  protected void setNextState(final ModifyTableState state) {
    if (aborted.get() && isRollbackSupported(state)) {
      setAbortFailure("modify-table", "abort requested");
    } else {
      super.setNextState(state);
    }
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    aborted.set(true);
    return true;
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) return false;
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(this, getTableName());
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableExclusiveLock(this, getTableName());
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.ModifyTableStateData.Builder modifyTableMsg =
        MasterProcedureProtos.ModifyTableStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(user))
            .setModifiedTableSchema(modifiedHTableDescriptor.convert())
            .setDeleteColumnFamilyInModify(deleteColumnFamilyInModify);

    if (unmodifiedHTableDescriptor != null) {
      modifyTableMsg.setUnmodifiedTableSchema(unmodifiedHTableDescriptor.convert());
    }

    modifyTableMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.ModifyTableStateData modifyTableMsg =
        MasterProcedureProtos.ModifyTableStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(modifyTableMsg.getUserInfo());
    modifiedHTableDescriptor = HTableDescriptor.convert(modifyTableMsg.getModifiedTableSchema());
    deleteColumnFamilyInModify = modifyTableMsg.getDeleteColumnFamilyInModify();

    if (modifyTableMsg.hasUnmodifiedTableSchema()) {
      unmodifiedHTableDescriptor =
          HTableDescriptor.convert(modifyTableMsg.getUnmodifiedTableSchema());
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(")");
  }

  @Override
  public TableName getTableName() {
    return modifiedHTableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  /**
   * Check conditions before any real action of modifying a table.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareModify(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table exists
    if (!MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), getTableName())) {
      throw new TableNotFoundException(getTableName());
    }

    // check that we have at least 1 CF
    if (modifiedHTableDescriptor.getColumnFamilies().length == 0) {
      throw new DoNotRetryIOException("Table " + getTableName().toString() +
        " should have at least one column family.");
    }

    // In order to update the descriptor, we need to retrieve the old descriptor for comparison.
    this.unmodifiedHTableDescriptor =
        env.getMasterServices().getTableDescriptors().get(getTableName());

    if (env.getMasterServices().getAssignmentManager().getTableStateManager()
        .isTableState(getTableName(), TableState.State.ENABLED)) {
      // We only execute this procedure with table online if online schema change config is set.
      if (!MasterDDLOperationHelper.isOnlineSchemaChangeAllowed(env)) {
        throw new TableNotDisabledException(getTableName());
      }

      if (modifiedHTableDescriptor.getRegionReplication() != unmodifiedHTableDescriptor
          .getRegionReplication()) {
        throw new IOException("REGION_REPLICATION change is not supported for enabled tables");
      }
    }

    // Find out whether all column families in unmodifiedHTableDescriptor also exists in
    // the modifiedHTableDescriptor. This is to determine whether we are safe to rollback.
    final Set<byte[]> oldFamilies = unmodifiedHTableDescriptor.getFamiliesKeys();
    final Set<byte[]> newFamilies = modifiedHTableDescriptor.getFamiliesKeys();
    for (byte[] familyName : oldFamilies) {
      if (!newFamilies.contains(familyName)) {
        this.deleteColumnFamilyInModify = true;
        break;
      }
    }
  }

  /**
   * Action before modifying table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void preModify(final MasterProcedureEnv env, final ModifyTableState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Update descriptor
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void updateTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().add(modifiedHTableDescriptor);
  }

  /**
   * Undo the descriptor change (for rollback)
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void restoreTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().add(unmodifiedHTableDescriptor);

    // delete any new column families from the modifiedHTableDescriptor.
    deleteFromFs(env, modifiedHTableDescriptor, unmodifiedHTableDescriptor);

    // Make sure regions are opened after table descriptor is updated.
    reOpenAllRegionsIfTableIsOnline(env);
  }

  /**
   * Removes from hdfs the families that are not longer present in the new table descriptor.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void deleteFromFs(final MasterProcedureEnv env,
      final HTableDescriptor oldHTableDescriptor, final HTableDescriptor newHTableDescriptor)
      throws IOException {
    final Set<byte[]> oldFamilies = oldHTableDescriptor.getFamiliesKeys();
    final Set<byte[]> newFamilies = newHTableDescriptor.getFamiliesKeys();
    for (byte[] familyName : oldFamilies) {
      if (!newFamilies.contains(familyName)) {
        MasterDDLOperationHelper.deleteColumnFamilyFromFileSystem(
          env,
          getTableName(),
          getRegionInfoList(env),
          familyName,
          oldHTableDescriptor.getFamily(familyName).isMobEnabled());
      }
    }
  }

  /**
   * update replica column families if necessary.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void updateReplicaColumnsIfNeeded(
    final MasterProcedureEnv env,
    final HTableDescriptor oldHTableDescriptor,
    final HTableDescriptor newHTableDescriptor) throws IOException {
    final int oldReplicaCount = oldHTableDescriptor.getRegionReplication();
    final int newReplicaCount = newHTableDescriptor.getRegionReplication();

    if (newReplicaCount < oldReplicaCount) {
      Set<byte[]> tableRows = new HashSet<byte[]>();
      Connection connection = env.getMasterServices().getConnection();
      Scan scan = MetaTableAccessor.getScanForTableName(connection, getTableName());
      scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);

      try (Table metaTable = connection.getTable(TableName.META_TABLE_NAME)) {
        ResultScanner resScanner = metaTable.getScanner(scan);
        for (Result result : resScanner) {
          tableRows.add(result.getRow());
        }
        MetaTableAccessor.removeRegionReplicasFromMeta(
          tableRows,
          newReplicaCount,
          oldReplicaCount - newReplicaCount,
          connection);
      }
    }

    // Setup replication for region replicas if needed
    if (newReplicaCount > 1 && oldReplicaCount <= 1) {
      ServerRegionReplicaUtil.setupRegionReplicaReplication(env.getMasterConfiguration());
    }
  }

  /**
   * Action after modifying table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void postModify(final MasterProcedureEnv env, final ModifyTableState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Last action from the procedure - executed when online schema change is supported.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void reOpenAllRegionsIfTableIsOnline(final MasterProcedureEnv env) throws IOException {
    // This operation only run when the table is enabled.
    if (!env.getMasterServices().getAssignmentManager().getTableStateManager()
        .isTableState(getTableName(), TableState.State.ENABLED)) {
      return;
    }

    if (MasterDDLOperationHelper.reOpenAllRegions(env, getTableName(), getRegionInfoList(env))) {
      LOG.info("Completed modify table operation on table " + getTableName());
    } else {
      LOG.warn("Error on reopening the regions on table " + getTableName());
    }
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return traceEnabled whether the trace is enabled
   */
  private Boolean isTraceEnabled() {
    if (traceEnabled == null) {
      traceEnabled = LOG.isTraceEnabled();
    }
    return traceEnabled;
  }

  /**
   * Coprocessor Action.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final ModifyTableState state)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          switch (state) {
          case MODIFY_TABLE_PRE_OPERATION:
            cpHost.preModifyTableHandler(getTableName(), modifiedHTableDescriptor);
            break;
          case MODIFY_TABLE_POST_OPERATION:
            cpHost.postModifyTableHandler(getTableName(), modifiedHTableDescriptor);
            break;
          default:
            throw new UnsupportedOperationException(this + " unhandled state=" + state);
          }
          return null;
        }
      });
    }
  }

  /*
   * Check whether we are in the state that can be rollback
   */
  private boolean isRollbackSupported(final ModifyTableState state) {
    if (deleteColumnFamilyInModify) {
      switch (state) {
      case MODIFY_TABLE_DELETE_FS_LAYOUT:
      case MODIFY_TABLE_POST_OPERATION:
      case MODIFY_TABLE_REOPEN_ALL_REGIONS:
        // It is not safe to rollback if we reach to these states.
        return false;
      default:
        break;
      }
    }
    return true;
  }

  private List<HRegionInfo> getRegionInfoList(final MasterProcedureEnv env) throws IOException {
    if (regionInfoList == null) {
      regionInfoList = ProcedureSyncWait.getRegionsFromMeta(env, getTableName());
    }
    return regionInfoList;
  }
}