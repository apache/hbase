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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DeleteColumnFamilyState;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * The procedure to delete a column family from an existing table.
 */
@InterfaceAudience.Private
public class DeleteColumnFamilyProcedure
    extends StateMachineProcedure<MasterProcedureEnv, DeleteColumnFamilyState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(DeleteColumnFamilyProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  private HTableDescriptor unmodifiedHTableDescriptor;
  private TableName tableName;
  private byte [] familyName;
  private boolean hasMob;
  private UserGroupInformation user;

  private List<HRegionInfo> regionInfoList;
  private Boolean traceEnabled;

  public DeleteColumnFamilyProcedure() {
    this.unmodifiedHTableDescriptor = null;
    this.regionInfoList = null;
    this.traceEnabled = null;
  }

  public DeleteColumnFamilyProcedure(
      final MasterProcedureEnv env,
      final TableName tableName,
      final byte[] familyName) throws IOException {
    this.tableName = tableName;
    this.familyName = familyName;
    this.user = env.getRequestUser().getUGI();
    this.setOwner(this.user.getShortUserName());
    this.unmodifiedHTableDescriptor = null;
    this.regionInfoList = null;
    this.traceEnabled = null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, DeleteColumnFamilyState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case DELETE_COLUMN_FAMILY_PREPARE:
        prepareDelete(env);
        setNextState(DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_PRE_OPERATION);
        break;
      case DELETE_COLUMN_FAMILY_PRE_OPERATION:
        preDelete(env, state);
        setNextState(DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR);
        break;
      case DELETE_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR:
        updateTableDescriptor(env);
        setNextState(DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_DELETE_FS_LAYOUT);
        break;
      case DELETE_COLUMN_FAMILY_DELETE_FS_LAYOUT:
        deleteFromFs(env);
        setNextState(DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_POST_OPERATION);
        break;
      case DELETE_COLUMN_FAMILY_POST_OPERATION:
        postDelete(env, state);
        setNextState(DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_REOPEN_ALL_REGIONS);
        break;
      case DELETE_COLUMN_FAMILY_REOPEN_ALL_REGIONS:
        reOpenAllRegionsIfTableIsOnline(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retry.
        LOG.warn("Error trying to delete the column family " + getColumnFamilyName()
          + " from table " + tableName + "(in state=" + state + ")", e);
      } else {
        LOG.error("Error trying to delete the column family " + getColumnFamilyName()
          + " from table " + tableName + "(in state=" + state + ")", e);
        setFailure("master-delete-column-family", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DeleteColumnFamilyState state)
      throws IOException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }
    try {
      switch (state) {
      case DELETE_COLUMN_FAMILY_REOPEN_ALL_REGIONS:
        break; // Nothing to undo.
      case DELETE_COLUMN_FAMILY_POST_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo?
        break;
      case DELETE_COLUMN_FAMILY_DELETE_FS_LAYOUT:
        // Once we reach to this state - we could NOT rollback - as it is tricky to undelete
        // the deleted files. We are not suppose to reach here, throw exception so that we know
        // there is a code bug to investigate.
        throw new UnsupportedOperationException(this + " rollback of state=" + state
            + " is unsupported.");
      case DELETE_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR:
        restoreTableDescriptor(env);
        break;
      case DELETE_COLUMN_FAMILY_PRE_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo?
        break;
      case DELETE_COLUMN_FAMILY_PREPARE:
        break; // nothing to do
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for deleting the column family"
          + getColumnFamilyName() + " to the table " + tableName, e);
      throw e;
    }
  }

  @Override
  protected DeleteColumnFamilyState getState(final int stateId) {
    return DeleteColumnFamilyState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final DeleteColumnFamilyState state) {
    return state.getNumber();
  }

  @Override
  protected DeleteColumnFamilyState getInitialState() {
    return DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_PREPARE;
  }

  @Override
  protected void setNextState(DeleteColumnFamilyState state) {
    if (aborted.get() && isRollbackSupported(state)) {
      setAbortFailure("delete-columnfamily", "abort requested");
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
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(this, tableName);
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableExclusiveLock(this, tableName);
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.DeleteColumnFamilyStateData.Builder deleteCFMsg =
        MasterProcedureProtos.DeleteColumnFamilyStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(user))
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setColumnfamilyName(ByteStringer.wrap(familyName));
    if (unmodifiedHTableDescriptor != null) {
      deleteCFMsg.setUnmodifiedTableSchema(unmodifiedHTableDescriptor.convert());
    }

    deleteCFMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);
    MasterProcedureProtos.DeleteColumnFamilyStateData deleteCFMsg =
        MasterProcedureProtos.DeleteColumnFamilyStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(deleteCFMsg.getUserInfo());
    tableName = ProtobufUtil.toTableName(deleteCFMsg.getTableName());
    familyName = deleteCFMsg.getColumnfamilyName().toByteArray();

    if (deleteCFMsg.hasUnmodifiedTableSchema()) {
      unmodifiedHTableDescriptor = HTableDescriptor.convert(deleteCFMsg.getUnmodifiedTableSchema());
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(tableName);
    sb.append(", columnfamily=");
    if (familyName != null) {
      sb.append(getColumnFamilyName());
    } else {
      sb.append("Unknown");
    }
    sb.append(")");
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  /**
   * Action before any real action of deleting column family.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareDelete(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table is allowed to be modified.
    MasterDDLOperationHelper.checkTableModifiable(env, tableName);

    // In order to update the descriptor, we need to retrieve the old descriptor for comparison.
    unmodifiedHTableDescriptor = env.getMasterServices().getTableDescriptors().get(tableName);
    if (unmodifiedHTableDescriptor == null) {
      throw new IOException("HTableDescriptor missing for " + tableName);
    }
    if (!unmodifiedHTableDescriptor.hasFamily(familyName)) {
      throw new InvalidFamilyOperationException("Family '" + getColumnFamilyName()
          + "' does not exist, so it cannot be deleted");
    }

    if (unmodifiedHTableDescriptor.getColumnFamilies().length == 1) {
      throw new InvalidFamilyOperationException("Family '" + getColumnFamilyName()
        + "' is the only column family in the table, so it cannot be deleted");
    }

    // whether mob family
    hasMob = unmodifiedHTableDescriptor.getFamily(familyName).isMobEnabled();
  }

  /**
   * Action before deleting column family.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void preDelete(final MasterProcedureEnv env, final DeleteColumnFamilyState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Remove the column family from the file system and update the table descriptor
   */
  private void updateTableDescriptor(final MasterProcedureEnv env) throws IOException {
    // Update table descriptor
    LOG.info("DeleteColumn. Table = " + tableName + " family = " + getColumnFamilyName());

    HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(tableName);

    if (!htd.hasFamily(familyName)) {
      // It is possible to reach this situation, as we could already delete the column family
      // from table descriptor, but the master failover happens before we complete this state.
      // We should be able to handle running this function multiple times without causing problem.
      return;
    }

    htd.removeFamily(familyName);
    env.getMasterServices().getTableDescriptors().add(htd);
  }

  /**
   * Restore back to the old descriptor
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void restoreTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().add(unmodifiedHTableDescriptor);

    // Make sure regions are opened after table descriptor is updated.
    reOpenAllRegionsIfTableIsOnline(env);
  }

  /**
   * Remove the column family from the file system
   **/
  private void deleteFromFs(final MasterProcedureEnv env) throws IOException {
    MasterDDLOperationHelper.deleteColumnFamilyFromFileSystem(env, tableName,
      getRegionInfoList(env), familyName, hasMob);
  }

  /**
   * Action after deleting column family.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void postDelete(final MasterProcedureEnv env, final DeleteColumnFamilyState state)
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
      LOG.info("Completed delete column family operation on table " + getTableName());
    } else {
      LOG.warn("Error on reopening the regions on table " + getTableName());
    }
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return traceEnabled
   */
  private Boolean isTraceEnabled() {
    if (traceEnabled == null) {
      traceEnabled = LOG.isTraceEnabled();
    }
    return traceEnabled;
  }

  private String getColumnFamilyName() {
    return Bytes.toString(familyName);
  }

  /**
   * Coprocessor Action.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void runCoprocessorAction(final MasterProcedureEnv env,
      final DeleteColumnFamilyState state) throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          switch (state) {
          case DELETE_COLUMN_FAMILY_PRE_OPERATION:
            cpHost.preDeleteColumnHandler(tableName, familyName);
            break;
          case DELETE_COLUMN_FAMILY_POST_OPERATION:
            cpHost.postDeleteColumnHandler(tableName, familyName);
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
  private boolean isRollbackSupported(final DeleteColumnFamilyState state) {
    switch (state) {
    case DELETE_COLUMN_FAMILY_REOPEN_ALL_REGIONS:
    case DELETE_COLUMN_FAMILY_POST_OPERATION:
    case DELETE_COLUMN_FAMILY_DELETE_FS_LAYOUT:
        // It is not safe to rollback if we reach to these states.
        return false;
      default:
        break;
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