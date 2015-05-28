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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.ModifyColumnFamilyState;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * The procedure to modify a column family from an existing table.
 */
@InterfaceAudience.Private
public class ModifyColumnFamilyProcedure
    extends StateMachineProcedure<MasterProcedureEnv, ModifyColumnFamilyState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(ModifyColumnFamilyProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  private TableName tableName;
  private HTableDescriptor unmodifiedHTableDescriptor;
  private HColumnDescriptor cfDescriptor;
  private UserGroupInformation user;

  private Boolean traceEnabled;

  public ModifyColumnFamilyProcedure() {
    this.unmodifiedHTableDescriptor = null;
    this.traceEnabled = null;
  }

  public ModifyColumnFamilyProcedure(
      final MasterProcedureEnv env,
      final TableName tableName,
      final HColumnDescriptor cfDescriptor) throws IOException {
    this.tableName = tableName;
    this.cfDescriptor = cfDescriptor;
    this.user = env.getRequestUser().getUGI();
    this.unmodifiedHTableDescriptor = null;
    this.traceEnabled = null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env,
      final ModifyColumnFamilyState state) {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case MODIFY_COLUMN_FAMILY_PREPARE:
        prepareModify(env);
        setNextState(ModifyColumnFamilyState.MODIFY_COLUMN_FAMILY_PRE_OPERATION);
        break;
      case MODIFY_COLUMN_FAMILY_PRE_OPERATION:
        preModify(env, state);
        setNextState(ModifyColumnFamilyState.MODIFY_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR);
        break;
      case MODIFY_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR:
        updateTableDescriptor(env);
        setNextState(ModifyColumnFamilyState.MODIFY_COLUMN_FAMILY_POST_OPERATION);
        break;
      case MODIFY_COLUMN_FAMILY_POST_OPERATION:
        postModify(env, state);
        setNextState(ModifyColumnFamilyState.MODIFY_COLUMN_FAMILY_REOPEN_ALL_REGIONS);
        break;
      case MODIFY_COLUMN_FAMILY_REOPEN_ALL_REGIONS:
        reOpenAllRegionsIfTableIsOnline(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (InterruptedException|IOException e) {
      LOG.warn("Error trying to modify the column family " + getColumnFamilyName()
          + " of the table " + tableName + "(in state=" + state + ")", e);

      setFailure("master-modify-columnfamily", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final ModifyColumnFamilyState state)
      throws IOException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }
    try {
      switch (state) {
      case MODIFY_COLUMN_FAMILY_REOPEN_ALL_REGIONS:
        break; // Nothing to undo.
      case MODIFY_COLUMN_FAMILY_POST_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo?
        break;
      case MODIFY_COLUMN_FAMILY_UPDATE_TABLE_DESCRIPTOR:
        restoreTableDescriptor(env);
        break;
      case MODIFY_COLUMN_FAMILY_PRE_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo?
        break;
      case MODIFY_COLUMN_FAMILY_PREPARE:
        break; // nothing to do
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for adding the column family"
          + getColumnFamilyName() + " to the table " + tableName, e);
      throw e;
    }
  }

  @Override
  protected ModifyColumnFamilyState getState(final int stateId) {
    return ModifyColumnFamilyState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final ModifyColumnFamilyState state) {
    return state.getNumber();
  }

  @Override
  protected ModifyColumnFamilyState getInitialState() {
    return ModifyColumnFamilyState.MODIFY_COLUMN_FAMILY_PREPARE;
  }

  @Override
  protected void setNextState(ModifyColumnFamilyState state) {
    if (aborted.get()) {
      setAbortFailure("modify-columnfamily", "abort requested");
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
    if (!env.isInitialized()) return false;
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(
      tableName,
      EventType.C_M_MODIFY_FAMILY.toString());
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableExclusiveLock(tableName);
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.ModifyColumnFamilyStateData.Builder modifyCFMsg =
        MasterProcedureProtos.ModifyColumnFamilyStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(user))
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setColumnfamilySchema(cfDescriptor.convert());
    if (unmodifiedHTableDescriptor != null) {
      modifyCFMsg.setUnmodifiedTableSchema(unmodifiedHTableDescriptor.convert());
    }

    modifyCFMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.ModifyColumnFamilyStateData modifyCFMsg =
        MasterProcedureProtos.ModifyColumnFamilyStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(modifyCFMsg.getUserInfo());
    tableName = ProtobufUtil.toTableName(modifyCFMsg.getTableName());
    cfDescriptor = HColumnDescriptor.convert(modifyCFMsg.getColumnfamilySchema());
    if (modifyCFMsg.hasUnmodifiedTableSchema()) {
      unmodifiedHTableDescriptor = HTableDescriptor.convert(modifyCFMsg.getUnmodifiedTableSchema());
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(tableName);
    sb.append(", columnfamily=");
    if (cfDescriptor != null) {
      sb.append(getColumnFamilyName());
    } else {
      sb.append("Unknown");
    }
    sb.append(") user=");
    sb.append(user);
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
   * Action before any real action of modifying column family.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareModify(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table is allowed to be modified.
    MasterDDLOperationHelper.checkTableModifiable(env, tableName);

    unmodifiedHTableDescriptor = env.getMasterServices().getTableDescriptors().get(tableName);
    if (unmodifiedHTableDescriptor == null) {
      throw new IOException("HTableDescriptor missing for " + tableName);
    }
    if (!unmodifiedHTableDescriptor.hasFamily(cfDescriptor.getName())) {
      throw new InvalidFamilyOperationException("Family '" + getColumnFamilyName()
          + "' does not exist, so it cannot be modified");
    }
  }

  /**
   * Action before modifying column family.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void preModify(final MasterProcedureEnv env, final ModifyColumnFamilyState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Modify the column family from the file system
   */
  private void updateTableDescriptor(final MasterProcedureEnv env) throws IOException {
    // Update table descriptor
    LOG.info("ModifyColumnFamily. Table = " + tableName + " HCD = " + cfDescriptor.toString());

    HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(tableName);
    htd.modifyFamily(cfDescriptor);
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
   * Action after modifying column family.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void postModify(final MasterProcedureEnv env, final ModifyColumnFamilyState state)
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
        .isTableState(getTableName(), ZooKeeperProtos.Table.State.ENABLED)) {
      return;
    }

    List<HRegionInfo> regionInfoList = ProcedureSyncWait.getRegionsFromMeta(env, getTableName());
    if (MasterDDLOperationHelper.reOpenAllRegions(env, getTableName(), regionInfoList)) {
      LOG.info("Completed add column family operation on table " + getTableName());
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
    return cfDescriptor.getNameAsString();
  }

  /**
   * Coprocessor Action.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void runCoprocessorAction(final MasterProcedureEnv env,
      final ModifyColumnFamilyState state) throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          switch (state) {
          case MODIFY_COLUMN_FAMILY_PRE_OPERATION:
            cpHost.preModifyColumnHandler(tableName, cfDescriptor);
            break;
          case MODIFY_COLUMN_FAMILY_POST_OPERATION:
            cpHost.postModifyColumnHandler(tableName, cfDescriptor);
            break;
          default:
            throw new UnsupportedOperationException(this + " unhandled state=" + state);
          }
          return null;
        }
      });
    }
  }
}