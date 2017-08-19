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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyColumnFamilyState;

/**
 * The procedure to modify a column family from an existing table.
 */
@InterfaceAudience.Private
public class ModifyColumnFamilyProcedure
    extends AbstractStateMachineTableProcedure<ModifyColumnFamilyState> {
  private static final Log LOG = LogFactory.getLog(ModifyColumnFamilyProcedure.class);

  private TableName tableName;
  private TableDescriptor unmodifiedtableDescriptor;
  private ColumnFamilyDescriptor cfDescriptor;

  private Boolean traceEnabled;

  public ModifyColumnFamilyProcedure() {
    super();
    this.unmodifiedtableDescriptor = null;
    this.traceEnabled = null;
  }

  public ModifyColumnFamilyProcedure(final MasterProcedureEnv env, final TableName tableName,
      final ColumnFamilyDescriptor cfDescriptor) {
    this(env, tableName, cfDescriptor, null);
  }

  public ModifyColumnFamilyProcedure(final MasterProcedureEnv env, final TableName tableName,
      final ColumnFamilyDescriptor cfDescriptor, final ProcedurePrepareLatch latch) {
    super(env, latch);
    this.tableName = tableName;
    this.cfDescriptor = cfDescriptor;
    this.unmodifiedtableDescriptor = null;
    this.traceEnabled = null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env,
      final ModifyColumnFamilyState state) throws InterruptedException {
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
        if (env.getAssignmentManager().isTableEnabled(getTableName())) {
          addChildProcedure(env.getAssignmentManager().createReopenProcedures(getTableName()));
        }
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-modify-columnfamily", e);
      } else {
        LOG.warn("Retriable error trying to disable table=" + tableName +
          " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final ModifyColumnFamilyState state)
      throws IOException {
    if (state == ModifyColumnFamilyState.MODIFY_COLUMN_FAMILY_PREPARE ||
        state == ModifyColumnFamilyState.MODIFY_COLUMN_FAMILY_PRE_OPERATION) {
      // nothing to rollback, pre-modify is just checks.
      // TODO: coprocessor rollback semantic is still undefined.
      return;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final ModifyColumnFamilyState state) {
    switch (state) {
      case MODIFY_COLUMN_FAMILY_PRE_OPERATION:
      case MODIFY_COLUMN_FAMILY_PREPARE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void completionCleanup(final MasterProcedureEnv env) {
    releaseSyncLatch();
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
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.ModifyColumnFamilyStateData.Builder modifyCFMsg =
        MasterProcedureProtos.ModifyColumnFamilyStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setColumnfamilySchema(ProtobufUtil.toColumnFamilySchema(cfDescriptor));
    if (unmodifiedtableDescriptor != null) {
      modifyCFMsg
          .setUnmodifiedTableSchema(ProtobufUtil.toTableSchema(unmodifiedtableDescriptor));
    }

    serializer.serialize(modifyCFMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.ModifyColumnFamilyStateData modifyCFMsg =
        serializer.deserialize(MasterProcedureProtos.ModifyColumnFamilyStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(modifyCFMsg.getUserInfo()));
    tableName = ProtobufUtil.toTableName(modifyCFMsg.getTableName());
    cfDescriptor = ProtobufUtil.toColumnFamilyDescriptor(modifyCFMsg.getColumnfamilySchema());
    if (modifyCFMsg.hasUnmodifiedTableSchema()) {
      unmodifiedtableDescriptor = ProtobufUtil.toTableDescriptor(modifyCFMsg.getUnmodifiedTableSchema());
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
   * Action before any real action of modifying column family.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareModify(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table is allowed to be modified.
    checkTableModifiable(env);

    unmodifiedtableDescriptor = env.getMasterServices().getTableDescriptors().get(tableName);
    if (unmodifiedtableDescriptor == null) {
      throw new IOException("TableDescriptor missing for " + tableName);
    }
    if (!unmodifiedtableDescriptor.hasColumnFamily(cfDescriptor.getName())) {
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

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(env.getMasterServices().getTableDescriptors().get(tableName));
    builder.modifyColumnFamily(cfDescriptor);
    env.getMasterServices().getTableDescriptors().add(builder.build());
  }

  /**
   * Restore back to the old descriptor
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void restoreTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().add(unmodifiedtableDescriptor);

    // Make sure regions are opened after table descriptor is updated.
    //reOpenAllRegionsIfTableIsOnline(env);
    // TODO: NUKE ROLLBACK!!!!
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
      switch (state) {
        case MODIFY_COLUMN_FAMILY_PRE_OPERATION:
          cpHost.preModifyColumnFamilyAction(tableName, cfDescriptor, getUser());
          break;
        case MODIFY_COLUMN_FAMILY_POST_OPERATION:
          cpHost.postCompletedModifyColumnFamilyAction(tableName, cfDescriptor, getUser());
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }
}
