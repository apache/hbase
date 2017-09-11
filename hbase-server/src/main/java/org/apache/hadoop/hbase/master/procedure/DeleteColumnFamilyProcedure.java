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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DeleteColumnFamilyState;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The procedure to delete a column family from an existing table.
 */
@InterfaceAudience.Private
public class DeleteColumnFamilyProcedure
    extends AbstractStateMachineTableProcedure<DeleteColumnFamilyState> {
  private static final Log LOG = LogFactory.getLog(DeleteColumnFamilyProcedure.class);

  private TableDescriptor unmodifiedTableDescriptor;
  private TableName tableName;
  private byte [] familyName;
  private boolean hasMob;

  private List<HRegionInfo> regionInfoList;
  private Boolean traceEnabled;

  public DeleteColumnFamilyProcedure() {
    super();
    this.unmodifiedTableDescriptor = null;
    this.regionInfoList = null;
    this.traceEnabled = null;
  }

  public DeleteColumnFamilyProcedure(final MasterProcedureEnv env, final TableName tableName,
      final byte[] familyName) {
    this(env, tableName, familyName, null);
  }

  public DeleteColumnFamilyProcedure(final MasterProcedureEnv env, final TableName tableName,
      final byte[] familyName, final ProcedurePrepareLatch latch) {
    super(env, latch);
    this.tableName = tableName;
    this.familyName = familyName;
    this.unmodifiedTableDescriptor = null;
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
        if (env.getAssignmentManager().isTableEnabled(getTableName())) {
          addChildProcedure(env.getAssignmentManager()
            .createReopenProcedures(getRegionInfoList(env)));
        }
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-delete-columnfamily", e);
      } else {
        LOG.warn("Retriable error trying to delete the column family " + getColumnFamilyName() +
          " from table " + tableName + " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DeleteColumnFamilyState state)
      throws IOException {
    if (state == DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_PREPARE ||
        state == DeleteColumnFamilyState.DELETE_COLUMN_FAMILY_PRE_OPERATION) {
      // nothing to rollback, pre is just table-state checks.
      // We can fail if the table does not exist or is not disabled.
      // TODO: coprocessor rollback semantic is still undefined.
      return;
    }

    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final DeleteColumnFamilyState state) {
    switch (state) {
      case DELETE_COLUMN_FAMILY_PRE_OPERATION:
      case DELETE_COLUMN_FAMILY_PREPARE:
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
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.DeleteColumnFamilyStateData.Builder deleteCFMsg =
        MasterProcedureProtos.DeleteColumnFamilyStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setColumnfamilyName(UnsafeByteOperations.unsafeWrap(familyName));
    if (unmodifiedTableDescriptor != null) {
      deleteCFMsg
          .setUnmodifiedTableSchema(ProtobufUtil.toTableSchema(unmodifiedTableDescriptor));
    }

    serializer.serialize(deleteCFMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);
    MasterProcedureProtos.DeleteColumnFamilyStateData deleteCFMsg =
        serializer.deserialize(MasterProcedureProtos.DeleteColumnFamilyStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(deleteCFMsg.getUserInfo()));
    tableName = ProtobufUtil.toTableName(deleteCFMsg.getTableName());
    familyName = deleteCFMsg.getColumnfamilyName().toByteArray();

    if (deleteCFMsg.hasUnmodifiedTableSchema()) {
      unmodifiedTableDescriptor = ProtobufUtil.toTableDescriptor(deleteCFMsg.getUnmodifiedTableSchema());
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
    checkTableModifiable(env);

    // In order to update the descriptor, we need to retrieve the old descriptor for comparison.
    unmodifiedTableDescriptor = env.getMasterServices().getTableDescriptors().get(tableName);
    if (unmodifiedTableDescriptor == null) {
      throw new IOException("TableDescriptor missing for " + tableName);
    }
    if (!unmodifiedTableDescriptor.hasColumnFamily(familyName)) {
      throw new InvalidFamilyOperationException("Family '" + getColumnFamilyName()
          + "' does not exist, so it cannot be deleted");
    }

    if (unmodifiedTableDescriptor.getColumnFamilyCount() == 1) {
      throw new InvalidFamilyOperationException("Family '" + getColumnFamilyName()
        + "' is the only column family in the table, so it cannot be deleted");
    }

    // whether mob family
    hasMob = unmodifiedTableDescriptor.getColumnFamily(familyName).isMobEnabled();
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

    TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(tableName);

    if (!htd.hasColumnFamily(familyName)) {
      // It is possible to reach this situation, as we could already delete the column family
      // from table descriptor, but the master failover happens before we complete this state.
      // We should be able to handle running this function multiple times without causing problem.
      return;
    }

    env.getMasterServices().getTableDescriptors().add(
            TableDescriptorBuilder.newBuilder(htd).removeColumnFamily(familyName).build());
  }

  /**
   * Restore back to the old descriptor
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void restoreTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().add(unmodifiedTableDescriptor);

    // Make sure regions are opened after table descriptor is updated.
    //reOpenAllRegionsIfTableIsOnline(env);
    // TODO: NUKE ROLLBACK!!!!
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
      switch (state) {
        case DELETE_COLUMN_FAMILY_PRE_OPERATION:
          cpHost.preDeleteColumnFamilyAction(tableName, familyName, getUser());
          break;
        case DELETE_COLUMN_FAMILY_POST_OPERATION:
          cpHost.postCompletedDeleteColumnFamilyAction(tableName, familyName, getUser());
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }

  private List<HRegionInfo> getRegionInfoList(final MasterProcedureEnv env) throws IOException {
    if (regionInfoList == null) {
      regionInfoList = env.getAssignmentManager().getRegionStates()
          .getRegionsOfTable(getTableName());
    }
    return regionInfoList;
  }
}
