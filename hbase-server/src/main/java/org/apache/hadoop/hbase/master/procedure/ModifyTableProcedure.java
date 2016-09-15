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
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.ModifyTableState;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

@InterfaceAudience.Private
public class ModifyTableProcedure
    extends AbstractStateMachineTableProcedure<ModifyTableState> {
  private static final Log LOG = LogFactory.getLog(ModifyTableProcedure.class);

  private HTableDescriptor unmodifiedHTableDescriptor = null;
  private HTableDescriptor modifiedHTableDescriptor;
  private boolean deleteColumnFamilyInModify;

  private List<HRegionInfo> regionInfoList;
  private Boolean traceEnabled = null;

  public ModifyTableProcedure() {
    super();
    initilize();
  }

  public ModifyTableProcedure(final MasterProcedureEnv env, final HTableDescriptor htd) {
    this(env, htd, null);
  }

  public ModifyTableProcedure(final MasterProcedureEnv env, final HTableDescriptor htd,
      final ProcedurePrepareLatch latch) {
    super(env, latch);
    initilize();
    this.modifiedHTableDescriptor = htd;
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
      if (isRollbackSupported(state)) {
        setFailure("master-modify-table", e);
      } else {
        LOG.warn("Retriable error trying to modify table=" + getTableName() +
          " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final ModifyTableState state)
      throws IOException {
    if (state == ModifyTableState.MODIFY_TABLE_PREPARE ||
        state == ModifyTableState.MODIFY_TABLE_PRE_OPERATION) {
      // nothing to rollback, pre-modify is just checks.
      // TODO: coprocessor rollback semantic is still undefined.
      return;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final ModifyTableState state) {
    switch (state) {
      case MODIFY_TABLE_PRE_OPERATION:
      case MODIFY_TABLE_PREPARE:
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
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.ModifyTableStateData.Builder modifyTableMsg =
        MasterProcedureProtos.ModifyTableStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
            .setModifiedTableSchema(ProtobufUtil.convertToTableSchema(modifiedHTableDescriptor))
            .setDeleteColumnFamilyInModify(deleteColumnFamilyInModify);

    if (unmodifiedHTableDescriptor != null) {
      modifyTableMsg
          .setUnmodifiedTableSchema(ProtobufUtil.convertToTableSchema(unmodifiedHTableDescriptor));
    }

    modifyTableMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.ModifyTableStateData modifyTableMsg =
        MasterProcedureProtos.ModifyTableStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(modifyTableMsg.getUserInfo()));
    modifiedHTableDescriptor = ProtobufUtil.convertToHTableDesc(modifyTableMsg.getModifiedTableSchema());
    deleteColumnFamilyInModify = modifyTableMsg.getDeleteColumnFamilyInModify();

    if (modifyTableMsg.hasUnmodifiedTableSchema()) {
      unmodifiedHTableDescriptor =
          ProtobufUtil.convertToHTableDesc(modifyTableMsg.getUnmodifiedTableSchema());
    }
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

    if (env.getMasterServices().getTableStateManager()
        .isTableState(getTableName(), TableState.State.ENABLED)) {
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
    if (!env.getMasterServices().getTableStateManager()
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
      switch (state) {
        case MODIFY_TABLE_PRE_OPERATION:
          cpHost.preModifyTableAction(getTableName(), modifiedHTableDescriptor, getUser());
          break;
        case MODIFY_TABLE_POST_OPERATION:
          cpHost.postCompletedModifyTableAction(getTableName(), modifiedHTableDescriptor,getUser());
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }

  private List<HRegionInfo> getRegionInfoList(final MasterProcedureEnv env) throws IOException {
    if (regionInfoList == null) {
      regionInfoList = ProcedureSyncWait.getRegionsFromMeta(env, getTableName());
    }
    return regionInfoList;
  }
}