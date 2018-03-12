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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyTableState;

@InterfaceAudience.Private
public class ModifyTableProcedure
    extends AbstractStateMachineTableProcedure<ModifyTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(ModifyTableProcedure.class);

  private TableDescriptor unmodifiedTableDescriptor = null;
  private TableDescriptor modifiedTableDescriptor;
  private boolean deleteColumnFamilyInModify;

  private List<RegionInfo> regionInfoList;
  private Boolean traceEnabled = null;

  public ModifyTableProcedure() {
    super();
    initilize();
  }

  public ModifyTableProcedure(final MasterProcedureEnv env, final TableDescriptor htd)
  throws HBaseIOException {
    this(env, htd, null);
  }

  public ModifyTableProcedure(final MasterProcedureEnv env, final TableDescriptor htd,
      final ProcedurePrepareLatch latch)
  throws HBaseIOException {
    super(env, latch);
    initilize();
    this.modifiedTableDescriptor = htd;
    preflightChecks(env, null/*No table checks; if changing peers, table can be online*/);
  }

  private void initilize() {
    this.unmodifiedTableDescriptor = null;
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
        updateReplicaColumnsIfNeeded(env, unmodifiedTableDescriptor, modifiedTableDescriptor);
        if (deleteColumnFamilyInModify) {
          setNextState(ModifyTableState.MODIFY_TABLE_DELETE_FS_LAYOUT);
        } else {
          setNextState(ModifyTableState.MODIFY_TABLE_POST_OPERATION);
        }
        break;
      case MODIFY_TABLE_DELETE_FS_LAYOUT:
        deleteFromFs(env, unmodifiedTableDescriptor, modifiedTableDescriptor);
        setNextState(ModifyTableState.MODIFY_TABLE_POST_OPERATION);
        break;
      case MODIFY_TABLE_POST_OPERATION:
        postModify(env, state);
        setNextState(ModifyTableState.MODIFY_TABLE_REOPEN_ALL_REGIONS);
        break;
      case MODIFY_TABLE_REOPEN_ALL_REGIONS:
        if (env.getAssignmentManager().isTableEnabled(getTableName())) {
          addChildProcedure(env.getAssignmentManager()
            .createReopenProcedures(getRegionInfoList(env)));
        }
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
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.ModifyTableStateData.Builder modifyTableMsg =
        MasterProcedureProtos.ModifyTableStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
            .setModifiedTableSchema(ProtobufUtil.toTableSchema(modifiedTableDescriptor))
            .setDeleteColumnFamilyInModify(deleteColumnFamilyInModify);

    if (unmodifiedTableDescriptor != null) {
      modifyTableMsg
          .setUnmodifiedTableSchema(ProtobufUtil.toTableSchema(unmodifiedTableDescriptor));
    }

    serializer.serialize(modifyTableMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.ModifyTableStateData modifyTableMsg =
        serializer.deserialize(MasterProcedureProtos.ModifyTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(modifyTableMsg.getUserInfo()));
    modifiedTableDescriptor = ProtobufUtil.toTableDescriptor(modifyTableMsg.getModifiedTableSchema());
    deleteColumnFamilyInModify = modifyTableMsg.getDeleteColumnFamilyInModify();

    if (modifyTableMsg.hasUnmodifiedTableSchema()) {
      unmodifiedTableDescriptor =
          ProtobufUtil.toTableDescriptor(modifyTableMsg.getUnmodifiedTableSchema());
    }
  }

  @Override
  public TableName getTableName() {
    return modifiedTableDescriptor.getTableName();
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
    if (modifiedTableDescriptor.getColumnFamilyCount() == 0) {
      throw new DoNotRetryIOException("Table " + getTableName().toString() +
        " should have at least one column family.");
    }

    // In order to update the descriptor, we need to retrieve the old descriptor for comparison.
    this.unmodifiedTableDescriptor =
        env.getMasterServices().getTableDescriptors().get(getTableName());

    if (env.getMasterServices().getTableStateManager()
        .isTableState(getTableName(), TableState.State.ENABLED)) {
      if (modifiedTableDescriptor.getRegionReplication() != unmodifiedTableDescriptor
          .getRegionReplication()) {
        throw new IOException("REGION_REPLICATION change is not supported for enabled tables");
      }
    }

    // Find out whether all column families in unmodifiedTableDescriptor also exists in
    // the modifiedTableDescriptor. This is to determine whether we are safe to rollback.
    final Set<byte[]> oldFamilies = unmodifiedTableDescriptor.getColumnFamilyNames();
    final Set<byte[]> newFamilies = modifiedTableDescriptor.getColumnFamilyNames();
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
    env.getMasterServices().getTableDescriptors().add(modifiedTableDescriptor);
  }

  /**
   * Undo the descriptor change (for rollback)
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void restoreTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().add(unmodifiedTableDescriptor);

    // delete any new column families from the modifiedTableDescriptor.
    deleteFromFs(env, modifiedTableDescriptor, unmodifiedTableDescriptor);

    // Make sure regions are opened after table descriptor is updated.
    //reOpenAllRegionsIfTableIsOnline(env);
    // TODO: NUKE ROLLBACK!!!!
  }

  /**
   * Removes from hdfs the families that are not longer present in the new table descriptor.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void deleteFromFs(final MasterProcedureEnv env,
      final TableDescriptor oldTableDescriptor, final TableDescriptor newTableDescriptor)
      throws IOException {
    final Set<byte[]> oldFamilies = oldTableDescriptor.getColumnFamilyNames();
    final Set<byte[]> newFamilies = newTableDescriptor.getColumnFamilyNames();
    for (byte[] familyName : oldFamilies) {
      if (!newFamilies.contains(familyName)) {
        MasterDDLOperationHelper.deleteColumnFamilyFromFileSystem(
          env,
          getTableName(),
          getRegionInfoList(env),
          familyName, oldTableDescriptor.getColumnFamily(familyName).isMobEnabled());
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
    final TableDescriptor oldTableDescriptor,
    final TableDescriptor newTableDescriptor) throws IOException {
    final int oldReplicaCount = oldTableDescriptor.getRegionReplication();
    final int newReplicaCount = newTableDescriptor.getRegionReplication();

    if (newReplicaCount < oldReplicaCount) {
      Set<byte[]> tableRows = new HashSet<>();
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
    if (newReplicaCount > oldReplicaCount) {
      Connection connection = env.getMasterServices().getConnection();
      // Get the existing table regions
      List<RegionInfo> existingTableRegions =
          MetaTableAccessor.getTableRegions(connection, getTableName());
      // add all the new entries to the meta table
      addRegionsToMeta(env, newTableDescriptor, existingTableRegions);
      if (oldReplicaCount <= 1) {
        // The table has been newly enabled for replica. So check if we need to setup
        // region replication
        ServerRegionReplicaUtil.setupRegionReplicaReplication(env.getMasterConfiguration());
      }
    }
  }

  private static void addRegionsToMeta(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, final List<RegionInfo> regionInfos)
      throws IOException {
    MetaTableAccessor.addRegionsToMeta(env.getMasterServices().getConnection(), regionInfos,
      tableDescriptor.getRegionReplication());
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
          cpHost.preModifyTableAction(getTableName(), modifiedTableDescriptor, getUser());
          break;
        case MODIFY_TABLE_POST_OPERATION:
          cpHost.postCompletedModifyTableAction(getTableName(), modifiedTableDescriptor,getUser());
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }

  private List<RegionInfo> getRegionInfoList(final MasterProcedureEnv env) throws IOException {
    if (regionInfoList == null) {
      regionInfoList = env.getAssignmentManager().getRegionStates()
          .getRegionsOfTable(getTableName());
    }
    return regionInfoList;
  }
}
