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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateTableState;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;

@InterfaceAudience.Private
public class TruncateTableProcedure
    extends AbstractStateMachineTableProcedure<TruncateTableState> {
  private static final Log LOG = LogFactory.getLog(TruncateTableProcedure.class);

  private boolean preserveSplits;
  private List<HRegionInfo> regions;
  private TableDescriptor tableDescriptor;
  private TableName tableName;

  public TruncateTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public TruncateTableProcedure(final MasterProcedureEnv env, final TableName tableName,
      boolean preserveSplits) {
    this(env, tableName, preserveSplits, null);
  }

  public TruncateTableProcedure(final MasterProcedureEnv env, final TableName tableName,
      boolean preserveSplits, ProcedurePrepareLatch latch) {
    super(env, latch);
    this.tableName = tableName;
    this.preserveSplits = preserveSplits;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, TruncateTableState state)
      throws InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case TRUNCATE_TABLE_PRE_OPERATION:
          // Verify if we can truncate the table
          if (!prepareTruncate(env)) {
            assert isFailed() : "the truncate should have an exception here";
            return Flow.NO_MORE_STATE;
          }

          // TODO: Move out... in the acquireLock()
          LOG.debug("waiting for '" + getTableName() + "' regions in transition");
          regions = env.getAssignmentManager().getRegionStates().getRegionsOfTable(getTableName());
          assert regions != null && !regions.isEmpty() : "unexpected 0 regions";
          ProcedureSyncWait.waitRegionInTransition(env, regions);

          // Call coprocessors
          preTruncate(env);

          setNextState(TruncateTableState.TRUNCATE_TABLE_REMOVE_FROM_META);
          break;
        case TRUNCATE_TABLE_REMOVE_FROM_META:
          tableDescriptor = env.getMasterServices().getTableDescriptors()
              .get(tableName);
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), regions);
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          setNextState(TruncateTableState.TRUNCATE_TABLE_CLEAR_FS_LAYOUT);
          break;
        case TRUNCATE_TABLE_CLEAR_FS_LAYOUT:
          DeleteTableProcedure.deleteFromFs(env, getTableName(), regions, true);
          if (!preserveSplits) {
            // if we are not preserving splits, generate a new single region
            regions = Arrays.asList(ModifyRegionUtils.createHRegionInfos(tableDescriptor, null));
          } else {
            regions = recreateRegionInfo(regions);
          }
          setNextState(TruncateTableState.TRUNCATE_TABLE_CREATE_FS_LAYOUT);
          break;
        case TRUNCATE_TABLE_CREATE_FS_LAYOUT:
          regions = CreateTableProcedure.createFsLayout(env, tableDescriptor, regions);
          CreateTableProcedure.updateTableDescCache(env, getTableName());
          setNextState(TruncateTableState.TRUNCATE_TABLE_ADD_TO_META);
          break;
        case TRUNCATE_TABLE_ADD_TO_META:
          regions = CreateTableProcedure.addTableToMeta(env, tableDescriptor, regions);
          setNextState(TruncateTableState.TRUNCATE_TABLE_ASSIGN_REGIONS);
          break;
        case TRUNCATE_TABLE_ASSIGN_REGIONS:
          CreateTableProcedure.setEnablingState(env, getTableName());
          addChildProcedure(env.getAssignmentManager().createAssignProcedures(regions));
          setNextState(TruncateTableState.TRUNCATE_TABLE_POST_OPERATION);
          tableDescriptor = null;
          regions = null;
          break;
        case TRUNCATE_TABLE_POST_OPERATION:
          CreateTableProcedure.setEnabledState(env, getTableName());
          postTruncate(env);
          LOG.debug("truncate '" + getTableName() + "' completed");
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (HBaseException|IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-truncate-table", e);
      } else {
        LOG.warn("Retriable error trying to truncate table=" + getTableName() + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final TruncateTableState state) {
    if (state == TruncateTableState.TRUNCATE_TABLE_PRE_OPERATION) {
      // nothing to rollback, pre-truncate is just table-state checks.
      // We can fail if the table does not exist or is not disabled.
      // TODO: coprocessor rollback semantic is still undefined.
      return;
    }

    // The truncate doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected void completionCleanup(final MasterProcedureEnv env) {
    releaseSyncLatch();
  }

  @Override
  protected boolean isRollbackSupported(final TruncateTableState state) {
    switch (state) {
      case TRUNCATE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected TruncateTableState getState(final int stateId) {
    return TruncateTableState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final TruncateTableState state) {
    return state.getNumber();
  }

  @Override
  protected TruncateTableState getInitialState() {
    return TruncateTableState.TRUNCATE_TABLE_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(" preserveSplits=");
    sb.append(preserveSplits);
    sb.append(")");
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.TruncateTableStateData.Builder state =
      MasterProcedureProtos.TruncateTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setPreserveSplits(preserveSplits);
    if (tableDescriptor != null) {
      state.setTableSchema(ProtobufUtil.toTableSchema(tableDescriptor));
    } else {
      state.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    if (regions != null) {
      for (HRegionInfo hri: regions) {
        state.addRegionInfo(HRegionInfo.convert(hri));
      }
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.TruncateTableStateData state =
        serializer.deserialize(MasterProcedureProtos.TruncateTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    if (state.hasTableSchema()) {
      tableDescriptor = ProtobufUtil.toTableDescriptor(state.getTableSchema());
      tableName = tableDescriptor.getTableName();
    } else {
      tableName = ProtobufUtil.toTableName(state.getTableName());
    }
    preserveSplits = state.getPreserveSplits();
    if (state.getRegionInfoCount() == 0) {
      regions = null;
    } else {
      regions = new ArrayList<>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: state.getRegionInfoList()) {
        regions.add(HRegionInfo.convert(hri));
      }
    }
  }

  private static List<HRegionInfo> recreateRegionInfo(final List<HRegionInfo> regions) {
    ArrayList<HRegionInfo> newRegions = new ArrayList<>(regions.size());
    for (HRegionInfo hri: regions) {
      newRegions.add(new HRegionInfo(hri.getTable(), hri.getStartKey(), hri.getEndKey()));
    }
    return newRegions;
  }

  private boolean prepareTruncate(final MasterProcedureEnv env) throws IOException {
    try {
      env.getMasterServices().checkTableModifiable(getTableName());
    } catch (TableNotFoundException|TableNotDisabledException e) {
      setFailure("master-truncate-table", e);
      return false;
    }
    return true;
  }

  private boolean preTruncate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = getTableName();
      cpHost.preTruncateTableAction(tableName, getUser());
    }
    return true;
  }

  private void postTruncate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = getTableName();
      cpHost.postCompletedTruncateTableAction(tableName, getUser());
    }
  }
}
