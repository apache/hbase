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
import java.util.List;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RestoreBackupSystemTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class RestoreBackupSystemTableProcedure
  extends AbstractStateMachineTableProcedure<RestoreBackupSystemTableState> {
  private static final Logger LOG =
    LoggerFactory.getLogger(RestoreBackupSystemTableProcedure.class);

  private final SnapshotDescription snapshot;
  private boolean enableOnRollback = false;

  // Necessary for the procedure framework. Do not remove.
  public RestoreBackupSystemTableProcedure() {
    this(null);
  }

  public RestoreBackupSystemTableProcedure(SnapshotDescription snapshot) {
    this.snapshot = snapshot;
  }

  @Override
  public TableName getTableName() {
    return TableName.valueOf(snapshot.getTable());
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.RESTORE_BACKUP_SYSTEM_TABLE;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RestoreBackupSystemTableState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.info("{} execute state={}", this, state);

    try {
      switch (state) {
        case RESTORE_BACKUP_SYSTEM_TABLE_PREPARE:
          prepare(env);
          return moreState(RestoreBackupSystemTableState.RESTORE_BACKUP_SYSTEM_TABLE_DISABLE);
        case RESTORE_BACKUP_SYSTEM_TABLE_DISABLE:
          TableState tableState =
            env.getMasterServices().getTableStateManager().getTableState(getTableName());
          if (tableState.isEnabled()) {
            addChildProcedure(createDisableTableProcedure(env));
          }
          return moreState(RestoreBackupSystemTableState.RESTORE_BACKUP_SYSTEM_TABLE_RESTORE);
        case RESTORE_BACKUP_SYSTEM_TABLE_RESTORE:
          addChildProcedure(createRestoreSnapshotProcedure(env));
          return moreState(RestoreBackupSystemTableState.RESTORE_BACKUP_SYSTEM_TABLE_ENABLE);
        case RESTORE_BACKUP_SYSTEM_TABLE_ENABLE:
          addChildProcedure(createEnableTableProcedure(env));
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (Exception e) {
      setFailure("restore-backup-system-table", e);
      LOG.warn("unexpected exception while execute {}. Mark procedure Failed.", this, e);
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RestoreBackupSystemTableState state)
    throws IOException, InterruptedException {
    switch (state) {
      case RESTORE_BACKUP_SYSTEM_TABLE_DISABLE, RESTORE_BACKUP_SYSTEM_TABLE_PREPARE:
        return;
      case RESTORE_BACKUP_SYSTEM_TABLE_RESTORE, RESTORE_BACKUP_SYSTEM_TABLE_ENABLE:
        if (enableOnRollback) {
          addChildProcedure(createEnableTableProcedure(env));
        }
        return;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected RestoreBackupSystemTableState getState(int stateId) {
    return RestoreBackupSystemTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RestoreBackupSystemTableState state) {
    return state.getNumber();
  }

  @Override
  protected RestoreBackupSystemTableState getInitialState() {
    return RestoreBackupSystemTableState.RESTORE_BACKUP_SYSTEM_TABLE_PREPARE;
  }

  private Flow moreState(RestoreBackupSystemTableState next) {
    setNextState(next);
    return Flow.HAS_MORE_STATE;
  }

  private Procedure<MasterProcedureEnv>[] createDisableTableProcedure(MasterProcedureEnv env)
    throws HBaseIOException {
    DisableTableProcedure disableTableProcedure =
      new DisableTableProcedure(env, getTableName(), true);
    return new DisableTableProcedure[] { disableTableProcedure };
  }

  private Procedure<MasterProcedureEnv>[] createEnableTableProcedure(MasterProcedureEnv env) {
    EnableTableProcedure enableTableProcedure = new EnableTableProcedure(env, getTableName());
    return new EnableTableProcedure[] { enableTableProcedure };
  }

  private Procedure<MasterProcedureEnv>[] createRestoreSnapshotProcedure(MasterProcedureEnv env)
    throws IOException {
    TableDescriptor desc = env.getMasterServices().getTableDescriptors().get(getTableName());
    RestoreSnapshotProcedure restoreSnapshotProcedure =
      new RestoreSnapshotProcedure(env, desc, snapshot);
    return new RestoreSnapshotProcedure[] { restoreSnapshotProcedure };
  }

  private void prepare(MasterProcedureEnv env) throws IOException {
    List<SnapshotDescription> snapshots =
      env.getMasterServices().getSnapshotManager().getCompletedSnapshots();
    boolean exists = snapshots.stream().anyMatch(s -> s.getName().equals(snapshot.getName()));
    if (!exists) {
      throw new SnapshotDoesNotExistException(ProtobufUtil.createSnapshotDesc(snapshot));
    }

    TableState tableState =
      env.getMasterServices().getTableStateManager().getTableState(getTableName());
    if (tableState.isEnabled()) {
      enableOnRollback = true;
    }
  }
}
