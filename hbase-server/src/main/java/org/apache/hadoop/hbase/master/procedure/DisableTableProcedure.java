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
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.replication.ReplicationBarrierFamilyFormat;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DisableTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ServerCrashState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@InterfaceAudience.Private
public class DisableTableProcedure extends AbstractStateMachineTableProcedure<DisableTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(DisableTableProcedure.class);

  private TableName tableName;
  private boolean skipTableStateCheck;

  private RetryCounter retryCounter;

  public DisableTableProcedure() {
    super();
  }

  /**
   * Constructor
   * @param env                 MasterProcedureEnv
   * @param tableName           the table to operate on
   * @param skipTableStateCheck whether to check table state
   */
  public DisableTableProcedure(final MasterProcedureEnv env, final TableName tableName,
    final boolean skipTableStateCheck) throws HBaseIOException {
    this(env, tableName, skipTableStateCheck, null);
  }

  /**
   * Constructor
   * @param env                 MasterProcedureEnv
   * @param tableName           the table to operate on
   * @param skipTableStateCheck whether to check table state
   */
  public DisableTableProcedure(final MasterProcedureEnv env, final TableName tableName,
    final boolean skipTableStateCheck, final ProcedurePrepareLatch syncLatch)
    throws HBaseIOException {
    super(env, syncLatch);
    this.tableName = tableName;
    preflightChecks(env, true);
    this.skipTableStateCheck = skipTableStateCheck;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final DisableTableState state)
    throws InterruptedException, ProcedureSuspendedException {
    LOG.trace("{} execute state={}", this, state);
    try {
      switch (state) {
        case DISABLE_TABLE_PREPARE:
          if (prepareDisable(env)) {
            setNextState(DisableTableState.DISABLE_TABLE_PRE_OPERATION);
          } else {
            assert isFailed() : "disable should have an exception here";
            return Flow.NO_MORE_STATE;
          }
          break;
        case DISABLE_TABLE_PRE_OPERATION:
          preDisable(env, state);
          setNextState(DisableTableState.DISABLE_TABLE_SET_DISABLING_TABLE_STATE);
          break;
        case DISABLE_TABLE_SET_DISABLING_TABLE_STATE:
          setTableStateToDisabling(env, tableName);
          setNextState(DisableTableState.DISABLE_TABLE_MARK_REGIONS_OFFLINE);
          break;
        case DISABLE_TABLE_MARK_REGIONS_OFFLINE:
          addChildProcedure(
            env.getAssignmentManager().createUnassignProceduresForDisabling(tableName));
          setNextState(DisableTableState.DISABLE_TABLE_ADD_REPLICATION_BARRIER);
          break;
        case DISABLE_TABLE_ADD_REPLICATION_BARRIER:
          if (
            env.getMasterServices().getTableDescriptors().get(tableName).hasGlobalReplicationScope()
          ) {
            MasterFileSystem fs = env.getMasterFileSystem();
            try (BufferedMutator mutator = env.getMasterServices().getConnection()
              .getBufferedMutator(TableName.META_TABLE_NAME)) {
              for (RegionInfo region : env.getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableName)) {
                long maxSequenceId = WALSplitUtil.getMaxRegionSequenceId(
                  env.getMasterConfiguration(), region, fs::getFileSystem, fs::getWALFileSystem);
                long openSeqNum = maxSequenceId > 0 ? maxSequenceId + 1 : HConstants.NO_SEQNUM;
                mutator.mutate(ReplicationBarrierFamilyFormat.makePutForReplicationBarrier(region,
                  openSeqNum, EnvironmentEdgeManager.currentTime()));
              }
            }
          }
          setNextState(DisableTableState.DISABLE_TABLE_SET_DISABLED_TABLE_STATE);
          break;
        case DISABLE_TABLE_SET_DISABLED_TABLE_STATE:
          setTableStateToDisabled(env, tableName);
          setNextState(DisableTableState.DISABLE_TABLE_POST_OPERATION);
          break;
        case DISABLE_TABLE_POST_OPERATION:
          postDisable(env, state);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("Unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-disable-table", e);
      } else {
        if (retryCounter == null) {
          retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
        }
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
        LOG.warn("Retryable error in {}, suspend {}secs", this, backoff / 1000, e);
        throw suspend(Math.toIntExact(backoff), true);
      }
    }
    retryCounter = null;
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DisableTableState state)
    throws IOException {
    // nothing to rollback, prepare-disable is just table-state checks.
    // We can fail if the table does not exist or is not disabled.
    switch (state) {
      case DISABLE_TABLE_PRE_OPERATION:
        return;
      case DISABLE_TABLE_PREPARE:
        releaseSyncLatch();
        return;
      default:
        break;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("Unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final DisableTableState state) {
    switch (state) {
      case DISABLE_TABLE_PREPARE:
      case DISABLE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DisableTableState getState(final int stateId) {
    return DisableTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final DisableTableState state) {
    return state.getNumber();
  }

  @Override
  protected DisableTableState getInitialState() {
    return DisableTableState.DISABLE_TABLE_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.DisableTableStateData.Builder disableTableMsg =
      MasterProcedureProtos.DisableTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName))
        .setSkipTableStateCheck(skipTableStateCheck);

    serializer.serialize(disableTableMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.DisableTableStateData disableTableMsg =
      serializer.deserialize(MasterProcedureProtos.DisableTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(disableTableMsg.getUserInfo()));
    tableName = ProtobufUtil.toTableName(disableTableMsg.getTableName());
    skipTableStateCheck = disableTableMsg.getSkipTableStateCheck();
  }

  // For disabling a table, we does not care whether a region can be online so hold the table xlock
  // for ever. This will simplify the logic as we will not be conflict with procedures other than
  // SCP.
  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    // We will test holdLock when releaseLock, if we are still at the first state, it means we fail
    // the SCP check and want to retry later, so we should release the lock
    return getCurrentState() != DisableTableState.DISABLE_TABLE_PREPARE;
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.DISABLE;
  }

  // check whether this SCP may schedule TRSP which conflits with this DTP, if so we need to sleep a
  // bit and retry again.
  private boolean mayHaveSkew(ServerCrashProcedure scp) {
    if (
      scp.getCurrentState() == ServerCrashState.SERVER_CRASH_ASSIGN
        && scp.getState() == ProcedureState.RUNNABLE
    ) {
      LOG.info("{} is assigning regions, which may conflict with {}", scp, this);
      return true;
    }
    if (
      (scp.getCurrentState() == ServerCrashState.SERVER_CRASH_CLAIM_REPLICATION_QUEUES
        || scp.getCurrentState() == ServerCrashState.SERVER_CRASH_FINISH)
        && scp.getState() == ProcedureState.WAITING
    ) {
      LOG.info("{} is waiting for assigning regions to finish, which may conflict with {}", scp,
        this);
      return true;
    }
    return false;
  }

  /**
   * Action before any real action of disabling table. Set the exception in the procedure instead of
   * throwing it. This approach is to deal with backward compatible with 1.0.
   * @param env MasterProcedureEnv
   */
  private boolean prepareDisable(final MasterProcedureEnv env)
    throws IOException, ProcedureSuspendedException {
    boolean canTableBeDisabled = true;
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      setFailure("master-disable-table",
        new ConstraintException("Cannot disable " + this.tableName));
      canTableBeDisabled = false;
    } else if (!env.getMasterServices().getTableDescriptors().exists(tableName)) {
      setFailure("master-disable-table", new TableNotFoundException(tableName));
      canTableBeDisabled = false;
    } else if (!skipTableStateCheck) {
      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      //
      // Note: in 1.0 release, we called TableStateManager.setTableStateIfInStates() to set
      // the state to DISABLING from ENABLED. The implementation was done before table lock
      // was implemented. With table lock, there is no need to set the state here (it will
      // set the state later on). A quick state check should be enough for us to move forward.
      TableStateManager tsm = env.getMasterServices().getTableStateManager();
      TableState ts = tsm.getTableState(tableName);
      if (!ts.isEnabled()) {
        LOG.info("Not ENABLED, state={}, skipping disable; {}", ts.getState(), this);
        setFailure("master-disable-table", new TableNotEnabledException(ts.toString()));
        canTableBeDisabled = false;
      }
    }
    if (!isFailed()) {
      // if we passed all the above check, then we need to check whether there are possible SCPs
      // which could introduce data race, see HBASE-28522 for more details
      long count = env.getMasterServices().getMasterProcedureExecutor().getProcedures().stream()
        .filter(p -> !p.isFailed()).filter(p -> p instanceof ServerCrashProcedure)
        .map(p -> (ServerCrashProcedure) p).filter(this::mayHaveSkew).count();
      if (count > 0) {
        if (retryCounter == null) {
          retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
        }
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
        LOG.info("There are {} SCPs may conflict with {}, sleep {}ms", count, this, backoff);
        throw suspend(Math.toIntExact(backoff), true);
      }
    }

    // We are done the check. Future actions in this procedure could be done asynchronously.
    releaseSyncLatch();

    return canTableBeDisabled;
  }

  /**
   * Action before disabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void preDisable(final MasterProcedureEnv env, final DisableTableState state)
    throws IOException, InterruptedException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preDisableTableAction(tableName, getUser());
    }
  }

  /**
   * Mark table state to Disabling
   * @param env MasterProcedureEnv
   */
  private static void setTableStateToDisabling(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Set table disabling flag up in zk.
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.DISABLING);
    LOG.info("Set {} to state={}", tableName, TableState.State.DISABLING);
  }

  /**
   * Mark table state to Disabled
   * @param env MasterProcedureEnv
   */
  protected static void setTableStateToDisabled(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Flip the table to disabled
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.DISABLED);
    LOG.info("Set {} to state={}", tableName, TableState.State.DISABLED);
  }

  /**
   * Action after disabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void postDisable(final MasterProcedureEnv env, final DisableTableState state)
    throws IOException, InterruptedException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postCompletedDisableTableAction(tableName, getUser());
    }
  }
}
