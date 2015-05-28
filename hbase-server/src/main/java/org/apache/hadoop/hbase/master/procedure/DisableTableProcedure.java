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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkAssigner;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DisableTableState;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.htrace.Trace;

@InterfaceAudience.Private
public class DisableTableProcedure
    extends StateMachineProcedure<MasterProcedureEnv, DisableTableState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(DisableTableProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  // This is for back compatible with 1.0 asynchronized operations.
  private final ProcedurePrepareLatch syncLatch;

  private TableName tableName;
  private boolean skipTableStateCheck;
  private UserGroupInformation user;

  private Boolean traceEnabled = null;

  enum MarkRegionOfflineOpResult {
    MARK_ALL_REGIONS_OFFLINE_SUCCESSFUL,
    BULK_ASSIGN_REGIONS_FAILED,
    MARK_ALL_REGIONS_OFFLINE_INTERRUPTED,
  }

  public DisableTableProcedure() {
    syncLatch = null;
  }

  /**
   * Constructor
   * @param env MasterProcedureEnv
   * @param tableName the table to operate on
   * @param skipTableStateCheck whether to check table state
   * @throws IOException
   */
  public DisableTableProcedure(
      final MasterProcedureEnv env,
      final TableName tableName,
      final boolean skipTableStateCheck) throws IOException {
    this(env, tableName, skipTableStateCheck, null);
  }

  /**
   * Constructor
   * @param env MasterProcedureEnv
   * @param tableName the table to operate on
   * @param skipTableStateCheck whether to check table state
   * @throws IOException
   */
  public DisableTableProcedure(
      final MasterProcedureEnv env,
      final TableName tableName,
      final boolean skipTableStateCheck,
      final ProcedurePrepareLatch syncLatch) throws IOException {
    this.tableName = tableName;
    this.skipTableStateCheck = skipTableStateCheck;
    this.user = env.getRequestUser().getUGI();

    // Compatible with 1.0: We use latch to make sure that this procedure implementation is
    // compatible with 1.0 asynchronized operations. We need to lock the table and check
    // whether the Disable operation could be performed (table exists and online; table state
    // is ENABLED). Once it is done, we are good to release the latch and the client can
    // start asynchronously wait for the operation.
    //
    // Note: the member syncLatch could be null if we are in failover or recovery scenario.
    // This is ok for backward compatible, as 1.0 client would not able to peek at procedure.
    this.syncLatch = syncLatch;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final DisableTableState state) {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

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
        if (markRegionsOffline(env, tableName, true) ==
            MarkRegionOfflineOpResult.MARK_ALL_REGIONS_OFFLINE_SUCCESSFUL) {
          setNextState(DisableTableState.DISABLE_TABLE_SET_DISABLED_TABLE_STATE);
        } else {
          LOG.trace("Retrying later to disable the missing regions");
        }
        break;
      case DISABLE_TABLE_SET_DISABLED_TABLE_STATE:
        setTableStateToDisabled(env, tableName);
        setNextState(DisableTableState.DISABLE_TABLE_POST_OPERATION);
        break;
      case DISABLE_TABLE_POST_OPERATION:
        postDisable(env, state);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (InterruptedException|HBaseException|IOException e) {
      LOG.warn("Retriable error trying to disable table=" + tableName + " state=" + state, e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DisableTableState state)
      throws IOException {
    if (state == DisableTableState.DISABLE_TABLE_PREPARE) {
      undoTableStateChange(env);
      ProcedurePrepareLatch.releaseLatch(syncLatch, this);
      return;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected DisableTableState getState(final int stateId) {
    return DisableTableState.valueOf(stateId);
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
  protected void setNextState(final DisableTableState state) {
    if (aborted.get()) {
      setAbortFailure("disable-table", "abort requested");
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
      EventType.C_M_DISABLE_TABLE.toString());
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableExclusiveLock(tableName);
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.DisableTableStateData.Builder disableTableMsg =
        MasterProcedureProtos.DisableTableStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(user))
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setSkipTableStateCheck(skipTableStateCheck);

    disableTableMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.DisableTableStateData disableTableMsg =
        MasterProcedureProtos.DisableTableStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(disableTableMsg.getUserInfo());
    tableName = ProtobufUtil.toTableName(disableTableMsg.getTableName());
    skipTableStateCheck = disableTableMsg.getSkipTableStateCheck();
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(tableName);
    sb.append(") user=");
    sb.append(user);
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.DISABLE;
  }

  /**
   * Action before any real action of disabling table. Set the exception in the procedure instead
   * of throwing it.  This approach is to deal with backward compatible with 1.0.
   * @param env MasterProcedureEnv
   * @throws HBaseException
   * @throws IOException
   */
  private boolean prepareDisable(final MasterProcedureEnv env) throws HBaseException, IOException {
    boolean canTableBeDisabled = true;
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      setFailure("master-disable-table", new ConstraintException("Cannot disable catalog table"));
      canTableBeDisabled = false;
    } else if (!MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      setFailure("master-disable-table", new TableNotFoundException(tableName));
      canTableBeDisabled = false;
    } else if (!skipTableStateCheck) {
      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      //
      // Note: A quick state check should be enough for us to move forward. However, instead of
      // calling TableStateManager.isTableState() to just check the state, we called
      // TableStateManager.setTableStateIfInStates() to set the state to DISABLING from ENABLED.
      // This is because we treat empty state as enabled from 0.92-clusters. See
      // ZKTableStateManager.setTableStateIfInStates() that has a hack solution to work around
      // this issue.
      TableStateManager tsm =
        env.getMasterServices().getAssignmentManager().getTableStateManager();
      if (!tsm.setTableStateIfInStates(tableName, ZooKeeperProtos.Table.State.DISABLING,
            ZooKeeperProtos.Table.State.DISABLING, ZooKeeperProtos.Table.State.ENABLED)) {
        LOG.info("Table " + tableName + " isn't enabled; skipping disable");
        setFailure("master-disable-table", new TableNotEnabledException(tableName));
        canTableBeDisabled = false;
      }
    }

    // We are done the check. Future actions in this procedure could be done asynchronously.
    ProcedurePrepareLatch.releaseLatch(syncLatch, this);

    return canTableBeDisabled;
  }

  /**
   * Rollback of table state change in prepareDisable()
   * @param env MasterProcedureEnv
   */
  private void undoTableStateChange(final MasterProcedureEnv env) {
    if (!skipTableStateCheck) {
      try {
        // If the state was changed, undo it.
        if (env.getMasterServices().getAssignmentManager().getTableStateManager().isTableState(
            tableName, ZooKeeperProtos.Table.State.DISABLING)) {
          EnableTableProcedure.setTableStateToEnabled(env, tableName);
        }
      } catch (Exception e) {
        // Ignore exception.
      }
    }
  }

  /**
   * Action before disabling table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  protected void preDisable(final MasterProcedureEnv env, final DisableTableState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Mark table state to Disabling
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  protected static void setTableStateToDisabling(
      final MasterProcedureEnv env,
      final TableName tableName) throws HBaseException, IOException {
    // Set table disabling flag up in zk.
    env.getMasterServices().getAssignmentManager().getTableStateManager().setTableState(
      tableName,
      ZooKeeperProtos.Table.State.DISABLING);
  }

  /**
   * Mark regions of the table offline with retries
   * @param env MasterProcedureEnv
   * @param tableName the target table
   * @param retryRequired whether to retry if the first run failed
   * @return whether the operation is fully completed or being interrupted.
   * @throws IOException
   */
  protected static MarkRegionOfflineOpResult markRegionsOffline(
      final MasterProcedureEnv env,
      final TableName tableName,
      final Boolean retryRequired) throws IOException {
    // Dev consideration: add a config to control max number of retry. For now, it is hard coded.
    int maxTry = (retryRequired ? 10 : 1);
    MarkRegionOfflineOpResult operationResult =
        MarkRegionOfflineOpResult.BULK_ASSIGN_REGIONS_FAILED;
    do {
      try {
        operationResult = markRegionsOffline(env, tableName);
        if (operationResult == MarkRegionOfflineOpResult.MARK_ALL_REGIONS_OFFLINE_SUCCESSFUL) {
          break;
        }
        maxTry--;
      } catch (Exception e) {
        LOG.warn("Received exception while marking regions online. tries left: " + maxTry, e);
        maxTry--;
        if (maxTry > 0) {
          continue; // we still have some retry left, try again.
        }
        throw e;
      }
    } while (maxTry > 0);

    if (operationResult != MarkRegionOfflineOpResult.MARK_ALL_REGIONS_OFFLINE_SUCCESSFUL) {
      LOG.warn("Some or all regions of the Table '" + tableName + "' were still online");
    }

    return operationResult;
  }

  /**
   * Mark regions of the table offline
   * @param env MasterProcedureEnv
   * @param tableName the target table
   * @return whether the operation is fully completed or being interrupted.
   * @throws IOException
   */
  private static MarkRegionOfflineOpResult markRegionsOffline(
      final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    // Get list of online regions that are of this table.  Regions that are
    // already closed will not be included in this list; i.e. the returned
    // list is not ALL regions in a table, its all online regions according
    // to the in-memory state on this master.
    MarkRegionOfflineOpResult operationResult =
        MarkRegionOfflineOpResult.MARK_ALL_REGIONS_OFFLINE_SUCCESSFUL;
    final List<HRegionInfo> regions =
        env.getMasterServices().getAssignmentManager().getRegionStates()
            .getRegionsOfTable(tableName);
    if (regions.size() > 0) {
      LOG.info("Offlining " + regions.size() + " regions.");

      BulkDisabler bd = new BulkDisabler(env, tableName, regions);
      try {
        if (!bd.bulkAssign()) {
          operationResult = MarkRegionOfflineOpResult.BULK_ASSIGN_REGIONS_FAILED;
        }
      } catch (InterruptedException e) {
        LOG.warn("Disable was interrupted");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
        operationResult = MarkRegionOfflineOpResult.MARK_ALL_REGIONS_OFFLINE_INTERRUPTED;
      }
    }
    return operationResult;
  }

  /**
   * Mark table state to Disabled
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  protected static void setTableStateToDisabled(
      final MasterProcedureEnv env,
      final TableName tableName) throws HBaseException, IOException {
    // Flip the table to disabled
    env.getMasterServices().getAssignmentManager().getTableStateManager().setTableState(
      tableName,
      ZooKeeperProtos.Table.State.DISABLED);
    LOG.info("Disabled table, " + tableName + ", is completed.");
  }

  /**
   * Action after disabling table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  protected void postDisable(final MasterProcedureEnv env, final DisableTableState state)
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

  /**
   * Coprocessor Action.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final DisableTableState state)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          switch (state) {
          case DISABLE_TABLE_PRE_OPERATION:
            cpHost.preDisableTableHandler(tableName);
            break;
          case DISABLE_TABLE_POST_OPERATION:
            cpHost.postDisableTableHandler(tableName);
            break;
          default:
            throw new UnsupportedOperationException(this + " unhandled state=" + state);
          }
          return null;
        }
      });
    }
  }

  /**
   * Run bulk disable.
   */
  private static class BulkDisabler extends BulkAssigner {
    private final AssignmentManager assignmentManager;
    private final List<HRegionInfo> regions;
    private final TableName tableName;
    private final int waitingTimeForEvents;

    public BulkDisabler(final MasterProcedureEnv env, final TableName tableName,
        final List<HRegionInfo> regions) {
      super(env.getMasterServices());
      this.assignmentManager = env.getMasterServices().getAssignmentManager();
      this.tableName = tableName;
      this.regions = regions;
      this.waitingTimeForEvents =
          env.getMasterServices().getConfiguration()
              .getInt("hbase.master.event.waiting.time", 1000);
    }

    @Override
    protected void populatePool(ExecutorService pool) {
      RegionStates regionStates = assignmentManager.getRegionStates();
      for (final HRegionInfo region : regions) {
        if (regionStates.isRegionInTransition(region)
            && !regionStates.isRegionInState(region, RegionState.State.FAILED_CLOSE)) {
          continue;
        }
        pool.execute(Trace.wrap("DisableTableHandler.BulkDisabler", new Runnable() {
          @Override
          public void run() {
            assignmentManager.unassign(region);
          }
        }));
      }
    }

    @Override
    protected boolean waitUntilDone(long timeout) throws InterruptedException {
      long startTime = EnvironmentEdgeManager.currentTime();
      long remaining = timeout;
      List<HRegionInfo> regions = null;
      long lastLogTime = startTime;
      while (!server.isStopped() && remaining > 0) {
        Thread.sleep(waitingTimeForEvents);
        regions = assignmentManager.getRegionStates().getRegionsOfTable(tableName);
        long now = EnvironmentEdgeManager.currentTime();
        // Don't log more than once every ten seconds. Its obnoxious. And only log table regions
        // if we are waiting a while for them to go down...
        if (LOG.isDebugEnabled() && ((now - lastLogTime) > 10000)) {
          lastLogTime = now;
          LOG.debug("Disable waiting until done; " + remaining + " ms remaining; " + regions);
        }
        if (regions.isEmpty()) break;
        remaining = timeout - (now - startTime);
      }
      return regions != null && regions.isEmpty();
    }
  }
}