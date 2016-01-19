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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkAssigner;
import org.apache.hadoop.hbase.master.GeneralBulkAssigner;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.EnableTableState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Private
public class EnableTableProcedure
    extends StateMachineProcedure<MasterProcedureEnv, EnableTableState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(EnableTableProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  // This is for back compatible with 1.0 asynchronized operations.
  private final ProcedurePrepareLatch syncLatch;

  private TableName tableName;
  private boolean skipTableStateCheck;
  private UserGroupInformation user;

  private Boolean traceEnabled = null;

  public EnableTableProcedure() {
    syncLatch = null;
  }

  /**
   * Constructor
   * @param env MasterProcedureEnv
   * @param tableName the table to operate on
   * @param skipTableStateCheck whether to check table state
   * @throws IOException
   */
  public EnableTableProcedure(
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
  public EnableTableProcedure(
      final MasterProcedureEnv env,
      final TableName tableName,
      final boolean skipTableStateCheck,
      final ProcedurePrepareLatch syncLatch) throws IOException {
    this.tableName = tableName;
    this.skipTableStateCheck = skipTableStateCheck;
    this.user = env.getRequestUser().getUGI();
    this.setOwner(this.user.getShortUserName());

    // Compatible with 1.0: We use latch to make sure that this procedure implementation is
    // compatible with 1.0 asynchronized operations. We need to lock the table and check
    // whether the Enable operation could be performed (table exists and offline; table state
    // is DISABLED). Once it is done, we are good to release the latch and the client can
    // start asynchronously wait for the operation.
    //
    // Note: the member syncLatch could be null if we are in failover or recovery scenario.
    // This is ok for backward compatible, as 1.0 client would not able to peek at procedure.
    this.syncLatch = syncLatch;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final EnableTableState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case ENABLE_TABLE_PREPARE:
        if (prepareEnable(env)) {
          setNextState(EnableTableState.ENABLE_TABLE_PRE_OPERATION);
        } else {
          assert isFailed() : "enable should have an exception here";
          return Flow.NO_MORE_STATE;
        }
        break;
      case ENABLE_TABLE_PRE_OPERATION:
        preEnable(env, state);
        setNextState(EnableTableState.ENABLE_TABLE_SET_ENABLING_TABLE_STATE);
        break;
      case ENABLE_TABLE_SET_ENABLING_TABLE_STATE:
        setTableStateToEnabling(env, tableName);
        setNextState(EnableTableState.ENABLE_TABLE_MARK_REGIONS_ONLINE);
        break;
      case ENABLE_TABLE_MARK_REGIONS_ONLINE:
        markRegionsOnline(env, tableName, true);
        setNextState(EnableTableState.ENABLE_TABLE_SET_ENABLED_TABLE_STATE);
        break;
      case ENABLE_TABLE_SET_ENABLED_TABLE_STATE:
        setTableStateToEnabled(env, tableName);
        setNextState(EnableTableState.ENABLE_TABLE_POST_OPERATION);
        break;
      case ENABLE_TABLE_POST_OPERATION:
        postEnable(env, state);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.error("Error trying to enable table=" + tableName + " state=" + state, e);
      setFailure("master-enable-table", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final EnableTableState state)
      throws IOException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }
    try {
      switch (state) {
      case ENABLE_TABLE_POST_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo (eg. DisableTableProcedure.preDisable())?
        break;
      case ENABLE_TABLE_SET_ENABLED_TABLE_STATE:
        DisableTableProcedure.setTableStateToDisabling(env, tableName);
        break;
      case ENABLE_TABLE_MARK_REGIONS_ONLINE:
        markRegionsOfflineDuringRecovery(env);
        break;
      case ENABLE_TABLE_SET_ENABLING_TABLE_STATE:
        DisableTableProcedure.setTableStateToDisabled(env, tableName);
        break;
      case ENABLE_TABLE_PRE_OPERATION:
        // TODO-MAYBE: call the coprocessor event to undo (eg. DisableTableProcedure.postDisable())?
        break;
      case ENABLE_TABLE_PREPARE:
        // Nothing to undo for this state.
        // We do need to count down the latch count so that we don't stuck.
        ProcedurePrepareLatch.releaseLatch(syncLatch, this);
        break;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed enable table rollback attempt step=" + state + " table=" + tableName, e);
      throw e;
    }
  }

  @Override
  protected EnableTableState getState(final int stateId) {
    return EnableTableState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final EnableTableState state) {
    return state.getNumber();
  }

  @Override
  protected EnableTableState getInitialState() {
    return EnableTableState.ENABLE_TABLE_PREPARE;
  }

  @Override
  protected void setNextState(final EnableTableState state) {
    if (aborted.get()) {
      setAbortFailure("Enable-table", "abort requested");
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

    MasterProcedureProtos.EnableTableStateData.Builder enableTableMsg =
        MasterProcedureProtos.EnableTableStateData.newBuilder()
            .setUserInfo(MasterProcedureUtil.toProtoUserInfo(user))
            .setTableName(ProtobufUtil.toProtoTableName(tableName))
            .setSkipTableStateCheck(skipTableStateCheck);

    enableTableMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.EnableTableStateData enableTableMsg =
        MasterProcedureProtos.EnableTableStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(enableTableMsg.getUserInfo());
    tableName = ProtobufUtil.toTableName(enableTableMsg.getTableName());
    skipTableStateCheck = enableTableMsg.getSkipTableStateCheck();
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(tableName);
    sb.append(")");
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.ENABLE;
  }


  /**
   * Action before any real action of enabling table. Set the exception in the procedure instead
   * of throwing it.  This approach is to deal with backward compatible with 1.0.
   * @param env MasterProcedureEnv
   * @return whether the table passes the necessary checks
   * @throws IOException
   */
  private boolean prepareEnable(final MasterProcedureEnv env) throws IOException {
    boolean canTableBeEnabled = true;

    // Check whether table exists
    if (!MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      setFailure("master-enable-table", new TableNotFoundException(tableName));
      canTableBeEnabled = false;
    } else if (!skipTableStateCheck) {
      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      //
      // Note: in 1.0 release, we called TableStateManager.setTableStateIfInStates() to set
      // the state to ENABLING from DISABLED. The implementation was done before table lock
      // was implemented. With table lock, there is no need to set the state here (it will
      // set the state later on). A quick state check should be enough for us to move forward.
      TableStateManager tsm = env.getMasterServices().getAssignmentManager().getTableStateManager();
      TableState.State state = tsm.getTableState(tableName);
      if(!state.equals(TableState.State.DISABLED)){
        LOG.info("Table " + tableName + " isn't disabled;is "+state.name()+"; skipping enable");
        setFailure("master-enable-table", new TableNotDisabledException(
                this.tableName+" state is "+state.name()));
        canTableBeEnabled = false;
      }
    }

    // We are done the check. Future actions in this procedure could be done asynchronously.
    ProcedurePrepareLatch.releaseLatch(syncLatch, this);

    return canTableBeEnabled;
  }

  /**
   * Action before enabling table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void preEnable(final MasterProcedureEnv env, final EnableTableState state)
      throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Mark table state to Enabling
   * @param env MasterProcedureEnv
   * @param tableName the target table
   * @throws IOException
   */
  protected static void setTableStateToEnabling(
      final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    // Set table disabling flag up in zk.
    LOG.info("Attempting to enable the table " + tableName);
    env.getMasterServices().getAssignmentManager().getTableStateManager().setTableState(
      tableName,
      TableState.State.ENABLING);
  }

  /**
   * Mark offline regions of the table online with retry
   * @param env MasterProcedureEnv
   * @param tableName the target table
   * @param retryRequired whether to retry if the first run failed
   * @throws IOException
   */
  protected static void markRegionsOnline(
      final MasterProcedureEnv env,
      final TableName tableName,
      final Boolean retryRequired) throws IOException {
    // This is best effort approach to make all regions of a table online.  If we fail to do
    // that, it is ok that the table has some offline regions; user can fix it manually.

    // Dev consideration: add a config to control max number of retry. For now, it is hard coded.
    int maxTry = (retryRequired ? 10 : 1);
    boolean done = false;

    do {
      try {
        done = markRegionsOnline(env, tableName);
        if (done) {
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

    if (!done) {
      LOG.warn("Some or all regions of the Table '" + tableName + "' were offline");
    }
  }

  /**
   * Mark offline regions of the table online
   * @param env MasterProcedureEnv
   * @param tableName the target table
   * @return whether the operation is fully completed or being interrupted.
   * @throws IOException
   */
  private static boolean markRegionsOnline(final MasterProcedureEnv env, final TableName tableName)
      throws IOException {
    final AssignmentManager assignmentManager = env.getMasterServices().getAssignmentManager();
    final MasterServices masterServices = env.getMasterServices();
    final ServerManager serverManager = masterServices.getServerManager();
    boolean done = false;
    // Get the regions of this table. We're done when all listed
    // tables are onlined.
    List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations;

    if (TableName.META_TABLE_NAME.equals(tableName)) {
      tableRegionsAndLocations =
          new MetaTableLocator().getMetaRegionsAndLocations(masterServices.getZooKeeper());
    } else {
      tableRegionsAndLocations =
          MetaTableAccessor.getTableRegionsAndLocations(masterServices.getConnection(), tableName);
    }

    int countOfRegionsInTable = tableRegionsAndLocations.size();
    Map<HRegionInfo, ServerName> regionsToAssign =
        regionsToAssignWithServerName(env, tableRegionsAndLocations);

    // need to potentially create some regions for the replicas
    List<HRegionInfo> unrecordedReplicas =
        AssignmentManager.replicaRegionsNotRecordedInMeta(new HashSet<HRegionInfo>(
            regionsToAssign.keySet()), masterServices);
    Map<ServerName, List<HRegionInfo>> srvToUnassignedRegs =
        assignmentManager.getBalancer().roundRobinAssignment(unrecordedReplicas,
          serverManager.getOnlineServersList());
    if (srvToUnassignedRegs != null) {
      for (Map.Entry<ServerName, List<HRegionInfo>> entry : srvToUnassignedRegs.entrySet()) {
        for (HRegionInfo h : entry.getValue()) {
          regionsToAssign.put(h, entry.getKey());
        }
      }
    }

    int offlineRegionsCount = regionsToAssign.size();

    LOG.info("Table '" + tableName + "' has " + countOfRegionsInTable + " regions, of which "
        + offlineRegionsCount + " are offline.");
    if (offlineRegionsCount == 0) {
      return true;
    }

    List<ServerName> onlineServers = serverManager.createDestinationServersList();
    Map<ServerName, List<HRegionInfo>> bulkPlan =
        env.getMasterServices().getAssignmentManager().getBalancer()
            .retainAssignment(regionsToAssign, onlineServers);
    if (bulkPlan != null) {
      LOG.info("Bulk assigning " + offlineRegionsCount + " region(s) across " + bulkPlan.size()
          + " server(s), retainAssignment=true");

      BulkAssigner ba = new GeneralBulkAssigner(masterServices, bulkPlan, assignmentManager, true);
      try {
        if (ba.bulkAssign()) {
          done = true;
        }
      } catch (InterruptedException e) {
        LOG.warn("Enable operation was interrupted when enabling table '" + tableName + "'");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
      }
    } else {
      LOG.info("Balancer was unable to find suitable servers for table " + tableName
          + ", leaving unassigned");
    }
    return done;
  }

  /**
   * Mark regions of the table offline during recovery
   * @param env MasterProcedureEnv
   */
  private void markRegionsOfflineDuringRecovery(final MasterProcedureEnv env) {
    try {
      // This is a best effort attempt. We will move on even it does not succeed. We will retry
      // several times until we giving up.
      DisableTableProcedure.markRegionsOffline(env, tableName, true);
    } catch (Exception e) {
      LOG.debug("Failed to offline all regions of table " + tableName + ". Ignoring", e);
    }
  }

  /**
   * Mark table state to Enabled
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  protected static void setTableStateToEnabled(
      final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    // Flip the table to Enabled
    env.getMasterServices().getAssignmentManager().getTableStateManager().setTableState(
      tableName,
      TableState.State.ENABLED);
    LOG.info("Table '" + tableName + "' was successfully enabled.");
  }

  /**
   * Action after enabling table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void postEnable(final MasterProcedureEnv env, final EnableTableState state)
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
   * @param regionsInMeta
   * @return List of regions neither in transition nor assigned.
   * @throws IOException
   */
  private static Map<HRegionInfo, ServerName> regionsToAssignWithServerName(
      final MasterProcedureEnv env,
      final List<Pair<HRegionInfo, ServerName>> regionsInMeta) throws IOException {
    Map<HRegionInfo, ServerName> regionsToAssign =
        new HashMap<HRegionInfo, ServerName>(regionsInMeta.size());
    RegionStates regionStates = env.getMasterServices().getAssignmentManager().getRegionStates();
    for (Pair<HRegionInfo, ServerName> regionLocation : regionsInMeta) {
      HRegionInfo hri = regionLocation.getFirst();
      ServerName sn = regionLocation.getSecond();
      if (regionStates.isRegionOffline(hri)) {
        regionsToAssign.put(hri, sn);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping assign for the region " + hri + " during enable table "
              + hri.getTable() + " because its already in tranition or assigned.");
        }
      }
    }
    return regionsToAssign;
  }

  /**
   * Coprocessor Action.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final EnableTableState state)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          switch (state) {
          case ENABLE_TABLE_PRE_OPERATION:
            cpHost.preEnableTableHandler(getTableName());
            break;
          case ENABLE_TABLE_POST_OPERATION:
            cpHost.postEnableTableHandler(getTableName());
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
