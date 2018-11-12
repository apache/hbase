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
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.EnableTableState;

@InterfaceAudience.Private
public class EnableTableProcedure
    extends AbstractStateMachineTableProcedure<EnableTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(EnableTableProcedure.class);

  private TableName tableName;

  private Boolean traceEnabled = null;

  public EnableTableProcedure() {
  }

  /**
   * Constructor
   * @param env MasterProcedureEnv
   * @param tableName the table to operate on
   */
  public EnableTableProcedure(MasterProcedureEnv env, TableName tableName) {
    this(env, tableName, null);
  }

  /**
   * Constructor
   * @param env MasterProcedureEnv
   * @param tableName the table to operate on
   */
  public EnableTableProcedure(MasterProcedureEnv env, TableName tableName,
      ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.tableName = tableName;
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
          Connection connection = env.getMasterServices().getConnection();
          // we will need to get the tableDescriptor here to see if there is a change in the replica
          // count
          TableDescriptor hTableDescriptor =
              env.getMasterServices().getTableDescriptors().get(tableName);

          // Get the replica count
          int regionReplicaCount = hTableDescriptor.getRegionReplication();

          // Get the regions for the table from memory; get both online and offline regions
          // ('true').
          List<RegionInfo> regionsOfTable =
              env.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName, true);

          int currentMaxReplica = 0;
          // Check if the regions in memory have replica regions as marked in META table
          for (RegionInfo regionInfo : regionsOfTable) {
            if (regionInfo.getReplicaId() > currentMaxReplica) {
              // Iterating through all the list to identify the highest replicaID region.
              // We can stop after checking with the first set of regions??
              currentMaxReplica = regionInfo.getReplicaId();
            }
          }

          // read the META table to know the actual number of replicas for the table - if there
          // was a table modification on region replica then this will reflect the new entries also
          int replicasFound =
              getNumberOfReplicasFromMeta(connection, regionReplicaCount, regionsOfTable);
          assert regionReplicaCount - 1 == replicasFound;
          LOG.info(replicasFound + " META entries added for the given regionReplicaCount "
              + regionReplicaCount + " for the table " + tableName.getNameAsString());
          if (currentMaxReplica == (regionReplicaCount - 1)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("There is no change to the number of region replicas."
                  + " Assigning the available regions." + " Current and previous"
                  + "replica count is " + regionReplicaCount);
            }
          } else if (currentMaxReplica > (regionReplicaCount - 1)) {
            // we have additional regions as the replica count has been decreased. Delete
            // those regions because already the table is in the unassigned state
            LOG.info("The number of replicas " + (currentMaxReplica + 1)
                + "  is more than the region replica count " + regionReplicaCount);
            List<RegionInfo> copyOfRegions = new ArrayList<RegionInfo>(regionsOfTable);
            for (RegionInfo regionInfo : copyOfRegions) {
              if (regionInfo.getReplicaId() > (regionReplicaCount - 1)) {
                // delete the region from the regionStates
                env.getAssignmentManager().getRegionStates().deleteRegion(regionInfo);
                // remove it from the list of regions of the table
                LOG.info("The regioninfo being removed is " + regionInfo + " "
                    + regionInfo.getReplicaId());
                regionsOfTable.remove(regionInfo);
              }
            }
          } else {
            // the replicasFound is less than the regionReplication
            LOG.info("The number of replicas has been changed(increased)."
                + " Lets assign the new region replicas. The previous replica count was "
                + (currentMaxReplica + 1) + ". The current replica count is " + regionReplicaCount);
            regionsOfTable = RegionReplicaUtil.addReplicas(hTableDescriptor, regionsOfTable,
              currentMaxReplica + 1, regionReplicaCount);
          }
          // Assign all the table regions. (including region replicas if added).
          // createAssignProcedure will try to retain old assignments if possible.
          addChildProcedure(env.getAssignmentManager().createAssignProcedures(regionsOfTable));
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
      if (isRollbackSupported(state)) {
        setFailure("master-enable-table", e);
      } else {
        LOG.warn(
          "Retriable error trying to enable table=" + tableName + " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  private int getNumberOfReplicasFromMeta(Connection connection, int regionReplicaCount,
      List<RegionInfo> regionsOfTable) throws IOException {
    Result r = getRegionFromMeta(connection, regionsOfTable);
    int replicasFound = 0;
    for (int i = 1; i < regionReplicaCount; i++) {
      // Since we have already added the entries to the META we will be getting only that here
      List<Cell> columnCells =
          r.getColumnCells(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i));
      if (!columnCells.isEmpty()) {
        replicasFound++;
      }
    }
    return replicasFound;
  }

  private Result getRegionFromMeta(Connection connection, List<RegionInfo> regionsOfTable)
      throws IOException {
    byte[] metaKeyForRegion = MetaTableAccessor.getMetaKeyForRegion(regionsOfTable.get(0));
    Get get = new Get(metaKeyForRegion);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Table metaTable = MetaTableAccessor.getMetaHTable(connection);
    Result r = metaTable.get(get);
    return r;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final EnableTableState state)
      throws IOException {
    // nothing to rollback, prepare-disable is just table-state checks.
    // We can fail if the table does not exist or is not disabled.
    switch (state) {
      case ENABLE_TABLE_PRE_OPERATION:
        return;
      case ENABLE_TABLE_PREPARE:
        releaseSyncLatch();
        return;
      default:
        break;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final EnableTableState state) {
    switch (state) {
      case ENABLE_TABLE_PREPARE:
      case ENABLE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected EnableTableState getState(final int stateId) {
    return EnableTableState.forNumber(stateId);
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
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    // the skipTableStateCheck is false so we still need to set it...
    @SuppressWarnings("deprecation")
    MasterProcedureProtos.EnableTableStateData.Builder enableTableMsg =
      MasterProcedureProtos.EnableTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName)).setSkipTableStateCheck(false);

    serializer.serialize(enableTableMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.EnableTableStateData enableTableMsg =
      serializer.deserialize(MasterProcedureProtos.EnableTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(enableTableMsg.getUserInfo()));
    tableName = ProtobufUtil.toTableName(enableTableMsg.getTableName());
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
    } else {
      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      //
      // Note: in 1.0 release, we called TableStateManager.setTableStateIfInStates() to set
      // the state to ENABLING from DISABLED. The implementation was done before table lock
      // was implemented. With table lock, there is no need to set the state here (it will
      // set the state later on). A quick state check should be enough for us to move forward.
      TableStateManager tsm = env.getMasterServices().getTableStateManager();
      TableState ts = tsm.getTableState(tableName);
      if(!ts.isDisabled()){
        LOG.info("Not DISABLED tableState={}; skipping enable; {}", ts.getState(), this);
        setFailure("master-enable-table", new TableNotDisabledException(ts.toString()));
        canTableBeEnabled = false;
      }
    }

    // We are done the check. Future actions in this procedure could be done asynchronously.
    releaseSyncLatch();

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
    env.getMasterServices().getTableStateManager().setTableState(
      tableName,
      TableState.State.ENABLING);
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
    env.getMasterServices().getTableStateManager().setTableState(
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
      switch (state) {
        case ENABLE_TABLE_PRE_OPERATION:
          cpHost.preEnableTableAction(getTableName(), getUser());
          break;
        case ENABLE_TABLE_POST_OPERATION:
          cpHost.postCompletedEnableTableAction(getTableName(), getUser());
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }
}
