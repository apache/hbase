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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.StorageIdentifier;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class CreateTableProcedure
    extends AbstractStateMachineTableProcedure<CreateTableState> {
  private static final Log LOG = LogFactory.getLog(CreateTableProcedure.class);

  private HTableDescriptor hTableDescriptor;
  private List<HRegionInfo> newRegions;

  public CreateTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions) {
    this(env, hTableDescriptor, newRegions, null);
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions,
      final ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.hTableDescriptor = hTableDescriptor;
    this.newRegions = newRegions != null ? Lists.newArrayList(newRegions) : null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final CreateTableState state)
      throws InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case CREATE_TABLE_PRE_OPERATION:
          // Verify if we can create the table
          boolean exists = !prepareCreate(env);
          releaseSyncLatch();

          if (exists) {
            assert isFailed() : "the delete should have an exception here";
            return Flow.NO_MORE_STATE;
          }

          preCreate(env);
          setNextState(CreateTableState.CREATE_TABLE_WRITE_FS_LAYOUT);
          break;
        case CREATE_TABLE_WRITE_FS_LAYOUT:
          newRegions = createTableOnStorage(env, hTableDescriptor, newRegions);
          setNextState(CreateTableState.CREATE_TABLE_ADD_TO_META);
          break;
        case CREATE_TABLE_ADD_TO_META:
          newRegions = addTableToMeta(env, hTableDescriptor, newRegions);
          setNextState(CreateTableState.CREATE_TABLE_ASSIGN_REGIONS);
          break;
        case CREATE_TABLE_ASSIGN_REGIONS:
          assignRegions(env, getTableName(), newRegions);
          setNextState(CreateTableState.CREATE_TABLE_UPDATE_DESC_CACHE);
          break;
        case CREATE_TABLE_UPDATE_DESC_CACHE:
          updateTableDescCache(env, getTableName());
          setNextState(CreateTableState.CREATE_TABLE_POST_OPERATION);
          break;
        case CREATE_TABLE_POST_OPERATION:
          postCreate(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-create-table", e);
      } else {
        LOG.warn("Retriable error trying to create table=" + getTableName() + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final CreateTableState state)
      throws IOException {
    if (state == CreateTableState.CREATE_TABLE_PRE_OPERATION) {
      // nothing to rollback, pre-create is just table-state checks.
      // We can fail if the table does exist or the descriptor is malformed.
      // TODO: coprocessor rollback semantic is still undefined.
      DeleteTableProcedure.deleteTableStates(env, getTableName());
      releaseSyncLatch();
      return;
    }

    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final CreateTableState state) {
    switch (state) {
      case CREATE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected CreateTableState getState(final int stateId) {
    return CreateTableState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final CreateTableState state) {
    return state.getNumber();
  }

  @Override
  protected CreateTableState getInitialState() {
    return CreateTableState.CREATE_TABLE_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
    return hTableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.CREATE;
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.CreateTableStateData.Builder state =
      MasterProcedureProtos.CreateTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
            .setTableSchema(ProtobufUtil.convertToTableSchema(hTableDescriptor));
    if (newRegions != null) {
      for (HRegionInfo hri: newRegions) {
        state.addRegionInfo(HRegionInfo.convert(hri));
      }
    }
    state.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.CreateTableStateData state =
      MasterProcedureProtos.CreateTableStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    hTableDescriptor = ProtobufUtil.convertToHTableDesc(state.getTableSchema());
    if (state.getRegionInfoCount() == 0) {
      newRegions = null;
    } else {
      newRegions = new ArrayList<HRegionInfo>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: state.getRegionInfoList()) {
        newRegions.add(HRegionInfo.convert(hri));
      }
    }
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (!getTableName().isSystemTable() && env.waitInitialized(this)) {
      return false;
    }
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(this, getTableName());
  }

  private boolean prepareCreate(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    if (MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      setFailure("master-create-table", new TableExistsException(getTableName()));
      return false;
    }

    // check that we have at least 1 CF
    if (hTableDescriptor.getColumnFamilies().length == 0) {
      setFailure("master-create-table", new DoNotRetryIOException("Table " +
          getTableName().toString() + " should have at least one column family."));
      return false;
    }

    return true;
  }

  private void preCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    if (!getTableName().isSystemTable()) {
      ProcedureSyncWait.getMasterQuotaManager(env)
        .checkNamespaceTableAndRegionQuota(
          getTableName(), (newRegions != null ? newRegions.size() : 0));
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = newRegions == null ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      cpHost.preCreateTableAction(hTableDescriptor, regions, getUser());
    }
  }

  private void postCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = (newRegions == null) ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      cpHost.postCompletedCreateTableAction(hTableDescriptor, regions, getUser());
    }
  }

  protected interface CreateStorageRegions {
    List<HRegionInfo> createRegionsOnStorage(final MasterProcedureEnv env,
        final TableName tableName, final List<HRegionInfo> newRegions) throws IOException;
  }

  protected static List<HRegionInfo> createTableOnStorage(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final List<HRegionInfo> newRegions)
      throws IOException {
    return createTableOnStorage(env, hTableDescriptor, newRegions, new CreateStorageRegions() {
      @Override
      public List<HRegionInfo> createRegionsOnStorage(final MasterProcedureEnv env,
          final TableName tableName, final List<HRegionInfo> newRegions) throws IOException {
        HRegionInfo[] regions = newRegions != null ?
          newRegions.toArray(new HRegionInfo[newRegions.size()]) : null;
        return ModifyRegionUtils.createRegions(env.getMasterConfiguration(), hTableDescriptor,
            regions, null);
      }
    });
  }

  protected static List<HRegionInfo> createTableOnStorage(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, List<HRegionInfo> newRegions,
      final CreateStorageRegions storageRegionHandler) throws IOException {
    final MasterStorage<? extends StorageIdentifier> ms =
        env.getMasterServices().getMasterStorage();

    // 1. Delete existing artifacts (dir, files etc) for the table
    ms.deleteTable(hTableDescriptor.getTableName());

    // 2. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    HTableDescriptor underConstruction = new HTableDescriptor(hTableDescriptor);
    ms.createTableDescriptor(underConstruction, true);

    // 3. Create Regions
    newRegions = storageRegionHandler.createRegionsOnStorage(env, hTableDescriptor.getTableName(),
        newRegions);

    return newRegions;
  }

  protected static List<HRegionInfo> addTableToMeta(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor,
      final List<HRegionInfo> regions) throws IOException {
    if (regions != null && regions.size() > 0) {
      ProcedureSyncWait.waitMetaRegions(env);

      // Add regions to META
      addRegionsToMeta(env, hTableDescriptor, regions);
      // Add replicas if needed
      List<HRegionInfo> newRegions = addReplicas(env, hTableDescriptor, regions);

      // Setup replication for region replicas if needed
      if (hTableDescriptor.getRegionReplication() > 1) {
        ServerRegionReplicaUtil.setupRegionReplicaReplication(env.getMasterConfiguration());
      }
      return newRegions;
    }
    return regions;
  }

  /**
   * Create any replicas for the regions (the default replicas that was
   * already created is passed to the method)
   * @param hTableDescriptor descriptor to use
   * @param regions default replicas
   * @return the combined list of default and non-default replicas
   */
  private static List<HRegionInfo> addReplicas(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor,
      final List<HRegionInfo> regions) {
    int numRegionReplicas = hTableDescriptor.getRegionReplication() - 1;
    if (numRegionReplicas <= 0) {
      return regions;
    }
    List<HRegionInfo> hRegionInfos =
        new ArrayList<HRegionInfo>((numRegionReplicas+1)*regions.size());
    for (int i = 0; i < regions.size(); i++) {
      for (int j = 1; j <= numRegionReplicas; j++) {
        hRegionInfos.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(i), j));
      }
    }
    hRegionInfos.addAll(regions);
    return hRegionInfos;
  }

  protected static void assignRegions(final MasterProcedureEnv env,
      final TableName tableName, final List<HRegionInfo> regions) throws IOException {
    ProcedureSyncWait.waitRegionServers(env);

    // Mark the table as Enabling
    env.getMasterServices().getTableStateManager()
      .setTableState(tableName, TableState.State.ENABLING);

    // Trigger immediate assignment of the regions in round-robin fashion
    final AssignmentManager assignmentManager = env.getMasterServices().getAssignmentManager();
    ModifyRegionUtils.assignRegions(assignmentManager, regions);

    // Enable table
    env.getMasterServices().getTableStateManager()
      .setTableState(tableName, TableState.State.ENABLED);
  }

  /**
   * Add the specified set of regions to the hbase:meta table.
   */
  private static void addRegionsToMeta(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor,
      final List<HRegionInfo> regionInfos) throws IOException {
    MetaTableAccessor.addRegionsToMeta(env.getMasterServices().getConnection(),
      regionInfos, hTableDescriptor.getRegionReplication());
  }

  protected static void updateTableDescCache(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    env.getMasterServices().getTableDescriptors().get(tableName);
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
    // system tables are created on bootstrap internally by the system
    // the client does not know about this procedures.
    return !getTableName().isSystemTable();
  }
}
