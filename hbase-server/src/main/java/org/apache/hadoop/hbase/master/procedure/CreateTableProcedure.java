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
import java.util.function.Supplier;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerValidationUtils;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CreateTableState;

@InterfaceAudience.Private
public class CreateTableProcedure
    extends AbstractStateMachineTableProcedure<CreateTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableProcedure.class);

  private TableDescriptor tableDescriptor;
  private List<RegionInfo> newRegions;

  public CreateTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, final RegionInfo[] newRegions) {
    this(env, tableDescriptor, newRegions, null);
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, final RegionInfo[] newRegions,
      final ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.tableDescriptor = tableDescriptor;
    this.newRegions = newRegions != null ? Lists.newArrayList(newRegions) : null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final CreateTableState state)
      throws InterruptedException {
    LOG.info("{} execute state={}", this, state);
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
          DeleteTableProcedure.deleteFromFs(env, getTableName(), newRegions, true);
          newRegions = createFsLayout(env, tableDescriptor, newRegions);
          env.getMasterServices().getTableDescriptors().update(tableDescriptor, true);
          setNextState(CreateTableState.CREATE_TABLE_ADD_TO_META);
          break;
        case CREATE_TABLE_ADD_TO_META:
          newRegions = addTableToMeta(env, tableDescriptor, newRegions);
          setNextState(CreateTableState.CREATE_TABLE_ASSIGN_REGIONS);
          break;
        case CREATE_TABLE_ASSIGN_REGIONS:
          setEnablingState(env, getTableName());
          addChildProcedure(env.getAssignmentManager()
            .createRoundRobinAssignProcedures(newRegions));
          setNextState(CreateTableState.CREATE_TABLE_UPDATE_DESC_CACHE);
          break;
        case CREATE_TABLE_UPDATE_DESC_CACHE:
          // XXX: this stage should be named as set table enabled, as now we will cache the
          // descriptor after writing fs layout.
          setEnabledState(env, getTableName());
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
      if (hasException() /* avoid NPE */ &&
          getException().getCause().getClass() != TableExistsException.class) {
        DeleteTableProcedure.deleteTableStates(env, getTableName());

        final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
        if (cpHost != null) {
          cpHost.postDeleteTable(getTableName());
        }
      }

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
    return CreateTableState.forNumber(stateId);
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
    return tableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.CREATE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.CreateTableStateData.Builder state =
      MasterProcedureProtos.CreateTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
            .setTableSchema(ProtobufUtil.toTableSchema(tableDescriptor));
    if (newRegions != null) {
      for (RegionInfo hri: newRegions) {
        state.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
      }
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.CreateTableStateData state =
        serializer.deserialize(MasterProcedureProtos.CreateTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    tableDescriptor = ProtobufUtil.toTableDescriptor(state.getTableSchema());
    if (state.getRegionInfoCount() == 0) {
      newRegions = null;
    } else {
      newRegions = new ArrayList<>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: state.getRegionInfoList()) {
        newRegions.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    if (getTableName().isSystemTable()) {
      // Creating system table is part of the initialization, so only wait for meta loaded instead
      // of waiting for master fully initialized.
      return env.getAssignmentManager().waitMetaLoaded(this);
    }
    return super.waitInitialized(env);
  }

  private boolean prepareCreate(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    if (env.getMasterServices().getTableDescriptors().exists(tableName)) {
      setFailure("master-create-table", new TableExistsException(getTableName()));
      return false;
    }

    // check that we have at least 1 CF
    if (tableDescriptor.getColumnFamilyCount() == 0) {
      setFailure("master-create-table", new DoNotRetryIOException(
        "Table " + getTableName().toString() + " should have at least one column family."));
      return false;
    }
    if (!tableName.isSystemTable()) {
      // do not check rs group for system tables as we may block the bootstrap.
      Supplier<String> forWhom = () -> "table " + tableName;
      RSGroupInfo rsGroupInfo = MasterProcedureUtil.checkGroupExists(
        env.getMasterServices().getRSGroupInfoManager()::getRSGroup,
        tableDescriptor.getRegionServerGroup(), forWhom);
      if (rsGroupInfo == null) {
        // we do not set rs group info on table, check if we have one on namespace
        String namespace = tableName.getNamespaceAsString();
        NamespaceDescriptor nd = env.getMasterServices().getClusterSchema().getNamespace(namespace);
        forWhom = () -> "table " + tableName + "(inherit from namespace)";
        rsGroupInfo = MasterProcedureUtil.checkGroupExists(
          env.getMasterServices().getRSGroupInfoManager()::getRSGroup,
          MasterProcedureUtil.getNamespaceGroup(nd), forWhom);
      }
      MasterProcedureUtil.checkGroupNotEmpty(rsGroupInfo, forWhom);
    }

    // check for store file tracker configurations
    StoreFileTrackerValidationUtils.checkForCreateTable(env.getMasterConfiguration(),
      tableDescriptor);

    return true;
  }

  private void preCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    if (!getTableName().isSystemTable()) {
      ProcedureSyncWait.getMasterQuotaManager(env).checkNamespaceTableAndRegionQuota(getTableName(),
        (newRegions != null ? newRegions.size() : 0));
    }

    tableDescriptor = StoreFileTrackerFactory.updateWithTrackerConfigs(env.getMasterConfiguration(),
      tableDescriptor);

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final RegionInfo[] regions = newRegions == null ? null :
        newRegions.toArray(new RegionInfo[newRegions.size()]);
      cpHost.preCreateTableAction(tableDescriptor, regions, getUser());
    }
  }

  private void postCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final RegionInfo[] regions = (newRegions == null) ? null :
        newRegions.toArray(new RegionInfo[newRegions.size()]);
      cpHost.postCompletedCreateTableAction(tableDescriptor, regions, getUser());
    }
  }

  protected interface CreateHdfsRegions {
    List<RegionInfo> createHdfsRegions(final MasterProcedureEnv env,
      final Path tableRootDir, final TableName tableName,
      final List<RegionInfo> newRegions) throws IOException;
  }

  protected static List<RegionInfo> createFsLayout(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, final List<RegionInfo> newRegions)
      throws IOException {
    return createFsLayout(env, tableDescriptor, newRegions, new CreateHdfsRegions() {
      @Override
      public List<RegionInfo> createHdfsRegions(final MasterProcedureEnv env,
          final Path tableRootDir, final TableName tableName,
          final List<RegionInfo> newRegions) throws IOException {
        RegionInfo[] regions = newRegions != null ?
          newRegions.toArray(new RegionInfo[newRegions.size()]) : null;
        return ModifyRegionUtils.createRegions(env.getMasterConfiguration(),
            tableRootDir, tableDescriptor, regions, null);
      }
    });
  }

  protected static List<RegionInfo> createFsLayout(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, List<RegionInfo> newRegions,
      final CreateHdfsRegions hdfsRegionHandler) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    final Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(),
      tableDescriptor.getTableName());
    ((FSTableDescriptors)(env.getMasterServices().getTableDescriptors()))
        .createTableDescriptorForTableDirectory(
          tableDir, tableDescriptor, false);

    // 2. Create Regions
    newRegions = hdfsRegionHandler.createHdfsRegions(env, mfs.getRootDir(),
            tableDescriptor.getTableName(), newRegions);

    return newRegions;
  }

  protected static List<RegionInfo> addTableToMeta(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final List<RegionInfo> regions) throws IOException {
    assert (regions != null && regions.size() > 0) : "expected at least 1 region, got " + regions;

    ProcedureSyncWait.waitMetaRegions(env);

    // Add replicas if needed
    // we need to create regions with replicaIds starting from 1
    List<RegionInfo> newRegions =
      RegionReplicaUtil.addReplicas(regions, 1, tableDescriptor.getRegionReplication());

    // Add regions to META
    addRegionsToMeta(env, tableDescriptor, newRegions);

    return newRegions;
  }

  protected static void setEnablingState(final MasterProcedureEnv env, final TableName tableName)
      throws IOException {
    // Mark the table as Enabling
    env.getMasterServices().getTableStateManager()
      .setTableState(tableName, TableState.State.ENABLING);
  }

  protected static void setEnabledState(final MasterProcedureEnv env, final TableName tableName)
      throws IOException {
    // Enable table
    env.getMasterServices().getTableStateManager()
      .setTableState(tableName, TableState.State.ENABLED);
  }

  /**
   * Add the specified set of regions to the hbase:meta table.
   */
  private static void addRegionsToMeta(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor,
      final List<RegionInfo> regionInfos) throws IOException {
    MetaTableAccessor.addRegionsToMeta(env.getMasterServices().getConnection(),
      regionInfos, tableDescriptor.getRegionReplication());
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
    // system tables are created on bootstrap internally by the system
    // the client does not know about this procedures.
    return !getTableName().isSystemTable();
  }

  RegionInfo getFirstRegionInfo() {
    if (newRegions == null || newRegions.isEmpty()) {
      return null;
    }
    return newRegions.get(0);
  }
}
