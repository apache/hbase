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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;

@InterfaceAudience.Private
public class CreateTableProcedure
    extends AbstractStateMachineTableProcedure<CreateTableState> {
  private static final Log LOG = LogFactory.getLog(CreateTableProcedure.class);

  private TableDescriptor tableDescriptor;
  private List<HRegionInfo> newRegions;

  public CreateTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, final HRegionInfo[] newRegions) {
    this(env, tableDescriptor, newRegions, null);
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, final HRegionInfo[] newRegions,
      final ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.tableDescriptor = tableDescriptor;
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
          newRegions = createFsLayout(env, tableDescriptor, newRegions);
          setNextState(CreateTableState.CREATE_TABLE_ADD_TO_META);
          break;
        case CREATE_TABLE_ADD_TO_META:
          newRegions = addTableToMeta(env, tableDescriptor, newRegions);
          setNextState(CreateTableState.CREATE_TABLE_ASSIGN_REGIONS);
          break;
        case CREATE_TABLE_ASSIGN_REGIONS:
          setEnablingState(env, getTableName());
          addChildProcedure(env.getAssignmentManager().createAssignProcedures(newRegions));
          setNextState(CreateTableState.CREATE_TABLE_UPDATE_DESC_CACHE);
          break;
        case CREATE_TABLE_UPDATE_DESC_CACHE:
          setEnabledState(env, getTableName());
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
      for (HRegionInfo hri: newRegions) {
        state.addRegionInfo(HRegionInfo.convert(hri));
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
        newRegions.add(HRegionInfo.convert(hri));
      }
    }
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (!getTableName().isSystemTable() && env.waitInitialized(this)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    if (env.getProcedureScheduler().waitTableExclusiveLock(this, getTableName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  private boolean prepareCreate(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    if (MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      setFailure("master-create-table", new TableExistsException(getTableName()));
      return false;
    }

    // check that we have at least 1 CF
    if (tableDescriptor.getColumnFamilyCount() == 0) {
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
      cpHost.preCreateTableAction(tableDescriptor, regions, getUser());
    }
  }

  private void postCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = (newRegions == null) ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      cpHost.postCompletedCreateTableAction(tableDescriptor, regions, getUser());
    }
  }

  protected interface CreateHdfsRegions {
    List<HRegionInfo> createHdfsRegions(final MasterProcedureEnv env,
      final Path tableRootDir, final TableName tableName,
      final List<HRegionInfo> newRegions) throws IOException;
  }

  protected static List<HRegionInfo> createFsLayout(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, final List<HRegionInfo> newRegions)
      throws IOException {
    return createFsLayout(env, tableDescriptor, newRegions, new CreateHdfsRegions() {
      @Override
      public List<HRegionInfo> createHdfsRegions(final MasterProcedureEnv env,
          final Path tableRootDir, final TableName tableName,
          final List<HRegionInfo> newRegions) throws IOException {
        HRegionInfo[] regions = newRegions != null ?
          newRegions.toArray(new HRegionInfo[newRegions.size()]) : null;
        return ModifyRegionUtils.createRegions(env.getMasterConfiguration(),
            tableRootDir, tableDescriptor, regions, null);
      }
    });
  }

  protected static List<HRegionInfo> createFsLayout(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor, List<HRegionInfo> newRegions,
      final CreateHdfsRegions hdfsRegionHandler) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tempdir = mfs.getTempDir();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    final Path tempTableDir = FSUtils.getTableDir(tempdir, tableDescriptor.getTableName());
    ((FSTableDescriptors)(env.getMasterServices().getTableDescriptors()))
        .createTableDescriptorForTableDirectory(
          tempTableDir, tableDescriptor, false);

    // 2. Create Regions
    newRegions = hdfsRegionHandler.createHdfsRegions(env, tempdir,
            tableDescriptor.getTableName(), newRegions);

    // 3. Move Table temp directory to the hbase root location
    moveTempDirectoryToHBaseRoot(env, tableDescriptor, tempTableDir);

    return newRegions;
  }

  protected static void moveTempDirectoryToHBaseRoot(
    final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor,
    final Path tempTableDir) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), tableDescriptor.getTableName());
    FileSystem fs = mfs.getFileSystem();
    if (!fs.delete(tableDir, true) && fs.exists(tableDir)) {
      throw new IOException("Couldn't delete " + tableDir);
    }
    if (!fs.rename(tempTableDir, tableDir)) {
      throw new IOException("Unable to move table from temp=" + tempTableDir +
        " to hbase root=" + tableDir);
    }
  }

  protected static List<HRegionInfo> addTableToMeta(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor,
      final List<HRegionInfo> regions) throws IOException {
    assert (regions != null && regions.size() > 0) : "expected at least 1 region, got " + regions;

    ProcedureSyncWait.waitMetaRegions(env);

    // Add replicas if needed
    List<HRegionInfo> newRegions = addReplicas(env, tableDescriptor, regions);

    // Add regions to META
    addRegionsToMeta(env, tableDescriptor, newRegions);

    // Setup replication for region replicas if needed
    if (tableDescriptor.getRegionReplication() > 1) {
      ServerRegionReplicaUtil.setupRegionReplicaReplication(env.getMasterConfiguration());
    }
    return newRegions;
  }

  /**
   * Create any replicas for the regions (the default replicas that was
   * already created is passed to the method)
   * @param tableDescriptor descriptor to use
   * @param regions default replicas
   * @return the combined list of default and non-default replicas
   */
  private static List<HRegionInfo> addReplicas(final MasterProcedureEnv env,
      final TableDescriptor tableDescriptor,
      final List<HRegionInfo> regions) {
    int numRegionReplicas = tableDescriptor.getRegionReplication() - 1;
    if (numRegionReplicas <= 0) {
      return regions;
    }
    List<HRegionInfo> hRegionInfos = new ArrayList<>((numRegionReplicas+1)*regions.size());
    for (int i = 0; i < regions.size(); i++) {
      for (int j = 1; j <= numRegionReplicas; j++) {
        hRegionInfos.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(i), j));
      }
    }
    hRegionInfos.addAll(regions);
    return hRegionInfos;
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
      final List<HRegionInfo> regionInfos) throws IOException {
    MetaTableAccessor.addRegionsToMeta(env.getMasterServices().getConnection(),
      regionInfos, tableDescriptor.getRegionReplication());
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
