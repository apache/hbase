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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class CreateTableProcedure
    extends StateMachineProcedure<MasterProcedureEnv, CreateTableState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(CreateTableProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  // used for compatibility with old clients
  private final ProcedurePrepareLatch syncLatch;

  private HTableDescriptor hTableDescriptor;
  private List<HRegionInfo> newRegions;
  private UserGroupInformation user;

  public CreateTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    syncLatch = null;
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions)
      throws IOException {
    this(env, hTableDescriptor, newRegions, null);
  }

  public CreateTableProcedure(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions,
      final ProcedurePrepareLatch syncLatch)
      throws IOException {
    this.hTableDescriptor = hTableDescriptor;
    this.newRegions = newRegions != null ? Lists.newArrayList(newRegions) : null;
    this.user = env.getRequestUser().getUGI();
    this.setOwner(this.user.getShortUserName());

    // used for compatibility with clients without procedures
    // they need a sync TableExistsException
    this.syncLatch = syncLatch;
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
          ProcedurePrepareLatch.releaseLatch(syncLatch, this);

          if (exists) {
            assert isFailed() : "the delete should have an exception here";
            return Flow.NO_MORE_STATE;
          }

          preCreate(env);
          setNextState(CreateTableState.CREATE_TABLE_WRITE_FS_LAYOUT);
          break;
        case CREATE_TABLE_WRITE_FS_LAYOUT:
          newRegions = createFsLayout(env, hTableDescriptor, newRegions);
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
    } catch (HBaseException|IOException e) {
      LOG.error("Error trying to create table=" + getTableName() + " state=" + state, e);
      setFailure("master-create-table", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final CreateTableState state)
      throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }
    try {
      switch (state) {
        case CREATE_TABLE_POST_OPERATION:
          break;
        case CREATE_TABLE_UPDATE_DESC_CACHE:
          DeleteTableProcedure.deleteTableDescriptorCache(env, getTableName());
          break;
        case CREATE_TABLE_ASSIGN_REGIONS:
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          break;
        case CREATE_TABLE_ADD_TO_META:
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), newRegions);
          break;
        case CREATE_TABLE_WRITE_FS_LAYOUT:
          DeleteTableProcedure.deleteFromFs(env, getTableName(), newRegions, false);
          break;
        case CREATE_TABLE_PRE_OPERATION:
          DeleteTableProcedure.deleteTableStates(env, getTableName());
          // TODO-MAYBE: call the deleteTable coprocessor event?
          ProcedurePrepareLatch.releaseLatch(syncLatch, this);
          break;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (HBaseException e) {
      LOG.warn("Failed rollback attempt step=" + state + " table=" + getTableName(), e);
      throw new IOException(e);
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step=" + state + " table=" + getTableName(), e);
      throw e;
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
  protected void setNextState(final CreateTableState state) {
    if (aborted.get()) {
      setAbortFailure("create-table", "abort requested");
    } else {
      super.setNextState(state);
    }
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
  public boolean abort(final MasterProcedureEnv env) {
    aborted.set(true);
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(")");
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.CreateTableStateData.Builder state =
      MasterProcedureProtos.CreateTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(this.user))
        .setTableSchema(hTableDescriptor.convert());
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
    user = MasterProcedureUtil.toUserInfo(state.getUserInfo());
    hTableDescriptor = HTableDescriptor.convert(state.getTableSchema());
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
    if (!env.isInitialized() && !getTableName().isSystemTable()) {
      return false;
    }
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(getTableName(), "create table");
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableExclusiveLock(getTableName());
  }

  private boolean prepareCreate(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    if (MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      setFailure("master-create-table", new TableExistsException(getTableName()));
      return false;
    }
    // During master initialization, the ZK state could be inconsistent from failed DDL
    // in the past. If we fail here, it would prevent master to start.  We should force
    // setting the system table state regardless the table state.
    boolean skipTableStateCheck =
        !(env.getMasterServices().isInitialized()) && tableName.isSystemTable();
    if (!skipTableStateCheck) {
      TableStateManager tsm = env.getMasterServices().getAssignmentManager().getTableStateManager();
      if (tsm.isTableState(tableName, true, ZooKeeperProtos.Table.State.ENABLING,
          ZooKeeperProtos.Table.State.ENABLED)) {
        LOG.warn("The table " + tableName + " does not exist in meta but has a znode. " +
               "run hbck to fix inconsistencies.");
        setFailure("master-create-table", new TableExistsException(getTableName()));
        return false;
      }
    }
    return true;
  }

  private void preCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    if (!getTableName().isSystemTable()) {
      ProcedureSyncWait.getMasterQuotaManager(env)
        .checkNamespaceTableAndRegionQuota(getTableName(), newRegions.size());
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = newRegions == null ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          cpHost.preCreateTableHandler(hTableDescriptor, regions);
          return null;
        }
      });
    }
  }

  private void postCreate(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = (newRegions == null) ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          cpHost.postCreateTableHandler(hTableDescriptor, regions);
          return null;
        }
      });
    }
  }

  protected interface CreateHdfsRegions {
    List<HRegionInfo> createHdfsRegions(final MasterProcedureEnv env,
      final Path tableRootDir, final TableName tableName,
      final List<HRegionInfo> newRegions) throws IOException;
  }

  protected static List<HRegionInfo> createFsLayout(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final List<HRegionInfo> newRegions)
      throws IOException {
    return createFsLayout(env, hTableDescriptor, newRegions, new CreateHdfsRegions() {
      @Override
      public List<HRegionInfo> createHdfsRegions(final MasterProcedureEnv env,
          final Path tableRootDir, final TableName tableName,
          final List<HRegionInfo> newRegions) throws IOException {
        HRegionInfo[] regions = newRegions != null ?
          newRegions.toArray(new HRegionInfo[newRegions.size()]) : null;
        return ModifyRegionUtils.createRegions(env.getMasterConfiguration(),
            tableRootDir, hTableDescriptor, regions, null);
      }
    });
  }

  protected static List<HRegionInfo> createFsLayout(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, List<HRegionInfo> newRegions,
      final CreateHdfsRegions hdfsRegionHandler) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tempdir = mfs.getTempDir();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    final Path tempTableDir = FSUtils.getTableDir(tempdir, hTableDescriptor.getTableName());
    new FSTableDescriptors(env.getMasterConfiguration()).createTableDescriptorForTableDirectory(
      tempTableDir, hTableDescriptor, false);

    // 2. Create Regions
    newRegions = hdfsRegionHandler.createHdfsRegions(env, tempdir,
      hTableDescriptor.getTableName(), newRegions);

    // 3. Move Table temp directory to the hbase root location
    final Path tableDir = FSUtils.getTableDir(mfs.getRootDir(), hTableDescriptor.getTableName());
    FileSystem fs = mfs.getFileSystem();
    if (!fs.delete(tableDir, true) && fs.exists(tableDir)) {
      throw new IOException("Couldn't delete " + tableDir);
    }
    if (!fs.rename(tempTableDir, tableDir)) {
      throw new IOException("Unable to move table from temp=" + tempTableDir +
        " to hbase root=" + tableDir);
    }
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
      final TableName tableName, final List<HRegionInfo> regions)
      throws HBaseException, IOException {
    ProcedureSyncWait.waitRegionServers(env);

    final AssignmentManager assignmentManager = env.getMasterServices().getAssignmentManager();

    // Mark the table as Enabling
    assignmentManager.getTableStateManager().setTableState(tableName,
        ZooKeeperProtos.Table.State.ENABLING);

    // Trigger immediate assignment of the regions in round-robin fashion
    ModifyRegionUtils.assignRegions(assignmentManager, regions);

    // Enable table
    assignmentManager.getTableStateManager()
      .setTableState(tableName, ZooKeeperProtos.Table.State.ENABLED);
  }

  /**
   * Add the specified set of regions to the hbase:meta table.
   */
  protected static void addRegionsToMeta(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor,
      final List<HRegionInfo> regionInfos) throws IOException {
    MetaTableAccessor.addRegionsToMeta(env.getMasterServices().getConnection(),
      regionInfos, hTableDescriptor.getRegionReplication());
  }

  protected static void updateTableDescCache(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    env.getMasterServices().getTableDescriptors().get(tableName);
  }
}
