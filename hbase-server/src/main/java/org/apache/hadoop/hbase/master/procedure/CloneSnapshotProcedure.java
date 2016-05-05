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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure.CreateHdfsRegions;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CloneSnapshotState;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
public class CloneSnapshotProcedure
    extends StateMachineProcedure<MasterProcedureEnv, CloneSnapshotState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(CloneSnapshotProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  private UserGroupInformation user;
  private HTableDescriptor hTableDescriptor;
  private SnapshotDescription snapshot;
  private List<HRegionInfo> newRegions = null;
  private Map<String, Pair<String, String> > parentsToChildrenPairMap =
    new HashMap<String, Pair<String, String>>();

  // Monitor
  private MonitoredTask monitorStatus = null;

  private Boolean traceEnabled = null;

  /**
   * Constructor (for failover)
   */
  public CloneSnapshotProcedure() {
  }

  /**
   * Constructor
   * @param env MasterProcedureEnv
   * @param hTableDescriptor the table to operate on
   * @param snapshot snapshot to clone from
   * @throws IOException
   */
  public CloneSnapshotProcedure(
      final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor,
      final SnapshotDescription snapshot)
      throws IOException {
    this.hTableDescriptor = hTableDescriptor;
    this.snapshot = snapshot;
    this.user = env.getRequestUser().getUGI();
    this.setOwner(this.user.getShortUserName());

    getMonitorStatus();
  }

  /**
   * Set up monitor status if it is not created.
   */
  private MonitoredTask getMonitorStatus() {
    if (monitorStatus == null) {
      monitorStatus = TaskMonitor.get().createStatus("Cloning  snapshot '" + snapshot.getName() +
        "' to table " + getTableName());
    }
    return monitorStatus;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final CloneSnapshotState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case CLONE_SNAPSHOT_PRE_OPERATION:
          // Verify if we can clone the table
          prepareClone(env);

          preCloneSnapshot(env);
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_WRITE_FS_LAYOUT);
          break;
        case CLONE_SNAPSHOT_WRITE_FS_LAYOUT:
          newRegions = createFilesystemLayout(env, hTableDescriptor, newRegions);
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_ADD_TO_META);
          break;
        case CLONE_SNAPSHOT_ADD_TO_META:
          addRegionsToMeta(env);
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_ASSIGN_REGIONS);
          break;
        case CLONE_SNAPSHOT_ASSIGN_REGIONS:
          CreateTableProcedure.assignRegions(env, getTableName(), newRegions);
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_UPDATE_DESC_CACHE);
          break;
        case CLONE_SNAPSHOT_UPDATE_DESC_CACHE:
          CreateTableProcedure.updateTableDescCache(env, getTableName());
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_POST_OPERATION);
          break;
        case CLONE_SNAPSHOT_POST_OPERATION:
          postCloneSnapshot(env);

          MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
          metricsSnapshot.addSnapshotClone(
            getMonitorStatus().getCompletionTimestamp() - getMonitorStatus().getStartTime());
          getMonitorStatus().markComplete("Clone snapshot '"+ snapshot.getName() +"' completed!");
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.error("Error trying to create table=" + getTableName() + " state=" + state, e);
      setFailure("master-create-table", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final CloneSnapshotState state)
      throws IOException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }
    try {
      switch (state) {
        case CLONE_SNAPSHOT_POST_OPERATION:
          // TODO-MAYBE: call the deleteTable coprocessor event?
          break;
        case CLONE_SNAPSHOT_UPDATE_DESC_CACHE:
          DeleteTableProcedure.deleteTableDescriptorCache(env, getTableName());
          break;
        case CLONE_SNAPSHOT_ASSIGN_REGIONS:
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          break;
        case CLONE_SNAPSHOT_ADD_TO_META:
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), newRegions);
          break;
        case CLONE_SNAPSHOT_WRITE_FS_LAYOUT:
          DeleteTableProcedure.deleteFromFs(env, getTableName(), newRegions, false);
          break;
        case CLONE_SNAPSHOT_PRE_OPERATION:
          DeleteTableProcedure.deleteTableStates(env, getTableName());
          // TODO-MAYBE: call the deleteTable coprocessor event?
          break;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step=" + state + " table=" + getTableName(), e);
      throw e;
    }
  }

  @Override
  protected CloneSnapshotState getState(final int stateId) {
    return CloneSnapshotState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final CloneSnapshotState state) {
    return state.getNumber();
  }

  @Override
  protected CloneSnapshotState getInitialState() {
    return CloneSnapshotState.CLONE_SNAPSHOT_PRE_OPERATION;
  }

  @Override
  protected void setNextState(final CloneSnapshotState state) {
    if (aborted.get()) {
      setAbortFailure("clone-snapshot", "abort requested");
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
    return TableOperationType.CREATE; // Clone is creating a table
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
    sb.append(" snapshot=");
    sb.append(snapshot);
    sb.append(")");
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.CloneSnapshotStateData.Builder cloneSnapshotMsg =
      MasterProcedureProtos.CloneSnapshotStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(this.user))
        .setSnapshot(this.snapshot)
        .setTableSchema(ProtobufUtil.convertToTableSchema(hTableDescriptor));
    if (newRegions != null) {
      for (HRegionInfo hri: newRegions) {
        cloneSnapshotMsg.addRegionInfo(HRegionInfo.convert(hri));
      }
    }
    if (!parentsToChildrenPairMap.isEmpty()) {
      final Iterator<Map.Entry<String, Pair<String, String>>> it =
        parentsToChildrenPairMap.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<String, Pair<String, String>> entry = it.next();

        MasterProcedureProtos.RestoreParentToChildRegionsPair.Builder parentToChildrenPair =
          MasterProcedureProtos.RestoreParentToChildRegionsPair.newBuilder()
          .setParentRegionName(entry.getKey())
          .setChild1RegionName(entry.getValue().getFirst())
          .setChild2RegionName(entry.getValue().getSecond());
        cloneSnapshotMsg.addParentToChildRegionsPairList(parentToChildrenPair);
      }
    }
    cloneSnapshotMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.CloneSnapshotStateData cloneSnapshotMsg =
      MasterProcedureProtos.CloneSnapshotStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(cloneSnapshotMsg.getUserInfo());
    snapshot = cloneSnapshotMsg.getSnapshot();
    hTableDescriptor = ProtobufUtil.convertToHTableDesc(cloneSnapshotMsg.getTableSchema());
    if (cloneSnapshotMsg.getRegionInfoCount() == 0) {
      newRegions = null;
    } else {
      newRegions = new ArrayList<HRegionInfo>(cloneSnapshotMsg.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri: cloneSnapshotMsg.getRegionInfoList()) {
        newRegions.add(HRegionInfo.convert(hri));
      }
    }
    if (cloneSnapshotMsg.getParentToChildRegionsPairListCount() > 0) {
      parentsToChildrenPairMap = new HashMap<String, Pair<String, String>>();
      for (MasterProcedureProtos.RestoreParentToChildRegionsPair parentToChildrenPair:
        cloneSnapshotMsg.getParentToChildRegionsPairListList()) {
        parentsToChildrenPairMap.put(
          parentToChildrenPair.getParentRegionName(),
          new Pair<String, String>(
            parentToChildrenPair.getChild1RegionName(),
            parentToChildrenPair.getChild2RegionName()));
      }
    }
    // Make sure that the monitor status is set up
    getMonitorStatus();
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) {
      return false;
    }
    return env.getProcedureQueue().tryAcquireTableExclusiveLock(this, getTableName());
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().releaseTableExclusiveLock(this, getTableName());
  }

  /**
   * Action before any real action of cloning from snapshot.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareClone(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    if (MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      throw new TableExistsException(getTableName());
    }
  }

  /**
   * Action before cloning from snapshot.
   * @param env MasterProcedureEnv
   * @throws IOException
   * @throws InterruptedException
   */
  private void preCloneSnapshot(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    if (!getTableName().isSystemTable()) {
      // Check and update namespace quota
      final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();

      SnapshotManifest manifest = SnapshotManifest.open(
        env.getMasterConfiguration(),
        mfs.getFileSystem(),
        SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, mfs.getRootDir()),
        snapshot);

      ProcedureSyncWait.getMasterQuotaManager(env)
        .checkNamespaceTableAndRegionQuota(getTableName(), manifest.getRegionManifestsMap().size());
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          cpHost.preCreateTableAction(hTableDescriptor, null);
          return null;
        }
      });
    }
  }

  /**
   * Action after cloning from snapshot.
   * @param env MasterProcedureEnv
   * @throws IOException
   * @throws InterruptedException
   */
  private void postCloneSnapshot(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final HRegionInfo[] regions = (newRegions == null) ? null :
        newRegions.toArray(new HRegionInfo[newRegions.size()]);
      user.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          cpHost.postCompletedCreateTableAction(hTableDescriptor, regions);
          return null;
        }
      });
    }
  }

  /**
   * Create regions in file system.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private List<HRegionInfo> createFilesystemLayout(
    final MasterProcedureEnv env,
    final HTableDescriptor hTableDescriptor,
    final List<HRegionInfo> newRegions) throws IOException {
    return createFsLayout(env, hTableDescriptor, newRegions, new CreateHdfsRegions() {
      @Override
      public List<HRegionInfo> createHdfsRegions(
        final MasterProcedureEnv env,
        final Path tableRootDir, final TableName tableName,
        final List<HRegionInfo> newRegions) throws IOException {

        final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
        final FileSystem fs = mfs.getFileSystem();
        final Path rootDir = mfs.getRootDir();
        final Configuration conf = env.getMasterConfiguration();
        final ForeignExceptionDispatcher monitorException = new ForeignExceptionDispatcher();

        getMonitorStatus().setStatus("Clone snapshot - creating regions for table: " + tableName);

        try {
          // 1. Execute the on-disk Clone
          Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
          SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshot);
          RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(
            conf, fs, manifest, hTableDescriptor, tableRootDir, monitorException, monitorStatus);
          RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();

          // Clone operation should not have stuff to restore or remove
          Preconditions.checkArgument(
            !metaChanges.hasRegionsToRestore(), "A clone should not have regions to restore");
          Preconditions.checkArgument(
            !metaChanges.hasRegionsToRemove(), "A clone should not have regions to remove");

          // At this point the clone is complete. Next step is enabling the table.
          String msg =
            "Clone snapshot="+ snapshot.getName() +" on table=" + tableName + " completed!";
          LOG.info(msg);
          monitorStatus.setStatus(msg + " Waiting for table to be enabled...");

          // 2. Let the next step to add the regions to meta
          return metaChanges.getRegionsToAdd();
        } catch (Exception e) {
          String msg = "clone snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot) +
            " failed because " + e.getMessage();
          LOG.error(msg, e);
          IOException rse = new RestoreSnapshotException(msg, e, snapshot);

          // these handlers aren't futures so we need to register the error here.
          monitorException.receive(new ForeignException("Master CloneSnapshotProcedure", rse));
          throw rse;
        }
      }
    });
  }

  /**
   * Create region layout in file system.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private List<HRegionInfo> createFsLayout(
    final MasterProcedureEnv env,
    final HTableDescriptor hTableDescriptor,
    List<HRegionInfo> newRegions,
    final CreateHdfsRegions hdfsRegionHandler) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tempdir = mfs.getTempDir();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    TableDescriptor underConstruction = new TableDescriptor(hTableDescriptor);
    final Path tempTableDir = FSUtils.getTableDir(tempdir, hTableDescriptor.getTableName());
    ((FSTableDescriptors)(env.getMasterServices().getTableDescriptors()))
      .createTableDescriptorForTableDirectory(tempTableDir, underConstruction, false);

    // 2. Create Regions
    newRegions = hdfsRegionHandler.createHdfsRegions(
      env, tempdir, hTableDescriptor.getTableName(), newRegions);

    // 3. Move Table temp directory to the hbase root location
    CreateTableProcedure.moveTempDirectoryToHBaseRoot(env, hTableDescriptor, tempTableDir);

    return newRegions;
  }

  /**
   * Add regions to hbase:meta table.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void addRegionsToMeta(final MasterProcedureEnv env) throws IOException {
    newRegions = CreateTableProcedure.addTableToMeta(env, hTableDescriptor, newRegions);

    RestoreSnapshotHelper.RestoreMetaChanges metaChanges =
        new RestoreSnapshotHelper.RestoreMetaChanges(
          hTableDescriptor, parentsToChildrenPairMap);
    metaChanges.updateMetaParentRegions(env.getMasterServices().getConnection(), newRegions);
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
}
