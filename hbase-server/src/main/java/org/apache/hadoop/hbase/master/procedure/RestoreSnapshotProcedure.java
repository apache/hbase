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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.RestoreSnapshotState;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Private
public class RestoreSnapshotProcedure
    extends StateMachineProcedure<MasterProcedureEnv, RestoreSnapshotState>
    implements TableProcedureInterface {
  private static final Log LOG = LogFactory.getLog(RestoreSnapshotProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);

  private HTableDescriptor modifiedHTableDescriptor;
  private List<HRegionInfo> regionsToRestore = null;
  private List<HRegionInfo> regionsToRemove = null;
  private List<HRegionInfo> regionsToAdd = null;
  private Map<String, Pair<String, String>> parentsToChildrenPairMap =
    new HashMap<String, Pair<String, String>>();

  private UserGroupInformation user;
  private SnapshotDescription snapshot;

  // Monitor
  private MonitoredTask monitorStatus = null;

  private Boolean traceEnabled = null;

  /**
   * Constructor (for failover)
   */
  public RestoreSnapshotProcedure() {
  }

  /**
   * Constructor
   * @param env MasterProcedureEnv
   * @param hTableDescriptor the table to operate on
   * @param snapshot snapshot to restore from
   * @throws IOException
   */
  public RestoreSnapshotProcedure(
      final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor,
      final SnapshotDescription snapshot)
      throws IOException {
    // This is the new schema we are going to write out as this modification.
    this.modifiedHTableDescriptor = hTableDescriptor;
    // Snapshot information
    this.snapshot = snapshot;
    // User and owner information
    this.user = env.getRequestUser().getUGI();
    this.setOwner(this.user.getShortUserName());

    // Monitor
    getMonitorStatus();
  }

  /**
   * Set up monitor status if it is not created.
   */
  private MonitoredTask getMonitorStatus() {
    if (monitorStatus == null) {
      monitorStatus = TaskMonitor.get().createStatus("Restoring  snapshot '" + snapshot.getName()
        + "' to table " + getTableName());
    }
    return monitorStatus;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final RestoreSnapshotState state)
      throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    // Make sure that the monitor status is set up
    getMonitorStatus();

    try {
      switch (state) {
        case RESTORE_SNAPSHOT_PRE_OPERATION:
          // Verify if we can restore the table
          prepareRestore(env);
          setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_UPDATE_TABLE_DESCRIPTOR);
          break;
        case RESTORE_SNAPSHOT_UPDATE_TABLE_DESCRIPTOR:
          updateTableDescriptor(env);
          setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_WRITE_FS_LAYOUT);
          break;
        case RESTORE_SNAPSHOT_WRITE_FS_LAYOUT:
          restoreSnapshot(env);
          setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_UPDATE_META);
          break;
        case RESTORE_SNAPSHOT_UPDATE_META:
          updateMETA(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.error("Error trying to restore snapshot=" + getTableName() + " state=" + state, e);
      setFailure("master-restore-snapshot", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final RestoreSnapshotState state)
      throws IOException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }

    if (state == RestoreSnapshotState.RESTORE_SNAPSHOT_PRE_OPERATION) {
      // nothing to rollback
      return;
    }

    // The restore snapshot doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected RestoreSnapshotState getState(final int stateId) {
    return RestoreSnapshotState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final RestoreSnapshotState state) {
    return state.getNumber();
  }

  @Override
  protected RestoreSnapshotState getInitialState() {
    return RestoreSnapshotState.RESTORE_SNAPSHOT_PRE_OPERATION;
  }

  @Override
  protected void setNextState(final RestoreSnapshotState state) {
    if (aborted.get()) {
      setAbortFailure("create-table", "abort requested");
    } else {
      super.setNextState(state);
    }
  }

  @Override
  public TableName getTableName() {
    return modifiedHTableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT; // Restore is modifying a table
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

    MasterProcedureProtos.RestoreSnapshotStateData.Builder restoreSnapshotMsg =
      MasterProcedureProtos.RestoreSnapshotStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(this.user))
        .setSnapshot(this.snapshot)
        .setModifiedTableSchema(ProtobufUtil.convertToTableSchema(modifiedHTableDescriptor));

    if (regionsToRestore != null) {
      for (HRegionInfo hri: regionsToRestore) {
        restoreSnapshotMsg.addRegionInfoForRestore(HRegionInfo.convert(hri));
      }
    }
    if (regionsToRemove != null) {
      for (HRegionInfo hri: regionsToRemove) {
        restoreSnapshotMsg.addRegionInfoForRemove(HRegionInfo.convert(hri));
      }
    }
    if (regionsToAdd != null) {
      for (HRegionInfo hri: regionsToAdd) {
        restoreSnapshotMsg.addRegionInfoForAdd(HRegionInfo.convert(hri));
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
        restoreSnapshotMsg.addParentToChildRegionsPairList (parentToChildrenPair);
      }
    }
    restoreSnapshotMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.RestoreSnapshotStateData restoreSnapshotMsg =
      MasterProcedureProtos.RestoreSnapshotStateData.parseDelimitedFrom(stream);
    user = MasterProcedureUtil.toUserInfo(restoreSnapshotMsg.getUserInfo());
    snapshot = restoreSnapshotMsg.getSnapshot();
    modifiedHTableDescriptor =
      ProtobufUtil.convertToHTableDesc(restoreSnapshotMsg.getModifiedTableSchema());

    if (restoreSnapshotMsg.getRegionInfoForRestoreCount() == 0) {
      regionsToRestore = null;
    } else {
      regionsToRestore =
        new ArrayList<HRegionInfo>(restoreSnapshotMsg.getRegionInfoForRestoreCount());
      for (HBaseProtos.RegionInfo hri: restoreSnapshotMsg.getRegionInfoForRestoreList()) {
        regionsToRestore.add(HRegionInfo.convert(hri));
      }
    }
    if (restoreSnapshotMsg.getRegionInfoForRemoveCount() == 0) {
      regionsToRemove = null;
    } else {
      regionsToRemove =
        new ArrayList<HRegionInfo>(restoreSnapshotMsg.getRegionInfoForRemoveCount());
      for (HBaseProtos.RegionInfo hri: restoreSnapshotMsg.getRegionInfoForRemoveList()) {
        regionsToRemove.add(HRegionInfo.convert(hri));
      }
    }
    if (restoreSnapshotMsg.getRegionInfoForAddCount() == 0) {
      regionsToAdd = null;
    } else {
      regionsToAdd = new ArrayList<HRegionInfo>(restoreSnapshotMsg.getRegionInfoForAddCount());
      for (HBaseProtos.RegionInfo hri: restoreSnapshotMsg.getRegionInfoForAddList()) {
        regionsToAdd.add(HRegionInfo.convert(hri));
      }
    }
    if (restoreSnapshotMsg.getParentToChildRegionsPairListCount() > 0) {
      for (MasterProcedureProtos.RestoreParentToChildRegionsPair parentToChildrenPair:
        restoreSnapshotMsg.getParentToChildRegionsPairListList()) {
        parentsToChildrenPairMap.put(
          parentToChildrenPair.getParentRegionName(),
          new Pair<String, String>(
            parentToChildrenPair.getChild1RegionName(),
            parentToChildrenPair.getChild2RegionName()));
      }
    }
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
   * Action before any real action of restoring from snapshot.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareRestore(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    // Checks whether the table exists
    if (!MetaTableAccessor.tableExists(env.getMasterServices().getConnection(), tableName)) {
      throw new TableNotFoundException(tableName);
    }

    // Check whether table is disabled.
    env.getMasterServices().checkTableModifiable(tableName);

    // Check that we have at least 1 CF
    if (modifiedHTableDescriptor.getColumnFamilies().length == 0) {
      throw new DoNotRetryIOException("Table " + getTableName().toString() +
        " should have at least one column family.");
    }

    if (!getTableName().isSystemTable()) {
      // Table already exist. Check and update the region quota for this table namespace.
      final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
      SnapshotManifest manifest = SnapshotManifest.open(
        env.getMasterConfiguration(),
        mfs.getFileSystem(),
        SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, mfs.getRootDir()),
        snapshot);
      int snapshotRegionCount = manifest.getRegionManifestsMap().size();
      int tableRegionCount =
          ProcedureSyncWait.getMasterQuotaManager(env).getRegionCountOfTable(tableName);

      if (snapshotRegionCount > 0 && tableRegionCount != snapshotRegionCount) {
        ProcedureSyncWait.getMasterQuotaManager(env).checkAndUpdateNamespaceRegionQuota(
          tableName, snapshotRegionCount);
      }
    }
  }

  /**
   * Update descriptor
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void updateTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().add(modifiedHTableDescriptor);
  }

  /**
   * Execute the on-disk Restore
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void restoreSnapshot(final MasterProcedureEnv env) throws IOException {
    MasterFileSystem fileSystemManager = env.getMasterServices().getMasterFileSystem();
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    final ForeignExceptionDispatcher monitorException = new ForeignExceptionDispatcher();

    LOG.info("Starting restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot));
    try {
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      SnapshotManifest manifest = SnapshotManifest.open(
        env.getMasterServices().getConfiguration(), fs, snapshotDir, snapshot);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(
        env.getMasterServices().getConfiguration(),
        fs,
        manifest,
        modifiedHTableDescriptor,
        rootDir,
        monitorException,
        getMonitorStatus());

      RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();
      regionsToRestore = metaChanges.getRegionsToRestore();
      regionsToRemove = metaChanges.getRegionsToRemove();
      regionsToAdd = metaChanges.getRegionsToAdd();
      parentsToChildrenPairMap = metaChanges.getParentToChildrenPairMap();
    } catch (IOException e) {
      String msg = "restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
        + " failed in on-disk restore. Try re-running the restore command.";
      LOG.error(msg, e);
      monitorException.receive(
        new ForeignException(env.getMasterServices().getServerName().toString(), e));
      throw new IOException(msg, e);
    }
  }

  /**
   * Apply changes to hbase:meta
   * @param env MasterProcedureEnv
   * @throws IOException
   **/
  private void updateMETA(final MasterProcedureEnv env) throws IOException {
    try {
      Connection conn = env.getMasterServices().getConnection();

      // 1. Forces all the RegionStates to be offline
      //
      // The AssignmentManager keeps all the region states around
      // with no possibility to remove them, until the master is restarted.
      // This means that a region marked as SPLIT before the restore will never be assigned again.
      // To avoid having all states around all the regions are switched to the OFFLINE state,
      // which is the same state that the regions will be after a delete table.
      forceRegionsOffline(env, regionsToAdd);
      forceRegionsOffline(env, regionsToRestore);
      forceRegionsOffline(env, regionsToRemove);

      getMonitorStatus().setStatus("Preparing to restore each region");

      // 2. Applies changes to hbase:meta
      // (2.1). Removes the current set of regions from META
      //
      // By removing also the regions to restore (the ones present both in the snapshot
      // and in the current state) we ensure that no extra fields are present in META
      // e.g. with a simple add addRegionToMeta() the splitA and splitB attributes
      // not overwritten/removed, so you end up with old informations
      // that are not correct after the restore.
      if (regionsToRemove != null) {
        MetaTableAccessor.deleteRegions(conn, regionsToRemove);
      }

      // (2.2). Add the new set of regions to META
      //
      // At this point the old regions are no longer present in META.
      // and the set of regions present in the snapshot will be written to META.
      // All the information in hbase:meta are coming from the .regioninfo of each region present
      // in the snapshot folder.
      if (regionsToAdd != null) {
        MetaTableAccessor.addRegionsToMeta(
          conn,
          regionsToAdd,
          modifiedHTableDescriptor.getRegionReplication());
      }

      if (regionsToRestore != null) {
        MetaTableAccessor.overwriteRegions(
          conn,
          regionsToRestore,
          modifiedHTableDescriptor.getRegionReplication());
      }

      RestoreSnapshotHelper.RestoreMetaChanges metaChanges =
        new RestoreSnapshotHelper.RestoreMetaChanges(
          modifiedHTableDescriptor, parentsToChildrenPairMap);
      metaChanges.updateMetaParentRegions(conn, regionsToAdd);

      // At this point the restore is complete.
      LOG.info("Restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot) +
        " on table=" + getTableName() + " completed!");
    } catch (IOException e) {
      final ForeignExceptionDispatcher monitorException = new ForeignExceptionDispatcher();
      String msg = "restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " failed in meta update. Try re-running the restore command.";
      LOG.error(msg, e);
      monitorException.receive(
        new ForeignException(env.getMasterServices().getServerName().toString(), e));
      throw new IOException(msg, e);
    }

    monitorStatus.markComplete("Restore snapshot '"+ snapshot.getName() +"'!");
    MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
    metricsSnapshot.addSnapshotRestore(
      monitorStatus.getCompletionTimestamp() - monitorStatus.getStartTime());
  }

  /**
   * Make sure that region states of the region list is in OFFLINE state.
   * @param env MasterProcedureEnv
   * @param hris region info list
   **/
  private void forceRegionsOffline(final MasterProcedureEnv env, final List<HRegionInfo> hris) {
    RegionStates states = env.getMasterServices().getAssignmentManager().getRegionStates();
    if (hris != null) {
      for (HRegionInfo hri: hris) {
        states.regionOffline(hri);
      }
    }
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
