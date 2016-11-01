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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.StorageIdentifier;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure.CreateStorageRegions;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CloneSnapshotState;
import org.apache.hadoop.hbase.snapshot.SnapshotRestoreMetaChanges;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
public class CloneSnapshotProcedure
    extends AbstractStateMachineTableProcedure<CloneSnapshotState> {
  private static final Log LOG = LogFactory.getLog(CloneSnapshotProcedure.class);

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
   */
  public CloneSnapshotProcedure(final MasterProcedureEnv env,
      final HTableDescriptor hTableDescriptor, final SnapshotDescription snapshot) {
    super(env);
    this.hTableDescriptor = hTableDescriptor;
    this.snapshot = snapshot;

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
          newRegions = createStorageLayout(env, hTableDescriptor, newRegions);
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
      if (isRollbackSupported(state)) {
        setFailure("master-clone-snapshot", e);
      } else {
        LOG.warn("Retriable error trying to clone snapshot=" + snapshot.getName() +
          " to table=" + getTableName() + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final CloneSnapshotState state)
      throws IOException {
    if (state == CloneSnapshotState.CLONE_SNAPSHOT_PRE_OPERATION) {
      DeleteTableProcedure.deleteTableStates(env, getTableName());
      // TODO-MAYBE: call the deleteTable coprocessor event?
      return;
    }

    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final CloneSnapshotState state) {
    switch (state) {
      case CLONE_SNAPSHOT_PRE_OPERATION:
        return true;
      default:
        return false;
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
  public TableName getTableName() {
    return hTableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.CREATE; // Clone is creating a table
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
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
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
    setUser(MasterProcedureUtil.toUserInfo(cloneSnapshotMsg.getUserInfo()));
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
      final MasterStorage masterStorage = env.getMasterServices().getMasterStorage();

      ProcedureSyncWait.getMasterQuotaManager(env).checkNamespaceTableAndRegionQuota(getTableName(),
          masterStorage.getSnapshotRegions(snapshot).size());
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preCreateTableAction(hTableDescriptor, null, getUser());
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
      cpHost.postCompletedCreateTableAction(hTableDescriptor, regions, getUser());
    }
  }

  /**
   * Create regions on storage.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private List<HRegionInfo> createStorageLayout(
    final MasterProcedureEnv env,
    final HTableDescriptor hTableDescriptor,
    final List<HRegionInfo> newRegions) throws IOException {
    return createTableOnStorage(env, hTableDescriptor, newRegions, new CreateStorageRegions() {
      @Override
      public List<HRegionInfo> createRegionsOnStorage(
          final MasterProcedureEnv env,
          final TableName tableName,
          final List<HRegionInfo> newRegions) throws IOException {

        final Configuration conf = env.getMasterConfiguration();
        final ForeignExceptionDispatcher monitorException = new ForeignExceptionDispatcher();

        getMonitorStatus().setStatus("Clone snapshot - creating regions for table: " + tableName);

        try {
          // 1. Execute the on-disk Clone
          MasterStorage<? extends StorageIdentifier> masterStorage =
              env.getMasterServices().getMasterStorage();
          SnapshotRestoreMetaChanges metaChanges = masterStorage.restoreSnapshot(snapshot,
              hTableDescriptor, monitorException, monitorStatus);

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
          IOException rse = new RestoreSnapshotException(msg, e,
              ProtobufUtil.createSnapshotDesc(snapshot));

          // these handlers aren't futures so we need to register the error here.
          monitorException.receive(new ForeignException("Master CloneSnapshotProcedure", rse));
          throw rse;
        }
      }
    });
  }

  /**
   * Create region layout on storage.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private List<HRegionInfo> createTableOnStorage(
    final MasterProcedureEnv env,
    final HTableDescriptor hTableDescriptor,
    List<HRegionInfo> newRegions,
    final CreateStorageRegions storageRegionHandler) throws IOException {
    final MasterStorage masterStorage = env.getMasterServices().getMasterStorage();

    // 1. Delete existing storage artifacts (dir, files etc) for the table
    masterStorage.deleteTable(hTableDescriptor.getTableName());

    // 2. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    HTableDescriptor underConstruction = new HTableDescriptor(hTableDescriptor);
    masterStorage.createTableDescriptor(underConstruction, true);

    // 3. Create Regions
    newRegions = storageRegionHandler.createRegionsOnStorage(env, hTableDescriptor.getTableName(),
        newRegions);

    return newRegions;
  }

  /**
   * Add regions to hbase:meta table.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void addRegionsToMeta(final MasterProcedureEnv env) throws IOException {
    newRegions = CreateTableProcedure.addTableToMeta(env, hTableDescriptor, newRegions);

    SnapshotRestoreMetaChanges metaChanges =
        new SnapshotRestoreMetaChanges(hTableDescriptor, parentsToChildrenPairMap);
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
