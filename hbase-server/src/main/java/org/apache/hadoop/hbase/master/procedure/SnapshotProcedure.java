/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure;
import org.apache.hadoop.hbase.master.snapshot.MasterSnapshotVerifier;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 *  A procedure used to take snapshot on tables.
 */
@InterfaceAudience.Private
public class SnapshotProcedure
    extends AbstractStateMachineTableProcedure<SnapshotState> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotProcedure.class);
  private final MetricsSnapshot metricsSnapshot = new MetricsSnapshot();

  private Configuration conf;
  private SnapshotDescription snapshot;
  private Path rootDir;
  private Path snapshotDir;
  private Path workingDir;
  private FileSystem workingDirFS;
  private FileSystem rootFs;
  private TableName snapshotTable;
  private MonitoredTask status;
  private SnapshotManifest snapshotManifest;
  private TableDescriptor htd;

  private RetryCounter retryCounter;

  public SnapshotProcedure() { }

  public SnapshotProcedure(final MasterProcedureEnv env, final SnapshotDescription snapshot) {
    super(env);
    this.snapshot = snapshot;
  }

  @Override
  public TableName getTableName() {
    return TableName.valueOf(snapshot.getTable());
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.SNAPSHOT;
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    // AbstractStateMachineTableProcedure acquires exclusive table lock by default,
    // but we may need to downgrade it to shared lock for some reasons:
    // a. exclusive lock has a negative effect on assigning region. See HBASE-21480 for details.
    // b. we want to support taking multiple different snapshots on same table on the same time.
    if (env.getProcedureScheduler().waitTableSharedLock(this, getTableName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeTableSharedLock(this, getTableName());
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    // In order to avoid enabling/disabling/modifying/deleting table during snapshot,
    // we don't release lock during suspend
    return true;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, SnapshotState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.info("{} execute state={}", this, state);

    try {
      switch (state) {
        case SNAPSHOT_PREPARE:
          prepareSnapshot(env);
          setNextState(SnapshotState.SNAPSHOT_PRE_OPERATION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_PRE_OPERATION:
          preSnapshot(env);
          setNextState(SnapshotState.SNAPSHOT_WRITE_SNAPSHOT_INFO);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_WRITE_SNAPSHOT_INFO:
          SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, workingDir, workingDirFS);
          TableState tableState =
            env.getMasterServices().getTableStateManager().getTableState(snapshotTable);
          if (tableState.isEnabled()) {
            setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_ONLINE_REGIONS);
          } else if (tableState.isDisabled()) {
            setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_CLOSED_REGIONS);
          }
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_ONLINE_REGIONS:
          addChildProcedure(createRemoteSnapshotProcedures(env));
          setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_SPLIT_REGIONS);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_SPLIT_REGIONS:
          snapshotSplitRegions(env);
          setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_MOB_REGION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_CLOSED_REGIONS:
          snapshotClosedRegions(env);
          setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_MOB_REGION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_MOB_REGION:
          snapshotMobRegion(env);
          setNextState(SnapshotState.SNAPSHOT_CONSOLIDATE_SNAPSHOT);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_CONSOLIDATE_SNAPSHOT:
          // flush the in-memory state, and write the single manifest
          status.setStatus("Consolidate snapshot: " + snapshot.getName());
          snapshotManifest.consolidate();
          setNextState(SnapshotState.SNAPSHOT_VERIFIER_SNAPSHOT);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_VERIFIER_SNAPSHOT:
          status.setStatus("Verifying snapshot: " + snapshot.getName());
          verifySnapshot(env);
          setNextState(SnapshotState.SNAPSHOT_COMPLETE_SNAPSHOT);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_COMPLETE_SNAPSHOT:
          if (isSnapshotCorrupted()) {
            throw new CorruptedSnapshotException(snapshot.getName());
          }
          completeSnapshot(env);
          setNextState(SnapshotState.SNAPSHOT_POST_OPERATION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_POST_OPERATION:
          postSnapshot(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (ProcedureSuspendedException e) {
      throw e;
    } catch (Exception e) {
      setFailure("master-snapshot", e);
      LOG.warn("unexpected exception while execute {}. Mark procedure Failed.", this, e);
      status.abort("Abort Snapshot " + snapshot.getName() + " on Table " + snapshotTable);
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, SnapshotState state)
    throws IOException, InterruptedException {
    if (state == SnapshotState.SNAPSHOT_PRE_OPERATION) {
      try {
        if (!workingDirFS.delete(workingDir, true)) {
          LOG.error("Couldn't delete snapshot working directory {}", workingDir);
        }
      } catch (IOException e) {
        LOG.error("Couldn't delete snapshot working directory {}", workingDir, e);
      }
    }
  }

  @Override
  protected boolean isRollbackSupported(SnapshotState state) {
    return true;
  }

  @Override
  protected SnapshotState getState(final int stateId) {
    return SnapshotState.forNumber(stateId);
  }

  @Override
  protected int getStateId(SnapshotState state) {
    return state.getNumber();
  }

  @Override
  protected SnapshotState getInitialState() {
    return SnapshotState.SNAPSHOT_PREPARE;
  }

  private void prepareSnapshot(MasterProcedureEnv env)
    throws ProcedureSuspendedException, IOException {
    if (isAnySplitOrMergeProcedureRunning(env)) {
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn("{} waits {} ms for Split/Merge procedure to finish", this, backoff);
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
    prepareSnapshotEnv(env);
  }

  private void prepareSnapshotEnv(MasterProcedureEnv env) throws IOException {
    this.conf = env.getMasterConfiguration();
    this.snapshotTable = TableName.valueOf(snapshot.getTable());
    this.htd = loadTableDescriptorSnapshot(env);
    this.rootFs = env.getMasterFileSystem().getFileSystem();
    this.rootDir = CommonFSUtils.getRootDir(conf);
    this.snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    this.workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir, conf);
    this.workingDirFS = workingDir.getFileSystem(conf);
    this.status = TaskMonitor.get()
      .createStatus("Taking " + snapshot.getType() + " snapshot on table: " + snapshotTable);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(snapshot.getName());
    this.snapshotManifest = SnapshotManifest.create(conf,
      rootFs, workingDir, snapshot, monitor, status);
    this.snapshotManifest.addTableDescriptor(htd);
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  private boolean isAnySplitOrMergeProcedureRunning(MasterProcedureEnv env) {
    return env.getMasterServices().getMasterProcedureExecutor().getProcedures().stream()
      .filter(p -> !p.isFinished())
      .filter(p -> p instanceof SplitTableRegionProcedure ||
        p instanceof MergeTableRegionsProcedure)
      .anyMatch(p -> ((AbstractStateMachineTableProcedure<?>) p)
        .getTableName().equals(getTableName()));
  }

  private TableDescriptor loadTableDescriptorSnapshot(MasterProcedureEnv env) throws IOException {
    TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(snapshotTable);
    if (htd == null) {
      throw new IOException("TableDescriptor missing for " + snapshotTable);
    }
    if (htd.getMaxFileSize() == -1 && this.snapshot.getMaxFileSize() > 0) {
      return TableDescriptorBuilder.newBuilder(htd).setValue(TableDescriptorBuilder.MAX_FILESIZE,
        Long.toString(this.snapshot.getMaxFileSize())).build();
    }
    return htd;
  }

  private void preSnapshot(MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getSnapshotManager().prepareWorkingDirectory(snapshot);

    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSnapshot(ProtobufUtil.createSnapshotDesc(snapshot), htd, getUser());
    }
  }

  private void postSnapshot(MasterProcedureEnv env) throws IOException {
    SnapshotManager sm = env.getMasterServices().getSnapshotManager();
    if (sm != null) {
      sm.unregisterSnapshotProcedure(snapshot, getProcId());
    }

    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postSnapshot(ProtobufUtil.createSnapshotDesc(snapshot), htd, getUser());
    }
  }

  private void verifySnapshot(MasterProcedureEnv env) throws IOException {
    int verifyThreshold = env.getMasterConfiguration()
      .getInt("hbase.snapshot.remote.verify.threshold", 10000);
    List<RegionInfo> regions = env.getAssignmentManager()
      .getTableRegions(snapshotTable, false)
      .stream().filter(r -> RegionReplicaUtil.isDefaultReplica(r)).collect(Collectors.toList());
    int numRegions = regions.size();

    MasterSnapshotVerifier verifier =
      new MasterSnapshotVerifier(env.getMasterServices(), snapshot, workingDirFS);
    if (numRegions >= verifyThreshold) {
      verifier.verifySnapshot(workingDir, false);
      addChildProcedure(regions.stream()
        .map(r -> new SnapshotVerifyProcedure(snapshot, r))
        .toArray(SnapshotVerifyProcedure[]::new));
    } else {
      verifier.verifySnapshot(workingDir, true);
    }
  }

  private void completeSnapshot(MasterProcedureEnv env) throws IOException {
    // complete the snapshot, atomically moving from tmp to .snapshot dir.
    SnapshotDescriptionUtils.completeSnapshot(snapshotDir, workingDir,
      env.getMasterFileSystem().getFileSystem(), workingDirFS, conf);
    // update metric. when master restarts, the metric value is wrong
    metricsSnapshot.addSnapshot(status.getCompletionTimestamp() - status.getStartTime());
    if (env.getMasterCoprocessorHost() != null) {
      env.getMasterCoprocessorHost()
        .postCompletedSnapshotAction(ProtobufUtil.createSnapshotDesc(snapshot), htd);
    }
    status.markComplete("Snapshot " + snapshot.getName() + "  completed");
  }

  private void snapshotSplitRegions(MasterProcedureEnv env) throws IOException {
    List<RegionInfo> regions = getDefaultRegionReplica(env)
      .filter(RegionInfo::isSplit).collect(Collectors.toList());
    snapshotSplitOrClosedRegions(env, regions, "SplitRegionsSnapshotPool");
  }

  private void snapshotClosedRegions(MasterProcedureEnv env) throws IOException {
    List<RegionInfo> regions = getDefaultRegionReplica(env).collect(Collectors.toList());
    snapshotSplitOrClosedRegions(env, regions, "ClosedRegionsSnapshotPool");
  }

  private Stream<RegionInfo> getDefaultRegionReplica(MasterProcedureEnv env) {
    return env.getAssignmentManager().getTableRegions(snapshotTable, false)
      .stream().filter(r -> RegionReplicaUtil.isDefaultReplica(r));
  }

  private void snapshotSplitOrClosedRegions(MasterProcedureEnv env,
      List<RegionInfo> regions, String threadPoolName) throws IOException {
    ThreadPoolExecutor exec = SnapshotManifest
      .createExecutor(env.getMasterConfiguration(), threadPoolName);
    try {
      ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
        @Override
        public void editRegion(final RegionInfo region) throws IOException {
          snapshotManifest.addRegion(CommonFSUtils.getTableDir(rootDir, snapshotTable), region);
          LOG.info("take snapshot region={}, table={}", region, snapshotTable);
        }
      });
    } finally {
      exec.shutdown();
    }
    status.setStatus("Completed referencing closed/split regions of table: " + snapshotTable);
  }

  private void snapshotMobRegion(MasterProcedureEnv env) throws IOException {
    if (!MobUtils.hasMobColumns(htd)) {
      return;
    }
    ThreadPoolExecutor exec = SnapshotManifest
      .createExecutor(env.getMasterConfiguration(), "MobRegionSnapshotPool");
    RegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(htd.getTableName());
    try {
      ModifyRegionUtils.editRegions(exec, Collections.singleton(mobRegionInfo),
          new ModifyRegionUtils.RegionEditTask() {
          @Override
          public void editRegion(final RegionInfo region) throws IOException {
            snapshotManifest.addRegion(CommonFSUtils.getTableDir(rootDir, snapshotTable), region);
          }
        });
    } finally {
      exec.shutdown();
    }
    status.setStatus("Completed referencing HFiles for the mob region of table: " + snapshotTable);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(SnapshotProcedureStateData
      .newBuilder().setSnapshot(this.snapshot).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    SnapshotProcedureStateData data = serializer.deserialize(SnapshotProcedureStateData.class);
    this.snapshot = data.getSnapshot();
  }

  private Procedure<MasterProcedureEnv>[] createRemoteSnapshotProcedures(MasterProcedureEnv env) {
    return env.getAssignmentManager().getTableRegions(snapshotTable, true)
      .stream().filter(r -> RegionReplicaUtil.isDefaultReplica(r))
      .map(r -> new SnapshotRegionProcedure(snapshot, r))
      .toArray(SnapshotRegionProcedure[]::new);
  }

  @Override
  public void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getName())
      .append(", id=").append(getProcId())
      .append(", snapshot=").append(ClientSnapshotDescriptionUtils.toString(snapshot));
  }

  public SnapshotDescription getSnapshotDesc() {
    return snapshot;
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    if (getCurrentState() == getInitialState()) {
      // if we are in the initial state, it is unnecessary to call prepareSnapshotEnv().
      return;
    }
    try {
      prepareSnapshotEnv(env);
      boolean snapshotProcedureEnabled = conf.getBoolean(SnapshotManager.SNAPSHOT_PROCEDURE_ENABLED,
        SnapshotManager.SNAPSHOT_PROCEDURE_ENABLED_DEFAULT);
      if (!snapshotProcedureEnabled) {
        throw new IOException("SnapshotProcedure is DISABLED");
      }
    } catch (IOException e) {
      LOG.error("Failed replaying {}, mark procedure as FAILED", this, e);
      setFailure("master-snapshot", e);
    }
  }

  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  public synchronized void markSnapshotCorrupted() throws IOException {
    Path flagFile = SnapshotDescriptionUtils.getCorruptedFlagFileForSnapshot(workingDir);
    if (!workingDirFS.exists(flagFile)) {
      workingDirFS.create(flagFile).close();
      LOG.info("touch corrupted snapshot flag file {} for {}", flagFile, snapshot.getName());
    }
  }

  public boolean isSnapshotCorrupted() throws IOException {
    return workingDirFS.exists(SnapshotDescriptionUtils
      .getCorruptedFlagFileForSnapshot(workingDir));
  }
}
