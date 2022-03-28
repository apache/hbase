/*
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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.RegionSplitRestriction;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SplitTableRegionState;

/**
 * The procedure to split a region in a table.
 * Takes lock on the parent region.
 * It holds the lock for the life of the procedure.
 * <p>Throws exception on construction if determines context hostile to spllt (cluster going
 * down or master is shutting down or table is disabled).</p>
 */
@InterfaceAudience.Private
public class SplitTableRegionProcedure
    extends AbstractStateMachineRegionProcedure<SplitTableRegionState> {
  private static final Logger LOG = LoggerFactory.getLogger(SplitTableRegionProcedure.class);
  private RegionInfo daughterOneRI;
  private RegionInfo daughterTwoRI;
  private byte[] bestSplitRow;
  private RegionSplitPolicy splitPolicy;

  public SplitTableRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public SplitTableRegionProcedure(final MasterProcedureEnv env,
      final RegionInfo regionToSplit, final byte[] splitRow) throws IOException {
    super(env, regionToSplit);
    preflightChecks(env, true);
    // When procedure goes to run in its prepare step, it also does these checkOnline checks. Here
    // we fail-fast on construction. There it skips the split with just a warning.
    checkOnline(env, regionToSplit);
    this.bestSplitRow = splitRow;
    TableDescriptor tableDescriptor = env.getMasterServices().getTableDescriptors()
      .get(getTableName());
    Configuration conf = env.getMasterConfiguration();
    if (hasBestSplitRow()) {
      // Apply the split restriction for the table to the user-specified split point
      RegionSplitRestriction splitRestriction =
        RegionSplitRestriction.create(tableDescriptor, conf);
      byte[] restrictedSplitRow = splitRestriction.getRestrictedSplitPoint(bestSplitRow);
      if (!Bytes.equals(bestSplitRow, restrictedSplitRow)) {
        LOG.warn("The specified split point {} violates the split restriction of the table. "
            + "Using {} as a split point.", Bytes.toStringBinary(bestSplitRow),
          Bytes.toStringBinary(restrictedSplitRow));
        bestSplitRow = restrictedSplitRow;
      }
    }
    checkSplittable(env, regionToSplit);
    final TableName table = regionToSplit.getTable();
    final long rid = getDaughterRegionIdTimestamp(regionToSplit);
    this.daughterOneRI = RegionInfoBuilder.newBuilder(table)
        .setStartKey(regionToSplit.getStartKey())
        .setEndKey(bestSplitRow)
        .setSplit(false)
        .setRegionId(rid)
        .build();
    this.daughterTwoRI = RegionInfoBuilder.newBuilder(table)
        .setStartKey(bestSplitRow)
        .setEndKey(regionToSplit.getEndKey())
        .setSplit(false)
        .setRegionId(rid)
        .build();

    if (tableDescriptor.getRegionSplitPolicyClassName() != null) {
      // Since we don't have region reference here, creating the split policy instance without it.
      // This can be used to invoke methods which don't require Region reference. This instantiation
      // of a class on Master-side though it only makes sense on the RegionServer-side is
      // for Phoenix Local Indexing. Refer HBASE-12583 for more information.
      Class<? extends RegionSplitPolicy> clazz =
          RegionSplitPolicy.getSplitPolicyClass(tableDescriptor, conf);
      this.splitPolicy = ReflectionUtils.newInstance(clazz, conf);
    }
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitRegions(this, getTableName(), getParentRegion(),
      daughterOneRI, daughterTwoRI)) {
      try {
        LOG.debug(LockState.LOCK_EVENT_WAIT + " " + env.getProcedureScheduler().dumpLocks());
      } catch (IOException e) {
        // Ignore, just for logging
      }
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegions(this, getTableName(), getParentRegion(), daughterOneRI,
      daughterTwoRI);
  }

  public RegionInfo getDaughterOneRI() {
    return daughterOneRI;
  }

  public RegionInfo getDaughterTwoRI() {
    return daughterTwoRI;
  }

  private boolean hasBestSplitRow() {
    return bestSplitRow != null && bestSplitRow.length > 0;
  }

  /**
   * Check whether the region is splittable
   * @param env MasterProcedureEnv
   * @param regionToSplit parent Region to be split
   */
  private void checkSplittable(final MasterProcedureEnv env,
      final RegionInfo regionToSplit) throws IOException {
    // Ask the remote RS if this region is splittable.
    // If we get an IOE, report it along w/ the failure so can see why we are not splittable at
    // this time.
    if(regionToSplit.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException("Can't invoke split on non-default regions directly");
    }
    RegionStateNode node =
        env.getAssignmentManager().getRegionStates().getRegionStateNode(getParentRegion());
    IOException splittableCheckIOE = null;
    boolean splittable = false;
    if (node != null) {
      try {
        GetRegionInfoResponse response;
        if (!hasBestSplitRow()) {
          LOG.info(
            "{} splitKey isn't explicitly specified, will try to find a best split key from RS {}",
            node.getRegionInfo().getRegionNameAsString(), node.getRegionLocation());
          response = AssignmentManagerUtil.getRegionInfoResponse(env, node.getRegionLocation(),
            node.getRegionInfo(), true);
          bestSplitRow =
            response.hasBestSplitRow() ? response.getBestSplitRow().toByteArray() : null;
        } else {
          response = AssignmentManagerUtil.getRegionInfoResponse(env, node.getRegionLocation(),
            node.getRegionInfo(), false);
        }
        splittable = response.hasSplittable() && response.getSplittable();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Splittable=" + splittable + " " + node.toShortString());
        }
      } catch (IOException e) {
        splittableCheckIOE = e;
      }
    }

    if (!splittable) {
      IOException e =
        new DoNotRetryIOException(regionToSplit.getShortNameToLog() + " NOT splittable");
      if (splittableCheckIOE != null) {
        e.initCause(splittableCheckIOE);
      }
      throw e;
    }

    if (!hasBestSplitRow()) {
      throw new DoNotRetryIOException("Region not splittable because bestSplitPoint = null, " +
        "maybe table is too small for auto split. For force split, try specifying split row");
    }

    if (Bytes.equals(regionToSplit.getStartKey(), bestSplitRow)) {
      throw new DoNotRetryIOException(
        "Split row is equal to startkey: " + Bytes.toStringBinary(bestSplitRow));
    }

    if (!regionToSplit.containsRow(bestSplitRow)) {
      throw new DoNotRetryIOException("Split row is not inside region key range splitKey:" +
        Bytes.toStringBinary(bestSplitRow) + " region: " + regionToSplit);
    }
  }

  /**
   * Calculate daughter regionid to use.
   * @param hri Parent {@link RegionInfo}
   * @return Daughter region id (timestamp) to use.
   */
  private static long getDaughterRegionIdTimestamp(final RegionInfo hri) {
    long rid = EnvironmentEdgeManager.currentTime();
    // Regionid is timestamp.  Can't be less than that of parent else will insert
    // at wrong location in hbase:meta (See HBASE-710).
    if (rid < hri.getRegionId()) {
      LOG.warn("Clock skew; parent regions id is " + hri.getRegionId() +
        " but current time here is " + rid);
      rid = hri.getRegionId() + 1;
    }
    return rid;
  }

  private void removeNonDefaultReplicas(MasterProcedureEnv env) throws IOException {
    AssignmentManagerUtil.removeNonDefaultReplicas(env, Stream.of(getParentRegion()),
      getRegionReplication(env));
  }

  private void checkClosedRegions(MasterProcedureEnv env) throws IOException {
    // theoretically this should not happen any more after we use TRSP, but anyway let's add a check
    // here
    AssignmentManagerUtil.checkClosedRegion(env, getParentRegion());
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, SplitTableRegionState state)
      throws InterruptedException {
    LOG.trace("{} execute state={}", this, state);

    try {
      switch (state) {
        case SPLIT_TABLE_REGION_PREPARE:
          if (prepareSplitRegion(env)) {
            setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION);
            break;
          } else {
            return Flow.NO_MORE_STATE;
          }
        case SPLIT_TABLE_REGION_PRE_OPERATION:
          preSplitRegion(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_CLOSE_PARENT_REGION);
          break;
        case SPLIT_TABLE_REGION_CLOSE_PARENT_REGION:
          addChildProcedure(createUnassignProcedures(env));
          // createUnassignProcedures() can throw out IOException. If this happens,
          // it wont reach state SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGION and no parent regions
          // is closed as all created UnassignProcedures are rolled back. If it rolls back with
          // state SPLIT_TABLE_REGION_CLOSE_PARENT_REGION, no need to call openParentRegion(),
          // otherwise, it will result in OpenRegionProcedure for an already open region.
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS);
          break;
        case SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS:
          checkClosedRegions(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS);
          break;
        case SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS:
          removeNonDefaultReplicas(env);
          createDaughterRegions(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE);
          break;
        case SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE:
          writeMaxSequenceIdFile(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META);
          break;
        case SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META:
          preSplitRegionBeforeMETA(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_UPDATE_META);
          break;
        case SPLIT_TABLE_REGION_UPDATE_META:
          updateMeta(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META);
          break;
        case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META:
          preSplitRegionAfterMETA(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS);
          break;
        case SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS:
          addChildProcedure(createAssignProcedures(env));
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_POST_OPERATION);
          break;
        case SPLIT_TABLE_REGION_POST_OPERATION:
          postSplitRegion(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      String msg = "Splitting " + getParentRegion().getEncodedName() + ", " + this;
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retrying.
        LOG.warn(msg, e);
      } else {
        LOG.error(msg, e);
        setFailure("master-split-regions", e);
      }
    }
    // if split fails,  need to call ((HRegion)parent).clearSplit() when it is a force split
    return Flow.HAS_MORE_STATE;
  }

  /**
   * To rollback {@link SplitTableRegionProcedure}, an AssignProcedure is asynchronously
   * submitted for parent region to be split (rollback doesn't wait on the completion of the
   * AssignProcedure) . This can be improved by changing rollback() to support sub-procedures.
   * See HBASE-19851 for details.
   */
  @Override
  protected void rollbackState(final MasterProcedureEnv env, final SplitTableRegionState state)
      throws IOException, InterruptedException {
    LOG.trace("{} rollback state={}", this, state);

    try {
      switch (state) {
        case SPLIT_TABLE_REGION_POST_OPERATION:
        case SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS:
        case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META:
        case SPLIT_TABLE_REGION_UPDATE_META:
          // PONR
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
        case SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META:
          break;
        case SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS:
        case SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE:
          deleteDaughterRegions(env);
          break;
        case SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS:
          openParentRegion(env);
          break;
        case SPLIT_TABLE_REGION_CLOSE_PARENT_REGION:
          // If it rolls back with state SPLIT_TABLE_REGION_CLOSE_PARENT_REGION, no need to call
          // openParentRegion(), otherwise, it will result in OpenRegionProcedure for an
          // already open region.
          break;
        case SPLIT_TABLE_REGION_PRE_OPERATION:
          postRollBackSplitRegion(env);
          break;
        case SPLIT_TABLE_REGION_PREPARE:
          break; // nothing to do
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("pid=" + getProcId() + " failed rollback attempt step " + state +
          " for splitting the region "
        + getParentRegion().getEncodedName() + " in table " + getTableName(), e);
      throw e;
    }
  }

  /*
   * Check whether we are in the state that can be rollback
   */
  @Override
  protected boolean isRollbackSupported(final SplitTableRegionState state) {
    switch (state) {
      case SPLIT_TABLE_REGION_POST_OPERATION:
      case SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS:
      case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META:
      case SPLIT_TABLE_REGION_UPDATE_META:
        // It is not safe to rollback if we reach to these states.
        return false;
      default:
        break;
    }
    return true;
  }

  @Override
  protected SplitTableRegionState getState(final int stateId) {
    return SplitTableRegionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final SplitTableRegionState state) {
    return state.getNumber();
  }

  @Override
  protected SplitTableRegionState getInitialState() {
    return SplitTableRegionState.SPLIT_TABLE_REGION_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    final MasterProcedureProtos.SplitTableRegionStateData.Builder splitTableRegionMsg =
        MasterProcedureProtos.SplitTableRegionStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setParentRegionInfo(ProtobufUtil.toRegionInfo(getRegion()))
        .addChildRegionInfo(ProtobufUtil.toRegionInfo(daughterOneRI))
        .addChildRegionInfo(ProtobufUtil.toRegionInfo(daughterTwoRI));
    serializer.serialize(splitTableRegionMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    final MasterProcedureProtos.SplitTableRegionStateData splitTableRegionsMsg =
        serializer.deserialize(MasterProcedureProtos.SplitTableRegionStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(splitTableRegionsMsg.getUserInfo()));
    setRegion(ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getParentRegionInfo()));
    assert(splitTableRegionsMsg.getChildRegionInfoCount() == 2);
    daughterOneRI = ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getChildRegionInfo(0));
    daughterTwoRI = ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getChildRegionInfo(1));
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
    sb.append(", parent=");
    sb.append(getParentRegion().getShortNameToLog());
    sb.append(", daughterA=");
    sb.append(daughterOneRI.getShortNameToLog());
    sb.append(", daughterB=");
    sb.append(daughterTwoRI.getShortNameToLog());
  }

  private RegionInfo getParentRegion() {
    return getRegion();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_SPLIT;
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getSplitProcMetrics();
  }

  private byte[] getSplitRow() {
    return daughterTwoRI.getStartKey();
  }

  private static final State[] EXPECTED_SPLIT_STATES = new State[] { State.OPEN, State.CLOSED };

  /**
   * Prepare to Split region.
   * @param env MasterProcedureEnv
   */
  public boolean prepareSplitRegion(final MasterProcedureEnv env) throws IOException {
    // Fail if we are taking snapshot for the given table
    if (env.getMasterServices().getSnapshotManager()
      .isTakingSnapshot(getParentRegion().getTable())) {
      setFailure(new IOException("Skip splitting region " + getParentRegion().getShortNameToLog() +
        ", because we are taking snapshot for the table " + getParentRegion().getTable()));
      return false;
    }
    // Check whether the region is splittable
    RegionStateNode node =
        env.getAssignmentManager().getRegionStates().getRegionStateNode(getParentRegion());

    if (node == null) {
      throw new UnknownRegionException(getParentRegion().getRegionNameAsString());
    }

    RegionInfo parentHRI = node.getRegionInfo();
    if (parentHRI == null) {
      LOG.info("Unsplittable; parent region is null; node={}", node);
      return false;
    }
    // Lookup the parent HRI state from the AM, which has the latest updated info.
    // Protect against the case where concurrent SPLIT requests came in and succeeded
    // just before us.
    if (node.isInState(State.SPLIT)) {
      LOG.info("Split of " + parentHRI + " skipped; state is already SPLIT");
      return false;
    }
    if (parentHRI.isSplit() || parentHRI.isOffline()) {
      LOG.info("Split of " + parentHRI + " skipped because offline/split.");
      return false;
    }

    // expected parent to be online or closed
    if (!node.isInState(EXPECTED_SPLIT_STATES)) {
      // We may have SPLIT already?
      setFailure(new IOException("Split " + parentHRI.getRegionNameAsString() +
          " FAILED because state=" + node.getState() + "; expected " +
          Arrays.toString(EXPECTED_SPLIT_STATES)));
      return false;
    }

    // Mostly this check is not used because we already check the switch before submit a split
    // procedure. Just for safe, check the switch again. This procedure can be rollbacked if
    // the switch was set to false after submit.
    if (!env.getMasterServices().isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
      LOG.warn("pid=" + getProcId() + " split switch is off! skip split of " + parentHRI);
      setFailure(new IOException("Split region " + parentHRI.getRegionNameAsString() +
          " failed due to split switch off"));
      return false;
    }

    if (!env.getMasterServices().getTableDescriptors().get(getTableName()).isSplitEnabled()) {
      LOG.warn("pid={}, split is disabled for the table! Skipping split of {}", getProcId(),
        parentHRI);
      setFailure(new IOException("Split region " + parentHRI.getRegionNameAsString()
          + " failed as region split is disabled for the table"));
      return false;
    }

    // set node state as SPLITTING
    node.setState(State.SPLITTING);

    // Since we have the lock and the master is coordinating the operation
    // we are always able to split the region
    return true;
  }

  /**
   * Action before splitting region in a table.
   * @param env MasterProcedureEnv
   */
  private void preSplitRegion(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSplitRegionAction(getTableName(), getSplitRow(), getUser());
    }

    // TODO: Clean up split and merge. Currently all over the place.
    // Notify QuotaManager and RegionNormalizer
    try {
      env.getMasterServices().getMasterQuotaManager().onRegionSplit(this.getParentRegion());
    } catch (QuotaExceededException e) {
      // TODO: why is this here? split requests can be submitted by actors other than the normalizer
      env.getMasterServices()
        .getRegionNormalizerManager()
        .planSkipped(NormalizationPlan.PlanType.SPLIT);
      throw e;
    }
  }

  /**
   * Action after rollback a split table region action.
   * @param env MasterProcedureEnv
   */
  private void postRollBackSplitRegion(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postRollBackSplitRegionAction(getUser());
    }
  }

  /**
   * Rollback close parent region
   */
  private void openParentRegion(MasterProcedureEnv env) throws IOException {
    AssignmentManagerUtil.reopenRegionsForRollback(env,
      Collections.singletonList((getParentRegion())), getRegionReplication(env),
      getParentRegionServerName(env));
  }

  /**
   * Create daughter regions
   */
  public void createDaughterRegions(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = CommonFSUtils.getTableDir(mfs.getRootDir(), getTableName());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, getParentRegion(), false);
    regionFs.createSplitsDir(daughterOneRI, daughterTwoRI);

    Pair<List<Path>, List<Path>> expectedReferences = splitStoreFiles(env, regionFs);

    assertSplitResultFilesCount(fs, expectedReferences.getFirst().size(),
      regionFs.getSplitsDir(daughterOneRI));
    regionFs.commitDaughterRegion(daughterOneRI, expectedReferences.getFirst(), env);
    assertSplitResultFilesCount(fs, expectedReferences.getFirst().size(),
      new Path(tabledir, daughterOneRI.getEncodedName()));

    assertSplitResultFilesCount(fs, expectedReferences.getSecond().size(),
      regionFs.getSplitsDir(daughterTwoRI));
    regionFs.commitDaughterRegion(daughterTwoRI, expectedReferences.getSecond(), env);
    assertSplitResultFilesCount(fs, expectedReferences.getSecond().size(),
      new Path(tabledir, daughterTwoRI.getEncodedName()));
  }

  private void deleteDaughterRegions(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = CommonFSUtils.getTableDir(mfs.getRootDir(), getTableName());
    HRegionFileSystem.deleteRegionFromFileSystem(env.getMasterConfiguration(),
      mfs.getFileSystem(), tabledir, daughterOneRI);
    HRegionFileSystem.deleteRegionFromFileSystem(env.getMasterConfiguration(),
      mfs.getFileSystem(), tabledir, daughterTwoRI);
  }

  /**
   * Create Split directory
   * @param env MasterProcedureEnv
   */
  private Pair<List<Path>, List<Path>> splitStoreFiles(final MasterProcedureEnv env,
      final HRegionFileSystem regionFs) throws IOException {
    final Configuration conf = env.getMasterConfiguration();
    TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    // The following code sets up a thread pool executor with as many slots as
    // there's files to split. It then fires up everything, waits for
    // completion and finally checks for any exception
    //
    // Note: From HBASE-26187, splitStoreFiles now creates daughter region dirs straight under the
    // table dir. In case of failure, the proc would go through this again, already existing
    // region dirs and split files would just be ignored, new split files should get created.
    int nbFiles = 0;
    final Map<String, Collection<StoreFileInfo>> files =
        new HashMap<String, Collection<StoreFileInfo>>(htd.getColumnFamilyCount());
    for (ColumnFamilyDescriptor cfd : htd.getColumnFamilies()) {
      String family = cfd.getNameAsString();
      StoreFileTracker tracker =
        StoreFileTrackerFactory.create(env.getMasterConfiguration(), htd, cfd, regionFs);
      Collection<StoreFileInfo> sfis = tracker.load();
      if (sfis == null) {
        continue;
      }
      Collection<StoreFileInfo> filteredSfis = null;
      for (StoreFileInfo sfi : sfis) {
        // Filter. There is a lag cleaning up compacted reference files. They get cleared
        // after a delay in case outstanding Scanners still have references. Because of this,
        // the listing of the Store content may have straggler reference files. Skip these.
        // It should be safe to skip references at this point because we checked above with
        // the region if it thinks it is splittable and if we are here, it thinks it is
        // splitable.
        if (sfi.isReference()) {
          LOG.info("Skipping split of " + sfi + "; presuming ready for archiving.");
          continue;
        }
        if (filteredSfis == null) {
          filteredSfis = new ArrayList<StoreFileInfo>(sfis.size());
          files.put(family, filteredSfis);
        }
        filteredSfis.add(sfi);
        nbFiles++;
      }
    }
    if (nbFiles == 0) {
      // no file needs to be splitted.
      return new Pair<>(Collections.emptyList(), Collections.emptyList());
    }
    // Max #threads is the smaller of the number of storefiles or the default max determined above.
    int maxThreads = Math.min(
      conf.getInt(HConstants.REGION_SPLIT_THREADS_MAX,
        conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT)),
      nbFiles);
    LOG.info("pid=" + getProcId() + " splitting " + nbFiles + " storefiles, region=" +
        getParentRegion().getShortNameToLog() + ", threads=" + maxThreads);
    final ExecutorService threadPool = Executors.newFixedThreadPool(maxThreads,
      new ThreadFactoryBuilder().setNameFormat("StoreFileSplitter-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    final List<Future<Pair<Path, Path>>> futures = new ArrayList<Future<Pair<Path, Path>>>(nbFiles);

    // Split each store file.
    for (Map.Entry<String, Collection<StoreFileInfo>> e : files.entrySet()) {
      byte[] familyName = Bytes.toBytes(e.getKey());
      final ColumnFamilyDescriptor hcd = htd.getColumnFamily(familyName);
      final Collection<StoreFileInfo> storeFiles = e.getValue();
      if (storeFiles != null && storeFiles.size() > 0) {
        final Configuration storeConfiguration =
          StoreUtils.createStoreConfiguration(env.getMasterConfiguration(), htd, hcd);
        for (StoreFileInfo storeFileInfo : storeFiles) {
          // As this procedure is running on master, use CacheConfig.DISABLED means
          // don't cache any block.
          // We also need to pass through a suitable CompoundConfiguration as if this
          // is running in a regionserver's Store context, or we might not be able
          // to read the hfiles.
          storeFileInfo.setConf(storeConfiguration);
          StoreFileSplitter sfs = new StoreFileSplitter(regionFs, familyName,
            new HStoreFile(storeFileInfo, hcd.getBloomFilterType(), CacheConfig.DISABLED));
          futures.add(threadPool.submit(sfs));
        }
      }
    }
    // Shutdown the pool
    threadPool.shutdown();

    // Wait for all the tasks to finish.
    // When splits ran on the RegionServer, how-long-to-wait-configuration was named
    // hbase.regionserver.fileSplitTimeout. If set, use its value.
    long fileSplitTimeout = conf.getLong("hbase.master.fileSplitTimeout",
      conf.getLong("hbase.regionserver.fileSplitTimeout", 600000));
    try {
      boolean stillRunning = !threadPool.awaitTermination(fileSplitTimeout, TimeUnit.MILLISECONDS);
      if (stillRunning) {
        threadPool.shutdownNow();
        // wait for the thread to shutdown completely.
        while (!threadPool.isTerminated()) {
          Thread.sleep(50);
        }
        throw new IOException(
            "Took too long to split the" + " files and create the references, aborting split");
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }

    List<Path> daughterA = new ArrayList<>();
    List<Path> daughterB = new ArrayList<>();
    // Look for any exception
    for (Future<Pair<Path, Path>> future : futures) {
      try {
        Pair<Path, Path> p = future.get();
        if(p.getFirst() != null){
          daughterA.add(p.getFirst());
        }
        if(p.getSecond() != null){
          daughterB.add(p.getSecond());
        }
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("pid=" + getProcId() + " split storefiles for region " +
          getParentRegion().getShortNameToLog() + " Daughter A: " + daughterA +
          " storefiles, Daughter B: " + daughterB + " storefiles.");
    }
    return new Pair<>(daughterA, daughterB);
  }

  private void assertSplitResultFilesCount(final FileSystem fs,
      final int expectedSplitResultFileCount, Path dir)
    throws IOException {
    if (expectedSplitResultFileCount != 0) {
      int resultFileCount = FSUtils.getRegionReferenceAndLinkFileCount(fs, dir);
      if (expectedSplitResultFileCount != resultFileCount) {
        throw new IOException("Failing split. Didn't have expected reference and HFileLink files"
            + ", expected=" + expectedSplitResultFileCount + ", actual=" + resultFileCount);
      }
    }
  }

  private Pair<Path, Path> splitStoreFile(HRegionFileSystem regionFs, byte[] family, HStoreFile sf)
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("pid=" + getProcId() + " splitting started for store file: " +
          sf.getPath() + " for region: " + getParentRegion().getShortNameToLog());
    }

    final byte[] splitRow = getSplitRow();
    final String familyName = Bytes.toString(family);
    final Path path_first = regionFs.splitStoreFile(this.daughterOneRI, familyName, sf, splitRow,
        false, splitPolicy);
    final Path path_second = regionFs.splitStoreFile(this.daughterTwoRI, familyName, sf, splitRow,
       true, splitPolicy);
    if (LOG.isDebugEnabled()) {
      LOG.debug("pid=" + getProcId() + " splitting complete for store file: " +
          sf.getPath() + " for region: " + getParentRegion().getShortNameToLog());
    }
    return new Pair<Path,Path>(path_first, path_second);
  }

  /**
   * Utility class used to do the file splitting / reference writing
   * in parallel instead of sequentially.
   */
  private class StoreFileSplitter implements Callable<Pair<Path,Path>> {
    private final HRegionFileSystem regionFs;
    private final byte[] family;
    private final HStoreFile sf;

    /**
     * Constructor that takes what it needs to split
     * @param regionFs the file system
     * @param family Family that contains the store file
     * @param sf which file
     */
    public StoreFileSplitter(HRegionFileSystem regionFs, byte[] family, HStoreFile sf) {
      this.regionFs = regionFs;
      this.sf = sf;
      this.family = family;
    }

    @Override
    public Pair<Path,Path> call() throws IOException {
      return splitStoreFile(regionFs, family, sf);
    }
  }

  /**
   * Post split region actions before the Point-of-No-Return step
   * @param env MasterProcedureEnv
   **/
  private void preSplitRegionBeforeMETA(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final List<Mutation> metaEntries = new ArrayList<Mutation>();
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSplitBeforeMETAAction(getSplitRow(), metaEntries, getUser());
      try {
        for (Mutation p : metaEntries) {
          RegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("pid=" + getProcId() + " row key of mutation from coprocessor not parsable as "
            + "region name."
            + "Mutations from coprocessor should only for hbase:meta table.");
        throw e;
      }
    }
  }

  /**
   * Add daughter regions to META
   * @param env MasterProcedureEnv
   */
  private void updateMeta(final MasterProcedureEnv env) throws IOException {
    env.getAssignmentManager().markRegionAsSplit(getParentRegion(), getParentRegionServerName(env),
        daughterOneRI, daughterTwoRI);
  }

  /**
   * Pre split region actions after the Point-of-No-Return step
   * @param env MasterProcedureEnv
   **/
  private void preSplitRegionAfterMETA(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSplitAfterMETAAction(getUser());
    }
  }

  /**
   * Post split region actions
   * @param env MasterProcedureEnv
   **/
  private void postSplitRegion(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postCompletedSplitRegionAction(daughterOneRI, daughterTwoRI, getUser());
    }
  }

  private ServerName getParentRegionServerName(final MasterProcedureEnv env) {
    return env.getMasterServices().getAssignmentManager().getRegionStates()
      .getRegionServerOfRegion(getParentRegion());
  }

  private TransitRegionStateProcedure[] createUnassignProcedures(MasterProcedureEnv env)
      throws IOException {
    return AssignmentManagerUtil.createUnassignProceduresForSplitOrMerge(env,
      Stream.of(getParentRegion()), getRegionReplication(env));
  }

  private TransitRegionStateProcedure[] createAssignProcedures(MasterProcedureEnv env)
      throws IOException {
    List<RegionInfo> hris = new ArrayList<RegionInfo>(2);
    hris.add(daughterOneRI);
    hris.add(daughterTwoRI);
    return AssignmentManagerUtil.createAssignProceduresForSplitDaughters(env, hris,
      getRegionReplication(env), getParentRegionServerName(env));
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    final TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    return htd.getRegionReplication();
  }

  private void writeMaxSequenceIdFile(MasterProcedureEnv env) throws IOException {
    MasterFileSystem fs = env.getMasterFileSystem();
    long maxSequenceId = WALSplitUtil.getMaxRegionSequenceId(env.getMasterConfiguration(),
      getParentRegion(), fs::getFileSystem, fs::getWALFileSystem);
    if (maxSequenceId > 0) {
      WALSplitUtil.writeRegionSequenceIdFile(fs.getWALFileSystem(),
        getWALRegionDir(env, daughterOneRI), maxSequenceId);
      WALSplitUtil.writeRegionSequenceIdFile(fs.getWALFileSystem(),
        getWALRegionDir(env, daughterTwoRI), maxSequenceId);
    }
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // Abort means rollback. We can't rollback all steps. HBASE-18018 added abort to all
    // Procedures. Here is a Procedure that has a PONR and cannot be aborted wants it enters this
    // range of steps; what do we do for these should an operator want to cancel them? HBASE-20022.
    return isRollbackSupported(getCurrentState())? super.abort(env): false;
  }
}
