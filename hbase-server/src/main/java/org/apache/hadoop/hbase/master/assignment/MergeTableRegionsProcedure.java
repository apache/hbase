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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MergeTableRegionsState;

/**
 * The procedure to Merge regions in a table. This procedure takes an exclusive table
 * lock since it is working over multiple regions. It holds the lock for the life of the procedure.
 * Throws exception on construction if determines context hostile to merge (cluster going down or
 * master is shutting down or table is disabled).
 */
@InterfaceAudience.Private
public class MergeTableRegionsProcedure
    extends AbstractStateMachineTableProcedure<MergeTableRegionsState> {
  private static final Logger LOG = LoggerFactory.getLogger(MergeTableRegionsProcedure.class);
  private ServerName regionLocation;

  /**
   * Two or more regions to merge, the 'merge parents'.
   */
  private RegionInfo[] regionsToMerge;

  /**
   * The resulting merged region.
   */
  private RegionInfo mergedRegion;

  private boolean force;

  public MergeTableRegionsProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public MergeTableRegionsProcedure(final MasterProcedureEnv env,
      final RegionInfo[] regionsToMerge, final boolean force)
      throws IOException {
    super(env);
    // Check parent regions. Make sure valid before starting work.
    // This check calls the super method #checkOnline also.
    checkRegionsToMerge(env, regionsToMerge, force);
    // Sort the regions going into the merge.
    Arrays.sort(regionsToMerge);
    this.regionsToMerge = regionsToMerge;
    this.mergedRegion = createMergedRegionInfo(regionsToMerge);
    // Preflight depends on mergedRegion being set (at least).
    preflightChecks(env, true);
    this.force = force;
  }

  /**
   * @throws MergeRegionException If unable to merge regions for whatever reasons.
   */
  private static void checkRegionsToMerge(MasterProcedureEnv env, final RegionInfo[] regions,
      final boolean force) throws MergeRegionException {
    long count = Arrays.stream(regions).distinct().count();
    if (regions.length != count) {
      throw new MergeRegionException("Duplicate regions specified; cannot merge a region to " +
          "itself. Passed in " + regions.length + " but only " + count + " unique.");
    }
    if (count < 2) {
      throw new MergeRegionException("Need two Regions at least to run a Merge");
    }
    RegionInfo previous = null;
    for (RegionInfo ri: regions) {
      if (previous != null) {
        if (!previous.getTable().equals(ri.getTable())) {
          String msg = "Can't merge regions from different tables: " + previous + ", " + ri;
          LOG.warn(msg);
          throw new MergeRegionException(msg);
        }
        if (!force && !ri.isAdjacent(previous) && !ri.isOverlap(previous)) {
          String msg = "Unable to merge non-adjacent or non-overlapping regions '" +
              previous.getShortNameToLog() + "', '" + ri.getShortNameToLog() + "' when force=false";
          LOG.warn(msg);
          throw new MergeRegionException(msg);
        }
      }

      if (ri.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
        throw new MergeRegionException("Can't merge non-default replicas; " + ri);
      }
      try {
        checkOnline(env, ri);
      } catch (DoNotRetryRegionException dnrre) {
        throw new MergeRegionException(dnrre);
      }

      previous = ri;
    }
  }

  /**
   * Create merged region info by looking at passed in <code>regionsToMerge</code>
   * to figure what extremes for start and end keys to use; merged region needs
   * to have an extent sufficient to cover all regions-to-merge.
   */
  private static RegionInfo createMergedRegionInfo(final RegionInfo[] regionsToMerge) {
    byte [] lowestStartKey = null;
    byte [] highestEndKey = null;
    // Region Id is a timestamp. Merged region's id can't be less than that of
    // merging regions else will insert at wrong location in hbase:meta (See HBASE-710).
    long highestRegionId = -1;
    for (RegionInfo ri: regionsToMerge) {
      if (lowestStartKey == null) {
        lowestStartKey = ri.getStartKey();
      } else if (Bytes.compareTo(ri.getStartKey(), lowestStartKey) < 0) {
        lowestStartKey = ri.getStartKey();
      }
      if (highestEndKey == null) {
        highestEndKey = ri.getEndKey();
      } else if (ri.isLast() || Bytes.compareTo(ri.getEndKey(), highestEndKey) > 0) {
        highestEndKey = ri.getEndKey();
      }
      highestRegionId = ri.getRegionId() > highestRegionId? ri.getRegionId(): highestRegionId;
    }
    // Merged region is sorted between two merging regions in META
    return RegionInfoBuilder.newBuilder(regionsToMerge[0].getTable()).
        setStartKey(lowestStartKey).
        setEndKey(highestEndKey).
        setSplit(false).
        setRegionId(highestRegionId + 1/*Add one so new merged region is highest*/).
        build();
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env,
      MergeTableRegionsState state) {
    LOG.trace("{} execute state={}", this, state);
    try {
      switch (state) {
        case MERGE_TABLE_REGIONS_PREPARE:
          if (!prepareMergeRegion(env)) {
            assert isFailed() : "Merge region should have an exception here";
            return Flow.NO_MORE_STATE;
          }
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION);
          break;
        case MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION:
          preMergeRegions(env);
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CLOSE_REGIONS);
          break;
        case MERGE_TABLE_REGIONS_CLOSE_REGIONS:
          addChildProcedure(createUnassignProcedures(env));
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CHECK_CLOSED_REGIONS);
          break;
        case MERGE_TABLE_REGIONS_CHECK_CLOSED_REGIONS:
          checkClosedRegions(env);
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CREATE_MERGED_REGION);
          break;
        case MERGE_TABLE_REGIONS_CREATE_MERGED_REGION:
          removeNonDefaultReplicas(env);
          createMergedRegion(env);
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_WRITE_MAX_SEQUENCE_ID_FILE);
          break;
        case MERGE_TABLE_REGIONS_WRITE_MAX_SEQUENCE_ID_FILE:
          writeMaxSequenceIdFile(env);
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION);
          break;
        case MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION:
          preMergeRegionsCommit(env);
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_UPDATE_META);
          break;
        case MERGE_TABLE_REGIONS_UPDATE_META:
          updateMetaForMergedRegions(env);
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION);
          break;
        case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
          postMergeRegionsCommit(env);
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_OPEN_MERGED_REGION);
          break;
        case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
          addChildProcedure(createAssignProcedures(env));
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_POST_OPERATION);
          break;
        case MERGE_TABLE_REGIONS_POST_OPERATION:
          postCompletedMergeRegions(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      String msg = "Error trying to merge " + RegionInfo.getShortNameToLog(regionsToMerge) +
          " in " + getTableName() + " (in state=" + state + ")";
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retrying.
        LOG.warn(msg, e);
      } else {
        LOG.error(msg, e);
        setFailure("master-merge-regions", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  /**
   * To rollback {@link MergeTableRegionsProcedure}, two AssignProcedures are asynchronously
   * submitted for each region to be merged (rollback doesn't wait on the completion of the
   * AssignProcedures) . This can be improved by changing rollback() to support sub-procedures.
   * See HBASE-19851 for details.
   */
  @Override
  protected void rollbackState(final MasterProcedureEnv env, final MergeTableRegionsState state)
      throws IOException {
    LOG.trace("{} rollback state={}", this, state);

    try {
      switch (state) {
        case MERGE_TABLE_REGIONS_POST_OPERATION:
        case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
        case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
        case MERGE_TABLE_REGIONS_UPDATE_META:
          String msg = this + " We are in the " + state + " state." +
            " It is complicated to rollback the merge operation that region server is working on." +
            " Rollback is not supported and we should let the merge operation to complete";
          LOG.warn(msg);
          // PONR
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
        case MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION:
          break;
        case MERGE_TABLE_REGIONS_CREATE_MERGED_REGION:
        case MERGE_TABLE_REGIONS_WRITE_MAX_SEQUENCE_ID_FILE:
          cleanupMergedRegion(env);
          break;
        case MERGE_TABLE_REGIONS_CHECK_CLOSED_REGIONS:
          break;
        case MERGE_TABLE_REGIONS_CLOSE_REGIONS:
          rollbackCloseRegionsForMerge(env);
          break;
        case MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION:
          postRollBackMergeRegions(env);
          break;
        case MERGE_TABLE_REGIONS_PREPARE:
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (Exception e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for merging the regions "
          + RegionInfo.getShortNameToLog(regionsToMerge) + " in table " + getTableName(), e);
      throw e;
    }
  }

  /*
   * Check whether we are in the state that can be rolled back
   */
  @Override
  protected boolean isRollbackSupported(final MergeTableRegionsState state) {
    switch (state) {
      case MERGE_TABLE_REGIONS_POST_OPERATION:
      case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
      case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
      case MERGE_TABLE_REGIONS_UPDATE_META:
        // It is not safe to rollback in these states.
        return false;
      default:
        break;
    }
    return true;
  }

  private void removeNonDefaultReplicas(MasterProcedureEnv env) throws IOException {
    AssignmentManagerUtil.removeNonDefaultReplicas(env, Stream.of(regionsToMerge),
        getRegionReplication(env));
  }

  private void checkClosedRegions(MasterProcedureEnv env) throws IOException {
    // Theoretically this should not happen any more after we use TRSP, but anyway
    // let's add a check here
    for (RegionInfo region : regionsToMerge) {
      AssignmentManagerUtil.checkClosedRegion(env, region);
    }
  }

  @Override
  protected MergeTableRegionsState getState(final int stateId) {
    return MergeTableRegionsState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final MergeTableRegionsState state) {
    return state.getNumber();
  }

  @Override
  protected MergeTableRegionsState getInitialState() {
    return MergeTableRegionsState.MERGE_TABLE_REGIONS_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    final MasterProcedureProtos.MergeTableRegionsStateData.Builder mergeTableRegionsMsg =
        MasterProcedureProtos.MergeTableRegionsStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setMergedRegionInfo(ProtobufUtil.toRegionInfo(mergedRegion))
        .setForcible(force);
    for (RegionInfo ri: regionsToMerge) {
      mergeTableRegionsMsg.addRegionInfo(ProtobufUtil.toRegionInfo(ri));
    }
    serializer.serialize(mergeTableRegionsMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    final MasterProcedureProtos.MergeTableRegionsStateData mergeTableRegionsMsg =
        serializer.deserialize(MasterProcedureProtos.MergeTableRegionsStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(mergeTableRegionsMsg.getUserInfo()));

    assert(mergeTableRegionsMsg.getRegionInfoCount() == 2);
    regionsToMerge = new RegionInfo[mergeTableRegionsMsg.getRegionInfoCount()];
    for (int i = 0; i < regionsToMerge.length; i++) {
      regionsToMerge[i] = ProtobufUtil.toRegionInfo(mergeTableRegionsMsg.getRegionInfo(i));
    }

    mergedRegion = ProtobufUtil.toRegionInfo(mergeTableRegionsMsg.getMergedRegionInfo());
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
    sb.append(", regions=");
    sb.append(RegionInfo.getShortNameToLog(regionsToMerge));
    sb.append(", force=");
    sb.append(force);
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    RegionInfo[] lockRegions = Arrays.copyOf(regionsToMerge, regionsToMerge.length + 1);
    lockRegions[lockRegions.length - 1] = mergedRegion;

    if (env.getProcedureScheduler().waitRegions(this, getTableName(), lockRegions)) {
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
    RegionInfo[] lockRegions = Arrays.copyOf(regionsToMerge, regionsToMerge.length + 1);
    lockRegions[lockRegions.length - 1] = mergedRegion;

    env.getProcedureScheduler().wakeRegions(this, getTableName(), lockRegions);
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    return true;
  }

  @Override
  public TableName getTableName() {
    return mergedRegion.getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_MERGE;
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getMergeProcMetrics();
  }

  /**
   * Prepare merge and do some check
   */
  private boolean prepareMergeRegion(final MasterProcedureEnv env) throws IOException {
    // Fail if we are taking snapshot for the given table
    TableName tn = regionsToMerge[0].getTable();
    if (env.getMasterServices().getSnapshotManager().isTakingSnapshot(tn)) {
      throw new MergeRegionException("Skip merging regions " +
          RegionInfo.getShortNameToLog(regionsToMerge) + ", because we are snapshotting " + tn);
    }

    // Mostly this check is not used because we already check the switch before submit a merge
    // procedure. Just for safe, check the switch again. This procedure can be rollbacked if
    // the switch was set to false after submit.
    if (!env.getMasterServices().isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
      String regionsStr = Arrays.deepToString(this.regionsToMerge);
      LOG.warn("Merge switch is off! skip merge of " + regionsStr);
      setFailure(getClass().getSimpleName(),
          new IOException("Merge of " + regionsStr + " failed because merge switch is off"));
      return false;
    }

    if (!env.getMasterServices().getTableDescriptors().get(getTableName()).isMergeEnabled()) {
      String regionsStr = Arrays.deepToString(regionsToMerge);
      LOG.warn("Merge is disabled for the table! Skipping merge of {}", regionsStr);
      setFailure(getClass().getSimpleName(), new IOException(
          "Merge of " + regionsStr + " failed as region merge is disabled for the table"));
      return false;
    }

    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    for (RegionInfo ri : this.regionsToMerge) {
      if (MetaTableAccessor.hasMergeRegions(env.getMasterServices().getConnection(),
        ri.getRegionName())) {
        String msg = "Skip merging " + RegionInfo.getShortNameToLog(regionsToMerge) +
          ", because a parent, " + RegionInfo.getShortNameToLog(ri) + ", has a merge qualifier " +
          "(if a 'merge column' in parent, it was recently merged but still has outstanding " +
          "references to its parents that must be cleared before it can participate in merge -- " +
          "major compact it to hurry clearing of its references)";
        LOG.warn(msg);
        throw new MergeRegionException(msg);
      }
      RegionState state = regionStates.getRegionState(ri.getEncodedName());
      if (state == null) {
        throw new UnknownRegionException(RegionInfo.getShortNameToLog(ri) +
          " UNKNOWN (Has it been garbage collected?)");
      }
      if (!state.isOpened()) {
        throw new MergeRegionException("Unable to merge regions that are NOT online: " + ri);
      }
      // Ask the remote regionserver if regions are mergeable. If we get an IOE, report it
      // along with the failure, so we can see why regions are not mergeable at this time.
      try {
        if (!isMergeable(env, state)) {
          setFailure(getClass().getSimpleName(),
            new MergeRegionException(
              "Skip merging " + RegionInfo.getShortNameToLog(regionsToMerge) +
                ", because a parent, " + RegionInfo.getShortNameToLog(ri) + ", is not mergeable"));
          return false;
        }
      } catch (IOException e) {
        IOException ioe = new IOException(RegionInfo.getShortNameToLog(ri) + " NOT mergeable", e);
        setFailure(getClass().getSimpleName(), ioe);
        return false;
      }
    }

    // Update region states to Merging
    setRegionStateToMerging(env);
    return true;
  }

  private boolean isMergeable(final MasterProcedureEnv env, final RegionState rs)
      throws IOException {
    GetRegionInfoResponse response =
      AssignmentManagerUtil.getRegionInfoResponse(env, rs.getServerName(), rs.getRegion());
    return response.hasMergeable() && response.getMergeable();
  }

  /**
   * Pre merge region action
   * @param env MasterProcedureEnv
   **/
  private void preMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preMergeRegionsAction(regionsToMerge, getUser());
    }
    // TODO: Clean up split and merge. Currently all over the place.
    try {
      env.getMasterServices().getMasterQuotaManager().onRegionMerged(this.mergedRegion);
    } catch (QuotaExceededException e) {
      // TODO: why is this here? merge requests can be submitted by actors other than the normalizer
      env.getMasterServices()
        .getRegionNormalizerManager()
        .planSkipped(NormalizationPlan.PlanType.MERGE);
      throw e;
    }
  }

  /**
   * Action after rollback a merge table regions action.
   */
  private void postRollBackMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postRollBackMergeRegionsAction(regionsToMerge, getUser());
    }
  }

  /**
   * Set the region states to MERGING state
   */
  private void setRegionStateToMerging(final MasterProcedureEnv env) {
    // Set State.MERGING to regions to be merged
    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    for (RegionInfo ri: this.regionsToMerge) {
      regionStates.getRegionStateNode(ri).setState(State.MERGING);
    }
  }

  /**
   * Create merged region.
   * The way the merge works is that we make a 'merges' temporary
   * directory in the FIRST parent region to merge (Do not change this without
   * also changing the rollback where we look in this FIRST region for the
   * merge dir). We then collect here references to all the store files in all
   * the parent regions including those of the FIRST parent region into a
   * subdirectory, named for the resultant merged region. We then call
   * commitMergeRegion. It finds this subdirectory of storefile references
   * and moves them under the new merge region (creating the region layout
   * as side effect). After assign of the new merge region, we will run a
   * compaction. This will undo the references but the reference files remain
   * in place until the archiver runs (which it does on a period as a chore
   * in the RegionServer that hosts the merge region -- see
   * CompactedHFilesDischarger). Once the archiver has moved aside the
   * no-longer used references, the merge region no longer has references.
   * The catalog janitor will notice when it runs next and it will remove
   * the old parent regions.
   */
  private void createMergedRegion(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = CommonFSUtils.getTableDir(mfs.getRootDir(), regionsToMerge[0].getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem mergeRegionFs = null;
    for (RegionInfo ri: this.regionsToMerge) {
      HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
          env.getMasterConfiguration(), fs, tabledir, ri, false);
      if (mergeRegionFs == null) {
        mergeRegionFs = regionFs;
        mergeRegionFs.createMergesDir();
      }
      mergeStoreFiles(env, regionFs, mergeRegionFs.getMergesDir());
    }
    assert mergeRegionFs != null;
    mergeRegionFs.commitMergedRegion(mergedRegion);

    // Prepare to create merged regions
    env.getAssignmentManager().getRegionStates().
        getOrCreateRegionStateNode(mergedRegion).setState(State.MERGING_NEW);
  }

  /**
   * Create reference file(s) to parent region hfiles in the <code>mergeDir</code>
   * @param regionFs merge parent region file system
   * @param mergeDir the temp directory in which we are accumulating references.
   */
  private void mergeStoreFiles(final MasterProcedureEnv env, final HRegionFileSystem regionFs,
      final Path mergeDir) throws IOException {
    final TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
      String family = hcd.getNameAsString();
      final Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family);
      if (storeFiles != null && storeFiles.size() > 0) {
        for (StoreFileInfo storeFileInfo : storeFiles) {
          // Create reference file(s) to parent region file here in mergedDir.
          // As this procedure is running on master, use CacheConfig.DISABLED means
          // don't cache any block.
          regionFs.mergeStoreFile(mergedRegion, family, new HStoreFile(
              storeFileInfo, hcd.getBloomFilterType(), CacheConfig.DISABLED), mergeDir);
        }
      }
    }
  }

  /**
   * Clean up a merged region on rollback after failure.
   */
  private void cleanupMergedRegion(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    TableName tn = this.regionsToMerge[0].getTable();
    final Path tabledir = CommonFSUtils.getTableDir(mfs.getRootDir(), tn);
    final FileSystem fs = mfs.getFileSystem();
    // See createMergedRegion above where we specify the merge dir as being in the
    // FIRST merge parent region.
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[0], false);
    regionFs.cleanupMergedRegion(mergedRegion);
  }

  /**
   * Rollback close regions
   **/
  private void rollbackCloseRegionsForMerge(MasterProcedureEnv env) throws IOException {
    AssignmentManagerUtil.reopenRegionsForRollback(env, Arrays.asList(regionsToMerge),
      getRegionReplication(env), getServerName(env));
  }

  private TransitRegionStateProcedure[] createUnassignProcedures(MasterProcedureEnv env)
      throws IOException {
    return AssignmentManagerUtil.createUnassignProceduresForSplitOrMerge(env,
      Stream.of(regionsToMerge), getRegionReplication(env));
  }

  private TransitRegionStateProcedure[] createAssignProcedures(MasterProcedureEnv env)
      throws IOException {
    return AssignmentManagerUtil.createAssignProceduresForOpeningNewRegions(env,
      Collections.singletonList(mergedRegion), getRegionReplication(env), getServerName(env));
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    return env.getMasterServices().getTableDescriptors().get(getTableName()).
        getRegionReplication();
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void preMergeRegionsCommit(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      @MetaMutationAnnotation
      final List<Mutation> metaEntries = new ArrayList<>();
      cpHost.preMergeRegionsCommit(regionsToMerge, metaEntries, getUser());
      try {
        for (Mutation p : metaEntries) {
          RegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("Row key of mutation from coprocessor is not parsable as region name. "
          + "Mutations from coprocessor should only be for hbase:meta table.", e);
        throw e;
      }
    }
  }

  /**
   * Add merged region to META and delete original regions.
   */
  private void updateMetaForMergedRegions(final MasterProcedureEnv env) throws IOException {
    env.getAssignmentManager().markRegionAsMerged(mergedRegion, getServerName(env),
        this.regionsToMerge);
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void postMergeRegionsCommit(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postMergeRegionsCommit(regionsToMerge, mergedRegion, getUser());
    }
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void postCompletedMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postCompletedMergeRegionsAction(regionsToMerge, mergedRegion, getUser());
    }
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return serverName
   */
  private ServerName getServerName(final MasterProcedureEnv env) {
    if (regionLocation == null) {
      regionLocation = env.getAssignmentManager().getRegionStates().
          getRegionServerOfRegion(regionsToMerge[0]);
      // May still be null here but return null and let caller deal.
      // Means we lost the in-memory-only location. We are in recovery
      // or so. The caller should be able to deal w/ a null ServerName.
      // Let them go to the Balancer to find one to use instead.
    }
    return regionLocation;
  }

  private void writeMaxSequenceIdFile(MasterProcedureEnv env) throws IOException {
    MasterFileSystem fs = env.getMasterFileSystem();
    long maxSequenceId = -1L;
    for (RegionInfo region : regionsToMerge) {
      maxSequenceId =
        Math.max(maxSequenceId, WALSplitUtil.getMaxRegionSequenceId(env.getMasterConfiguration(),
          region, fs::getFileSystem, fs::getWALFileSystem));
    }
    if (maxSequenceId > 0) {
      WALSplitUtil.writeRegionSequenceIdFile(fs.getWALFileSystem(),
        getWALRegionDir(env, mergedRegion), maxSequenceId);
    }
  }

  /**
   * @return The merged region. Maybe be null if called to early or we failed.
   */
  RegionInfo getMergedRegion() {
    return this.mergedRegion;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // Abort means rollback. We can't rollback all steps. HBASE-18018 added abort to all
    // Procedures. Here is a Procedure that has a PONR and cannot be aborted once it enters this
    // range of steps; what do we do for these should an operator want to cancel them? HBASE-20022.
    return isRollbackSupported(getCurrentState()) && super.abort(env);
  }
}
