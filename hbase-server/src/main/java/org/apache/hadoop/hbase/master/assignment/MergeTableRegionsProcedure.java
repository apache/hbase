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
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.CatalogJanitor;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.TableQueue;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MergeTableRegionsState;

/**
 * The procedure to Merge a region in a table.
 * This procedure takes an exclusive table lock since it is working over multiple regions.
 * It holds the lock for the life of the procedure.
 * <p>Throws exception on construction if determines context hostile to merge (cluster going
 * down or master is shutting down or table is disabled).</p>
 */
@InterfaceAudience.Private
public class MergeTableRegionsProcedure
    extends AbstractStateMachineTableProcedure<MergeTableRegionsState> {
  private static final Logger LOG = LoggerFactory.getLogger(MergeTableRegionsProcedure.class);
  private Boolean traceEnabled;
  private ServerName regionLocation;
  private RegionInfo[] regionsToMerge;
  private RegionInfo mergedRegion;
  private boolean forcible;

  public MergeTableRegionsProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public MergeTableRegionsProcedure(final MasterProcedureEnv env,
      final RegionInfo regionToMergeA, final RegionInfo regionToMergeB) throws IOException {
    this(env, regionToMergeA, regionToMergeB, false);
  }

  public MergeTableRegionsProcedure(final MasterProcedureEnv env,
      final RegionInfo regionToMergeA, final RegionInfo regionToMergeB,
      final boolean forcible) throws IOException {
    this(env, new RegionInfo[] {regionToMergeA, regionToMergeB}, forcible);
  }

  public MergeTableRegionsProcedure(final MasterProcedureEnv env,
      final RegionInfo[] regionsToMerge, final boolean forcible)
      throws IOException {
    super(env);

    // Check daughter regions and make sure that we have valid daughter regions
    // before doing the real work. This check calls the super method #checkOnline also.
    checkRegionsToMerge(env, regionsToMerge, forcible);

    // WARN: make sure there is no parent region of the two merging regions in
    // hbase:meta If exists, fixing up daughters would cause daughter regions(we
    // have merged one) online again when we restart master, so we should clear
    // the parent region to prevent the above case
    // Since HBASE-7721, we don't need fix up daughters any more. so here do nothing
    this.regionsToMerge = regionsToMerge;
    this.mergedRegion = createMergedRegionInfo(regionsToMerge);
    preflightChecks(env, true);
    this.forcible = forcible;
  }

  private static void checkRegionsToMerge(MasterProcedureEnv env, final RegionInfo[] regionsToMerge,
      final boolean forcible) throws MergeRegionException {
    // For now, we only merge 2 regions.
    // It could be extended to more than 2 regions in the future.
    if (regionsToMerge == null || regionsToMerge.length != 2) {
      throw new MergeRegionException("Expected to merge 2 regions, got: " +
        Arrays.toString(regionsToMerge));
    }

    checkRegionsToMerge(env, regionsToMerge[0], regionsToMerge[1], forcible);
  }

  /**
   * One time checks.
   */
  private static void checkRegionsToMerge(MasterProcedureEnv env, final RegionInfo regionToMergeA,
      final RegionInfo regionToMergeB, final boolean forcible) throws MergeRegionException {
    if (!regionToMergeA.getTable().equals(regionToMergeB.getTable())) {
      throw new MergeRegionException("Can't merge regions from two different tables: " +
        regionToMergeA + ", " + regionToMergeB);
    }

    if (regionToMergeA.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID ||
        regionToMergeB.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      throw new MergeRegionException("Can't merge non-default replicas");
    }

    try {
      checkOnline(env, regionToMergeA);
      checkOnline(env, regionToMergeB);
    } catch (DoNotRetryRegionException dnrre) {
      throw new MergeRegionException(dnrre);
    }

    if (!RegionInfo.areAdjacent(regionToMergeA, regionToMergeB)) {
      String msg = "Unable to merge non-adjacent regions " + regionToMergeA.getShortNameToLog() +
          ", " + regionToMergeB.getShortNameToLog() + " where forcible = " + forcible;
      LOG.warn(msg);
      if (!forcible) {
        throw new MergeRegionException(msg);
      }
    }
  }


  private static RegionInfo createMergedRegionInfo(final RegionInfo[] regionsToMerge) {
    return createMergedRegionInfo(regionsToMerge[0], regionsToMerge[1]);
  }

  /**
   * Create merged region info through the specified two regions
   */
  private static RegionInfo createMergedRegionInfo(final RegionInfo regionToMergeA,
      final RegionInfo regionToMergeB) {
    // Choose the smaller as start key
    final byte[] startKey;
    if (RegionInfo.COMPARATOR.compare(regionToMergeA, regionToMergeB) <= 0) {
      startKey = regionToMergeA.getStartKey();
    } else {
      startKey = regionToMergeB.getStartKey();
    }

    // Choose the bigger as end key
    final byte[] endKey;
    if (Bytes.equals(regionToMergeA.getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
        || (!Bytes.equals(regionToMergeB.getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
            && Bytes.compareTo(regionToMergeA.getEndKey(), regionToMergeB.getEndKey()) > 0)) {
      endKey = regionToMergeA.getEndKey();
    } else {
      endKey = regionToMergeB.getEndKey();
    }

    // Merged region is sorted between two merging regions in META
    return RegionInfoBuilder.newBuilder(regionToMergeA.getTable())
        .setStartKey(startKey)
        .setEndKey(endKey)
        .setSplit(false)
        .setRegionId(getMergedRegionIdTimestamp(regionToMergeA, regionToMergeB))
        .build();
  }

  private static long getMergedRegionIdTimestamp(final RegionInfo regionToMergeA,
      final RegionInfo regionToMergeB) {
    long rid = EnvironmentEdgeManager.currentTime();
    // Region Id is a timestamp. Merged region's id can't be less than that of
    // merging regions else will insert at wrong location in hbase:meta (See HBASE-710).
    if (rid < regionToMergeA.getRegionId() || rid < regionToMergeB.getRegionId()) {
      LOG.warn("Clock skew; merging regions id are " + regionToMergeA.getRegionId()
          + " and " + regionToMergeB.getRegionId() + ", but current time here is " + rid);
      rid = Math.max(regionToMergeA.getRegionId(), regionToMergeB.getRegionId()) + 1;
    }
    return rid;
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
          addChildProcedure(createUnassignProcedures(env, getRegionReplication(env)));
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CHECK_CLOSED_REGIONS);
          break;
        case MERGE_TABLE_REGIONS_CHECK_CLOSED_REGIONS:
          List<RegionInfo> ris = hasRecoveredEdits(env);
          if (ris.isEmpty()) {
            setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CREATE_MERGED_REGION);
          } else {
            // Need to reopen parent regions to pickup missed recovered.edits. Do it by creating
            // child assigns and then stepping back to MERGE_TABLE_REGIONS_CLOSE_REGIONS.
            // Just assign the primary regions recovering the missed recovered.edits -- no replicas.
            // May need to cycle here a few times if heavy writes.
            // TODO: Add an assign read-only.
            for (RegionInfo ri: ris) {
              LOG.info("Found recovered.edits under {}, reopen to pickup missed edits!", ri);
              addChildProcedure(env.getAssignmentManager().createAssignProcedure(ri));
            }
            setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CLOSE_REGIONS);
          }
          break;
        case MERGE_TABLE_REGIONS_CREATE_MERGED_REGION:
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
          addChildProcedure(createAssignProcedures(env, getRegionReplication(env)));
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_POST_OPERATION);
          break;
        case MERGE_TABLE_REGIONS_POST_OPERATION:
          postCompletedMergeRegions(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      String msg = "Error trying to merge regions " +
        RegionInfo.getShortNameToLog(regionsToMerge) + " in the table " + getTableName() +
           " (in state=" + state + ")";
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
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }

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
        .setForcible(forcible);
    for (int i = 0; i < regionsToMerge.length; ++i) {
      mergeTableRegionsMsg.addRegionInfo(ProtobufUtil.toRegionInfo(regionsToMerge[i]));
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
    sb.append(", forcibly=");
    sb.append(forcible);
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitRegions(this, getTableName(),
        mergedRegion, regionsToMerge[0], regionsToMerge[1])) {
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
    env.getProcedureScheduler().wakeRegions(this, getTableName(),
      mergedRegion, regionsToMerge[0], regionsToMerge[1]);
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
   * Return list of regions that have recovered.edits... usually its an empty list.
   * @param env the master env
   * @throws IOException IOException
   */
  private List<RegionInfo> hasRecoveredEdits(final MasterProcedureEnv env) throws IOException {
    List<RegionInfo> ris =  new ArrayList<RegionInfo>(regionsToMerge.length);
    for (int i = 0; i < regionsToMerge.length; i++) {
      RegionInfo ri = regionsToMerge[i];
      if (SplitTableRegionProcedure.hasRecoveredEdits(env, ri)) {
        ris.add(ri);
      }
    }
    return ris;
  }

  /**
   * Prepare merge and do some check
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private boolean prepareMergeRegion(final MasterProcedureEnv env) throws IOException {
    // Fail if we are taking snapshot for the given table
    if (env.getMasterServices().getSnapshotManager()
      .isTakingSnapshot(regionsToMerge[0].getTable())) {
      throw new MergeRegionException(
        "Skip merging regions " + RegionInfo.getShortNameToLog(regionsToMerge) +
          ", because we are taking snapshot for the table " + regionsToMerge[0].getTable());
    }
    // Note: the following logic assumes that we only have 2 regions to merge.  In the future,
    // if we want to extend to more than 2 regions, the code needs to be modified a little bit.
    CatalogJanitor catalogJanitor = env.getMasterServices().getCatalogJanitor();
    boolean regionAHasMergeQualifier = !catalogJanitor.cleanMergeQualifier(regionsToMerge[0]);
    if (regionAHasMergeQualifier
        || !catalogJanitor.cleanMergeQualifier(regionsToMerge[1])) {
      String msg = "Skip merging regions " + RegionInfo.getShortNameToLog(regionsToMerge) +
        ", because region "
        + (regionAHasMergeQualifier ? regionsToMerge[0].getEncodedName() : regionsToMerge[1]
              .getEncodedName()) + " has merge qualifier";
      LOG.warn(msg);
      throw new MergeRegionException(msg);
    }

    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    RegionState regionStateA = regionStates.getRegionState(regionsToMerge[0].getEncodedName());
    RegionState regionStateB = regionStates.getRegionState(regionsToMerge[1].getEncodedName());
    if (regionStateA == null || regionStateB == null) {
      throw new UnknownRegionException(
        regionStateA == null ?
            regionsToMerge[0].getEncodedName() : regionsToMerge[1].getEncodedName());
    }

    if (!regionStateA.isOpened() || !regionStateB.isOpened()) {
      throw new MergeRegionException(
        "Unable to merge regions that are not online " + regionStateA + ", " + regionStateB);
    }

    if (!env.getMasterServices().isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
      String regionsStr = Arrays.deepToString(regionsToMerge);
      LOG.warn("merge switch is off! skip merge of " + regionsStr);
      super.setFailure(getClass().getSimpleName(),
          new IOException("Merge of " + regionsStr + " failed because merge switch is off"));
      return false;
    }
    // See HBASE-21395, for 2.0.x and 2.1.x only.
    // A safe fence here, if there is a table procedure going on, abort the merge.
    // There some cases that may lead to table procedure roll back (more serious
    // than roll back the merge procedure here), or the merged regions was brought online
    // by the table procedure because of the race between merge procedure and table procedure
    List<AbstractStateMachineTableProcedure> tableProcedures = env
        .getMasterServices().getProcedures().stream()
        .filter(p -> p instanceof AbstractStateMachineTableProcedure)
        .map(p -> (AbstractStateMachineTableProcedure) p)
        .filter(p -> p.getProcId() != this.getProcId() && p.getTableName()
            .equals(regionsToMerge[0].getTable()) && !p.isFinished()
            && TableQueue.requireTableExclusiveLock(p))
        .collect(Collectors.toList());
    if (tableProcedures != null && tableProcedures.size() > 0) {
      throw new MergeRegionException(tableProcedures.get(0).toString()
          + " is going on against the same table, abort the merge of " + this
          .toString());
    }

    // Ask the remote regionserver if regions are mergeable. If we get an IOE, report it
    // along with the failure, so we can see why regions are not mergeable at this time.
    IOException mergeableCheckIOE = null;
    boolean mergeable = false;
    RegionState current = regionStateA;
    try {
      mergeable = isMergeable(env, current);
    } catch (IOException e) {
      mergeableCheckIOE = e;
    }
    if (mergeable && mergeableCheckIOE == null) {
      current = regionStateB;
      try {
        mergeable = isMergeable(env, current);
      } catch (IOException e) {
        mergeableCheckIOE = e;
      }
    }
    if (!mergeable) {
      IOException e = new IOException(current.getRegion().getShortNameToLog() + " NOT mergeable");
      if (mergeableCheckIOE != null) e.initCause(mergeableCheckIOE);
      super.setFailure(getClass().getSimpleName(), e);
      return false;
    }

    // Update region states to Merging
    setRegionStateToMerging(env);
    return true;
  }

  private boolean isMergeable(final MasterProcedureEnv env, final RegionState rs)
  throws IOException {
    GetRegionInfoResponse response =
      Util.getRegionInfoResponse(env, rs.getServerName(), rs.getRegion());
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
      env.getMasterServices().getRegionNormalizer().planSkipped(this.mergedRegion,
          NormalizationPlan.PlanType.MERGE);
      throw e;
    }
  }

  /**
   * Action after rollback a merge table regions action.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void postRollBackMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postRollBackMergeRegionsAction(regionsToMerge, getUser());
    }
  }

  /**
   * Set the region states to MERGING state
   * @param env MasterProcedureEnv
   */
  public void setRegionStateToMerging(final MasterProcedureEnv env) {
    // Set State.MERGING to regions to be merged
    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    regionStates.getRegionStateNode(regionsToMerge[0]).setState(State.MERGING);
    regionStates.getRegionStateNode(regionsToMerge[1]).setState(State.MERGING);
  }

  /**
   * Create a merged region
   * @param env MasterProcedureEnv
   */
  private void createMergedRegion(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = FSUtils.getTableDir(mfs.getRootDir(), regionsToMerge[0].getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[0], false);
    regionFs.createMergesDir();

    mergeStoreFiles(env, regionFs, regionFs.getMergesDir());
    HRegionFileSystem regionFs2 = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[1], false);
    mergeStoreFiles(env, regionFs2, regionFs.getMergesDir());

    regionFs.commitMergedRegion(mergedRegion);

    //Prepare to create merged regions
    env.getAssignmentManager().getRegionStates().
        getOrCreateRegionStateNode(mergedRegion).setState(State.MERGING_NEW);
  }

  /**
   * Create reference file(s) of merging regions under the merged directory
   * @param env MasterProcedureEnv
   * @param regionFs region file system
   * @param mergedDir the temp directory of merged region
   */
  private void mergeStoreFiles(
      final MasterProcedureEnv env, final HRegionFileSystem regionFs, final Path mergedDir)
      throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Configuration conf = env.getMasterConfiguration();
    final TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());

    for (String family: regionFs.getFamilies()) {
      final ColumnFamilyDescriptor hcd = htd.getColumnFamily(Bytes.toBytes(family));
      final Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family);

      if (storeFiles != null && storeFiles.size() > 0) {
        final CacheConfig cacheConf = new CacheConfig(conf, hcd);
        for (StoreFileInfo storeFileInfo: storeFiles) {
          // Create reference file(s) of the region in mergedDir
          regionFs.mergeStoreFile(mergedRegion, family, new HStoreFile(mfs.getFileSystem(),
              storeFileInfo, conf, cacheConf, hcd.getBloomFilterType(), true),
            mergedDir);
        }
      }
    }
  }

  /**
   * Clean up a merged region
   * @param env MasterProcedureEnv
   */
  private void cleanupMergedRegion(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = FSUtils.getTableDir(mfs.getRootDir(), regionsToMerge[0].getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[0], false);
    regionFs.cleanupMergedRegion(mergedRegion);
  }

  /**
   * Rollback close regions
   * @param env MasterProcedureEnv
   **/
  private void rollbackCloseRegionsForMerge(final MasterProcedureEnv env) throws IOException {
    // Check whether the region is closed; if so, open it in the same server
    final int regionReplication = getRegionReplication(env);
    final ServerName serverName = getServerName(env);

    AssignProcedure[] procs =
        createAssignProcedures(regionReplication, env, Arrays.asList(regionsToMerge), serverName);
    env.getMasterServices().getMasterProcedureExecutor().submitProcedures(procs);
  }
  
  private AssignProcedure[] createAssignProcedures(final int regionReplication,
      final MasterProcedureEnv env, final List<RegionInfo> hris, final ServerName serverName) {
    final AssignProcedure[] procs = new AssignProcedure[hris.size() * regionReplication];
    int procsIdx = 0;
    for (int i = 0; i < hris.size(); ++i) {
      // create procs for the primary region with the target server.
      final RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(hris.get(i), 0);
      procs[procsIdx++] = env.getAssignmentManager().createAssignProcedure(hri, serverName);
    }
    if (regionReplication > 1) {
      List<RegionInfo> regionReplicas =
          new ArrayList<RegionInfo>(hris.size() * (regionReplication - 1));
      for (int i = 0; i < hris.size(); ++i) {
        // We don't include primary replica here
        for (int j = 1; j < regionReplication; ++j) {
          regionReplicas.add(RegionReplicaUtil.getRegionInfoForReplica(hris.get(i), j));
        }
      }
      // for the replica regions exclude the primary region's server and call LB's roundRobin
      // assignment
      AssignProcedure[] replicaAssignProcs = env.getAssignmentManager()
          .createRoundRobinAssignProcedures(regionReplicas, Collections.singletonList(serverName));
      for (AssignProcedure proc : replicaAssignProcs) {
        procs[procsIdx++] = proc;
      }
    }
    return procs;
  }

  private UnassignProcedure[] createUnassignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final UnassignProcedure[] procs =
        new UnassignProcedure[regionsToMerge.length * regionReplication];
    int procsIdx = 0;
    for (int i = 0; i < regionsToMerge.length; ++i) {
      for (int j = 0; j < regionReplication; ++j) {
        final RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(regionsToMerge[i], j);
        procs[procsIdx++] = env.getAssignmentManager().
            createUnassignProcedure(hri, null, true, !RegionReplicaUtil.isDefaultReplica(hri));
      }
    }
    return procs;
  }

  private AssignProcedure[] createAssignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final ServerName targetServer = getServerName(env);
    return createAssignProcedures(regionReplication, env, Collections.singletonList(mergedRegion),
      targetServer);
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    final TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    return htd.getRegionReplication();
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
    final ServerName serverName = getServerName(env);
    env.getAssignmentManager().markRegionAsMerged(mergedRegion, serverName,
      regionsToMerge[0], regionsToMerge[1]);
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
          Math.max(maxSequenceId, WALSplitter.getMaxRegionSequenceId(env.getMasterConfiguration(),
            region, fs::getFileSystem, fs::getWALFileSystem));
    }
    if (maxSequenceId > 0) {
      WALSplitter.writeRegionSequenceIdFile(fs.getWALFileSystem(),
        getWALRegionDir(env, mergedRegion), maxSequenceId);
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

  /**
   * @return The merged region. Maybe be null if called to early or we failed.
   */
  @VisibleForTesting
  public RegionInfo getMergedRegion() {
    return this.mergedRegion;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // Abort means rollback. We can't rollback all steps. HBASE-18018 added abort to all
    // Procedures. Here is a Procedure that has a PONR and cannot be aborted once it enters this
    // range of steps; what do we do for these should an operator want to cancel them? HBASE-20022.
    return isRollbackSupported(getCurrentState())? super.abort(env): false;
  }
}
