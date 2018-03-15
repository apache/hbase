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

package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
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
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

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
  private Boolean traceEnabled = null;
  private RegionInfo daughter_1_RI;
  private RegionInfo daughter_2_RI;
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
    checkSplittable(env, regionToSplit, bestSplitRow);
    final TableName table = regionToSplit.getTable();
    final long rid = getDaughterRegionIdTimestamp(regionToSplit);
    this.daughter_1_RI = RegionInfoBuilder.newBuilder(table)
        .setStartKey(regionToSplit.getStartKey())
        .setEndKey(bestSplitRow)
        .setSplit(false)
        .setRegionId(rid)
        .build();
    this.daughter_2_RI = RegionInfoBuilder.newBuilder(table)
        .setStartKey(bestSplitRow)
        .setEndKey(regionToSplit.getEndKey())
        .setSplit(false)
        .setRegionId(rid)
        .build();
    TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    if(htd.getRegionSplitPolicyClassName() != null) {
      // Since we don't have region reference here, creating the split policy instance without it.
      // This can be used to invoke methods which don't require Region reference. This instantiation
      // of a class on Master-side though it only makes sense on the RegionServer-side is
      // for Phoenix Local Indexing. Refer HBASE-12583 for more information.
      Class<? extends RegionSplitPolicy> clazz =
          RegionSplitPolicy.getSplitPolicyClass(htd, env.getMasterConfiguration());
      this.splitPolicy = ReflectionUtils.newInstance(clazz, env.getMasterConfiguration());
    }
  }

  /**
   * Check whether the region is splittable
   * @param env MasterProcedureEnv
   * @param regionToSplit parent Region to be split
   * @param splitRow if splitRow is not specified, will first try to get bestSplitRow from RS
   * @throws IOException
   */
  private void checkSplittable(final MasterProcedureEnv env,
      final RegionInfo regionToSplit, final byte[] splitRow) throws IOException {
    // Ask the remote RS if this region is splittable.
    // If we get an IOE, report it along w/ the failure so can see why we are not splittable at this time.
    if(regionToSplit.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException ("Can't invoke split on non-default regions directly");
    }
    RegionStateNode node =
        env.getAssignmentManager().getRegionStates().getRegionStateNode(getParentRegion());
    IOException splittableCheckIOE = null;
    boolean splittable = false;
    if (node != null) {
      try {
        if (bestSplitRow == null || bestSplitRow.length == 0) {
          LOG.info("splitKey isn't explicitly specified, " + " will try to find a best split key from RS");
        }
        // Always set bestSplitRow request as true here,
        // need to call Region#checkSplit to check it splittable or not
        GetRegionInfoResponse response =
            Util.getRegionInfoResponse(env, node.getRegionLocation(), node.getRegionInfo(), true);
        if(bestSplitRow == null || bestSplitRow.length == 0) {
          bestSplitRow = response.hasBestSplitRow() ? response.getBestSplitRow().toByteArray() : null;
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
      IOException e = new IOException(regionToSplit.getShortNameToLog() + " NOT splittable");
      if (splittableCheckIOE != null) e.initCause(splittableCheckIOE);
      throw e;
    }

    if(bestSplitRow == null || bestSplitRow.length == 0) {
      throw new DoNotRetryIOException("Region not splittable because bestSplitPoint = null, "
          + "maybe table is too small for auto split. For force split, try specifying split row");
    }

    if (Bytes.equals(regionToSplit.getStartKey(), bestSplitRow)) {
      throw new DoNotRetryIOException(
        "Split row is equal to startkey: " + Bytes.toStringBinary(splitRow));
    }

    if (!regionToSplit.containsRow(bestSplitRow)) {
      throw new DoNotRetryIOException(
        "Split row is not inside region key range splitKey:" + Bytes.toStringBinary(splitRow) +
        " region: " + regionToSplit);
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

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final SplitTableRegionState state)
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
          addChildProcedure(createUnassignProcedures(env, getRegionReplication(env)));
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS);
          break;
        case SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS:
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
          addChildProcedure(createAssignProcedures(env, getRegionReplication(env)));
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_POST_OPERATION);
          break;
        case SPLIT_TABLE_REGION_POST_OPERATION:
          postSplitRegion(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      String msg = "Error trying to split region " + getParentRegion().getEncodedName() +
          " in the table " + getTableName() + " (in state=" + state + ")";
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retry.
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
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }

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
          // Doing nothing, as re-open parent region would clean up daughter region directories.
          break;
        case SPLIT_TABLE_REGION_CLOSE_PARENT_REGION:
          openParentRegion(env);
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
        .addChildRegionInfo(ProtobufUtil.toRegionInfo(daughter_1_RI))
        .addChildRegionInfo(ProtobufUtil.toRegionInfo(daughter_2_RI));
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
    daughter_1_RI = ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getChildRegionInfo(0));
    daughter_2_RI = ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getChildRegionInfo(1));
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
    sb.append(", parent=");
    sb.append(getParentRegion().getShortNameToLog());
    sb.append(", daughterA=");
    sb.append(daughter_1_RI.getShortNameToLog());
    sb.append(", daughterB=");
    sb.append(daughter_2_RI.getShortNameToLog());
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
    return daughter_2_RI.getStartKey();
  }

  private static final State[] EXPECTED_SPLIT_STATES = new State[] { State.OPEN, State.CLOSED };

  /**
   * Prepare to Split region.
   * @param env MasterProcedureEnv
   */
  @VisibleForTesting
  public boolean prepareSplitRegion(final MasterProcedureEnv env) throws IOException {
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

    // Since we have the lock and the master is coordinating the operation
    // we are always able to split the region
    if (!env.getMasterServices().isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
      LOG.warn("pid=" + getProcId() + " split switch is off! skip split of " + parentHRI);
      setFailure(new IOException("Split region " + parentHRI.getRegionNameAsString() +
          " failed due to split switch off"));
      return false;
    }

    // set node state as SPLITTING
    node.setState(State.SPLITTING);

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
      env.getAssignmentManager().getRegionNormalizer().planSkipped(this.getParentRegion(),
          NormalizationPlan.PlanType.SPLIT);
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
   * @param env MasterProcedureEnv
   */
  private void openParentRegion(final MasterProcedureEnv env) throws IOException {
    // Check whether the region is closed; if so, open it in the same server
    final int regionReplication = getRegionReplication(env);
    final ServerName serverName = getParentRegionServerName(env);

    final AssignProcedure[] procs = new AssignProcedure[regionReplication];
    for (int i = 0; i < regionReplication; ++i) {
      final RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(getParentRegion(), i);
      procs[i] = env.getAssignmentManager().createAssignProcedure(hri, serverName);
    }
    env.getMasterServices().getMasterProcedureExecutor().submitProcedures(procs);
  }

  /**
   * Create daughter regions
   * @param env MasterProcedureEnv
   */
  @VisibleForTesting
  public void createDaughterRegions(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = FSUtils.getTableDir(mfs.getRootDir(), getTableName());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, getParentRegion(), false);
    regionFs.createSplitsDir();

    Pair<Integer, Integer> expectedReferences = splitStoreFiles(env, regionFs);

    assertReferenceFileCount(fs, expectedReferences.getFirst(),
      regionFs.getSplitsDir(daughter_1_RI));
    //Move the files from the temporary .splits to the final /table/region directory
    regionFs.commitDaughterRegion(daughter_1_RI);
    assertReferenceFileCount(fs, expectedReferences.getFirst(),
      new Path(tabledir, daughter_1_RI.getEncodedName()));

    assertReferenceFileCount(fs, expectedReferences.getSecond(),
      regionFs.getSplitsDir(daughter_2_RI));
    regionFs.commitDaughterRegion(daughter_2_RI);
    assertReferenceFileCount(fs, expectedReferences.getSecond(),
      new Path(tabledir, daughter_2_RI.getEncodedName()));
  }

  /**
   * Create Split directory
   * @param env MasterProcedureEnv
   */
  private Pair<Integer, Integer> splitStoreFiles(final MasterProcedureEnv env,
      final HRegionFileSystem regionFs) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Configuration conf = env.getMasterConfiguration();
    // The following code sets up a thread pool executor with as many slots as
    // there's files to split. It then fires up everything, waits for
    // completion and finally checks for any exception
    //
    // Note: splitStoreFiles creates daughter region dirs under the parent splits dir
    // Nothing to unroll here if failure -- re-run createSplitsDir will
    // clean this up.
    int nbFiles = 0;
    final Map<String, Collection<StoreFileInfo>> files =
      new HashMap<String, Collection<StoreFileInfo>>(regionFs.getFamilies().size());
    for (String family: regionFs.getFamilies()) {
      Collection<StoreFileInfo> sfis = regionFs.getStoreFiles(family);
      if (sfis == null) continue;
      Collection<StoreFileInfo> filteredSfis = null;
      for (StoreFileInfo sfi: sfis) {
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
      return new Pair<Integer, Integer>(0,0);
    }
    // Max #threads is the smaller of the number of storefiles or the default max determined above.
    int maxThreads = Math.min(
      conf.getInt(HConstants.REGION_SPLIT_THREADS_MAX,
        conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT)),
      nbFiles);
    LOG.info("pid=" + getProcId() + " splitting " + nbFiles + " storefiles, region=" +
      getParentRegion().getShortNameToLog() + ", threads=" + maxThreads);
    final ExecutorService threadPool = Executors.newFixedThreadPool(
      maxThreads, Threads.getNamedThreadFactory("StoreFileSplitter-%1$d"));
    final List<Future<Pair<Path,Path>>> futures = new ArrayList<Future<Pair<Path,Path>>>(nbFiles);

    TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    // Split each store file.
    for (Map.Entry<String, Collection<StoreFileInfo>>e: files.entrySet()) {
      byte [] familyName = Bytes.toBytes(e.getKey());
      final ColumnFamilyDescriptor hcd = htd.getColumnFamily(familyName);
      final Collection<StoreFileInfo> storeFiles = e.getValue();
      if (storeFiles != null && storeFiles.size() > 0) {
        final CacheConfig cacheConf = new CacheConfig(conf, hcd);
        for (StoreFileInfo storeFileInfo: storeFiles) {
          StoreFileSplitter sfs =
              new StoreFileSplitter(regionFs, familyName, new HStoreFile(mfs.getFileSystem(),
                  storeFileInfo, conf, cacheConf, hcd.getBloomFilterType(), true));
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
        throw new IOException("Took too long to split the" +
            " files and create the references, aborting split");
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }

    int daughterA = 0;
    int daughterB = 0;
    // Look for any exception
    for (Future<Pair<Path, Path>> future : futures) {
      try {
        Pair<Path, Path> p = future.get();
        daughterA += p.getFirst() != null ? 1 : 0;
        daughterB += p.getSecond() != null ? 1 : 0;
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("pid=" + getProcId() + " split storefiles for region " +
        getParentRegion().getShortNameToLog() +
          " Daughter A: " + daughterA + " storefiles, Daughter B: " +
          daughterB + " storefiles.");
    }
    return new Pair<Integer, Integer>(daughterA, daughterB);
  }

  private void assertReferenceFileCount(final FileSystem fs, final int expectedReferenceFileCount,
      final Path dir) throws IOException {
    if (expectedReferenceFileCount != 0 &&
        expectedReferenceFileCount != FSUtils.getRegionReferenceFileCount(fs, dir)) {
      throw new IOException("Failing split. Expected reference file count isn't equal.");
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
    final Path path_first = regionFs.splitStoreFile(this.daughter_1_RI, familyName, sf, splitRow,
        false, splitPolicy);
    final Path path_second = regionFs.splitStoreFile(this.daughter_2_RI, familyName, sf, splitRow,
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
      daughter_1_RI, daughter_2_RI);
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
      cpHost.postCompletedSplitRegionAction(daughter_1_RI, daughter_2_RI, getUser());
    }
  }

  private ServerName getParentRegionServerName(final MasterProcedureEnv env) {
    return env.getMasterServices().getAssignmentManager()
      .getRegionStates().getRegionServerOfRegion(getParentRegion());
  }

  private UnassignProcedure[] createUnassignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final UnassignProcedure[] procs = new UnassignProcedure[regionReplication];
    for (int i = 0; i < procs.length; ++i) {
      final RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(getParentRegion(), i);
      procs[i] = env.getAssignmentManager().
          createUnassignProcedure(hri, null, true, !RegionReplicaUtil.isDefaultReplica(hri));
    }
    return procs;
  }

  private AssignProcedure[] createAssignProcedures(final MasterProcedureEnv env,
      final int regionReplication) {
    final ServerName targetServer = getParentRegionServerName(env);
    final AssignProcedure[] procs = new AssignProcedure[regionReplication * 2];
    int procsIdx = 0;
    for (int i = 0; i < regionReplication; ++i) {
      final RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(daughter_1_RI, i);
      procs[procsIdx++] = env.getAssignmentManager().createAssignProcedure(hri, targetServer);
    }
    for (int i = 0; i < regionReplication; ++i) {
      final RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(daughter_2_RI, i);
      procs[procsIdx++] = env.getAssignmentManager().createAssignProcedure(hri, targetServer);
    }
    return procs;
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    final TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    return htd.getRegionReplication();
  }

  private void writeMaxSequenceIdFile(MasterProcedureEnv env) throws IOException {
    FileSystem fs = env.getMasterServices().getMasterFileSystem().getFileSystem();
    long maxSequenceId =
      WALSplitter.getMaxRegionSequenceId(fs, getRegionDir(env, getParentRegion()));
    if (maxSequenceId > 0) {
      WALSplitter.writeRegionSequenceIdFile(fs, getRegionDir(env, daughter_1_RI), maxSequenceId);
      WALSplitter.writeRegionSequenceIdFile(fs, getRegionDir(env, daughter_2_RI), maxSequenceId);
    }
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return traceEnabled
   */
  private boolean isTraceEnabled() {
    if (traceEnabled == null) {
      traceEnabled = LOG.isTraceEnabled();
    }
    return traceEnabled;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // Abort means rollback. We can't rollback all steps. HBASE-18018 added abort to all
    // Procedures. Here is a Procedure that has a PONR and cannot be aborted wants it enters this
    // range of steps; what do we do for these should an operator want to cancel them? HBASE-20022.
    return isRollbackSupported(getCurrentState())? super.abort(env): false;
  }
}
