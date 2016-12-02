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
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.CatalogJanitor;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MergeTableRegionsState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * The procedure to Merge a region in a table.
 */
@InterfaceAudience.Private
public class MergeTableRegionsProcedure
    extends AbstractStateMachineTableProcedure<MergeTableRegionsState> {
  private static final Log LOG = LogFactory.getLog(MergeTableRegionsProcedure.class);

  private Boolean traceEnabled;
  private AssignmentManager assignmentManager;
  private int timeout;
  private ServerName regionLocation;
  private String regionsToMergeListFullName;
  private String regionsToMergeListEncodedName;

  private HRegionInfo [] regionsToMerge;
  private HRegionInfo mergedRegionInfo;
  private boolean forcible;

  public MergeTableRegionsProcedure() {
    this.traceEnabled = isTraceEnabled();
    this.assignmentManager = null;
    this.timeout = -1;
    this.regionLocation = null;
    this.regionsToMergeListFullName = null;
    this.regionsToMergeListEncodedName = null;
  }

  public MergeTableRegionsProcedure(
      final MasterProcedureEnv env,
      final HRegionInfo[] regionsToMerge,
      final boolean forcible) throws IOException {
    super(env);
    this.traceEnabled = isTraceEnabled();
    this.assignmentManager = getAssignmentManager(env);
    // For now, we only merge 2 regions.  It could be extended to more than 2 regions in
    // the future.
    assert(regionsToMerge.length == 2);
    assert(regionsToMerge[0].getTable() == regionsToMerge[1].getTable());
    this.regionsToMerge = regionsToMerge;
    this.forcible = forcible;

    this.timeout = -1;
    this.regionsToMergeListFullName = getRegionsToMergeListFullNameString();
    this.regionsToMergeListEncodedName = getRegionsToMergeListEncodedNameString();

    // Check daughter regions and make sure that we have valid daughter regions before
    // doing the real work.
    checkDaughterRegions();
    // WARN: make sure there is no parent region of the two merging regions in
    // hbase:meta If exists, fixing up daughters would cause daughter regions(we
    // have merged one) online again when we restart master, so we should clear
    // the parent region to prevent the above case
    // Since HBASE-7721, we don't need fix up daughters any more. so here do
    // nothing
    setupMergedRegionInfo();
  }

  @Override
  protected Flow executeFromState(
      final MasterProcedureEnv env,
      final MergeTableRegionsState state) throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case MERGE_TABLE_REGIONS_PREPARE:
        prepareMergeRegion(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_MOVE_REGION_TO_SAME_RS);
        break;
      case MERGE_TABLE_REGIONS_MOVE_REGION_TO_SAME_RS:
        if (MoveRegionsToSameRS(env)) {
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION);
        } else {
          LOG.info("Cancel merging regions " + getRegionsToMergeListFullNameString()
            + ", because can't move them to the same RS");
          setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_POST_OPERATION);
        }
        break;
      case MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION:
        preMergeRegions(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_SET_MERGING_TABLE_STATE);
        break;
      case MERGE_TABLE_REGIONS_SET_MERGING_TABLE_STATE:
        setRegionStateToMerging(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CLOSE_REGIONS);
        break;
      case MERGE_TABLE_REGIONS_CLOSE_REGIONS:
        closeRegionsForMerge(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_CREATE_MERGED_REGION);
        break;
      case MERGE_TABLE_REGIONS_CREATE_MERGED_REGION:
        createMergedRegion(env);
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
        openMergedRegions(env);
        setNextState(MergeTableRegionsState.MERGE_TABLE_REGIONS_POST_OPERATION);
        break;
      case MERGE_TABLE_REGIONS_POST_OPERATION:
        postCompletedMergeRegions(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.warn("Error trying to merge regions " + getRegionsToMergeListFullNameString() +
        " in the table " + getTableName() + " (in state=" + state + ")", e);

      setFailure("master-merge-regions", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      final MasterProcedureEnv env,
      final MergeTableRegionsState state) throws IOException, InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }

    try {
      switch (state) {
      case MERGE_TABLE_REGIONS_POST_OPERATION:
      case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
      case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
      case MERGE_TABLE_REGIONS_UPDATE_META:
        String msg = this + " We are in the " + state + " state."
            + " It is complicated to rollback the merge operation that region server is working on."
            + " Rollback is not supported and we should let the merge operation to complete";
        LOG.warn(msg);
        // PONR
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      case MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION:
        break;
      case MERGE_TABLE_REGIONS_CREATE_MERGED_REGION:
        cleanupMergedRegion(env);
        break;
      case MERGE_TABLE_REGIONS_CLOSE_REGIONS:
        rollbackCloseRegionsForMerge(env);
        break;
      case MERGE_TABLE_REGIONS_SET_MERGING_TABLE_STATE:
        setRegionStateToRevertMerging(env);
        break;
      case MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION:
        postRollBackMergeRegions(env);
        break;
      case MERGE_TABLE_REGIONS_MOVE_REGION_TO_SAME_RS:
        break; // nothing to rollback
      case MERGE_TABLE_REGIONS_PREPARE:
        break; // nothing to rollback
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (Exception e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for merging the regions "
          + getRegionsToMergeListFullNameString() + " in table " + getTableName(), e);
      throw e;
    }
  }

  /*
   * Check whether we are in the state that can be rollback
   */
  @Override
  protected boolean isRollbackSupported(final MergeTableRegionsState state) {
    switch (state) {
    case MERGE_TABLE_REGIONS_POST_OPERATION:
    case MERGE_TABLE_REGIONS_OPEN_MERGED_REGION:
    case MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION:
    case MERGE_TABLE_REGIONS_UPDATE_META:
        // It is not safe to rollback if we reach to these states.
        return false;
      default:
        break;
    }
    return true;
  }

  @Override
  protected MergeTableRegionsState getState(final int stateId) {
    return MergeTableRegionsState.valueOf(stateId);
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
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.MergeTableRegionsStateData.Builder mergeTableRegionsMsg =
        MasterProcedureProtos.MergeTableRegionsStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setMergedRegionInfo(HRegionInfo.convert(mergedRegionInfo))
        .setForcible(forcible);
    for (HRegionInfo hri: regionsToMerge) {
      mergeTableRegionsMsg.addRegionInfo(HRegionInfo.convert(hri));
    }
    mergeTableRegionsMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.MergeTableRegionsStateData mergeTableRegionsMsg =
        MasterProcedureProtos.MergeTableRegionsStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(mergeTableRegionsMsg.getUserInfo()));

    assert(mergeTableRegionsMsg.getRegionInfoCount() == 2);
    regionsToMerge = new HRegionInfo[mergeTableRegionsMsg.getRegionInfoCount()];
    for (int i = 0; i < regionsToMerge.length; i++) {
      regionsToMerge[i] = HRegionInfo.convert(mergeTableRegionsMsg.getRegionInfo(i));
    }

    mergedRegionInfo = HRegionInfo.convert(mergeTableRegionsMsg.getMergedRegionInfo());
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(" regions=");
    sb.append(getRegionsToMergeListFullNameString());
    sb.append(" forcible=");
    sb.append(forcible);
    sb.append(")");
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) {
      return false;
    }
    return !env.getProcedureQueue().waitRegions(
      this, getTableName(), regionsToMerge[0], regionsToMerge[1]);
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().wakeRegions(this, getTableName(), regionsToMerge[0], regionsToMerge[1]);
  }

  @Override
  public TableName getTableName() {
    return regionsToMerge[0].getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.MERGE;
  }

  /**
   * check daughter regions
   * @throws IOException
   */
  private void checkDaughterRegions() throws IOException {
    // Note: the following logic assumes that we only have 2 regions to merge.  In the future,
    // if we want to extend to more than 2 regions, the code needs to modify a little bit.
    //
    if (regionsToMerge[0].getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID ||
        regionsToMerge[1].getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
      throw new MergeRegionException("Can't merge non-default replicas");
    }

    if (!HRegionInfo.areAdjacent(regionsToMerge[0], regionsToMerge[1])) {
      String msg = "Trying to merge non-adjacent regions "
          + getRegionsToMergeListFullNameString() + " where forcible = " + forcible;
      LOG.warn(msg);
      if (!forcible) {
        throw new DoNotRetryIOException(msg);
      }
    }
  }

  /**
   * Prepare merge and do some check
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareMergeRegion(final MasterProcedureEnv env) throws IOException {
    // Note: the following logic assumes that we only have 2 regions to merge.  In the future,
    // if we want to extend to more than 2 regions, the code needs to modify a little bit.
    //
    CatalogJanitor catalogJanitor = env.getMasterServices().getCatalogJanitor();
    boolean regionAHasMergeQualifier = !catalogJanitor.cleanMergeQualifier(regionsToMerge[0]);
    if (regionAHasMergeQualifier
        || !catalogJanitor.cleanMergeQualifier(regionsToMerge[1])) {
      String msg = "Skip merging regions " + getRegionsToMergeListFullNameString()
        + ", because region "
        + (regionAHasMergeQualifier ? regionsToMerge[0].getEncodedName() : regionsToMerge[1]
              .getEncodedName()) + " has merge qualifier";
      LOG.warn(msg);
      throw new MergeRegionException(msg);
    }

    RegionStates regionStates = getAssignmentManager(env).getRegionStates();
    RegionState regionStateA = regionStates.getRegionState(regionsToMerge[0].getEncodedName());
    RegionState regionStateB = regionStates.getRegionState(regionsToMerge[1].getEncodedName());
    if (regionStateA == null || regionStateB == null) {
      throw new UnknownRegionException(
        regionStateA == null ?
            regionsToMerge[0].getEncodedName() : regionsToMerge[1].getEncodedName());
    }

    if (!regionStateA.isOpened() || !regionStateB.isOpened()) {
      throw new MergeRegionException(
        "Unable to merge regions not online " + regionStateA + ", " + regionStateB);
    }
  }

  /**
   * Create merged region info through the specified two regions
   */
  private void setupMergedRegionInfo() {
    long rid = EnvironmentEdgeManager.currentTime();
    // Regionid is timestamp. Merged region's id can't be less than that of
    // merging regions else will insert at wrong location in hbase:meta
    if (rid < regionsToMerge[0].getRegionId() || rid < regionsToMerge[1].getRegionId()) {
      LOG.warn("Clock skew; merging regions id are " + regionsToMerge[0].getRegionId()
          + " and " + regionsToMerge[1].getRegionId() + ", but current time here is " + rid);
      rid = Math.max(regionsToMerge[0].getRegionId(), regionsToMerge[1].getRegionId()) + 1;
    }

    byte[] startKey = null;
    byte[] endKey = null;
    // Choose the smaller as start key
    if (regionsToMerge[0].compareTo(regionsToMerge[1]) <= 0) {
      startKey = regionsToMerge[0].getStartKey();
    } else {
      startKey = regionsToMerge[1].getStartKey();
    }
    // Choose the bigger as end key
    if (Bytes.equals(regionsToMerge[0].getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
        || (!Bytes.equals(regionsToMerge[1].getEndKey(), HConstants.EMPTY_BYTE_ARRAY)
            && Bytes.compareTo(regionsToMerge[0].getEndKey(), regionsToMerge[1].getEndKey()) > 0)) {
      endKey = regionsToMerge[0].getEndKey();
    } else {
      endKey = regionsToMerge[1].getEndKey();
    }

    // Merged region is sorted between two merging regions in META
    mergedRegionInfo = new HRegionInfo(getTableName(), startKey, endKey, false, rid);
  }

  /**
   * Move all regions to the same region server
   * @param env MasterProcedureEnv
   * @return whether target regions hosted by the same RS
   * @throws IOException
   */
  private boolean MoveRegionsToSameRS(final MasterProcedureEnv env) throws IOException {
    // Make sure regions are on the same regionserver before send merge
    // regions request to region server.
    //
    boolean onSameRS = isRegionsOnTheSameServer(env);
    if (!onSameRS) {
      // Note: the following logic assumes that we only have 2 regions to merge.  In the future,
      // if we want to extend to more than 2 regions, the code needs to modify a little bit.
      //
      RegionStates regionStates = getAssignmentManager(env).getRegionStates();
      ServerName regionLocation2 = regionStates.getRegionServerOfRegion(regionsToMerge[1]);

      RegionLoad loadOfRegionA = getRegionLoad(env, regionLocation, regionsToMerge[0]);
      RegionLoad loadOfRegionB = getRegionLoad(env, regionLocation2, regionsToMerge[1]);
      if (loadOfRegionA != null && loadOfRegionB != null
          && loadOfRegionA.getRequestsCount() < loadOfRegionB.getRequestsCount()) {
        // switch regionsToMerge[0] and regionsToMerge[1]
        HRegionInfo tmpRegion = this.regionsToMerge[0];
        this.regionsToMerge[0] = this.regionsToMerge[1];
        this.regionsToMerge[1] = tmpRegion;
        ServerName tmpLocation = regionLocation;
        regionLocation = regionLocation2;
        regionLocation2 = tmpLocation;
      }

      long startTime = EnvironmentEdgeManager.currentTime();

      RegionPlan regionPlan = new RegionPlan(regionsToMerge[1], regionLocation2, regionLocation);
      LOG.info("Moving regions to same server for merge: " + regionPlan.toString());
      getAssignmentManager(env).balance(regionPlan);
      do {
        try {
          Thread.sleep(20);
          // Make sure check RIT first, then get region location, otherwise
          // we would make a wrong result if region is online between getting
          // region location and checking RIT
          boolean isRIT = regionStates.isRegionInTransition(regionsToMerge[1]);
          regionLocation2 = regionStates.getRegionServerOfRegion(regionsToMerge[1]);
          onSameRS = regionLocation.equals(regionLocation2);
          if (onSameRS || !isRIT) {
            // Regions are on the same RS, or regionsToMerge[1] is not in
            // RegionInTransition any more
            break;
          }
        } catch (InterruptedException e) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(e);
          throw iioe;
        }
      } while ((EnvironmentEdgeManager.currentTime() - startTime) <= getTimeout(env));
    }
    return onSameRS;
  }

  /**
   * Pre merge region action
   * @param env MasterProcedureEnv
   **/
  private void preMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      boolean ret = cpHost.preMergeRegionsAction(regionsToMerge, getUser());
      if (ret) {
        throw new IOException(
          "Coprocessor bypassing regions " + getRegionsToMergeListFullNameString() + " merge.");
      }
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
   * @throws IOException
   */
  public void setRegionStateToMerging(final MasterProcedureEnv env) throws IOException {
    RegionStateTransition.Builder transition = RegionStateTransition.newBuilder();
    transition.setTransitionCode(TransitionCode.READY_TO_MERGE);
    transition.addRegionInfo(HRegionInfo.convert(mergedRegionInfo));
    transition.addRegionInfo(HRegionInfo.convert(regionsToMerge[0]));
    transition.addRegionInfo(HRegionInfo.convert(regionsToMerge[1]));
    if (env.getMasterServices().getAssignmentManager().onRegionTransition(
      getServerName(env), transition.build()) != null) {
      throw new IOException("Failed to update region state to MERGING for "
          + getRegionsToMergeListFullNameString());
    }
  }

  /**
   * Rollback the region state change
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void setRegionStateToRevertMerging(final MasterProcedureEnv env) throws IOException {
    RegionStateTransition.Builder transition = RegionStateTransition.newBuilder();
    transition.setTransitionCode(TransitionCode.MERGE_REVERTED);
    transition.addRegionInfo(HRegionInfo.convert(mergedRegionInfo));
    transition.addRegionInfo(HRegionInfo.convert(regionsToMerge[0]));
    transition.addRegionInfo(HRegionInfo.convert(regionsToMerge[1]));
    String msg = env.getMasterServices().getAssignmentManager().onRegionTransition(
      getServerName(env), transition.build());
    if (msg != null) {
      // If daughter regions are online, the msg is coming from RPC retry.  Ignore it.
      RegionStates regionStates = getAssignmentManager(env).getRegionStates();
      if (!regionStates.isRegionOnline(regionsToMerge[0]) ||
          !regionStates.isRegionOnline(regionsToMerge[1])) {
        throw new IOException("Failed to update region state for "
          + getRegionsToMergeListFullNameString()
          + " as part of operation for reverting merge.  Error message: " + msg);
      }
    }
  }

  /**
   * Create merged region
   * @param env MasterProcedureEnv
   * @throws IOException
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

    regionFs.commitMergedRegion(mergedRegionInfo);
  }

  /**
   * Create reference file(s) of merging regions under the merges directory
   * @param env MasterProcedureEnv
   * @param regionFs region file system
   * @param mergedDir the temp directory of merged region
   * @throws IOException
   */
  private void mergeStoreFiles(
      final MasterProcedureEnv env, final HRegionFileSystem regionFs, final Path mergedDir)
      throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Configuration conf = env.getMasterConfiguration();
    final HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());

    for (String family: regionFs.getFamilies()) {
      final HColumnDescriptor hcd = htd.getFamily(family.getBytes());
      final Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family);

      if (storeFiles != null && storeFiles.size() > 0) {
        final CacheConfig cacheConf = new CacheConfig(conf, hcd);
        for (StoreFileInfo storeFileInfo: storeFiles) {
          // Create reference file(s) of the region in mergedDir
          regionFs.mergeStoreFile(
            mergedRegionInfo,
            family,
            new StoreFile(
              mfs.getFileSystem(), storeFileInfo, conf, cacheConf, hcd.getBloomFilterType()),
            mergedDir);
        }
      }
    }
  }

  /**
   * Clean up merged region
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void cleanupMergedRegion(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = FSUtils.getTableDir(mfs.getRootDir(), regionsToMerge[0].getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, regionsToMerge[0], false);
    regionFs.cleanupMergedRegion(mergedRegionInfo);
  }

  /**
   * RPC to region server that host the regions to merge, ask for close these regions
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void closeRegionsForMerge(final MasterProcedureEnv env) throws IOException {
    boolean success = env.getMasterServices().getServerManager().sendRegionCloseForSplitOrMerge(
      getServerName(env), regionsToMerge[0], regionsToMerge[1]);
    if (!success) {
      throw new IOException("Close regions " + getRegionsToMergeListFullNameString()
          + " for merging failed. Check region server log for more details.");
    }
  }

  /**
   * Rollback close regions
   * @param env MasterProcedureEnv
   **/
  private void rollbackCloseRegionsForMerge(final MasterProcedureEnv env) throws IOException {
    // Check whether the region is closed; if so, open it in the same server
    RegionStates regionStates = getAssignmentManager(env).getRegionStates();
    for(int i = 1; i < regionsToMerge.length; i++) {
      RegionState state = regionStates.getRegionState(regionsToMerge[i]);
      if (state != null && (state.isClosing() || state.isClosed())) {
        env.getMasterServices().getServerManager().sendRegionOpen(
          getServerName(env),
          regionsToMerge[i],
          ServerName.EMPTY_SERVER_LIST);
      }
    }
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void preMergeRegionsCommit(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      @MetaMutationAnnotation
      final List<Mutation> metaEntries = new ArrayList<Mutation>();
      boolean ret = cpHost.preMergeRegionsCommit(regionsToMerge, metaEntries, getUser());

      if (ret) {
        throw new IOException(
          "Coprocessor bypassing regions " + getRegionsToMergeListFullNameString() + " merge.");
      }
      try {
        for (Mutation p : metaEntries) {
          HRegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("Row key of mutation from coprocessor is not parsable as region name."
          + "Mutations from coprocessor should only be for hbase:meta table.", e);
        throw e;
      }
    }
  }

  /**
   * Add merged region to META and delete original regions.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void updateMetaForMergedRegions(final MasterProcedureEnv env) throws IOException {
    RegionStateTransition.Builder transition = RegionStateTransition.newBuilder();
    transition.setTransitionCode(TransitionCode.MERGE_PONR);
    transition.addRegionInfo(HRegionInfo.convert(mergedRegionInfo));
    transition.addRegionInfo(HRegionInfo.convert(regionsToMerge[0]));
    transition.addRegionInfo(HRegionInfo.convert(regionsToMerge[1]));
    // Add merged region and delete original regions
    // as an atomic update. See HBASE-7721. This update to hbase:meta makes the region
    // will determine whether the region is merged or not in case of failures.
    if (env.getMasterServices().getAssignmentManager().onRegionTransition(
      getServerName(env), transition.build()) != null) {
      throw new IOException("Failed to update meta to add merged region that merges "
          + getRegionsToMergeListFullNameString());
    }
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void postMergeRegionsCommit(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postMergeRegionsCommit(regionsToMerge, mergedRegionInfo, getUser());
    }
  }

  /**
   * Assign merged region
   * @param env MasterProcedureEnv
   * @throws IOException
   * @throws InterruptedException
   **/
  private void openMergedRegions(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    // Check whether the merged region is already opened; if so,
    // this is retry and we should just ignore.
    RegionState regionState =
        getAssignmentManager(env).getRegionStates().getRegionState(mergedRegionInfo);
    if (regionState != null && regionState.isOpened()) {
      LOG.info("Skip opening merged region " + mergedRegionInfo.getRegionNameAsString()
        + " as it is already opened.");
      return;
    }

    // TODO: The new AM should provide an API to force assign the merged region to the same RS
    // as daughter regions; if the RS is unavailable, then assign to a different RS.
    env.getMasterServices().getAssignmentManager().assignMergedRegion(
      mergedRegionInfo, regionsToMerge[0], regionsToMerge[1]);
  }

  /**
   * Post merge region action
   * @param env MasterProcedureEnv
   **/
  private void postCompletedMergeRegions(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postCompletedMergeRegionsAction(regionsToMerge, mergedRegionInfo, getUser());
    }
  }

  private RegionLoad getRegionLoad(
      final MasterProcedureEnv env,
      final ServerName sn,
      final HRegionInfo hri) {
    ServerManager serverManager =  env.getMasterServices().getServerManager();
    ServerLoad load = serverManager.getLoad(sn);
    if (load != null) {
      Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
      if (regionsLoad != null) {
        return regionsLoad.get(hri.getRegionName());
      }
    }
    return null;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return whether target regions hosted by the same RS
   */
  private boolean isRegionsOnTheSameServer(final MasterProcedureEnv env) throws IOException{
    Boolean onSameRS = true;
    int i = 0;
    RegionStates regionStates = getAssignmentManager(env).getRegionStates();
    regionLocation = regionStates.getRegionServerOfRegion(regionsToMerge[i]);
    if (regionLocation != null) {
      for(i = 1; i < regionsToMerge.length; i++) {
        ServerName regionLocation2 = regionStates.getRegionServerOfRegion(regionsToMerge[i]);
        if (regionLocation2 != null) {
          if (onSameRS) {
            onSameRS = regionLocation.equals(regionLocation2);
          }
        } else {
          // At least one region is not online, merge will fail, no need to continue.
          break;
        }
      }
      if (i == regionsToMerge.length) {
        // Finish checking all regions, return the result;
        return onSameRS;
      }
    }

    // If reaching here, at least one region is not online.
    String msg = "Skip merging regions " + getRegionsToMergeListFullNameString() +
        ", because region " + regionsToMerge[i].getEncodedName() + " is not online now.";
    LOG.warn(msg);
    throw new IOException(msg);
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return assignmentManager
   */
  private AssignmentManager getAssignmentManager(final MasterProcedureEnv env) {
    if (assignmentManager == null) {
      assignmentManager = env.getMasterServices().getAssignmentManager();
    }
    return assignmentManager;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return timeout value
   */
  private int getTimeout(final MasterProcedureEnv env) {
    if (timeout == -1) {
      timeout = env.getMasterConfiguration().getInt(
        "hbase.master.regionmerge.timeout", regionsToMerge.length * 60 * 1000);
    }
    return timeout;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return serverName
   */
  private ServerName getServerName(final MasterProcedureEnv env) {
    if (regionLocation == null) {
      regionLocation =
          getAssignmentManager(env).getRegionStates().getRegionServerOfRegion(regionsToMerge[0]);
    }
    return regionLocation;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param fullName whether return only encoded name
   * @return region names in a list
   */
  private String getRegionsToMergeListFullNameString() {
    if (regionsToMergeListFullName == null) {
      StringBuilder sb = new StringBuilder("[");
      int i = 0;
      while(i < regionsToMerge.length - 1) {
        sb.append(regionsToMerge[i].getRegionNameAsString() + ", ");
        i++;
      }
      sb.append(regionsToMerge[i].getRegionNameAsString() + " ]");
      regionsToMergeListFullName = sb.toString();
    }
    return regionsToMergeListFullName;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return encoded region names
   */
  private String getRegionsToMergeListEncodedNameString() {
    if (regionsToMergeListEncodedName == null) {
      StringBuilder sb = new StringBuilder("[");
      int i = 0;
      while(i < regionsToMerge.length - 1) {
        sb.append(regionsToMerge[i].getEncodedName() + ", ");
        i++;
      }
      sb.append(regionsToMerge[i].getEncodedName() + " ]");
      regionsToMergeListEncodedName = sb.toString();
    }
    return regionsToMergeListEncodedName;
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
