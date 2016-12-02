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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SplitTableRegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.annotations.VisibleForTesting;

/**
 * The procedure to split a region in a table.
 */
@InterfaceAudience.Private
public class SplitTableRegionProcedure
    extends AbstractStateMachineTableProcedure<SplitTableRegionState> {
  private static final Log LOG = LogFactory.getLog(SplitTableRegionProcedure.class);

  private Boolean traceEnabled;

  /*
   * Region to split
   */
  private HRegionInfo parentHRI;
  private HRegionInfo daughter_1_HRI;
  private HRegionInfo daughter_2_HRI;

  public SplitTableRegionProcedure() {
    this.traceEnabled = null;
  }

  public SplitTableRegionProcedure(final MasterProcedureEnv env,
      final HRegionInfo regionToSplit, final byte[] splitRow) throws IOException {
    super(env);

    checkSplitRow(regionToSplit, splitRow);

    this.traceEnabled = null;
    this.parentHRI = regionToSplit;

    final TableName table = regionToSplit.getTable();
    final long rid = getDaughterRegionIdTimestamp(regionToSplit);
    this.daughter_1_HRI = new HRegionInfo(table, regionToSplit.getStartKey(), splitRow, false, rid);
    this.daughter_2_HRI = new HRegionInfo(table, splitRow, regionToSplit.getEndKey(), false, rid);
  }

  private static void checkSplitRow(final HRegionInfo regionToSplit, final byte[] splitRow)
      throws IOException {
    if (splitRow == null || splitRow.length == 0) {
      throw new DoNotRetryIOException("Split row cannot be null");
    }

    if (Bytes.equals(regionToSplit.getStartKey(), splitRow)) {
      throw new DoNotRetryIOException(
        "Split row is equal to startkey: " + Bytes.toStringBinary(splitRow));
    }

    if (!regionToSplit.containsRow(splitRow)) {
      throw new DoNotRetryIOException(
        "Split row is not inside region key range splitKey:" + Bytes.toStringBinary(splitRow) +
        " region: " + regionToSplit);
    }
  }

  /**
   * Calculate daughter regionid to use.
   * @param hri Parent {@link HRegionInfo}
   * @return Daughter region id (timestamp) to use.
   */
  private static long getDaughterRegionIdTimestamp(final HRegionInfo hri) {
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
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case SPLIT_TABLE_REGION_PREPARE:
        if (prepareSplitRegion(env)) {
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION);
          break;
        } else {
          assert isFailed() : "split region should have an exception here";
          return Flow.NO_MORE_STATE;
        }
      case SPLIT_TABLE_REGION_PRE_OPERATION:
        preSplitRegion(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_SET_SPLITTING_TABLE_STATE);
        break;
      case SPLIT_TABLE_REGION_SET_SPLITTING_TABLE_STATE:
        setRegionStateToSplitting(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_CLOSE_PARENT_REGION);
        break;
      case SPLIT_TABLE_REGION_CLOSE_PARENT_REGION:
        closeParentRegionForSplit(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS);
        break;
      case SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS:
        createDaughterRegions(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_PONR);
        break;
      case SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_PONR:
        preSplitRegionBeforePONR(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_UPDATE_META);
        break;
      case SPLIT_TABLE_REGION_UPDATE_META:
        // This is the point of no return.  Adding subsequent edits to .META. as we
        // do below when we do the daughter opens adding each to .META. can fail in
        // various interesting ways the most interesting of which is a timeout
        // BUT the edits all go through (See HBASE-3872).  IF we reach the PONR
        // then subsequent failures need to crash out this region server; the
        // server shutdown processing should be able to fix-up the incomplete split.
        // The offlined parent will have the daughters as extra columns.  If
        // we leave the daughter regions in place and do not remove them when we
        // crash out, then they will have their references to the parent in place
        // still and the server shutdown fixup of .META. will point to these
        // regions.
        // We should add PONR JournalEntry before offlineParentInMeta,so even if
        // OfflineParentInMeta timeout,this will cause regionserver exit,and then
        // master ServerShutdownHandler will fix daughter & avoid data loss. (See
        // HBase-4562).
        updateMetaForDaughterRegions(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_PONR);
        break;
      case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_PONR:
        preSplitRegionAfterPONR(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS);
        break;
      case SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS:
        openDaughterRegions(env);
        setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_POST_OPERATION);
        break;
      case SPLIT_TABLE_REGION_POST_OPERATION:
        postSplitRegion(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      String msg = "Error trying to split region " + parentHRI.getEncodedName() + " in the table "
          + getTableName() + " (in state=" + state + ")";
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retry.
        LOG.warn(msg, e);
      } else {
        LOG.error(msg, e);
        setFailure("master-split-region", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

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
      case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_PONR:
      case SPLIT_TABLE_REGION_UPDATE_META:
        // PONR
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      case SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_PONR:
        break;
      case SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS:
        // Doing nothing, as re-open parent region would clean up daughter region directories.
        break;
      case SPLIT_TABLE_REGION_CLOSE_PARENT_REGION:
        openParentRegion(env);
        break;
      case SPLIT_TABLE_REGION_SET_SPLITTING_TABLE_STATE:
        setRegionStateToRevertSplitting(env);
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
      LOG.warn("Failed rollback attempt step " + state + " for splitting the region "
        + parentHRI.getEncodedName() + " in table " + getTableName(), e);
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
      case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_PONR:
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
    return SplitTableRegionState.valueOf(stateId);
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
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    final MasterProcedureProtos.SplitTableRegionStateData.Builder splitTableRegionMsg =
        MasterProcedureProtos.SplitTableRegionStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setParentRegionInfo(HRegionInfo.convert(parentHRI))
        .addChildRegionInfo(HRegionInfo.convert(daughter_1_HRI))
        .addChildRegionInfo(HRegionInfo.convert(daughter_2_HRI));
    splitTableRegionMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    final MasterProcedureProtos.SplitTableRegionStateData splitTableRegionsMsg =
        MasterProcedureProtos.SplitTableRegionStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(splitTableRegionsMsg.getUserInfo()));
    parentHRI = HRegionInfo.convert(splitTableRegionsMsg.getParentRegionInfo());
    if (splitTableRegionsMsg.getChildRegionInfoCount() == 0) {
      daughter_1_HRI = daughter_2_HRI = null;
    } else {
      assert(splitTableRegionsMsg.getChildRegionInfoCount() == 2);
      daughter_1_HRI = HRegionInfo.convert(splitTableRegionsMsg.getChildRegionInfoList().get(0));
      daughter_2_HRI = HRegionInfo.convert(splitTableRegionsMsg.getChildRegionInfoList().get(1));
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(" parent region=");
    sb.append(parentHRI);
    if (daughter_1_HRI != null) {
      sb.append(" first daughter region=");
      sb.append(daughter_1_HRI);
    }
    if (daughter_2_HRI != null) {
      sb.append(" and second daughter region=");
      sb.append(daughter_2_HRI);
    }
    sb.append(")");
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    if (env.waitInitialized(this)) {
      return false;
    }
    return !env.getProcedureScheduler().waitRegions(this, getTableName(), parentHRI);
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegions(this, getTableName(), parentHRI);
  }

  @Override
  public TableName getTableName() {
    return parentHRI.getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.SPLIT;
  }

  private byte[] getSplitRow() {
    return daughter_2_HRI.getStartKey();
  }

  /**
   * Prepare to Split region.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  @VisibleForTesting
  public boolean prepareSplitRegion(final MasterProcedureEnv env) throws IOException {
    // Check whether the region is splittable
    final RegionState state = getParentRegionState(env);
    if (state.isClosing() || state.isClosed() ||
        state.isSplittingOrSplitOnServer(state.getServerName())) {
      setFailure(
        "master-split-region",
        new IOException("Split region " + parentHRI + " failed due to region is not splittable"));
      return false;
    }
    return true;
  }

  /**
   * Action before splitting region in a table.
   * @param env MasterProcedureEnv
   * @param state the procedure state
   * @throws IOException
   * @throws InterruptedException
   */
  private void preSplitRegion(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSplitRegionAction(getTableName(), getSplitRow(), getUser());
    }
  }

  /**
   * Action after rollback a split table region action.
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void postRollBackSplitRegion(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postRollBackSplitRegionAction(getUser());
    }
  }

  /**
   * Set the parent region state to SPLITTING state
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  @VisibleForTesting
  public void setRegionStateToSplitting(final MasterProcedureEnv env) throws IOException {
    RegionStateTransition.Builder transition = RegionStateTransition.newBuilder();
    transition.setTransitionCode(TransitionCode.READY_TO_SPLIT);
    transition.addRegionInfo(HRegionInfo.convert(parentHRI));
    transition.addRegionInfo(HRegionInfo.convert(daughter_1_HRI));
    transition.addRegionInfo(HRegionInfo.convert(daughter_2_HRI));
    if (env.getMasterServices().getAssignmentManager().onRegionTransition(
      getParentRegionState(env).getServerName(), transition.build()) != null) {
      throw new IOException("Failed to update region state to SPLITTING for "
          + parentHRI.getRegionNameAsString());
    }
  }

  /**
   * Rollback the region state change
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void setRegionStateToRevertSplitting(final MasterProcedureEnv env) throws IOException {
    RegionStateTransition.Builder transition = RegionStateTransition.newBuilder();
    transition.setTransitionCode(TransitionCode.SPLIT_REVERTED);
    transition.addRegionInfo(HRegionInfo.convert(parentHRI));
    transition.addRegionInfo(HRegionInfo.convert(daughter_1_HRI));
    transition.addRegionInfo(HRegionInfo.convert(daughter_2_HRI));
    if (env.getMasterServices().getAssignmentManager().onRegionTransition(
      getParentRegionState(env).getServerName(), transition.build()) != null) {
      throw new IOException("Failed to update region state for "
          + parentHRI.getRegionNameAsString() + " as part of operation for reverting split");
    }
  }

  /**
   * RPC to region server that host the parent region, ask for close the parent regions
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  @VisibleForTesting
  public void closeParentRegionForSplit(final MasterProcedureEnv env) throws IOException {
    boolean success = env.getMasterServices().getServerManager().sendRegionCloseForSplitOrMerge(
      getParentRegionState(env).getServerName(), parentHRI);
    if (!success) {
      throw new IOException("Close parent region " + parentHRI + " for splitting failed."
        + "  Check region server log for more details");
    }
  }

  /**
   * Rollback close parent region
   * @param env MasterProcedureEnv
   **/
  private void openParentRegion(final MasterProcedureEnv env) throws IOException {
    // Check whether the region is closed; if so, open it in the same server
    RegionState state = getParentRegionState(env);
    if (state.isClosing() || state.isClosed()) {
      env.getMasterServices().getServerManager().sendRegionOpen(
        getParentRegionState(env).getServerName(),
        parentHRI,
        ServerName.EMPTY_SERVER_LIST);
    }
  }

  /**
   * Create daughter regions
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  @VisibleForTesting
  public void createDaughterRegions(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = FSUtils.getTableDir(mfs.getRootDir(), parentHRI.getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, parentHRI, false);
    regionFs.createSplitsDir();

    Pair<Integer, Integer> expectedReferences = splitStoreFiles(env, regionFs);

    assertReferenceFileCount(
      fs, expectedReferences.getFirst(), regionFs.getSplitsDir(daughter_1_HRI));
    //Move the files from the temporary .splits to the final /table/region directory
    regionFs.commitDaughterRegion(daughter_1_HRI);
    assertReferenceFileCount(
      fs,
      expectedReferences.getFirst(),
      new Path(tabledir, daughter_1_HRI.getEncodedName()));

    assertReferenceFileCount(
      fs, expectedReferences.getSecond(), regionFs.getSplitsDir(daughter_2_HRI));
    regionFs.commitDaughterRegion(daughter_2_HRI);
    assertReferenceFileCount(
      fs,
      expectedReferences.getSecond(),
      new Path(tabledir, daughter_2_HRI.getEncodedName()));
  }

  /**
   * Create Split directory
   * @param env MasterProcedureEnv
   * @throws IOException
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
    for (String family: regionFs.getFamilies()) {
      Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family);
      if (storeFiles != null) {
        nbFiles += storeFiles.size();
      }
    }
    if (nbFiles == 0) {
      // no file needs to be splitted.
      return new Pair<Integer, Integer>(0,0);
    }
    // Default max #threads to use is the smaller of table's configured number of blocking store
    // files or the available number of logical cores.
    int defMaxThreads = Math.min(
      conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT),
      Runtime.getRuntime().availableProcessors());
    // Max #threads is the smaller of the number of storefiles or the default max determined above.
    int maxThreads = Math.min(
      conf.getInt(HConstants.REGION_SPLIT_THREADS_MAX, defMaxThreads), nbFiles);
    LOG.info("Preparing to split " + nbFiles + " storefiles for region " + parentHRI +
            " using " + maxThreads + " threads");
    ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
      maxThreads, Threads.getNamedThreadFactory("StoreFileSplitter-%1$d"));
    List<Future<Pair<Path,Path>>> futures = new ArrayList<Future<Pair<Path,Path>>> (nbFiles);

    // Split each store file.
    final HTableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    for (String family: regionFs.getFamilies()) {
      final HColumnDescriptor hcd = htd.getFamily(family.getBytes());
      final Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family);
      if (storeFiles != null && storeFiles.size() > 0) {
        final CacheConfig cacheConf = new CacheConfig(conf, hcd);
        for (StoreFileInfo storeFileInfo: storeFiles) {
          StoreFileSplitter sfs = new StoreFileSplitter(regionFs, family.getBytes(),
            new StoreFile(mfs.getFileSystem(), storeFileInfo, conf,
              cacheConf, hcd.getBloomFilterType()));
          futures.add(threadPool.submit(sfs));
        }
      }
    }
    // Shutdown the pool
    threadPool.shutdown();

    // Wait for all the tasks to finish
    long fileSplitTimeout = conf.getLong("hbase.master.fileSplitTimeout", 30000);
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
      LOG.debug("Split storefiles for region " + parentHRI + " Daughter A: " + daughterA
          + " storefiles, Daughter B: " + daughterB + " storefiles.");
    }
    return new Pair<Integer, Integer>(daughterA, daughterB);
  }

  private void assertReferenceFileCount(
      final FileSystem fs,
      final int expectedReferenceFileCount,
      final Path dir)
      throws IOException {
    if (expectedReferenceFileCount != 0 &&
        expectedReferenceFileCount != FSUtils.getRegionReferenceFileCount(fs, dir)) {
      throw new IOException("Failing split. Expected reference file count isn't equal.");
    }
  }

  private Pair<Path, Path> splitStoreFile(final HRegionFileSystem regionFs,
      final byte[] family, final StoreFile sf) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Splitting started for store file: " + sf.getPath() + " for region: " + parentHRI);
    }

    final byte[] splitRow = getSplitRow();
    final String familyName = Bytes.toString(family);
    final Path path_first =
        regionFs.splitStoreFile(this.daughter_1_HRI, familyName, sf, splitRow, false, null);
    final Path path_second =
        regionFs.splitStoreFile(this.daughter_2_HRI, familyName, sf, splitRow, true, null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Splitting complete for store file: " + sf.getPath() + " for region: " + parentHRI);
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
    private final StoreFile sf;

    /**
     * Constructor that takes what it needs to split
     * @param regionFs the file system
     * @param family Family that contains the store file
     * @param sf which file
     */
    public StoreFileSplitter(
        final HRegionFileSystem regionFs,
        final byte[] family,
        final StoreFile sf) {
      this.regionFs = regionFs;
      this.sf = sf;
      this.family = family;
    }

    public Pair<Path,Path> call() throws IOException {
      return splitStoreFile(regionFs, family, sf);
    }
  }

  /**
   * Post split region actions before the Point-of-No-Return step
   * @param env MasterProcedureEnv
   **/
  private void preSplitRegionBeforePONR(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
    final List<Mutation> metaEntries = new ArrayList<Mutation>();
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      if (cpHost.preSplitBeforePONRAction(getSplitRow(), metaEntries, getUser())) {
        throw new IOException("Coprocessor bypassing region " +
            parentHRI.getRegionNameAsString() + " split.");
      }
      try {
        for (Mutation p : metaEntries) {
          HRegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("Row key of mutation from coprocessor is not parsable as region name."
            + "Mutations from coprocessor should only for hbase:meta table.");
        throw e;
      }
    }
  }

  /**
   * Add daughter regions to META
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void updateMetaForDaughterRegions(final MasterProcedureEnv env) throws IOException {
    RegionStateTransition.Builder transition = RegionStateTransition.newBuilder();
    transition.setTransitionCode(TransitionCode.SPLIT_PONR);
    transition.addRegionInfo(HRegionInfo.convert(parentHRI));
    transition.addRegionInfo(HRegionInfo.convert(daughter_1_HRI));
    transition.addRegionInfo(HRegionInfo.convert(daughter_2_HRI));
    if (env.getMasterServices().getAssignmentManager().onRegionTransition(
      getParentRegionState(env).getServerName(), transition.build()) != null) {
      throw new IOException("Failed to update meta to add daughter regions in split region "
          + parentHRI.getRegionNameAsString());
    }
  }

  /**
   * Pre split region actions after the Point-of-No-Return step
   * @param env MasterProcedureEnv
   **/
  private void preSplitRegionAfterPONR(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSplitAfterPONRAction(getUser());
    }
  }

  /**
   * Assign daughter regions
   * @param env MasterProcedureEnv
   * @throws IOException
   * @throws InterruptedException
   **/
  private void openDaughterRegions(final MasterProcedureEnv env)
      throws IOException, InterruptedException {
    env.getMasterServices().getAssignmentManager().assignDaughterRegions(
      parentHRI, daughter_1_HRI, daughter_2_HRI);
  }

  /**
   * Post split region actions
   * @param env MasterProcedureEnv
   **/
  private void postSplitRegion(final MasterProcedureEnv env) throws IOException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postCompletedSplitRegionAction(daughter_1_HRI, daughter_2_HRI, getUser());
    }
  }

  /**
   * Get parent region state
   * @param env MasterProcedureEnv
   * @return parent region state
   */
  private RegionState getParentRegionState(final MasterProcedureEnv env) {
    RegionStates regionStates = env.getMasterServices().getAssignmentManager().getRegionStates();
    RegionState state = regionStates.getRegionState(parentHRI);
    if (state == null) {
      LOG.warn("Split but not in region states: " + parentHRI);
      state = regionStates.createRegionState(parentHRI);
    }
    return state;
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
}
