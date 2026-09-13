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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_ENABLE_SEPARATE_CHILD_REGIONS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;

/**
 * Utility for this assignment package only.
 */
@InterfaceAudience.Private
final class AssignmentManagerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(AssignmentManagerUtil.class);
  private static final int DEFAULT_REGION_REPLICA = 1;

  private AssignmentManagerUtil() {
  }

  /**
   * Raw call to remote regionserver to get info on a particular region.
   * @throws IOException Let it out so can report this IOE as reason for failure
   */
  static GetRegionInfoResponse getRegionInfoResponse(final MasterProcedureEnv env,
    final ServerName regionLocation, final RegionInfo hri) throws IOException {
    return getRegionInfoResponse(env, regionLocation, hri, false);
  }

  static GetRegionInfoResponse getRegionInfoResponse(final MasterProcedureEnv env,
    final ServerName regionLocation, final RegionInfo hri, boolean includeBestSplitRow)
    throws IOException {
    AsyncRegionServerAdmin admin =
      env.getMasterServices().getAsyncClusterConnection().getRegionServerAdmin(regionLocation);
    GetRegionInfoRequest request = null;
    if (includeBestSplitRow) {
      request = RequestConverter.buildGetRegionInfoRequest(hri.getRegionName(), false, true);
    } else {
      request = RequestConverter.buildGetRegionInfoRequest(hri.getRegionName());
    }
    return FutureUtils.get(admin.getRegionInfo(request));
  }

  private static void lock(List<RegionStateNode> regionNodes) {
    regionNodes.iterator().forEachRemaining(RegionStateNode::lock);
  }

  private static void unlock(List<RegionStateNode> regionNodes) {
    for (ListIterator<RegionStateNode> iter = regionNodes.listIterator(regionNodes.size()); iter
      .hasPrevious();) {
      iter.previous().unlock();
    }
  }

  static TransitRegionStateProcedure[] createUnassignProceduresForSplitOrMerge(
    MasterProcedureEnv env, Stream<RegionInfo> regions, int regionReplication) throws IOException {
    List<RegionStateNode> regionNodes = regions
      .flatMap(hri -> IntStream.range(0, regionReplication)
        .mapToObj(i -> RegionReplicaUtil.getRegionInfoForReplica(hri, i)))
      .map(env.getAssignmentManager().getRegionStates()::getOrCreateRegionStateNode)
      .collect(Collectors.toList());
    TransitRegionStateProcedure[] procs = new TransitRegionStateProcedure[regionNodes.size()];
    boolean rollback = true;
    int i = 0;
    // hold the lock at once, and then release it in finally. This is important as SCP may jump in
    // if we release the lock in the middle when we want to do rollback, and cause problems.
    lock(regionNodes);
    try {
      for (; i < procs.length; i++) {
        RegionStateNode regionNode = regionNodes.get(i);
        TransitRegionStateProcedure proc =
          TransitRegionStateProcedure.unassignSplitMerge(env, regionNode.getRegionInfo());
        if (regionNode.getProcedure() != null) {
          throw new HBaseIOException(
            "The parent region " + regionNode + " is currently in transition, give up");
        }
        regionNode.setProcedure(proc);
        procs[i] = proc;
      }
      // all succeeded, set rollback to false
      rollback = false;
    } finally {
      if (rollback) {
        for (;;) {
          i--;
          if (i < 0) {
            break;
          }
          RegionStateNode regionNode = regionNodes.get(i);
          regionNode.unsetProcedure(procs[i]);
        }
      }
      unlock(regionNodes);
    }
    return procs;
  }

  /**
   * Create assign procedures for the give regions, according to the {@code regionReplication}.
   * <p/>
   * For rolling back, we will submit procedures directly to the {@code ProcedureExecutor}, so it is
   * possible that we persist the newly scheduled procedures, and then crash before persisting the
   * rollback state, so when we arrive here the second time, it is possible that some regions have
   * already been associated with a TRSP.
   * @param ignoreIfInTransition if true, will skip creating TRSP for the given region if it is
   *                             already in transition, otherwise we will add an assert that it
   *                             should not in transition.
   */
  private static TransitRegionStateProcedure[] createAssignProcedures(MasterProcedureEnv env,
    List<RegionInfo> regions, int regionReplication, ServerName targetServer,
    boolean ignoreIfInTransition) {
    // create the assign procs only for the primary region using the targetServer
    TransitRegionStateProcedure[] primaryRegionProcs =
      regions.stream().map(env.getAssignmentManager().getRegionStates()::getOrCreateRegionStateNode)
        .map(regionNode -> {
          TransitRegionStateProcedure proc =
            TransitRegionStateProcedure.assign(env, regionNode.getRegionInfo(), targetServer);
          regionNode.lock();
          try {
            if (ignoreIfInTransition) {
              if (regionNode.isTransitionScheduled()) {
                return null;
              }
            } else {
              // should never fail, as we have the exclusive region lock, and the region is newly
              // created, or has been successfully closed so should not be on any servers, so SCP
              // will
              // not process it either.
              assert !regionNode.isTransitionScheduled();
            }
            regionNode.setProcedure(proc);
          } finally {
            regionNode.unlock();
          }
          return proc;
        }).filter(p -> p != null).toArray(TransitRegionStateProcedure[]::new);
    if (regionReplication == DEFAULT_REGION_REPLICA) {
      // this is the default case
      return primaryRegionProcs;
    }
    // collect the replica region infos
    List<RegionInfo> replicaRegionInfos =
      new ArrayList<RegionInfo>(regions.size() * (regionReplication - 1));
    for (RegionInfo hri : regions) {
      // start the index from 1
      for (int i = 1; i < regionReplication; i++) {
        RegionInfo ri = RegionReplicaUtil.getRegionInfoForReplica(hri, i);
        // apply ignoreRITs to replica regions as well.
        if (
          !ignoreIfInTransition || !env.getAssignmentManager().getRegionStates()
            .getOrCreateRegionStateNode(ri).isTransitionScheduled()
        ) {
          replicaRegionInfos.add(ri);
        }
      }
    }

    // create round robin procs. Note that we exclude the primary region's target server
    TransitRegionStateProcedure[] replicaRegionAssignProcs =
      env.getAssignmentManager().createRoundRobinAssignProcedures(replicaRegionInfos,
        Collections.singletonList(targetServer));
    // combine both the procs and return the result
    return ArrayUtils.addAll(primaryRegionProcs, replicaRegionAssignProcs);
  }

  /**
   * Create round robin assign procedures for the given regions, according to the
   * {@code regionReplication}.
   * <p/>
   * For rolling back, we will submit procedures directly to the {@code ProcedureExecutor}, so it is
   * possible that we persist the newly scheduled procedures, and then crash before persisting the
   * rollback state, so when we arrive here the second time, it is possible that some regions have
   * already been associated with a TRSP.
   * @param ignoreIfInTransition if true, will skip creating TRSP for the given region if it is
   *                             already in transition, otherwise we will add an assert that it
   *                             should not in transition.
   */
  private static TransitRegionStateProcedure[] createRoundRobinAssignProcedures(
    MasterProcedureEnv env, List<RegionInfo> regions, int regionReplication,
    List<ServerName> serversToExclude, boolean ignoreIfInTransition) {
    List<RegionInfo> regionsAndReplicas = new ArrayList<>(regions);
    if (regionReplication != DEFAULT_REGION_REPLICA) {

      // collect the replica region infos
      List<RegionInfo> replicaRegionInfos =
        new ArrayList<RegionInfo>(regions.size() * (regionReplication - 1));
      for (RegionInfo hri : regions) {
        // start the index from 1
        for (int i = 1; i < regionReplication; i++) {
          replicaRegionInfos.add(RegionReplicaUtil.getRegionInfoForReplica(hri, i));
        }
      }
      regionsAndReplicas.addAll(replicaRegionInfos);
    }
    if (ignoreIfInTransition) {
      for (RegionInfo region : regionsAndReplicas) {
        if (
          env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(region)
            .isTransitionScheduled()
        ) {
          return null;
        }
      }
    }
    // create round robin procs. Note that we exclude the primary region's target server
    return env.getAssignmentManager().createRoundRobinAssignProcedures(regionsAndReplicas,
      serversToExclude);
  }

  static TransitRegionStateProcedure[] createAssignProceduresForSplitDaughters(
    MasterProcedureEnv env, List<RegionInfo> daughters, int regionReplication,
    ServerName parentServer) {
    if (
      env.getMasterConfiguration().getBoolean(HConstants.HBASE_ENABLE_SEPARATE_CHILD_REGIONS,
        DEFAULT_HBASE_ENABLE_SEPARATE_CHILD_REGIONS)
    ) {
      // keep one daughter on the parent region server
      TransitRegionStateProcedure[] daughterOne = createAssignProcedures(env,
        Collections.singletonList(daughters.get(0)), regionReplication, parentServer, false);
      // round robin assign the other daughter
      TransitRegionStateProcedure[] daughterTwo =
        createRoundRobinAssignProcedures(env, Collections.singletonList(daughters.get(1)),
          regionReplication, Collections.singletonList(parentServer), false);
      return ArrayUtils.addAll(daughterOne, daughterTwo);
    }
    return createAssignProceduresForOpeningNewRegions(env, daughters, regionReplication,
      parentServer);
  }

  static TransitRegionStateProcedure[] createAssignProceduresForOpeningNewRegions(
    MasterProcedureEnv env, List<RegionInfo> regions, int regionReplication,
    ServerName targetServer) {
    return createAssignProcedures(env, regions, regionReplication, targetServer, false);
  }

  static void reopenRegionsForRollback(MasterProcedureEnv env, List<RegionInfo> regions,
    int regionReplication, ServerName targetServer) {
    TransitRegionStateProcedure[] procs =
      createAssignProcedures(env, regions, regionReplication, targetServer, true);
    if (procs.length > 0) {
      env.getMasterServices().getMasterProcedureExecutor().submitProcedures(procs);
    }
  }

  static void removeNonDefaultReplicas(MasterProcedureEnv env, Stream<RegionInfo> regions,
    int regionReplication) {
    // Remove from in-memory states
    regions.flatMap(hri -> IntStream.range(1, regionReplication)
      .mapToObj(i -> RegionReplicaUtil.getRegionInfoForReplica(hri, i))).forEach(hri -> {
        env.getAssignmentManager().getRegionStates().deleteRegion(hri);
        env.getMasterServices().getServerManager().removeRegion(hri);
        FavoredNodesManager fnm = env.getMasterServices().getFavoredNodesManager();
        if (fnm != null) {
          fnm.deleteFavoredNodesForRegions(Collections.singletonList(hri));
        }
      });
  }

  static void checkClosedRegion(MasterProcedureEnv env, RegionInfo regionInfo) throws IOException {
    if (!WALSplitUtil.hasRecoveredEdits(env.getMasterConfiguration(), regionInfo)) {
      return;
    }
    // A recovered.edits file whose max seqid is <= the region's last flushed seqid is stale:
    // its edits are already durable in HFiles. This happens e.g. when a graceful region move
    // is followed by a WAL split of the source RS - the split creates recovered.edits for a
    // region that has already been reopened elsewhere and flushed. Cleaning up such files here
    // (rather than aborting split/merge) matches the tolerance HRegion itself applies at open.
    if (tryDropStaleRecoveredEdits(env, regionInfo)) {
      return;
    }
    throw new IOException("Recovered.edits are found in Region: " + regionInfo
      + ", abort split/merge to prevent data loss");
  }

  /**
   * Try to remove recovered.edits files that are provably below the region's last flushed seqid.
   * @return true if, after cleanup, no recovered.edits remain for the region
   */
  private static boolean tryDropStaleRecoveredEdits(MasterProcedureEnv env, RegionInfo regionInfo) {
    long durableSeqId = env.getMasterServices().getServerManager()
      .getLastFlushedSequenceId(regionInfo.getEncodedNameAsBytes()).getLastFlushedSequenceId();
    if (durableSeqId <= 0L) {
      // No authoritative durability info at the master; play safe and let the caller abort.
      return false;
    }
    try {
      Configuration conf = env.getMasterConfiguration();
      Path regionWALDir =
        CommonFSUtils.getWALRegionDir(conf, regionInfo.getTable(), regionInfo.getEncodedName());
      Path regionDir = FSUtils.getRegionDirFromRootDir(CommonFSUtils.getRootDir(conf), regionInfo);
      Path wrongRegionWALDir = CommonFSUtils.getWrongWALRegionDir(conf, regionInfo.getTable(),
        regionInfo.getEncodedName());
      FileSystem walFs = CommonFSUtils.getWALFileSystem(conf);
      FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf);
      return dropStaleEditsUnder(walFs, regionWALDir, durableSeqId, regionInfo)
        && dropStaleEditsUnder(rootFs, regionDir, durableSeqId, regionInfo)
        && dropStaleEditsUnder(walFs, wrongRegionWALDir, durableSeqId, regionInfo);
    } catch (IOException e) {
      LOG.warn("Failed to inspect recovered.edits for {}; falling back to abort", regionInfo, e);
      return false;
    }
  }

  private static boolean dropStaleEditsUnder(FileSystem fs, Path regionDir, long durableSeqId,
    RegionInfo regionInfo) throws IOException {
    NavigableSet<Path> files = WALSplitUtil.getSplitEditFilesSorted(fs, regionDir);
    if (files.isEmpty()) {
      return true;
    }
    for (Path p : files) {
      long fileMaxSeqId;
      try {
        fileMaxSeqId = Long.parseLong(p.getName());
      } catch (NumberFormatException e) {
        LOG.warn("Non-numeric recovered.edits filename {} for {}; not dropping", p, regionInfo);
        return false;
      }
      if (fileMaxSeqId > durableSeqId) {
        LOG.info("Recovered.edits {} for {} has maxSeqId={} > durableSeqId={}; needs replay", p,
          regionInfo, fileMaxSeqId, durableSeqId);
        return false;
      }
    }
    for (Path p : files) {
      LOG.info("Removing stale recovered.edits {} for {} (durableSeqId={})", p, regionInfo,
        durableSeqId);
      if (!fs.delete(p, false)) {
        LOG.warn("Failed to delete stale recovered.edits {} for {}", p, regionInfo);
        return false;
      }
    }
    return true;
  }

  /**
   * For splitting, need to test both region info and state, and will return true if either of the
   * test returns true. Please see the comments in
   * {@link AssignmentManager#markRegionAsSplit(RegionInfo, ServerName, RegionInfo, RegionInfo)} for
   * more details on why we need to test two conditions.
   */
  static boolean isSplitOrMerged(RegionStateNode regionStateNode) {
    return regionStateNode.getState() == RegionState.State.SPLIT
      || regionStateNode.getRegionInfo().isSplit()
      || regionStateNode.getState() == RegionState.State.MERGED;
  }
}
