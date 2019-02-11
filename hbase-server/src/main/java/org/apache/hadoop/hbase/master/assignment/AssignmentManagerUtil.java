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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;

/**
 * Utility for this assignment package only.
 */
@InterfaceAudience.Private
final class AssignmentManagerUtil {
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
      MasterProcedureEnv env, Stream<RegionInfo> regions, int regionReplication)
      throws IOException {
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
          TransitRegionStateProcedure.unassign(env, regionNode.getRegionInfo());
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
   *          already in transition, otherwise we will add an assert that it should not in
   *          transition.
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
              if (regionNode.isInTransition()) {
                return null;
              }
            } else {
              // should never fail, as we have the exclusive region lock, and the region is newly
              // created, or has been successfully closed so should not be on any servers, so SCP
              // will
              // not process it either.
              assert !regionNode.isInTransition();
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
        replicaRegionInfos.add(RegionReplicaUtil.getRegionInfoForReplica(hri, i));
      }
    }
    // create round robin procs. Note that we exclude the primary region's target server
    TransitRegionStateProcedure[] replicaRegionAssignProcs =
        env.getAssignmentManager().createRoundRobinAssignProcedures(replicaRegionInfos,
          Collections.singletonList(targetServer));
    // combine both the procs and return the result
    return ArrayUtils.addAll(primaryRegionProcs, replicaRegionAssignProcs);
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
    if (WALSplitter.hasRecoveredEdits(env.getMasterServices().getFileSystem(),
      env.getMasterConfiguration(), regionInfo)) {
      throw new IOException("Recovered.edits are found in Region: " + regionInfo +
        ", abort split/merge to prevent data loss");
    }
  }
}
