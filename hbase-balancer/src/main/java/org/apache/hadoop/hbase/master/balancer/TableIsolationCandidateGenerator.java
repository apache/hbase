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
package org.apache.hadoop.hbase.master.balancer;

import static java.util.Collections.shuffle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class TableIsolationCandidateGenerator
  extends RegionPlanConditionalCandidateGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(TableIsolationCandidateGenerator.class);

  abstract boolean shouldBeIsolated(RegionInfo regionInfo);

  // Once we choose a server to evacuate system tables to (when separating meta & system),
  // we remember it here to ensure all system table moves go to the same host.
  private Integer preferredSystemHostForEvacuation = null;

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
    return generateCandidate(cluster, false);
  }

  BalanceAction generateCandidate(BalancerClusterState cluster, boolean isWeighing) {
    if (
      !BalancerConditionals.INSTANCE.isSystemTableIsolationEnabled()
        && !BalancerConditionals.INSTANCE.isMetaTableIsolationEnabled()
    ) {
      return BalanceAction.NULL_ACTION;
    }

    for (int serverIdx = 0; serverIdx < cluster.numServers; serverIdx++) {
      Set<TableName> tablesToIsolate = new HashSet<>();
      boolean hasRegionsToIsolate = false;
      boolean hasRegionsToMove = false;
      for (int regionIdx : cluster.regionsPerServer[serverIdx]) {
        RegionInfo regionInfo = cluster.regions[regionIdx];
        if (shouldBeIsolated(regionInfo)) {
          hasRegionsToIsolate = true;
          tablesToIsolate.add(regionInfo.getTable());
        } else {
          hasRegionsToMove = true;
        }
        if (hasRegionsToMove && hasRegionsToIsolate) {
          break;
        }
      }
      if (hasRegionsToMove && hasRegionsToIsolate) {
        return new IsolateTablesAction(cluster, serverIdx, tablesToIsolate);
      }
    }

    boolean metaIsolationEnabled = BalancerConditionals.INSTANCE.isMetaTableIsolationEnabled();

    // **FAST VALIDATION STEP**:
    // If meta isolation is enabled, check if meta is already isolated.
    // If system isolation is enabled, check if system tables are already isolated.
    // If everything is isolated as desired, immediately return NULL_ACTION to speed up balancing.
    if (isAlreadyIsolated(cluster, metaIsolationEnabled)) {
      LOG.trace("Meta and system tables are already isolated as desired. Returning NULL_ACTION.");
      return BalanceAction.NULL_ACTION;
    }

    // PHASE 1: If meta isolation is enabled, first attempt to separate meta and system tables.
    if (metaIsolationEnabled) {
      BalanceAction metaSystemSeparationAction = attemptMetaSystemSeparation(cluster);
      if (metaSystemSeparationAction != BalanceAction.NULL_ACTION) {
        return metaSystemSeparationAction;
      }
    }

    // PHASE 2: If no meta-system separation was needed or possible, proceed with normal logic.
    List<Integer> shuffledServerIndices = new ArrayList<>(cluster.numServers);
    for (int i = 0; i < cluster.servers.length; i++) {
      shuffledServerIndices.add(i);
    }
    shuffle(shuffledServerIndices);

    int lastMovableRegion = -1;

    for (int serverIndex : shuffledServerIndices) {
      boolean hasRegionToBeIsolated = false;
      lastMovableRegion = -1;

      for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
        RegionInfo regionInfo = cluster.regions[regionIndex];
        if (shouldBeIsolated(regionInfo)) {
          hasRegionToBeIsolated = true;
        } else {
          lastMovableRegion = regionIndex;
        }

        if (hasRegionToBeIsolated && lastMovableRegion != -1) {
          boolean isMetaRegion = regionInfo.isMetaRegion();
          boolean isSystemTable = !isMetaRegion && regionInfo.getTable().isSystemTable();

          for (int maybeToServer : shuffledServerIndices) {
            if (maybeToServer == serverIndex) {
              continue;
            }

            if (isMetaRegion && metaIsolationEnabled) {
              // Meta must be on a meta-only server
              if (!isExclusiveMetaServer(cluster, maybeToServer)) {
                continue;
              }
            } else if (isSystemTable) {
              // System must be on a system-only server
              if (!isExclusiveSystemServer(cluster, maybeToServer)) {
                continue;
              }
            } else {
              // For user tables, avoid meta-only or system-only servers
              if (
                isExclusiveMetaServer(cluster, maybeToServer)
                  || isExclusiveSystemServer(cluster, maybeToServer)
              ) {
                continue;
              }
            }

            BalanceAction maybeAction =
              getAction(serverIndex, lastMovableRegion, maybeToServer, -1);
            if (willBeAccepted(cluster, maybeAction)) {
              return maybeAction;
            }
          }
        }
      }
    }

    return BalanceAction.NULL_ACTION;
  }

  /**
   * Phase 1 Attempt: If meta isolation is enabled and a server hosts both meta and system, we try
   * to separate them. We do NOT shuffle the servers here, so we always pick the first acceptable
   * host in index order, ensuring consistency. Once we find a suitable host for system tables, we
   * store it in preferredSystemHostForEvacuation to always use the same host.
   * @param cluster current cluster state
   * @return a BalanceAction that moves either meta or system tables off a mixed server, or
   *         NULL_ACTION if none found
   */
  private BalanceAction attemptMetaSystemSeparation(BalancerClusterState cluster) {
    // Don't shuffle here. Use servers in a stable order.
    List<Integer> orderedServers = new ArrayList<>(cluster.numServers);
    for (int i = 0; i < cluster.numServers; i++) {
      orderedServers.add(i);
    }

    for (int serverIndex : orderedServers) {
      boolean hasMeta = false;
      boolean hasSystem = false;
      List<Integer> nonMetaSystemRegions = new ArrayList<>();

      // Identify what's on this server
      for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
        RegionInfo ri = cluster.regions[regionIndex];
        if (ri.isMetaRegion()) {
          hasMeta = true;
        } else if (ri.getTable().isSystemTable()) {
          hasSystem = true;
        } else {
          nonMetaSystemRegions.add(regionIndex);
        }
      }

      // If this server hosts both meta and system, we need to separate them.
      if (hasMeta && hasSystem) {
        // Move user (non-meta-system) regions out first
        for (int regionIndex : nonMetaSystemRegions) {
          for (int targetServer : orderedServers) {
            if (targetServer == serverIndex) continue;
            // Avoid placing user regions on meta/system-only servers
            if (
              isExclusiveMetaServer(cluster, targetServer)
                || isExclusiveSystemServer(cluster, targetServer)
            ) {
              continue;
            }

            BalanceAction maybeAction = getAction(serverIndex, regionIndex, targetServer, -1);
            if (willBeAccepted(cluster, maybeAction)) {
              return maybeAction;
            }
          }
        }

        // Try moving system regions
        for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
          RegionInfo ri = cluster.regions[regionIndex];
          if (ri.getTable().isSystemTable() && !ri.isMetaRegion()) {
            // First try strict placement (system-only or empty) if we haven't chosen a host yet
            if (preferredSystemHostForEvacuation == null) {
              // Attempt to pick a strict host first
              Integer strictHost = findSystemHostStrict(cluster, orderedServers, serverIndex);
              if (strictHost == null) {
                // No strict host found, pick ANY host
                Integer anyHost = findAnyHostForSystem(cluster, orderedServers, serverIndex);
                if (anyHost == null) {
                  // If we can't find any host, continue trying other actions
                  continue;
                }
                preferredSystemHostForEvacuation = anyHost;
              } else {
                preferredSystemHostForEvacuation = strictHost;
              }
            }

            // Now we have preferredSystemHostForEvacuation chosen.
            BalanceAction moveAction =
              getAction(serverIndex, regionIndex, preferredSystemHostForEvacuation, -1);
            if (willBeAccepted(cluster, moveAction)) {
              return moveAction;
            }
          }
        }

        // If we couldn't move system, try moving meta
        // (But normally meta is rarer, so system moves are prioritized)
        for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
          RegionInfo ri = cluster.regions[regionIndex];
          if (ri.isMetaRegion()) {
            // Try meta-only or empty suitable for meta
            Integer metaHost = findMetaHostStrict(cluster, orderedServers, serverIndex);
            if (metaHost == null) {
              // If no strict meta host found, pick ANY host
              metaHost = findAnyHostForSystem(cluster, orderedServers, serverIndex);
              if (metaHost == null) {
                continue;
              }
            }

            BalanceAction moveAction = getAction(serverIndex, regionIndex, metaHost, -1);
            if (willBeAccepted(cluster, moveAction)) {
              return moveAction;
            }
          }
        }
      }
    }

    // No meta-system separation action found
    return BalanceAction.NULL_ACTION;
  }

  /**
   * Quick check to see if meta and system tables are already isolated as desired. Conditions: - If
   * meta isolation is enabled: Check that meta regions are all on a meta-only server. - If system
   * isolation is enabled: Check that system tables are all on a system-only server. If all
   * conditions are met, return true, else false.
   */
  private boolean isAlreadyIsolated(BalancerClusterState cluster, boolean metaIsolationEnabled) {
    boolean systemIsolationEnabled = BalancerConditionals.INSTANCE.isSystemTableIsolationEnabled();

    // Track whether we found a meta-only server if meta isolation is enabled
    boolean metaIsolated = !metaIsolationEnabled; // If not enabled, trivially isolated
    boolean systemIsolated = !systemIsolationEnabled; // If not enabled, trivially isolated

    // Identify servers
    for (int serverIndex = 0; serverIndex < cluster.numServers; serverIndex++) {
      if (cluster.regionsPerServer[serverIndex].length == 0) {
        // Empty server is no violation. Continue.
        continue;
      }

      boolean hasMeta = false;
      boolean hasSystem = false;
      boolean hasUser = false;

      for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
        RegionInfo ri = cluster.regions[regionIndex];
        if (ri.isMetaRegion()) {
          hasMeta = true;
        } else if (ri.getTable().isSystemTable()) {
          hasSystem = true;
        } else {
          hasUser = true;
        }

        // If we find a mixed condition on a server that should be isolated,
        // we know isolation isn't achieved.
        // E.g., a server with meta+user or meta+system but meta isolation is enabled means not
        // isolated.
      }

      if (metaIsolationEnabled) {
        // If we have meta regions, they must be alone (no system, no user)
        if (hasMeta && (hasSystem || hasUser)) {
          // Meta is not isolated properly
          metaIsolated = false;
        } else if (hasMeta && !hasSystem && !hasUser) {
          // Found a proper meta-only server
          metaIsolated = true;
        }
      }

      if (systemIsolationEnabled) {
        // If we have system regions, they must be alone (no meta, no user)
        if (hasSystem && (hasMeta || hasUser)) {
          // System not isolated properly
          systemIsolated = false;
        } else if (hasSystem && !hasMeta && !hasUser) {
          // Found a proper system-only server
          systemIsolated = true;
        }
      }
    }

    // If both required isolations are confirmed, return true
    return metaIsolated && systemIsolated;
  }

  /**
   * Find a strict system host (system-only or empty) from a stable server list.
   */
  private Integer findSystemHostStrict(BalancerClusterState cluster, List<Integer> servers,
    int excludeServer) {
    for (int s : servers) {
      if (s == excludeServer) continue;
      if (couldHostSystem(cluster, s)) {
        return s;
      }
    }
    return null;
  }

  /**
   * Find any host to place system regions on. This is a fallback if no strict host is found.
   */
  private Integer findAnyHostForSystem(BalancerClusterState cluster, List<Integer> servers,
    int excludeServer) {
    for (int s : servers) {
      if (s == excludeServer) continue;
      // No checks here, any server will do
      return s;
    }
    return null;
  }

  /**
   * Find a meta host (meta-only or empty) from a stable server list.
   */
  private Integer findMetaHostStrict(BalancerClusterState cluster, List<Integer> servers,
    int excludeServer) {
    for (int s : servers) {
      if (s == excludeServer) continue;
      if (couldHostMeta(cluster, s)) {
        return s;
      }
    }
    return null;
  }

  /**
   * Determine if a server could host system tables alone or is empty.
   */
  private boolean couldHostSystem(BalancerClusterState cluster, int serverIndex) {
    for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
      RegionInfo ri = cluster.regions[regionIndex];
      if (ri.isMetaRegion() || !ri.getTable().isSystemTable()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determine if a server could host meta only or is empty.
   */
  private boolean couldHostMeta(BalancerClusterState cluster, int serverIndex) {
    for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
      RegionInfo ri = cluster.regions[regionIndex];
      if (!ri.isMetaRegion()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the given server exclusively hosts meta regions (and nothing else).
   */
  private boolean isExclusiveMetaServer(BalancerClusterState cluster, int serverIndex) {
    boolean foundAny = false;
    for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
      RegionInfo ri = cluster.regions[regionIndex];
      if (!ri.isMetaRegion()) {
        return false;
      }
      foundAny = true;
    }
    return foundAny;
  }

  /**
   * Checks if the given server exclusively hosts system table regions (no meta, no user).
   */
  private boolean isExclusiveSystemServer(BalancerClusterState cluster, int serverIndex) {
    boolean foundAny = false;
    for (int regionIndex : cluster.regionsPerServer[serverIndex]) {
      RegionInfo ri = cluster.regions[regionIndex];
      if (ri.isMetaRegion() || !ri.getTable().isSystemTable()) {
        return false;
      }
      foundAny = true;
    }
    return foundAny;
  }
}
