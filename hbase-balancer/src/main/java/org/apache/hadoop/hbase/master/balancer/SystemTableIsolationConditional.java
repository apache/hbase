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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;

/**
 * If enabled, this class will help the balancer ensure that system tables live on their own
 * RegionServer. System tables will share one RegionServer! This conditional can be used in tandem
 * with {@link MetaTableIsolationConditional} to add a second RegionServer specifically for meta
 * table hosting. Configure this via {@link BalancerConditionals#ISOLATE_SYSTEM_TABLES_KEY}
 */
class SystemTableIsolationConditional extends RegionPlanConditional {

  private final Set<ServerName> serversHostingSystemTables = new HashSet<>();
  private final Set<ServerName> metaOnlyServers = new HashSet<>();
  private final Set<ServerName> metaAndSystemServers = new HashSet<>();

  public SystemTableIsolationConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);

    boolean isolatingMeta = conf.getBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, false);

    // Track what each server holds
    Set<ServerName> metaServers = new HashSet<>();
    Set<ServerName> systemServers = new HashSet<>();

    for (int i = 0; i < cluster.regions.length; i++) {
      RegionInfo regionInfo = cluster.regions[i];
      ServerName server = cluster.servers[cluster.regionIndexToServerIndex[i]];
      if (regionInfo.isMetaRegion()) {
        if (isolatingMeta) {
          metaOnlyServers.add(server);
        }
        metaServers.add(server);
      } else if (regionInfo.getTable().isSystemTable()) {
        serversHostingSystemTables.add(server);
        systemServers.add(server);
      }
    }

    // Identify servers that have both meta and system
    for (ServerName s : metaServers) {
      if (systemServers.contains(s)) {
        metaAndSystemServers.add(s);
      }
    }
  }

  @Override
  Optional<RegionPlanConditionalCandidateGenerator> getCandidateGenerator() {
    return Optional.of(new SystemTableIsolationCandidateGenerator(
      BalancerConditionals.INSTANCE.isMetaTableIsolationEnabled()));
  }

  @Override
  public boolean isViolating(RegionPlan regionPlan) {
    return checkViolation(regionPlan, serversHostingSystemTables, metaOnlyServers,
      metaAndSystemServers);
  }

  protected static boolean checkViolation(RegionPlan regionPlan,
    Set<ServerName> serversHostingSystemTables, Set<ServerName> metaOnlyServers,
    Set<ServerName> metaAndSystemServers) {

    // If we're moving from a server that has both meta and system, allow any move
    // for system tables to escape that server.
    boolean isSystemTable = regionPlan.getRegionInfo().getTable().isSystemTable();
    if (isSystemTable && metaAndSystemServers.contains(regionPlan.getSource())) {
      // Relax all constraints. This move is an improvement because it separates meta and system.
      return false;
    }

    // If meta isolation is enabled and we're dealing with a meta region
    if (!metaOnlyServers.isEmpty() && regionPlan.getRegionInfo().isMetaRegion()) {
      return metaOnlyServers.contains(regionPlan.getDestination());
    }

    if (isSystemTable) {
      // If metaOnlyServers contain the source, and destination isn't metaOnly, allow the move.
      if (
        metaOnlyServers.contains(regionPlan.getSource())
          && !metaOnlyServers.contains(regionPlan.getDestination())
      ) {
        return false;
      }

      // If there's already a server that exclusively hosts system tables,
      // disallow placing system tables on a server that doesn't currently host them.
      if (
        !serversHostingSystemTables.isEmpty()
          && !serversHostingSystemTables.contains(regionPlan.getDestination())
      ) {
        return true; // violation
      }

      // Otherwise no violation
      return false;

    } else {
      // For non-system tables, ensure the destination server does not host system tables.
      return serversHostingSystemTables.contains(regionPlan.getDestination());
    }
  }

}
