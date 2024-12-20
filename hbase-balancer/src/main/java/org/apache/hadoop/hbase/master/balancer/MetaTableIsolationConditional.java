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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * If enabled, this class will help the balancer ensure that the meta table lives on its own
 * RegionServer. Configure this via {@link BalancerConditionals#ISOLATE_META_TABLE_KEY}
 */
@InterfaceAudience.Private
class MetaTableIsolationConditional extends RegionPlanConditional {

  private final Set<ServerName> emptyServers = new HashSet<>();
  private final Set<ServerName> serversHostingMeta = new HashSet<>();

  public MetaTableIsolationConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);

    for (int i = 0; i < cluster.servers.length; i++) {
      ServerName server = cluster.servers[i];
      boolean hasMeta = false;
      boolean hasOtherRegions = false;

      for (int region : cluster.regionsPerServer[i]) {
        RegionInfo regionInfo = cluster.regions[region];
        if (regionInfo.getTable().equals(TableName.META_TABLE_NAME)) {
          hasMeta = true;
        } else {
          hasOtherRegions = true;
        }
      }

      if (hasMeta) {
        serversHostingMeta.add(server);
      } else if (!hasOtherRegions) {
        emptyServers.add(server);
      }
    }
  }

  @Override
  Optional<RegionPlanConditionalCandidateGenerator> getCandidateGenerator() {
    return Optional.of(new MetaTableIsolationCandidateGenerator());
  }

  @Override
  public boolean isViolating(RegionPlan regionPlan) {
    return checkViolation(regionPlan, serversHostingMeta, emptyServers);
  }

  /**
   * Checks if the placement of `hbase:meta` adheres to isolation rules.
   * @param regionPlan         The region plan being evaluated.
   * @param serversHostingMeta Servers currently hosting `hbase:meta`.
   * @param emptyServers       Servers with no regions.
   * @return True if the placement violates isolation, false otherwise.
   */
  protected static boolean checkViolation(RegionPlan regionPlan, Set<ServerName> serversHostingMeta,
    Set<ServerName> emptyServers) {
    boolean isMeta = regionPlan.getRegionInfo().getTable().equals(TableName.META_TABLE_NAME);
    ServerName destination = regionPlan.getDestination();
    if (isMeta) {
      // meta must go to an empty server or a server already hosting meta
      return !(serversHostingMeta.contains(destination) || emptyServers.contains(destination));
    } else {
      // Non-meta regions must not go to servers hosting meta
      return serversHostingMeta.contains(destination);
    }
  }
}
