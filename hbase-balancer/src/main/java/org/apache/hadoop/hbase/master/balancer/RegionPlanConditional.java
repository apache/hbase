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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Evolving
public abstract class RegionPlanConditional {
  private static final Logger LOG = LoggerFactory.getLogger(RegionPlanConditional.class);
  private BalancerClusterState cluster;

  RegionPlanConditional(Configuration conf, BalancerClusterState cluster) {
    this.cluster = cluster;
  }

  public enum ValidationLevel {
    /**
     * Just check the server.
     */
    SERVER,
    /**
     * Check the server and the host.
     */
    SERVER_HOST,
    /**
     * Check the server, host, and rack.
     */
    SERVER_HOST_RACK
  }

  void setClusterState(BalancerClusterState cluster) {
    this.cluster = cluster;
  }

  /**
   * Returns a {@link ValidationLevel} that is appropriate for this conditional.
   * @return the validation level
   */
  abstract ValidationLevel getValidationLevel();

  /**
   * Get the candidate generator(s) for this conditional. This can be useful to provide the balancer
   * with hints that will appease your conditional. Your conditionals will be triggered in order.
   * @return the candidate generator for this conditional
   */
  abstract List<RegionPlanConditionalCandidateGenerator> getCandidateGenerators();

  /**
   * Check if the conditional is violated by the given region plan.
   * @param regionPlan the region plan to check
   * @return true if the conditional is violated
   */
  boolean isViolating(RegionPlan regionPlan) {
    if (regionPlan == null) {
      return false;
    }
    int destinationServerIdx = cluster.serversToIndex.get(regionPlan.getDestination().getAddress());

    // Check Server
    int[] destinationRegionIndices = cluster.regionsPerServer[destinationServerIdx];
    Set<RegionInfo> serverRegions =
      getRegionsFromIndex(destinationServerIdx, cluster.regionsPerServer);
    for (int regionIdx : destinationRegionIndices) {
      serverRegions.add(cluster.regions[regionIdx]);
    }
    if (isViolatingServer(regionPlan, serverRegions)) {
      return true;
    }

    if (getValidationLevel() == ValidationLevel.SERVER) {
      return false;
    }

    // Check Host
    int hostIdx = cluster.serverIndexToHostIndex[destinationServerIdx];
    Set<RegionInfo> hostRegions = getRegionsFromIndex(hostIdx, cluster.regionsPerHost);
    if (isViolatingHost(regionPlan, hostRegions)) {
      return true;
    }

    if (getValidationLevel() == ValidationLevel.SERVER_HOST) {
      return false;
    }

    // Check Rack
    int rackIdx = cluster.serverIndexToRackIndex[destinationServerIdx];
    Set<RegionInfo> rackRegions = getRegionsFromIndex(rackIdx, cluster.regionsPerRack);
    if (isViolatingRack(regionPlan, rackRegions)) {
      return true;
    }

    return false;
  }

  abstract boolean isViolatingServer(RegionPlan regionPlan, Set<RegionInfo> destinationRegions);

  boolean isViolatingHost(RegionPlan regionPlan, Set<RegionInfo> destinationRegions) {
    return false;
  }

  boolean isViolatingRack(RegionPlan regionPlan, Set<RegionInfo> destinationRegions) {
    return false;
  }

  private Set<RegionInfo> getRegionsFromIndex(int index, int[][] regionsPerIndex) {
    int[] regionIndices = regionsPerIndex[index];
    if (regionIndices == null) {
      return Collections.emptySet();
    }
    return Arrays.stream(regionIndices).mapToObj(idx -> cluster.regions[idx])
      .collect(Collectors.toSet());
  }
}
