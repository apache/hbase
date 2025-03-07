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

import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

abstract class TableIsolationConditional extends RegionPlanConditional {

  private final List<RegionPlanConditionalCandidateGenerator> candidateGenerators;

  TableIsolationConditional(TableIsolationCandidateGenerator generator,
    BalancerConditionals balancerConditionals, BalancerClusterState cluster) {
    super(balancerConditionals.getConf(), cluster);

    this.candidateGenerators =
      ImmutableList.of(generator, new SlopFixingCandidateGenerator(balancerConditionals));
  }

  abstract boolean isRegionToIsolate(RegionInfo regionInfo);

  boolean isServerHostingIsolatedTables(BalancerClusterState cluster, int serverIdx) {
    for (int regionIdx : cluster.regionsPerServer[serverIdx]) {
      if (isRegionToIsolate(cluster.regions[regionIdx])) {
        return true;
      }
    }
    return false;
  }

  @Override
  ValidationLevel getValidationLevel() {
    return ValidationLevel.SERVER;
  }

  @Override
  List<RegionPlanConditionalCandidateGenerator> getCandidateGenerators() {
    return candidateGenerators;
  }

  @Override
  public boolean isViolatingServer(RegionPlan regionPlan, Set<RegionInfo> serverRegions) {
    RegionInfo regionBeingMoved = regionPlan.getRegionInfo();
    boolean shouldIsolateMovingRegion = isRegionToIsolate(regionBeingMoved);
    for (RegionInfo destinationRegion : serverRegions) {
      if (destinationRegion.getEncodedName().equals(regionBeingMoved.getEncodedName())) {
        // Skip the region being moved
        continue;
      }
      if (shouldIsolateMovingRegion && !isRegionToIsolate(destinationRegion)) {
        // Ensure every destination region is also a region to isolate
        return true;
      } else if (!shouldIsolateMovingRegion && isRegionToIsolate(destinationRegion)) {
        // Ensure no destination region is a region to isolate
        return true;
      }
    }
    return false;
  }

}
