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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.replicas.ReplicaKey;
import org.apache.hadoop.hbase.master.balancer.replicas.ReplicaKeyCache;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * If enabled, this class will help the balancer ensure that replicas aren't placed on the same
 * servers or racks as their primary. Configure this via
 * {@link BalancerConditionals#DISTRIBUTE_REPLICAS_KEY}
 */
@InterfaceAudience.Private
public class DistributeReplicasConditional extends RegionPlanConditional {

  /**
   * Local mini cluster tests are only run on one host/rack by design. If enabled, this will pretend
   * that only server isolation is necessary for sufficient replica distribution. This should only
   * be used in tests.
   */
  public static final String TEST_MODE_ENABLED_KEY =
    "hbase.replica.distribution.conditional.testModeEnabled";

  private final boolean isTestModeEnabled;
  private final List<RegionPlanConditionalCandidateGenerator> candidateGenerators;

  public DistributeReplicasConditional(BalancerConditionals balancerConditionals,
    BalancerClusterState cluster) {
    super(balancerConditionals.getConf(), cluster);
    Configuration conf = balancerConditionals.getConf();
    this.isTestModeEnabled = conf.getBoolean(TEST_MODE_ENABLED_KEY, false);
    float slop =
      conf.getFloat(BaseLoadBalancer.REGIONS_SLOP_KEY, BaseLoadBalancer.REGIONS_SLOP_DEFAULT);
    this.candidateGenerators =
      ImmutableList.of(new DistributeReplicasCandidateGenerator(balancerConditionals),
        new SlopFixingCandidateGenerator(balancerConditionals, slop));
  }

  @Override
  public ValidationLevel getValidationLevel() {
    if (isTestModeEnabled) {
      return ValidationLevel.SERVER;
    }
    return ValidationLevel.SERVER_HOST_RACK;
  }

  @Override
  List<RegionPlanConditionalCandidateGenerator> getCandidateGenerators() {
    return candidateGenerators;
  }

  @Override
  boolean isViolatingServer(RegionPlan regionPlan, Set<RegionInfo> serverRegions) {
    return checkViolation(regionPlan.getRegionInfo(), getReplicaKey(regionPlan.getRegionInfo()),
      serverRegions);
  }

  @Override
  boolean isViolatingHost(RegionPlan regionPlan, Set<RegionInfo> hostRegions) {
    return checkViolation(regionPlan.getRegionInfo(), getReplicaKey(regionPlan.getRegionInfo()),
      hostRegions);
  }

  @Override
  boolean isViolatingRack(RegionPlan regionPlan, Set<RegionInfo> rackRegions) {
    return checkViolation(regionPlan.getRegionInfo(), getReplicaKey(regionPlan.getRegionInfo()),
      rackRegions);
  }

  private boolean checkViolation(RegionInfo movingRegion, ReplicaKey movingReplicaKey,
    Set<RegionInfo> destinationRegions) {
    for (RegionInfo regionInfo : destinationRegions) {
      if (regionInfo.equals(movingRegion)) {
        continue;
      }
      if (getReplicaKey(regionInfo).equals(movingReplicaKey)) {
        return true;
      }
    }
    return false;
  }

  static ReplicaKey getReplicaKey(RegionInfo regionInfo) {
    return ReplicaKeyCache.getInstance().getReplicaKey(regionInfo);
  }

}
