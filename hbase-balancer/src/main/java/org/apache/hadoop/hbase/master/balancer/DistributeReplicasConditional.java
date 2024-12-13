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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;

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

  private static final Logger LOG = LoggerFactory.getLogger(DistributeReplicasConditional.class);
  private static final LoadingCache<RegionInfo, ReplicaKey> REPLICA_KEY_CACHE =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(Duration.ofMinutes(5))
      .build(new CacheLoader<RegionInfo, ReplicaKey>() {
        @Override
        public ReplicaKey load(RegionInfo region) {
          return new ReplicaKey(region);
        }
      });

  private final boolean isTestModeEnabled;

  public DistributeReplicasConditional(Configuration conf, BalancerClusterState cluster) {
    super(conf, cluster);
    this.isTestModeEnabled = conf.getBoolean(TEST_MODE_ENABLED_KEY, false);
  }

  static ReplicaKey getReplicaKey(RegionInfo regionInfo) {
    return REPLICA_KEY_CACHE.getUnchecked(regionInfo);
  }

  @Override
  public ValidationLevel getValidationLevel() {
    if (isTestModeEnabled) {
      return ValidationLevel.SERVER;
    }
    return ValidationLevel.RACK;
  }

  @Override
  List<RegionPlanConditionalCandidateGenerator> getCandidateGenerators() {
    return Collections.singletonList(DistributeReplicasCandidateGenerator.INSTANCE);
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

  /**
   * This is necessary because it would be too expensive to use
   * {@link RegionReplicaUtil#isReplicasForSameRegion(RegionInfo, RegionInfo)} for every combo of
   * regions.
   */
  static class ReplicaKey {
    private final Pair<ByteArrayWrapper, ByteArrayWrapper> startAndStopKeys;

    ReplicaKey(RegionInfo regionInfo) {
      this.startAndStopKeys = new Pair<>(new ByteArrayWrapper(regionInfo.getStartKey()),
        new ByteArrayWrapper(regionInfo.getEndKey()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ReplicaKey)) {
        return false;
      }
      ReplicaKey other = (ReplicaKey) o;
      return this.startAndStopKeys.equals(other.startAndStopKeys);
    }

    @Override
    public int hashCode() {
      return startAndStopKeys.hashCode();
    }
  }

  static class ByteArrayWrapper {
    private final byte[] bytes;

    ByteArrayWrapper(byte[] prefix) {
      this.bytes = Arrays.copyOf(prefix, prefix.length);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ByteArrayWrapper)) {
        return false;
      }
      ByteArrayWrapper other = (ByteArrayWrapper) o;
      return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

}
