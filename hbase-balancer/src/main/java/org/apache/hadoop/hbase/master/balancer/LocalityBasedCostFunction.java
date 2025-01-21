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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.balancer.BalancerClusterState.LocalityType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Compute a cost of a potential cluster configuration based upon where
 * {@link org.apache.hadoop.hbase.regionserver.HStoreFile}s are located.
 */
@InterfaceAudience.Private
abstract class LocalityBasedCostFunction extends CostFunction {

  private final LocalityType type;

  private double bestLocality; // best case locality across cluster weighted by local data size
  private double locality; // current locality across cluster weighted by local data size

  LocalityBasedCostFunction(Configuration conf, LocalityType type, String localityCostKey,
    float defaultLocalityCost) {
    this.type = type;
    this.setMultiplier(conf.getFloat(localityCostKey, defaultLocalityCost));
    this.locality = 0.0;
    this.bestLocality = 0.0;
  }

  /**
   * Maps region to the current entity (server or rack) on which it is stored
   */
  abstract int regionIndexToEntityIndex(int region);

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    locality = 0.0;
    bestLocality = 0.0;

    for (int region = 0; region < cluster.numRegions; region++) {
      locality += getWeightedLocality(region, regionIndexToEntityIndex(region));
      bestLocality += getWeightedLocality(region, getMostLocalEntityForRegion(region));
    }

    // We normalize locality to be a score between 0 and 1.0 representing how good it
    // is compared to how good it could be. If bestLocality is 0, assume locality is 100
    // (and the cost is 0)
    locality = bestLocality == 0 ? 1.0 : locality / bestLocality;
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    int oldEntity =
      type == LocalityType.SERVER ? oldServer : cluster.serverIndexToRackIndex[oldServer];
    int newEntity =
      type == LocalityType.SERVER ? newServer : cluster.serverIndexToRackIndex[newServer];
    double localityDelta =
      getWeightedLocality(region, newEntity) - getWeightedLocality(region, oldEntity);
    double normalizedDelta = bestLocality == 0 ? 0.0 : localityDelta / bestLocality;
    locality += normalizedDelta;
  }

  @Override
  protected final double cost() {
    return 1 - locality;
  }

  private int getMostLocalEntityForRegion(int region) {
    return cluster.getOrComputeRegionsToMostLocalEntities(type)[region];
  }

  private double getWeightedLocality(int region, int entity) {
    return cluster.getOrComputeWeightedLocality(region, entity, type);
  }

  @Override
  public final void updateWeight(Map<Class<? extends CandidateGenerator>, Double> weights) {
    weights.merge(LocalityBasedCandidateGenerator.class, cost(), Double::sum);
  }
}
