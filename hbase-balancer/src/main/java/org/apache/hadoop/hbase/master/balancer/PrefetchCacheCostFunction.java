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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Compute the cost of a potential cluster configuration based on the number of HFile's already
 * cached in the bucket cache
 */
@InterfaceAudience.Private
public class PrefetchCacheCostFunction extends CostFunction {
  private static final String PREFETCH_CACHE_COST_KEY =
    "hbase.master.balancer.stochastic.prefetchCacheCost";
  private static final float DEFAULT_PREFETCH_COST = 500;

  private String prefetchedFileListPath;
  private double prefetchRatio;
  private double bestPrefetchRatio;

  /**
   * The prefetch cache cost function is enabled only when the prefetch file list persistence is
   * enabled by setting the parameter PREFETCH_PERSISTENCE_PATH_KEY. The prefetch file list
   * persistence is disabled by default. The prefetch cache cost function is also disabled if the
   * multiplier is set to 0. The prefetch cache ratio function would be most relevant for non-hdfs
   * deployments, which then makes locality irrelevant. In those cases, prefetch and region skewness
   * would be competing to prevail over the final balancer decision.
   * @param conf Cluster configuration
   */
  PrefetchCacheCostFunction(Configuration conf) {
    prefetchedFileListPath = conf.get(HConstants.PREFETCH_PERSISTENCE_PATH_KEY);
    this.setMultiplier(prefetchedFileListPath == null
      ? 0.0f
      : conf.getFloat(PREFETCH_CACHE_COST_KEY, DEFAULT_PREFETCH_COST));
    prefetchRatio = 0.0;
    bestPrefetchRatio = 0.0;
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    prefetchRatio = 0.0;
    bestPrefetchRatio = 0.0;

    for (int region = 0; region < cluster.numRegions; region++) {
      prefetchRatio +=
        cluster.getOrComputeWeightedPrefetchRatio(region, cluster.regionIndexToServerIndex[region]);
      bestPrefetchRatio += cluster.getOrComputeWeightedPrefetchRatio(region,
        cluster.getOrComputeServerWithBestPrefetchRatio()[region]);
    }
    prefetchRatio = bestPrefetchRatio == 0.0 ? 1.0 : prefetchRatio / bestPrefetchRatio;
  }

  @Override
  protected double cost() {
    return 1 - prefetchRatio;
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    double oldServerPrefetch = cluster.getOrComputeWeightedPrefetchRatio(region, oldServer);
    double newServerPrefetch = cluster.getOrComputeWeightedPrefetchRatio(region, newServer);
    double prefetchDelta = newServerPrefetch - oldServerPrefetch;
    double normalizeDelta = bestPrefetchRatio == 0.0 ? 0.0 : prefetchDelta / bestPrefetchRatio;
    prefetchRatio += normalizeDelta;
  }

  @Override
  public final void updateWeight(double[] weights) {
    weights[StochasticLoadBalancer.GeneratorType.PREFETCH.ordinal()] += cost();
  }
}
