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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Compute the cost of a potential cluster configuration based on
 * the number of HFile's already cached in the bucket cache
 */
@InterfaceAudience.Private
public class PrefetchCacheCostFunction extends CostFunction {
  private String prefetchedFileListPath;
  private float prefetchRatio;
  private float bestPrefetchRatio;

  public static final String PREFETCH_PERSISTENCE_PATH_KEY = "hbase.prefetch.file-list.path";

  PrefetchCacheCostFunction(Configuration conf) {
    prefetchedFileListPath = conf.get(PREFETCH_PERSISTENCE_PATH_KEY);
    this.setMultiplier(prefetchedFileListPath == null ? 0 : 1);
    prefetchRatio = 0.0f;
    bestPrefetchRatio = 0.0f;
  }

  @Override
  void prepare(BalancerClusterState cluster) {
    super.prepare(cluster);
    prefetchRatio = 0.0f;
    bestPrefetchRatio = 0.0f;

    for (int region = 0; region < cluster.numRegions; region++) {
      prefetchRatio += cluster.getOrComputeWeightedPrefetchRatio(region,
        cluster.regionIndexToServerIndex[region]);
      bestPrefetchRatio += cluster.getOrComputeWeightedPrefetchRatio(region,
        cluster.getOrComputeServerWithBestPrefetchRatio()[region]);
    }
    prefetchRatio = bestPrefetchRatio == 0.0f ? 1.0f :
      prefetchRatio/bestPrefetchRatio;
  }

  @Override
  protected double cost() {
    return 1 - prefetchRatio;
  }

  @Override
  protected void regionMoved(int region, int oldServer, int newServer) {
    float oldServerPrefetch = getWeightedPrefetchRatio(region, oldServer);
    float newServerPrefetch = getWeightedPrefetchRatio(region, newServer);
    float prefetchDelta = newServerPrefetch - oldServerPrefetch;
    float normalizeDelta = bestPrefetchRatio == 0.0f ? 0.0f :
      prefetchDelta/bestPrefetchRatio;
    prefetchRatio += normalizeDelta;
  }

  @Override
  public final void updateWeight(double[] weights) {
    weights[StochasticLoadBalancer.GeneratorType.PREFETCH.ordinal()] += cost();
  }

  private float getWeightedPrefetchRatio(int region, int server) {
    return cluster.getOrComputeWeightedPrefetchRatio(region, server);
  }
}
