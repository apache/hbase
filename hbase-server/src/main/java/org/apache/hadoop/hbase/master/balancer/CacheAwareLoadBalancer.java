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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.io.hfile.BlockCacheFactory.BUCKET_CACHE_PERSISTENT_PATH_KEY;

/**
 * An implementation of the {@link org.apache.hadoop.hbase.master.LoadBalancer} that assigns regions
 * based on the amount they are prefetched on a given server. A region can move across the region
 * servers whenever a region server shuts down or crashes. The region server preserves the cache
 * before shutting down and restores the cache when it is restarted. This balancer implements a
 * mechanism where it maintains the amount by which a region is prefetched on a region server.
 * During balancer run, a region plan is generated that takes into account this prefetch information
 * and tries to move the regions so that the cache is minimally affected.
 */

@InterfaceAudience.Private
public class CacheAwareLoadBalancer extends StochasticLoadBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(CacheAwareLoadBalancer.class);

  private Configuration conf;

  public enum GeneratorFunctionType {
    LOAD,
    PREFETCH
  }

  @Override
  public void initialize() throws HBaseIOException {
    configureGenerators();
    super.initialize();
    super.setConf(conf);
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
    this.costFunctions = new ArrayList<>();
    addCostFunction(new PrefetchAwareRegionSkewnessCostFunction(conf));
    addCostFunction(new PrefetchCacheCostFunction(conf));
    configureGenerators();
  }

  private void addCostFunction(CostFunction function) {
    if (function.getMultiplier() > 0) {
      costFunctions.add(function);
    }
  }

  protected void configureGenerators() {
    List<CandidateGenerator> candidateGenerators = new ArrayList<>();
    candidateGenerators.add(GeneratorFunctionType.LOAD.ordinal(), new PrefetchBasedSkewnessCandidateGenerator());
    candidateGenerators.add(GeneratorFunctionType.PREFETCH.ordinal(), new PrefetchCandidateGenerator());
    setCandidateGenerators(candidateGenerators);
  }

  @Override
  public synchronized void setMasterServices(MasterServices masterServices) {
    super.setMasterServices(masterServices);
  }

  @Override
  public synchronized void setClusterMetrics(ClusterMetrics clusterMetrics) {
    this.clusterStatus = clusterMetrics;
    updateRegionLoad();
  }

  /**
   * Collect the prefetch information for all the regions from all the active region servers.
   */
  private synchronized void updateRegionLoad() {
    loads = new HashMap<>();
    oldRegionPrefetchMap = new HashMap<>();
    Map<String, Pair<ServerName, Integer>> currentPrefetchMap = new HashMap<>();

    // Build current region prefetch statistics
    clusterStatus.getLiveServerMetrics().forEach((ServerName sn, ServerMetrics sm) -> {
      // Create a map of region and the server where it is currently being hosted
      sm.getRegionMetrics().forEach((byte[] regionName, RegionMetrics rm) -> {
        String regionEncodedName = RegionInfo.encodeRegionName(regionName);
        Deque<BalancerRegionLoad> rload = new ArrayDeque<>();

        // Get the total size of the hFiles in this region
        int regionSizeMB = (int) rm.getRegionSizeMB().get(Size.Unit.MEGABYTE);

        rload.add(new BalancerRegionLoad(rm));

        // Maintain a map of region and it's total size. This is needed to calculate the prefetch
        // ratios for the regions cached on old region servers
        currentPrefetchMap.put(regionEncodedName, new Pair<>(sn, regionSizeMB));
        loads.put(regionEncodedName, rload);
      });
    });

    // Build old region prefetch statistics
    clusterStatus.getLiveServerMetrics().forEach((ServerName sn, ServerMetrics sm) -> {
      // Find if a region was previously hosted on a server other than the one it is currently
      // hosted on.
      sm.getRegionPrefetchInfo().forEach((String regionEncodedName, Integer prefetchSize) -> {
        // If the region is found in currentPrefetchMap, it is currently hosted on this server
        if (currentPrefetchMap.containsKey(regionEncodedName)) {
          ServerName currentServer = currentPrefetchMap.get(regionEncodedName).getFirst();
          if (!ServerName.isSameAddress(currentServer, sn)) {
            int regionSizeMB = currentPrefetchMap.get(regionEncodedName).getSecond();
            float regionPrefetchRatio = regionSizeMB == 0 ? 0.0f : (float) prefetchSize / regionSizeMB;
            oldRegionPrefetchMap.put(regionEncodedName, new Pair<>(sn, regionPrefetchRatio));
          }
        } else {
          LOG.warn("Region {} not currently hosted on any server", regionEncodedName);
        }
      });
    });
  }

  private RegionInfo getRegionInfoByEncodedName(Cluster cluster, String regionEncodedName) {
    Optional<RegionInfo> regionInfoOptional = Arrays.stream(cluster.regions).filter((RegionInfo ri)
      -> {
      return regionEncodedName.equals(ri.getEncodedName());
    }).findFirst();

    if (regionInfoOptional.isPresent()) {
      return regionInfoOptional.get();
    }

    return null;
  }

  private class PrefetchCandidateGenerator extends CandidateGenerator {
    @Override
    protected Cluster.Action generate(Cluster cluster) {
      // Move the regions to the servers they were previously hosted on
      // after moving all the regions to their old hosts, move the regions based on the
      // amount of regions cached on them.

      if (!oldRegionPrefetchMap.isEmpty() && oldRegionPrefetchMap.entrySet().iterator().hasNext()) {
        Map.Entry<String, Pair<ServerName, Float>> regionPrefetchEntry =
          oldRegionPrefetchMap.entrySet().iterator().next();
        // Get the server where this region was previously hosted
        String regionEncodedName = regionPrefetchEntry.getKey();
        RegionInfo regionInfo = getRegionInfoByEncodedName(cluster, regionEncodedName);
        if (regionInfo == null) {
          LOG.warn("Region {} not found", regionEncodedName);
          oldRegionPrefetchMap.remove(regionEncodedName);
          return Cluster.NullAction;
        }
        if (regionInfo.isMetaRegion() || regionInfo.getTable().isSystemTable()) {
          oldRegionPrefetchMap.remove(regionEncodedName);
          return Cluster.NullAction;
        }
        int regionIndex = cluster.regionsToIndex.get(regionInfo);
        int oldServerIndex = cluster.serversToIndex.get(
          oldRegionPrefetchMap.get(regionEncodedName).getFirst().getAddress());
        if (oldServerIndex < 0) {
          LOG.warn("Server previously hosting region {} not found", regionEncodedName);
          oldRegionPrefetchMap.remove(regionEncodedName);
          return Cluster.NullAction;
        }
        float oldPrefetchRatio = cluster.getOrComputeRegionPrefetchRatio(regionIndex, oldServerIndex);

        int currentServerIndex = cluster.regionIndexToServerIndex[regionIndex];
        float currentPrefetchRatio =
          cluster.getOrComputeRegionPrefetchRatio(regionIndex, currentServerIndex);

        Cluster.Action action = generatePlan(cluster, regionIndex, currentServerIndex,
          currentPrefetchRatio, oldServerIndex, oldPrefetchRatio);
        oldRegionPrefetchMap.remove(regionEncodedName);
        return action;
      }
      return Cluster.NullAction;
    }

    private Cluster.Action generatePlan(Cluster cluster, int regionIndex, int currentServerIndex,
      float currentServerPrefetchRatio, int oldServerIndex, float oldServerPrefetchRatio) {
      return moveRegionToOldServer(cluster, regionIndex, currentServerIndex, currentServerPrefetchRatio,
        oldServerIndex, oldServerPrefetchRatio) ?
        getAction(currentServerIndex, regionIndex, oldServerIndex, -1) :
        Cluster.NullAction;
    }

    private boolean moveRegionToOldServer(Cluster cluster, int regionIndex, int currentServerIndex,
      float currentServerPrefetchRatio, int oldServerIndex, float oldServerPrefetchRatio) {

      // Find if the region has already moved by comparing the current serverindex with the
      // currentServerIndex. This can happen when other candidate generator has moved the region
      if (currentServerIndex < 0 || oldServerIndex < 0) { return false; }

      float prefetchDiffThreshold = 0.6f;

      // Conditions for moving the region

      // If the region is fully prefetched on the old server, move the region back
      if (oldServerPrefetchRatio == 1.0f) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("PrefetchCandidateGenerator: Region {} moved to the old server {} as it is "
            + "fully prefetched there", cluster.regions[regionIndex].getEncodedName(),
            cluster.servers[oldServerIndex].getHostname());
        }
        return true;
      }

      if (currentServerPrefetchRatio == oldServerPrefetchRatio) {
        // Move the region if it is fully prefetched on both the servers
        if (LOG.isDebugEnabled()) {
          LOG.debug("PrefetchCandidateGenerator: Region {} moved from {} to {} as the prefetch is same {}",
            cluster.regions[regionIndex].getEncodedName(), cluster.servers[currentServerIndex].getHostname(),
            cluster.servers[oldServerIndex].getHostname(), currentServerPrefetchRatio);
        }
        return true;
      }

      // If the region is not fully prefetched on either the current server or the old server,
      // move the region back to the old server if the current server prefetch is still much less
      // than the old server
      if (oldServerPrefetchRatio > 0.0f &&
        currentServerPrefetchRatio/oldServerPrefetchRatio < prefetchDiffThreshold) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("PrefetchCandidateGenerator: Region {} moved from {} to {} old prefetch {} "
            + "is better than current prefetch {}", cluster.regions[regionIndex].getEncodedName(),
            cluster.servers[currentServerIndex].getHostname(), cluster.servers[oldServerIndex].getHostname(),
            oldServerPrefetchRatio, currentServerPrefetchRatio);
        }
        return true;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("PrefetchCandidateGenerator: Region {} not moved from {} to {} with current "
            + "prefetch {} and old prefetch {}", cluster.regions[regionIndex].getEncodedName(),
          cluster.servers[currentServerIndex].getHostname(), cluster.servers[oldServerIndex].getHostname(),
          currentServerPrefetchRatio, oldServerPrefetchRatio);
      }
      return false;
    }
  }

  private class PrefetchBasedSkewnessCandidateGenerator extends LoadCandidateGenerator {
    @Override
    BaseLoadBalancer.Cluster.Action pickRandomRegions(BaseLoadBalancer.Cluster cluster,
      int thisServer, int otherServer) {

      // First move all the regions which were hosted previously on some other servers back to
      // their historical servers
      if (!oldRegionPrefetchMap.isEmpty() && oldRegionPrefetchMap.entrySet().iterator().hasNext()) {
        // Get the first region index in the historical prefetch list
        Map.Entry<String, Pair<ServerName, Float>> regionEntry =
          oldRegionPrefetchMap.entrySet().iterator().next();
        String regionEncodedName = regionEntry.getKey();

        RegionInfo regionInfo = getRegionInfoByEncodedName(cluster, regionEncodedName);
        if (regionInfo == null) {
          LOG.warn("Region {} does not exist", regionEncodedName);
          oldRegionPrefetchMap.remove(regionEncodedName);
          return Cluster.NullAction;
        }
        if (regionInfo.isMetaRegion() || regionInfo.getTable().isSystemTable()) {
          oldRegionPrefetchMap.remove(regionEncodedName);
          return Cluster.NullAction;
        }

        int regionIndex = cluster.regionsToIndex.get(regionInfo);

        // Get the current host name for this region
        thisServer = cluster.regionIndexToServerIndex[regionIndex];

        // Get the old server index
        otherServer = cluster.serversToIndex.get(regionEntry.getValue().getFirst().getAddress());

        oldRegionPrefetchMap.remove(regionEncodedName);

        if (otherServer < 0) {
          // The old server has been moved to other host and hence, the region cannot be moved back
          // to the old server
          if (LOG.isDebugEnabled()) {
            LOG.debug("PrefetchBasedSkewnessCandidateGenerator: Region {} not moved to the old "
                + "server {} as the server does not exist", regionEncodedName,
              regionEntry.getValue().getFirst().getHostname());
          }
          return Cluster.NullAction;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("PrefetchBasedSkewnessCandidateGenerator: Region {} moved from {} to {} as it "
              + "was hosted their earlier", regionEncodedName,
            cluster.servers[thisServer].getHostname(), cluster.servers[otherServer].getHostname());
        }

        return getAction(thisServer, regionIndex, otherServer, -1);
      }

      if (thisServer < 0 || otherServer < 0) {
        return Cluster.NullAction;
      }

      int regionIndexToMove = pickLeastPrefetchedRegion(cluster, thisServer);
      if (regionIndexToMove < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("PrefetchBasedSkewnessCandidateGenerator: No region found for movement");
        }
        return Cluster.NullAction;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("PrefetchBasedSkewnessCandidateGenerator: Region {} moved from {} to {} as it is "
          + "least prefetched on current server", cluster.regions[regionIndexToMove].getEncodedName(),
          cluster.servers[thisServer].getHostname(), cluster.servers[otherServer].getHostname());
      }
      return getAction(thisServer, regionIndexToMove, otherServer, -1);
    }

    private int pickLeastPrefetchedRegion(Cluster cluster, int thisServer) {
      float minPrefetchRatio = Float.MAX_VALUE;
      int regionWithMinPrefetch = -1;
      for (int i = 0; i < cluster.regionsPerServer[thisServer].length; i++) {
        int regionIndex = cluster.regionsPerServer[thisServer][i];

        float currentPrefetchRatio = cluster.getOrComputeRegionPrefetchRatio(regionIndex, thisServer);
        if (currentPrefetchRatio < minPrefetchRatio) {
          minPrefetchRatio = currentPrefetchRatio;
          regionWithMinPrefetch = regionIndex;
        }
      }
      return regionWithMinPrefetch;
    }
  }

  static class PrefetchAwareRegionSkewnessCostFunction extends CostFunction {
    static final String REGION_COUNT_SKEW_COST_KEY =
      "hbase.master.balancer.stochastic.regionCountCost";
    static final float DEFAULT_REGION_COUNT_SKEW_COST = 20;

    private final DoubleArrayCost cost = new DoubleArrayCost();

    PrefetchAwareRegionSkewnessCostFunction(Configuration conf) {
      super(conf);
      // Load multiplier should be the greatest as it is the most general way to balance data.
      this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      cost.prepare(cluster.numServers);
      cost.applyCostsChange(costs -> {
        for (int i = 0; i < cluster.numServers; i++) {
          costs[i] = cluster.regionsPerServer[i].length;
        }
      });
    }

    @Override
    protected double cost() {
      return cost.cost();
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      cost.applyCostsChange(costs -> {
        costs[oldServer] = cluster.regionsPerServer[oldServer].length;
        costs[newServer] = cluster.regionsPerServer[newServer].length;
      });
    }

    public final void updateWeight(double[] weights) {
      weights[GeneratorFunctionType.LOAD.ordinal()] += cost();
    }
  }

  static class PrefetchCacheCostFunction extends CostFunction {
    private static final String PREFETCH_CACHE_COST_KEY =
      "hbase.master.balancer.stochastic.prefetchCacheCost";
    private double prefetchRatio;
    private double bestPrefetchRatio;

    private static final float DEFAULT_PREFETCH_COST = 20;

    PrefetchCacheCostFunction(Configuration conf) {
      super(conf);
      boolean isPersistentCache = conf.get(BUCKET_CACHE_PERSISTENT_PATH_KEY) != null;
      // Disable the prefetchCacheCostFunction if the prefetch file list persistence is not enabled
      this.setMultiplier( !isPersistentCache ? 0.0f :
        conf.getFloat(PREFETCH_CACHE_COST_KEY, DEFAULT_PREFETCH_COST));
      bestPrefetchRatio = 0.0;
      prefetchRatio = 0.0;
    }

    @Override
    void init(Cluster cluster) {
      super.init(cluster);
      prefetchRatio = 0.0;
      bestPrefetchRatio = 0.0;

      for (int region = 0; region < cluster.numRegions; region++) {
        prefetchRatio += cluster.getOrComputeWeightedPrefetchRatio(region,
          cluster.regionIndexToServerIndex[region]);
        bestPrefetchRatio += cluster.getOrComputeWeightedPrefetchRatio(region,
          getServerWithBestPrefetchRatioForRegion(region));
      }

      prefetchRatio = bestPrefetchRatio == 0 ? 1.0 : prefetchRatio/bestPrefetchRatio;
      if (LOG.isDebugEnabled() /*&& (prefetchRatio < 0.0 || prefetchRatio > 1.0)*/) {
        LOG.debug("PrefetchCacheCostFunction: Cost: {}", 1 - prefetchRatio);
      }
    }

    @Override
    protected double cost() {
      return scale(0, 1, 1 - prefetchRatio);
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      double oldServerPrefetch = cluster.getOrComputeWeightedPrefetchRatio(region, oldServer);
      double newServerPrefetch = cluster.getOrComputeWeightedPrefetchRatio(region, newServer);
      double prefetchDelta = newServerPrefetch - oldServerPrefetch;
      double normalizedDelta = bestPrefetchRatio == 0.0 ? 0.0 : prefetchDelta / bestPrefetchRatio;
      prefetchRatio += normalizedDelta;
      if (LOG.isDebugEnabled() && (prefetchRatio < 0.0 || prefetchRatio > 1.0)) {
        LOG.debug(
          "PrefetchCacheCostFunction:regionMoved:region:{}:from:{}:to:{}:oldServerPrefetch:{}:"
            + "newServerPrefetch:{}:bestPrefetchRatio:{}:prefetchRatio:{}",
          cluster.regions[region].getEncodedName(), cluster.servers[oldServer].getHostname(),
          cluster.servers[newServer].getHostname(), oldServerPrefetch, newServerPrefetch, bestPrefetchRatio,
          prefetchRatio);
      }
    }

    private int getServerWithBestPrefetchRatioForRegion(int region) {
      return cluster.getOrComputeServerWithBestPrefetchRatio()[region];
    }

    @Override
    public final void updateWeight(double[] weights) {
      weights[GeneratorFunctionType.PREFETCH.ordinal()] += cost();
    }
  }
}
