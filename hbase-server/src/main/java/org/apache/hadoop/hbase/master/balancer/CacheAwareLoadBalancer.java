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

/** An implementation of the {@link org.apache.hadoop.hbase.master.LoadBalancer} that assigns regions
 * based on the amount they are cached on a given server. A region can move across the region
 * servers whenever a region server shuts down or crashes. The region server preserves the cache
 * periodically and restores the cache when it is restarted. This balancer implements a mechanism
 * where it maintains the amount by which a region is cached on a region server. During balancer
 * run, a region plan is generated that takes into account this cache information and tries to
 * move the regions so that the cache minimally impacted.
 */

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY;

import java.text.DecimalFormat;
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
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class CacheAwareLoadBalancer extends StochasticLoadBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(CacheAwareLoadBalancer.class);

  public static final String CACHE_RATIO_THRESHOLD =
    "hbase.master.balancer.stochastic.throttling.cacheRatio";
  public static final float CACHE_RATIO_THRESHOLD_DEFAULT = 0.8f;

  public Float ratioThreshold;

  private Long sleepTime;
  private Configuration configuration;

  public enum GeneratorFunctionType {
    LOAD,
    CACHE_RATIO
  }

  @Override
  public synchronized void loadConf(Configuration configuration) {
    this.configuration = configuration;
    this.costFunctions = new ArrayList<>();
    super.loadConf(configuration);
    ratioThreshold =
      this.configuration.getFloat(CACHE_RATIO_THRESHOLD, CACHE_RATIO_THRESHOLD_DEFAULT);
    sleepTime = configuration.getLong(MOVE_THROTTLING, MOVE_THROTTLING_DEFAULT.toMillis());
  }

  @Override
  protected Map<Class<? extends CandidateGenerator>, CandidateGenerator>
    createCandidateGenerators() {
    Map<Class<? extends CandidateGenerator>, CandidateGenerator> candidateGenerators =
      new HashMap<>(2);
    candidateGenerators.put(CacheAwareSkewnessCandidateGenerator.class,
      new CacheAwareSkewnessCandidateGenerator());
    candidateGenerators.put(CacheAwareCandidateGenerator.class, new CacheAwareCandidateGenerator());
    return candidateGenerators;
  }

  @Override
  protected List<CostFunction> createCostFunctions(Configuration configuration) {
    List<CostFunction> costFunctions = new ArrayList<>();
    addCostFunction(costFunctions, new CacheAwareRegionSkewnessCostFunction(configuration));
    addCostFunction(costFunctions, new CacheAwareCostFunction(configuration));
    return costFunctions;
  }

  private void addCostFunction(List<CostFunction> costFunctions, CostFunction costFunction) {
    if (costFunction.getMultiplier() > 0) {
      costFunctions.add(costFunction);
    }
  }

  @Override
  public void updateClusterMetrics(ClusterMetrics clusterMetrics) {
    this.clusterStatus = clusterMetrics;
    updateRegionLoad();
  }

  /**
   * Collect the amount of region cached for all the regions from all the active region servers.
   */
  private void updateRegionLoad() {
    loads = new HashMap<>();
    regionCacheRatioOnOldServerMap = new HashMap<>();
    Map<String, Pair<ServerName, Integer>> regionCacheRatioOnCurrentServerMap = new HashMap<>();

    // Build current region cache statistics
    clusterStatus.getLiveServerMetrics().forEach((ServerName sn, ServerMetrics sm) -> {
      // Create a map of region and the server where it is currently hosted
      sm.getRegionMetrics().forEach((byte[] regionName, RegionMetrics rm) -> {
        String regionEncodedName = RegionInfo.encodeRegionName(regionName);

        Deque<BalancerRegionLoad> rload = new ArrayDeque<>();

        // Get the total size of the hFiles in this region
        int regionSizeMB = (int) rm.getRegionSizeMB().get(Size.Unit.MEGABYTE);

        rload.add(new BalancerRegionLoad(rm));
        // Maintain a map of region and it's total size. This is needed to calculate the cache
        // ratios for the regions cached on old region servers
        regionCacheRatioOnCurrentServerMap.put(regionEncodedName, new Pair<>(sn, regionSizeMB));
        loads.put(regionEncodedName, rload);
      });
    });

    // Build cache statistics for the regions hosted previously on old region servers
    clusterStatus.getLiveServerMetrics().forEach((ServerName sn, ServerMetrics sm) -> {
      // Find if a region was previously hosted on a server other than the one it is currently
      // hosted on.
      sm.getRegionCachedInfo().forEach((String regionEncodedName, Integer regionSizeInCache) -> {
        // If the region is found in regionCacheRatioOnCurrentServerMap, it is currently hosted on
        // this server
        if (regionCacheRatioOnCurrentServerMap.containsKey(regionEncodedName)) {
          ServerName currentServer =
            regionCacheRatioOnCurrentServerMap.get(regionEncodedName).getFirst();
          if (!ServerName.isSameAddress(currentServer, sn)) {
            int regionSizeMB =
              regionCacheRatioOnCurrentServerMap.get(regionEncodedName).getSecond();
            float regionCacheRatioOnOldServer =
              regionSizeMB == 0 ? 0.0f : (float) regionSizeInCache / regionSizeMB;
            regionCacheRatioOnOldServerMap.put(regionEncodedName,
              new Pair<>(sn, regionCacheRatioOnOldServer));
          }
        }
      });
    });
  }

  private RegionInfo getRegionInfoByEncodedName(BalancerClusterState cluster, String regionName) {
    Optional<RegionInfo> regionInfoOptional =
      Arrays.stream(cluster.regions).filter((RegionInfo ri) -> {
        return regionName.equals(ri.getEncodedName());
      }).findFirst();

    if (regionInfoOptional.isPresent()) {
      return regionInfoOptional.get();
    }
    return null;
  }

  @Override
  public void throttle(RegionPlan plan) {
    Pair<ServerName, Float> rsRatio = this.regionCacheRatioOnOldServerMap.get(plan.getRegionName());
    if (
      rsRatio != null && plan.getDestination().equals(rsRatio.getFirst())
        && rsRatio.getSecond() >= ratioThreshold
    ) {
      LOG.debug("Moving region {} to server {} with cache ratio {}. No throttling needed.",
        plan.getRegionInfo().getEncodedName(), plan.getDestination(), rsRatio.getSecond());
    } else {
      if (rsRatio != null) {
        LOG.debug("Moving region {} to server {} with cache ratio: {}. Throttling move for {}ms.",
          plan.getRegionInfo().getEncodedName(), plan.getDestination(),
          plan.getDestination().equals(rsRatio.getFirst()) ? rsRatio.getSecond() : "unknown",
          sleepTime);
      } else {
        LOG.debug(
          "Moving region {} to server {} with no cache ratio info for the region. "
            + "Throttling move for {}ms.",
          plan.getRegionInfo().getEncodedName(), plan.getDestination(), sleepTime);
      }
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected List<RegionPlan> balanceTable(TableName tableName,
    Map<ServerName, List<RegionInfo>> loadOfOneTable) {
    final Map<String, Pair<ServerName, Float>> snapshot = new HashMap<>();
    snapshot.putAll(this.regionCacheRatioOnOldServerMap);
    List<RegionPlan> plans = super.balanceTable(tableName, loadOfOneTable);
    if (plans == null) {
      return plans;
    }
    plans.sort((p1, p2) -> {
      Pair<ServerName, Float> pair1 = snapshot.get(p1.getRegionName());
      Float ratio1 =
        pair1 == null ? 0f : pair1.getFirst().equals(p1.getDestination()) ? pair1.getSecond() : 0f;
      Pair<ServerName, Float> pair2 = snapshot.get(p2.getRegionName());
      Float ratio2 =
        pair2 == null ? 0f : pair2.getFirst().equals(p2.getDestination()) ? pair2.getSecond() : 0f;
      return ratio1.compareTo(ratio2) * (-1);
    });
    return plans;
  }

  private class CacheAwareCandidateGenerator extends CandidateGenerator {
    @Override
    protected BalanceAction generate(BalancerClusterState cluster) {
      // Move the regions to the servers they were previously hosted on based on the cache ratio
      if (
        !regionCacheRatioOnOldServerMap.isEmpty()
          && regionCacheRatioOnOldServerMap.entrySet().iterator().hasNext()
      ) {
        Map.Entry<String, Pair<ServerName, Float>> regionCacheRatioServerMap =
          regionCacheRatioOnOldServerMap.entrySet().iterator().next();
        // Get the server where this region was previously hosted
        String regionEncodedName = regionCacheRatioServerMap.getKey();
        RegionInfo regionInfo = getRegionInfoByEncodedName(cluster, regionEncodedName);
        if (regionInfo == null) {
          LOG.warn("Region {} not found", regionEncodedName);
          regionCacheRatioOnOldServerMap.remove(regionEncodedName);
          return BalanceAction.NULL_ACTION;
        }
        if (regionInfo.isMetaRegion() || regionInfo.getTable().isSystemTable()) {
          regionCacheRatioOnOldServerMap.remove(regionEncodedName);
          return BalanceAction.NULL_ACTION;
        }
        int regionIndex = cluster.regionsToIndex.get(regionInfo);
        int oldServerIndex = cluster.serversToIndex
          .get(regionCacheRatioOnOldServerMap.get(regionEncodedName).getFirst().getAddress());
        if (oldServerIndex < 0) {
          LOG.warn("Server previously hosting region {} not found", regionEncodedName);
          regionCacheRatioOnOldServerMap.remove(regionEncodedName);
          return BalanceAction.NULL_ACTION;
        }

        float oldRegionCacheRatio =
          cluster.getOrComputeRegionCacheRatio(regionIndex, oldServerIndex);
        int currentServerIndex = cluster.regionIndexToServerIndex[regionIndex];
        float currentRegionCacheRatio =
          cluster.getOrComputeRegionCacheRatio(regionIndex, currentServerIndex);

        BalanceAction action = generatePlan(cluster, regionIndex, currentServerIndex,
          currentRegionCacheRatio, oldServerIndex, oldRegionCacheRatio);
        regionCacheRatioOnOldServerMap.remove(regionEncodedName);
        return action;
      }
      return BalanceAction.NULL_ACTION;
    }

    private BalanceAction generatePlan(BalancerClusterState cluster, int regionIndex,
      int currentServerIndex, float cacheRatioOnCurrentServer, int oldServerIndex,
      float cacheRatioOnOldServer) {
      return moveRegionToOldServer(cluster, regionIndex, currentServerIndex,
        cacheRatioOnCurrentServer, oldServerIndex, cacheRatioOnOldServer)
          ? getAction(currentServerIndex, regionIndex, oldServerIndex, -1)
          : BalanceAction.NULL_ACTION;
    }

    private boolean moveRegionToOldServer(BalancerClusterState cluster, int regionIndex,
      int currentServerIndex, float cacheRatioOnCurrentServer, int oldServerIndex,
      float cacheRatioOnOldServer) {
      // Find if the region has already moved by comparing the current server index with the
      // current server index. This can happen when other candidate generator has moved the region
      if (currentServerIndex < 0 || oldServerIndex < 0) {
        return false;
      }

      DecimalFormat df = new DecimalFormat("#");
      df.setMaximumFractionDigits(4);

      float cacheRatioDiffThreshold = 0.6f;

      // Conditions for moving the region

      // If the region is fully cached on the old server, move the region back
      if (cacheRatioOnOldServer == 1.0f) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Region {} moved to the old server {} as it is fully cached there",
            cluster.regions[regionIndex].getEncodedName(), cluster.servers[oldServerIndex]);
        }
        return true;
      }

      // Move the region back to the old server if it is cached equally on both the servers
      if (cacheRatioOnCurrentServer == cacheRatioOnOldServer) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Region {} moved from {} to {} as the region is cached {} equally on both servers",
            cluster.regions[regionIndex].getEncodedName(), cluster.servers[currentServerIndex],
            cluster.servers[oldServerIndex], df.format(cacheRatioOnCurrentServer));
        }
        return true;
      }

      // If the region is not fully cached on either of the servers, move the region back to the
      // old server if the region cache ratio on the current server is still much less than the old
      // server
      if (
        cacheRatioOnOldServer > 0.0f
          && cacheRatioOnCurrentServer / cacheRatioOnOldServer < cacheRatioDiffThreshold
      ) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Region {} moved from {} to {} as region cache ratio {} is better than the current "
              + "cache ratio {}",
            cluster.regions[regionIndex].getEncodedName(), cluster.servers[currentServerIndex],
            cluster.servers[oldServerIndex], cacheRatioOnCurrentServer,
            df.format(cacheRatioOnCurrentServer));
        }
        return true;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Region {} not moved from {} to {} with current cache ratio {} and old cache ratio {}",
          cluster.regions[regionIndex], cluster.servers[currentServerIndex],
          cluster.servers[oldServerIndex], cacheRatioOnCurrentServer,
          df.format(cacheRatioOnCurrentServer));
      }
      return false;
    }
  }

  private class CacheAwareSkewnessCandidateGenerator extends LoadCandidateGenerator {
    @Override
    BalanceAction pickRandomRegions(BalancerClusterState cluster, int thisServer, int otherServer) {
      // First move all the regions which were hosted previously on some other server back to their
      // old servers
      if (
        !regionCacheRatioOnOldServerMap.isEmpty()
          && regionCacheRatioOnOldServerMap.entrySet().iterator().hasNext()
      ) {
        // Get the first region index in the historical cache ratio list
        Map.Entry<String, Pair<ServerName, Float>> regionEntry =
          regionCacheRatioOnOldServerMap.entrySet().iterator().next();
        String regionEncodedName = regionEntry.getKey();

        RegionInfo regionInfo = getRegionInfoByEncodedName(cluster, regionEncodedName);
        if (regionInfo == null) {
          LOG.warn("Region {} does not exist", regionEncodedName);
          regionCacheRatioOnOldServerMap.remove(regionEncodedName);
          return BalanceAction.NULL_ACTION;
        }
        if (regionInfo.isMetaRegion() || regionInfo.getTable().isSystemTable()) {
          regionCacheRatioOnOldServerMap.remove(regionEncodedName);
          return BalanceAction.NULL_ACTION;
        }

        int regionIndex = cluster.regionsToIndex.get(regionInfo);

        // Get the current host name for this region
        thisServer = cluster.regionIndexToServerIndex[regionIndex];

        // Get the old server index
        otherServer = cluster.serversToIndex.get(regionEntry.getValue().getFirst().getAddress());

        regionCacheRatioOnOldServerMap.remove(regionEncodedName);

        if (otherServer < 0) {
          // The old server has been moved to other host and hence, the region cannot be moved back
          // to the old server
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "CacheAwareSkewnessCandidateGenerator: Region {} not moved to the old "
                + "server {} as the server does not exist",
              regionEncodedName, regionEntry.getValue().getFirst().getHostname());
          }
          return BalanceAction.NULL_ACTION;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "CacheAwareSkewnessCandidateGenerator: Region {} moved from {} to {} as it "
              + "was hosted their earlier",
            regionEncodedName, cluster.servers[thisServer].getHostname(),
            cluster.servers[otherServer].getHostname());
        }

        return getAction(thisServer, regionIndex, otherServer, -1);
      }

      if (thisServer < 0 || otherServer < 0) {
        return BalanceAction.NULL_ACTION;
      }

      int regionIndexToMove = pickLeastCachedRegion(cluster, thisServer);
      if (regionIndexToMove < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("CacheAwareSkewnessCandidateGenerator: No region found for movement");
        }
        return BalanceAction.NULL_ACTION;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "CacheAwareSkewnessCandidateGenerator: Region {} moved from {} to {} as it is "
            + "least cached on current server",
          cluster.regions[regionIndexToMove].getEncodedName(),
          cluster.servers[thisServer].getHostname(), cluster.servers[otherServer].getHostname());
      }
      return getAction(thisServer, regionIndexToMove, otherServer, -1);
    }

    private int pickLeastCachedRegion(BalancerClusterState cluster, int thisServer) {
      float minCacheRatio = Float.MAX_VALUE;
      int leastCachedRegion = -1;
      for (int i = 0; i < cluster.regionsPerServer[thisServer].length; i++) {
        int regionIndex = cluster.regionsPerServer[thisServer][i];

        float cacheRatioOnCurrentServer =
          cluster.getOrComputeRegionCacheRatio(regionIndex, thisServer);
        if (cacheRatioOnCurrentServer < minCacheRatio) {
          minCacheRatio = cacheRatioOnCurrentServer;
          leastCachedRegion = regionIndex;
        }
      }
      return leastCachedRegion;
    }
  }

  static class CacheAwareRegionSkewnessCostFunction extends CostFunction {
    static final String REGION_COUNT_SKEW_COST_KEY =
      "hbase.master.balancer.stochastic.regionCountCost";
    static final float DEFAULT_REGION_COUNT_SKEW_COST = 20;
    private final DoubleArrayCost cost = new DoubleArrayCost();

    CacheAwareRegionSkewnessCostFunction(Configuration conf) {
      // Load multiplier should be the greatest as it is the most general way to balance data.
      this.setMultiplier(conf.getFloat(REGION_COUNT_SKEW_COST_KEY, DEFAULT_REGION_COUNT_SKEW_COST));
    }

    @Override
    void prepare(BalancerClusterState cluster) {
      super.prepare(cluster);
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

    @Override
    public final void updateWeight(Map<Class<? extends CandidateGenerator>, Double> weights) {
      weights.merge(LoadCandidateGenerator.class, cost(), Double::sum);
    }
  }

  static class CacheAwareCostFunction extends CostFunction {
    private static final String CACHE_COST_KEY = "hbase.master.balancer.stochastic.cacheCost";
    private double cacheRatio;
    private double bestCacheRatio;

    private static final float DEFAULT_CACHE_COST = 20;

    CacheAwareCostFunction(Configuration conf) {
      boolean isPersistentCache = conf.get(BUCKET_CACHE_PERSISTENT_PATH_KEY) != null;
      // Disable the CacheAwareCostFunction if the cached file list persistence is not enabled
      this.setMultiplier(
        !isPersistentCache ? 0.0f : conf.getFloat(CACHE_COST_KEY, DEFAULT_CACHE_COST));
      bestCacheRatio = 0.0;
      cacheRatio = 0.0;
    }

    @Override
    void prepare(BalancerClusterState cluster) {
      super.prepare(cluster);
      cacheRatio = 0.0;
      bestCacheRatio = 0.0;

      for (int region = 0; region < cluster.numRegions; region++) {
        cacheRatio += cluster.getOrComputeWeightedRegionCacheRatio(region,
          cluster.regionIndexToServerIndex[region]);
        bestCacheRatio += cluster.getOrComputeWeightedRegionCacheRatio(region,
          getServerWithBestCacheRatioForRegion(region));
      }

      cacheRatio = bestCacheRatio == 0 ? 1.0 : cacheRatio / bestCacheRatio;
      if (LOG.isDebugEnabled()) {
        LOG.debug("CacheAwareCostFunction: Cost: {}", 1 - cacheRatio);
      }
    }

    @Override
    protected double cost() {
      return scale(0, 1, 1 - cacheRatio);
    }

    @Override
    protected void regionMoved(int region, int oldServer, int newServer) {
      double regionCacheRatioOnOldServer =
        cluster.getOrComputeWeightedRegionCacheRatio(region, oldServer);
      double regionCacheRatioOnNewServer =
        cluster.getOrComputeWeightedRegionCacheRatio(region, newServer);
      double cacheRatioDiff = regionCacheRatioOnNewServer - regionCacheRatioOnOldServer;
      double normalizedDelta = bestCacheRatio == 0.0 ? 0.0 : cacheRatioDiff / bestCacheRatio;
      cacheRatio += normalizedDelta;
      if (LOG.isDebugEnabled() && (cacheRatio < 0.0 || cacheRatio > 1.0)) {
        LOG.debug(
          "CacheAwareCostFunction:regionMoved:region:{}:from:{}:to:{}:regionCacheRatioOnOldServer:{}:"
            + "regionCacheRatioOnNewServer:{}:bestRegionCacheRatio:{}:cacheRatio:{}",
          cluster.regions[region].getEncodedName(), cluster.servers[oldServer].getHostname(),
          cluster.servers[newServer].getHostname(), regionCacheRatioOnOldServer,
          regionCacheRatioOnNewServer, bestCacheRatio, cacheRatio);
      }
    }

    private int getServerWithBestCacheRatioForRegion(int region) {
      return cluster.getOrComputeServerWithBestRegionCachedRatio()[region];
    }

    @Override
    public void updateWeight(Map<Class<? extends CandidateGenerator>, Double> weights) {
      weights.merge(LoadCandidateGenerator.class, cost(), Double::sum);
    }
  }
}
