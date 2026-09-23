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
package org.apache.hadoop.hbase.io.hfile.cache;

import java.util.Objects;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.FirstLevelBlockCache;
import org.apache.hadoop.hbase.io.hfile.InclusiveCombinedBlockCache;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory helpers for topology-backed {@link CacheAccessService} instances.
 * <p>
 * These helpers are intended for transitional wiring while existing cache implementations still
 * expose the legacy {@link BlockCache} API. The supplied block caches are adapted to
 * {@link CacheEngine} using {@link BlockCacheBackedCacheEngine}, assembled into a
 * {@link TieredExclusiveTopology}, and exposed through {@link TopologyBackedCacheAccessService}.
 * </p>
 * <p>
 * This class does not create or remove any concrete cache implementation by itself. It only
 * provides a reusable construction path for tests and migration steps that need a
 * CombinedBlockCache-compatible topology-backed service.
 * </p>
 */
@InterfaceAudience.Private
public final class TopologyBackedCacheAccessServices {

  private static final int COMBINED_BLOCK_CACHE_TIER_COUNT = 2;

  private TopologyBackedCacheAccessServices() {
  }

  /**
   * Creates a topology-backed cache access service from an existing combined block cache.
   * <p>
   * The supplied {@link CombinedBlockCache} is used only as a legacy holder for the participating
   * L1 and L2 {@link BlockCache} instances. The returned service uses
   * {@link TieredExclusiveTopology} as the actual orchestration model.
   * </p>
   * @param combinedBlockCache combined block cache containing L1 and L2 caches
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService
    fromCombinedBlockCache(CombinedBlockCache combinedBlockCache) {
    return fromCombinedBlockCache(combinedBlockCache,
      new DefaultHBaseCachePlacementAdmissionPolicy());
  }

  /**
   * Creates a topology-backed cache access service from an existing combined block cache.
   * <p>
   * This overload allows tests and future wiring code to provide an explicit policy while still
   * extracting L1 and L2 caches from the supplied {@link CombinedBlockCache}.
   * </p>
   * @param combinedBlockCache combined block cache containing L1 and L2 caches
   * @param policy             placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromCombinedBlockCache(
    CombinedBlockCache combinedBlockCache, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(combinedBlockCache, "combinedBlockCache must not be null");
    Objects.requireNonNull(policy, "policy must not be null");

    BlockCache[] blockCaches = combinedBlockCache.getBlockCaches();
    if (blockCaches.length != COMBINED_BLOCK_CACHE_TIER_COUNT) {
      throw new IllegalArgumentException("combinedBlockCache must expose exactly two block caches");
    }

    return fromTieredExclusiveBlockCaches("combined", blockCaches[0], blockCaches[1], policy);
  }

  /**
   * Creates a topology-backed cache access service by adapting two legacy block caches as an
   * exclusive tiered topology.
   * @param name   topology name
   * @param l1     first-level legacy block cache
   * @param l2     second-level legacy block cache
   * @param policy cache placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromTieredExclusiveBlockCaches(String name,
    BlockCache l1, BlockCache l2, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(l1, "l1 must not be null");
    Objects.requireNonNull(l2, "l2 must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    if (l1 instanceof FirstLevelBlockCache) {
      ((FirstLevelBlockCache) l1).unsetVictimCache();
    }
    return fromTieredExclusiveCacheEngines(name, CacheEngines.fromBlockCache(l1),
      CacheEngines.fromBlockCache(l2), policy);
  }

  /**
   * Creates a topology-backed cache access service for an {@link InclusiveCombinedBlockCache}.
   * <p>
   * The inclusive combined cache must expose exactly two non-null legacy block caches. The first
   * cache is adapted as L1 using a non-victim-delegating engine, and the second cache is adapted as
   * L2. This prevents L1 misses from internally consulting L2 through the legacy victim-cache
   * mechanism and lets the topology-backed service control tier lookup and promotion policy.
   * </p>
   * @param combinedBlockCache inclusive combined block cache to adapt
   * @return topology-backed cache access service using a tiered inclusive topology
   * @throws NullPointerException     if {@code combinedBlockCache} is {@code null}
   * @throws IllegalArgumentException if the combined cache does not expose exactly two non-null
   *                                  block caches
   */
  public static TopologyBackedCacheAccessService
    fromInclusiveCombinedBlockCache(InclusiveCombinedBlockCache combinedBlockCache) {
    Objects.requireNonNull(combinedBlockCache, "combinedBlockCache must not be null");

    BlockCache[] blockCaches = combinedBlockCache.getBlockCaches();
    if (blockCaches == null || blockCaches.length != 2) {
      throw new IllegalArgumentException(
        "InclusiveCombinedBlockCache must expose exactly two block caches");
    }
    if (blockCaches[0] == null || blockCaches[1] == null) {
      throw new IllegalArgumentException(
        "InclusiveCombinedBlockCache must expose non-null L1 and L2 block caches");
    }

    return fromTieredInclusiveBlockCaches("inclusive-combined", blockCaches[0], blockCaches[1],
      DefaultHBaseCachePlacementAdmissionPolicy.INSTANCE);
  }

  /**
   * Creates a topology-backed cache access service by adapting two legacy block caches as an
   * inclusive tiered topology.
   * @param name   topology name
   * @param l1     first-level legacy block cache
   * @param l2     second-level legacy block cache
   * @param policy cache placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromTieredInclusiveBlockCaches(String name,
    BlockCache l1, BlockCache l2, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(l1, "l1 must not be null");
    Objects.requireNonNull(l2, "l2 must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    if (l1 instanceof FirstLevelBlockCache) {
      ((FirstLevelBlockCache) l1).unsetVictimCache();
    }

    return fromTieredInclusiveCacheEngines(name, CacheEngines.fromBlockCache(l1),
      CacheEngines.fromBlockCache(l2), policy);
  }

  /**
   * Creates a topology-backed cache access service by adapting a legacy block cache as a single
   * cache engine.
   * @param name       topology name
   * @param blockCache legacy block cache
   * @param policy     cache placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromSingleBlockCache(String name,
    BlockCache blockCache, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(blockCache, "blockCache must not be null");

    return fromSingleCacheEngine(name, CacheEngines.fromBlockCache(blockCache), policy);
  }

  /**
   * Returns the legacy {@link BlockCache} wrapped by the cache engine for the requested tier.
   * <p>
   * This helper is intended for tests that need to verify compatibility with legacy block cache
   * implementations during the migration to topology-backed cache access. Production code should
   * prefer {@link CacheAccessService} capability methods instead of unwrapping the underlying
   * {@link BlockCache}.
   * </p>
   * <p>
   * The supplied service must be a {@link TopologyBackedCacheAccessService}. The requested tier
   * must resolve to a {@link BlockCacheBackedCacheEngine}. If either condition is not true, this
   * method fails fast with an {@link IllegalArgumentException}.
   * </p>
   * @param cacheAccessService cache access service to inspect
   * @param tier               cache tier to unwrap
   * @return legacy block cache wrapped by the cache engine for the requested tier
   * @throws NullPointerException     if {@code cacheAccessService} or {@code tier} is {@code null}
   * @throws IllegalArgumentException if the service is not topology-backed, if the requested tier
   *                                  is not present, or if the tier is not backed by a
   *                                  {@link BlockCacheBackedCacheEngine}
   */
  public static BlockCache getBlockCache(CacheAccessService cacheAccessService, CacheTier tier) {
    Objects.requireNonNull(cacheAccessService, "cacheAccessService must not be null");
    Objects.requireNonNull(tier, "tier must not be null");

    if (!(cacheAccessService instanceof TopologyBackedCacheAccessService)) {
      throw new IllegalArgumentException(
        "cacheAccessService must be a TopologyBackedCacheAccessService");
    }

    TopologyBackedCacheAccessService topologyBackedService =
      (TopologyBackedCacheAccessService) cacheAccessService;
    CacheTopology topology = topologyBackedService.getTopology();

    CacheEngine engine = topology.getEngine(tier)
      .orElseThrow(() -> new IllegalArgumentException("No cache engine found for tier " + tier));

    if (!(engine instanceof BlockCacheBackedCacheEngine)) {
      throw new IllegalArgumentException(
        "Cache engine for tier " + tier + " must be a BlockCacheBackedCacheEngine");
    }

    return ((BlockCacheBackedCacheEngine) engine).getBlockCache();
  }

  /**
   * Returns the legacy {@link BlockCache} wrapped by a single-tier topology-backed cache access
   * service.
   * <p>
   * Single-tier topology exposes its only engine through {@link CacheTier#SINGLE}. The only active
   * engine is not assumed to be L1 or L2 because a single-tier configuration may be backed by
   * different concrete cache implementations, including bucket cache.
   * </p>
   * @param cacheAccessService cache access service to inspect
   * @return legacy block cache wrapped by the single-tier cache engine
   * @throws NullPointerException     if {@code cacheAccessService} is {@code null}
   * @throws IllegalArgumentException if the supplied service is not a topology-backed single-tier
   *                                  cache service or if the single tier is not backed by a
   *                                  {@link BlockCacheBackedCacheEngine}
   */
  public static BlockCache getBlockCache(CacheAccessService cacheAccessService) {
    Objects.requireNonNull(cacheAccessService, "cacheAccessService must not be null");

    if (!(cacheAccessService instanceof TopologyBackedCacheAccessService)) {
      throw new IllegalArgumentException(
        "cacheAccessService must be a TopologyBackedCacheAccessService");
    }

    TopologyBackedCacheAccessService topologyBackedService =
      (TopologyBackedCacheAccessService) cacheAccessService;
    CacheTopology topology = topologyBackedService.getTopology();

    if (topology.getType() != CacheTopologyType.SINGLE_TIER) {
      throw new IllegalArgumentException(
        "cacheAccessService must be backed by a single-tier topology");
    }

    return getBlockCache(cacheAccessService, CacheTier.SINGLE);
  }

  /**
   * Creates a topology-backed cache access service using a single cache engine.
   * @param name   topology name
   * @param engine cache engine
   * @param policy cache placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromSingleCacheEngine(String name,
    CacheEngine engine, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(engine, "engine must not be null");
    Objects.requireNonNull(policy, "policy must not be null");

    CacheTopology topology = new SingleTierTopology(name, engine);
    return new TopologyBackedCacheAccessService(topology, policy);
  }

  /**
   * Creates a topology-backed cache access service using two independent cache engines in an
   * exclusive tiered topology.
   * @param name   topology name
   * @param l1     first-level cache engine
   * @param l2     second-level cache engine
   * @param policy cache placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromTieredExclusiveCacheEngines(String name,
    CacheEngine l1, CacheEngine l2, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(l1, "l1 must not be null");
    Objects.requireNonNull(l2, "l2 must not be null");
    Objects.requireNonNull(policy, "policy must not be null");

    CacheTopology topology = new TieredExclusiveTopology(name, l1, l2);
    return new TopologyBackedCacheAccessService(topology, policy);
  }

  /**
   * Creates a topology-backed cache access service using two independent cache engines in an
   * inclusive tiered topology.
   * @param name   topology name
   * @param l1     first-level cache engine
   * @param l2     second-level cache engine
   * @param policy cache placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromTieredInclusiveCacheEngines(String name,
    CacheEngine l1, CacheEngine l2, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(l1, "l1 must not be null");
    Objects.requireNonNull(l2, "l2 must not be null");
    Objects.requireNonNull(policy, "policy must not be null");

    CacheTopology topology = new TieredInclusiveTopology(name, l1, l2);
    return new TopologyBackedCacheAccessService(topology, policy);
  }

}
