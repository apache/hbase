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
   * Creates a topology-backed cache access service from existing L1 and L2 block caches.
   * <p>
   * The resulting service uses {@link TieredExclusiveTopology}, which models the current
   * CombinedBlockCache-compatible L1/L2 behavior where promotion can move a block from one tier to
   * another.
   * </p>
   * @param name   human-readable topology/service name
   * @param l1     L1 block cache
   * @param l2     L2 block cache
   * @param policy placement and admission policy
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
    CacheEngine l1Engine = CacheEngines.fromBlockCache(l1);// fromL1BlockCache(l1);
    CacheEngine l2Engine = CacheEngines.fromBlockCache(l2);
    CacheTopology topology = new TieredExclusiveTopology(name, l1Engine, l2Engine);
    return new TopologyBackedCacheAccessService(topology, policy);
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
   * Creates a topology-backed cache access service from two legacy block caches using an inclusive
   * tiered topology.
   * <p>
   * The first supplied block cache is treated as the L1 tier and the second supplied block cache is
   * treated as the L2 tier. Both legacy caches are adapted to {@link CacheEngine} instances using
   * {@link CacheEngines#fromBlockCache(BlockCache)} and then assembled into a
   * {@link TieredInclusiveTopology}.
   * </p>
   * <p>
   * This helper is intended for compatibility with legacy inclusive combined-cache configurations
   * while moving cache access and diagnostics to the {@link CacheAccessService} abstraction.
   * Inclusive topology semantics differ from exclusive topology semantics: a block may exist in
   * both tiers, and eviction from one tier does not necessarily imply eviction from the other tier.
   * </p>
   * @param name   topology name used for diagnostics
   * @param l1     first-level block cache
   * @param l2     second-level block cache
   * @param policy cache placement and admission policy to use with the topology-backed service
   * @return topology-backed cache access service backed by a tiered inclusive topology
   * @throws NullPointerException if {@code name}, {@code l1}, {@code l2}, or {@code policy} is
   *                              {@code null}
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
    CacheEngine l1Engine = CacheEngines.fromBlockCache(l1);
    CacheEngine l2Engine = CacheEngines.fromBlockCache(l2);
    CacheTopology topology = new TieredInclusiveTopology(name, l1Engine, l2Engine);

    return new TopologyBackedCacheAccessService(topology, policy);
  }

  /**
   * Creates a topology-backed cache access service for a single legacy {@link BlockCache}.
   * <p>
   * The supplied block cache is adapted to a {@link CacheEngine} and placed behind a
   * {@link SingleTierTopology}. This makes single-tier caches use the same
   * {@link TopologyBackedCacheAccessService} path as combined caches while preserving the existing
   * block cache implementation underneath.
   * </p>
   * @param name       topology name used for diagnostics
   * @param blockCache legacy block cache to adapt
   * @param policy     cache placement and admission policy
   * @return topology-backed cache access service backed by a single-tier topology
   * @throws NullPointerException if {@code name}, {@code blockCache}, or {@code policy} is
   *                              {@code null}
   */
  public static TopologyBackedCacheAccessService fromSingleBlockCache(String name,
    BlockCache blockCache, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(blockCache, "blockCache must not be null");
    Objects.requireNonNull(policy, "policy must not be null");

    CacheEngine engine = CacheEngines.fromBlockCache(blockCache);
    CacheTopology topology = new SingleTierTopology(name, engine);
    return new TopologyBackedCacheAccessService(topology, policy);
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
}
