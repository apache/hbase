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
    wireVictimCache(l1, l2);
    CacheEngine l1Engine = CacheEngines.fromBlockCache(l1);
    CacheEngine l2Engine = CacheEngines.fromBlockCache(l2);
    CacheTopology topology = new TieredExclusiveTopology(name, l1Engine, l2Engine);
    return new TopologyBackedCacheAccessService(topology, policy);
  }

  /**
   * Configures the legacy L1 to L2 victim-cache relationship used by CombinedBlockCache.
   * <p>
   * The topology-backed service owns lookup and placement orchestration, but existing
   * {@link FirstLevelBlockCache} implementations still use a direct victim-cache reference to move
   * evicted blocks from L1 to L2. Keep this wiring while L1 and L2 are still legacy
   * {@link BlockCache} implementations.
   * </p>
   * @param l1 first-level block cache
   * @param l2 second-level block cache
   */
  private static void wireVictimCache(BlockCache l1, BlockCache l2) {
    if (l1 instanceof FirstLevelBlockCache) {
      try {
        ((FirstLevelBlockCache) l1).setVictimCache(l2);
      } catch (IllegalArgumentException e) {
        // ignore if already wired
      }
    }
  }
}
