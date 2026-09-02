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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Single-tier cache topology.
 * <p>
 * A single-tier topology contains exactly one cache engine. It is used to represent legacy
 * single-tier {@code BlockCache} implementations inside the topology-backed cache access framework.
 * Unlike tiered topologies, this topology does not perform tier orchestration, promotion, or
 * demotion. All cache operations are directed to the single L1 engine.
 * </p>
 * <p>
 * This topology allows plain block caches to use the same {@link TopologyBackedCacheAccessService}
 * path as combined caches while preserving the existing cache implementation underneath.
 * </p>
 */
@InterfaceAudience.Private
public class SingleTierTopology implements CacheTopology {

  private final String name;
  private final CacheEngine engine;
  private final CacheTopologyView view;

  /**
   * Creates a single-tier cache topology.
   * @param name   topology name used for diagnostics
   * @param engine cache engine backing the single tier
   */
  public SingleTierTopology(String name, CacheEngine engine) {
    this.name = name;
    this.engine = engine;
    this.view = new CacheTopologyView(this);
  }

  /**
   * Returns the diagnostic name of this topology.
   * @return topology name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Returns the topology type.
   * @return {@link CacheTopologyType#SINGLE_TIER}
   */
  @Override
  public CacheTopologyType getType() {
    return CacheTopologyType.SINGLE_TIER;
  }

  /**
   * Returns the cache engines that participate in this topology.
   * @return singleton list containing the single cache engine
   */
  @Override
  public List<CacheEngine> getEngines() {
    return Collections.singletonList(engine);
  }

  /**
   * Returns the tiers available in this topology.
   * <p>
   * A single-tier topology exposes its only engine through {@link CacheTier#SINGLE}. This avoids
   * assigning L1 or L2 semantics to a cache configuration that has only one active engine.
   * </p>
   * @return singleton list containing {@link CacheTier#SINGLE}
   */
  @Override
  public List<CacheTier> getTiers() {
    return Collections.singletonList(CacheTier.SINGLE);
  }

  /**
   * Returns the cache engine associated with the requested tier.
   * <p>
   * The only valid tier for this topology is {@link CacheTier#SINGLE}. The single tier does not
   * imply L1 or L2 behavior; it only means that the topology has one active cache engine.
   * </p>
   * @param tier cache tier to resolve
   * @return the single cache engine for {@link CacheTier#SINGLE}; otherwise
   *         {@link Optional#empty()}
   */
  @Override
  public Optional<CacheEngine> getEngine(CacheTier tier) {
    switch (tier) {
      case SINGLE:
        return Optional.of(engine);
      default:
        return Optional.empty();
    }
  }

  /**
   * Returns the topology view associated with this topology.
   * @return topology view
   */
  @Override
  public CacheTopologyView getView() {
    return view;
  }

  /**
   * Returns cache statistics for the single cache engine.
   * @return cache statistics exposed by the backing engine
   */
  @Override
  public CacheStats getStats() {
    return engine.getStats();
  }

  /**
   * Attempts to promote a block within this topology.
   * <p>
   * Single-tier topology has no higher or lower tier, so promotion is not supported. The method
   * returns {@code false} without modifying the cache.
   * </p>
   * @param cacheKey     cache key identifying the block
   * @param block        cached block
   * @param sourceEngine source cache engine
   * @param targetEngine target cache engine
   * @return {@code false} because single-tier topology does not support promotion
   */
  @Override
  public boolean promote(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine,
    CacheEngine targetEngine) {
    return false;
  }

  /**
   * Attempts to demote a block within this topology.
   * <p>
   * Single-tier topology has no lower tier, so demotion is not supported. The method returns
   * {@code false} without modifying the cache.
   * </p>
   * @param cacheKey     cache key identifying the block
   * @param block        cached block
   * @param sourceEngine source cache engine
   * @param targetEngine target cache engine
   * @return {@code false} because single-tier topology does not support demotion
   */
  @Override
  public boolean demote(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine,
    CacheEngine targetEngine) {
    return false;
  }

  /**
   * Shuts down the single cache engine.
   */
  @Override
  public void shutdown() {
    engine.shutdown();
  }
}
