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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Tiered inclusive cache topology.
 * <p>
 * In an inclusive topology, a block promoted from L2 to L1 may remain in L2. Promotion is therefore
 * modeled as a copy rather than a move.
 * </p>
 * <p>
 * This class is introduced as a topology foundation. Production wiring and policy-driven routing
 * are handled in later migration phases.
 * </p>
 */
@InterfaceAudience.Private
public class TieredInclusiveTopology implements CacheTopology {

  private final String name;
  private final CacheEngine l1;
  private final CacheEngine l2;
  private final CacheTopologyView view;
  private final CacheStats stats;

  public TieredInclusiveTopology(String name, CacheEngine l1, CacheEngine l2) {
    this.name = name;
    this.l1 = l1;
    this.l2 = l2;
    this.view = new CacheTopologyView(this);
    this.stats = new AggregateCacheStats(name, l1.getStats(), l2.getStats());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public CacheTopologyType getType() {
    return CacheTopologyType.TIERED_INCLUSIVE;
  }

  @Override
  public List<CacheEngine> getEngines() {
    return Arrays.asList(l1, l2);
  }

  @Override
  public List<CacheTier> getTiers() {
    return Arrays.asList(CacheTier.L1, CacheTier.L2);
  }

  @Override
  public Optional<CacheEngine> getEngine(CacheTier tier) {
    switch (tier) {
      case L1:
        return Optional.of(l1);
      case L2:
        return Optional.of(l2);
      default:
        return Optional.empty();
    }
  }

  @Override
  public CacheTopologyView getView() {
    return view;
  }

  @Override
  public CacheStats getStats() {
    return stats;
  }

  @Override
  public boolean promote(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine,
    CacheEngine targetEngine) {
    if (targetEngine == null || block == null) {
      return false;
    }

    targetEngine.cacheBlock(cacheKey, block);
    return true;
  }

  @Override
  public void shutdown() {
    l1.shutdown();
    l2.shutdown();
  }

  /**
   * Handles a capacity-driven eviction from this inclusive topology.
   * <p>
   * L1 eviction does not require demotion because inclusive placement normally maintains a
   * corresponding block in L2. Cache placement is best-effort and is not atomic across tiers, so
   * there may be short windows where an L2 copy is not present. The topology deliberately does not
   * perform an L2 membership check on every L1 eviction to avoid adding cross-tier lookup overhead
   * to the eviction path. A missing copy results only in a subsequent cache miss.
   * </p>
   * <p>
   * L2 pressure eviction likewise does not cause movement to another tier.
   * </p>
   * @param cacheKey     key identifying the evicted block
   * @param block        evicted block
   * @param sourceEngine engine that evicted the block
   * @return {@code false}, because no additional placement is required
   */
  @Override
  public boolean handleEviction(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine) {
    return false;
  }
}
