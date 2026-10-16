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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Placement and admission policy implementing AdaptiveLRU behavior using a native
 * {@link LruCacheEngine}.
 * <p>
 * The policy preserves the adaptive admission behavior of the legacy {@code LruAdaptiveBlockCache}.
 * Under sustained LRU eviction pressure, it reduces the percentage of data blocks admitted to the
 * first-level cache. When eviction pressure decreases, data-block admission is gradually restored.
 * </p>
 * <p>
 * Non-data blocks are not filtered by the adaptive algorithm.
 * </p>
 */
@InterfaceAudience.Private
public final class AdaptiveLruCachePlacementAdmissionPolicy
  implements CachePlacementAdmissionPolicy {

  private static final Logger LOG =
    LoggerFactory.getLogger(AdaptiveLruCachePlacementAdmissionPolicy.class);

  private static final String LRU_CACHE_HEAVY_EVICTION_COUNT_LIMIT =
    "hbase.lru.cache.heavy.eviction.count.limit";

  private static final int DEFAULT_LRU_CACHE_HEAVY_EVICTION_COUNT_LIMIT = Integer.MAX_VALUE;

  private static final String LRU_CACHE_HEAVY_EVICTION_MB_SIZE_LIMIT =
    "hbase.lru.cache.heavy.eviction.mb.size.limit";

  private static final long DEFAULT_LRU_CACHE_HEAVY_EVICTION_MB_SIZE_LIMIT = 500L;

  private static final String LRU_CACHE_HEAVY_EVICTION_OVERHEAD_COEFFICIENT =
    "hbase.lru.cache.heavy.eviction.overhead.coefficient";

  private static final float DEFAULT_LRU_CACHE_HEAVY_EVICTION_OVERHEAD_COEFFICIENT = 0.01f;

  private static final long EVICTION_EVALUATION_PERIOD_MS = 10_000L;

  private static final int MIN_CACHE_DATA_BLOCK_PERCENT = 1;

  private static final int MAX_CACHE_DATA_BLOCK_PERCENT = 100;

  private static final int MAX_ADMISSION_REDUCTION_PER_PERIOD = 15;

  private static final CachePlacementAdmissionPolicy DELEGATE =
    DefaultHBaseCachePlacementAdmissionPolicy.INSTANCE;

  private final int heavyEvictionCountLimit;

  private final long heavyEvictionMbSizeLimit;

  private final float heavyEvictionOverheadCoefficient;

  private volatile int cacheDataBlockPercent = MAX_CACHE_DATA_BLOCK_PERCENT;

  private long freedSumMb;

  private int heavyEvictionCount;

  private long evaluationStartTime;

  /**
   * Creates an AdaptiveLRU placement/admission policy from configuration.
   * @param conf cache configuration
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public AdaptiveLruCachePlacementAdmissionPolicy(Configuration conf) {
    Objects.requireNonNull(conf, "conf must not be null");

    this.heavyEvictionCountLimit = Math.max(conf.getInt(LRU_CACHE_HEAVY_EVICTION_COUNT_LIMIT,
      DEFAULT_LRU_CACHE_HEAVY_EVICTION_COUNT_LIMIT), 0);

    this.heavyEvictionMbSizeLimit = Math.max(conf.getLong(LRU_CACHE_HEAVY_EVICTION_MB_SIZE_LIMIT,
      DEFAULT_LRU_CACHE_HEAVY_EVICTION_MB_SIZE_LIMIT), 1L);

    float configuredCoefficient = conf.getFloat(LRU_CACHE_HEAVY_EVICTION_OVERHEAD_COEFFICIENT,
      DEFAULT_LRU_CACHE_HEAVY_EVICTION_OVERHEAD_COEFFICIENT);
    configuredCoefficient = Math.min(configuredCoefficient, 1.0f);
    configuredCoefficient = Math.max(configuredCoefficient, 0.001f);
    this.heavyEvictionOverheadCoefficient = configuredCoefficient;

    this.evaluationStartTime = EnvironmentEdgeManager.currentTime();
  }

  /**
   * Decides whether a block should be admitted into the cache.
   * <p>
   * In a single-tier topology, adaptive rejection means the data block is not cached. In a tiered
   * topology, admission remains enabled because adaptive filtering applies specifically to the L1
   * placement decision rather than to the entire cache hierarchy.
   * </p>
   * @param cacheKey     block cache key
   * @param block        block to cache
   * @param context      cache write context
   * @param priority     admission priority
   * @param topologyView read-only topology view
   * @return admission decision
   */
  @Override
  public AdmissionDecision shouldAdmit(BlockCacheKey cacheKey, Cacheable block,
    CacheWriteContext context, AdmissionPriority priority, CacheTopologyView topologyView) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(priority, "priority must not be null");
    Objects.requireNonNull(topologyView, "topologyView must not be null");

    AdmissionDecision decision =
      DELEGATE.shouldAdmit(cacheKey, block, context, priority, topologyView);
    if (!decision.isAdmitted()) {
      return decision;
    }

    if (
      topologyView.getType() == CacheTopologyType.SINGLE_TIER
        && !shouldCacheInFirstLevel(cacheKey, block)
    ) {
      return AdmissionDecision.reject("Rejected by AdaptiveLRU data-block admission");
    }

    return decision;
  }

  /**
   * Selects the target tier or tiers for an admitted block.
   * <p>
   * The default HBase placement decision is preserved unless AdaptiveLRU rejects a data block from
   * L1. In a tiered topology this removes L1 from the target set while retaining other selected
   * tiers such as L2.
   * </p>
   * @param cacheKey     block cache key
   * @param block        block to cache
   * @param context      cache write context
   * @param topologyView read-only topology view
   * @return cache tier decision
   */
  @Override
  public TierDecision selectTier(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context,
    CacheTopologyView topologyView) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(topologyView, "topologyView must not be null");

    TierDecision decision = DELEGATE.selectTier(cacheKey, block, context, topologyView);

    if (!decision.getTiers().contains(CacheTier.L1) || shouldCacheInFirstLevel(cacheKey, block)) {
      return decision;
    }

    List<CacheTier> tiers = new ArrayList<>();
    for (CacheTier tier : decision.getTiers()) {
      if (tier != CacheTier.L1) {
        tiers.add(tier);
      }
    }
    if (tiers.isEmpty()) {
      return TierDecision.none();
    }
    return TierDecision.multiple(tiers);
  }

  /**
   * Selects the cached representation for an admitted block.
   * @param cacheKey     block cache key
   * @param block        block to cache
   * @param context      cache write context
   * @param topologyView read-only topology view
   * @return representation decision
   */
  @Override
  public RepresentationDecision selectRepresentation(BlockCacheKey cacheKey, Cacheable block,
    CacheWriteContext context, CacheTopologyView topologyView) {
    return DELEGATE.selectRepresentation(cacheKey, block, context, topologyView);
  }

  /**
   * Decides whether a cache hit should trigger promotion.
   * @param cacheKey     block cache key
   * @param block        cached block
   * @param sourceTier   tier containing the block
   * @param context      cache request context
   * @param topologyView read-only topology view
   * @return promotion decision
   */
  @Override
  public PromotionDecision shouldPromote(BlockCacheKey cacheKey, Cacheable block,
    CacheTier sourceTier, CacheRequestContext context, CacheTopologyView topologyView) {
    return DELEGATE.shouldPromote(cacheKey, block, sourceTier, context, topologyView);
  }

  /**
   * Processes cache pressure reported by the first-level LRU engine.
   * <p>
   * Pressure from L2 is ignored because AdaptiveLRU controls admission into the first-level cache.
   * The algorithm intentionally preserves the behavior of the legacy {@code LruAdaptiveBlockCache},
   * including its approximately ten-second evaluation window and megabyte-based pressure
   * accounting.
   * </p>
   * @param sourceTier   tier whose engine experienced cache pressure
   * @param bytesToFree  amount of cache pressure observed, in bytes
   * @param topologyView read-only topology view
   */
  @Override
  public synchronized void onEvictionPressure(CacheTier sourceTier, long bytesToFree,
    CacheTopologyView topologyView) {
    Objects.requireNonNull(sourceTier, "sourceTier must not be null");
    Objects.requireNonNull(topologyView, "topologyView must not be null");

    if (sourceTier != CacheTier.SINGLE && sourceTier != CacheTier.L1) {
      return;
    }

    freedSumMb += Math.max(bytesToFree, 0L) / 1024L / 1024L;

    long stopTime = EnvironmentEdgeManager.currentTime();
    if (stopTime - evaluationStartTime <= EVICTION_EVALUATION_PERIOD_MS - 1L) {
      return;
    }

    int freedDataOverheadPercent = (int) (freedSumMb * 100L / heavyEvictionMbSizeLimit) - 100;

    if (freedSumMb > heavyEvictionMbSizeLimit) {
      heavyEvictionCount++;

      if (heavyEvictionCount > heavyEvictionCountLimit) {
        int change = (int) (freedDataOverheadPercent * heavyEvictionOverheadCoefficient);
        change = Math.min(MAX_ADMISSION_REDUCTION_PER_PERIOD, change);
        change = Math.max(0, change);

        cacheDataBlockPercent -= change;
        cacheDataBlockPercent = Math.max(MIN_CACHE_DATA_BLOCK_PERCENT, cacheDataBlockPercent);
      }
    } else if (freedSumMb >= heavyEvictionMbSizeLimit * 0.1) {
      int change = (int) (-freedDataOverheadPercent * 0.1 + 1);
      cacheDataBlockPercent += change;
      cacheDataBlockPercent = Math.min(MAX_CACHE_DATA_BLOCK_PERCENT, cacheDataBlockPercent);
    } else {
      heavyEvictionCount = 0;
      cacheDataBlockPercent = MAX_CACHE_DATA_BLOCK_PERCENT;
    }

    LOG.info(
      "BlockCache evicted (MB): {}, overhead (%): {}, heavy eviction counter: {}, "
        + "current caching DataBlock (%): {}",
      freedSumMb, freedDataOverheadPercent, heavyEvictionCount, cacheDataBlockPercent);

    freedSumMb = 0L;
    evaluationStartTime = stopTime;
  }

  /**
   * Returns whether the block should be cached in the first-level AdaptiveLRU cache.
   * <p>
   * Non-data blocks are always accepted. Data blocks use the same deterministic offset-based
   * filtering used by the legacy AdaptiveLRU implementation.
   * </p>
   * @param cacheKey block cache key
   * @param block    block being evaluated
   * @return {@code true} if the block should be cached in the first-level cache
   */
  private boolean shouldCacheInFirstLevel(BlockCacheKey cacheKey, Cacheable block) {
    if (!block.getBlockType().isData() || cacheDataBlockPercent == MAX_CACHE_DATA_BLOCK_PERCENT) {
      return true;
    }

    return cacheKey.getOffset() % 100 < cacheDataBlockPercent;
  }

  /**
   * Returns the current percentage of data blocks admitted to the first-level cache.
   * <p>
   * This method is intended for tests and diagnostics.
   * </p>
   * @return current data-block admission percentage
   */
  int getCacheDataBlockPercent() {
    return cacheDataBlockPercent;
  }
}
