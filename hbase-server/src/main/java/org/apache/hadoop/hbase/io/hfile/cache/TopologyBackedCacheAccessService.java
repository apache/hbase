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
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * {@link CacheAccessService} implementation backed by {@link CacheTopology} and {@link CacheEngine}
 * instances.
 * <p>
 * This implementation connects the cache access layer to the topology, policy, and engine
 * abstractions. {@link CachePlacementAdmissionPolicy} decides whether a block should be admitted,
 * where it should be placed, which representation should be used, and whether a hit should trigger
 * promotion. {@link CacheTopology} provides tier structure, engine lookup, aggregate statistics,
 * and topology-specific promotion mechanics. {@link CacheEngine} performs the actual storage
 * operations.
 * </p>
 * <p>
 * This class is the topology-backed counterpart to {@link BlockCacheBackedCacheAccessService}. The
 * block-cache-backed implementation is useful for incremental migration with no behavior change.
 * This implementation is useful once callers are ready to exercise the new topology and engine
 * abstractions directly through {@link CacheAccessService}.
 * </p>
 * <p>
 * Representation conversion is not performed by this initial implementation. This preserves current
 * behavior while leaving room for future packed/unpacked or engine-default representation handling.
 * </p>
 */
@InterfaceAudience.Private
public class TopologyBackedCacheAccessService implements CacheAccessService {

  private final CacheTopology topology;
  private final CachePlacementAdmissionPolicy policy;
  private final CacheTopologyView topologyView;

  /**
   * Creates a topology-backed cache access service.
   * @param topology cache topology used for tier structure, engine lookup, and promotion mechanics
   * @param policy   placement and admission policy used for cache access decisions
   */
  public TopologyBackedCacheAccessService(CacheTopology topology,
    CachePlacementAdmissionPolicy policy) {
    this.topology = Objects.requireNonNull(topology, "topology must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.topologyView =
      Objects.requireNonNull(topology.getView(), "topology view must not be null");
  }

  /**
   * Returns the topology used by this service.
   * <p>
   * This accessor is intended for tests, diagnostics, and transitional wiring. HBase read/write
   * path callers should use {@link CacheAccessService} methods instead of accessing topology
   * directly.
   * </p>
   * @return cache topology
   */
  public CacheTopology getTopology() {
    return topology;
  }

  /**
   * Returns the placement/admission policy used by this service.
   * @return placement/admission policy
   */
  public CachePlacementAdmissionPolicy getPolicy() {
    return policy;
  }

  /**
   * Returns the read-only topology view used by policy calls.
   * @return read-only topology view
   */
  public CacheTopologyView getTopologyView() {
    return topologyView;
  }

  /**
   * Returns a human-readable service name.
   * @return service name
   */
  @Override
  public String getName() {
    return topology.getName();
  }

  /**
   * Fetches a block by checking topology tiers in lookup order.
   * <p>
   * The lookup order is defined by {@link CacheTopology#getTiers()}. On a cache hit, this method
   * asks the configured {@link CachePlacementAdmissionPolicy} whether the block should be promoted.
   * If promotion is requested and the target tier exists, promotion mechanics are delegated to
   * {@link CacheTopology#promote(BlockCacheKey, Cacheable, CacheEngine, CacheEngine)}.
   * </p>
   * @param cacheKey block to fetch
   * @param context  cache request context
   * @return cached block, or {@code null} if not present in any tier
   */
  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, CacheRequestContext context) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(context, "context must not be null");

    for (CacheTier tier : topology.getTiers()) {
      Optional<CacheEngine> engine = topology.getEngine(tier);
      if (engine.isEmpty()) {
        continue;
      }

      Cacheable block = getBlockFromEngine(engine.get(), cacheKey, context);
      if (block != null) {
        maybePromote(cacheKey, block, tier, engine.get(), context);
        return block;
      }
    }

    return null;
  }

  /**
   * Adds a block to the cache using policy-selected target tiers.
   * <p>
   * This method first asks the configured policy whether the block should be admitted. If admitted,
   * the policy selects the target tier or tiers. The block is then inserted into each selected
   * engine using {@link CacheEngine#cacheBlock(BlockCacheKey, Cacheable, boolean, boolean)}.
   * </p>
   * <p>
   * The policy's representation decision is intentionally not applied in this initial
   * implementation. The current block object is passed through unchanged.
   * </p>
   * @param cacheKey block cache key
   * @param block    block contents
   * @param context  cache write context
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");

    AdmissionDecision admission =
      policy.shouldAdmit(cacheKey, block, context, AdmissionPriority.NORMAL, topologyView);
    if (!admission.isAdmitted()) {
      return;
    }

    policy.selectRepresentation(cacheKey, block, context, topologyView);
    TierDecision tierDecision = policy.selectTier(cacheKey, block, context, topologyView);
    for (CacheTier tier : tierDecision.getTiers()) {
      Optional<CacheEngine> engine = topology.getEngine(tier);
      if (engine.isPresent()) {
        engine.get().cacheBlock(cacheKey, block, context.isInMemory(), context.isWaitWhenCache());
      }
    }
  }

  /**
   * Evicts a single block from all engines participating in the topology.
   * @param cacheKey block to remove
   * @return {@code true} if at least one engine removed the block, {@code false} otherwise
   */
  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");

    boolean evicted = false;
    for (CacheEngine engine : topology.getEngines()) {
      evicted |= engine.evictBlock(cacheKey);
    }
    return evicted;
  }

  /**
   * Evicts all cached blocks for the given HFile from all engines participating in the topology.
   * @param hfileName HFile name
   * @return total number of blocks removed across all engines
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");

    int evicted = 0;
    for (CacheEngine engine : topology.getEngines()) {
      evicted += engine.evictBlocksByHfileName(hfileName);
    }
    return evicted;
  }

  /**
   * Evicts cached blocks for the given HFile range from all engines participating in the topology.
   * @param hfileName  HFile name
   * @param initOffset inclusive start offset
   * @param endOffset  inclusive end offset
   * @return total number of blocks removed across all engines
   */
  @Override
  public int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");

    int evicted = 0;
    for (CacheEngine engine : topology.getEngines()) {
      evicted += engine.evictBlocksRangeByHfileName(hfileName, initOffset, endOffset);
    }
    return evicted;
  }

  /**
   * Evicts cached blocks for the given region from all engines participating in the topology.
   * @param regionName region name
   * @return total number of blocks removed across all engines
   */
  @Override
  public int evictBlocksByRegionName(String regionName) {
    Objects.requireNonNull(regionName, "regionName must not be null");

    int evicted = 0;
    for (CacheEngine engine : topology.getEngines()) {
      evicted += engine.evictBlocksByRegionName(regionName);
    }
    return evicted;
  }

  /**
   * Returns aggregate topology statistics.
   * @return aggregate cache statistics
   */
  @Override
  public CacheStats getStats() {
    return topology.getStats();
  }

  /**
   * Shuts down the topology.
   */
  @Override
  public void shutdown() {
    topology.shutdown();
  }

  /**
   * Returns aggregate maximum configured cache size across participating engines.
   * @return aggregate maximum cache size
   */
  @Override
  public long getMaxSize() {
    long size = 0L;
    for (CacheEngine engine : topology.getEngines()) {
      size += engine.getMaxSize();
    }
    return size;
  }

  /**
   * Returns aggregate free cache size across participating engines.
   * @return aggregate free size
   */
  @Override
  public long getFreeSize() {
    long size = 0L;
    for (CacheEngine engine : topology.getEngines()) {
      size += engine.getFreeSize();
    }
    return size;
  }

  /**
   * Returns aggregate occupied cache size across participating engines.
   * @return aggregate occupied cache size
   */
  @Override
  public long size() {
    long size = 0L;
    for (CacheEngine engine : topology.getEngines()) {
      size += engine.size();
    }
    return size;
  }

  /**
   * Returns aggregate occupied data-block size across participating engines.
   * @return aggregate occupied data-block size
   */
  @Override
  public long getCurrentDataSize() {
    long size = 0L;
    for (CacheEngine engine : topology.getEngines()) {
      size += engine.getCurrentDataSize();
    }
    return size;
  }

  /**
   * Returns aggregate cached block count across participating engines.
   * @return aggregate cached block count
   */
  @Override
  public long getBlockCount() {
    long count = 0L;
    for (CacheEngine engine : topology.getEngines()) {
      count += engine.getBlockCount();
    }
    return count;
  }

  /**
   * Returns aggregate cached data block count across participating engines.
   * @return aggregate cached data block count
   */
  @Override
  public long getDataBlockCount() {
    long count = 0L;
    for (CacheEngine engine : topology.getEngines()) {
      count += engine.getDataBlockCount();
    }
    return count;
  }

  /**
   * Checks whether the given block fits into at least one participating engine that supports this
   * check.
   * @param block block to check
   * @return empty if no engine supports this check; otherwise whether at least one engine can fit
   *         the block
   */
  @Override
  public Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    Objects.requireNonNull(block, "block must not be null");

    boolean unsupported = true;
    for (CacheEngine engine : topology.getEngines()) {
      Optional<Boolean> result = engine.blockFitsIntoTheCache(block);
      if (result.isPresent()) {
        unsupported = false;
        if (result.get()) {
          return Optional.of(true);
        }
      }
    }
    return unsupported ? Optional.empty() : Optional.of(false);
  }

  /**
   * Checks whether the block represented by the given key is present in any participating engine
   * that supports this check.
   * @param key block cache key
   * @return empty if no engine supports this check; otherwise whether any engine has the block
   */
  @Override
  public Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");

    boolean unsupported = true;
    for (CacheEngine engine : topology.getEngines()) {
      Optional<Boolean> result = engine.isAlreadyCached(key);
      if (result.isPresent()) {
        unsupported = false;
        if (result.get()) {
          return Optional.of(true);
        }
      }
    }
    return unsupported ? Optional.empty() : Optional.of(false);
  }

  /**
   * Returns the first available cached block size reported by participating engines.
   * <p>
   * If the same block exists in multiple engines, this method returns the first present size in
   * topology engine order. It does not sum duplicate copies.
   * </p>
   * @param key block cache key
   * @return empty if unsupported or not present; otherwise cached block size
   */
  @Override
  public Optional<Integer> getBlockSize(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");

    for (CacheEngine engine : topology.getEngines()) {
      Optional<Integer> size = engine.getBlockSize(key);
      if (size.isPresent()) {
        return size;
      }
    }
    return Optional.empty();
  }

  /**
   * Returns whether at least one participating engine is enabled.
   * @return {@code true} if at least one engine is enabled, {@code false} otherwise
   */
  @Override
  public boolean isCacheEnabled() {
    for (CacheEngine engine : topology.getEngines()) {
      if (engine.isCacheEnabled()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Waits for all participating engines to complete initialization.
   * @param timeout maximum time to wait per engine
   * @return {@code true} if all engines report ready/enabled, {@code false} otherwise
   */
  @Override
  public boolean waitForCacheInitialization(long timeout) {
    boolean enabled = true;
    for (CacheEngine engine : topology.getEngines()) {
      enabled &= engine.waitForCacheInitialization(timeout);
    }
    return enabled;
  }

  /**
   * Propagates a configuration change to all participating engines.
   * @param config new configuration
   */
  @Override
  public void onConfigurationChange(Configuration config) {
    Objects.requireNonNull(config, "config must not be null");
    for (CacheEngine engine : topology.getEngines()) {
      engine.onConfigurationChange(config);
    }
  }

  private Cacheable getBlockFromEngine(CacheEngine engine, BlockCacheKey cacheKey,
    CacheRequestContext context) {
    Optional<BlockType> blockType = context.getBlockType();
    if (blockType.isPresent()) {
      return engine.getBlock(cacheKey, context.isCaching(), context.isRepeat(),
        context.isUpdateCacheMetrics(), blockType.get());
    }
    return engine.getBlock(cacheKey, context.isCaching(), context.isRepeat(),
      context.isUpdateCacheMetrics());
  }

  private void maybePromote(BlockCacheKey cacheKey, Cacheable block, CacheTier sourceTier,
    CacheEngine sourceEngine, CacheRequestContext context) {
    PromotionDecision decision =
      policy.shouldPromote(cacheKey, block, sourceTier, context, topologyView);
    if (decision == null || !decision.shouldPromote()) {
      return;
    }

    Optional<CacheEngine> targetEngine = topology.getEngine(decision.getTargetTier());
    if (targetEngine.isEmpty()) {
      return;
    }

    topology.promote(cacheKey, block, sourceEngine, targetEngine.get());
  }
}
