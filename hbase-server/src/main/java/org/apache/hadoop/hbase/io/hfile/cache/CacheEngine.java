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

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Storage abstraction for a concrete HBase block cache backend.
 * <p>
 * A {@code CacheEngine} represents the storage layer only. It is responsible for storing,
 * retrieving, invalidating, and reporting statistics for cached blocks. It does not perform tier
 * orchestration, admission control, placement decisions, or promotion/demotion across cache levels.
 * </p>
 * <p>
 * This interface is intentionally aligned with the storage-oriented subset of the current
 * {@code BlockCache} contract so that existing implementations such as LruBlockCache and
 * BucketCache can be migrated incrementally with minimal behavioral risk.
 * </p>
 * <p>
 * Responsibilities of a {@code CacheEngine} include:
 * </p>
 * <ul>
 * <li>block lookup</li>
 * <li>block insertion</li>
 * <li>targeted invalidation / eviction</li>
 * <li>capacity and occupancy reporting</li>
 * <li>engine-local statistics</li>
 * <li>optional implementation-specific fit/capability checks</li>
 * </ul>
 * <p>
 * Non-responsibilities include:
 * </p>
 * <ul>
 * <li>L1/L2 topology orchestration</li>
 * <li>admission policy</li>
 * <li>tier placement decisions</li>
 * <li>promotion or demotion across tiers</li>
 * </ul>
 */
@InterfaceAudience.Private
public interface CacheEngine {

  /**
   * Returns a human-readable name for this cache engine instance.
   * <p>
   * The name is intended for logging, metrics, diagnostics, and configuration reporting. It should
   * be stable for the lifetime of the engine instance.
   * </p>
   * @return engine name
   */
  String getName();

  /**
   * Adds a block to the cache.
   * @param cacheKey block cache key
   * @param buf      block contents
   * @param inMemory whether the block should be treated as in-memory
   */
  void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory);

  /**
   * Adds a block to the cache, optionally waiting for asynchronous cache backends.
   * <p>
   * This is primarily useful for implementations such as BucketCache that may buffer writes
   * asynchronously.
   * </p>
   * @param cacheKey      block cache key
   * @param buf           block contents
   * @param inMemory      whether the block should be treated as in-memory
   * @param waitWhenCache whether to wait for the cache operation to be accepted/flushed
   */
  default void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
    boolean waitWhenCache) {
    cacheBlock(cacheKey, buf, inMemory);
  }

  /**
   * Adds a block to the cache, defaulting to non in-memory treatment.
   * @param cacheKey block cache key
   * @param buf      block contents
   */
  void cacheBlock(BlockCacheKey cacheKey, Cacheable buf);

  /**
   * Fetches a block from the cache.
   * @param cacheKey           block to fetch
   * @param caching            whether caching is enabled for the request; used for metrics
   * @param repeat             whether this is a repeated lookup for the same block; used to avoid
   *                           double-counting misses
   * @param updateCacheMetrics whether cache metrics should be updated
   * @return cached block, or {@code null} if not present
   */
  Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics);

  /**
   * Fetches a block from the cache with an optional block type hint.
   * <p>
   * Implementations may ignore the block type if it is not needed.
   * </p>
   * @param cacheKey           block to fetch
   * @param caching            whether caching is enabled for the request; used for metrics
   * @param repeat             whether this is a repeated lookup for the same block; used to avoid
   *                           double-counting misses
   * @param updateCacheMetrics whether cache metrics should be updated
   * @param blockType          optional block type hint
   * @return cached block, or {@code null} if not present
   */
  default Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics, BlockType blockType) {
    return getBlock(cacheKey, caching, repeat, updateCacheMetrics);
  }

  /**
   * Evicts a single block from the cache.
   * @param cacheKey block to evict
   * @return {@code true} if the block existed and was evicted, {@code false} otherwise
   */
  boolean evictBlock(BlockCacheKey cacheKey);

  /**
   * Evicts all cached blocks for the given HFile.
   * @param hfileName HFile name
   * @return number of blocks evicted
   */
  int evictBlocksByHfileName(String hfileName);

  /**
   * Evicts all cached blocks for the given HFile within the specified offset range.
   * <p>
   * This is useful for targeted invalidation during file lifecycle events where only a subset of
   * blocks should be removed.
   * </p>
   * @param hfileName  HFile name
   * @param initOffset inclusive start offset
   * @param endOffset  inclusive end offset
   * @return number of blocks evicted
   */
  default int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    return 0;
  }

  /**
   * Evicts all cached blocks associated with the specified region.
   * <p>
   * This is a new API intended to support region-scoped invalidation in a storage-oriented way,
   * without requiring higher-level code to enumerate files first.
   * </p>
   * @param regionName region name
   * @return number of blocks evicted
   */
  default int evictBlocksByRegionName(String regionName) {
    return 0;
  }

  /**
   * Returns engine statistics.
   * @return cache statistics
   */
  CacheStats getStats();

  /**
   * Shuts down this cache engine and releases any owned resources.
   */
  void shutdown();

  /**
   * Returns the maximum configured cache size, in bytes.
   * @return maximum cache size
   */
  long getMaxSize();

  /**
   * Returns the amount of free space available in the cache, in bytes.
   * @return free size
   */
  long getFreeSize();

  /**
   * Returns the currently occupied cache size, in bytes.
   * @return occupied size
   */
  long size();

  /**
   * Returns the currently occupied size of data blocks, in bytes.
   * @return occupied data-block size
   */
  long getCurrentDataSize();

  /**
   * Returns the total number of cached blocks.
   * @return total block count
   */
  long getBlockCount();

  /**
   * Returns the number of cached data blocks.
   * @return data block count
   */
  long getDataBlockCount();

  /**
   * Checks whether the given block can fit into this cache engine.
   * <p>
   * This method is optional because not all engines expose a meaningful fit check.
   * </p>
   * @param block block to check
   * @return empty if unsupported; otherwise whether the block fits
   */
  default Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    return Optional.empty();
  }

  /**
   * Checks whether the block represented by the given key is already cached.
   * <p>
   * This method is optional because not all engines can answer it efficiently.
   * </p>
   * @param key block cache key
   * @return empty if unsupported; otherwise whether the block is cached
   */
  default Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    return Optional.empty();
  }

  /**
   * Returns the size of the cached block represented by the given key.
   * <p>
   * This method is optional because not all engines expose per-block size cheaply.
   * </p>
   * @param key block cache key
   * @return empty if unsupported or not present; otherwise cached block size
   */
  default Optional<Integer> getBlockSize(BlockCacheKey key) {
    return Optional.empty();
  }

  /**
   * Returns whether this cache engine is enabled.
   * @return {@code true} if enabled, {@code false} otherwise
   */
  default boolean isCacheEnabled() {
    return true;
  }

  /**
   * Waits for asynchronous engine initialization to complete.
   * <p>
   * Some cache backends may perform initialization asynchronously. Engines that do not require this
   * may simply return {@code true} immediately.
   * </p>
   * @param timeout maximum time to wait
   * @return {@code true} if the cache is enabled, {@code false} otherwise
   */
  default boolean waitForCacheInitialization(long timeout) {
    return true;
  }

  /**
   * Refreshes this engine's configuration.
   * <p>
   * The default implementation is a no-op.
   * </p>
   * @param config new configuration
   */
  default void onConfigurationChange(Configuration config) {
    // noop
  }
}
