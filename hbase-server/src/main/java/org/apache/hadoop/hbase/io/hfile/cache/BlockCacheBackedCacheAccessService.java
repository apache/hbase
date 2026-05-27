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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * {@link CacheAccessService} implementation backed by an existing {@link BlockCache} instance.
 * <p>
 * This adapter is the compatibility bridge for the first migration step. It allows new callers to
 * depend on {@link CacheAccessService} while the runtime implementation still uses the current
 * {@link BlockCache} hierarchy, including LruBlockCache, BucketCache, CombinedBlockCache, and other
 * existing implementations.
 * </p>
 * <p>
 * The adapter should not introduce new policy, placement, admission, representation, promotion, or
 * topology behavior. Its purpose is to translate the new context-based service API into the current
 * {@code BlockCache} API with no intentional behavior change.
 * </p>
 * <p>
 * A future topology-backed service can replace this adapter after call sites have migrated to
 * {@link CacheAccessService}. That future implementation may use {@link CacheTopology},
 * {@link CachePlacementAdmissionPolicy}, and {@link CacheEngine}; this adapter deliberately does
 * not.
 * </p>
 */
@InterfaceAudience.Private
public class BlockCacheBackedCacheAccessService implements CacheAccessService {

  private final BlockCache blockCache;

  /**
   * Creates a cache access service backed by the supplied legacy block cache.
   * @param blockCache block cache to wrap
   */
  public BlockCacheBackedCacheAccessService(BlockCache blockCache) {
    this.blockCache = Objects.requireNonNull(blockCache, "blockCache must not be null");
  }

  /**
   * Returns the wrapped {@link BlockCache} instance.
   * <p>
   * This accessor is intended for tests and transitional wiring only. New read/write path code
   * should use {@link CacheAccessService} methods instead of unwrapping the legacy cache.
   * </p>
   * @return wrapped block cache
   */
  public BlockCache getBlockCache() {
    return blockCache;
  }

  /**
   * Returns a human-readable service name.
   * @return service name
   */
  @Override
  public String getName() {
    return blockCache.getClass().getSimpleName();
  }

  /**
   * Fetches a block by delegating to the wrapped {@link BlockCache}.
   * @param cacheKey block to fetch
   * @param context  cache request context
   * @return cached block, or {@code null} if not present
   */
  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, CacheRequestContext context) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(context, "context must not be null");

    Optional<BlockType> blockType = context.getBlockType();
    if (blockType.isPresent()) {
      return blockCache.getBlock(cacheKey, context.isCaching(), context.isRepeat(),
        context.isUpdateCacheMetrics(), blockType.get());
    }
    return blockCache.getBlock(cacheKey, context.isCaching(), context.isRepeat(),
      context.isUpdateCacheMetrics());
  }

  /**
   * Caches a block by delegating to the wrapped {@link BlockCache}.
   * @param cacheKey block cache key
   * @param block    block contents
   * @param context  cache write context
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(block, "block must not be null");
    Objects.requireNonNull(context, "context must not be null");

    blockCache.cacheBlock(cacheKey, block, context.isInMemory(), context.isWaitWhenCache());
  }

  /**
   * Evicts a single block by delegating to the wrapped {@link BlockCache}.
   * @param cacheKey block to remove
   * @return {@code true} if the block existed and was removed, {@code false} otherwise
   */
  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    return blockCache.evictBlock(cacheKey);
  }

  /**
   * Evicts all cached blocks for the given HFile by delegating to the wrapped {@link BlockCache}.
   * @param hfileName HFile name
   * @return number of blocks removed
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");
    return blockCache.evictBlocksByHfileName(hfileName);
  }

  /**
   * Evicts cached blocks for an HFile range if the wrapped cache supports it.
   * @param hfileName  HFile name
   * @param initOffset inclusive start offset
   * @param endOffset  inclusive end offset
   * @return number of blocks removed
   */
  @Override
  public int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");
    return blockCache.evictBlocksRangeByHfileName(hfileName, initOffset, endOffset);
  }

  /**
   * Evicts cached blocks for a region if the wrapped cache supports it.
   * @param regionName region name
   * @return number of blocks removed
   */
  @Override
  public int evictBlocksByRegionName(String regionName) {
    Objects.requireNonNull(regionName, "regionName must not be null");
    // BlockCache does not support region-based eviction, so we return 0 to indicate no
    // blocks removed
    return 0;
  }

  /**
   * Returns statistics from the wrapped {@link BlockCache}.
   * @return cache statistics
   */
  @Override
  public CacheStats getStats() {
    return blockCache.getStats();
  }

  /**
   * Shuts down the wrapped {@link BlockCache}.
   */
  @Override
  public void shutdown() {
    blockCache.shutdown();
  }

  /**
   * Returns maximum configured cache size from the wrapped {@link BlockCache}.
   * @return maximum cache size
   */
  @Override
  public long getMaxSize() {
    return blockCache.getMaxSize();
  }

  /**
   * Returns free cache size from the wrapped {@link BlockCache}.
   * @return free size
   */
  @Override
  public long getFreeSize() {
    return blockCache.getFreeSize();
  }

  /**
   * Returns occupied cache size from the wrapped {@link BlockCache}.
   * @return occupied cache size
   */
  @Override
  public long size() {
    return blockCache.size();
  }

  /**
   * Returns occupied data-block size from the wrapped {@link BlockCache}.
   * @return occupied data-block size
   */
  @Override
  public long getCurrentDataSize() {
    return blockCache.getCurrentDataSize();
  }

  /**
   * Returns cached block count from the wrapped {@link BlockCache}.
   * @return total block count
   */
  @Override
  public long getBlockCount() {
    return blockCache.getBlockCount();
  }

  /**
   * Returns cached data block count from the wrapped {@link BlockCache}.
   * @return data block count
   */
  @Override
  public long getDataBlockCount() {
    return blockCache.getDataBlockCount();
  }

  /**
   * Delegates block fit check to the wrapped {@link BlockCache}.
   * @param block block to check
   * @return empty if unsupported; otherwise whether the block fits
   */
  @Override
  public Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    Objects.requireNonNull(block, "block must not be null");
    return blockCache.blockFitsIntoTheCache(block);
  }

  /**
   * Delegates already-cached check to the wrapped {@link BlockCache}.
   * @param key block cache key
   * @return empty if unsupported; otherwise whether the block is cached
   */
  @Override
  public Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");
    return blockCache.isAlreadyCached(key);
  }

  /**
   * Delegates per-block size lookup to the wrapped {@link BlockCache}.
   * @param key block cache key
   * @return empty if unsupported or not present; otherwise cached block size
   */
  @Override
  public Optional<Integer> getBlockSize(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");
    return blockCache.getBlockSize(key);
  }

  /**
   * Returns whether the wrapped {@link BlockCache} is enabled.
   * @return {@code true} if enabled, {@code false} otherwise
   */
  @Override
  public boolean isCacheEnabled() {
    return blockCache.isCacheEnabled();
  }

  /**
   * Waits for wrapped cache initialization if supported.
   * @param timeout maximum time to wait
   * @return {@code true} if the cache is enabled and ready, {@code false} otherwise
   */
  @Override
  public boolean waitForCacheInitialization(long timeout) {
    return blockCache.waitForCacheInitialization(timeout);
  }

  /**
   * Propagates configuration changes to the wrapped {@link BlockCache}.
   * @param config new configuration
   */
  @Override
  public void onConfigurationChange(Configuration config) {
    Objects.requireNonNull(config, "config must not be null");
    blockCache.onConfigurationChange(config);
  }

  @Override
  public void notifyFileCachingCompleted(Path fileName, int totalBlockCount, int dataBlockCount,
    long size) {
    Objects.requireNonNull(fileName, "fileName must not be null");
    blockCache.notifyFileCachingCompleted(fileName, totalBlockCount, dataBlockCount, size);
  }
}
