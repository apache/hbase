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
 * {@link CacheEngine} adapter backed by an existing {@link BlockCache} implementation.
 * <p>
 * This adapter is a compatibility bridge for the pluggable block cache migration. It allows
 * existing {@link BlockCache} implementations such as {@code LruBlockCache}, {@code BucketCache},
 * {@code TinyLfuBlockCache}, or {@code LruAdaptiveBlockCache} to participate in
 * {@link CacheTopology} implementations before those caches are migrated to implement
 * {@link CacheEngine} directly.
 * </p>
 * <p>
 * The adapter does not add placement, admission, promotion, or topology behavior. It delegates
 * storage-oriented operations to the wrapped {@link BlockCache}. Topology orchestration remains the
 * responsibility of {@link CacheTopology}, and admission or placement decisions remain the
 * responsibility of {@link CachePlacementAdmissionPolicy}.
 * </p>
 */
@InterfaceAudience.Private
public class BlockCacheBackedCacheEngine implements CacheEngine {

  private final BlockCache blockCache;

  /**
   * Creates a {@link CacheEngine} backed by the given {@link BlockCache}.
   * @param blockCache legacy block cache to adapt
   */
  public BlockCacheBackedCacheEngine(BlockCache blockCache) {
    this.blockCache = Objects.requireNonNull(blockCache, "blockCache must not be null");
  }

  /**
   * Returns the wrapped legacy {@link BlockCache}.
   * <p>
   * This accessor is intended for tests, diagnostics, and transitional wiring only.
   * </p>
   * @return wrapped block cache
   */
  public BlockCache getBlockCache() {
    return blockCache;
  }

  @Override
  public String getName() {
    return blockCache.getClass().getSimpleName();
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(buf, "buf must not be null");
    blockCache.cacheBlock(cacheKey, buf, inMemory);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
    boolean waitWhenCache) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(buf, "buf must not be null");
    blockCache.cacheBlock(cacheKey, buf, inMemory, waitWhenCache);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(buf, "buf must not be null");
    blockCache.cacheBlock(cacheKey, buf);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    return blockCache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics, BlockType blockType) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(blockType, "blockType must not be null");
    return blockCache.getBlock(cacheKey, caching, repeat, updateCacheMetrics, blockType);
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    return blockCache.evictBlock(cacheKey);
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");
    return blockCache.evictBlocksByHfileName(hfileName);
  }

  @Override
  public int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    Objects.requireNonNull(hfileName, "hfileName must not be null");
    return blockCache.evictBlocksRangeByHfileName(hfileName, initOffset, endOffset);
  }

  @Override
  public int evictBlocksByRegionName(String regionName) {
    Objects.requireNonNull(regionName, "regionName must not be null");
    // BlockCache does not support eviction by region name,
    // so we return 0 to indicate no blocks were evicted.
    return 0;
  }

  @Override
  public CacheStats getStats() {
    return blockCache.getStats();
  }

  @Override
  public void shutdown() {
    blockCache.shutdown();
  }

  @Override
  public long getMaxSize() {
    return blockCache.getMaxSize();
  }

  @Override
  public long getFreeSize() {
    return blockCache.getFreeSize();
  }

  @Override
  public long size() {
    return blockCache.size();
  }

  @Override
  public long getCurrentDataSize() {
    return blockCache.getCurrentDataSize();
  }

  @Override
  public long getBlockCount() {
    return blockCache.getBlockCount();
  }

  @Override
  public long getDataBlockCount() {
    return blockCache.getDataBlockCount();
  }

  @Override
  public Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    Objects.requireNonNull(block, "block must not be null");
    return blockCache.blockFitsIntoTheCache(block);
  }

  @Override
  public Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");
    return blockCache.isAlreadyCached(key);
  }

  @Override
  public Optional<Integer> getBlockSize(BlockCacheKey key) {
    Objects.requireNonNull(key, "key must not be null");
    return blockCache.getBlockSize(key);
  }

  @Override
  public boolean isCacheEnabled() {
    return blockCache.isCacheEnabled();
  }

  @Override
  public boolean waitForCacheInitialization(long timeout) {
    return blockCache.waitForCacheInitialization(timeout);
  }

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
