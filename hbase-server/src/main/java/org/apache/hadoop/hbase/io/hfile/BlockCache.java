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
package org.apache.hadoop.hbase.io.hfile;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Block cache interface. Anything that implements the {@link Cacheable} interface can be put in the
 * cache.
 */
@InterfaceAudience.Private
public interface BlockCache extends Iterable<CachedBlock>, ConfigurationObserver {
  /**
   * Add block to cache.
   * @param cacheKey The block's cache key.
   * @param buf      The block contents wrapped in a ByteBuffer.
   * @param inMemory Whether block should be treated as in-memory
   */
  void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory);

  /**
   * Add block to cache.
   * @param cacheKey      The block's cache key.
   * @param buf           The block contents wrapped in a ByteBuffer.
   * @param inMemory      Whether block should be treated as in-memory
   * @param waitWhenCache Whether to wait for the cache to be flushed mainly when BucketCache is
   *                      configured.
   */
  default void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
    boolean waitWhenCache) {
    cacheBlock(cacheKey, buf, inMemory);
  }

  /**
   * Add block to cache (defaults to not in-memory).
   * @param cacheKey The block's cache key.
   * @param buf      The object to cache.
   */
  void cacheBlock(BlockCacheKey cacheKey, Cacheable buf);

  /**
   * Fetch block from cache.
   * @param cacheKey           Block to fetch.
   * @param caching            Whether this request has caching enabled (used for stats)
   * @param repeat             Whether this is a repeat lookup for the same block (used to avoid
   *                           double counting cache misses when doing double-check locking)
   * @param updateCacheMetrics Whether to update cache metrics or not
   * @return Block or null if block is not in 2 cache.
   */
  Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics);

  /**
   * Evict block from cache.
   * @param cacheKey Block to evict
   * @return true if block existed and was evicted, false if not
   */
  boolean evictBlock(BlockCacheKey cacheKey);

  /**
   * Evicts all blocks for the given HFile.
   * @return the number of blocks evicted
   */
  int evictBlocksByHfileName(String hfileName);

  /**
   * Get the statistics for this block cache.
   */
  CacheStats getStats();

  /**
   * Shutdown the cache.
   */
  void shutdown();

  /**
   * Returns the total size of the block cache, in bytes.
   * @return size of cache, in bytes
   */
  long size();

  /**
   * Returns the Max size of the block cache, in bytes.
   * @return size of cache, in bytes
   */
  long getMaxSize();

  /**
   * Returns the free size of the block cache, in bytes.
   * @return free space in cache, in bytes
   */
  long getFreeSize();

  /**
   * Returns the occupied size of the block cache, in bytes.
   * @return occupied space in cache, in bytes
   */
  long getCurrentSize();

  /**
   * Returns the occupied size of data blocks, in bytes.
   * @return occupied space in cache, in bytes
   */
  long getCurrentDataSize();

  /**
   * Returns the number of blocks currently cached in the block cache.
   * @return number of blocks in the cache
   */
  long getBlockCount();

  /**
   * Returns the number of data blocks currently cached in the block cache.
   * @return number of blocks in the cache
   */
  long getDataBlockCount();

  /** Returns Iterator over the blocks in the cache. */
  @Override
  Iterator<CachedBlock> iterator();

  /** Returns The list of sub blockcaches that make up this one; returns null if no sub caches. */
  BlockCache[] getBlockCaches();

  /**
   * Check if block type is meta or index block
   * @param blockType block type of a given HFile block
   * @return true if block type is non-data block
   */
  default boolean isMetaBlock(BlockType blockType) {
    return blockType != null && blockType.getCategory() != BlockType.BlockCategory.DATA;
  }

  /**
   * Notifies the cache implementation that the given file has been fully cached (all its blocks
   * made into the cache).
   * @param fileName        the file that has been completely cached.
   * @param totalBlockCount the total of blocks cached for this file.
   * @param dataBlockCount  number of DATA block type cached.
   * @param size            the size, in bytes, cached.
   */
  default void notifyFileCachingCompleted(Path fileName, int totalBlockCount, int dataBlockCount,
    long size) {
    // noop
  }

  /**
   * Checks whether there's enough space left in the cache to accommodate the passed block. This
   * method may not be overridden by all implementing classes. In such cases, the returned Optional
   * will be empty. For subclasses implementing this logic, the returned Optional would contain the
   * boolean value reflecting if the passed block fits into the remaining cache space available.
   * @param block the block we want to check if fits into the cache.
   * @return empty optional if this method is not supported, otherwise the returned optional
   *         contains the boolean value informing if the block fits into the cache available space.
   */
  default Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    return Optional.empty();
  }

  /**
   * Checks whether blocks for the passed file should be cached or not. This method may not be
   * overridden by all implementing classes. In such cases, the returned Optional will be empty. For
   * subclasses implementing this logic, the returned Optional would contain the boolean value
   * reflecting if the passed file should indeed be cached.
   * @param fileName to check if it should be cached.
   * @return empty optional if this method is not supported, otherwise the returned optional
   *         contains the boolean value informing if the file should be cached.
   */
  default Optional<Boolean> shouldCacheFile(String fileName) {
    return Optional.empty();
  }

  /**
   * Checks whether the block for the passed key is already cached. This method may not be
   * overridden by all implementing classes. In such cases, the returned Optional will be empty. For
   * subclasses implementing this logic, the returned Optional would contain the boolean value
   * reflecting if the block for the passed key is already cached or not.
   * @param key for the block we want to check if it's already in the cache.
   * @return empty optional if this method is not supported, otherwise the returned optional
   *         contains the boolean value informing if the block is already cached.
   */
  default Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    return Optional.empty();
  }

  /**
   * Returns an Optional containing the size of the block related to the passed key. If the block is
   * not in the cache, returned optional will be empty. Also, this method may not be overridden by
   * all implementing classes. In such cases, the returned Optional will be empty.
   * @param key for the block we want to check if it's already in the cache.
   * @return empty optional if this method is not supported, otherwise the returned optional
   *         contains the boolean value informing if the block is already cached.
   */
  default Optional<Integer> getBlockSize(BlockCacheKey key) {
    return Optional.empty();
  }

  /**
   * Returns an Optional containing the map of files that have been fully cached (all its blocks are
   * present in the cache. This method may not be overridden by all implementing classes. In such
   * cases, the returned Optional will be empty.
   * @return empty optional if this method is not supported, otherwise the returned optional
   *         contains a map of all files that have been fully cached.
   */
  default Optional<Map<String, Pair<String, Long>>> getFullyCachedFiles() {
    return Optional.empty();
  }

  /**
   * Returns an Optional containing a map of regions and the percentage of how much of it has been
   * cached so far.
   * @return empty optional if this method is not supported, otherwise the returned optional
   *         contains a map of current regions caching percentage.
   */
  default Optional<Map<String, Long>> getRegionCachedInfo() {
    return Optional.empty();
  }

  /**
   * Evict all blocks for the given file name between the passed offset values.
   * @param hfileName  The file for which blocks should be evicted.
   * @param initOffset the initial offset for the range of blocks to be evicted.
   * @param endOffset  the end offset for the range of blocks to be evicted.
   * @return number of blocks evicted.
   */
  default int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    return 0;
  }

  /**
   * Allows for BlockCache implementations to provide a mean to refresh their configurations. Since
   * HBASE-29249, CacheConfig implements PropagatingConfigurationObserver and registers itself
   * together with the used BlockCache implementation for notifications of dynamic configuration
   * changes. The default is a noop.
   * @param config the new configuration to be updated.
   */
  default void onConfigurationChange(Configuration config) {
    // noop
  }
}
