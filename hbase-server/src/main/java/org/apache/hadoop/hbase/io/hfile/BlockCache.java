/**
 *
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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;

/**
 * Block cache interface. Anything that implements the {@link Cacheable}
 * interface can be put in the cache.
 */
@InterfaceAudience.Private
public interface BlockCache extends Iterable<CachedBlock> {
  /**
   * Add block to cache.
   * @param cacheKey The block's cache key.
   * @param buf The block contents wrapped in a ByteBuffer.
   * @param inMemory Whether block should be treated as in-memory
   * @param cacheDataInL1 If multi-tier block cache deploy -- i.e. has an L1 and L2 tier -- then
   * if this flag is true, cache data blocks up in the L1 tier (meta blocks are probably being
   * cached in L1 already).
   */
  void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory, boolean cacheDataInL1);

  /**
   * Add block to cache (defaults to not in-memory).
   * @param cacheKey The block's cache key.
   * @param buf The object to cache.
   */
  void cacheBlock(BlockCacheKey cacheKey, Cacheable buf);

  /**
   * Fetch block from cache.
   * @param cacheKey Block to fetch.
   * @param caching Whether this request has caching enabled (used for stats)
   * @param repeat Whether this is a repeat lookup for the same block
   *        (used to avoid double counting cache misses when doing double-check locking)
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
   *
   * @return the number of blocks evicted
   */
  int evictBlocksByHfileName(String hfileName);

  /**
   * Get the statistics for this block cache.
   * @return Stats
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
   * Returns the number of blocks currently cached in the block cache.
   * @return number of blocks in the cache
   */
  long getBlockCount();

  /**
   * @return Iterator over the blocks in the cache.
   */
  Iterator<CachedBlock> iterator();

  /**
   * @return The list of sub blockcaches that make up this one; returns null if no sub caches.
   */
  BlockCache [] getBlockCaches();

  /**
   * Called when the scanner using the block decides to return the block once its usage
   * is over.
   * This API should be called after the block is used, failing to do so may have adverse effects
   * by preventing the blocks from being evicted because of which it will prevent new hot blocks
   * from getting added to the block cache.  The implementation of the BlockCache will decide
   * on what to be done with the block based on the memory type of the block's {@link MemoryType}.
   * @param cacheKey the cache key of the block
   * @param block the hfileblock to be returned
   */
  void returnBlock(BlockCacheKey cacheKey, Cacheable block);
}
