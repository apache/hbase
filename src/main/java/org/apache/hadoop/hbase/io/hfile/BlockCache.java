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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * Block cache interface. Anything that implements the {@link Cacheable}
 * interface can be put in the cache.
 */
public interface BlockCache {
  /**
   * Add block to cache.
   * @param cacheKey The block's cache key.
   * @param buf The block contents wrapped in a ByteBuffer.
   * @param inMemory Whether block should be treated as in-memory
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory);

  /**
   * Add block to cache (defaults to not in-memory).
   * @param cacheKey The block's cache key.
   * @param buf The object to cache.
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf);

  /**
   * Fetch block from cache.
   * @param cacheKey Block to fetch.
   * @param caching Whether this request has caching enabled (used for stats)
   * @param repeat Whether this is a repeat lookup for the same block
   *        (used to avoid double counting cache misses when doing double-check locking)
   *        {@see HFileReaderV2#readBlock(long, long, boolean, boolean, boolean, BlockType)}
   * @return Block or null if block is not in 2 cache.
   */
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat);

  /**
   * Evict block from cache.
   * @param cacheKey Block to evict
   * @return true if block existed and was evicted, false if not
   */
  public boolean evictBlock(BlockCacheKey cacheKey);

  /**
   * Evicts all blocks for the given HFile.
   *
   * @return the number of blocks evicted
   */
  public int evictBlocksByHfileName(String hfileName);

  /**
   * Get the statistics for this block cache.
   * @return Stats
   */
  public CacheStats getStats();

  /**
   * Shutdown the cache.
   */
  public void shutdown();

  /**
   * Returns the total size of the block cache, in bytes.
   * @return size of cache, in bytes
   */
  public long size();

  /**
   * Returns the free size of the block cache, in bytes.
   * @return free space in cache, in bytes
   */
  public long getFreeSize();

  /**
   * Returns the occupied size of the block cache, in bytes.
   * @return occupied space in cache, in bytes
   */
  public long getCurrentSize();

  /**
   * Returns the number of evictions that have occurred.
   * @return number of evictions
   */
  public long getEvictedCount();

  /**
   * Returns the number of blocks currently cached in the block cache.
   * @return number of blocks in the cache
   */
  public long getBlockCount();

  /**
   * Performs a BlockCache summary and returns a List of BlockCacheColumnFamilySummary objects.
   * This method could be fairly heavyweight in that it evaluates the entire HBase file-system
   * against what is in the RegionServer BlockCache.
   * <br><br>
   * The contract of this interface is to return the List in sorted order by Table name, then
   * ColumnFamily.
   *
   * @param conf HBaseConfiguration
   * @return List of BlockCacheColumnFamilySummary
   * @throws IOException exception
   */
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(Configuration conf) throws IOException;
}
