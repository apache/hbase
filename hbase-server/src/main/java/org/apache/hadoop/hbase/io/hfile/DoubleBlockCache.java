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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.slab.SlabCache;
import org.apache.hadoop.util.StringUtils;

/**
 * DoubleBlockCache is an abstraction layer that combines two caches, the
 * smaller onHeapCache and the larger offHeapCache. CacheBlock attempts to cache
 * the block in both caches, while readblock reads first from the faster on heap
 * cache before looking for the block in the off heap cache. Metrics are the
 * combined size and hits and misses of both caches.
 *
 * @deprecated As of 1.0, replaced by {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache}.
 */
@InterfaceAudience.Private
@Deprecated
public class DoubleBlockCache implements BlockCache, HeapSize {

  static final Log LOG = LogFactory.getLog(DoubleBlockCache.class.getName());

  private final LruBlockCache onHeapCache;
  private final SlabCache offHeapCache;
  private final CacheStats stats;

  /**
   * Default constructor. Specify maximum size and expected average block size
   * (approximation is fine).
   * <p>
   * All other factors will be calculated based on defaults specified in this
   * class.
   *
   * @param onHeapSize maximum size of the onHeapCache, in bytes.
   * @param offHeapSize maximum size of the offHeapCache, in bytes.
   * @param onHeapBlockSize average block size of the on heap cache.
   * @param offHeapBlockSize average block size for the off heap cache
   * @param conf configuration file. currently used only by the off heap cache.
   */
  public DoubleBlockCache(long onHeapSize, long offHeapSize,
      long onHeapBlockSize, long offHeapBlockSize, Configuration conf) {

    LOG.info("Creating on-heap cache of size "
        + StringUtils.humanReadableInt(onHeapSize)
        + "bytes with an average block size of "
        + StringUtils.humanReadableInt(onHeapBlockSize) + " bytes.");
    onHeapCache = new LruBlockCache(onHeapSize, onHeapBlockSize, conf);

    LOG.info("Creating off-heap cache of size "
        + StringUtils.humanReadableInt(offHeapSize)
        + "bytes with an average block size of "
        + StringUtils.humanReadableInt(offHeapBlockSize) + " bytes.");
    offHeapCache = new SlabCache(offHeapSize, offHeapBlockSize);

    offHeapCache.addSlabByConf(conf);
    this.stats = new CacheStats();
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    onHeapCache.cacheBlock(cacheKey, buf, inMemory);
    offHeapCache.cacheBlock(cacheKey, buf);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    onHeapCache.cacheBlock(cacheKey, buf);
    offHeapCache.cacheBlock(cacheKey, buf);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
      boolean updateCacheMetrics) {
    Cacheable cachedBlock;

    if ((cachedBlock = onHeapCache.getBlock(cacheKey, caching, repeat,
        updateCacheMetrics)) != null) {
      if (updateCacheMetrics) stats.hit(caching, cacheKey.getBlockType());
      return cachedBlock;

    } else if ((cachedBlock = offHeapCache.getBlock(cacheKey, caching, repeat,
        updateCacheMetrics)) != null) {
      if (caching) {
        onHeapCache.cacheBlock(cacheKey, cachedBlock);
      }
      if (updateCacheMetrics) stats.hit(caching, cacheKey.getBlockType());
      return cachedBlock;
    }

    if (!repeat && updateCacheMetrics) stats.miss(caching);
    return null;
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    stats.evict();
    boolean cacheA = onHeapCache.evictBlock(cacheKey);
    boolean cacheB = offHeapCache.evictBlock(cacheKey);
    boolean evicted = cacheA || cacheB;
    if (evicted) {
      stats.evicted();
    }
    return evicted;
  }

  @Override
  public CacheStats getStats() {
    return this.stats;
  }

  @Override
  public void shutdown() {
    onHeapCache.shutdown();
    offHeapCache.shutdown();
  }

  @Override
  public long heapSize() {
    return onHeapCache.heapSize() + offHeapCache.heapSize();
  }

  public long size() {
    return onHeapCache.size() + offHeapCache.size();
  }

  public long getFreeSize() {
    return onHeapCache.getFreeSize() + offHeapCache.getFreeSize();
  }

  public long getCurrentSize() {
    return onHeapCache.getCurrentSize() + offHeapCache.getCurrentSize();
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    onHeapCache.evictBlocksByHfileName(hfileName);
    offHeapCache.evictBlocksByHfileName(hfileName);
    return 0;
  }

  @Override
  public long getBlockCount() {
    return onHeapCache.getBlockCount() + offHeapCache.getBlockCount();
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    return new BlockCachesIterator(getBlockCaches());
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return new BlockCache [] {this.onHeapCache, this.offHeapCache};
  }
}
