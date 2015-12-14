/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.util.Iterator;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;


/**
 * CombinedBlockCache is an abstraction layer that combines
 * {@link LruBlockCache} and {@link BucketCache}. The smaller lruCache is used
 * to cache bloom blocks and index blocks.  The larger l2Cache is used to
 * cache data blocks. {@link #getBlock(BlockCacheKey, boolean, boolean, boolean)} reads
 * first from the smaller lruCache before looking for the block in the l2Cache.  Blocks evicted
 * from lruCache are put into the bucket cache. 
 * Metrics are the combined size and hits and misses of both caches.
 * 
 */
@InterfaceAudience.Private
public class CombinedBlockCache implements ResizableBlockCache, HeapSize {
  protected final LruBlockCache lruCache;
  protected final BlockCache l2Cache;
  protected final CombinedCacheStats combinedCacheStats;

  public CombinedBlockCache(LruBlockCache lruCache, BlockCache l2Cache) {
    this.lruCache = lruCache;
    this.l2Cache = l2Cache;
    this.combinedCacheStats = new CombinedCacheStats(lruCache.getStats(),
        l2Cache.getStats());
  }

  @Override
  public long heapSize() {
    long l2size = 0;
    if (l2Cache instanceof HeapSize) {
      l2size = ((HeapSize) l2Cache).heapSize();
    }
    return lruCache.heapSize() + l2size;
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
      final boolean cacheDataInL1) {
    boolean isMetaBlock = buf.getBlockType().getCategory() != BlockCategory.DATA;
    if (isMetaBlock || cacheDataInL1) {
      lruCache.cacheBlock(cacheKey, buf, inMemory, cacheDataInL1);
    } else {
      l2Cache.cacheBlock(cacheKey, buf, inMemory, false);
    }
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false, false);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching,
      boolean repeat, boolean updateCacheMetrics) {
    // TODO: is there a hole here, or just awkwardness since in the lruCache getBlock
    // we end up calling l2Cache.getBlock.
    if (lruCache.containsBlock(cacheKey)) {
      return lruCache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
    }
    Cacheable result = l2Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);

    return result;
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    return lruCache.evictBlock(cacheKey) || l2Cache.evictBlock(cacheKey);
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    return lruCache.evictBlocksByHfileName(hfileName)
        + l2Cache.evictBlocksByHfileName(hfileName);
  }

  @Override
  public CacheStats getStats() {
    return this.combinedCacheStats;
  }

  @Override
  public void shutdown() {
    lruCache.shutdown();
    l2Cache.shutdown();
  }

  @Override
  public long size() {
    return lruCache.size() + l2Cache.size();
  }

  @Override
  public long getFreeSize() {
    return lruCache.getFreeSize() + l2Cache.getFreeSize();
  }

  @Override
  public long getCurrentSize() {
    return lruCache.getCurrentSize() + l2Cache.getCurrentSize();
  }

  @Override
  public long getBlockCount() {
    return lruCache.getBlockCount() + l2Cache.getBlockCount();
  }

  public static class CombinedCacheStats extends CacheStats {
    private final CacheStats lruCacheStats;
    private final CacheStats bucketCacheStats;

    CombinedCacheStats(CacheStats lbcStats, CacheStats fcStats) {
      super("CombinedBlockCache");
      this.lruCacheStats = lbcStats;
      this.bucketCacheStats = fcStats;
    }

    @Override
    public long getRequestCount() {
      return lruCacheStats.getRequestCount()
          + bucketCacheStats.getRequestCount();
    }

    @Override
    public long getRequestCachingCount() {
      return lruCacheStats.getRequestCachingCount()
          + bucketCacheStats.getRequestCachingCount();
    }

    @Override
    public long getMissCount() {
      return lruCacheStats.getMissCount() + bucketCacheStats.getMissCount();
    }

    @Override
    public long getMissCachingCount() {
      return lruCacheStats.getMissCachingCount()
          + bucketCacheStats.getMissCachingCount();
    }

    @Override
    public long getHitCount() {
      return lruCacheStats.getHitCount() + bucketCacheStats.getHitCount();
    }

    @Override
    public long getHitCachingCount() {
      return lruCacheStats.getHitCachingCount()
          + bucketCacheStats.getHitCachingCount();
    }

    @Override
    public long getEvictionCount() {
      return lruCacheStats.getEvictionCount()
          + bucketCacheStats.getEvictionCount();
    }

    @Override
    public long getEvictedCount() {
      return lruCacheStats.getEvictedCount()
          + bucketCacheStats.getEvictedCount();
    }

    @Override
    public void rollMetricsPeriod() {
      lruCacheStats.rollMetricsPeriod();
      bucketCacheStats.rollMetricsPeriod();
    }
    
    @Override
    public long getSumHitCountsPastNPeriods() {
      return lruCacheStats.getSumHitCountsPastNPeriods()
          + bucketCacheStats.getSumHitCountsPastNPeriods();
    }
    
    @Override
    public long getSumRequestCountsPastNPeriods() {
      return lruCacheStats.getSumRequestCountsPastNPeriods()
          + bucketCacheStats.getSumRequestCountsPastNPeriods();
    }
    
    @Override
    public long getSumHitCachingCountsPastNPeriods() {
      return lruCacheStats.getSumHitCachingCountsPastNPeriods()
          + bucketCacheStats.getSumHitCachingCountsPastNPeriods();
    }

    @Override
    public long getSumRequestCachingCountsPastNPeriods() {
      return lruCacheStats.getSumRequestCachingCountsPastNPeriods()
          + bucketCacheStats.getSumRequestCachingCountsPastNPeriods();
    }
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    return new BlockCachesIterator(getBlockCaches());
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return new BlockCache [] {this.lruCache, this.l2Cache};
  }

  @Override
  public void setMaxSize(long size) {
    this.lruCache.setMaxSize(size);
  }
}
