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

import com.google.common.annotations.VisibleForTesting;


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
    boolean metaBlock = buf.getBlockType().getCategory() != BlockCategory.DATA;
    if (metaBlock || cacheDataInL1) {
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
    return lruCache.containsBlock(cacheKey)?
        lruCache.getBlock(cacheKey, caching, repeat, updateCacheMetrics):
        l2Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
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
    public long getDataMissCount() {
      return lruCacheStats.getDataMissCount() + bucketCacheStats.getDataMissCount();
    }

    @Override
    public long getLeafIndexMissCount() {
      return lruCacheStats.getLeafIndexMissCount() + bucketCacheStats.getLeafIndexMissCount();
    }

    @Override
    public long getBloomChunkMissCount() {
      return lruCacheStats.getBloomChunkMissCount() + bucketCacheStats.getBloomChunkMissCount();
    }

    @Override
    public long getMetaMissCount() {
      return lruCacheStats.getMetaMissCount() + bucketCacheStats.getMetaMissCount();
    }

    @Override
    public long getRootIndexMissCount() {
      return lruCacheStats.getRootIndexMissCount() + bucketCacheStats.getRootIndexMissCount();
    }

    @Override
    public long getIntermediateIndexMissCount() {
      return lruCacheStats.getIntermediateIndexMissCount() +
          bucketCacheStats.getIntermediateIndexMissCount();
    }

    @Override
    public long getFileInfoMissCount() {
      return lruCacheStats.getFileInfoMissCount() + bucketCacheStats.getFileInfoMissCount();
    }

    @Override
    public long getGeneralBloomMetaMissCount() {
      return lruCacheStats.getGeneralBloomMetaMissCount() +
          bucketCacheStats.getGeneralBloomMetaMissCount();
    }

    @Override
    public long getDeleteFamilyBloomMissCount() {
      return lruCacheStats.getDeleteFamilyBloomMissCount() +
          bucketCacheStats.getDeleteFamilyBloomMissCount();
    }

    @Override
    public long getTrailerMissCount() {
      return lruCacheStats.getTrailerMissCount() + bucketCacheStats.getTrailerMissCount();
    }

    @Override
    public long getDataHitCount() {
      return lruCacheStats.getDataHitCount() + bucketCacheStats.getDataHitCount();
    }

    @Override
    public long getLeafIndexHitCount() {
      return lruCacheStats.getLeafIndexHitCount() + bucketCacheStats.getLeafIndexHitCount();
    }

    @Override
    public long getBloomChunkHitCount() {
      return lruCacheStats.getBloomChunkHitCount() + bucketCacheStats.getBloomChunkHitCount();
    }

    @Override
    public long getMetaHitCount() {
      return lruCacheStats.getMetaHitCount() + bucketCacheStats.getMetaHitCount();
    }

    @Override
    public long getRootIndexHitCount() {
      return lruCacheStats.getRootIndexHitCount() + bucketCacheStats.getRootIndexHitCount();
    }

    @Override
    public long getIntermediateIndexHitCount() {
      return lruCacheStats.getIntermediateIndexHitCount() +
          bucketCacheStats.getIntermediateIndexHitCount();
    }

    @Override
    public long getFileInfoHitCount() {
      return lruCacheStats.getFileInfoHitCount() + bucketCacheStats.getFileInfoHitCount();
    }

    @Override
    public long getGeneralBloomMetaHitCount() {
      return lruCacheStats.getGeneralBloomMetaHitCount() +
          bucketCacheStats.getGeneralBloomMetaHitCount();
    }

    @Override
    public long getDeleteFamilyBloomHitCount() {
      return lruCacheStats.getDeleteFamilyBloomHitCount() +
          bucketCacheStats.getDeleteFamilyBloomHitCount();
    }

    @Override
    public long getTrailerHitCount() {
      return lruCacheStats.getTrailerHitCount() + bucketCacheStats.getTrailerHitCount();
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
    public long getPrimaryMissCount() {
      return lruCacheStats.getPrimaryMissCount() + bucketCacheStats.getPrimaryMissCount();
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
    public long getPrimaryHitCount() {
      return lruCacheStats.getPrimaryHitCount() + bucketCacheStats.getPrimaryHitCount();
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
    public long getPrimaryEvictedCount() {
      return lruCacheStats.getPrimaryEvictedCount()
          + bucketCacheStats.getPrimaryEvictedCount();
    }

    @Override
    public void rollMetricsPeriod() {
      lruCacheStats.rollMetricsPeriod();
      bucketCacheStats.rollMetricsPeriod();
    }
    
    @Override
    public long getFailedInserts() {
      return lruCacheStats.getFailedInserts() + bucketCacheStats.getFailedInserts();
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

  @Override
  public void returnBlock(BlockCacheKey cacheKey, Cacheable block) {
    // returnBlock is meaningful for L2 cache alone.
    this.l2Cache.returnBlock(cacheKey, block);
  }

  @VisibleForTesting
  public int getRefCount(BlockCacheKey cacheKey) {
    return ((BucketCache) this.l2Cache).getRefCount(cacheKey);
  }
}
