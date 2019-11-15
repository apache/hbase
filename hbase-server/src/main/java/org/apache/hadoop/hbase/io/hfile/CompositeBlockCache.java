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
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class CompositeBlockCache implements BlockCache, HeapSize {
  protected final BlockCache l1Cache;
  protected final BlockCache l2Cache;
  protected final CombinedCacheStats combinedCacheStats;

  public CompositeBlockCache(BlockCache l1Cache, BlockCache l2Cache) {
    this.l1Cache = l1Cache;
    this.l2Cache = l2Cache;
    this.combinedCacheStats = new CombinedCacheStats(l1Cache.getStats(),
        l2Cache.getStats());
  }

  @Override
  public long heapSize() {
    long l1size = 0, l2size = 0;
    if (l1Cache instanceof HeapSize) {
      l1size = ((HeapSize) l1Cache).heapSize();
    }
    if (l2Cache instanceof HeapSize) {
      l2size = ((HeapSize) l2Cache).heapSize();
    }
    return l1size + l2size;
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    boolean metaBlock = buf.getBlockType().getCategory() != BlockCategory.DATA;
    if (metaBlock) {
      l1Cache.cacheBlock(cacheKey, buf, inMemory);
    } else {
      l2Cache.cacheBlock(cacheKey, buf, inMemory);
    }
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching,
      boolean repeat, boolean updateCacheMetrics) {
    // TODO: is there a hole here, or just awkwardness since in the lruCache getBlock
    // we end up calling l2Cache.getBlock.
    // We are not in a position to exactly look at LRU cache or BC as BlockType may not be getting
    // passed always.
    return l1Cache.containsBlock(cacheKey)?
        l1Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics):
        l2Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    return l1Cache.evictBlock(cacheKey) || l2Cache.evictBlock(cacheKey);
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    return l1Cache.evictBlocksByHfileName(hfileName)
        + l2Cache.evictBlocksByHfileName(hfileName);
  }

  @Override
  public CacheStats getStats() {
    return this.combinedCacheStats;
  }

  @Override
  public void shutdown() {
    l1Cache.shutdown();
    l2Cache.shutdown();
  }

  @Override
  public long size() {
    return l1Cache.size() + l2Cache.size();
  }

  @Override
  public long getMaxSize() {
    return l1Cache.getMaxSize() + l2Cache.getMaxSize();
  }

  @Override
  public long getCurrentDataSize() {
    return l1Cache.getCurrentDataSize() + l2Cache.getCurrentDataSize();
  }

  @Override
  public long getFreeSize() {
    return l1Cache.getFreeSize() + l2Cache.getFreeSize();
  }

  @Override
  public long getCurrentSize() {
    return l1Cache.getCurrentSize() + l2Cache.getCurrentSize();
  }

  @Override
  public long getBlockCount() {
    return l1Cache.getBlockCount() + l2Cache.getBlockCount();
  }

  @Override
  public long getDataBlockCount() {
    return l1Cache.getDataBlockCount() + l2Cache.getDataBlockCount();
  }

  public static class CombinedCacheStats extends CacheStats {
    private final CacheStats l1CacheStats;
    private final CacheStats l2CacheStats;

    CombinedCacheStats(CacheStats l1Stats, CacheStats l2Stats) {
      super("CombinedBlockCache");
      this.l1CacheStats = l1Stats;
      this.l2CacheStats = l2Stats;
    }

    public CacheStats getL1CacheStats() {
      return this.l1CacheStats;
    }

    public CacheStats getL2CacheStats() {
      return this.l2CacheStats;
    }

    public long getDataMissCount() {
      return l1CacheStats.getDataMissCount() + l2CacheStats.getDataMissCount();
    }

    @Override
    public long getLeafIndexMissCount() {
      return l1CacheStats.getLeafIndexMissCount() + l2CacheStats.getLeafIndexMissCount();
    }

    @Override
    public long getBloomChunkMissCount() {
      return l1CacheStats.getBloomChunkMissCount() + l2CacheStats.getBloomChunkMissCount();
    }

    @Override
    public long getMetaMissCount() {
      return l1CacheStats.getMetaMissCount() + l2CacheStats.getMetaMissCount();
    }

    @Override
    public long getRootIndexMissCount() {
      return l1CacheStats.getRootIndexMissCount() + l2CacheStats.getRootIndexMissCount();
    }

    @Override
    public long getIntermediateIndexMissCount() {
      return l1CacheStats.getIntermediateIndexMissCount() +
          l2CacheStats.getIntermediateIndexMissCount();
    }

    @Override
    public long getFileInfoMissCount() {
      return l1CacheStats.getFileInfoMissCount() + l2CacheStats.getFileInfoMissCount();
    }

    @Override
    public long getGeneralBloomMetaMissCount() {
      return l1CacheStats.getGeneralBloomMetaMissCount() +
          l2CacheStats.getGeneralBloomMetaMissCount();
    }

    @Override
    public long getDeleteFamilyBloomMissCount() {
      return l1CacheStats.getDeleteFamilyBloomMissCount() +
          l2CacheStats.getDeleteFamilyBloomMissCount();
    }

    @Override
    public long getTrailerMissCount() {
      return l1CacheStats.getTrailerMissCount() + l2CacheStats.getTrailerMissCount();
    }

    @Override
    public long getDataHitCount() {
      return l1CacheStats.getDataHitCount() + l2CacheStats.getDataHitCount();
    }

    @Override
    public long getLeafIndexHitCount() {
      return l1CacheStats.getLeafIndexHitCount() + l2CacheStats.getLeafIndexHitCount();
    }

    @Override
    public long getBloomChunkHitCount() {
      return l1CacheStats.getBloomChunkHitCount() + l2CacheStats.getBloomChunkHitCount();
    }

    @Override
    public long getMetaHitCount() {
      return l1CacheStats.getMetaHitCount() + l2CacheStats.getMetaHitCount();
    }

    @Override
    public long getRootIndexHitCount() {
      return l1CacheStats.getRootIndexHitCount() + l2CacheStats.getRootIndexHitCount();
    }

    @Override
    public long getIntermediateIndexHitCount() {
      return l1CacheStats.getIntermediateIndexHitCount() +
          l2CacheStats.getIntermediateIndexHitCount();
    }

    @Override
    public long getFileInfoHitCount() {
      return l1CacheStats.getFileInfoHitCount() + l2CacheStats.getFileInfoHitCount();
    }

    @Override
    public long getGeneralBloomMetaHitCount() {
      return l1CacheStats.getGeneralBloomMetaHitCount() +
          l2CacheStats.getGeneralBloomMetaHitCount();
    }

    @Override
    public long getDeleteFamilyBloomHitCount() {
      return l1CacheStats.getDeleteFamilyBloomHitCount() +
          l2CacheStats.getDeleteFamilyBloomHitCount();
    }

    @Override
    public long getTrailerHitCount() {
      return l1CacheStats.getTrailerHitCount() + l2CacheStats.getTrailerHitCount();
    }

    @Override
    public long getRequestCount() {
      return l1CacheStats.getRequestCount()
          + l2CacheStats.getRequestCount();
    }

    @Override
    public long getRequestCachingCount() {
      return l1CacheStats.getRequestCachingCount()
          + l2CacheStats.getRequestCachingCount();
    }

    @Override
    public long getMissCount() {
      return l1CacheStats.getMissCount() + l2CacheStats.getMissCount();
    }

    @Override
    public long getPrimaryMissCount() {
      return l1CacheStats.getPrimaryMissCount() + l2CacheStats.getPrimaryMissCount();
    }

    @Override
    public long getMissCachingCount() {
      return l1CacheStats.getMissCachingCount()
          + l2CacheStats.getMissCachingCount();
    }

    @Override
    public long getHitCount() {
      return l1CacheStats.getHitCount() + l2CacheStats.getHitCount();
    }

    @Override
    public long getPrimaryHitCount() {
      return l1CacheStats.getPrimaryHitCount() + l2CacheStats.getPrimaryHitCount();
    }

    @Override
    public long getHitCachingCount() {
      return l1CacheStats.getHitCachingCount()
          + l2CacheStats.getHitCachingCount();
    }

    @Override
    public long getEvictionCount() {
      return l1CacheStats.getEvictionCount()
          + l2CacheStats.getEvictionCount();
    }

    @Override
    public long getEvictedCount() {
      return l1CacheStats.getEvictedCount()
          + l2CacheStats.getEvictedCount();
    }

    @Override
    public long getPrimaryEvictedCount() {
      return l1CacheStats.getPrimaryEvictedCount()
          + l2CacheStats.getPrimaryEvictedCount();
    }

    @Override
    public void rollMetricsPeriod() {
      l1CacheStats.rollMetricsPeriod();
      l2CacheStats.rollMetricsPeriod();
    }

    @Override
    public long getFailedInserts() {
      return l1CacheStats.getFailedInserts() + l2CacheStats.getFailedInserts();
    }

    @Override
    public long getSumHitCountsPastNPeriods() {
      return l1CacheStats.getSumHitCountsPastNPeriods()
          + l2CacheStats.getSumHitCountsPastNPeriods();
    }

    @Override
    public long getSumRequestCountsPastNPeriods() {
      return l1CacheStats.getSumRequestCountsPastNPeriods()
          + l2CacheStats.getSumRequestCountsPastNPeriods();
    }

    @Override
    public long getSumHitCachingCountsPastNPeriods() {
      return l1CacheStats.getSumHitCachingCountsPastNPeriods()
          + l2CacheStats.getSumHitCachingCountsPastNPeriods();
    }

    @Override
    public long getSumRequestCachingCountsPastNPeriods() {
      return l1CacheStats.getSumRequestCachingCountsPastNPeriods()
          + l2CacheStats.getSumRequestCachingCountsPastNPeriods();
    }
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    return new BlockCachesIterator(getBlockCaches());
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return new BlockCache[] {this.l1Cache, this.l2Cache};
  }

  @VisibleForTesting
  public int getRpcRefCount(BlockCacheKey cacheKey) {
    return (this.l2Cache instanceof BucketCache)
        ? ((BucketCache) this.l2Cache).getRpcRefCount(cacheKey)
        : 0;
  }

  @Override
  public boolean containsBlock(BlockCacheKey cacheKey) {
    return this.l1Cache.containsBlock(cacheKey) || this.l2Cache.containsBlock(cacheKey);
  }
}
