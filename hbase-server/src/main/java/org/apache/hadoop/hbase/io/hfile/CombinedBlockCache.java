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
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CombinedBlockCache is an abstraction layer that combines {@link FirstLevelBlockCache} and
 * {@link BucketCache}. The smaller lruCache is used to cache bloom blocks and index blocks. The
 * larger Cache is used to cache data blocks.
 * {@link #getBlock(BlockCacheKey, boolean, boolean, boolean)} reads first from the smaller l1Cache
 * before looking for the block in the l2Cache. Blocks evicted from l1Cache are put into the bucket
 * cache. Metrics are the combined size and hits and misses of both caches.
 */
@InterfaceAudience.Private
public class CombinedBlockCache implements ResizableBlockCache, HeapSize {
  protected final FirstLevelBlockCache l1Cache;
  protected final BlockCache l2Cache;
  protected final CombinedCacheStats combinedCacheStats;

  private static final Logger LOG = LoggerFactory.getLogger(CombinedBlockCache.class);

  public CombinedBlockCache(FirstLevelBlockCache l1Cache, BlockCache l2Cache) {
    this.l1Cache = l1Cache;
    this.l2Cache = l2Cache;
    this.combinedCacheStats = new CombinedCacheStats(l1Cache.getStats(), l2Cache.getStats());
  }

  @Override
  public long heapSize() {
    long l2size = 0;
    if (l2Cache instanceof HeapSize) {
      l2size = ((HeapSize) l2Cache).heapSize();
    }
    return l1Cache.heapSize() + l2size;
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    cacheBlock(cacheKey, buf, inMemory, false);
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
    boolean waitWhenCache) {
    boolean metaBlock = isMetaBlock(buf.getBlockType());
    if (metaBlock) {
      l1Cache.cacheBlock(cacheKey, buf, inMemory);
    } else {
      l2Cache.cacheBlock(cacheKey, buf, inMemory, waitWhenCache);
    }
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    Cacheable block = null;
    // We don't know the block type. We should try to get it on one of the caches only,
    // but not both otherwise we'll over compute on misses. Here we check if the key is on L1,
    // if so, call getBlock on L1 and that will compute the hit. Otherwise, we'll try to get it from
    // L2 and whatever happens, we'll update the stats there.
    boolean existInL1 = l1Cache.containsBlock(cacheKey);
    // if we know it's in L1, just delegate call to l1 and return it
    if (existInL1) {
      block = l1Cache.getBlock(cacheKey, caching, repeat, false);
    } else {
      block = l2Cache.getBlock(cacheKey, caching, repeat, false);
    }
    if (updateCacheMetrics) {
      boolean metaBlock = isMetaBlock(cacheKey.getBlockType());
      if (metaBlock) {
        if (!existInL1 && block != null) {
          LOG.warn("Cache key {} had block type {}, but was found in L2 cache.", cacheKey,
            cacheKey.getBlockType());
          updateBlockMetrics(block, cacheKey, l2Cache, caching);
        } else {
          updateBlockMetrics(block, cacheKey, l1Cache, caching);
        }
      } else {
        if (existInL1) {
          updateBlockMetrics(block, cacheKey, l1Cache, caching);
        } else {
          updateBlockMetrics(block, cacheKey, l2Cache, caching);
        }
      }
    }
    return block;
  }

  private void updateBlockMetrics(Cacheable block, BlockCacheKey key, BlockCache cache,
    boolean caching) {
    if (block == null) {
      cache.getStats().miss(caching, key.isPrimary(), key.getBlockType());
    } else {
      cache.getStats().hit(caching, key.isPrimary(), key.getBlockType());

    }
  }

  private Cacheable getBlockWithType(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    boolean metaBlock = isMetaBlock(cacheKey.getBlockType());
    if (metaBlock) {
      return l1Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
    } else {
      return l2Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
    }
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    return l1Cache.evictBlock(cacheKey) || l2Cache.evictBlock(cacheKey);
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    return l1Cache.evictBlocksByHfileName(hfileName) + l2Cache.evictBlocksByHfileName(hfileName);
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
    private final CacheStats lruCacheStats;
    private final CacheStats bucketCacheStats;

    CombinedCacheStats(CacheStats lbcStats, CacheStats fcStats) {
      super("CombinedBlockCache");
      this.lruCacheStats = lbcStats;
      this.bucketCacheStats = fcStats;
    }

    public CacheStats getLruCacheStats() {
      return this.lruCacheStats;
    }

    public CacheStats getBucketCacheStats() {
      return this.bucketCacheStats;
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
      return lruCacheStats.getIntermediateIndexMissCount()
        + bucketCacheStats.getIntermediateIndexMissCount();
    }

    @Override
    public long getFileInfoMissCount() {
      return lruCacheStats.getFileInfoMissCount() + bucketCacheStats.getFileInfoMissCount();
    }

    @Override
    public long getGeneralBloomMetaMissCount() {
      return lruCacheStats.getGeneralBloomMetaMissCount()
        + bucketCacheStats.getGeneralBloomMetaMissCount();
    }

    @Override
    public long getDeleteFamilyBloomMissCount() {
      return lruCacheStats.getDeleteFamilyBloomMissCount()
        + bucketCacheStats.getDeleteFamilyBloomMissCount();
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
      return lruCacheStats.getIntermediateIndexHitCount()
        + bucketCacheStats.getIntermediateIndexHitCount();
    }

    @Override
    public long getFileInfoHitCount() {
      return lruCacheStats.getFileInfoHitCount() + bucketCacheStats.getFileInfoHitCount();
    }

    @Override
    public long getGeneralBloomMetaHitCount() {
      return lruCacheStats.getGeneralBloomMetaHitCount()
        + bucketCacheStats.getGeneralBloomMetaHitCount();
    }

    @Override
    public long getDeleteFamilyBloomHitCount() {
      return lruCacheStats.getDeleteFamilyBloomHitCount()
        + bucketCacheStats.getDeleteFamilyBloomHitCount();
    }

    @Override
    public long getTrailerHitCount() {
      return lruCacheStats.getTrailerHitCount() + bucketCacheStats.getTrailerHitCount();
    }

    @Override
    public long getRequestCount() {
      return lruCacheStats.getRequestCount() + bucketCacheStats.getRequestCount();
    }

    @Override
    public long getRequestCachingCount() {
      return lruCacheStats.getRequestCachingCount() + bucketCacheStats.getRequestCachingCount();
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
      return lruCacheStats.getMissCachingCount() + bucketCacheStats.getMissCachingCount();
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
      return lruCacheStats.getHitCachingCount() + bucketCacheStats.getHitCachingCount();
    }

    @Override
    public long getEvictionCount() {
      return lruCacheStats.getEvictionCount() + bucketCacheStats.getEvictionCount();
    }

    @Override
    public long getEvictedCount() {
      return lruCacheStats.getEvictedCount() + bucketCacheStats.getEvictedCount();
    }

    @Override
    public long getPrimaryEvictedCount() {
      return lruCacheStats.getPrimaryEvictedCount() + bucketCacheStats.getPrimaryEvictedCount();
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
    return new BlockCache[] { this.l1Cache, this.l2Cache };
  }

  /**
   * Returns the list of fully cached files
   */
  @Override
  public Optional<Map<String, Pair<String, Long>>> getFullyCachedFiles() {
    return this.l2Cache.getFullyCachedFiles();
  }

  @Override
  public Optional<Map<String, Long>> getRegionCachedInfo() {
    return l2Cache.getRegionCachedInfo();
  }

  @Override
  public void setMaxSize(long size) {
    this.l1Cache.setMaxSize(size);
  }

  public int getRpcRefCount(BlockCacheKey cacheKey) {
    return (this.l2Cache instanceof BucketCache)
      ? ((BucketCache) this.l2Cache).getRpcRefCount(cacheKey)
      : 0;
  }

  public FirstLevelBlockCache getFirstLevelCache() {
    return l1Cache;
  }

  public BlockCache getSecondLevelCache() {
    return l2Cache;
  }

  @Override
  public void notifyFileCachingCompleted(Path fileName, int totalBlockCount, int dataBlockCount,
    long size) {
    l1Cache.getBlockCount();
    l1Cache.notifyFileCachingCompleted(fileName, totalBlockCount, dataBlockCount, size);
    l2Cache.notifyFileCachingCompleted(fileName, totalBlockCount, dataBlockCount, size);

  }

  @Override
  public void onConfigurationChange(Configuration config) {
    l1Cache.onConfigurationChange(config);
    l2Cache.onConfigurationChange(config);
  }

  @Override
  public Optional<Boolean> blockFitsIntoTheCache(HFileBlock block) {
    if (isMetaBlock(block.getBlockType())) {
      return l1Cache.blockFitsIntoTheCache(block);
    } else {
      return l2Cache.blockFitsIntoTheCache(block);
    }
  }

  @Override
  public Optional<Boolean> shouldCacheFile(String fileName) {
    Optional<Boolean> l1Result = l1Cache.shouldCacheFile(fileName);
    Optional<Boolean> l2Result = l2Cache.shouldCacheFile(fileName);
    final Mutable<Boolean> combinedResult = new MutableBoolean(true);
    l1Result.ifPresent(b -> combinedResult.setValue(b && combinedResult.getValue()));
    l2Result.ifPresent(b -> combinedResult.setValue(b && combinedResult.getValue()));
    return Optional.of(combinedResult.getValue());
  }

  @Override
  public Optional<Boolean> isAlreadyCached(BlockCacheKey key) {
    boolean result =
      l1Cache.isAlreadyCached(key).orElseGet(() -> l2Cache.isAlreadyCached(key).orElse(false));
    return Optional.of(result);
  }

  @Override
  public Optional<Integer> getBlockSize(BlockCacheKey key) {
    Optional<Integer> l1Result = l1Cache.getBlockSize(key);
    return l1Result.isPresent() ? l1Result : l2Cache.getBlockSize(key);
  }

  @Override
  public int evictBlocksRangeByHfileName(String hfileName, long initOffset, long endOffset) {
    return l1Cache.evictBlocksRangeByHfileName(hfileName, initOffset, endOffset)
      + l2Cache.evictBlocksRangeByHfileName(hfileName, initOffset, endOffset);
  }

  @Override
  public boolean waitForCacheInitialization(long timeout) {
    return this.l1Cache.waitForCacheInitialization(timeout)
      && this.l2Cache.waitForCacheInitialization(timeout);
  }

  @Override
  public boolean isCacheEnabled() {
    return l1Cache.isCacheEnabled() && l2Cache.isCacheEnabled();
  }
}
