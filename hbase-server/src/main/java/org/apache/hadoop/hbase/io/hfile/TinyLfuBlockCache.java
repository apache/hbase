/**
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

import static java.util.Objects.requireNonNull;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.util.StringUtils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy.Eviction;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A block cache that is memory-aware using {@link HeapSize}, memory bounded using the W-TinyLFU
 * eviction algorithm, and concurrent. This implementation delegates to a Caffeine cache to provide
 * O(1) read and write operations.
 * <ul>
 *   <li>W-TinyLFU: http://arxiv.org/pdf/1512.00727.pdf</li>
 *   <li>Caffeine: https://github.com/ben-manes/caffeine</li>
 *   <li>Cache design: http://highscalability.com/blog/2016/1/25/design-of-a-modern-cache.html</li>
 * </ul>
 */
@InterfaceAudience.Private
public final class TinyLfuBlockCache implements FirstLevelBlockCache {
  private static final Log LOG = LogFactory.getLog(TinyLfuBlockCache.class);

  private static final String MAX_BLOCK_SIZE = "hbase.tinylfu.max.block.size";
  private static final long DEFAULT_MAX_BLOCK_SIZE = 16L * 1024L * 1024L;
  private static final int STAT_THREAD_PERIOD_SECONDS = 5 * 60;

  private final Eviction<BlockCacheKey, Cacheable> policy;
  private final ScheduledExecutorService statsThreadPool;
  private final long maxBlockSize;
  private final CacheStats stats;

  private BlockCache victimCache;

  @VisibleForTesting
  final Cache<BlockCacheKey, Cacheable> cache;

  /**
   * Creates a block cache.
   *
   * @param maximumSizeInBytes maximum size of this cache, in bytes
   * @param avgBlockSize expected average size of blocks, in bytes
   * @param executor the cache's executor
   * @param conf additional configuration
   */
  public TinyLfuBlockCache(long maximumSizeInBytes, long avgBlockSize,
      Executor executor, Configuration conf) {
    this(maximumSizeInBytes, avgBlockSize,
        conf.getLong(MAX_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE), executor);
  }

  /**
   * Creates a block cache.
   *
   * @param maximumSizeInBytes maximum size of this cache, in bytes
   * @param avgBlockSize expected average size of blocks, in bytes
   * @param maxBlockSize maximum size of a block, in bytes
   * @param executor the cache's executor
   */
  public TinyLfuBlockCache(long maximumSizeInBytes,
      long avgBlockSize, long maxBlockSize, Executor executor) {
    this.cache = Caffeine.newBuilder()
        .executor(executor)
        .maximumWeight(maximumSizeInBytes)
        .removalListener(new EvictionListener())
        .weigher((BlockCacheKey key, Cacheable value) ->
            (int) Math.min(value.heapSize(), Integer.MAX_VALUE))
        .initialCapacity((int) Math.ceil((1.2 * maximumSizeInBytes) / avgBlockSize))
        .build();
    this.maxBlockSize = maxBlockSize;
    this.policy = cache.policy().eviction().get();
    this.stats = new CacheStats(getClass().getSimpleName());

    statsThreadPool = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
        .setNameFormat("TinyLfuBlockCacheStatsExecutor").setDaemon(true).build());
    statsThreadPool.scheduleAtFixedRate(this::logStats,
        STAT_THREAD_PERIOD_SECONDS, STAT_THREAD_PERIOD_SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public void setVictimCache(BlockCache victimCache) {
    if (this.victimCache != null) {
      throw new IllegalArgumentException("The victim cache has already been set");
    }
    this.victimCache = requireNonNull(victimCache);
  }

  @Override
  public long size() {
    return policy.getMaximum();
  }

  @Override
  public long getFreeSize() {
    return size() - getCurrentSize();
  }

  @Override
  public long getCurrentSize() {
    return policy.weightedSize().getAsLong();
  }

  @Override
  public long getBlockCount() {
    return cache.estimatedSize();
  }

  @Override
  public long heapSize() {
    return getCurrentSize();
  }

  @Override
  public void setMaxSize(long size) {
    policy.setMaximum(size);
  }

  @Override
  public boolean containsBlock(BlockCacheKey cacheKey) {
    return cache.asMap().containsKey(cacheKey);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey,
      boolean caching, boolean repeat, boolean updateCacheMetrics) {
    Cacheable value = cache.getIfPresent(cacheKey);
    if (value == null) {
      if (repeat) {
        return null;
      }
      if (updateCacheMetrics) {
        stats.miss(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
      }
      if (victimCache != null) {
        value = victimCache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
        if ((value != null) && caching) {
          if ((value instanceof HFileBlock) && ((HFileBlock) value).usesSharedMemory()) {
            value = ((HFileBlock) value).deepClone();
          }
          cacheBlock(cacheKey, value);
        }
      }
    } else if (updateCacheMetrics) {
      stats.hit(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
    }
    return value;
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable value,
      boolean inMemory, boolean cacheDataInL1) {
    cacheBlock(cacheKey, value);
  }

  @Override
  public void cacheBlock(BlockCacheKey key, Cacheable value) {
    if (value.heapSize() > maxBlockSize) {
      // If there are a lot of blocks that are too big this can make the logs too noisy (2% logged)
      if (stats.failInsert() % 50 == 0) {
        LOG.warn(String.format(
            "Trying to cache too large a block %s @ %,d is %,d which is larger than %,d",
            key.getHfileName(), key.getOffset(), value.heapSize(), DEFAULT_MAX_BLOCK_SIZE));
      }
    } else {
      cache.put(key, value);
    }
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    Cacheable value = cache.asMap().remove(cacheKey);
    return (value != null);
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    int evicted = 0;
    for (BlockCacheKey key : cache.asMap().keySet()) {
      if (key.getHfileName().equals(hfileName) && evictBlock(key)) {
        evicted++;
      }
    }
    if (victimCache != null) {
      evicted += victimCache.evictBlocksByHfileName(hfileName);
    }
    return evicted;
  }

  @Override
  public CacheStats getStats() {
    return stats;
  }

  @Override
  public void shutdown() {
    if (victimCache != null) {
      victimCache.shutdown();
    }
    statsThreadPool.shutdown();
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return null;
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    long now = System.nanoTime();
    return cache.asMap().entrySet().stream()
        .map(entry -> (CachedBlock) new CachedBlockView(entry.getKey(), entry.getValue(), now))
        .iterator();
  }

  @Override
  public void returnBlock(BlockCacheKey cacheKey, Cacheable block) {
    // There is no SHARED type here in L1. But the block might have been served from the L2 victim
    // cache (when the Combined mode = false). So just try return this block to the victim cache.
    // Note : In case of CombinedBlockCache we will have this victim cache configured for L1
    // cache. But CombinedBlockCache will only call returnBlock on L2 cache.
    if (victimCache != null) {
      victimCache.returnBlock(cacheKey, block);
    }
  }

  private void logStats() {
    LOG.info(
        "totalSize=" + StringUtils.byteDesc(heapSize()) + ", " +
        "freeSize=" + StringUtils.byteDesc(getFreeSize()) + ", " +
        "max=" + StringUtils.byteDesc(size()) + ", " +
        "blockCount=" + getBlockCount() + ", " +
        "accesses=" + stats.getRequestCount() + ", " +
        "hits=" + stats.getHitCount() + ", " +
        "hitRatio=" + (stats.getHitCount() == 0 ?
          "0," : StringUtils.formatPercent(stats.getHitRatio(), 2) + ", ") +
        "cachingAccesses=" + stats.getRequestCachingCount() + ", " +
        "cachingHits=" + stats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" + (stats.getHitCachingCount() == 0 ?
          "0,": (StringUtils.formatPercent(stats.getHitCachingRatio(), 2) + ", ")) +
        "evictions=" + stats.getEvictionCount() + ", " +
        "evicted=" + stats.getEvictedCount());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("blockCount", getBlockCount())
      .add("currentSize", getCurrentSize())
      .add("freeSize", getFreeSize())
      .add("maxSize", size())
      .add("heapSize", heapSize())
      .add("victimCache", (victimCache != null))
      .toString();
  }

  /** A removal listener to asynchronously record evictions and populate the victim cache. */
  private final class EvictionListener implements RemovalListener<BlockCacheKey, Cacheable> {

    @Override
    public void onRemoval(BlockCacheKey key, Cacheable value, RemovalCause cause) {
      if (!cause.wasEvicted()) {
        // An explicit eviction (invalidation) is not added to the victim cache as the data may
        // no longer be valid for subsequent queries.
        return;
      }

      recordEviction();

      if (victimCache == null) {
        return;
      } else if (victimCache instanceof BucketCache) {
        BucketCache victimBucketCache = (BucketCache) victimCache;
        victimBucketCache.cacheBlockWithWait(key, value, /* inMemory */ true, /* wait */ true);
      } else {
        victimCache.cacheBlock(key, value);
      }
    }
  }

  /**
   * Records an eviction. The number of eviction operations and evicted blocks are identical, as
   * an eviction is triggered immediately when the capacity has been exceeded. An eviction is
   * performed asynchronously. See the library's documentation for details on write buffers,
   * batching, and maintenance behavior.
   */
  private void recordEviction() {
    // FIXME: Currently does not capture the insertion time
    stats.evicted(Long.MAX_VALUE, true);
    stats.evict();
  }

  private static final class CachedBlockView implements CachedBlock {
    private static final Comparator<CachedBlock> COMPARATOR = Comparator
        .comparing(CachedBlock::getFilename)
        .thenComparing(CachedBlock::getOffset)
        .thenComparing(CachedBlock::getCachedTime);

    private final BlockCacheKey key;
    private final Cacheable value;
    private final long now;

    public CachedBlockView(BlockCacheKey key, Cacheable value, long now) {
      this.now = now;
      this.key = key;
      this.value = value;
    }

    @Override
    public BlockPriority getBlockPriority() {
      // This does not appear to be used in any meaningful way and is irrelevant to this cache
      return BlockPriority.MEMORY;
    }

    @Override
    public BlockType getBlockType() {
      return value.getBlockType();
    }

    @Override
    public long getOffset() {
      return key.getOffset();
    }

    @Override
    public long getSize() {
      return value.heapSize();
    }

    @Override
    public long getCachedTime() {
      // This does not appear to be used in any meaningful way, so not captured
      return 0L;
    }

    @Override
    public String getFilename() {
      return key.getHfileName();
    }

    @Override
    public int compareTo(CachedBlock other) {
      return COMPARATOR.compare(this, other);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof CachedBlock)) {
        return false;
      }
      CachedBlock other = (CachedBlock) obj;
      return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }

    @Override
    public String toString() {
      return BlockCacheUtil.toString(this, now);
    }
  }
}
