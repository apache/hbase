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

package org.apache.hadoop.hbase.io.hfile.slab;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockPriority;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.BlockCacheUtil;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * SlabCache is composed of multiple SingleSizeCaches. It uses a TreeMap in
 * order to determine where a given element fits. Redirects gets and puts to the
 * correct SingleSizeCache.
 *
 * @deprecated As of 1.0, replaced by {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache}.
 */
@InterfaceAudience.Private
@Deprecated
public class SlabCache implements SlabItemActionWatcher, BlockCache, HeapSize {

  private final ConcurrentHashMap<BlockCacheKey, SingleSizeCache> backingStore;
  private final TreeMap<Integer, SingleSizeCache> slabs;
  static final Log LOG = LogFactory.getLog(SlabCache.class);
  static final int STAT_THREAD_PERIOD_SECS = 60 * 5;

  private final ScheduledExecutorService scheduleThreadPool = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Slab Statistics #%d").build());

  long size;
  private final CacheStats stats;
  final SlabStats requestStats;
  final SlabStats successfullyCachedStats;
  private final long avgBlockSize;
  private static final long CACHE_FIXED_OVERHEAD = ClassSize.estimateBase(
      SlabCache.class, false);

  /**
   * Default constructor, creates an empty SlabCache.
   *
   * @param size Total size allocated to the SlabCache. (Bytes)
   * @param avgBlockSize Average size of a block being cached.
   **/

  public SlabCache(long size, long avgBlockSize) {
    this.avgBlockSize = avgBlockSize;
    this.size = size;
    this.stats = new CacheStats();
    this.requestStats = new SlabStats();
    this.successfullyCachedStats = new SlabStats();

    backingStore = new ConcurrentHashMap<BlockCacheKey, SingleSizeCache>();
    slabs = new TreeMap<Integer, SingleSizeCache>();
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        STAT_THREAD_PERIOD_SECS, STAT_THREAD_PERIOD_SECS, TimeUnit.SECONDS);
  }

  public Map<Integer, SingleSizeCache> getSizer() {
    return slabs;
  }

  /**
   * A way of allocating the desired amount of Slabs of each particular size.
   *
   * This reads two lists from conf, hbase.offheap.slab.proportions and
   * hbase.offheap.slab.sizes.
   *
   * The first list is the percentage of our total space we allocate to the
   * slabs.
   *
   * The second list is blocksize of the slabs in bytes. (E.g. the slab holds
   * blocks of this size).
   *
   * @param conf Configuration file.
   */
  public void addSlabByConf(Configuration conf) {
    // Proportions we allocate to each slab of the total size.
    String[] porportions = conf.getStrings(
        "hbase.offheapcache.slab.proportions", "0.80", "0.20");
    String[] sizes = conf.getStrings("hbase.offheapcache.slab.sizes",
        Long.valueOf(avgBlockSize * 11 / 10).toString(),
        Long.valueOf(avgBlockSize * 21 / 10).toString());

    if (porportions.length != sizes.length) {
      throw new IllegalArgumentException(
          "SlabCache conf not "
              + "initialized, error in configuration. hbase.offheap.slab.proportions specifies "
              + porportions.length
              + " slabs while hbase.offheap.slab.sizes specifies "
              + sizes.length + " slabs "
              + "offheapslabporportions and offheapslabsizes");
    }
    /*
     * We use BigDecimals instead of floats because float rounding is annoying
     */

    BigDecimal[] parsedProportions = stringArrayToBigDecimalArray(porportions);
    BigDecimal[] parsedSizes = stringArrayToBigDecimalArray(sizes);

    BigDecimal sumProportions = new BigDecimal(0);
    for (BigDecimal b : parsedProportions) {
      /* Make sure all proportions are greater than 0 */
      Preconditions
          .checkArgument(b.compareTo(BigDecimal.ZERO) == 1,
              "Proportions in hbase.offheap.slab.proportions must be greater than 0!");
      sumProportions = sumProportions.add(b);
    }

    /* If the sum is greater than 1 */
    Preconditions
        .checkArgument(sumProportions.compareTo(BigDecimal.ONE) != 1,
            "Sum of all proportions in hbase.offheap.slab.proportions must be less than 1");

    /* If the sum of all proportions is less than 0.99 */
    if (sumProportions.compareTo(new BigDecimal("0.99")) == -1) {
      LOG.warn("Sum of hbase.offheap.slab.proportions is less than 0.99! Memory is being wasted");
    }
    for (int i = 0; i < parsedProportions.length; i++) {
      int blockSize = parsedSizes[i].intValue();
      int numBlocks = new BigDecimal(this.size).multiply(parsedProportions[i])
          .divide(parsedSizes[i], BigDecimal.ROUND_DOWN).intValue();
      addSlab(blockSize, numBlocks);
    }
  }

  /**
   * Gets the size of the slab cache a ByteBuffer of this size would be
   * allocated to.
   *
   * @param size Size of the ByteBuffer we are checking.
   *
   * @return the Slab that the above bytebuffer would be allocated towards. If
   *         object is too large, returns null.
   */
  Entry<Integer, SingleSizeCache> getHigherBlock(int size) {
    return slabs.higherEntry(size - 1);
  }

  private BigDecimal[] stringArrayToBigDecimalArray(String[] parsee) {
    BigDecimal[] parsed = new BigDecimal[parsee.length];
    for (int i = 0; i < parsee.length; i++) {
      parsed[i] = new BigDecimal(parsee[i].trim());
    }
    return parsed;
  }

  private void addSlab(int blockSize, int numBlocks) {
    LOG.info("Creating a slab of blockSize " + blockSize + " with " + numBlocks
        + " blocks, " + StringUtils.humanReadableInt(blockSize * (long) numBlocks) + "bytes.");
    slabs.put(blockSize, new SingleSizeCache(blockSize, numBlocks, this));
  }

  /**
   * Cache the block with the specified key and buffer. First finds what size
   * SingleSlabCache it should fit in. If the block doesn't fit in any, it will
   * return without doing anything.
   * <p>
   * It is assumed this will NEVER be called on an already cached block. If that
   * is done, it is assumed that you are reinserting the same exact block due to
   * a race condition, and will throw a runtime exception.
   *
   * @param cacheKey block cache key
   * @param cachedItem block buffer
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable cachedItem) {
    Entry<Integer, SingleSizeCache> scacheEntry = getHigherBlock(cachedItem
        .getSerializedLength());

    this.requestStats.addin(cachedItem.getSerializedLength());

    if (scacheEntry == null) {
      return; // we can't cache, something too big.
    }

    this.successfullyCachedStats.addin(cachedItem.getSerializedLength());
    SingleSizeCache scache = scacheEntry.getValue();

    /*
     * This will throw a runtime exception if we try to cache the same value
     * twice
     */
    scache.cacheBlock(cacheKey, cachedItem);
  } 

  /**
   * We don't care about whether its in memory or not, so we just pass the call
   * through.
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    cacheBlock(cacheKey, buf);
  }

  public CacheStats getStats() {
    return this.stats;
  }

  /**
   * Get the buffer of the block with the specified name.
   *
   * @return buffer of specified block name, or null if not in cache
   */
  public Cacheable getBlock(BlockCacheKey key, boolean caching, boolean repeat,
      boolean updateCacheMetrics) {
    SingleSizeCache cachedBlock = backingStore.get(key);
    if (cachedBlock == null) {
      if (!repeat) stats.miss(caching);
      return null;
    }

    Cacheable contentBlock = cachedBlock.getBlock(key, caching, false, updateCacheMetrics);

    if (contentBlock != null) {
      if (updateCacheMetrics) stats.hit(caching);
    } else if (!repeat) {
      if (updateCacheMetrics) stats.miss(caching);
    }
    return contentBlock;
  }

  /**
   * Evicts a block from the cache. This is public, and thus contributes to the
   * the evict counter.
   */
  public boolean evictBlock(BlockCacheKey cacheKey) {
    SingleSizeCache cacheEntry = backingStore.get(cacheKey);
    if (cacheEntry == null) {
      return false;
    } else {
      cacheEntry.evictBlock(cacheKey);
      return true;
    }
  }

  @Override
  public void onEviction(BlockCacheKey key, SingleSizeCache notifier) {
    stats.evicted();
    backingStore.remove(key);
  }
  
  @Override
  public void onInsertion(BlockCacheKey key, SingleSizeCache notifier) {
    backingStore.put(key, notifier);
  }

  /**
   * Sends a shutdown to all SingleSizeCache's contained by this cache.
   *
   * Also terminates the scheduleThreadPool.
   */
  public void shutdown() {
    for (SingleSizeCache s : slabs.values()) {
      s.shutdown();
    }
    this.scheduleThreadPool.shutdown();
  }

  public long heapSize() {
    long childCacheSize = 0;
    for (SingleSizeCache s : slabs.values()) {
      childCacheSize += s.heapSize();
    }
    return SlabCache.CACHE_FIXED_OVERHEAD + childCacheSize;
  }

  public long size() {
    return this.size;
  }

  public long getFreeSize() {
    long childFreeSize = 0;
    for (SingleSizeCache s : slabs.values()) {
      childFreeSize += s.getFreeSize();
    }
    return childFreeSize;
  }

  @Override
  public long getBlockCount() {
    long count = 0;
    for (SingleSizeCache cache : slabs.values()) {
      count += cache.getBlockCount();
    }
    return count;
  }

  public long getCurrentSize() {
    return size;
  }

  public long getEvictedCount() {
    return stats.getEvictedCount();
  }

  /*
   * Statistics thread. Periodically prints the cache statistics to the log.
   */
  static class StatisticsThread extends HasThread {
    SlabCache ourcache;

    public StatisticsThread(SlabCache slabCache) {
      super("SlabCache.StatisticsThread");
      setDaemon(true);
      this.ourcache = slabCache;
    }

    @Override
    public void run() {
      for (SingleSizeCache s : ourcache.slabs.values()) {
        s.logStats();
      }

      SlabCache.LOG.info("Current heap size is: "
          + StringUtils.humanReadableInt(ourcache.heapSize()));

      LOG.info("Request Stats");
      ourcache.requestStats.logStats();
      LOG.info("Successfully Cached Stats");
      ourcache.successfullyCachedStats.logStats();
    }

  }

  /**
   * Just like CacheStats, but more Slab specific. Finely grained profiling of
   * sizes we store using logs.
   *
   */
  static class SlabStats {
    // the maximum size somebody will ever try to cache, then we multiply by
    // 10
    // so we have finer grained stats.
    static final int MULTIPLIER = 10;
    final int NUMDIVISIONS = (int) (Math.log(Integer.MAX_VALUE) * MULTIPLIER);
    private final AtomicLong[] counts = new AtomicLong[NUMDIVISIONS];

    public SlabStats() {
      for (int i = 0; i < NUMDIVISIONS; i++) {
        counts[i] = new AtomicLong();
      }
    }

    public void addin(int size) {
      int index = (int) (Math.log(size) * MULTIPLIER);
      counts[index].incrementAndGet();
    }

    public AtomicLong[] getUsage() {
      return counts;
    }

    double getUpperBound(int index) {
      return Math.pow(Math.E, ((index + 0.5) / MULTIPLIER));
    }

    double getLowerBound(int index) {
      return Math.pow(Math.E, ((index - 0.5) / MULTIPLIER));
    }

    public void logStats() {
      AtomicLong[] fineGrainedStats = getUsage();
      for (int i = 0; i < fineGrainedStats.length; i++) {

        if (fineGrainedStats[i].get() > 0) {
          SlabCache.LOG.info("From  "
              + StringUtils.humanReadableInt((long) getLowerBound(i)) + "- "
              + StringUtils.humanReadableInt((long) getUpperBound(i)) + ": "
              + StringUtils.humanReadableInt(fineGrainedStats[i].get())
              + " requests");

        }
      }
    }
  }

  public int evictBlocksByHfileName(String hfileName) {
    int numEvicted = 0;
    for (BlockCacheKey key : backingStore.keySet()) {
      if (key.getHfileName().equals(hfileName)) {
        if (evictBlock(key))
          ++numEvicted;
      }
    }
    return numEvicted;
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    // Don't bother with ramcache since stuff is in here only a little while.
    final Iterator<Map.Entry<BlockCacheKey, SingleSizeCache>> i =
        this.backingStore.entrySet().iterator();
    return new Iterator<CachedBlock>() {
      private final long now = System.nanoTime();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public CachedBlock next() {
        final Map.Entry<BlockCacheKey, SingleSizeCache> e = i.next();
        final Cacheable cacheable = e.getValue().getBlock(e.getKey(), false, false, false);
        return new CachedBlock() {
          @Override
          public String toString() {
            return BlockCacheUtil.toString(this, now);
          }

          @Override
          public BlockPriority getBlockPriority() {
            return null;
          }

          @Override
          public BlockType getBlockType() {
            return cacheable.getBlockType();
          }

          @Override
          public long getOffset() {
            return e.getKey().getOffset();
          }

          @Override
          public long getSize() {
            return cacheable == null? 0: cacheable.getSerializedLength();
          }

          @Override
          public long getCachedTime() {
            return -1;
          }

          @Override
          public String getFilename() {
            return e.getKey().getHfileName();
          }

          @Override
          public int compareTo(CachedBlock other) {
            return (int)(this.getOffset() - other.getOffset());
          }
        };
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return null;
  }
}
