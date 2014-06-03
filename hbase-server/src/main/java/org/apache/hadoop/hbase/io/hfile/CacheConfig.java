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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.io.hfile.slab.SlabCache;
import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Stores all of the cache objects and configuration for a single HFile.
 */
@InterfaceAudience.Private
public class CacheConfig {
  private static final Log LOG = LogFactory.getLog(CacheConfig.class.getName());

  /**
   * Configuration key to cache data blocks on write. There are separate
   * switches for bloom blocks and non-root index blocks.
   */
  public static final String CACHE_BLOCKS_ON_WRITE_KEY =
      "hbase.rs.cacheblocksonwrite";

  /**
   * Configuration key to cache leaf and intermediate-level index blocks on
   * write.
   */
  public static final String CACHE_INDEX_BLOCKS_ON_WRITE_KEY =
      "hfile.block.index.cacheonwrite";

  /**
   * Configuration key to cache compound bloom filter blocks on write.
   */
  public static final String CACHE_BLOOM_BLOCKS_ON_WRITE_KEY =
      "hfile.block.bloom.cacheonwrite";

  /**
   * TODO: Implement this (jgray)
   * Configuration key to cache data blocks in compressed format.
   */
  public static final String CACHE_DATA_BLOCKS_COMPRESSED_KEY =
      "hbase.rs.blockcache.cachedatacompressed";

  /**
   * Configuration key to evict all blocks of a given file from the block cache
   * when the file is closed.
   */
  public static final String EVICT_BLOCKS_ON_CLOSE_KEY =
      "hbase.rs.evictblocksonclose";

  /**
   * Configuration keys for Bucket cache
   */

  /**
   * Current ioengine options in include: heap, offheap and file:PATH (where PATH is the path
   * to the file that will host the file-based cache.  See BucketCache#getIOEngineFromName() for
   * list of supported ioengine options.
   * 
   * <p>Set this option and a non-zero {@link #BUCKET_CACHE_SIZE_KEY} to enable bucket cache.
   */
  public static final String BUCKET_CACHE_IOENGINE_KEY = "hbase.bucketcache.ioengine";

  /**
   * When using bucket cache, this is a float that EITHER represents a percentage of total heap
   * memory size to give to the cache (if < 1.0) OR, it is the capacity in megabytes of the cache.
   * 
   * <p>The resultant size is further divided if {@link #BUCKET_CACHE_COMBINED_KEY} is set (It is
   * set by default. When false, bucket cache serves as an "L2" cache to the "L1"
   * {@link LruBlockCache}).  The percentage is set in
   * with {@link #BUCKET_CACHE_COMBINED_PERCENTAGE_KEY} float.
   */
  public static final String BUCKET_CACHE_SIZE_KEY = "hbase.bucketcache.size";

  /**
   * If the chosen ioengine can persist its state across restarts, the path to the file to
   * persist to.
   */
  public static final String BUCKET_CACHE_PERSISTENT_PATH_KEY = 
      "hbase.bucketcache.persistent.path";

  /**
   * If the bucket cache is used in league with the lru on-heap block cache (meta blocks such
   * as indices and blooms are kept in the lru blockcache and the data blocks in the
   * bucket cache).
   */
  public static final String BUCKET_CACHE_COMBINED_KEY = 
      "hbase.bucketcache.combinedcache.enabled";

  /**
   * A float which designates how much of the overall cache to give to bucket cache
   * and how much to on-heap lru cache when {@link #BUCKET_CACHE_COMBINED_KEY} is set.
   */
  public static final String BUCKET_CACHE_COMBINED_PERCENTAGE_KEY = 
      "hbase.bucketcache.percentage.in.combinedcache";

  public static final String BUCKET_CACHE_WRITER_THREADS_KEY = "hbase.bucketcache.writer.threads";
  public static final String BUCKET_CACHE_WRITER_QUEUE_KEY = 
      "hbase.bucketcache.writer.queuelength";

  /**
   * A comma-delimited array of values for use as bucket sizes.
   */
  public static final String BUCKET_CACHE_BUCKETS_KEY = "hbase.bucketcache.bucket.sizes";

  /**
   * Defaults for Bucket cache
   */
  public static final boolean DEFAULT_BUCKET_CACHE_COMBINED = true;
  public static final int DEFAULT_BUCKET_CACHE_WRITER_THREADS = 3;
  public static final int DEFAULT_BUCKET_CACHE_WRITER_QUEUE = 64;
  public static final float DEFAULT_BUCKET_CACHE_COMBINED_PERCENTAGE = 0.9f;

  /**
   * Setting this float to a non-null value turns on {@link DoubleBlockCache}
   * which makes use of the {@link LruBlockCache} and {@link SlabCache}.
   * 
   * The float value of between 0 and 1 will be multiplied against the setting for
   * <code>-XX:MaxDirectMemorySize</code> to figure what size of the offheap allocation to give
   * over to slab cache.
   * 
   * Slab cache has been little used and is likely to be deprecated in the near future.
   */
  public static final String SLAB_CACHE_OFFHEAP_PERCENTAGE_KEY =
    "hbase.offheapcache.percentage";

 /**
   * Configuration key to prefetch all blocks of a given file into the block cache
   * when the file is opened.
   */
  public static final String PREFETCH_BLOCKS_ON_OPEN_KEY =
      "hbase.rs.prefetchblocksonopen";

  // Defaults

  public static final boolean DEFAULT_CACHE_DATA_ON_READ = true;
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = false;
  public static final boolean DEFAULT_IN_MEMORY = false;
  public static final boolean DEFAULT_CACHE_INDEXES_ON_WRITE = false;
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;
  public static final boolean DEFAULT_EVICT_ON_CLOSE = false;
  public static final boolean DEFAULT_COMPRESSED_CACHE = false;
  public static final boolean DEFAULT_PREFETCH_ON_OPEN = false;

  /** Local reference to the block cache, null if completely disabled */
  private final BlockCache blockCache;

  /**
   * Whether blocks should be cached on read (default is on if there is a
   * cache but this can be turned off on a per-family or per-request basis).
   * If off we will STILL cache meta blocks; i.e. INDEX and BLOOM types.
   * This cannot be disabled.
   */
  private boolean cacheDataOnRead;

  /** Whether blocks should be flagged as in-memory when being cached */
  private final boolean inMemory;

  /** Whether data blocks should be cached when new files are written */
  private boolean cacheDataOnWrite;

  /** Whether index blocks should be cached when new files are written */
  private final boolean cacheIndexesOnWrite;

  /** Whether compound bloom filter blocks should be cached on write */
  private final boolean cacheBloomsOnWrite;

  /** Whether blocks of a file should be evicted when the file is closed */
  private boolean evictOnClose;

  /** Whether data blocks should be stored in compressed form in the cache */
  private final boolean cacheCompressed;

  /** Whether data blocks should be prefetched into the cache */
  private final boolean prefetchOnOpen;

  /**
   * Create a cache configuration using the specified configuration object and
   * family descriptor.
   * @param conf hbase configuration
   * @param family column family configuration
   */
  public CacheConfig(Configuration conf, HColumnDescriptor family) {
    this(CacheConfig.instantiateBlockCache(conf),
        family.isBlockCacheEnabled(),
        family.isInMemory(),
        // For the following flags we enable them regardless of per-schema settings
        // if they are enabled in the global configuration.
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_DATA_ON_WRITE) || family.shouldCacheDataOnWrite(),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_INDEXES_ON_WRITE) || family.shouldCacheIndexesOnWrite(),
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_BLOOMS_ON_WRITE) || family.shouldCacheBloomsOnWrite(),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY,
            DEFAULT_EVICT_ON_CLOSE) || family.shouldEvictBlocksOnClose(),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_COMPRESSED_CACHE),
        conf.getBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY,
            DEFAULT_PREFETCH_ON_OPEN) || family.shouldPrefetchBlocksOnOpen()
     );
  }

  /**
   * Create a cache configuration using the specified configuration object and
   * defaults for family level settings.
   * @param conf hbase configuration
   */
  public CacheConfig(Configuration conf) {
    this(CacheConfig.instantiateBlockCache(conf),
        DEFAULT_CACHE_DATA_ON_READ,
        DEFAULT_IN_MEMORY, // This is a family-level setting so can't be set
                           // strictly from conf
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_INDEXES_ON_WRITE),
            conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
                DEFAULT_CACHE_BLOOMS_ON_WRITE),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY,
            DEFAULT_COMPRESSED_CACHE),
        conf.getBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY, DEFAULT_PREFETCH_ON_OPEN)
     );
  }

  /**
   * Create a block cache configuration with the specified cache and
   * configuration parameters.
   * @param blockCache reference to block cache, null if completely disabled
   * @param cacheDataOnRead whether DATA blocks should be cached on read (we always cache INDEX
   * blocks and BLOOM blocks; this cannot be disabled).
   * @param inMemory whether blocks should be flagged as in-memory
   * @param cacheDataOnWrite whether data blocks should be cached on write
   * @param cacheIndexesOnWrite whether index blocks should be cached on write
   * @param cacheBloomsOnWrite whether blooms should be cached on write
   * @param evictOnClose whether blocks should be evicted when HFile is closed
   * @param cacheCompressed whether to store blocks as compressed in the cache
   * @param prefetchOnOpen whether to prefetch blocks upon open
   */
  CacheConfig(final BlockCache blockCache,
      final boolean cacheDataOnRead, final boolean inMemory,
      final boolean cacheDataOnWrite, final boolean cacheIndexesOnWrite,
      final boolean cacheBloomsOnWrite, final boolean evictOnClose,
      final boolean cacheCompressed, final boolean prefetchOnOpen) {
    this.blockCache = blockCache;
    this.cacheDataOnRead = cacheDataOnRead;
    this.inMemory = inMemory;
    this.cacheDataOnWrite = cacheDataOnWrite;
    this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    this.evictOnClose = evictOnClose;
    this.cacheCompressed = cacheCompressed;
    this.prefetchOnOpen = prefetchOnOpen;
    LOG.info(this);
  }

  /**
   * Constructs a cache configuration copied from the specified configuration.
   * @param cacheConf
   */
  public CacheConfig(CacheConfig cacheConf) {
    this(cacheConf.blockCache, cacheConf.cacheDataOnRead, cacheConf.inMemory,
        cacheConf.cacheDataOnWrite, cacheConf.cacheIndexesOnWrite,
        cacheConf.cacheBloomsOnWrite, cacheConf.evictOnClose,
        cacheConf.cacheCompressed, cacheConf.prefetchOnOpen);
  }

  /**
   * Checks whether the block cache is enabled.
   */
  public boolean isBlockCacheEnabled() {
    return this.blockCache != null;
  }

  /**
   * Returns the block cache.
   * @return the block cache, or null if caching is completely disabled
   */
  public BlockCache getBlockCache() {
    return this.blockCache;
  }

  /**
   * Returns whether the DATA blocks of this HFile should be cached on read or not (we always
   * cache the meta blocks, the INDEX and BLOOM blocks).
   * @return true if blocks should be cached on read, false if not
   */
  public boolean shouldCacheDataOnRead() {
    return isBlockCacheEnabled() && cacheDataOnRead;
  }

  /**
   * Should we cache a block of a particular category? We always cache
   * important blocks such as index blocks, as long as the block cache is
   * available.
   */
  public boolean shouldCacheBlockOnRead(BlockCategory category) {
    boolean shouldCache = isBlockCacheEnabled()
        && (cacheDataOnRead ||
            category == BlockCategory.INDEX ||
            category == BlockCategory.BLOOM ||
            (prefetchOnOpen &&
                (category != BlockCategory.META &&
                 category != BlockCategory.UNKNOWN)));
    return shouldCache;
  }

  /**
   * @return true if blocks in this file should be flagged as in-memory
   */
  public boolean isInMemory() {
    return isBlockCacheEnabled() && this.inMemory;
  }

  /**
   * @return true if data blocks should be written to the cache when an HFile is
   *         written, false if not
   */
  public boolean shouldCacheDataOnWrite() {
    return isBlockCacheEnabled() && this.cacheDataOnWrite;
  }

  /**
   * Only used for testing.
   * @param cacheDataOnWrite whether data blocks should be written to the cache
   *                         when an HFile is written
   */
  public void setCacheDataOnWrite(boolean cacheDataOnWrite) {
    this.cacheDataOnWrite = cacheDataOnWrite;
  }

  /**
   * @return true if index blocks should be written to the cache when an HFile
   *         is written, false if not
   */
  public boolean shouldCacheIndexesOnWrite() {
    return isBlockCacheEnabled() && this.cacheIndexesOnWrite;
  }

  /**
   * @return true if bloom blocks should be written to the cache when an HFile
   *         is written, false if not
   */
  public boolean shouldCacheBloomsOnWrite() {
    return isBlockCacheEnabled() && this.cacheBloomsOnWrite;
  }

  /**
   * @return true if blocks should be evicted from the cache when an HFile
   *         reader is closed, false if not
   */
  public boolean shouldEvictOnClose() {
    return isBlockCacheEnabled() && this.evictOnClose;
  }

  /**
   * Only used for testing.
   * @param evictOnClose whether blocks should be evicted from the cache when an
   *                     HFile reader is closed
   */
  public void setEvictOnClose(boolean evictOnClose) {
    this.evictOnClose = evictOnClose;
  }

  /**
   * @return true if blocks should be compressed in the cache, false if not
   */
  public boolean shouldCacheCompressed() {
    return isBlockCacheEnabled() && this.cacheCompressed;
  }

  /**
   * @return true if blocks should be prefetched into the cache on open, false if not
   */
  public boolean shouldPrefetchOnOpen() {
    return isBlockCacheEnabled() && this.prefetchOnOpen;
  }

  @Override
  public String toString() {
    if (!isBlockCacheEnabled()) {
      return "CacheConfig:disabled";
    }
    return "blockCache=" + getBlockCache() +
      ", cacheDataOnRead=" + shouldCacheDataOnRead() +
      ", cacheDataOnWrite=" + shouldCacheDataOnWrite() +
      ", cacheIndexesOnWrite=" + shouldCacheIndexesOnWrite() +
      ", cacheBloomsOnWrite=" + shouldCacheBloomsOnWrite() +
      ", cacheEvictOnClose=" + shouldEvictOnClose() +
      ", cacheCompressed=" + shouldCacheCompressed() +
      ", prefetchOnOpen=" + shouldPrefetchOnOpen();
  }

  // Static block cache reference and methods

  /**
   * Static reference to the block cache, or null if no caching should be used
   * at all.
   */
  // Clear this if in tests you'd make more than one block cache instance.
  @VisibleForTesting
  static BlockCache GLOBAL_BLOCK_CACHE_INSTANCE;

  /** Boolean whether we have disabled the block cache entirely. */
  @VisibleForTesting
  static boolean blockCacheDisabled = false;

  /**
   * Returns the block cache or <code>null</code> in case none should be used.
   *
   * @param conf  The current configuration.
   * @return The block cache or <code>null</code>.
   */
  public static synchronized BlockCache instantiateBlockCache(Configuration conf) {
    if (GLOBAL_BLOCK_CACHE_INSTANCE != null) return GLOBAL_BLOCK_CACHE_INSTANCE;
    if (blockCacheDisabled) return null;

    float cachePercentage = conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
      HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    if (cachePercentage <= 0.0001f) {
      blockCacheDisabled = true;
      return null;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY +
        " must be between 0.0 and 1.0, and not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long lruCacheSize = (long) (mu.getMax() * cachePercentage);
    int blockSize = conf.getInt("hbase.offheapcache.minblocksize", HConstants.DEFAULT_BLOCKSIZE);
    long slabCacheOffHeapCacheSize =
      (long) (conf.getFloat(SLAB_CACHE_OFFHEAP_PERCENTAGE_KEY, (float) 0) *
          DirectMemoryUtils.getDirectMemorySize());
    if (slabCacheOffHeapCacheSize <= 0) {
      String bucketCacheIOEngineName = conf.get(BUCKET_CACHE_IOENGINE_KEY, null);
      float bucketCachePercentage = conf.getFloat(BUCKET_CACHE_SIZE_KEY, 0F);
      // A percentage of max heap size or a absolute value with unit megabytes
      long bucketCacheSize = (long) (bucketCachePercentage < 1 ? mu.getMax()
          * bucketCachePercentage : bucketCachePercentage * 1024 * 1024);

      boolean combinedWithLru = conf.getBoolean(BUCKET_CACHE_COMBINED_KEY,
          DEFAULT_BUCKET_CACHE_COMBINED);
      BucketCache bucketCache = null;
      if (bucketCacheIOEngineName != null && bucketCacheSize > 0) {
        int writerThreads = conf.getInt(BUCKET_CACHE_WRITER_THREADS_KEY,
            DEFAULT_BUCKET_CACHE_WRITER_THREADS);
        int writerQueueLen = conf.getInt(BUCKET_CACHE_WRITER_QUEUE_KEY,
            DEFAULT_BUCKET_CACHE_WRITER_QUEUE);
        String persistentPath = conf.get(BUCKET_CACHE_PERSISTENT_PATH_KEY);
        float combinedPercentage = conf.getFloat(
            BUCKET_CACHE_COMBINED_PERCENTAGE_KEY,
            DEFAULT_BUCKET_CACHE_COMBINED_PERCENTAGE);
        String[] configuredBucketSizes = conf.getStrings(BUCKET_CACHE_BUCKETS_KEY);
        int[] bucketSizes = null;
        if (configuredBucketSizes != null) {
          bucketSizes = new int[configuredBucketSizes.length];
          for (int i = 0; i < configuredBucketSizes.length; i++) {
            bucketSizes[i] = Integer.parseInt(configuredBucketSizes[i]);
          }
        }
        if (combinedWithLru) {
          lruCacheSize = (long) ((1 - combinedPercentage) * bucketCacheSize);
          bucketCacheSize = (long) (combinedPercentage * bucketCacheSize);
        }
        try {
          int ioErrorsTolerationDuration = conf.getInt(
              "hbase.bucketcache.ioengine.errors.tolerated.duration",
              BucketCache.DEFAULT_ERROR_TOLERATION_DURATION);
          bucketCache = new BucketCache(bucketCacheIOEngineName,
              bucketCacheSize, blockSize, bucketSizes, writerThreads, writerQueueLen, persistentPath,
              ioErrorsTolerationDuration);
        } catch (IOException ioex) {
          LOG.error("Can't instantiate bucket cache", ioex);
          throw new RuntimeException(ioex);
        }
      }
      LOG.info("Allocating LruBlockCache size=" +
        StringUtils.byteDesc(lruCacheSize) + ", blockSize=" + StringUtils.byteDesc(blockSize));
      LruBlockCache lruCache = new LruBlockCache(lruCacheSize, blockSize);
      lruCache.setVictimCache(bucketCache);
      if (bucketCache != null && combinedWithLru) {
        GLOBAL_BLOCK_CACHE_INSTANCE = new CombinedBlockCache(lruCache, bucketCache);
      } else {
        GLOBAL_BLOCK_CACHE_INSTANCE = lruCache;
      }
    } else {
      GLOBAL_BLOCK_CACHE_INSTANCE = new DoubleBlockCache(
          lruCacheSize, slabCacheOffHeapCacheSize, blockSize, blockSize, conf);
    }
    return GLOBAL_BLOCK_CACHE_INSTANCE;
  }
}
