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

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;


/**
 * Stores all of the cache objects and configuration for a single HFile.
 */
@InterfaceAudience.Private
public class CacheConfig {
  private static final Logger LOG = LoggerFactory.getLogger(CacheConfig.class.getName());


  /**
   * Disabled cache configuration
   */
  public static final CacheConfig DISABLED = new CacheConfig();

  /**
   * Configuration key to cache data blocks on read. Bloom blocks and index blocks are always be
   * cached if the block cache is enabled.
   */
  public static final String CACHE_DATA_ON_READ_KEY = "hbase.block.data.cacheonread";

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
   * Configuration key to cache data blocks in compressed and/or encrypted format.
   */
  public static final String CACHE_DATA_BLOCKS_COMPRESSED_KEY =
      "hbase.block.data.cachecompressed";

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
   * If the chosen ioengine can persist its state across restarts, the path to the file to persist
   * to. This file is NOT the data file. It is a file into which we will serialize the map of
   * what is in the data file. For example, if you pass the following argument as
   * BUCKET_CACHE_IOENGINE_KEY ("hbase.bucketcache.ioengine"),
   * <code>file:/tmp/bucketcache.data </code>, then we will write the bucketcache data to the file
   * <code>/tmp/bucketcache.data</code> but the metadata on where the data is in the supplied file
   * is an in-memory map that needs to be persisted across restarts. Where to store this
   * in-memory state is what you supply here: e.g. <code>/tmp/bucketcache.map</code>.
   */
  public static final String BUCKET_CACHE_PERSISTENT_PATH_KEY =
      "hbase.bucketcache.persistent.path";

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
  public static final int DEFAULT_BUCKET_CACHE_WRITER_THREADS = 3;
  public static final int DEFAULT_BUCKET_CACHE_WRITER_QUEUE = 64;

 /**
   * Configuration key to prefetch all blocks of a given file into the block cache
   * when the file is opened.
   */
  public static final String PREFETCH_BLOCKS_ON_OPEN_KEY =
      "hbase.rs.prefetchblocksonopen";

  /**
   * The target block size used by blockcache instances. Defaults to
   * {@link HConstants#DEFAULT_BLOCKSIZE}.
   * TODO: this config point is completely wrong, as it's used to determine the
   * target block size of BlockCache instances. Rename.
   */
  public static final String BLOCKCACHE_BLOCKSIZE_KEY = "hbase.offheapcache.minblocksize";

  private static final String EXTERNAL_BLOCKCACHE_KEY = "hbase.blockcache.use.external";
  private static final boolean EXTERNAL_BLOCKCACHE_DEFAULT = false;

  private static final String EXTERNAL_BLOCKCACHE_CLASS_KEY = "hbase.blockcache.external.class";
  private static final String DROP_BEHIND_CACHE_COMPACTION_KEY =
      "hbase.hfile.drop.behind.compaction";
  private static final boolean DROP_BEHIND_CACHE_COMPACTION_DEFAULT = true;

  /**
   * Enum of all built in external block caches.
   * This is used for config.
   */
  private static enum ExternalBlockCaches {
    memcached("org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache");
    // TODO(eclark): Consider more. Redis, etc.
    Class<? extends BlockCache> clazz;
    ExternalBlockCaches(String clazzName) {
      try {
        clazz = (Class<? extends BlockCache>) Class.forName(clazzName);
      } catch (ClassNotFoundException cnef) {
        clazz = null;
      }
    }
    ExternalBlockCaches(Class<? extends BlockCache> clazz) {
      this.clazz = clazz;
    }
  }

  // Defaults
  public static final boolean DEFAULT_CACHE_DATA_ON_READ = true;
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = false;
  public static final boolean DEFAULT_IN_MEMORY = false;
  public static final boolean DEFAULT_CACHE_INDEXES_ON_WRITE = false;
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;
  public static final boolean DEFAULT_EVICT_ON_CLOSE = false;
  public static final boolean DEFAULT_CACHE_DATA_COMPRESSED = false;
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

  /** Whether data blocks should be stored in compressed and/or encrypted form in the cache */
  private final boolean cacheDataCompressed;

  /** Whether data blocks should be prefetched into the cache */
  private final boolean prefetchOnOpen;

  private final boolean dropBehindCompaction;

  /**
   * Create a cache configuration using the specified configuration object and
   * family descriptor.
   * @param conf hbase configuration
   * @param family column family configuration
   */
  public CacheConfig(Configuration conf, ColumnFamilyDescriptor family) {
    this(CacheConfig.instantiateBlockCache(conf),
        conf.getBoolean(CACHE_DATA_ON_READ_KEY, DEFAULT_CACHE_DATA_ON_READ)
           && family.isBlockCacheEnabled(),
        family.isInMemory(),
        // For the following flags we enable them regardless of per-schema settings
        // if they are enabled in the global configuration.
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_DATA_ON_WRITE) || family.isCacheDataOnWrite(),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_INDEXES_ON_WRITE) || family.isCacheIndexesOnWrite(),
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_BLOOMS_ON_WRITE) || family.isCacheBloomsOnWrite(),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY,
            DEFAULT_EVICT_ON_CLOSE) || family.isEvictBlocksOnClose(),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_CACHE_DATA_COMPRESSED),
        conf.getBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY,
            DEFAULT_PREFETCH_ON_OPEN) || family.isPrefetchBlocksOnOpen(),
        conf.getBoolean(DROP_BEHIND_CACHE_COMPACTION_KEY, DROP_BEHIND_CACHE_COMPACTION_DEFAULT)
     );
    LOG.info("Created cacheConfig for " + family.getNameAsString() + ": " + this);
  }

  /**
   * Create a cache configuration using the specified configuration object and
   * defaults for family level settings. Only use if no column family context. Prefer
   * {@link CacheConfig#CacheConfig(Configuration, ColumnFamilyDescriptor)}
   * @see #CacheConfig(Configuration, ColumnFamilyDescriptor)
   * @param conf hbase configuration
   */
  public CacheConfig(Configuration conf) {
    this(conf, true);
  }

  public CacheConfig(Configuration conf, boolean enableBlockCache) {
    this(conf, enableBlockCache,
        conf.getBoolean(CACHE_DATA_ON_READ_KEY, DEFAULT_CACHE_DATA_ON_READ),
        DEFAULT_IN_MEMORY, // This is a family-level setting so can't be set
                           // strictly from conf
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_INDEXES_ON_WRITE),
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_BLOOMS_ON_WRITE),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_CACHE_DATA_COMPRESSED),
        conf.getBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY, DEFAULT_PREFETCH_ON_OPEN),
        conf.getBoolean(DROP_BEHIND_CACHE_COMPACTION_KEY, DROP_BEHIND_CACHE_COMPACTION_DEFAULT));
    LOG.info("Created cacheConfig: " + this);
  }

  private CacheConfig(Configuration conf, boolean enableBlockCache,
      final boolean cacheDataOnRead, final boolean inMemory,
      final boolean cacheDataOnWrite, final boolean cacheIndexesOnWrite,
      final boolean cacheBloomsOnWrite, final boolean evictOnClose,
      final boolean cacheDataCompressed, final boolean prefetchOnOpen,
      final boolean dropBehindCompaction) {
    if (enableBlockCache) {
      this.blockCache = CacheConfig.instantiateBlockCache(conf);
    } else {
      this.blockCache = null;
    }
    this.cacheDataOnRead = cacheDataOnRead;
    this.inMemory = inMemory;
    this.cacheDataOnWrite = cacheDataOnWrite;
    this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    this.evictOnClose = evictOnClose;
    this.cacheDataCompressed = cacheDataCompressed;
    this.prefetchOnOpen = prefetchOnOpen;
    this.dropBehindCompaction = dropBehindCompaction;
  }

  /**
   * Create a block cache configuration with the specified cache and configuration parameters.
   * @param blockCache reference to block cache, null if completely disabled
   * @param cacheDataOnRead whether DATA blocks should be cached on read (we always cache INDEX
   *          blocks and BLOOM blocks; this cannot be disabled).
   * @param inMemory whether blocks should be flagged as in-memory
   * @param cacheDataOnWrite whether data blocks should be cached on write
   * @param cacheIndexesOnWrite whether index blocks should be cached on write
   * @param cacheBloomsOnWrite whether blooms should be cached on write
   * @param evictOnClose whether blocks should be evicted when HFile is closed
   * @param cacheDataCompressed whether to store blocks as compressed in the cache
   * @param prefetchOnOpen whether to prefetch blocks upon open
   * @param dropBehindCompaction indicate that we should set drop behind to true when open a store
   *          file reader for compaction
   */
  @VisibleForTesting
  CacheConfig(final BlockCache blockCache,
      final boolean cacheDataOnRead, final boolean inMemory,
      final boolean cacheDataOnWrite, final boolean cacheIndexesOnWrite,
      final boolean cacheBloomsOnWrite, final boolean evictOnClose,
      final boolean cacheDataCompressed, final boolean prefetchOnOpen,
      final boolean dropBehindCompaction) {
    this.blockCache = blockCache;
    this.cacheDataOnRead = cacheDataOnRead;
    this.inMemory = inMemory;
    this.cacheDataOnWrite = cacheDataOnWrite;
    this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    this.evictOnClose = evictOnClose;
    this.cacheDataCompressed = cacheDataCompressed;
    this.prefetchOnOpen = prefetchOnOpen;
    this.dropBehindCompaction = dropBehindCompaction;
  }

  /**
   * Constructs a cache configuration copied from the specified configuration.
   * @param cacheConf
   */
  public CacheConfig(CacheConfig cacheConf) {
    this(cacheConf.blockCache, cacheConf.cacheDataOnRead, cacheConf.inMemory,
        cacheConf.cacheDataOnWrite, cacheConf.cacheIndexesOnWrite,
        cacheConf.cacheBloomsOnWrite, cacheConf.evictOnClose,
        cacheConf.cacheDataCompressed, cacheConf.prefetchOnOpen,
        cacheConf.dropBehindCompaction);
  }

  private CacheConfig() {
    this(null, false, false, false, false, false, false, false, false, false);
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

  public boolean shouldDropBehindCompaction() {
    return dropBehindCompaction;
  }

  /**
   * Should we cache a block of a particular category? We always cache
   * important blocks such as index blocks, as long as the block cache is
   * available.
   */
  public boolean shouldCacheBlockOnRead(BlockCategory category) {
    return isBlockCacheEnabled()
        && (cacheDataOnRead ||
            category == BlockCategory.INDEX ||
            category == BlockCategory.BLOOM ||
            (prefetchOnOpen &&
                (category != BlockCategory.META &&
                 category != BlockCategory.UNKNOWN)));
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
  @VisibleForTesting
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
   * @return true if data blocks should be compressed in the cache, false if not
   */
  public boolean shouldCacheDataCompressed() {
    return isBlockCacheEnabled() && this.cacheDataOnRead && this.cacheDataCompressed;
  }

  /**
   * @return true if this {@link BlockCategory} should be compressed in blockcache, false otherwise
   */
  public boolean shouldCacheCompressed(BlockCategory category) {
    if (!isBlockCacheEnabled()) return false;
    switch (category) {
      case DATA:
        return this.cacheDataOnRead && this.cacheDataCompressed;
      default:
        return false;
    }
  }

  /**
   * @return true if blocks should be prefetched into the cache on open, false if not
   */
  public boolean shouldPrefetchOnOpen() {
    return isBlockCacheEnabled() && this.prefetchOnOpen;
  }

  /**
   * Return true if we may find this type of block in block cache.
   * <p>
   * TODO: today {@code family.isBlockCacheEnabled()} only means {@code cacheDataOnRead}, so here we
   * consider lots of other configurations such as {@code cacheDataOnWrite}. We should fix this in
   * the future, {@code cacheDataOnWrite} should honor the CF level {@code isBlockCacheEnabled}
   * configuration.
   */
  public boolean shouldReadBlockFromCache(BlockType blockType) {
    if (!isBlockCacheEnabled()) {
      return false;
    }
    if (cacheDataOnRead) {
      return true;
    }
    if (prefetchOnOpen) {
      return true;
    }
    if (cacheDataOnWrite) {
      return true;
    }
    if (blockType == null) {
      return true;
    }
    if (blockType.getCategory() == BlockCategory.BLOOM ||
            blockType.getCategory() == BlockCategory.INDEX) {
      return true;
    }
    return false;
  }

  /**
   * If we make sure the block could not be cached, we will not acquire the lock
   * otherwise we will acquire lock
   */
  public boolean shouldLockOnCacheMiss(BlockType blockType) {
    if (blockType == null) {
      return true;
    }
    return shouldCacheBlockOnRead(blockType.getCategory());
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
      ", cacheDataCompressed=" + shouldCacheDataCompressed() +
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
  private static LruBlockCache ONHEAP_CACHE_INSTANCE = null;
  private static BlockCache L2_CACHE_INSTANCE = null;// Can be BucketCache or External cache.

  /** Boolean whether we have disabled the block cache entirely. */
  @VisibleForTesting
  static boolean blockCacheDisabled = false;

  /**
   * @param c Configuration to use.
   * @return An L1 instance.  Currently an instance of LruBlockCache.
   */
  public static LruBlockCache getOnHeapCache(final Configuration c) {
    return getOnHeapCacheInternal(c);
  }

  public CacheStats getOnHeapCacheStats() {
    if (ONHEAP_CACHE_INSTANCE != null) {
      return ONHEAP_CACHE_INSTANCE.getStats();
    }
    return null;
  }

  public CacheStats getL2CacheStats() {
    if (L2_CACHE_INSTANCE != null) {
      return L2_CACHE_INSTANCE.getStats();
    }
    return null;
  }

  /**
   * @param c Configuration to use.
   * @return An L1 instance.  Currently an instance of LruBlockCache.
   */
  private synchronized static LruBlockCache getOnHeapCacheInternal(final Configuration c) {
    if (ONHEAP_CACHE_INSTANCE != null) {
      return ONHEAP_CACHE_INSTANCE;
    }
    final long cacheSize = MemorySizeUtil.getOnHeapCacheSize(c);
    if (cacheSize < 0) {
      blockCacheDisabled = true;
    }
    if (blockCacheDisabled) return null;
    int blockSize = c.getInt(BLOCKCACHE_BLOCKSIZE_KEY, HConstants.DEFAULT_BLOCKSIZE);
    LOG.info("Allocating onheap LruBlockCache size=" +
      StringUtils.byteDesc(cacheSize) + ", blockSize=" + StringUtils.byteDesc(blockSize));
    ONHEAP_CACHE_INSTANCE = new LruBlockCache(cacheSize, blockSize, true, c);
    return ONHEAP_CACHE_INSTANCE;
  }

  private static BlockCache getExternalBlockcache(Configuration c) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to use External l2 cache");
    }
    Class klass = null;

    // Get the class, from the config. s
    try {
      klass = ExternalBlockCaches.valueOf(c.get(EXTERNAL_BLOCKCACHE_CLASS_KEY, "memcache")).clazz;
    } catch (IllegalArgumentException exception) {
      try {
        klass = c.getClass(EXTERNAL_BLOCKCACHE_CLASS_KEY, Class.forName(
            "org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache"));
      } catch (ClassNotFoundException e) {
        return null;
      }
    }

    // Now try and create an instance of the block cache.
    try {
      LOG.info("Creating external block cache of type: " + klass);
      return (BlockCache) ReflectionUtils.newInstance(klass, c);
    } catch (Exception e) {
      LOG.warn("Error creating external block cache", e);
    }
    return null;

  }

  @VisibleForTesting
  static BucketCache getBucketCache(Configuration c) {
    // Check for L2.  ioengine name must be non-null.
    String bucketCacheIOEngineName = c.get(BUCKET_CACHE_IOENGINE_KEY, null);
    if (bucketCacheIOEngineName == null || bucketCacheIOEngineName.length() <= 0) return null;

    int blockSize = c.getInt(BLOCKCACHE_BLOCKSIZE_KEY, HConstants.DEFAULT_BLOCKSIZE);
    final long bucketCacheSize = MemorySizeUtil.getBucketCacheSize(c);
    if (bucketCacheSize <= 0) {
      throw new IllegalStateException("bucketCacheSize <= 0; Check " +
        BUCKET_CACHE_SIZE_KEY + " setting and/or server java heap size");
    }
    if (c.get("hbase.bucketcache.percentage.in.combinedcache") != null) {
      LOG.warn("Configuration 'hbase.bucketcache.percentage.in.combinedcache' is no longer "
          + "respected. See comments in http://hbase.apache.org/book.html#_changes_of_note");
    }
    int writerThreads = c.getInt(BUCKET_CACHE_WRITER_THREADS_KEY,
      DEFAULT_BUCKET_CACHE_WRITER_THREADS);
    int writerQueueLen = c.getInt(BUCKET_CACHE_WRITER_QUEUE_KEY,
      DEFAULT_BUCKET_CACHE_WRITER_QUEUE);
    String persistentPath = c.get(BUCKET_CACHE_PERSISTENT_PATH_KEY);
    String[] configuredBucketSizes = c.getStrings(BUCKET_CACHE_BUCKETS_KEY);
    int [] bucketSizes = null;
    if (configuredBucketSizes != null) {
      bucketSizes = new int[configuredBucketSizes.length];
      for (int i = 0; i < configuredBucketSizes.length; i++) {
        int bucketSize = Integer.parseInt(configuredBucketSizes[i].trim());
        if (bucketSize % 256 != 0) {
          // We need all the bucket sizes to be multiples of 256. Having all the configured bucket
          // sizes to be multiples of 256 will ensure that the block offsets within buckets,
          // that are calculated, will also be multiples of 256.
          // See BucketEntry where offset to each block is represented using 5 bytes (instead of 8
          // bytes long). We would like to save heap overhead as less as possible.
          throw new IllegalArgumentException("Illegal value: " + bucketSize + " configured for '"
              + BUCKET_CACHE_BUCKETS_KEY + "'. All bucket sizes to be multiples of 256");
        }
        bucketSizes[i] = bucketSize;
      }
    }
    BucketCache bucketCache = null;
    try {
      int ioErrorsTolerationDuration = c.getInt(
        "hbase.bucketcache.ioengine.errors.tolerated.duration",
        BucketCache.DEFAULT_ERROR_TOLERATION_DURATION);
      // Bucket cache logs its stats on creation internal to the constructor.
      bucketCache = new BucketCache(bucketCacheIOEngineName,
        bucketCacheSize, blockSize, bucketSizes, writerThreads, writerQueueLen, persistentPath,
        ioErrorsTolerationDuration, c);
    } catch (IOException ioex) {
      LOG.error("Can't instantiate bucket cache", ioex); throw new RuntimeException(ioex);
    }
    return bucketCache;
  }

  /**
   * Returns the block cache or <code>null</code> in case none should be used.
   * Sets GLOBAL_BLOCK_CACHE_INSTANCE
   *
   * @param conf  The current configuration.
   * @return The block cache or <code>null</code>.
   */
  public static synchronized BlockCache instantiateBlockCache(Configuration conf) {
    if (GLOBAL_BLOCK_CACHE_INSTANCE != null) return GLOBAL_BLOCK_CACHE_INSTANCE;
    if (blockCacheDisabled) return null;
    LruBlockCache onHeapCache = getOnHeapCacheInternal(conf);
    // blockCacheDisabled is set as a side-effect of getL1Internal(), so check it again after the
    // call.
    if (blockCacheDisabled) return null;
    boolean useExternal = conf.getBoolean(EXTERNAL_BLOCKCACHE_KEY, EXTERNAL_BLOCKCACHE_DEFAULT);
    if (useExternal) {
      L2_CACHE_INSTANCE = getExternalBlockcache(conf);
      GLOBAL_BLOCK_CACHE_INSTANCE = L2_CACHE_INSTANCE == null ? onHeapCache
          : new InclusiveCombinedBlockCache(onHeapCache, L2_CACHE_INSTANCE);
    } else {
      // otherwise use the bucket cache.
      L2_CACHE_INSTANCE = getBucketCache(conf);
      if (!conf.getBoolean("hbase.bucketcache.combinedcache.enabled", true)) {
        // Non combined mode is off from 2.0
        LOG.warn(
            "From HBase 2.0 onwards only combined mode of LRU cache and bucket cache is available");
      }
      GLOBAL_BLOCK_CACHE_INSTANCE = L2_CACHE_INSTANCE == null ? onHeapCache
          : new CombinedBlockCache(onHeapCache, L2_CACHE_INSTANCE);
    }
    return GLOBAL_BLOCK_CACHE_INSTANCE;
  }

  // Supposed to use only from tests. Some tests want to reinit the Global block cache instance
  @VisibleForTesting
  static synchronized void clearGlobalInstances() {
    ONHEAP_CACHE_INSTANCE = null;
    L2_CACHE_INSTANCE = null;
    GLOBAL_BLOCK_CACHE_INSTANCE = null;
  }
}
