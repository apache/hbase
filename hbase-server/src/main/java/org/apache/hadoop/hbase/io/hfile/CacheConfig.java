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

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores all of the cache objects and configuration for a single HFile.
 */
@InterfaceAudience.Private
public class CacheConfig implements PropagatingConfigurationObserver {
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
   * Configuration key to cache data blocks on write. There are separate switches for bloom blocks
   * and non-root index blocks.
   */
  public static final String CACHE_BLOCKS_ON_WRITE_KEY = "hbase.rs.cacheblocksonwrite";

  /**
   * Configuration key to cache leaf and intermediate-level index blocks on write.
   */
  public static final String CACHE_INDEX_BLOCKS_ON_WRITE_KEY = "hfile.block.index.cacheonwrite";

  /**
   * Configuration key to cache compound bloom filter blocks on write.
   */
  public static final String CACHE_BLOOM_BLOCKS_ON_WRITE_KEY = "hfile.block.bloom.cacheonwrite";

  /**
   * Configuration key to cache data blocks in compressed and/or encrypted format.
   */
  public static final String CACHE_DATA_BLOCKS_COMPRESSED_KEY = "hbase.block.data.cachecompressed";

  /**
   * Configuration key to evict all blocks of a given file from the block cache when the file is
   * closed.
   */
  public static final String EVICT_BLOCKS_ON_CLOSE_KEY = "hbase.rs.evictblocksonclose";

  /**
   * Configuration key to evict all blocks of a parent region from the block cache when the region
   * split or merge.
   */
  public static final String EVICT_BLOCKS_ON_SPLIT_KEY = "hbase.rs.evictblocksonsplit";

  /**
   * Configuration key to prefetch all blocks of a given file into the block cache when the file is
   * opened.
   */
  public static final String PREFETCH_BLOCKS_ON_OPEN_KEY = "hbase.rs.prefetchblocksonopen";

  /**
   * Configuration key to cache blocks when a compacted file is written
   */
  public static final String CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY =
    "hbase.rs.cachecompactedblocksonwrite";

  /**
   * Configuration key to determine total size in bytes of compacted files beyond which we do not
   * cache blocks on compaction
   */
  public static final String CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD_KEY =
    "hbase.rs.cachecompactedblocksonwrite.threshold";

  public static final String DROP_BEHIND_CACHE_COMPACTION_KEY =
    "hbase.hfile.drop.behind.compaction";

  /**
   * Configuration key to set interval for persisting bucket cache to disk.
   */
  public static final String BUCKETCACHE_PERSIST_INTERVAL_KEY =
    "hbase.bucketcache.persist.intervalinmillis";

  /**
   * Configuration key to set the heap usage threshold limit once prefetch threads should be
   * interrupted.
   */
  public static final String PREFETCH_HEAP_USAGE_THRESHOLD = "hbase.rs.prefetchheapusage";

  // Defaults
  public static final boolean DEFAULT_CACHE_DATA_ON_READ = true;
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = false;
  public static final boolean DEFAULT_IN_MEMORY = false;
  public static final boolean DEFAULT_CACHE_INDEXES_ON_WRITE = false;
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;
  public static final boolean DEFAULT_EVICT_ON_CLOSE = false;
  public static final boolean DEFAULT_EVICT_ON_SPLIT = true;
  public static final boolean DEFAULT_CACHE_DATA_COMPRESSED = false;
  public static final boolean DEFAULT_PREFETCH_ON_OPEN = false;
  public static final boolean DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE = false;
  public static final boolean DROP_BEHIND_CACHE_COMPACTION_DEFAULT = true;
  public static final long DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD = Long.MAX_VALUE;
  public static final double DEFAULT_PREFETCH_HEAP_USAGE_THRESHOLD = 1d;

  /**
   * Whether blocks should be cached on read (default is on if there is a cache but this can be
   * turned off on a per-family or per-request basis). If off we will STILL cache meta blocks; i.e.
   * INDEX and BLOOM types. This cannot be disabled.
   */
  private volatile boolean cacheDataOnRead;

  /** Whether blocks should be flagged as in-memory when being cached */
  private boolean inMemory;

  /** Whether data blocks should be cached when new files are written */
  private volatile boolean cacheDataOnWrite;

  /** Whether index blocks should be cached when new files are written */
  private boolean cacheIndexesOnWrite;

  /** Whether compound bloom filter blocks should be cached on write */
  private boolean cacheBloomsOnWrite;

  /** Whether blocks of a file should be evicted when the file is closed */
  private volatile boolean evictOnClose;

  /**
   * Whether blocks of a parent region should be evicted from cache when the region split or
   * merge
   */
  private boolean evictOnSplit;

  /** Whether data blocks should be stored in compressed and/or encrypted form in the cache */
  private boolean cacheDataCompressed;

  /** Whether data blocks should be prefetched into the cache */
  private boolean prefetchOnOpen;

  /**
   * Whether data blocks should be cached when compacted file is written
   */
  private boolean cacheCompactedDataOnWrite;

  /**
   * Determine threshold beyond which we do not cache blocks on compaction
   */
  private long cacheCompactedDataOnWriteThreshold;

  private boolean dropBehindCompaction;

  // Local reference to the block cache
  private final BlockCache blockCache;

  private final ByteBuffAllocator byteBuffAllocator;

  private double heapUsageThreshold;

  /**
   * Create a cache configuration using the specified configuration object and defaults for family
   * level settings. Only use if no column family context.
   * @param conf hbase configuration
   */
  public CacheConfig(Configuration conf) {
    this(conf, null);
  }

  public CacheConfig(Configuration conf, BlockCache blockCache) {
    this(conf, null, blockCache, ByteBuffAllocator.HEAP);
  }

  /**
   * Create a cache configuration using the specified configuration object and family descriptor.
   * @param conf   hbase configuration
   * @param family column family configuration
   */
  public CacheConfig(Configuration conf, ColumnFamilyDescriptor family, BlockCache blockCache,
    ByteBuffAllocator byteBuffAllocator) {
    if (family == null || family.isBlockCacheEnabled()) {
      this.cacheDataOnRead = conf.getBoolean(CACHE_DATA_ON_READ_KEY, DEFAULT_CACHE_DATA_ON_READ);
      this.inMemory = family == null ? DEFAULT_IN_MEMORY : family.isInMemory();
      this.cacheDataCompressed =
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_CACHE_DATA_COMPRESSED);
      this.dropBehindCompaction =
        conf.getBoolean(DROP_BEHIND_CACHE_COMPACTION_KEY, DROP_BEHIND_CACHE_COMPACTION_DEFAULT);
      // For the following flags we enable them regardless of per-schema settings
      // if they are enabled in the global configuration.
      this.cacheDataOnWrite =
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE)
          || (family != null && family.isCacheDataOnWrite());
      this.cacheIndexesOnWrite =
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_INDEXES_ON_WRITE)
          || (family != null && family.isCacheIndexesOnWrite());
      this.cacheBloomsOnWrite =
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_BLOOMS_ON_WRITE)
          || (family != null && family.isCacheBloomsOnWrite());
      this.evictOnClose = conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE)
        || (family != null && family.isEvictBlocksOnClose());
      this.evictOnSplit = conf.getBoolean(EVICT_BLOCKS_ON_SPLIT_KEY, DEFAULT_EVICT_ON_SPLIT);
      this.prefetchOnOpen = conf.getBoolean(PREFETCH_BLOCKS_ON_OPEN_KEY, DEFAULT_PREFETCH_ON_OPEN)
        || (family != null && family.isPrefetchBlocksOnOpen());
      this.cacheCompactedDataOnWrite = conf.getBoolean(CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY,
        DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE);
      this.cacheCompactedDataOnWriteThreshold = getCacheCompactedBlocksOnWriteThreshold(conf);
      this.heapUsageThreshold =
        conf.getDouble(PREFETCH_HEAP_USAGE_THRESHOLD, DEFAULT_PREFETCH_HEAP_USAGE_THRESHOLD);
    }
    this.blockCache = blockCache;
    this.byteBuffAllocator = byteBuffAllocator;
  }

  /**
   * Constructs a cache configuration copied from the specified configuration.
   */
  public CacheConfig(CacheConfig cacheConf) {
    this.cacheDataOnRead = cacheConf.cacheDataOnRead;
    this.inMemory = cacheConf.inMemory;
    this.cacheDataOnWrite = cacheConf.cacheDataOnWrite;
    this.cacheIndexesOnWrite = cacheConf.cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheConf.cacheBloomsOnWrite;
    this.evictOnClose = cacheConf.evictOnClose;
    this.evictOnSplit = cacheConf.evictOnSplit;
    this.cacheDataCompressed = cacheConf.cacheDataCompressed;
    this.prefetchOnOpen = cacheConf.prefetchOnOpen;
    this.cacheCompactedDataOnWrite = cacheConf.cacheCompactedDataOnWrite;
    this.cacheCompactedDataOnWriteThreshold = cacheConf.cacheCompactedDataOnWriteThreshold;
    this.dropBehindCompaction = cacheConf.dropBehindCompaction;
    this.blockCache = cacheConf.blockCache;
    this.byteBuffAllocator = cacheConf.byteBuffAllocator;
    this.heapUsageThreshold = cacheConf.heapUsageThreshold;
  }

  private CacheConfig() {
    this.cacheDataOnRead = false;
    this.inMemory = false;
    this.cacheDataOnWrite = false;
    this.cacheIndexesOnWrite = false;
    this.cacheBloomsOnWrite = false;
    this.evictOnClose = false;
    this.evictOnSplit = false;
    this.cacheDataCompressed = false;
    this.prefetchOnOpen = false;
    this.cacheCompactedDataOnWrite = false;
    this.cacheCompactedDataOnWriteThreshold = DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD;
    this.dropBehindCompaction = false;
    this.blockCache = null;
    this.byteBuffAllocator = ByteBuffAllocator.HEAP;
    this.heapUsageThreshold = DEFAULT_PREFETCH_HEAP_USAGE_THRESHOLD;
  }

  /**
   * Returns whether the DATA blocks of this HFile should be cached on read or not (we always cache
   * the meta blocks, the INDEX and BLOOM blocks).
   * @return true if blocks should be cached on read, false if not
   */
  public boolean shouldCacheDataOnRead() {
    return cacheDataOnRead;
  }

  public boolean shouldDropBehindCompaction() {
    return dropBehindCompaction;
  }

  /**
   * Should we cache a block of a particular category? We always cache important blocks such as
   * index blocks, as long as the block cache is available.
   */
  public boolean shouldCacheBlockOnRead(BlockCategory category) {
    return cacheDataOnRead || category == BlockCategory.INDEX || category == BlockCategory.BLOOM
      || (prefetchOnOpen && (category != BlockCategory.META && category != BlockCategory.UNKNOWN));
  }

  public boolean shouldCacheBlockOnRead(BlockCategory category, HFileInfo hFileInfo,
    Configuration conf) {
    Optional<Boolean> cacheFileBlock = Optional.of(true);
    // For DATA blocks only, if BucketCache is in use, we don't need to cache block again
    if (getBlockCache().isPresent() && category.equals(BlockCategory.DATA)) {
      Optional<Boolean> result = getBlockCache().get().shouldCacheFile(hFileInfo, conf);
      if (result.isPresent()) {
        cacheFileBlock = result;
      }
    }
    return shouldCacheBlockOnRead(category) && cacheFileBlock.get();
  }

  /** Returns true if blocks in this file should be flagged as in-memory */
  public boolean isInMemory() {
    return this.inMemory;
  }

  /**
   * @return true if data blocks should be written to the cache when an HFile is written, false if
   *         not
   */
  public boolean shouldCacheDataOnWrite() {
    return this.cacheDataOnWrite;
  }

  /**
   * @param cacheDataOnWrite whether data blocks should be written to the cache when an HFile is
   *                         written
   */
  public void setCacheDataOnWrite(boolean cacheDataOnWrite) {
    this.cacheDataOnWrite = cacheDataOnWrite;
  }

  /**
   * Enable cache on write including: cacheDataOnWrite cacheIndexesOnWrite cacheBloomsOnWrite
   */
  public void enableCacheOnWrite() {
    this.cacheDataOnWrite = true;
    this.cacheIndexesOnWrite = true;
    this.cacheBloomsOnWrite = true;
  }

  /**
   * @return true if index blocks should be written to the cache when an HFile is written, false if
   *         not
   */
  public boolean shouldCacheIndexesOnWrite() {
    return this.cacheIndexesOnWrite;
  }

  /**
   * @return true if bloom blocks should be written to the cache when an HFile is written, false if
   *         not
   */
  public boolean shouldCacheBloomsOnWrite() {
    return this.cacheBloomsOnWrite;
  }

  /**
   * @return true if blocks should be evicted from the cache when an HFile reader is closed, false
   *         if not
   */
  public boolean shouldEvictOnClose() {
    return this.evictOnClose;
  }

  /**
   * @param evictOnClose whether blocks should be evicted from the cache when an HFile reader is
   *                     closed
   */
  public void setEvictOnClose(boolean evictOnClose) {
    this.evictOnClose = evictOnClose;
  }

  /**
   * @return true if blocks of parent region should be evicted from the cache when the region split
   *         or merge, false if not
   */
  public boolean shouldEvictOnSplit() {
    return this.evictOnSplit;
  }

  /** Returns true if data blocks should be compressed in the cache, false if not */
  public boolean shouldCacheDataCompressed() {
    return this.cacheDataOnRead && this.cacheDataCompressed;
  }

  /**
   * Returns true if this {@link BlockCategory} should be compressed in BlockCache, false otherwise
   */
  public boolean shouldCacheCompressed(BlockCategory category) {
    switch (category) {
      case DATA:
        return this.cacheDataOnRead && this.cacheDataCompressed;
      default:
        return false;
    }
  }

  /** Returns true if blocks should be prefetched into the cache on open, false if not */
  public boolean shouldPrefetchOnOpen() {
    return this.prefetchOnOpen && this.cacheDataOnRead;
  }

  /** Returns true if blocks should be cached while writing during compaction, false if not */
  public boolean shouldCacheCompactedBlocksOnWrite() {
    return this.cacheCompactedDataOnWrite;
  }

  /** Returns total file size in bytes threshold for caching while writing during compaction */
  public long getCacheCompactedBlocksOnWriteThreshold() {
    return this.cacheCompactedDataOnWriteThreshold;
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
    if (
      blockType.getCategory() == BlockCategory.BLOOM
        || blockType.getCategory() == BlockCategory.INDEX
    ) {
      return true;
    }
    return false;
  }

  /**
   * Checks if the current heap usage is below the threshold configured by
   * "hbase.rs.prefetchheapusage" (0.8 by default).
   */
  public boolean isHeapUsageBelowThreshold() {
    double total = Runtime.getRuntime().maxMemory();
    double available = Runtime.getRuntime().freeMemory();
    double usedRatio = 1d - (available / total);
    return heapUsageThreshold > usedRatio;
  }

  /**
   * If we make sure the block could not be cached, we will not acquire the lock otherwise we will
   * acquire lock
   */
  public boolean shouldLockOnCacheMiss(BlockType blockType) {
    if (blockType == null) {
      return true;
    }
    return shouldCacheBlockOnRead(blockType.getCategory());
  }

  /**
   * Returns the block cache.
   * @return the block cache, or null if caching is completely disabled
   */
  public Optional<BlockCache> getBlockCache() {
    return Optional.ofNullable(this.blockCache);
  }

  public boolean isCombinedBlockCache() {
    return blockCache instanceof CombinedBlockCache;
  }

  public ByteBuffAllocator getByteBuffAllocator() {
    return this.byteBuffAllocator;
  }

  public double getHeapUsageThreshold() {
    return heapUsageThreshold;
  }

  private long getCacheCompactedBlocksOnWriteThreshold(Configuration conf) {
    long cacheCompactedBlocksOnWriteThreshold =
      conf.getLong(CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD_KEY,
        DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD);

    if (cacheCompactedBlocksOnWriteThreshold < 0) {
      LOG.warn(
        "cacheCompactedBlocksOnWriteThreshold value : {} is less than 0, resetting it to: {}",
        cacheCompactedBlocksOnWriteThreshold, DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD);
      cacheCompactedBlocksOnWriteThreshold = DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD;
    }

    return cacheCompactedBlocksOnWriteThreshold;
  }

  @Override
  public String toString() {
    return "cacheDataOnRead=" + shouldCacheDataOnRead() + ", cacheDataOnWrite="
      + shouldCacheDataOnWrite() + ", cacheIndexesOnWrite=" + shouldCacheIndexesOnWrite()
      + ", cacheBloomsOnWrite=" + shouldCacheBloomsOnWrite() + ", cacheEvictOnClose="
      + shouldEvictOnClose() + ", cacheEvictOnSplit=" + shouldEvictOnSplit()
      + ", cacheDataCompressed=" + shouldCacheDataCompressed() + ", prefetchOnOpen="
      + shouldPrefetchOnOpen() + ", cacheCompactedDataOnWrite="
      + shouldCacheCompactedBlocksOnWrite() + ", cacheCompactedDataOnWriteThreshold="
      + getCacheCompactedBlocksOnWriteThreshold() + ", dropBehindCompaction="
      + shouldDropBehindCompaction();
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    cacheDataOnRead = conf.getBoolean(CACHE_DATA_ON_READ_KEY, DEFAULT_CACHE_DATA_ON_READ);
    cacheDataOnWrite = conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE);
    evictOnClose = conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE);
    LOG.info(
      "Config hbase.block.data.cacheonread is changed to {}, "
        + "hbase.rs.cacheblocksonwrite is changed to {}, "
        + "hbase.rs.evictblocksonclose is changed to {}",
      cacheDataOnRead, cacheDataOnWrite, evictOnClose);
  }

  @Override
  public void registerChildren(ConfigurationManager manager) {
    manager.registerObserver(blockCache);
  }

  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    manager.deregisterObserver(blockCache);
  }
}
