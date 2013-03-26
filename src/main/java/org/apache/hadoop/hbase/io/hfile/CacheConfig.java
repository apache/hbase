/*
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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.util.StringUtils;

/**
 * Stores all of the cache objects and configuration for a single HFile.
 */
public class CacheConfig {
  private static final Log LOG = LogFactory.getLog(CacheConfig.class.getName());

  /**
   * Configuration key to cache data blocks on write. There are separate
   * switches for bloom blocks and non-root index blocks.
   */
  public static final String CACHE_BLOCKS_ON_FLUSH_KEY =
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
   * Configuration key to turn on cache hot blocks on compaction.
   */
  public static final String CACHE_DATA_BLOCKS_ON_COMPACTION =
      "hfile.block.cacheoncompaction";

  /**
   * Configuration key to determine the threshold for hot blocks
   * (No of KVs obtained from cache during compaction)/
   * (Total number of KVs in the block)
   * During compaction, the blocks in which the % of KVs cached exceed this
   * threshold will be automatically cached on write. For scenarios, where cacheOnWrite
   * is already set, this parameter will not have any effect.
   */
  public static final String CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD =
      "hfile.block.cacheoncompaction.threshold";

  // Defaults

  public static final boolean DEFAULT_CACHE_DATA_ON_READ = true;
  public static final boolean DEFAULT_CACHE_DATA_ON_FLUSH = false;
  public static final boolean DEFAULT_IN_MEMORY = false;
  public static final boolean DEFAULT_CACHE_INDEXES_ON_WRITE = false;
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;
  public static final boolean DEFAULT_EVICT_ON_CLOSE = false;
  public static final boolean DEFAULT_COMPRESSED_CACHE = false;
  public static final boolean DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION = false;
  public static final float DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD = 0.75F;

  /** Local reference to the block cache, null if completely disabled */
  private final BlockCache blockCache;

  /**
   * Whether blocks should be cached on read (default is on if there is a
   * cache but this can be turned off on a per-family or per-request basis)
   */
  private boolean cacheDataOnRead;

  /** Whether blocks should be flagged as in-memory when being cached */
  private final boolean inMemory;

  /** Whether data blocks should be cached when new files are written because
   * of a memstore flush */
  private boolean cacheDataOnFlush;

  /** Whether index blocks should be cached when new files are written */
  private final boolean cacheIndexesOnWrite;

  /** Whether compound bloom filter blocks should be cached on write */
  private final boolean cacheBloomsOnWrite;

  /** Whether blocks of a file should be evicted when the file is closed */
  private boolean evictOnClose;

  /** Whether data blocks should be stored in compressed form in the cache */
  private final boolean cacheCompressed;

  /** Whether data blocks during compaction are cached */
  private boolean cacheOnCompaction;

  /** Threshold for caching hot data blocks during compaction */
  private final float cacheOnCompactionThreshold;

  /**
   * Create a cache configuration using the specified configuration object and
   * family descriptor.
   * @param conf hbase configuration
   * @param family column family configuration
   */
  public CacheConfig(Configuration conf, HColumnDescriptor family) {
    this(CacheConfig.instantiateBlockCache(conf),
        family.isBlockCacheEnabled(), family.isInMemory(),
        conf.getBoolean(CACHE_BLOCKS_ON_FLUSH_KEY, DEFAULT_CACHE_DATA_ON_FLUSH),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_INDEXES_ON_WRITE),
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_BLOOMS_ON_WRITE),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_COMPRESSED_CACHE),
        conf.getBoolean(CACHE_DATA_BLOCKS_ON_COMPACTION,
            DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION),
        conf.getFloat(CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD,
            DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD)
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
        conf.getBoolean(CACHE_BLOCKS_ON_FLUSH_KEY, DEFAULT_CACHE_DATA_ON_FLUSH),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_INDEXES_ON_WRITE),
            conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
                DEFAULT_CACHE_BLOOMS_ON_WRITE),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY,
            DEFAULT_COMPRESSED_CACHE),
            conf.getBoolean(CACHE_DATA_BLOCKS_ON_COMPACTION,
                DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION),
            conf.getFloat(CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD,
                DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD)
     );
  }

  /**
   * Create a block cache configuration with the specified cache and
   * configuration parameters.
   * @param blockCache reference to block cache, null if completely disabled
   * @param cacheDataOnRead whether data blocks should be cached on read
   * @param inMemory whether blocks should be flagged as in-memory
   * @param cacheDataOnWrite whether data blocks should be cached on write
   * @param cacheIndexesOnWrite whether index blocks should be cached on write
   * @param cacheBloomsOnFlush whether blooms should be cached on write
   * @param evictOnClose whether blocks should be evicted when HFile is closed
   * @param cacheCompressed whether to store blocks as compressed in the cache
   * @param cacheOnCompaction whether to cache blocks during compaction
   * @param cacheOnCompactionThreshold threshold used of caching of blocks during compaction
   */
  CacheConfig(final BlockCache blockCache,
      final boolean cacheDataOnRead, final boolean inMemory,
      final boolean cacheDataOnFlush, final boolean cacheIndexesOnWrite,
      final boolean cacheBloomsOnWrite, final boolean evictOnClose,
      final boolean cacheCompressed, final boolean cacheOnCompaction,
      final float cacheOnCompactionThreshold) {
    this.blockCache = blockCache;
    this.cacheDataOnRead = cacheDataOnRead;
    this.inMemory = inMemory;
    this.cacheDataOnFlush = cacheDataOnFlush;
    this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    this.evictOnClose = evictOnClose;
    this.cacheCompressed = cacheCompressed;
    this.cacheOnCompaction = cacheOnCompaction;
    this.cacheOnCompactionThreshold = cacheOnCompactionThreshold;
  }

  /**
   * Constructs a cache configuration copied from the specified configuration.
   * @param cacheConf
   */
  public CacheConfig(CacheConfig cacheConf) {
    this(cacheConf.blockCache, cacheConf.cacheDataOnRead, cacheConf.inMemory,
        cacheConf.cacheDataOnFlush, cacheConf.cacheIndexesOnWrite,
        cacheConf.cacheBloomsOnWrite, cacheConf.evictOnClose,
        cacheConf.cacheCompressed, cacheConf.cacheOnCompaction,
        cacheConf.cacheOnCompactionThreshold);
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

  public static BlockCache getGlobalBlockCache() {
    return globalBlockCache;
  }
  
  /**
   * Returns whether the blocks of this HFile should be cached on read or not.
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
            category == BlockCategory.BLOOM);
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
  public boolean shouldCacheDataOnFlush() {
    return isBlockCacheEnabled() && this.cacheDataOnFlush;
  }

  /**
   * Only used for testing.
   * @param cacheDataOnWrite whether data blocks should be written to the cache
   *                         when an HFile is written
   */
  public void setCacheDataOnFlush(boolean cacheDataOnFlush) {
    this.cacheDataOnFlush = cacheDataOnFlush;
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

  public boolean shouldCacheOnCompaction() {
    return isBlockCacheEnabled() && cacheOnCompaction;
  }

  public void setCacheDataOnRead(final boolean cacheDataOnRead) {
    this.cacheDataOnRead = cacheDataOnRead;
  }

  public void setCacheOnCompaction(final boolean cacheOnCompaction) {
    this.cacheOnCompaction = cacheOnCompaction;
  }

  public float getCacheOnCompactionThreshold() {
    return cacheOnCompactionThreshold;
  }

  @Override
  public String toString() {
    if (!isBlockCacheEnabled()) {
      return "CacheConfig:disabled";
    }
    return "CacheConfig:enabled " +
      "[cacheDataOnRead=" + shouldCacheDataOnRead() + "] " +
      "[cacheDataOnWrite=" + shouldCacheDataOnFlush() + "] " +
      "[cacheIndexesOnWrite=" + shouldCacheIndexesOnWrite() + "] " +
      "[cacheBloomsOnWrite=" + shouldCacheBloomsOnWrite() + "] " +
      "[cacheEvictOnClose=" + shouldEvictOnClose() + "] " +
      "[cacheCompressed=" + shouldCacheCompressed() + "] " +
      "[cacheOnCompaction=" + shouldCacheOnCompaction() + "] " +
      "[cacheOnCompactionThreshold=" + getCacheOnCompactionThreshold() + "]";
  }

  // Static block cache reference and methods

  /**
   * Static reference to the block cache, or null if no caching should be used
   * at all.
   */
  private static BlockCache globalBlockCache;

  /** Boolean whether we have disabled the block cache entirely. */
  private static boolean blockCacheDisabled = false;

  /**
   * Returns the block cache or <code>null</code> in case none should be used.
   *
   * @param conf  The current configuration.
   * @return The block cache or <code>null</code>.
   */
  private static synchronized BlockCache instantiateBlockCache(
      Configuration conf) {
    if (globalBlockCache != null) return globalBlockCache;
    if (blockCacheDisabled) return null;

    float cachePercentage = conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    if (cachePercentage == 0L) {
      blockCacheDisabled = true;
      return null;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY +
        " must be between 0.0 and 1.0, not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long cacheSize = (long)(mu.getMax() * cachePercentage);
    LOG.info("Allocating LruBlockCache with maximum size " +
      StringUtils.humanReadableInt(cacheSize));
    globalBlockCache = new LruBlockCache(cacheSize,
        StoreFile.DEFAULT_BLOCKSIZE_SMALL);
    return globalBlockCache;
  }
}
