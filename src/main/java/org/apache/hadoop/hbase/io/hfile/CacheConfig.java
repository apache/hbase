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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.IOEngine;
import org.apache.hadoop.hbase.util.Strings;

import java.util.Arrays;

/**
 * Stores all of the cache objects and configuration for a single HFile.
 */
public class CacheConfig implements ConfigurationObserver {
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
   * IO engine to be used by the L2 cache. Current accepted settings:
   * "heap", and "offheap". If omitted, L2 cache will be disabled.
   * @see IOEngine
   */
  public static final String L2_BUCKET_CACHE_IOENGINE_KEY =
      "hfile.l2.bucketcache.ioengine";

  /**
   * Size of the L2 cache. If < 1.0, this is taken as a percentage of available
   * (depending on configuration) direct or heap memory, otherwise it is taken
   * as an absolute size in kb. If omitted or set to 0, L2 cache will be
   * disabled.
   */
  public static final String L2_BUCKET_CACHE_SIZE_KEY =
      "hfile.l2.bucketcache.size";

  /**
   * Number of writer threads for the BucketCache instance used for the L2
   * cache.
   * @see L2BucketCache
   */
  public static final String L2_BUCKET_CACHE_WRITER_THREADS_KEY =
      "hfile.l2.bucketcache.writer.threads";

  /**
   * Maximum length of the write queue for the BucketCache instance used for
   * the L2 cache.
   * @see L2BucketCache
   */
  public static final String L2_BUCKET_CACHE_QUEUE_KEY =
      "hfile.l2.bucketcache.queue.length";

  /**
   * If L2 cache is enabled, this configuration key controls whether or not
   * blocks are cached upon writes. Default: true if L2 cache is enabled.
   */
  public static final String L2_CACHE_BLOCKS_ON_FLUSH_KEY =
      "hfile.l2.cacheblocksonwrite";

  /**
   * Configuration key to enable evicting all blocks associated with an hfile
   * after this hfile has been compacted. Default: true.
   */
  public static final String L2_EVICT_ON_CLOSE_KEY =
      "hfile.l2.evictblocksonclose";

  /**
   * Configuration key to evict keys from the L2 cache once they have been
   * cached in the L1 cache (i.e., the regular Lru Block Cache). Default: false.
   * <b>Not yet implemented!</b>
   *
   * TODO (avf): needs to be implemented to be honoured correctly
   */
  public static final String L2_EVICT_ON_PROMOTION_KEY =
      "hfile.l2.evictblocksonpromotion";

  /**
   * Duration that BucketCache instanced used by the L2 cache will tolerate IO
   * errors until the cache is disabled. Default: 1 minute.
   */
  public static final String L2_BUCKET_CACHE_IOENGINE_ERRORS_TOLERATED_DURATION_KEY =
      "hfile.l2.bucketcache.ioengine.errors.tolerated.duration";

  /**
   * Size of individual buckets for the bucket allocator. This is expected to
   * a small (e.g., 5-15 items) list of comma-separated integer values
   * centered around the size (in bytes) of an average compressed block.
   */
  public static final String L2_BUCKET_CACHE_BUCKET_SIZES_KEY =
      "hfile.l2.bucketcache.bucket.sizes";

  /**
   * Size (in bytes) of an individual buffer inside ByteBufferArray which is used
   * by the ByteBufferIOEngine. Default is 4mb. Larger byte buffers might mean
   * greater lock contention; smaller byte buffer means more operations per
   * write and higher memory overhead for maintaining per-buffer locks.
   */
  public static final String L2_BUCKET_CACHE_BUFFER_SIZE_KEY =
      "hfile.l2.bucketcache.buffer.size";

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
  public static final boolean DEFAULT_L2_CACHE_BLOCKS_ON_FLUSH = true;
  public static final boolean DEFAULT_L2_EVICT_ON_PROMOTION = false;
  public static final boolean DEFAULT_L2_EVICT_ON_CLOSE = true;
  public static final int[] DEFAULT_L2_BUCKET_CACHE_BUCKET_SIZES =
      { 4 * 1024 + 1024, 8 * 1024 + 1024,
          16 * 1024 + 1024, 32 * 1024 + 1024, 40 * 1024 + 1024, 48 * 1024 + 1024,
          56 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024, 128 * 1024 + 1024,
          192 * 1024 + 1024, 256 * 1024 + 1024, 384 * 1024 + 1024,
          512 * 1024 + 1024 };
  public static final int DEFAULT_L2_BUCKET_CACHE_BUFFER_SIZE = 4 * 1024 * 1024;

  /** Local reference to the block cache, null if completely disabled */
  private final BlockCache blockCache;

  /** Local reference to the l2 cache, null if disabled */
  private final L2Cache l2Cache;

  /**                                                            nfig
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

  /** Size of the L2 cache */
  private volatile float l2CacheSize;

  /** Whether to cache all blocks on write in the L2 cache */
  private volatile boolean l2CacheDataOnWrite;

  /**
   * TODO (avf): once implemented, whether to evict blocks from the L2
   *             cache once they have been cached in the L1 cache
   */
  private volatile boolean l2EvictOnPromotion;

  /**
   * Whether blocks of a file should be evicted from the L2 cache when the file
   * is closed.
   */
  private volatile boolean l2EvictOnClose;

  /**
   * Parses a comma separated list of bucket sizes and returns a list of
   * sorted integers representing the various bucket sizes. Bucket sizes should
   * roughly correspond to block sizes encountered in production.
   * @param conf The configuration
   * @return
   */
  public static int[] getL2BucketSizes(Configuration conf) {
    String[] bucketSizesStr = conf.getStrings(L2_BUCKET_CACHE_BUCKET_SIZES_KEY);
    if (bucketSizesStr == null) {
      return DEFAULT_L2_BUCKET_CACHE_BUCKET_SIZES;
    }
    int[] retVal = new int[bucketSizesStr.length];
    for (int i = 0; i < bucketSizesStr.length; ++i) {
      try {
        retVal[i] = Integer.valueOf(bucketSizesStr[i]);
      } catch (NumberFormatException nfe) {
        LOG.fatal("Error parsing value " + bucketSizesStr[i], nfe);
        throw nfe;
      }
    }
    Arrays.sort(retVal);
    return retVal;
  }

  /**
   * Create a cache configuration using the specified configuration object and
   * defaults for family level settings.
   * @param conf hbase configuration
   */
  public CacheConfig(Configuration conf) {
    this(LruBlockCacheFactory.getInstance().getBlockCache(conf),
        L2BucketCacheFactory.getInstance().getL2Cache(conf),
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
                DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD),
        conf.getFloat(L2_BUCKET_CACHE_SIZE_KEY, 0F),
        conf.getBoolean(L2_CACHE_BLOCKS_ON_FLUSH_KEY,
            DEFAULT_L2_CACHE_BLOCKS_ON_FLUSH),
        conf.getBoolean(L2_EVICT_ON_PROMOTION_KEY, DEFAULT_L2_EVICT_ON_PROMOTION),
        conf.getBoolean(L2_EVICT_ON_CLOSE_KEY, DEFAULT_L2_EVICT_ON_CLOSE)
     );
  }

  /**
   * Create a block cache configuration with the specified cache and
   * configuration parameters.
   * @param blockCache reference to block cache, null if completely disabled
   * @param cacheDataOnRead whether data blocks should be cached on read
   * @param inMemory whether blocks should be flagged as in-memory
   * @param cacheDataOnFlush whether data blocks should be cached on write
   * @param cacheIndexesOnWrite whether index blocks should be cached on write
   * @param cacheBloomsOnWrite whether blooms should be cached on write
   * @param evictOnClose whether blocks should be evicted when HFile is closed
   * @param cacheCompressed whether to store blocks as compressed in the cache
   * @param cacheOnCompaction whether to cache blocks during compaction
   * @param cacheOnCompactionThreshold threshold used of caching of blocks during compaction
   * @param l2CacheSize size of the L2 cache
   * @param l2CacheDataOnWrite whether blocks should be cached on write
   * @param l2EvictOnPromotion whether blocks should be evicted from L2 upon promotion
   * @param l2EvictOnClose whether blocks should be evicted from L2 when HFile is closed
   */
  CacheConfig(final BlockCache blockCache, final L2Cache l2Cache,
      final boolean cacheDataOnRead, final boolean inMemory,
      final boolean cacheDataOnFlush, final boolean cacheIndexesOnWrite,
      final boolean cacheBloomsOnWrite, final boolean evictOnClose,
      final boolean cacheCompressed, final boolean cacheOnCompaction,
      final float cacheOnCompactionThreshold, final float l2CacheSize,
      final boolean l2CacheDataOnWrite, final boolean l2EvictOnPromotion,
      final boolean l2EvictOnClose) {
    this.blockCache = blockCache;
    this.l2Cache = l2Cache;
    this.cacheDataOnRead = cacheDataOnRead;
    this.inMemory = inMemory;
    this.cacheDataOnFlush = cacheDataOnFlush;
    this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    this.evictOnClose = evictOnClose;
    this.cacheCompressed = cacheCompressed;
    this.cacheOnCompaction = cacheOnCompaction;
    this.cacheOnCompactionThreshold = cacheOnCompactionThreshold;
    this.l2CacheSize = l2CacheSize;
    this.l2CacheDataOnWrite = l2CacheDataOnWrite;
    this.l2EvictOnPromotion = l2EvictOnPromotion;
    this.l2EvictOnClose = l2EvictOnClose;
  }

  /**
   * Constructs a cache configuration copied from the specified configuration.
   * @param cacheConf
   */
  public CacheConfig(CacheConfig cacheConf) {
    this(cacheConf.blockCache, cacheConf.l2Cache,
        cacheConf.cacheDataOnRead, cacheConf.inMemory,
        cacheConf.cacheDataOnFlush, cacheConf.cacheIndexesOnWrite,
        cacheConf.cacheBloomsOnWrite, cacheConf.evictOnClose,
        cacheConf.cacheCompressed, cacheConf.cacheOnCompaction,
        cacheConf.cacheOnCompactionThreshold, cacheConf.l2CacheSize,
        cacheConf.l2CacheDataOnWrite, cacheConf.l2EvictOnPromotion,
        cacheConf.l2EvictOnClose);
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
   * @param cacheDataOnFlush whether data blocks should be written to the cache
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

  public void setL2CacheDataOnWrite(final boolean l2CacheDataOnWrite) {
    this.l2CacheDataOnWrite = l2CacheDataOnWrite;
  }

  public void setL2EvictOnPromotion(final boolean l2EvictOnPromotion) {
    this.l2EvictOnPromotion = l2EvictOnPromotion;
  }

  public void setL2EvictOnClose(final boolean l2EvictOnClose) {
    this.l2EvictOnClose = l2EvictOnClose;
  }

  public boolean shouldL2CacheDataOnWrite() {
    return l2CacheDataOnWrite;
  }

  public boolean shouldL2EvictOnPromotion() {
    return l2EvictOnPromotion;
  }

  public boolean shouldL2EvictOnClose() {
    return l2EvictOnClose;
  }

  @Override
  public String toString() {
    if (!isBlockCacheEnabled() && !isL2CacheEnabled()) {
      return "CacheConfig:disabled";
    }
    StringBuilder sb = new StringBuilder("CacheConfig:enabled");
    if (isL2CacheEnabled()) {
      sb = Strings.appendKeyValue(sb, "l2CacheDataOnWrite",
          shouldL2CacheDataOnWrite());
      sb = Strings.appendKeyValue(sb, "l2EvictOnClose", shouldL2EvictOnClose());
      sb = Strings.appendKeyValue(sb, "l2EvictOnPromotion",
          shouldL2EvictOnPromotion());
    }
    if (isBlockCacheEnabled()) {
      sb = Strings.appendKeyValue(sb, "cacheDataOnRead", shouldCacheDataOnRead());
      sb = Strings.appendKeyValue(sb, "cacheDataOnWrite",
          shouldCacheDataOnFlush());
      sb = Strings.appendKeyValue(sb, "cacheIndexesOnWrite",
          shouldCacheIndexesOnWrite());
      sb = Strings.appendKeyValue(sb, "cacheBloomsOnWrite",
          shouldCacheBloomsOnWrite());
      sb = Strings.appendKeyValue(sb, "cacheEvictOnClose", shouldEvictOnClose());
      sb = Strings.appendKeyValue(sb, "cacheCompressed", shouldCacheCompressed());
      sb = Strings.appendKeyValue(sb, "cacheOnCompaction",
          shouldCacheOnCompaction());
      sb = Strings.appendKeyValue(sb, "cacheOnCompactionThreshold",
          getCacheOnCompactionThreshold());
    }
    return sb.toString();
  }

  public boolean isL2CacheEnabled() {
    return l2Cache != null && !l2Cache.isShutdown();
  }

  public L2Cache getL2Cache() {
    return l2Cache;
  }

  @Override
  public void notifyOnChange(Configuration conf) {
    new CacheConfigBuilder(conf).update(this);
  }

  /**
   * A builder for the CacheConfig class. Used to avoid having multiple
   * constructors.
   */
  public static class CacheConfigBuilder {
    private final L2CacheFactory l2CacheFactory;
    private final BlockCacheFactory blockCacheFactory;
    private final Configuration conf;

    private BlockCache blockCache;
    private L2Cache l2Cache;
    private boolean cacheDataOnRead = DEFAULT_CACHE_DATA_ON_READ;
    private boolean inMemory = DEFAULT_IN_MEMORY;
    private boolean cacheDataOnFlush;
    private boolean cacheIndexesOnWrite;
    private boolean cacheBloomsOnWrite;
    private boolean evictOnClose;
    private boolean cacheCompressed;
    private boolean cacheOnCompaction;
    private float cacheOnCompactionThreshold;
    private float l2CacheSize;
    private boolean l2CacheDataOnWrite;
    private boolean l2EvictOnPromotion;
    private boolean l2EvictOnClose;

    /**
     * Creates an empty builder, to be used for unit tests.
     */
    public CacheConfigBuilder() {
      this(null, null, null);
    }

    /**
     * Use {@link LruBlockCacheFactory} to construct the block cache and
     * {@link L2BucketCacheFactory} to construct the L2 cache. Reads all
     * other cache configuration from the specified configuration object.
     * @param conf The HBase configuration that contains cache related settings
     */
    public CacheConfigBuilder(Configuration conf) {
      this(conf, LruBlockCacheFactory.getInstance(),
          L2BucketCacheFactory.getInstance());
    }

    /**
     * Uses the specified {@link BlockCacheFactory} and {@link L2CacheFactory}
     * to constructor the respective caches. Reads all cache configuration
     * from the specified configuration object.
     * @param conf The HBase configuration that contains cache related settings
     * @param blockCacheFactory The factory used to construct the block cache
     * @param l2CacheFactory The factory used to construct the l2 cache
     */
    public CacheConfigBuilder(Configuration conf,
        BlockCacheFactory blockCacheFactory, L2CacheFactory l2CacheFactory) {
      this.blockCacheFactory = blockCacheFactory;
      this.l2CacheFactory = l2CacheFactory;
      this.conf = conf;
      cacheDataOnFlush = conf.getBoolean(
          CACHE_BLOCKS_ON_FLUSH_KEY, DEFAULT_CACHE_DATA_ON_FLUSH);
      cacheIndexesOnWrite = conf.getBoolean(
          CACHE_INDEX_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_INDEXES_ON_WRITE);
      cacheBloomsOnWrite = conf.getBoolean(
          CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_BLOOMS_ON_WRITE);
      evictOnClose = conf.getBoolean(
          EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE);
      cacheCompressed = conf.getBoolean(
          CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_COMPRESSED_CACHE);
      cacheOnCompaction = conf.getBoolean(
          CACHE_DATA_BLOCKS_ON_COMPACTION, DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION);
      cacheOnCompactionThreshold = conf.getFloat(
          CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD,
          DEFAULT_CACHE_DATA_BLOCKS_ON_COMPACTION_THRESHOLD);
      l2CacheSize = conf.getFloat(L2_BUCKET_CACHE_SIZE_KEY, 0F);
      l2CacheDataOnWrite = conf.getBoolean(
          L2_CACHE_BLOCKS_ON_FLUSH_KEY, DEFAULT_L2_CACHE_BLOCKS_ON_FLUSH);
      l2EvictOnPromotion = conf.getBoolean(
          L2_EVICT_ON_PROMOTION_KEY, DEFAULT_L2_EVICT_ON_PROMOTION);
      l2EvictOnClose = conf.getBoolean(
          L2_EVICT_ON_CLOSE_KEY, DEFAULT_L2_EVICT_ON_CLOSE);
    }

    public CacheConfigBuilder withBlockCache(BlockCache blockCache) {
      this.blockCache = blockCache;
      return this;
    }

    public CacheConfigBuilder withL2Cache(L2Cache l2Cache) {
      this.l2Cache = l2Cache;
      return this;
    }

    public CacheConfigBuilder withCacheDataOnRead(boolean cacheDataOnRead) {
      this.cacheDataOnRead = cacheDataOnRead;
      return this;
    }

    public CacheConfigBuilder withInMemory(boolean inMemory) {
      this.inMemory = inMemory;
      return this;
    }

    public CacheConfigBuilder withCacheDataOnFlush(boolean cacheDataOnFlush) {
      this.cacheDataOnFlush = cacheDataOnFlush;
      return this;
    }

    public CacheConfigBuilder withCacheIndexesOnWrite(boolean cacheIndexesOnWrite) {
      this.cacheIndexesOnWrite = cacheIndexesOnWrite;
      return this;
    }

    public CacheConfigBuilder withCacheBloomsOnWrite(boolean cacheBloomsOnWrite) {
      this.cacheBloomsOnWrite = cacheBloomsOnWrite;
      return this;
    }

    public CacheConfigBuilder withEvictOnClose(boolean evictOnClose) {
      this.evictOnClose = evictOnClose;
      return this;
    }

    public CacheConfigBuilder withCacheCompressed(boolean cacheCompressed){
      this.cacheCompressed = cacheCompressed;
      return this;
    }

    public CacheConfigBuilder withCacheOnCompaction(boolean cacheOnCompaction) {
      this.cacheOnCompaction = cacheOnCompaction;
      return this;
    }

    public CacheConfigBuilder withCacheOnCompactionThreshold(
        float cacheOnCompactionThreshold) {
      this.cacheOnCompactionThreshold = cacheOnCompactionThreshold;
      return this;
    }

    public CacheConfigBuilder withL2CacheDataOnWrite(boolean l2CacheDataOnFlush) {
      this.l2CacheDataOnWrite = l2CacheDataOnFlush;
      return this;
    }

    public CacheConfigBuilder withL2EvictOnPromotion(boolean l2EvictOnPromotion) {
      this.l2EvictOnPromotion = l2EvictOnPromotion;
      return this;
    }

    public CacheConfigBuilder withL2EvictOnClose(boolean l2EvictOnClose){
      this.l2EvictOnClose = l2EvictOnClose;
      return this;
    }

    /**
     * Create a cache configuration based on the current state of this
     * builder object. If the block cache and/or L2 cache have not been
     * explicitly passed in using the builder methods, use the respective
     * factories to create these caches.
     * @return The fully instantiated and configured {@link CacheConfig}
     *          instance
     */
    public CacheConfig build() {
      if (blockCache == null && blockCacheFactory != null) {
        blockCache = blockCacheFactory.getBlockCache(conf);
      }
      if (l2Cache == null && l2CacheFactory != null) {
        l2Cache = l2CacheFactory.getL2Cache(conf);
      }
      return new CacheConfig(blockCache, l2Cache,
          cacheDataOnRead, inMemory,
          cacheDataOnFlush, cacheIndexesOnWrite,
          cacheBloomsOnWrite, evictOnClose,
          cacheCompressed, cacheOnCompaction,
          cacheOnCompactionThreshold, l2CacheSize,
          l2CacheDataOnWrite, l2EvictOnPromotion,
          l2EvictOnClose);
    }

    public CacheConfig update(CacheConfig cacheConf) {
      if (l2CacheDataOnWrite != cacheConf.shouldL2CacheDataOnWrite()) {
        LOG.info("Updating " + CacheConfig.L2_CACHE_BLOCKS_ON_FLUSH_KEY +
                " from " + cacheConf.shouldL2CacheDataOnWrite() + " to " +
                l2CacheDataOnWrite);
        cacheConf.setL2CacheDataOnWrite(l2CacheDataOnWrite);
      }
      if (l2EvictOnPromotion != cacheConf.shouldL2EvictOnPromotion()) {
        LOG.info("Updating " + CacheConfig.L2_EVICT_ON_PROMOTION_KEY +
                " from " + cacheConf.shouldL2EvictOnPromotion() + " to " +
                l2EvictOnPromotion);
        cacheConf.setL2EvictOnPromotion(l2EvictOnPromotion);
      }
      if (l2EvictOnClose != cacheConf.shouldL2EvictOnClose()) {
        LOG.info("Updating " + CacheConfig.L2_EVICT_ON_CLOSE_KEY + " from " +
                cacheConf.shouldL2EvictOnClose() + " to " + l2EvictOnClose);
        cacheConf.setL2EvictOnClose(l2EvictOnClose);
      }
      if (l2CacheSize == 0F && cacheConf.isL2CacheEnabled()) {
        cacheConf.getL2Cache().shutdown();
        LOG.info("L2 cache disabled");
      }
      return cacheConf;
    }
  }
}
