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

import java.lang.ref.WeakReference;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.com.google.common.base.Objects;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A block cache implementation that is memory-aware using {@link HeapSize},
 * memory-bound using an LRU eviction algorithm, and concurrent: backed by a
 * {@link ConcurrentHashMap} and with a non-blocking eviction thread giving
 * constant-time {@link #cacheBlock} and {@link #getBlock} operations.<p>
 *
 * Contains three levels of block priority to allow for scan-resistance and in-memory families
 * {@link org.apache.hadoop.hbase.HColumnDescriptor#setInMemory(boolean)} (An in-memory column
 * family is a column family that should be served from memory if possible):
 * single-access, multiple-accesses, and in-memory priority.
 * A block is added with an in-memory priority flag if
 * {@link org.apache.hadoop.hbase.HColumnDescriptor#isInMemory()}, otherwise a block becomes a
 * single access priority the first time it is read into this block cache.  If a block is
 * accessed again while in cache, it is marked as a multiple access priority block.  This
 * delineation of blocks is used to prevent scans from thrashing the cache adding a
 * least-frequently-used element to the eviction algorithm.<p>
 *
 * Each priority is given its own chunk of the total cache to ensure
 * fairness during eviction.  Each priority will retain close to its maximum
 * size, however, if any priority is not using its entire chunk the others
 * are able to grow beyond their chunk size.<p>
 *
 * Instantiated at a minimum with the total size and average block size.
 * All sizes are in bytes.  The block size is not especially important as this
 * cache is fully dynamic in its sizing of blocks.  It is only used for
 * pre-allocating data structures and in initial heap estimation of the map.<p>
 *
 * The detailed constructor defines the sizes for the three priorities (they
 * should total to the <code>maximum size</code> defined).  It also sets the levels that
 * trigger and control the eviction thread.<p>
 *
 * The <code>acceptable size</code> is the cache size level which triggers the eviction
 * process to start.  It evicts enough blocks to get the size below the
 * minimum size specified.<p>
 *
 * Eviction happens in a separate thread and involves a single full-scan
 * of the map.  It determines how many bytes must be freed to reach the minimum
 * size, and then while scanning determines the fewest least-recently-used
 * blocks necessary from each of the three priorities (would be 3 times bytes
 * to free).  It then uses the priority chunk sizes to evict fairly according
 * to the relative sizes and usage.
 */
@InterfaceAudience.Private
public class LruBlockCache implements FirstLevelBlockCache {

  private static final Logger LOG = LoggerFactory.getLogger(LruBlockCache.class);

  /**
   * Percentage of total size that eviction will evict until; e.g. if set to .8, then we will keep
   * evicting during an eviction run till the cache size is down to 80% of the total.
   */
  private static final String LRU_MIN_FACTOR_CONFIG_NAME = "hbase.lru.blockcache.min.factor";

  /**
   * Acceptable size of cache (no evictions if size < acceptable)
   */
  private static final String LRU_ACCEPTABLE_FACTOR_CONFIG_NAME =
      "hbase.lru.blockcache.acceptable.factor";

  /**
   * Hard capacity limit of cache, will reject any put if size > this * acceptable
   */
  static final String LRU_HARD_CAPACITY_LIMIT_FACTOR_CONFIG_NAME =
      "hbase.lru.blockcache.hard.capacity.limit.factor";
  private static final String LRU_SINGLE_PERCENTAGE_CONFIG_NAME =
      "hbase.lru.blockcache.single.percentage";
  private static final String LRU_MULTI_PERCENTAGE_CONFIG_NAME =
      "hbase.lru.blockcache.multi.percentage";
  private static final String LRU_MEMORY_PERCENTAGE_CONFIG_NAME =
      "hbase.lru.blockcache.memory.percentage";

  /**
   * Configuration key to force data-block always (except in-memory are too much)
   * cached in memory for in-memory hfile, unlike inMemory, which is a column-family
   * configuration, inMemoryForceMode is a cluster-wide configuration
   */
  private static final String LRU_IN_MEMORY_FORCE_MODE_CONFIG_NAME =
      "hbase.lru.rs.inmemoryforcemode";

  /* Default Configuration Parameters*/

  /* Backing Concurrent Map Configuration */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  static final int DEFAULT_CONCURRENCY_LEVEL = 16;

  /* Eviction thresholds */
  private static final float DEFAULT_MIN_FACTOR = 0.95f;
  static final float DEFAULT_ACCEPTABLE_FACTOR = 0.99f;

  /* Priority buckets */
  private static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  private static final float DEFAULT_MULTI_FACTOR = 0.50f;
  private static final float DEFAULT_MEMORY_FACTOR = 0.25f;

  private static final float DEFAULT_HARD_CAPACITY_LIMIT_FACTOR = 1.2f;

  private static final boolean DEFAULT_IN_MEMORY_FORCE_MODE = false;

  /* Statistics thread */
  private static final int STAT_THREAD_PERIOD = 60 * 5;
  private static final String LRU_MAX_BLOCK_SIZE = "hbase.lru.max.block.size";
  private static final long DEFAULT_MAX_BLOCK_SIZE = 16L * 1024L * 1024L;

  /**
   * Defined the cache map as {@link ConcurrentHashMap} here, because in
   * {@link LruBlockCache#getBlock}, we need to guarantee the atomicity of map#computeIfPresent
   * (key, func). Besides, the func method must execute exactly once only when the key is present
   * and under the lock context, otherwise the reference count will be messed up. Notice that the
   * {@link java.util.concurrent.ConcurrentSkipListMap} can not guarantee that.
   */
  private transient final ConcurrentHashMap<BlockCacheKey, LruCachedBlock> map;

  /** Eviction lock (locked when eviction in process) */
  private transient final ReentrantLock evictionLock = new ReentrantLock(true);

  private final long maxBlockSize;

  /** Volatile boolean to track if we are in an eviction process or not */
  private volatile boolean evictionInProgress = false;

  /** Eviction thread */
  private transient final EvictionThread evictionThread;

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private transient final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setNameFormat("LruBlockCacheStatsExecutor").setDaemon(true).build());

  /** Current size of cache */
  private final AtomicLong size;

  /** Current size of data blocks */
  private final LongAdder dataBlockSize;

  /** Current number of cached elements */
  private final AtomicLong elements;

  /** Current number of cached data block elements */
  private final LongAdder dataBlockElements;

  /** Cache access count (sequential ID) */
  private final AtomicLong count;

  /** hard capacity limit */
  private float hardCapacityLimitFactor;

  /** Cache statistics */
  private final CacheStats stats;

  /** Maximum allowable size of cache (block put if size > max, evict) */
  private long maxSize;

  /** Approximate block size */
  private long blockSize;

  /** Acceptable size of cache (no evictions if size < acceptable) */
  private float acceptableFactor;

  /** Minimum threshold of cache (when evicting, evict until size < min) */
  private float minFactor;

  /** Single access bucket size */
  private float singleFactor;

  /** Multiple access bucket size */
  private float multiFactor;

  /** In-memory bucket size */
  private float memoryFactor;

  /** Overhead of the structure itself */
  private long overhead;

  /** Whether in-memory hfile's data block has higher priority when evicting */
  private boolean forceInMemory;

  /**
   * Where to send victims (blocks evicted/missing from the cache). This is used only when we use an
   * external cache as L2.
   * Note: See org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache
   */
  private transient BlockCache victimHandler = null;

  /**
   * Default constructor.  Specify maximum size and expected average block
   * size (approximation is fine).
   *
   * <p>All other factors will be calculated based on defaults specified in
   * this class.
   *
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   */
  public LruBlockCache(long maxSize, long blockSize) {
    this(maxSize, blockSize, true);
  }

  /**
   * Constructor used for testing.  Allows disabling of the eviction thread.
   */
  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread) {
    this(maxSize, blockSize, evictionThread,
        (int) Math.ceil(1.2 * maxSize / blockSize),
        DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL,
        DEFAULT_MIN_FACTOR, DEFAULT_ACCEPTABLE_FACTOR,
        DEFAULT_SINGLE_FACTOR,
        DEFAULT_MULTI_FACTOR,
        DEFAULT_MEMORY_FACTOR,
        DEFAULT_HARD_CAPACITY_LIMIT_FACTOR,
        false,
        DEFAULT_MAX_BLOCK_SIZE);
  }

  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread, Configuration conf) {
    this(maxSize, blockSize, evictionThread,
        (int) Math.ceil(1.2 * maxSize / blockSize),
        DEFAULT_LOAD_FACTOR,
        DEFAULT_CONCURRENCY_LEVEL,
        conf.getFloat(LRU_MIN_FACTOR_CONFIG_NAME, DEFAULT_MIN_FACTOR),
        conf.getFloat(LRU_ACCEPTABLE_FACTOR_CONFIG_NAME, DEFAULT_ACCEPTABLE_FACTOR),
        conf.getFloat(LRU_SINGLE_PERCENTAGE_CONFIG_NAME, DEFAULT_SINGLE_FACTOR),
        conf.getFloat(LRU_MULTI_PERCENTAGE_CONFIG_NAME, DEFAULT_MULTI_FACTOR),
        conf.getFloat(LRU_MEMORY_PERCENTAGE_CONFIG_NAME, DEFAULT_MEMORY_FACTOR),
        conf.getFloat(LRU_HARD_CAPACITY_LIMIT_FACTOR_CONFIG_NAME,
                      DEFAULT_HARD_CAPACITY_LIMIT_FACTOR),
        conf.getBoolean(LRU_IN_MEMORY_FORCE_MODE_CONFIG_NAME, DEFAULT_IN_MEMORY_FORCE_MODE),
        conf.getLong(LRU_MAX_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE));
  }

  public LruBlockCache(long maxSize, long blockSize, Configuration conf) {
    this(maxSize, blockSize, true, conf);
  }

  /**
   * Configurable constructor.  Use this constructor if not using defaults.
   *
   * @param maxSize             maximum size of this cache, in bytes
   * @param blockSize           expected average size of blocks, in bytes
   * @param evictionThread      whether to run evictions in a bg thread or not
   * @param mapInitialSize      initial size of backing ConcurrentHashMap
   * @param mapLoadFactor       initial load factor of backing ConcurrentHashMap
   * @param mapConcurrencyLevel initial concurrency factor for backing CHM
   * @param minFactor           percentage of total size that eviction will evict until
   * @param acceptableFactor    percentage of total size that triggers eviction
   * @param singleFactor        percentage of total size for single-access blocks
   * @param multiFactor         percentage of total size for multiple-access blocks
   * @param memoryFactor        percentage of total size for in-memory blocks
   */
  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread,
      int mapInitialSize, float mapLoadFactor, int mapConcurrencyLevel,
      float minFactor, float acceptableFactor, float singleFactor,
      float multiFactor, float memoryFactor, float hardLimitFactor,
      boolean forceInMemory, long maxBlockSize) {
    this.maxBlockSize = maxBlockSize;
    if(singleFactor + multiFactor + memoryFactor != 1 ||
        singleFactor < 0 || multiFactor < 0 || memoryFactor < 0) {
      throw new IllegalArgumentException("Single, multi, and memory factors " +
          " should be non-negative and total 1.0");
    }
    if (minFactor >= acceptableFactor) {
      throw new IllegalArgumentException("minFactor must be smaller than acceptableFactor");
    }
    if (minFactor >= 1.0f || acceptableFactor >= 1.0f) {
      throw new IllegalArgumentException("all factors must be < 1");
    }
    this.maxSize = maxSize;
    this.blockSize = blockSize;
    this.forceInMemory = forceInMemory;
    map = new ConcurrentHashMap<>(mapInitialSize, mapLoadFactor, mapConcurrencyLevel);
    this.minFactor = minFactor;
    this.acceptableFactor = acceptableFactor;
    this.singleFactor = singleFactor;
    this.multiFactor = multiFactor;
    this.memoryFactor = memoryFactor;
    this.stats = new CacheStats(this.getClass().getSimpleName());
    this.count = new AtomicLong(0);
    this.elements = new AtomicLong(0);
    this.dataBlockElements = new LongAdder();
    this.dataBlockSize = new LongAdder();
    this.overhead = calculateOverhead(maxSize, blockSize, mapConcurrencyLevel);
    this.size = new AtomicLong(this.overhead);
    this.hardCapacityLimitFactor = hardLimitFactor;
    if (evictionThread) {
      this.evictionThread = new EvictionThread(this);
      this.evictionThread.start(); // FindBugs SC_START_IN_CTOR
    } else {
      this.evictionThread = null;
    }
    // TODO: Add means of turning this off.  Bit obnoxious running thread just to make a log
    // every five minutes.
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this), STAT_THREAD_PERIOD,
                                                STAT_THREAD_PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public void setVictimCache(BlockCache victimCache) {
    if (victimHandler != null) {
      throw new IllegalArgumentException("The victim cache has already been set");
    }
    victimHandler = requireNonNull(victimCache);
  }

  @Override
  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    if (this.size.get() > acceptableSize() && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * The block cached in LRUBlockCache will always be an heap block: on the one side, the heap
   * access will be more faster then off-heap, the small index block or meta block cached in
   * CombinedBlockCache will benefit a lot. on other side, the LRUBlockCache size is always
   * calculated based on the total heap size, if caching an off-heap block in LRUBlockCache, the
   * heap size will be messed up. Here we will clone the block into an heap block if it's an
   * off-heap block, otherwise just use the original block. The key point is maintain the refCnt of
   * the block (HBASE-22127): <br>
   * 1. if cache the cloned heap block, its refCnt is an totally new one, it's easy to handle; <br>
   * 2. if cache the original heap block, we're sure that it won't be tracked in ByteBuffAllocator's
   * reservoir, if both RPC and LRUBlockCache release the block, then it can be garbage collected by
   * JVM, so need a retain here.
   * @param buf the original block
   * @return an block with an heap memory backend.
   */
  private Cacheable asReferencedHeapBlock(Cacheable buf) {
    if (buf instanceof HFileBlock) {
      HFileBlock blk = ((HFileBlock) buf);
      if (blk.isSharedMem()) {
        return HFileBlock.deepCloneOnHeap(blk);
      }
    }
    // The block will be referenced by this LRUBlockCache, so should increase its refCnt here.
    return buf.retain();
  }

  // BlockCache implementation

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NOT be called on an already cached block. In rare cases (HBASE-8547)
   * this can happen, for which we compare the buffer contents.
   *
   * @param cacheKey block's cache key
   * @param buf      block buffer
   * @param inMemory if block is in-memory
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    if (buf.heapSize() > maxBlockSize) {
      // If there are a lot of blocks that are too
      // big this can make the logs way too noisy.
      // So we log 2%
      if (stats.failInsert() % 50 == 0) {
        LOG.warn("Trying to cache too large a block "
            + cacheKey.getHfileName() + " @ "
            + cacheKey.getOffset()
            + " is " + buf.heapSize()
            + " which is larger than " + maxBlockSize);
      }
      return;
    }

    LruCachedBlock cb = map.get(cacheKey);
    if (cb != null && !BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf)) {
      return;
    }
    long currentSize = size.get();
    long currentAcceptableSize = acceptableSize();
    long hardLimitSize = (long) (hardCapacityLimitFactor * currentAcceptableSize);
    if (currentSize >= hardLimitSize) {
      stats.failInsert();
      if (LOG.isTraceEnabled()) {
        LOG.trace("LruBlockCache current size " + StringUtils.byteDesc(currentSize)
          + " has exceeded acceptable size " + StringUtils.byteDesc(currentAcceptableSize) + "."
          + " The hard limit size is " + StringUtils.byteDesc(hardLimitSize)
          + ", failed to put cacheKey:" + cacheKey + " into LruBlockCache.");
      }
      if (!evictionInProgress) {
        runEviction();
      }
      return;
    }
    // Ensure that the block is an heap one.
    buf = asReferencedHeapBlock(buf);
    cb = new LruCachedBlock(cacheKey, buf, count.incrementAndGet(), inMemory);
    long newSize = updateSizeMetrics(cb, false);
    map.put(cacheKey, cb);
    long val = elements.incrementAndGet();
    if (buf.getBlockType().isData()) {
      dataBlockElements.increment();
    }
    if (LOG.isTraceEnabled()) {
      long size = map.size();
      assertCounterSanity(size, val);
    }
    if (newSize > currentAcceptableSize && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * Sanity-checking for parity between actual block cache content and metrics.
   * Intended only for use with TRACE level logging and -ea JVM.
   */
  private static void assertCounterSanity(long mapSize, long counterVal) {
    if (counterVal < 0) {
      LOG.trace("counterVal overflow. Assertions unreliable. counterVal=" + counterVal +
        ", mapSize=" + mapSize);
      return;
    }
    if (mapSize < Integer.MAX_VALUE) {
      double pct_diff = Math.abs((((double) counterVal) / ((double) mapSize)) - 1.);
      if (pct_diff > 0.05) {
        LOG.trace("delta between reported and actual size > 5%. counterVal=" + counterVal +
          ", mapSize=" + mapSize);
      }
    }
  }

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * TODO after HBASE-22005, we may cache an block which allocated from off-heap, but our LRU cache
   * sizing is based on heap size, so we should handle this in HBASE-22127. It will introduce an
   * switch whether make the LRU on-heap or not, if so we may need copy the memory to on-heap,
   * otherwise the caching size is based on off-heap.
   * @param cacheKey block's cache key
   * @param buf block buffer
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  /**
   * Helper function that updates the local size counter and also updates any
   * per-cf or per-blocktype metrics it can discern from given
   * {@link LruCachedBlock}
   */
  private long updateSizeMetrics(LruCachedBlock cb, boolean evict) {
    long heapsize = cb.heapSize();
    BlockType bt = cb.getBuffer().getBlockType();
    if (evict) {
      heapsize *= -1;
    }
    if (bt != null && bt.isData()) {
      dataBlockSize.add(heapsize);
    }
    return size.addAndGet(heapsize);
  }

  /**
   * Get the buffer of the block with the specified name.
   *
   * @param cacheKey           block's cache key
   * @param caching            true if the caller caches blocks on cache misses
   * @param repeat             Whether this is a repeat lookup for the same block
   *                           (used to avoid double counting cache misses when doing double-check
   *                           locking)
   * @param updateCacheMetrics Whether to update cache metrics or not
   *
   * @return buffer of specified cache key, or null if not in cache
   */
  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
      boolean updateCacheMetrics) {
    LruCachedBlock cb = map.computeIfPresent(cacheKey, (key, val) -> {
      // It will be referenced by RPC path, so increase here. NOTICE: Must do the retain inside
      // this block. because if retain outside the map#computeIfPresent, the evictBlock may remove
      // the block and release, then we're retaining a block with refCnt=0 which is disallowed.
      // see HBASE-22422.
      val.getBuffer().retain();
      return val;
    });
    if (cb == null) {
      if (!repeat && updateCacheMetrics) {
        stats.miss(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
      }
      // If there is another block cache then try and read there.
      // However if this is a retry ( second time in double checked locking )
      // And it's already a miss then the l2 will also be a miss.
      if (victimHandler != null && !repeat) {
        // The handler will increase result's refCnt for RPC, so need no extra retain.
        Cacheable result = victimHandler.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
        // Promote this to L1.
        if (result != null) {
          if (caching) {
            cacheBlock(cacheKey, result, /* inMemory = */ false);
          }
        }
        return result;
      }
      return null;
    }
    if (updateCacheMetrics) {
      stats.hit(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
    }
    cb.access(count.incrementAndGet());
    return cb.getBuffer();
  }

  /**
   * Whether the cache contains block with specified cacheKey
   *
   * @return true if contains the block
   */
  @Override
  public boolean containsBlock(BlockCacheKey cacheKey) {
    return map.containsKey(cacheKey);
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    LruCachedBlock cb = map.get(cacheKey);
    return cb != null && evictBlock(cb, false) > 0;
  }

  /**
   * Evicts all blocks for a specific HFile. This is an
   * expensive operation implemented as a linear-time search through all blocks
   * in the cache. Ideally this should be a search in a log-access-time map.
   *
   * <p>
   * This is used for evict-on-close to remove all blocks of a specific HFile.
   *
   * @return the number of blocks evicted
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    int numEvicted = 0;
    for (BlockCacheKey key : map.keySet()) {
      if (key.getHfileName().equals(hfileName)) {
        if (evictBlock(key)) {
          ++numEvicted;
        }
      }
    }
    if (victimHandler != null) {
      numEvicted += victimHandler.evictBlocksByHfileName(hfileName);
    }
    return numEvicted;
  }

  /**
   * Evict the block, and it will be cached by the victim handler if exists &amp;&amp;
   * block may be read again later
   *
   * @param evictedByEvictionProcess true if the given block is evicted by
   *          EvictionThread
   * @return the heap size of evicted block
   */
  protected long evictBlock(LruCachedBlock block, boolean evictedByEvictionProcess) {
    LruCachedBlock previous = map.remove(block.getCacheKey());
    if (previous == null) {
      return 0;
    }
    updateSizeMetrics(block, true);
    long val = elements.decrementAndGet();
    if (LOG.isTraceEnabled()) {
      long size = map.size();
      assertCounterSanity(size, val);
    }
    if (block.getBuffer().getBlockType().isData()) {
      dataBlockElements.decrement();
    }
    if (evictedByEvictionProcess) {
      // When the eviction of the block happened because of invalidation of HFiles, no need to
      // update the stats counter.
      stats.evicted(block.getCachedTime(), block.getCacheKey().isPrimary());
      if (victimHandler != null) {
        victimHandler.cacheBlock(block.getCacheKey(), block.getBuffer());
      }
    }
    // Decrease the block's reference count, and if refCount is 0, then it'll auto-deallocate. DO
    // NOT move this up because if do that then the victimHandler may access the buffer with
    // refCnt = 0 which is disallowed.
    previous.getBuffer().release();
    return block.heapSize();
  }

  /**
   * Multi-threaded call to run the eviction process.
   */
  private void runEviction() {
    if (evictionThread == null || !evictionThread.isGo()) {
      evict();
    } else {
      evictionThread.evict();
    }
  }

  boolean isEvictionInProgress() {
    return evictionInProgress;
  }

  long getOverhead() {
    return overhead;
  }

  /**
   * Eviction method.
   */
  void evict() {

    // Ensure only one eviction at a time
    if (!evictionLock.tryLock()) {
      return;
    }

    try {
      evictionInProgress = true;
      long currentSize = this.size.get();
      long bytesToFree = currentSize - minSize();

      if (LOG.isTraceEnabled()) {
        LOG.trace("Block cache LRU eviction started; Attempting to free " +
          StringUtils.byteDesc(bytesToFree) + " of total=" +
          StringUtils.byteDesc(currentSize));
      }

      if (bytesToFree <= 0) {
        return;
      }

      // Instantiate priority buckets
      BlockBucket bucketSingle = new BlockBucket("single", bytesToFree, blockSize, singleSize());
      BlockBucket bucketMulti = new BlockBucket("multi", bytesToFree, blockSize, multiSize());
      BlockBucket bucketMemory = new BlockBucket("memory", bytesToFree, blockSize, memorySize());

      // Scan entire map putting into appropriate buckets
      for (LruCachedBlock cachedBlock : map.values()) {
        switch (cachedBlock.getPriority()) {
          case SINGLE: {
            bucketSingle.add(cachedBlock);
            break;
          }
          case MULTI: {
            bucketMulti.add(cachedBlock);
            break;
          }
          case MEMORY: {
            bucketMemory.add(cachedBlock);
            break;
          }
        }
      }

      long bytesFreed = 0;
      if (forceInMemory || memoryFactor > 0.999f) {
        long s = bucketSingle.totalSize();
        long m = bucketMulti.totalSize();
        if (bytesToFree > (s + m)) {
          // this means we need to evict blocks in memory bucket to make room,
          // so the single and multi buckets will be emptied
          bytesFreed = bucketSingle.free(s);
          bytesFreed += bucketMulti.free(m);
          if (LOG.isTraceEnabled()) {
            LOG.trace("freed " + StringUtils.byteDesc(bytesFreed) +
              " from single and multi buckets");
          }
          bytesFreed += bucketMemory.free(bytesToFree - bytesFreed);
          if (LOG.isTraceEnabled()) {
            LOG.trace("freed " + StringUtils.byteDesc(bytesFreed) +
              " total from all three buckets ");
          }
        } else {
          // this means no need to evict block in memory bucket,
          // and we try best to make the ratio between single-bucket and
          // multi-bucket is 1:2
          long bytesRemain = s + m - bytesToFree;
          if (3 * s <= bytesRemain) {
            // single-bucket is small enough that no eviction happens for it
            // hence all eviction goes from multi-bucket
            bytesFreed = bucketMulti.free(bytesToFree);
          } else if (3 * m <= 2 * bytesRemain) {
            // multi-bucket is small enough that no eviction happens for it
            // hence all eviction goes from single-bucket
            bytesFreed = bucketSingle.free(bytesToFree);
          } else {
            // both buckets need to evict some blocks
            bytesFreed = bucketSingle.free(s - bytesRemain / 3);
            if (bytesFreed < bytesToFree) {
              bytesFreed += bucketMulti.free(bytesToFree - bytesFreed);
            }
          }
        }
      } else {
        PriorityQueue<BlockBucket> bucketQueue = new PriorityQueue<>(3);

        bucketQueue.add(bucketSingle);
        bucketQueue.add(bucketMulti);
        bucketQueue.add(bucketMemory);

        int remainingBuckets = bucketQueue.size();

        BlockBucket bucket;
        while ((bucket = bucketQueue.poll()) != null) {
          long overflow = bucket.overflow();
          if (overflow > 0) {
            long bucketBytesToFree =
                Math.min(overflow, (bytesToFree - bytesFreed) / remainingBuckets);
            bytesFreed += bucket.free(bucketBytesToFree);
          }
          remainingBuckets--;
        }
      }
      if (LOG.isTraceEnabled()) {
        long single = bucketSingle.totalSize();
        long multi = bucketMulti.totalSize();
        long memory = bucketMemory.totalSize();
        LOG.trace("Block cache LRU eviction completed; " +
          "freed=" + StringUtils.byteDesc(bytesFreed) + ", " +
          "total=" + StringUtils.byteDesc(this.size.get()) + ", " +
          "single=" + StringUtils.byteDesc(single) + ", " +
          "multi=" + StringUtils.byteDesc(multi) + ", " +
          "memory=" + StringUtils.byteDesc(memory));
      }
    } finally {
      stats.evict();
      evictionInProgress = false;
      evictionLock.unlock();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("blockCount", getBlockCount())
      .add("currentSize", StringUtils.byteDesc(getCurrentSize()))
      .add("freeSize", StringUtils.byteDesc(getFreeSize()))
      .add("maxSize", StringUtils.byteDesc(getMaxSize()))
      .add("heapSize", StringUtils.byteDesc(heapSize()))
      .add("minSize", StringUtils.byteDesc(minSize()))
      .add("minFactor", minFactor)
      .add("multiSize", StringUtils.byteDesc(multiSize()))
      .add("multiFactor", multiFactor)
      .add("singleSize", StringUtils.byteDesc(singleSize()))
      .add("singleFactor", singleFactor)
      .toString();
  }

  /**
   * Used to group blocks into priority buckets.  There will be a BlockBucket
   * for each priority (single, multi, memory).  Once bucketed, the eviction
   * algorithm takes the appropriate number of elements out of each according
   * to configuration parameters and their relatives sizes.
   */
  private class BlockBucket implements Comparable<BlockBucket> {

    private final String name;
    private LruCachedBlockQueue queue;
    private long totalSize = 0;
    private long bucketSize;

    public BlockBucket(String name, long bytesToFree, long blockSize, long bucketSize) {
      this.name = name;
      this.bucketSize = bucketSize;
      queue = new LruCachedBlockQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(LruCachedBlock block) {
      totalSize += block.heapSize();
      queue.add(block);
    }

    public long free(long toFree) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("freeing " + StringUtils.byteDesc(toFree) + " from " + this);
      }
      LruCachedBlock cb;
      long freedBytes = 0;
      while ((cb = queue.pollLast()) != null) {
        freedBytes += evictBlock(cb, true);
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("freed " + StringUtils.byteDesc(freedBytes) + " from " + this);
      }
      return freedBytes;
    }

    public long overflow() {
      return totalSize - bucketSize;
    }

    public long totalSize() {
      return totalSize;
    }

    @Override
    public int compareTo(BlockBucket that) {
      return Long.compare(this.overflow(), that.overflow());
    }

    @Override
    public boolean equals(Object that) {
      if (that == null || !(that instanceof BlockBucket)) {
        return false;
      }
      return compareTo((BlockBucket)that) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, bucketSize, queue, totalSize);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("totalSize", StringUtils.byteDesc(totalSize))
        .add("bucketSize", StringUtils.byteDesc(bucketSize))
        .toString();
    }
  }

  /**
   * Get the maximum size of this cache.
   *
   * @return max size in bytes
   */

  @Override
  public long getMaxSize() {
    return this.maxSize;
  }

  @Override
  public long getCurrentSize() {
    return this.size.get();
  }

  @Override
  public long getCurrentDataSize() {
    return this.dataBlockSize.sum();
  }

  @Override
  public long getFreeSize() {
    return getMaxSize() - getCurrentSize();
  }

  @Override
  public long size() {
    return getMaxSize();
  }

  @Override
  public long getBlockCount() {
    return this.elements.get();
  }

  @Override
  public long getDataBlockCount() {
    return this.dataBlockElements.sum();
  }

  EvictionThread getEvictionThread() {
    return this.evictionThread;
  }

  /*
   * Eviction thread.  Sits in waiting state until an eviction is triggered
   * when the cache size grows above the acceptable level.<p>
   *
   * Thread is triggered into action by {@link LruBlockCache#runEviction()}
   */
  static class EvictionThread extends Thread {

    private WeakReference<LruBlockCache> cache;
    private volatile boolean go = true;
    // flag set after enter the run method, used for test
    private boolean enteringRun = false;

    public EvictionThread(LruBlockCache cache) {
      super(Thread.currentThread().getName() + ".LruBlockCache.EvictionThread");
      setDaemon(true);
      this.cache = new WeakReference<>(cache);
    }

    @Override
    public void run() {
      enteringRun = true;
      while (this.go) {
        synchronized (this) {
          try {
            this.wait(1000 * 10/*Don't wait for ever*/);
          } catch (InterruptedException e) {
            LOG.warn("Interrupted eviction thread ", e);
            Thread.currentThread().interrupt();
          }
        }
        LruBlockCache cache = this.cache.get();
        if (cache == null) {
          this.go = false;
          break;
        }
        cache.evict();
      }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NN_NAKED_NOTIFY",
        justification="This is what we want")
    public void evict() {
      synchronized (this) {
        this.notifyAll();
      }
    }

    synchronized void shutdown() {
      this.go = false;
      this.notifyAll();
    }

    public boolean isGo() {
      return go;
    }

    /**
     * Used for the test.
     */
    boolean isEnteringRun() {
      return this.enteringRun;
    }
  }

  /*
   * Statistics thread.  Periodically prints the cache statistics to the log.
   */
  static class StatisticsThread extends Thread {

    private final LruBlockCache lru;

    public StatisticsThread(LruBlockCache lru) {
      super("LruBlockCacheStats");
      setDaemon(true);
      this.lru = lru;
    }

    @Override
    public void run() {
      lru.logStats();
    }
  }

  public void logStats() {
    // Log size
    long totalSize = heapSize();
    long freeSize = maxSize - totalSize;
    LruBlockCache.LOG.info("totalSize=" + StringUtils.byteDesc(totalSize) + ", " +
        "freeSize=" + StringUtils.byteDesc(freeSize) + ", " +
        "max=" + StringUtils.byteDesc(this.maxSize) + ", " +
        "blockCount=" + getBlockCount() + ", " +
        "accesses=" + stats.getRequestCount() + ", " +
        "hits=" + stats.getHitCount() + ", " +
        "hitRatio=" + (stats.getHitCount() == 0 ?
          "0" : (StringUtils.formatPercent(stats.getHitRatio(), 2)+ ", ")) + ", " +
        "cachingAccesses=" + stats.getRequestCachingCount() + ", " +
        "cachingHits=" + stats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" + (stats.getHitCachingCount() == 0 ?
          "0,": (StringUtils.formatPercent(stats.getHitCachingRatio(), 2) + ", ")) +
        "evictions=" + stats.getEvictionCount() + ", " +
        "evicted=" + stats.getEvictedCount() + ", " +
        "evictedPerRun=" + stats.evictedPerEviction());
  }

  /**
   * Get counter statistics for this cache.
   *
   * <p>Includes: total accesses, hits, misses, evicted blocks, and runs
   * of the eviction processes.
   */
  @Override
  public CacheStats getStats() {
    return this.stats;
  }

  public final static long CACHE_FIXED_OVERHEAD =
      ClassSize.estimateBase(LruBlockCache.class, false);

  @Override
  public long heapSize() {
    return getCurrentSize();
  }

  private static long calculateOverhead(long maxSize, long blockSize, int concurrency) {
    // FindBugs ICAST_INTEGER_MULTIPLY_CAST_TO_LONG
    return CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP
           + ((long) Math.ceil(maxSize * 1.2 / blockSize) * ClassSize.CONCURRENT_HASHMAP_ENTRY)
           + ((long) concurrency * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    final Iterator<LruCachedBlock> iterator = map.values().iterator();

    return new Iterator<CachedBlock>() {
      private final long now = System.nanoTime();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public CachedBlock next() {
        final LruCachedBlock b = iterator.next();
        return new CachedBlock() {
          @Override
          public String toString() {
            return BlockCacheUtil.toString(this, now);
          }

          @Override
          public BlockPriority getBlockPriority() {
            return b.getPriority();
          }

          @Override
          public BlockType getBlockType() {
            return b.getBuffer().getBlockType();
          }

          @Override
          public long getOffset() {
            return b.getCacheKey().getOffset();
          }

          @Override
          public long getSize() {
            return b.getBuffer().heapSize();
          }

          @Override
          public long getCachedTime() {
            return b.getCachedTime();
          }

          @Override
          public String getFilename() {
            return b.getCacheKey().getHfileName();
          }

          @Override
          public int compareTo(CachedBlock other) {
            int diff = this.getFilename().compareTo(other.getFilename());
            if (diff != 0) {
              return diff;
            }
            diff = Long.compare(this.getOffset(), other.getOffset());
            if (diff != 0) {
              return diff;
            }
            if (other.getCachedTime() < 0 || this.getCachedTime() < 0) {
              throw new IllegalStateException(this.getCachedTime() + ", " + other.getCachedTime());
            }
            return Long.compare(other.getCachedTime(), this.getCachedTime());
          }

          @Override
          public int hashCode() {
            return b.hashCode();
          }

          @Override
          public boolean equals(Object obj) {
            if (obj instanceof CachedBlock) {
              CachedBlock cb = (CachedBlock)obj;
              return compareTo(cb) == 0;
            } else {
              return false;
            }
          }
        };
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  // Simple calculators of sizes given factors and maxSize

  long acceptableSize() {
    return (long)Math.floor(this.maxSize * this.acceptableFactor);
  }
  private long minSize() {
    return (long)Math.floor(this.maxSize * this.minFactor);
  }
  private long singleSize() {
    return (long)Math.floor(this.maxSize * this.singleFactor * this.minFactor);
  }
  private long multiSize() {
    return (long)Math.floor(this.maxSize * this.multiFactor * this.minFactor);
  }
  private long memorySize() {
    return (long) Math.floor(this.maxSize * this.memoryFactor * this.minFactor);
  }

  @Override
  public void shutdown() {
    if (victimHandler != null) {
      victimHandler.shutdown();
    }
    this.scheduleThreadPool.shutdown();
    for (int i = 0; i < 10; i++) {
      if (!this.scheduleThreadPool.isShutdown()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while sleeping");
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (!this.scheduleThreadPool.isShutdown()) {
      List<Runnable> runnables = this.scheduleThreadPool.shutdownNow();
      LOG.debug("Still running " + runnables);
    }
    this.evictionThread.shutdown();
  }

  /** Clears the cache. Used in tests. */
  public void clearCache() {
    this.map.clear();
    this.elements.set(0);
  }

  /**
   * Used in testing. May be very inefficient.
   *
   * @return the set of cached file names
   */
  SortedSet<String> getCachedFileNamesForTest() {
    SortedSet<String> fileNames = new TreeSet<>();
    for (BlockCacheKey cacheKey : map.keySet()) {
      fileNames.add(cacheKey.getHfileName());
    }
    return fileNames;
  }

  public Map<DataBlockEncoding, Integer> getEncodingCountsForTest() {
    Map<DataBlockEncoding, Integer> counts = new EnumMap<>(DataBlockEncoding.class);
    for (LruCachedBlock block : map.values()) {
      DataBlockEncoding encoding = ((HFileBlock) block.getBuffer()).getDataBlockEncoding();
      Integer count = counts.get(encoding);
      counts.put(encoding, (count == null ? 0 : count) + 1);
    }
    return counts;
  }

  Map<BlockCacheKey, LruCachedBlock> getMapForTests() {
    return map;
  }

  @Override
  public BlockCache[] getBlockCaches() {
    if (victimHandler != null) {
      return new BlockCache[] { this, this.victimHandler };
    }
    return null;
  }
}
