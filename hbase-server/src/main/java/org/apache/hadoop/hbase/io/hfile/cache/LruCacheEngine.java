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
package org.apache.hadoop.hbase.io.hfile.cache;

import java.lang.ref.WeakReference;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockCacheUtil;
import org.apache.hadoop.hbase.io.hfile.BlockPriority;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.LruCachedBlock;
import org.apache.hadoop.hbase.io.hfile.LruCachedBlockQueue;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.com.google.common.base.Objects;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Native LRU {@link CacheEngine} implementation.
 * <p>
 * This cache is memory-aware using {@link HeapSize}, memory-bound using an LRU eviction algorithm,
 * and concurrent. It is backed by a {@link ConcurrentHashMap} and can use a non-blocking eviction
 * thread, providing constant-time {@link #cacheBlock(BlockCacheKey, Cacheable, boolean)} and
 * {@link #getBlock(BlockCacheKey, boolean, boolean, boolean)} operations.
 * </p>
 * <p>
 * The cache maintains three block-priority levels to provide scan resistance and support in-memory
 * column families:
 * </p>
 * <ul>
 * <li>single-access blocks</li>
 * <li>multiple-access blocks</li>
 * <li>in-memory blocks</li>
 * </ul>
 * <p>
 * Each priority is assigned a portion of the total cache capacity. During eviction the cache tries
 * to preserve the configured relative sizes while allowing unused capacity in one priority to be
 * consumed by another.
 * </p>
 * <p>
 * This class is a storage engine only. It does not perform L1/L2 orchestration, victim-cache
 * delegation, tier placement, admission control, promotion, or demotion. Those responsibilities
 * belong to the cache topology and policy layers.
 * </p>
 */
@InterfaceAudience.Private
public class LruCacheEngine implements ResizableCacheEngine, HeapSize, Iterable<CachedBlock> {

  private static final Logger LOG = LoggerFactory.getLogger(LruCacheEngine.class);

  /**
   * Percentage of total size that eviction will evict until.
   */
  private static final String LRU_MIN_FACTOR_CONFIG_NAME = "hbase.lru.blockcache.min.factor";

  /**
   * Acceptable cache size above which eviction is triggered.
   */
  private static final String LRU_ACCEPTABLE_FACTOR_CONFIG_NAME =
    "hbase.lru.blockcache.acceptable.factor";

  /**
   * Hard capacity limit. Inserts are rejected once the cache exceeds this factor multiplied by the
   * acceptable size.
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
   * Configuration key that gives data blocks from in-memory HFiles higher eviction priority.
   */
  private static final String LRU_IN_MEMORY_FORCE_MODE_CONFIG_NAME =
    "hbase.lru.rs.inmemoryforcemode";

  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  static final int DEFAULT_CONCURRENCY_LEVEL = 16;

  private static final float DEFAULT_MIN_FACTOR = 0.95f;
  static final float DEFAULT_ACCEPTABLE_FACTOR = 0.99f;

  private static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  private static final float DEFAULT_MULTI_FACTOR = 0.50f;
  private static final float DEFAULT_MEMORY_FACTOR = 0.25f;

  private static final float DEFAULT_HARD_CAPACITY_LIMIT_FACTOR = 1.2f;

  private static final boolean DEFAULT_IN_MEMORY_FORCE_MODE = false;

  private static final int STAT_THREAD_PERIOD = 60 * 5;

  private static final String LRU_MAX_BLOCK_SIZE = "hbase.lru.max.block.size";

  private static final long DEFAULT_MAX_BLOCK_SIZE = 16L * 1024L * 1024L;

  /**
   * Fixed heap overhead of an LRU cache engine instance.
   */
  public static final long CACHE_FIXED_OVERHEAD =
    ClassSize.estimateBase(LruCacheEngine.class, false);

  /**
   * Cached blocks keyed by their HFile block cache key.
   * <p>
   * A {@link ConcurrentHashMap} is required because {@link #getBlock} and eviction depend on the
   * atomicity guarantees of {@code computeIfPresent}.
   * </p>
   */
  private transient final ConcurrentHashMap<BlockCacheKey, LruCachedBlock> map;

  /** Lock protecting the eviction process. */
  private transient final ReentrantLock evictionLock = new ReentrantLock(true);

  /** Maximum size of an individual block accepted by this cache. */
  private final long maxBlockSize;

  /** Whether an eviction pass is currently running. */
  private volatile boolean evictionInProgress;

  /** Optional background eviction thread. */
  private transient final EvictionThread evictionThread;

  /**
   * Listener notified about capacity-driven block evictions.
   */
  private volatile CacheEvictionListener evictionListener;

  /** Executor used to periodically report cache statistics. */
  private transient final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setNameFormat("LruCacheEngineStatsExecutor").setDaemon(true).build());

  /** Current total heap size used by the cache. */
  private final AtomicLong size;

  /** Current heap size of data blocks. */
  private final LongAdder dataBlockSize = new LongAdder();

  /** Current heap size of index blocks. */
  private final LongAdder indexBlockSize = new LongAdder();

  /** Current heap size of bloom blocks. */
  private final LongAdder bloomBlockSize = new LongAdder();

  /** Current number of cached blocks. */
  private final AtomicLong elements;

  /** Current number of cached data blocks. */
  private final LongAdder dataBlockElements = new LongAdder();

  /** Current number of cached index blocks. */
  private final LongAdder indexBlockElements = new LongAdder();

  /** Current number of cached bloom blocks. */
  private final LongAdder bloomBlockElements = new LongAdder();

  /** Sequential cache access identifier. */
  private final AtomicLong count;

  /** Hard cache capacity limit factor. */
  private float hardCapacityLimitFactor;

  /** Cache statistics. */
  private final CacheStats stats;

  /** Maximum cache size in bytes. */
  private long maxSize;

  /** Expected average block size. */
  private long blockSize;

  /** Cache size factor at which eviction is triggered. */
  private float acceptableFactor;

  /** Cache size factor to which an eviction pass should reduce the cache. */
  private float minFactor;

  /** Fraction of capacity assigned to single-access blocks. */
  private float singleFactor;

  /** Fraction of capacity assigned to multiple-access blocks. */
  private float multiFactor;

  /** Fraction of capacity assigned to in-memory blocks. */
  private float memoryFactor;

  /** Heap overhead of the cache structure itself. */
  private long overhead;

  /** Whether data blocks from in-memory HFiles receive stronger retention priority. */
  private boolean forceInMemory;

  /**
   * Creates an LRU cache engine using default configuration values.
   * @param maxSize   maximum size of the cache, in bytes
   * @param blockSize expected average block size, in bytes
   */
  public LruCacheEngine(long maxSize, long blockSize) {
    this(maxSize, blockSize, true);
  }

  /**
   * Creates an LRU cache engine and optionally enables the background eviction thread.
   * @param maxSize        maximum size of the cache, in bytes
   * @param blockSize      expected average block size, in bytes
   * @param evictionThread whether background eviction should be enabled
   */
  public LruCacheEngine(long maxSize, long blockSize, boolean evictionThread) {
    this(maxSize, blockSize, evictionThread, (int) Math.ceil(1.2 * maxSize / blockSize),
      DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, DEFAULT_MIN_FACTOR, DEFAULT_ACCEPTABLE_FACTOR,
      DEFAULT_SINGLE_FACTOR, DEFAULT_MULTI_FACTOR, DEFAULT_MEMORY_FACTOR,
      DEFAULT_HARD_CAPACITY_LIMIT_FACTOR, false, DEFAULT_MAX_BLOCK_SIZE);
  }

  /**
   * Creates an LRU cache engine using values from the supplied configuration.
   * @param maxSize        maximum size of the cache, in bytes
   * @param blockSize      expected average block size, in bytes
   * @param evictionThread whether background eviction should be enabled
   * @param conf           cache configuration
   */
  public LruCacheEngine(long maxSize, long blockSize, boolean evictionThread, Configuration conf) {
    this(maxSize, blockSize, evictionThread, (int) Math.ceil(1.2 * maxSize / blockSize),
      DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL,
      conf.getFloat(LRU_MIN_FACTOR_CONFIG_NAME, DEFAULT_MIN_FACTOR),
      conf.getFloat(LRU_ACCEPTABLE_FACTOR_CONFIG_NAME, DEFAULT_ACCEPTABLE_FACTOR),
      conf.getFloat(LRU_SINGLE_PERCENTAGE_CONFIG_NAME, DEFAULT_SINGLE_FACTOR),
      conf.getFloat(LRU_MULTI_PERCENTAGE_CONFIG_NAME, DEFAULT_MULTI_FACTOR),
      conf.getFloat(LRU_MEMORY_PERCENTAGE_CONFIG_NAME, DEFAULT_MEMORY_FACTOR),
      conf.getFloat(LRU_HARD_CAPACITY_LIMIT_FACTOR_CONFIG_NAME, DEFAULT_HARD_CAPACITY_LIMIT_FACTOR),
      conf.getBoolean(LRU_IN_MEMORY_FORCE_MODE_CONFIG_NAME, DEFAULT_IN_MEMORY_FORCE_MODE),
      conf.getLong(LRU_MAX_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE));
  }

  /**
   * Creates an LRU cache engine using the supplied configuration and background eviction.
   * @param maxSize   maximum size of the cache, in bytes
   * @param blockSize expected average block size, in bytes
   * @param conf      cache configuration
   */
  public LruCacheEngine(long maxSize, long blockSize, Configuration conf) {
    this(maxSize, blockSize, true, conf);
  }

  /**
   * Creates a fully configured LRU cache engine.
   * @param maxSize             maximum size of this cache, in bytes
   * @param blockSize           expected average size of blocks, in bytes
   * @param evictionThread      whether to run eviction in a background thread
   * @param mapInitialSize      initial size of the backing map
   * @param mapLoadFactor       load factor of the backing map
   * @param mapConcurrencyLevel concurrency level of the backing map
   * @param minFactor           fraction of maximum size retained after eviction
   * @param acceptableFactor    fraction of maximum size that triggers eviction
   * @param singleFactor        fraction assigned to single-access blocks
   * @param multiFactor         fraction assigned to multiple-access blocks
   * @param memoryFactor        fraction assigned to in-memory blocks
   * @param hardLimitFactor     hard capacity limit factor
   * @param forceInMemory       whether in-memory HFile blocks receive stronger retention priority
   * @param maxBlockSize        largest individual block accepted by this cache
   */
  public LruCacheEngine(long maxSize, long blockSize, boolean evictionThread, int mapInitialSize,
    float mapLoadFactor, int mapConcurrencyLevel, float minFactor, float acceptableFactor,
    float singleFactor, float multiFactor, float memoryFactor, float hardLimitFactor,
    boolean forceInMemory, long maxBlockSize) {
    this.maxBlockSize = maxBlockSize;

    if (
      singleFactor + multiFactor + memoryFactor != 1 || singleFactor < 0 || multiFactor < 0
        || memoryFactor < 0
    ) {
      throw new IllegalArgumentException(
        "Single, multi, and memory factors should be non-negative and total 1.0");
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
    this.map = new ConcurrentHashMap<>(mapInitialSize, mapLoadFactor, mapConcurrencyLevel);
    this.minFactor = minFactor;
    this.acceptableFactor = acceptableFactor;
    this.singleFactor = singleFactor;
    this.multiFactor = multiFactor;
    this.memoryFactor = memoryFactor;
    this.stats = new CacheStats(getClass().getSimpleName());
    this.count = new AtomicLong(0);
    this.elements = new AtomicLong(0);
    this.overhead = calculateOverhead(maxSize, blockSize, mapConcurrencyLevel);
    this.size = new AtomicLong(this.overhead);
    this.hardCapacityLimitFactor = hardLimitFactor;

    if (evictionThread) {
      this.evictionThread = new EvictionThread(this);
      this.evictionThread.start();
    } else {
      this.evictionThread = null;
    }

    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this), STAT_THREAD_PERIOD,
      STAT_THREAD_PERIOD, TimeUnit.SECONDS);
  }

  /**
   * Returns the human-readable name of this cache engine.
   * @return cache engine name
   */
  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  /**
   * Updates the maximum size of this cache.
   * <p>
   * If the cache is already larger than the new acceptable size, an eviction pass is started.
   * </p>
   * @param maxSize new maximum size, in bytes
   */
  @Override
  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    if (size.get() > acceptableSize() && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * Returns a heap-backed reference suitable for storage in this cache.
   * <p>
   * Shared-memory {@link HFileBlock}s are cloned onto the heap. Other blocks are retained before
   * being referenced by the cache.
   * </p>
   * @param buf block to convert
   * @return heap-backed retained block
   */
  private Cacheable asReferencedHeapBlock(Cacheable buf) {
    if (buf instanceof HFileBlock) {
      HFileBlock block = (HFileBlock) buf;
      if (block.isSharedMem()) {
        return HFileBlock.deepCloneOnHeap(block);
      }
    }

    return buf.retain();
  }

  /**
   * Caches the specified block.
   * @param cacheKey block cache key
   * @param buf      block contents
   * @param inMemory whether the block should receive in-memory priority
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    if (buf.heapSize() > maxBlockSize) {
      if (stats.failInsert() % 50 == 0) {
        LOG.warn("Trying to cache too large a block " + cacheKey.getHfileName() + " @ "
          + cacheKey.getOffset() + " is " + buf.heapSize() + " which is larger than "
          + maxBlockSize);
      }
      return;
    }

    LruCachedBlock existingBlock = map.get(cacheKey);
    if (
      existingBlock != null && !BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf)
    ) {
      return;
    }

    long currentSize = size.get();
    long currentAcceptableSize = acceptableSize();
    long hardLimitSize = (long) (hardCapacityLimitFactor * currentAcceptableSize);

    if (currentSize >= hardLimitSize) {
      stats.failInsert();
      if (LOG.isTraceEnabled()) {
        LOG.trace("LruCacheEngine current size " + StringUtils.byteDesc(currentSize)
          + " has exceeded acceptable size " + StringUtils.byteDesc(currentAcceptableSize) + "."
          + " The hard limit size is " + StringUtils.byteDesc(hardLimitSize)
          + ", failed to put cacheKey:" + cacheKey + " into LruCacheEngine.");
      }
      if (!evictionInProgress) {
        runEviction();
      }
      return;
    }

    Cacheable referencedBlock = asReferencedHeapBlock(buf);
    LruCachedBlock newBlock =
      new LruCachedBlock(cacheKey, referencedBlock, count.incrementAndGet(), inMemory);

    long newSize;
    long elementCount;

    if (existingBlock != null) {
      if (!replaceBlock(cacheKey, existingBlock, newBlock)) {
        referencedBlock.release();
        return;
      }

      newSize = size.get();
      elementCount = elements.get();
    } else {
      newSize = updateSizeMetrics(newBlock, false);
      map.put(cacheKey, newBlock);

      elementCount = elements.incrementAndGet();
      incrementBlockTypeElementCount(referencedBlock);
    }

    if (LOG.isTraceEnabled()) {
      assertCounterSanity(map.size(), elementCount);
    }

    if (newSize > currentAcceptableSize && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * Atomically replaces the expected cached block and updates cache accounting.
   * <p>
   * Replacement does not change the total block count because the map cardinality remains
   * unchanged. The cache-owned reference held by the replaced block is released before the
   * replacement is published.
   * </p>
   * @param cacheKey    block cache key
   * @param expected    block expected to be currently mapped
   * @param replacement replacement block
   * @return {@code true} if the expected block was replaced
   */
  private boolean replaceBlock(BlockCacheKey cacheKey, LruCachedBlock expected,
    LruCachedBlock replacement) {
    MutableBoolean replaced = new MutableBoolean(false);

    map.computeIfPresent(cacheKey, (key, current) -> {
      if (current != expected) {
        return current;
      }

      updateSizeMetrics(current, true);
      decrementBlockTypeElementCount(current.getBuffer());
      current.getBuffer().release();

      updateSizeMetrics(replacement, false);
      incrementBlockTypeElementCount(replacement.getBuffer());

      replaced.setTrue();
      return replacement;
    });

    return replaced.booleanValue();
  }

  /**
   * Increments the cached-element counter associated with the specified block type.
   * @param block cached block
   */
  private void incrementBlockTypeElementCount(Cacheable block) {
    BlockType blockType = block.getBlockType();
    if (blockType.isBloom()) {
      bloomBlockElements.increment();
    } else if (blockType.isIndex()) {
      indexBlockElements.increment();
    } else if (blockType.isData()) {
      dataBlockElements.increment();
    }
  }

  /**
   * Decrements the cached-element counter associated with the specified block type.
   * @param block cached block
   */
  private void decrementBlockTypeElementCount(Cacheable block) {
    BlockType blockType = block.getBlockType();
    if (blockType.isBloom()) {
      bloomBlockElements.decrement();
    } else if (blockType.isIndex()) {
      indexBlockElements.decrement();
    } else if (blockType.isData()) {
      dataBlockElements.decrement();
    }
  }

  /**
   * Caches the specified block with normal cache priority.
   * @param cacheKey block cache key
   * @param buf      block contents
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  /**
   * Caches the specified block.
   * <p>
   * LRU insertion is synchronous, so {@code waitWhenCache} has no effect.
   * </p>
   * @param cacheKey      block cache key
   * @param buf           block contents
   * @param inMemory      whether the block should receive in-memory priority
   * @param waitWhenCache whether the caller requests synchronous completion
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
    boolean waitWhenCache) {
    cacheBlock(cacheKey, buf, inMemory);
  }

  /**
   * Checks consistency between the backing-map size and the element counter.
   * <p>
   * This method is intended for TRACE-level diagnostics and assertion-enabled JVMs.
   * </p>
   * @param mapSize    current backing-map size
   * @param counterVal current element-counter value
   */
  private static void assertCounterSanity(long mapSize, long counterVal) {
    if (counterVal < 0) {
      LOG.trace("counterVal overflow. Assertions unreliable. counterVal=" + counterVal
        + ", mapSize=" + mapSize);
      return;
    }

    if (mapSize < Integer.MAX_VALUE) {
      double percentageDifference = Math.abs((((double) counterVal) / ((double) mapSize)) - 1.0);
      if (percentageDifference > 0.05) {
        LOG.trace("delta between reported and actual size > 5%. counterVal=" + counterVal
          + ", mapSize=" + mapSize);
      }
    }
  }

  /**
   * Updates total and block-type-specific size metrics.
   * @param cachedBlock cached block whose size should be applied
   * @param evict       whether this operation represents removal
   * @return new total cache size
   */
  private long updateSizeMetrics(LruCachedBlock cachedBlock, boolean evict) {
    long heapSize = cachedBlock.heapSize();
    BlockType blockType = cachedBlock.getBuffer().getBlockType();

    if (evict) {
      heapSize *= -1;
    }

    if (blockType != null) {
      if (blockType.isBloom()) {
        bloomBlockSize.add(heapSize);
      } else if (blockType.isIndex()) {
        indexBlockSize.add(heapSize);
      } else if (blockType.isData()) {
        dataBlockSize.add(heapSize);
      }
    }

    return size.addAndGet(heapSize);
  }

  /**
   * Returns the cached block associated with the specified key.
   * <p>
   * Lookup is strictly local to this cache engine. A miss is returned to the topology layer rather
   * than being delegated to another cache tier.
   * </p>
   * @param cacheKey           block cache key
   * @param caching            whether the caller caches blocks on misses
   * @param repeat             whether this is a repeated lookup for the same block
   * @param updateCacheMetrics whether cache statistics should be updated
   * @return cached block, or {@code null} if not present
   */
  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    LruCachedBlock cachedBlock = map.computeIfPresent(cacheKey, (key, value) -> {
      value.getBuffer().retain();
      return value;
    });

    if (cachedBlock == null) {
      if (!repeat && updateCacheMetrics) {
        stats.miss(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
      }
      return null;
    }

    if (updateCacheMetrics) {
      stats.hit(caching, cacheKey.isPrimary(), cacheKey.getBlockType());
    }

    cachedBlock.access(count.incrementAndGet());
    return cachedBlock.getBuffer();
  }

  /**
   * Returns whether the specified block is currently cached.
   * @param cacheKey block cache key
   * @return {@code true} if the block is present
   */
  public boolean containsBlock(BlockCacheKey cacheKey) {
    return map.containsKey(cacheKey);
  }

  /**
   * Returns whether the specified block is currently cached.
   * @param cacheKey block cache key
   * @return optional containing the local cache-membership result
   */
  @Override
  public Optional<Boolean> isAlreadyCached(BlockCacheKey cacheKey) {
    return Optional.of(containsBlock(cacheKey));
  }

  /**
   * Evicts the specified block.
   * @param cacheKey block cache key
   * @return {@code true} if a block was found and evicted
   */
  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    LruCachedBlock cachedBlock = map.get(cacheKey);
    return cachedBlock != null && evictBlock(cachedBlock, false) > 0;
  }

  /**
   * Evicts all cached blocks belonging to the specified HFile.
   * <p>
   * This is a linear scan over the cache contents.
   * </p>
   * @param hfileName HFile name
   * @return number of blocks evicted
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    int numEvicted = 0;

    for (BlockCacheKey key : map.keySet()) {
      if (key.getHfileName().equals(hfileName) && evictBlock(key)) {
        numEvicted++;
      }
    }

    return numEvicted;
  }

  /**
   * Evicts the specified block from this cache.
   * <p>
   * For capacity-driven evictions, the configured eviction listener is notified while a temporary
   * reference to the evicted block is retained. Explicit invalidations do not generate eviction
   * notifications.
   * </p>
   * @param block                    block to evict
   * @param evictedByEvictionProcess whether the eviction was caused by cache pressure
   * @return heap size of the evicted block, or {@code 0} if the block was not present
   */
  protected long evictBlock(LruCachedBlock block, boolean evictedByEvictionProcess) {
    final AtomicReference<LruCachedBlock> removedBlock = new AtomicReference<>();
    final CacheEvictionListener listener = evictedByEvictionProcess ? evictionListener : null;

    map.computeIfPresent(block.getCacheKey(), (key, value) -> {
      if (value != block) {
        return value;
      }

      Cacheable buffer = value.getBuffer();

      // Keep the block alive after removing the cache-owned reference. This retain must happen
      // while the value is still protected by the map operation.
      buffer.retain();
      removedBlock.set(value);

      // Release the reference owned by this cache entry.
      buffer.release();
      return null;
    });

    LruCachedBlock removed = removedBlock.get();
    if (removed == null) {
      return 0;
    }

    Cacheable buffer = removed.getBuffer();
    try {
      updateSizeMetrics(removed, true);

      long elementCount = elements.decrementAndGet();
      if (LOG.isTraceEnabled()) {
        assertCounterSanity(map.size(), elementCount);
      }

      decrementBlockTypeElementCount(buffer);

      if (evictedByEvictionProcess) {
        stats.evicted(removed.getCachedTime(), removed.getCacheKey().isPrimary());
      }

      if (listener != null) {
        listener.onEviction(this, removed.getCacheKey(), buffer);
      }

      return removed.heapSize();
    } finally {
      // Release the temporary reference acquired inside computeIfPresent().
      buffer.release();
    }
  }

  /**
   * Starts an eviction pass synchronously or notifies the background eviction thread.
   */
  private void runEviction() {
    if (evictionThread == null || !evictionThread.isGo()) {
      evict();
    } else {
      evictionThread.evict();
    }
  }

  /**
   * Returns whether an eviction pass is currently executing.
   * @return {@code true} if eviction is in progress
   */
  boolean isEvictionInProgress() {
    return evictionInProgress;
  }

  /**
   * Returns the calculated fixed and map overhead for this cache.
   * @return cache overhead, in bytes
   */
  long getOverhead() {
    return overhead;
  }

  /**
   * Performs an LRU eviction pass.
   */
  void evict() {
    if (!evictionLock.tryLock()) {
      return;
    }

    try {
      evictionInProgress = true;

      long currentSize = size.get();
      long bytesToFree = currentSize - minSize();

      if (LOG.isTraceEnabled()) {
        LOG.trace("LRU cache eviction started; Attempting to free "
          + StringUtils.byteDesc(bytesToFree) + " of total=" + StringUtils.byteDesc(currentSize));
      }

      if (bytesToFree <= 0) {
        return;
      }

      BlockBucket bucketSingle = new BlockBucket("single", bytesToFree, blockSize, singleSize());
      BlockBucket bucketMulti = new BlockBucket("multi", bytesToFree, blockSize, multiSize());
      BlockBucket bucketMemory = new BlockBucket("memory", bytesToFree, blockSize, memorySize());

      for (LruCachedBlock cachedBlock : map.values()) {
        switch (cachedBlock.getPriority()) {
          case SINGLE:
            bucketSingle.add(cachedBlock);
            break;
          case MULTI:
            bucketMulti.add(cachedBlock);
            break;
          case MEMORY:
            bucketMemory.add(cachedBlock);
            break;
          default:
            throw new IllegalStateException(
              "Unsupported block priority " + cachedBlock.getPriority());
        }
      }

      long bytesFreed = 0;

      if (forceInMemory || memoryFactor > 0.999f) {
        long singleSize = bucketSingle.totalSize();
        long multiSize = bucketMulti.totalSize();

        if (bytesToFree > singleSize + multiSize) {
          bytesFreed = bucketSingle.free(singleSize);
          bytesFreed += bucketMulti.free(multiSize);

          if (LOG.isTraceEnabled()) {
            LOG.trace(
              "freed " + StringUtils.byteDesc(bytesFreed) + " from single and multi buckets");
          }

          bytesFreed += bucketMemory.free(bytesToFree - bytesFreed);

          if (LOG.isTraceEnabled()) {
            LOG
              .trace("freed " + StringUtils.byteDesc(bytesFreed) + " total from all three buckets");
          }
        } else {
          long bytesRemain = singleSize + multiSize - bytesToFree;

          if (3 * singleSize <= bytesRemain) {
            bytesFreed = bucketMulti.free(bytesToFree);
          } else if (3 * multiSize <= 2 * bytesRemain) {
            bytesFreed = bucketSingle.free(bytesToFree);
          } else {
            bytesFreed = bucketSingle.free(singleSize - bytesRemain / 3);
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

        LOG.trace("LRU cache eviction completed; freed=" + StringUtils.byteDesc(bytesFreed)
          + ", total=" + StringUtils.byteDesc(size.get()) + ", single="
          + StringUtils.byteDesc(single) + ", multi=" + StringUtils.byteDesc(multi) + ", memory="
          + StringUtils.byteDesc(memory));
      }
    } finally {
      stats.evict();
      evictionInProgress = false;
      evictionLock.unlock();
    }
  }

  /**
   * Returns a diagnostic string describing this cache.
   * @return cache description
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("blockCount", getBlockCount())
      .add("currentSize", StringUtils.byteDesc(getCurrentSize()))
      .add("freeSize", StringUtils.byteDesc(getFreeSize()))
      .add("maxSize", StringUtils.byteDesc(getMaxSize()))
      .add("heapSize", StringUtils.byteDesc(heapSize()))
      .add("minSize", StringUtils.byteDesc(minSize())).add("minFactor", minFactor)
      .add("multiSize", StringUtils.byteDesc(multiSize())).add("multiFactor", multiFactor)
      .add("singleSize", StringUtils.byteDesc(singleSize())).add("singleFactor", singleFactor)
      .toString();
  }

  /**
   * Groups cached blocks belonging to the same LRU priority.
   */
  private class BlockBucket implements Comparable<BlockBucket> {

    private final String name;
    private final LruCachedBlockQueue queue;
    private long totalSize;
    private final long bucketSize;

    /**
     * Creates an LRU priority bucket.
     * @param name        bucket name
     * @param bytesToFree number of bytes the eviction pass attempts to free
     * @param blockSize   expected average block size
     * @param bucketSize  target size of this priority bucket
     */
    BlockBucket(String name, long bytesToFree, long blockSize, long bucketSize) {
      this.name = name;
      this.bucketSize = bucketSize;
      this.queue = new LruCachedBlockQueue(bytesToFree, blockSize);
    }

    /**
     * Adds a block to this priority bucket.
     * @param block cached block
     */
    void add(LruCachedBlock block) {
      totalSize += block.heapSize();
      queue.add(block);
    }

    /**
     * Evicts least-recently-used blocks until at least the requested number of bytes has been
     * released or the bucket is exhausted.
     * @param toFree requested number of bytes to release
     * @return number of bytes actually released
     */
    long free(long toFree) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("freeing " + StringUtils.byteDesc(toFree) + " from " + this);
      }

      LruCachedBlock cachedBlock;
      long freedBytes = 0;

      while ((cachedBlock = queue.pollLast()) != null) {
        freedBytes += evictBlock(cachedBlock, true);
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("freed " + StringUtils.byteDesc(freedBytes) + " from " + this);
      }

      return freedBytes;
    }

    /**
     * Returns the number of bytes by which this bucket exceeds its target size.
     * @return bucket overflow in bytes
     */
    long overflow() {
      return totalSize - bucketSize;
    }

    /**
     * Returns the total size of blocks assigned to this bucket.
     * @return bucket size in bytes
     */
    long totalSize() {
      return totalSize;
    }

    /**
     * Compares this bucket with another bucket by overflow.
     * @param that other bucket
     * @return comparison result
     */
    @Override
    public int compareTo(BlockBucket that) {
      return Long.compare(overflow(), that.overflow());
    }

    /**
     * Returns whether another object represents a bucket with the same overflow.
     * @param that object to compare
     * @return {@code true} when the buckets compare equally
     */
    @Override
    public boolean equals(Object that) {
      if (!(that instanceof LruCacheEngine.BlockBucket)) {
        return false;
      }
      return compareTo((BlockBucket) that) == 0;
    }

    /**
     * Returns the hash code of this bucket.
     * @return bucket hash code
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(name, bucketSize, queue, totalSize);
    }

    /**
     * Returns a diagnostic string describing this bucket.
     * @return bucket description
     */
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("name", name)
        .add("totalSize", StringUtils.byteDesc(totalSize))
        .add("bucketSize", StringUtils.byteDesc(bucketSize)).toString();
    }
  }

  /**
   * Returns the maximum cache size.
   * @return maximum cache size in bytes
   */
  @Override
  public long getMaxSize() {
    return maxSize;
  }

  /**
   * Returns the current total heap size consumed by this cache.
   * @return current size in bytes
   */
  @Override
  public long getCurrentSize() {
    return size.get();
  }

  /**
   * Returns the current heap size of cached data blocks.
   * @return data block size in bytes
   */
  @Override
  public long getCurrentDataSize() {
    return dataBlockSize.sum();
  }

  /**
   * Returns the current heap size of cached index blocks.
   * @return index block size in bytes
   */
  public long getCurrentIndexSize() {
    return indexBlockSize.sum();
  }

  /**
   * Returns the current heap size of cached bloom blocks.
   * @return bloom block size in bytes
   */
  public long getCurrentBloomSize() {
    return bloomBlockSize.sum();
  }

  /**
   * Returns unused cache capacity.
   * @return free size in bytes
   */
  @Override
  public long getFreeSize() {
    return getMaxSize() - getCurrentSize();
  }

  /**
   * Returns the configured capacity of this cache.
   * <p>
   * This preserves the existing LRU cache behavior where {@code size()} reports configured cache
   * capacity rather than current occupancy.
   * </p>
   * @return configured cache capacity in bytes
   */
  @Override
  public long size() {
    return getMaxSize();
  }

  /**
   * Returns the total number of blocks currently cached.
   * @return cached block count
   */
  @Override
  public long getBlockCount() {
    return elements.get();
  }

  /**
   * Returns the number of data blocks currently cached.
   * @return cached data block count
   */
  @Override
  public long getDataBlockCount() {
    return dataBlockElements.sum();
  }

  /**
   * Returns the number of index blocks currently cached.
   * @return cached index block count
   */
  public long getIndexBlockCount() {
    return indexBlockElements.sum();
  }

  /**
   * Returns the number of bloom blocks currently cached.
   * @return cached bloom block count
   */
  public long getBloomBlockCount() {
    return bloomBlockElements.sum();
  }

  /**
   * Returns the background eviction thread.
   * @return eviction thread, or {@code null} if background eviction is disabled
   */
  EvictionThread getEvictionThread() {
    return evictionThread;
  }

  /**
   * Background thread responsible for initiating LRU eviction passes.
   */
  static class EvictionThread extends Thread {

    private final WeakReference<LruCacheEngine> cache;

    private volatile boolean go = true;

    private boolean enteringRun;

    /**
     * Creates an eviction thread for the specified cache.
     * @param cache owning cache engine
     */
    EvictionThread(LruCacheEngine cache) {
      super(Thread.currentThread().getName() + ".LruCacheEngine.EvictionThread");
      setDaemon(true);
      this.cache = new WeakReference<>(cache);
    }

    /**
     * Waits for eviction notifications and executes eviction passes.
     */
    @Override
    public void run() {
      enteringRun = true;

      while (go) {
        synchronized (this) {
          try {
            wait(1000 * 10);
          } catch (InterruptedException e) {
            LOG.warn("Interrupted eviction thread", e);
            Thread.currentThread().interrupt();
          }
        }

        LruCacheEngine cacheEngine = cache.get();
        if (cacheEngine == null) {
          go = false;
          break;
        }

        cacheEngine.evict();
      }
    }

    /**
     * Wakes this thread so that it can execute an eviction pass.
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
        justification = "This is what we want")
    void evict() {
      synchronized (this) {
        notifyAll();
      }
    }

    /**
     * Stops the background eviction thread.
     */
    synchronized void shutdown() {
      go = false;
      notifyAll();
    }

    /**
     * Returns whether this thread should continue running.
     * @return {@code true} while the thread is active
     */
    boolean isGo() {
      return go;
    }

    /**
     * Returns whether this thread has entered its run method.
     * @return {@code true} after {@link #run()} has started
     */
    boolean isEnteringRun() {
      return enteringRun;
    }
  }

  /**
   * Periodically logs LRU cache statistics.
   */
  static class StatisticsThread extends Thread {

    private final LruCacheEngine lru;

    /**
     * Creates a statistics thread for the specified cache.
     * @param lru cache whose statistics should be logged
     */
    StatisticsThread(LruCacheEngine lru) {
      super("LruCacheEngineStats");
      setDaemon(true);
      this.lru = lru;
    }

    /**
     * Logs the current cache statistics.
     */
    @Override
    public void run() {
      lru.logStats();
    }
  }

  /**
   * Logs current LRU cache size and access statistics.
   */
  public void logStats() {
    long usedSize = heapSize();
    long freeSize = maxSize - usedSize;

    LOG.info("totalSize=" + StringUtils.byteDesc(maxSize) + ", usedSize="
      + StringUtils.byteDesc(usedSize) + ", freeSize=" + StringUtils.byteDesc(freeSize) + ", max="
      + StringUtils.byteDesc(maxSize) + ", blockCount=" + getBlockCount() + ", accesses="
      + stats.getRequestCount() + ", hits=" + stats.getHitCount() + ", hitRatio="
      + (stats.getHitCount() == 0 ? "0" : StringUtils.formatPercent(stats.getHitRatio(), 2) + ", ")
      + ", cachingAccesses=" + stats.getRequestCachingCount() + ", cachingHits="
      + stats.getHitCachingCount() + ", cachingHitsRatio="
      + (stats.getHitCachingCount() == 0
        ? "0,"
        : StringUtils.formatPercent(stats.getHitCachingRatio(), 2) + ", ")
      + "evictions=" + stats.getEvictionCount() + ", evicted=" + stats.getEvictedCount()
      + ", evictedPerRun=" + stats.evictedPerEviction());
  }

  /**
   * Returns cache access and eviction statistics.
   * @return cache statistics
   */
  @Override
  public CacheStats getStats() {
    return stats;
  }

  /**
   * Returns the total heap size currently consumed by this cache.
   * @return current heap size in bytes
   */
  @Override
  public long heapSize() {
    return getCurrentSize();
  }

  /**
   * Calculates the estimated fixed and backing-map overhead for a cache.
   * @param maxSize     maximum cache size
   * @param blockSize   expected average block size
   * @param concurrency backing-map concurrency level
   * @return estimated overhead in bytes
   */
  private static long calculateOverhead(long maxSize, long blockSize, int concurrency) {
    return CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP
      + ((long) Math.ceil(maxSize * 1.2 / blockSize) * ClassSize.CONCURRENT_HASHMAP_ENTRY)
      + ((long) concurrency * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
  }

  /**
   * Returns an iterator over the cached blocks.
   * @return cached block iterator
   */
  @Override
  public Iterator<CachedBlock> iterator() {
    final Iterator<LruCachedBlock> iterator = map.values().iterator();

    return new Iterator<CachedBlock>() {

      private final long now = System.nanoTime();

      /**
       * Returns whether another cached block is available.
       * @return {@code true} if another cached block is available
       */
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      /**
       * Returns the next cached block.
       * @return next cached block
       */
      @Override
      public CachedBlock next() {
        final LruCachedBlock block = iterator.next();

        return new CachedBlock() {

          /**
           * Returns a diagnostic representation of this cached block.
           * @return cached block description
           */
          @Override
          public String toString() {
            return BlockCacheUtil.toString(this, now);
          }

          /**
           * Returns the LRU priority assigned to this cached block.
           * @return block priority
           */
          @Override
          public BlockPriority getBlockPriority() {
            return block.getPriority();
          }

          /**
           * Returns the HFile block type.
           * @return block type
           */
          @Override
          public BlockType getBlockType() {
            return block.getBuffer().getBlockType();
          }

          /**
           * Returns the block offset in its HFile.
           * @return HFile offset
           */
          @Override
          public long getOffset() {
            return block.getCacheKey().getOffset();
          }

          /**
           * Returns the heap size of the cached block.
           * @return block size in bytes
           */
          @Override
          public long getSize() {
            return block.getBuffer().heapSize();
          }

          /**
           * Returns the time at which the block was cached.
           * @return cached time
           */
          @Override
          public long getCachedTime() {
            return block.getCachedTime();
          }

          /**
           * Returns the name of the HFile containing this block.
           * @return HFile name
           */
          @Override
          public String getFilename() {
            return block.getCacheKey().getHfileName();
          }

          /**
           * Compares cached blocks by filename, offset, and cache time.
           * @param other cached block to compare
           * @return comparison result
           */
          @Override
          public int compareTo(CachedBlock other) {
            int difference = getFilename().compareTo(other.getFilename());
            if (difference != 0) {
              return difference;
            }

            difference = Long.compare(getOffset(), other.getOffset());
            if (difference != 0) {
              return difference;
            }

            if (other.getCachedTime() < 0 || getCachedTime() < 0) {
              throw new IllegalStateException(getCachedTime() + ", " + other.getCachedTime());
            }

            return Long.compare(other.getCachedTime(), getCachedTime());
          }

          /**
           * Returns the hash code of the underlying cached block.
           * @return cached block hash code
           */
          @Override
          public int hashCode() {
            return block.hashCode();
          }

          /**
           * Returns whether another object represents the same cached block.
           * @param object object to compare
           * @return {@code true} when the objects represent the same cached block
           */
          @Override
          public boolean equals(Object object) {
            if (!(object instanceof CachedBlock)) {
              return false;
            }

            return compareTo((CachedBlock) object) == 0;
          }
        };
      }

      /**
       * Removal through this iterator is unsupported.
       * @throws UnsupportedOperationException always
       */
      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Returns an iterable view over cached blocks.
   * @return optional containing this cache as a cached-block iterable
   */
  @Override
  public Optional<Iterable<CachedBlock>> asCachedBlockIterable() {
    return Optional.of(this);
  }

  /**
   * Returns the acceptable size above which eviction is triggered.
   * @return acceptable size in bytes
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.UNITTEST)
  public long acceptableSize() {
    return (long) Math.floor(maxSize * acceptableFactor);
  }

  /**
   * Returns the target size below which an eviction pass should reduce the cache.
   * @return minimum target size in bytes
   */
  private long minSize() {
    return (long) Math.floor(maxSize * minFactor);
  }

  /**
   * Returns the target capacity assigned to single-access blocks.
   * @return single-access bucket size in bytes
   */
  private long singleSize() {
    return (long) Math.floor(maxSize * singleFactor * minFactor);
  }

  /**
   * Returns the target capacity assigned to multiple-access blocks.
   * @return multiple-access bucket size in bytes
   */
  private long multiSize() {
    return (long) Math.floor(maxSize * multiFactor * minFactor);
  }

  /**
   * Returns the target capacity assigned to in-memory blocks.
   * @return in-memory bucket size in bytes
   */
  private long memorySize() {
    return (long) Math.floor(maxSize * memoryFactor * minFactor);
  }

  /**
   * Shuts down this cache engine and its background threads.
   */
  @Override
  public void shutdown() {
    scheduleThreadPool.shutdown();

    for (int i = 0; i < 10; i++) {
      if (!scheduleThreadPool.isShutdown()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while sleeping");
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (!scheduleThreadPool.isShutdown()) {
      List<Runnable> runnables = scheduleThreadPool.shutdownNow();
      LOG.debug("Still running " + runnables);
    }

    if (evictionThread != null) {
      evictionThread.shutdown();
    }
  }

  /**
   * Clears all cached blocks.
   * <p>
   * This method is intended for tests.
   * </p>
   */
  public void clearCache() {
    map.clear();
    elements.set(0);
  }

  /**
   * Sets the listener that receives capacity-driven block eviction events.
   * @param listener eviction listener, or {@code null} to clear the current listener
   */
  @Override
  public void setEvictionListener(CacheEvictionListener listener) {
    this.evictionListener = listener;
  }

  /**
   * Returns the names of files that currently have blocks in this cache.
   * <p>
   * This method is intended for tests and performs a full cache scan.
   * </p>
   * @return sorted set of cached HFile names
   */
  SortedSet<String> getCachedFileNamesForTest() {
    SortedSet<String> fileNames = new TreeSet<>();

    for (BlockCacheKey cacheKey : map.keySet()) {
      fileNames.add(cacheKey.getHfileName());
    }

    return fileNames;
  }

  /**
   * Returns counts of cached blocks grouped by data block encoding.
   * <p>
   * This method is intended for tests.
   * </p>
   * @return block counts grouped by data block encoding
   */
  public Map<DataBlockEncoding, Integer> getEncodingCountsForTest() {
    Map<DataBlockEncoding, Integer> counts = new EnumMap<>(DataBlockEncoding.class);

    for (LruCachedBlock cachedBlock : map.values()) {
      DataBlockEncoding encoding = ((HFileBlock) cachedBlock.getBuffer()).getDataBlockEncoding();
      Integer currentCount = counts.get(encoding);
      counts.put(encoding, currentCount == null ? 1 : currentCount + 1);
    }

    return counts;
  }

  /**
   * Returns the internal cached-block map.
   * <p>
   * This method is intended for tests.
   * </p>
   * @return internal cached-block map
   */
  Map<BlockCacheKey, LruCachedBlock> getMapForTests() {
    return map;
  }

}
