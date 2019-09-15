/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockCacheUtil;
import org.apache.hadoop.hbase.io.hfile.BlockPriority;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializerIdManager;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.IdReadWriteLock;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * BucketCache uses {@link BucketAllocator} to allocate/free blocks, and uses
 * BucketCache#ramCache and BucketCache#backingMap in order to
 * determine if a given element is in the cache. The bucket cache can use on-heap or
 * off-heap memory {@link ByteBufferIOEngine} or in a file {@link FileIOEngine} to
 * store/read the block data.
 *
 * <p>Eviction is via a similar algorithm as used in
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache}
 *
 * <p>BucketCache can be used as mainly a block cache (see
 * {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache}), combined with
 * combined with LruBlockCache to decrease CMS GC and heap fragmentation.
 *
 * <p>It also can be used as a secondary cache (e.g. using a file on ssd/fusionio to store
 * blocks) to enlarge cache space via
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache#setVictimCache}
 */
@InterfaceAudience.Private
public class BucketCache implements BlockCache, HeapSize {
  private static final Log LOG = LogFactory.getLog(BucketCache.class);

  /** Priority buckets config */
  static final String SINGLE_FACTOR_CONFIG_NAME = "hbase.bucketcache.single.factor";
  static final String MULTI_FACTOR_CONFIG_NAME = "hbase.bucketcache.multi.factor";
  static final String MEMORY_FACTOR_CONFIG_NAME = "hbase.bucketcache.memory.factor";
  static final String EXTRA_FREE_FACTOR_CONFIG_NAME = "hbase.bucketcache.extrafreefactor";
  static final String ACCEPT_FACTOR_CONFIG_NAME = "hbase.bucketcache.acceptfactor";
  static final String MIN_FACTOR_CONFIG_NAME = "hbase.bucketcache.minfactor";

  /** Priority buckets */
  @VisibleForTesting
  static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  static final float DEFAULT_MULTI_FACTOR = 0.50f;
  static final float DEFAULT_MEMORY_FACTOR = 0.25f;
  static final float DEFAULT_MIN_FACTOR = 0.85f;

  private static final float DEFAULT_EXTRA_FREE_FACTOR = 0.10f;
  private static final float DEFAULT_ACCEPT_FACTOR = 0.95f;

  // Number of blocks to clear for each of the bucket size that is full
  private static final int DEFAULT_FREE_ENTIRE_BLOCK_FACTOR = 2;

  /** Statistics thread */
  private static final int statThreadPeriod = 5 * 60;

  final static int DEFAULT_WRITER_THREADS = 3;
  final static int DEFAULT_WRITER_QUEUE_ITEMS = 64;

  // Store/read block data
  final IOEngine ioEngine;

  // Store the block in this map before writing it to cache
  @VisibleForTesting
  final ConcurrentMap<BlockCacheKey, RAMQueueEntry> ramCache;
  // In this map, store the block's meta data like offset, length
  @VisibleForTesting
  ConcurrentMap<BlockCacheKey, BucketEntry> backingMap;

  /**
   * Flag if the cache is enabled or not... We shut it off if there are IO
   * errors for some time, so that Bucket IO exceptions/errors don't bring down
   * the HBase server.
   */
  private volatile boolean cacheEnabled;

  /**
   * A list of writer queues.  We have a queue per {@link WriterThread} we have running.
   * In other words, the work adding blocks to the BucketCache is divided up amongst the
   * running WriterThreads.  Its done by taking hash of the cache key modulo queue count.
   * WriterThread when it runs takes whatever has been recently added and 'drains' the entries
   * to the BucketCache.  It then updates the ramCache and backingMap accordingly.
   */
  @VisibleForTesting
  final ArrayList<BlockingQueue<RAMQueueEntry>> writerQueues =
      new ArrayList<BlockingQueue<RAMQueueEntry>>();
  @VisibleForTesting
  final WriterThread[] writerThreads;

  /** Volatile boolean to track if free space is in process or not */
  private volatile boolean freeInProgress = false;
  private final Lock freeSpaceLock = new ReentrantLock();

  private UniqueIndexMap<Integer> deserialiserMap = new UniqueIndexMap<Integer>();

  private final AtomicLong realCacheSize = new AtomicLong(0);
  private final AtomicLong heapSize = new AtomicLong(0);
  /** Current number of cached elements */
  private final AtomicLong blockNumber = new AtomicLong(0);

  /** Cache access count (sequential ID) */
  private final AtomicLong accessCount = new AtomicLong(0);

  private static final int DEFAULT_CACHE_WAIT_TIME = 50;
  // Used in test now. If the flag is false and the cache speed is very fast,
  // bucket cache will skip some blocks when caching. If the flag is true, we
  // will wait blocks flushed to IOEngine for some time when caching
  boolean wait_when_cache = false;

  private final BucketCacheStats cacheStats = new BucketCacheStats();

  private final String persistencePath;
  private final long cacheCapacity;
  /** Approximate block size */
  private final long blockSize;

  /** Duration of IO errors tolerated before we disable cache, 1 min as default */
  private final int ioErrorsTolerationDuration;
  // 1 min
  public static final int DEFAULT_ERROR_TOLERATION_DURATION = 60 * 1000;

  // Start time of first IO error when reading or writing IO Engine, it will be
  // reset after a successful read/write.
  private volatile long ioErrorStartTime = -1;

  /**
   * A ReentrantReadWriteLock to lock on a particular block identified by offset.
   * The purpose of this is to avoid freeing the block which is being read.
   */
  @VisibleForTesting
  final IdReadWriteLock offsetLock = new IdReadWriteLock();

  private final NavigableSet<BlockCacheKey> blocksByHFile =
      new ConcurrentSkipListSet<BlockCacheKey>(new Comparator<BlockCacheKey>() {
        @Override
        public int compare(BlockCacheKey a, BlockCacheKey b) {
          int nameComparison = a.getHfileName().compareTo(b.getHfileName());
          if (nameComparison != 0) {
            return nameComparison;
          }

          if (a.getOffset() == b.getOffset()) {
            return 0;
          } else if (a.getOffset() < b.getOffset()) {
            return -1;
          }
          return 1;
        }
      });

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private final ScheduledExecutorService scheduleThreadPool = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setNameFormat("BucketCacheStatsExecutor").setDaemon(true).build());

  // Allocate or free space for the block
  private BucketAllocator bucketAllocator;

  /** Acceptable size of cache (no evictions if size < acceptable) */
  private float acceptableFactor;

  /** Minimum threshold of cache (when evicting, evict until size < min) */
  private float minFactor;

  /** Free this floating point factor of extra blocks when evicting. For example free the number of blocks requested * (1 + extraFreeFactor) */
  private float extraFreeFactor;

  /** Single access bucket size */
  private float singleFactor;

  /** Multiple access bucket size */
  private float multiFactor;

  /** In-memory bucket size */
  private float memoryFactor;

  private String ioEngineName;
  private static final String FILE_VERIFY_ALGORITHM =
    "hbase.bucketcache.persistent.file.integrity.check.algorithm";
  private static final String DEFAULT_FILE_VERIFY_ALGORITHM = "MD5";

  /**
   * Use {@link java.security.MessageDigest} class's encryption algorithms to check
   * persistent file integrity, default algorithm is MD5
   * */
  private String algorithm;

  public BucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
      int writerThreadNum, int writerQLen, String persistencePath) throws FileNotFoundException,
      IOException {
    this(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum, writerQLen,
      persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, HBaseConfiguration.create());
  }

  public BucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
                     int writerThreadNum, int writerQLen, String persistencePath, int ioErrorsTolerationDuration,
                     Configuration conf)
      throws IOException {
    this.writerThreads = new WriterThread[writerThreadNum];
    long blockNumCapacity = capacity / blockSize;
    if (blockNumCapacity >= Integer.MAX_VALUE) {
      // Enough for about 32TB of cache!
      throw new IllegalArgumentException("Cache capacity is too large, only support 32TB now");
    }

    this.acceptableFactor = conf.getFloat(ACCEPT_FACTOR_CONFIG_NAME, DEFAULT_ACCEPT_FACTOR);
    this.minFactor = conf.getFloat(MIN_FACTOR_CONFIG_NAME, DEFAULT_MIN_FACTOR);
    this.extraFreeFactor = conf.getFloat(EXTRA_FREE_FACTOR_CONFIG_NAME, DEFAULT_EXTRA_FREE_FACTOR);
    this.singleFactor = conf.getFloat(SINGLE_FACTOR_CONFIG_NAME, DEFAULT_SINGLE_FACTOR);
    this.multiFactor = conf.getFloat(MULTI_FACTOR_CONFIG_NAME, DEFAULT_MULTI_FACTOR);
    this.memoryFactor = conf.getFloat(MEMORY_FACTOR_CONFIG_NAME, DEFAULT_MEMORY_FACTOR);

    sanityCheckConfigs();

    LOG.info("Instantiating BucketCache with acceptableFactor: " + acceptableFactor + ", minFactor: " + minFactor +
        ", extraFreeFactor: " + extraFreeFactor + ", singleFactor: " + singleFactor + ", multiFactor: " + multiFactor +
        ", memoryFactor: " + memoryFactor);

    this.cacheCapacity = capacity;
    this.ioEngineName = ioEngineName;
    this.persistencePath = persistencePath;
    this.blockSize = blockSize;
    this.ioErrorsTolerationDuration = ioErrorsTolerationDuration;

    bucketAllocator = new BucketAllocator(capacity, bucketSizes);
    for (int i = 0; i < writerThreads.length; ++i) {
      writerQueues.add(new ArrayBlockingQueue<RAMQueueEntry>(writerQLen));
    }

    assert writerQueues.size() == writerThreads.length;
    this.ramCache = new ConcurrentHashMap<BlockCacheKey, RAMQueueEntry>();

    this.backingMap = new ConcurrentHashMap<BlockCacheKey, BucketEntry>((int) blockNumCapacity);
    this.algorithm = conf.get(FILE_VERIFY_ALGORITHM, DEFAULT_FILE_VERIFY_ALGORITHM);
    ioEngine = getIOEngineFromName();
    if (ioEngine.isPersistent() && persistencePath != null) {
      try {
        retrieveFromFile(bucketSizes);
      } catch (IOException ioex) {
        LOG.error("Can't restore from file because of", ioex);
      } catch (ClassNotFoundException cnfe) {
        LOG.error("Can't restore from file in rebuild because can't deserialise", cnfe);
        throw new RuntimeException(cnfe);
      }
    }
    final String threadName = Thread.currentThread().getName();
    this.cacheEnabled = true;
    for (int i = 0; i < writerThreads.length; ++i) {
      writerThreads[i] = new WriterThread(writerQueues.get(i));
      writerThreads[i].setName(threadName + "-BucketCacheWriter-" + i);
      writerThreads[i].setDaemon(true);
    }
    startWriterThreads();

    // Run the statistics thread periodically to print the cache statistics log
    // TODO: Add means of turning this off.  Bit obnoxious running thread just to make a log
    // every five minutes.
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        statThreadPeriod, statThreadPeriod, TimeUnit.SECONDS);
    LOG.info("Started bucket cache; ioengine=" + ioEngineName +
        ", capacity=" + StringUtils.byteDesc(capacity) +
      ", blockSize=" + StringUtils.byteDesc(blockSize) + ", writerThreadNum=" +
        writerThreadNum + ", writerQLen=" + writerQLen + ", persistencePath=" +
      persistencePath + ", bucketAllocator=" + this.bucketAllocator.getClass().getName());
  }

  private void sanityCheckConfigs() {
    Preconditions.checkArgument(acceptableFactor <= 1 && acceptableFactor >= 0, ACCEPT_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(minFactor <= 1 && minFactor >= 0, MIN_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(minFactor <= acceptableFactor, MIN_FACTOR_CONFIG_NAME + " must be <= " + ACCEPT_FACTOR_CONFIG_NAME);
    Preconditions.checkArgument(extraFreeFactor >= 0, EXTRA_FREE_FACTOR_CONFIG_NAME + " must be greater than 0.0");
    Preconditions.checkArgument(singleFactor <= 1 && singleFactor >= 0, SINGLE_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(multiFactor <= 1 && multiFactor >= 0, MULTI_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument(memoryFactor <= 1 && memoryFactor >= 0, MEMORY_FACTOR_CONFIG_NAME + " must be between 0.0 and 1.0");
    Preconditions.checkArgument((singleFactor + multiFactor + memoryFactor) == 1, SINGLE_FACTOR_CONFIG_NAME + ", " +
        MULTI_FACTOR_CONFIG_NAME + ", and " + MEMORY_FACTOR_CONFIG_NAME + " segments must add up to 1.0");
  }

  /**
   * Called by the constructor to start the writer threads. Used by tests that need to override
   * starting the threads.
   */
  @VisibleForTesting
  protected void startWriterThreads() {
    for (WriterThread thread : writerThreads) {
      thread.start();
    }
  }

  @VisibleForTesting
  boolean isCacheEnabled() {
    return this.cacheEnabled;
  }

  @Override
  public long getMaxSize() {
    return this.cacheCapacity;
  }

  public String getIoEngine() {
    return ioEngine.toString();
  }

  /**
   * Get the IOEngine from the IO engine name
   * @return the IOEngine
   * @throws IOException
   */
  private IOEngine getIOEngineFromName()
      throws IOException {
    if (ioEngineName.startsWith("file:") || ioEngineName.startsWith("files:")) {
      // In order to make the usage simple, we only need the prefix 'files:' in
      // document whether one or multiple file(s), but also support 'file:' for
      // the compatibility
      String[] filePaths =
          ioEngineName.substring(ioEngineName.indexOf(":") + 1).split(FileIOEngine.FILE_DELIMITER);
      return new FileIOEngine(algorithm, persistencePath, cacheCapacity, filePaths);
    } else if (ioEngineName.startsWith("offheap"))
      return new ByteBufferIOEngine(cacheCapacity, true);
    else if (ioEngineName.startsWith("heap"))
      return new ByteBufferIOEngine(cacheCapacity, false);
    else
      throw new IllegalArgumentException(
          "Don't understand io engine name for cache - prefix with file:, heap or offheap");
  }

  /**
   * Cache the block with the specified name and buffer.
   * @param cacheKey block's cache key
   * @param buf block buffer
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false, false);
  }

  /**
   * Cache the block with the specified name and buffer.
   * @param cacheKey block's cache key
   * @param cachedItem block buffer
   * @param inMemory if block is in-memory
   * @param cacheDataInL1
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable cachedItem, boolean inMemory,
      final boolean cacheDataInL1) {
    cacheBlockWithWait(cacheKey, cachedItem, inMemory, cacheDataInL1, wait_when_cache);
  }

  /**
   * Cache the block to ramCache
   * @param cacheKey block's cache key
   * @param cachedItem block buffer
   * @param inMemory if block is in-memory
   * @param wait if true, blocking wait when queue is full
   */
  public void cacheBlockWithWait(BlockCacheKey cacheKey, Cacheable cachedItem, boolean inMemory,
      boolean cacheDataInL1, boolean wait) {
    if (cacheEnabled) {
      if (backingMap.containsKey(cacheKey) || ramCache.containsKey(cacheKey)) {
        if (!cacheDataInL1
            && BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, cachedItem)) {
          cacheBlockWithWaitInternal(cacheKey, cachedItem, inMemory, cacheDataInL1, wait);
        }
      } else {
        cacheBlockWithWaitInternal(cacheKey, cachedItem, inMemory, cacheDataInL1, wait);
      }
    }
  }

  private void cacheBlockWithWaitInternal(BlockCacheKey cacheKey, Cacheable cachedItem,
      boolean inMemory, boolean cacheDataInL1, boolean wait) {
    if (LOG.isTraceEnabled()) LOG.trace("Caching key=" + cacheKey + ", item=" + cachedItem);
    if (!cacheEnabled) {
      return;
    }

    // Stuff the entry into the RAM cache so it can get drained to the persistent store
    RAMQueueEntry re =
        new RAMQueueEntry(cacheKey, cachedItem, accessCount.incrementAndGet(), inMemory);
    /**
     * Don't use ramCache.put(cacheKey, re) here. because there may be a existing entry with same
     * key in ramCache, the heap size of bucket cache need to update if replacing entry from
     * ramCache. But WriterThread will also remove entry from ramCache and update heap size, if
     * using ramCache.put(), It's possible that the removed entry in WriterThread is not the correct
     * one, then the heap size will mess up (HBASE-20789)
     */
    if (ramCache.putIfAbsent(cacheKey, re) != null) {
      return;
    }
    int queueNum = (cacheKey.hashCode() & 0x7FFFFFFF) % writerQueues.size();
    BlockingQueue<RAMQueueEntry> bq = writerQueues.get(queueNum);
    boolean successfulAddition = false;
    if (wait) {
      try {
        successfulAddition = bq.offer(re, DEFAULT_CACHE_WAIT_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      successfulAddition = bq.offer(re);
    }
    if (!successfulAddition) {
      ramCache.remove(cacheKey);
      cacheStats.failInsert();
    } else {
      this.blockNumber.incrementAndGet();
      this.heapSize.addAndGet(cachedItem.heapSize());
      blocksByHFile.add(cacheKey);
    }
  }

  /**
   * Get the buffer of the block with the specified key.
   * @param key block's cache key
   * @param caching true if the caller caches blocks on cache misses
   * @param repeat Whether this is a repeat lookup for the same block
   * @param updateCacheMetrics Whether we should update cache metrics or not
   * @return buffer of specified cache key, or null if not in cache
   */
  @Override
  public Cacheable getBlock(BlockCacheKey key, boolean caching, boolean repeat,
      boolean updateCacheMetrics) {
    if (!cacheEnabled) {
      return null;
    }
    RAMQueueEntry re = ramCache.get(key);
    if (re != null) {
      if (updateCacheMetrics) {
        cacheStats.hit(caching, key.isPrimary(), key.getBlockType());
      }
      re.access(accessCount.incrementAndGet());
      return re.getData();
    }
    BucketEntry bucketEntry = backingMap.get(key);
    if (bucketEntry != null) {
      long start = System.nanoTime();
      ReentrantReadWriteLock lock = offsetLock.getLock(bucketEntry.offset());
      try {
        lock.readLock().lock();
        // We can not read here even if backingMap does contain the given key because its offset
        // maybe changed. If we lock BlockCacheKey instead of offset, then we can only check
        // existence here.
        if (bucketEntry.equals(backingMap.get(key))) {
          int len = bucketEntry.getLength();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Read offset=" + bucketEntry.offset() + ", len=" + len);
          }
          ByteBuffer bb = ByteBuffer.allocate(len);
          int lenRead = ioEngine.read(bb, bucketEntry.offset());
          if (lenRead != len) {
            throw new RuntimeException("Only " + lenRead + " bytes read, " + len + " expected");
          }
          CacheableDeserializer<Cacheable> deserializer =
            bucketEntry.deserializerReference(this.deserialiserMap);
          Cacheable cachedBlock = deserializer.deserialize(bb, true);
          long timeTaken = System.nanoTime() - start;
          if (updateCacheMetrics) {
            cacheStats.hit(caching, key.isPrimary(), key.getBlockType());
            cacheStats.ioHit(timeTaken);
          }
          bucketEntry.access(accessCount.incrementAndGet());
          if (this.ioErrorStartTime > 0) {
            ioErrorStartTime = -1;
          }
          return cachedBlock;
        }
      } catch (IOException ioex) {
        LOG.error("Failed reading block " + key + " from bucket cache", ioex);
        checkIOErrorIsTolerated();
      } finally {
        lock.readLock().unlock();
      }
    }
    if (!repeat && updateCacheMetrics) {
      cacheStats.miss(caching, key.isPrimary(), key.getBlockType());
    }
    return null;
  }

  @VisibleForTesting
  void blockEvicted(BlockCacheKey cacheKey, BucketEntry bucketEntry, boolean decrementBlockNumber) {
    bucketAllocator.freeBlock(bucketEntry.offset());
    realCacheSize.addAndGet(-1 * bucketEntry.getLength());
    blocksByHFile.remove(cacheKey);
    if (decrementBlockNumber) {
      this.blockNumber.decrementAndGet();
    }
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    if (!cacheEnabled) {
      return false;
    }
    RAMQueueEntry removedBlock = ramCache.remove(cacheKey);
    if (removedBlock != null) {
      this.blockNumber.decrementAndGet();
      this.heapSize.addAndGet(-1 * removedBlock.getData().heapSize());
    }
    BucketEntry bucketEntry = backingMap.get(cacheKey);
    if (bucketEntry == null) {
      if (removedBlock != null) {
        cacheStats.evicted(0, cacheKey.isPrimary());
        return true;
      } else {
        return false;
      }
    }
    ReentrantReadWriteLock lock = offsetLock.getLock(bucketEntry.offset());
    try {
      lock.writeLock().lock();
      if (backingMap.remove(cacheKey, bucketEntry)) {
        blockEvicted(cacheKey, bucketEntry, removedBlock == null);
      } else {
        return false;
      }
    } finally {
      lock.writeLock().unlock();
    }
    cacheStats.evicted(bucketEntry.getCachedTime(), cacheKey.isPrimary());
    return true;
  }

  /*
   * Statistics thread.  Periodically output cache statistics to the log.
   */
  private static class StatisticsThread extends Thread {
    private final BucketCache bucketCache;

    public StatisticsThread(BucketCache bucketCache) {
      super("BucketCacheStatsThread");
      setDaemon(true);
      this.bucketCache = bucketCache;
    }

    @Override
    public void run() {
      bucketCache.logStats();
    }
  }

  public void logStats() {
    long totalSize = bucketAllocator.getTotalSize();
    long usedSize = bucketAllocator.getUsedSize();
    long freeSize = totalSize - usedSize;
    long cacheSize = getRealCacheSize();
    LOG.info("failedBlockAdditions=" + cacheStats.getFailedInserts() + ", " +
        "totalSize=" + StringUtils.byteDesc(totalSize) + ", " +
        "freeSize=" + StringUtils.byteDesc(freeSize) + ", " +
        "usedSize=" + StringUtils.byteDesc(usedSize) +", " +
        "cacheSize=" + StringUtils.byteDesc(cacheSize) +", " +
        "accesses=" + cacheStats.getRequestCount() + ", " +
        "hits=" + cacheStats.getHitCount() + ", " +
        "IOhitsPerSecond=" + cacheStats.getIOHitsPerSecond() + ", " +
        "IOTimePerHit=" + String.format("%.2f", cacheStats.getIOTimePerHit())+ ", " +
        "hitRatio=" + (cacheStats.getHitCount() == 0 ? "0," :
          (StringUtils.formatPercent(cacheStats.getHitRatio(), 2)+ ", ")) +
        "cachingAccesses=" + cacheStats.getRequestCachingCount() + ", " +
        "cachingHits=" + cacheStats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" +(cacheStats.getHitCachingCount() == 0 ? "0," :
          (StringUtils.formatPercent(cacheStats.getHitCachingRatio(), 2)+ ", ")) +
        "evictions=" + cacheStats.getEvictionCount() + ", " +
        "evicted=" + cacheStats.getEvictedCount() + ", " +
        "evictedPerRun=" + cacheStats.evictedPerEviction());
    cacheStats.reset();
  }

  public long getRealCacheSize() {
    return this.realCacheSize.get();
  }

  private long acceptableSize() {
    return (long) Math.floor(bucketAllocator.getTotalSize() * acceptableFactor);
  }

  @VisibleForTesting
  long getPartitionSize(float partitionFactor) {
    return (long) Math.floor(bucketAllocator.getTotalSize() * partitionFactor * minFactor);
  }

  /**
   * Return the count of bucketSizeinfos still need free space
   */
  private int bucketSizesAboveThresholdCount(float minFactor) {
    BucketAllocator.IndexStatistics[] stats = bucketAllocator.getIndexStatistics();
    int fullCount = 0;
    for (int i = 0; i < stats.length; i++) {
      long freeGoal = (long) Math.floor(stats[i].totalCount() * (1 - minFactor));
      freeGoal = Math.max(freeGoal, 1);
      if (stats[i].freeCount() < freeGoal) {
        fullCount++;
      }
    }
    return fullCount;
  }

  /**
   * This method will find the buckets that are minimally occupied
   * and are not reference counted and will free them completely
   * without any constraint on the access times of the elements,
   * and as a process will completely free at most the number of buckets
   * passed, sometimes it might not due to changing refCounts
   *
   * @param completelyFreeBucketsNeeded number of buckets to free
   **/
  private void freeEntireBuckets(int completelyFreeBucketsNeeded) {
    if (completelyFreeBucketsNeeded != 0) {
      // First we will build a set where the offsets are reference counted, usually
      // this set is small around O(Handler Count) unless something else is wrong
      Set<Integer> inUseBuckets = new HashSet<Integer>();
      for (BucketEntry entry : backingMap.values()) {
        inUseBuckets.add(bucketAllocator.getBucketIndex(entry.offset()));
      }

      Set<Integer> candidateBuckets = bucketAllocator.getLeastFilledBuckets(
          inUseBuckets, completelyFreeBucketsNeeded);
      for (Map.Entry<BlockCacheKey, BucketEntry> entry : backingMap.entrySet()) {
        if (candidateBuckets.contains(bucketAllocator
            .getBucketIndex(entry.getValue().offset()))) {
          evictBlock(entry.getKey());
        }
      }
    }
  }

  /**
   * Free the space if the used size reaches acceptableSize() or one size block
   * couldn't be allocated. When freeing the space, we use the LRU algorithm and
   * ensure there must be some blocks evicted
   * @param why Why we are being called
   */
  private void freeSpace(final String why) {
    // Ensure only one freeSpace progress at a time
    if (!freeSpaceLock.tryLock()) {
      return;
    }
    try {
      freeInProgress = true;
      long bytesToFreeWithoutExtra = 0;
      // Calculate free byte for each bucketSizeinfo
      StringBuffer msgBuffer = LOG.isDebugEnabled()? new StringBuffer(): null;
      BucketAllocator.IndexStatistics[] stats = bucketAllocator.getIndexStatistics();
      long[] bytesToFreeForBucket = new long[stats.length];
      for (int i = 0; i < stats.length; i++) {
        bytesToFreeForBucket[i] = 0;
        long freeGoal = (long) Math.floor(stats[i].totalCount() * (1 - minFactor));
        freeGoal = Math.max(freeGoal, 1);
        if (stats[i].freeCount() < freeGoal) {
          bytesToFreeForBucket[i] = stats[i].itemSize() * (freeGoal - stats[i].freeCount());
          bytesToFreeWithoutExtra += bytesToFreeForBucket[i];
          if (msgBuffer != null) {
            msgBuffer.append("Free for bucketSize(" + stats[i].itemSize() + ")="
              + StringUtils.byteDesc(bytesToFreeForBucket[i]) + ", ");
          }
        }
      }
      if (msgBuffer != null) {
        msgBuffer.append("Free for total=" + StringUtils.byteDesc(bytesToFreeWithoutExtra) + ", ");
      }

      if (bytesToFreeWithoutExtra <= 0) {
        return;
      }
      long currentSize = bucketAllocator.getUsedSize();
      long totalSize = bucketAllocator.getTotalSize();
      if (LOG.isDebugEnabled() && msgBuffer != null) {
        LOG.debug("Free started because \"" + why + "\"; " + msgBuffer.toString() +
          " of current used=" + StringUtils.byteDesc(currentSize) + ", actual cacheSize=" +
          StringUtils.byteDesc(realCacheSize.get()) + ", total=" + StringUtils.byteDesc(totalSize));
      }

      long bytesToFreeWithExtra = (long) Math.floor(bytesToFreeWithoutExtra
          * (1 + extraFreeFactor));

      // Instantiate priority buckets
      BucketEntryGroup bucketSingle = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, getPartitionSize(singleFactor));
      BucketEntryGroup bucketMulti = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, getPartitionSize(multiFactor));
      BucketEntryGroup bucketMemory = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, getPartitionSize(memoryFactor));

      // Scan entire map putting bucket entry into appropriate bucket entry
      // group
      for (Map.Entry<BlockCacheKey, BucketEntry> bucketEntryWithKey : backingMap.entrySet()) {
        switch (bucketEntryWithKey.getValue().getPriority()) {
          case SINGLE: {
            bucketSingle.add(bucketEntryWithKey);
            break;
          }
          case MULTI: {
            bucketMulti.add(bucketEntryWithKey);
            break;
          }
          case MEMORY: {
            bucketMemory.add(bucketEntryWithKey);
            break;
          }
        }
      }

      PriorityQueue<BucketEntryGroup> bucketQueue = new PriorityQueue<BucketEntryGroup>(3);

      bucketQueue.add(bucketSingle);
      bucketQueue.add(bucketMulti);
      bucketQueue.add(bucketMemory);

      int remainingBuckets = bucketQueue.size();
      long bytesFreed = 0;

      BucketEntryGroup bucketGroup;
      while ((bucketGroup = bucketQueue.poll()) != null) {
        long overflow = bucketGroup.overflow();
        if (overflow > 0) {
          long bucketBytesToFree = Math.min(overflow,
              (bytesToFreeWithoutExtra - bytesFreed) / remainingBuckets);
          bytesFreed += bucketGroup.free(bucketBytesToFree);
        }
        remainingBuckets--;
      }

      // Check and free if there are buckets that still need freeing of space
      if (bucketSizesAboveThresholdCount(minFactor) > 0) {
        bucketQueue.clear();
        remainingBuckets = 3;

        bucketQueue.add(bucketSingle);
        bucketQueue.add(bucketMulti);
        bucketQueue.add(bucketMemory);

        while ((bucketGroup = bucketQueue.poll()) != null) {
          long bucketBytesToFree = (bytesToFreeWithExtra - bytesFreed) / remainingBuckets;
          bytesFreed += bucketGroup.free(bucketBytesToFree);
          remainingBuckets--;
        }
      }

      // Even after the above free we might still need freeing because of the
      // De-fragmentation of the buckets (also called Slab Calcification problem), i.e
      // there might be some buckets where the occupancy is very sparse and thus are not
      // yielding the free for the other bucket sizes, the fix for this to evict some
      // of the buckets, we do this by evicting the buckets that are least fulled
      freeEntireBuckets(DEFAULT_FREE_ENTIRE_BLOCK_FACTOR *
          bucketSizesAboveThresholdCount(1.0f));

      if (LOG.isDebugEnabled()) {
        long single = bucketSingle.totalSize();
        long multi = bucketMulti.totalSize();
        long memory = bucketMemory.totalSize();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Bucket cache free space completed; " + "freed="
            + StringUtils.byteDesc(bytesFreed) + ", " + "total="
            + StringUtils.byteDesc(totalSize) + ", " + "single="
            + StringUtils.byteDesc(single) + ", " + "multi="
            + StringUtils.byteDesc(multi) + ", " + "memory="
            + StringUtils.byteDesc(memory));
        }
      }

    } catch (Throwable t) {
      LOG.warn("Failed freeing space", t);
    } finally {
      cacheStats.evict();
      freeInProgress = false;
      freeSpaceLock.unlock();
    }
  }

  // This handles flushing the RAM cache to IOEngine.
  @VisibleForTesting
  class WriterThread extends HasThread {
    private final BlockingQueue<RAMQueueEntry> inputQueue;
    private volatile boolean writerEnabled = true;

    WriterThread(BlockingQueue<RAMQueueEntry> queue) {
      super("BucketCacheWriterThread");
      this.inputQueue = queue;
    }

    // Used for test
    @VisibleForTesting
    void disableWriter() {
      this.writerEnabled = false;
    }

    @Override
    public void run() {
      List<RAMQueueEntry> entries = new ArrayList<RAMQueueEntry>();
      try {
        while (cacheEnabled && writerEnabled) {
          try {
            try {
              // Blocks
              entries = getRAMQueueEntries(inputQueue, entries);
            } catch (InterruptedException ie) {
              if (!cacheEnabled) break;
            }
            doDrain(entries);
          } catch (Exception ioe) {
            LOG.error("WriterThread encountered error", ioe);
          }
        }
      } catch (Throwable t) {
        LOG.warn("Failed doing drain", t);
      }
      LOG.info(this.getName() + " exiting, cacheEnabled=" + cacheEnabled);
    }

    /**
     * Put the new bucket entry into backingMap. Notice that we are allowed to replace the existing
     * cache with a new block for the same cache key. there's a corner case: one thread cache a
     * block in ramCache, copy to io-engine and add a bucket entry to backingMap. Caching another
     * new block with the same cache key do the same thing for the same cache key, so if not evict
     * the previous bucket entry, then memory leak happen because the previous bucketEntry is gone
     * but the bucketAllocator do not free its memory.
     * @see BlockCacheUtil#shouldReplaceExistingCacheBlock(BlockCache blockCache,BlockCacheKey
     *      cacheKey, Cacheable newBlock)
     * @param key Block cache key
     * @param bucketEntry Bucket entry to put into backingMap.
     */
    private void putIntoBackingMap(BlockCacheKey key, BucketEntry bucketEntry) {
      BucketEntry previousEntry = backingMap.put(key, bucketEntry);
      if (previousEntry != null && previousEntry != bucketEntry) {
        ReentrantReadWriteLock lock = offsetLock.getLock(previousEntry.offset());
        lock.writeLock().lock();
        try {
          blockEvicted(key, previousEntry, false);
        } finally {
          lock.writeLock().unlock();
        }
      }
    }

    /**
     * Flush the entries in ramCache to IOEngine and add bucket entry to backingMap.
     * Process all that are passed in even if failure being sure to remove from ramCache else we'll
     * never undo the references and we'll OOME.
     * @param entries Presumes list passed in here will be processed by this invocation only. No
     * interference expected.
     * @throws InterruptedException
     */
    @VisibleForTesting
    void doDrain(final List<RAMQueueEntry> entries) throws InterruptedException {
      if (entries.isEmpty()) {
        return;
      }
      // This method is a little hard to follow. We run through the passed in entries and for each
      // successful add, we add a non-null BucketEntry to the below bucketEntries.  Later we must
      // do cleanup making sure we've cleared ramCache of all entries regardless of whether we
      // successfully added the item to the bucketcache; if we don't do the cleanup, we'll OOME by
      // filling ramCache.  We do the clean up by again running through the passed in entries
      // doing extra work when we find a non-null bucketEntries corresponding entry.
      final int size = entries.size();
      BucketEntry[] bucketEntries = new BucketEntry[size];
      // Index updated inside loop if success or if we can't succeed. We retry if cache is full
      // when we go to add an entry by going around the loop again without upping the index.
      int index = 0;
      while (cacheEnabled && index < size) {
        RAMQueueEntry re = null;
        try {
          re = entries.get(index);
          if (re == null) {
            LOG.warn("Couldn't get entry or changed on us; who else is messing with it?");
            index++;
            continue;
          }
          BucketEntry bucketEntry =
            re.writeToCache(ioEngine, bucketAllocator, deserialiserMap, realCacheSize);
          // Successfully added.  Up index and add bucketEntry. Clear io exceptions.
          bucketEntries[index] = bucketEntry;
          if (ioErrorStartTime > 0) {
            ioErrorStartTime = -1;
          }
          index++;
        } catch (BucketAllocatorException fle) {
          LOG.warn("Failed allocation for " + (re == null ? "" : re.getKey()) + "; " + fle);
          // Presume can't add. Too big? Move index on. Entry will be cleared from ramCache below.
          bucketEntries[index] = null;
          index++;
        } catch (CacheFullException cfe) {
          // Cache full when we tried to add. Try freeing space and then retrying (don't up index)
          if (!freeInProgress) {
            freeSpace("Full!");
          } else {
            Thread.sleep(50);
          }
        } catch (IOException ioex) {
          // Hopefully transient. Retry. checkIOErrorIsTolerated disables cache if problem.
          LOG.error("Failed writing to bucket cache", ioex);
          checkIOErrorIsTolerated();
        }
      }

      // Make sure data pages are written on media before we update maps.
      try {
        ioEngine.sync();
      } catch (IOException ioex) {
        LOG.error("Failed syncing IO engine", ioex);
        checkIOErrorIsTolerated();
        // Since we failed sync, free the blocks in bucket allocator
        for (int i = 0; i < entries.size(); ++i) {
          if (bucketEntries[i] != null) {
            bucketAllocator.freeBlock(bucketEntries[i].offset());
            bucketEntries[i] = null;
          }
        }
      }

      // Now add to backingMap if successfully added to bucket cache.  Remove from ramCache if
      // success or error.
      for (int i = 0; i < size; ++i) {
        BlockCacheKey key = entries.get(i).getKey();
        // Only add if non-null entry.
        if (bucketEntries[i] != null) {
          putIntoBackingMap(key, bucketEntries[i]);
        }
        // Always remove from ramCache even if we failed adding it to the block cache above.
        RAMQueueEntry ramCacheEntry = ramCache.remove(key);
        if (ramCacheEntry != null) {
          heapSize.addAndGet(-1 * entries.get(i).getData().heapSize());
        } else if (bucketEntries[i] != null){
          // Block should have already been evicted. Remove it and free space.
          ReentrantReadWriteLock lock = offsetLock.getLock(bucketEntries[i].offset());
          try {
            lock.writeLock().lock();
            if (backingMap.remove(key, bucketEntries[i])) {
              blockEvicted(key, bucketEntries[i], false);
            }
          } finally {
            lock.writeLock().unlock();
          }
        }
      }

      long used = bucketAllocator.getUsedSize();
      if (used > acceptableSize()) {
        freeSpace("Used=" + used + " > acceptable=" + acceptableSize());
      }
      return;
    }
  }

  /**
   * Blocks until elements available in <code>q</code> then tries to grab as many as possible
   * before returning.
   * @param receptical Where to stash the elements taken from queue. We clear before we use it
   * just in case.
   * @param q The queue to take from.
   * @return receptical laden with elements taken from the queue or empty if none found.
   */
  @VisibleForTesting
  static List<RAMQueueEntry> getRAMQueueEntries(final BlockingQueue<RAMQueueEntry> q,
      final List<RAMQueueEntry> receptical)
  throws InterruptedException {
    // Clear sets all entries to null and sets size to 0. We retain allocations. Presume it
    // ok even if list grew to accommodate thousands.
    receptical.clear();
    receptical.add(q.take());
    q.drainTo(receptical);
    return receptical;
  }

  private void persistToFile() throws IOException {
    assert !cacheEnabled;
    try (ObjectOutputStream oos = new ObjectOutputStream(
      new FileOutputStream(persistencePath, false))){
      if (!ioEngine.isPersistent()) {
        throw new IOException("Attempt to persist non-persistent cache mappings!");
      }
      if (ioEngine instanceof PersistentIOEngine) {
        oos.write(ProtobufUtil.PB_MAGIC);
        byte[] checksum = ((PersistentIOEngine) ioEngine).calculateChecksum();
        oos.writeInt(checksum.length);
        oos.write(checksum);
      }
      oos.writeLong(cacheCapacity);
      oos.writeUTF(ioEngine.getClass().getName());
      oos.writeUTF(backingMap.getClass().getName());
      oos.writeObject(deserialiserMap);
      oos.writeObject(backingMap);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("No such algorithm : " + algorithm + "! Failed to persist data on exit",e);
    }
  }

  @SuppressWarnings("unchecked")
  private void retrieveFromFile(int[] bucketSizes) throws IOException,
      ClassNotFoundException {
    File persistenceFile = new File(persistencePath);
    if (!persistenceFile.exists()) {
      return;
    }
    assert !cacheEnabled;
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(persistencePath))){
      if (!ioEngine.isPersistent())
        throw new IOException(
            "Attempt to restore non-persistent cache mappings!");
      // for backward compatibility
      if (ioEngine instanceof PersistentIOEngine &&
        !((PersistentIOEngine) ioEngine).isOldVersion()) {
        byte[] PBMagic = new byte[ProtobufUtil.PB_MAGIC.length];
        ois.read(PBMagic);
        int length = ois.readInt();
        byte[] persistenceChecksum = new byte[length];
        ois.read(persistenceChecksum);
      }
      long capacitySize = ois.readLong();
      if (capacitySize != cacheCapacity)
        throw new IOException("Mismatched cache capacity:"
            + StringUtils.byteDesc(capacitySize) + ", expected: "
            + StringUtils.byteDesc(cacheCapacity));
      String ioclass = ois.readUTF();
      String mapclass = ois.readUTF();
      if (!ioEngine.getClass().getName().equals(ioclass))
        throw new IOException("Class name for IO engine mismatch: " + ioclass
            + ", expected:" + ioEngine.getClass().getName());
      if (!backingMap.getClass().getName().equals(mapclass))
        throw new IOException("Class name for cache map mismatch: " + mapclass
            + ", expected:" + backingMap.getClass().getName());
      UniqueIndexMap<Integer> deserMap = (UniqueIndexMap<Integer>) ois
          .readObject();
      ConcurrentHashMap<BlockCacheKey, BucketEntry> backingMapFromFile =
          (ConcurrentHashMap<BlockCacheKey, BucketEntry>) ois.readObject();
      BucketAllocator allocator = new BucketAllocator(cacheCapacity, bucketSizes,
        backingMapFromFile, realCacheSize);
      bucketAllocator = allocator;
      deserialiserMap = deserMap;
      backingMap = backingMapFromFile;
      blockNumber.set(backingMap.size());
    } finally {
      if (!persistenceFile.delete()) {
        throw new IOException("Failed deleting persistence file "
            + persistenceFile.getAbsolutePath());
      }
    }
  }

  /**
   * Check whether we tolerate IO error this time. If the duration of IOEngine
   * throwing errors exceeds ioErrorsDurationTimeTolerated, we will disable the
   * cache
   */
  private void checkIOErrorIsTolerated() {
    long now = EnvironmentEdgeManager.currentTime();
    if (this.ioErrorStartTime > 0) {
      if (cacheEnabled && (now - ioErrorStartTime) > this.ioErrorsTolerationDuration) {
        LOG.error("IO errors duration time has exceeded " + ioErrorsTolerationDuration +
          "ms, disabling cache, please check your IOEngine");
        disableCache();
      }
    } else {
      this.ioErrorStartTime = now;
    }
  }

  /**
   * Used to shut down the cache -or- turn it off in the case of something broken.
   */
  private void disableCache() {
    if (!cacheEnabled) return;
    cacheEnabled = false;
    ioEngine.shutdown();
    this.scheduleThreadPool.shutdown();
    for (int i = 0; i < writerThreads.length; ++i) writerThreads[i].interrupt();
    this.ramCache.clear();
    if (!ioEngine.isPersistent() || persistencePath == null) {
      // If persistent ioengine and a path, we will serialize out the backingMap.
      this.backingMap.clear();
    }
  }

  private void join() throws InterruptedException {
    for (int i = 0; i < writerThreads.length; ++i)
      writerThreads[i].join();
  }

  @Override
  public void shutdown() {
    disableCache();
    LOG.info("Shutdown bucket cache: IO persistent=" + ioEngine.isPersistent()
        + "; path to write=" + persistencePath);
    if (ioEngine.isPersistent() && persistencePath != null) {
      try {
        join();
        persistToFile();
      } catch (IOException ex) {
        LOG.error("Unable to persist data on exit: " + ex.toString(), ex);
      } catch (InterruptedException e) {
        LOG.warn("Failed to persist data on exit", e);
      }
    }
  }

  @Override
  public CacheStats getStats() {
    return cacheStats;
  }

  public BucketAllocator getAllocator() {
    return this.bucketAllocator;
  }

  @Override
  public long heapSize() {
    return this.heapSize.get();
  }

  @Override
  public long size() {
    return this.realCacheSize.get();
  }

  @Override
  public long getCurrentDataSize() {
    return size();
  }

  @Override
  public long getFreeSize() {
    return this.bucketAllocator.getFreeSize();
  }

  @Override
  public long getBlockCount() {
    return this.blockNumber.get();
  }

  @Override
  public long getDataBlockCount() {
    return getBlockCount();
  }

  @Override
  public long getCurrentSize() {
    return this.bucketAllocator.getUsedSize();
  }

  /**
   * Evicts all blocks for a specific HFile.
   * <p>
   * This is used for evict-on-close to remove all blocks of a specific HFile.
   *
   * @return the number of blocks evicted
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    Set<BlockCacheKey> keySet = blocksByHFile.subSet(
        new BlockCacheKey(hfileName, Long.MIN_VALUE), true,
        new BlockCacheKey(hfileName, Long.MAX_VALUE), true);

    int numEvicted = 0;
    for (BlockCacheKey key : keySet) {
      if (evictBlock(key)) {
          ++numEvicted;
      }
    }

    return numEvicted;
  }

  /**
   * Item in cache. We expect this to be where most memory goes. Java uses 8
   * bytes just for object headers; after this, we want to use as little as
   * possible - so we only use 8 bytes, but in order to do so we end up messing
   * around with all this Java casting stuff. Offset stored as 5 bytes that make
   * up the long. Doubt we'll see devices this big for ages. Offsets are divided
   * by 256. So 5 bytes gives us 256TB or so.
   */
  static class BucketEntry implements Serializable {
    private static final long serialVersionUID = -6741504807982257534L;

    // access counter comparator, descending order
    static final Comparator<BucketEntry> COMPARATOR = new Comparator<BucketCache.BucketEntry>() {

      @Override
      public int compare(BucketEntry o1, BucketEntry o2) {
        return Long.compare(o2.accessCounter, o1.accessCounter);
      }
    };

    private int offsetBase;
    private int length;
    private byte offset1;
    byte deserialiserIndex;
    private volatile long accessCounter;
    private BlockPriority priority;
    /**
     * Time this block was cached.  Presumes we are created just before we are added to the cache.
     */
    private final long cachedTime = System.nanoTime();

    BucketEntry(long offset, int length, long accessCounter, boolean inMemory) {
      setOffset(offset);
      this.length = length;
      this.accessCounter = accessCounter;
      if (inMemory) {
        this.priority = BlockPriority.MEMORY;
      } else {
        this.priority = BlockPriority.SINGLE;
      }
    }

    long offset() { // Java has no unsigned numbers
      long o = ((long) offsetBase) & 0xFFFFFFFFL; //This needs the L cast otherwise it will be sign extended as a negative number.
      o += (((long) (offset1)) & 0xFF) << 32; //The 0xFF here does not need the L cast because it is treated as a positive int.
      return o << 8;
    }

    private void setOffset(long value) {
      assert (value & 0xFF) == 0;
      value >>= 8;
      offsetBase = (int) value;
      offset1 = (byte) (value >> 32);
    }

    public int getLength() {
      return length;
    }

    protected CacheableDeserializer<Cacheable> deserializerReference(
        UniqueIndexMap<Integer> deserialiserMap) {
      return CacheableDeserializerIdManager.getDeserializer(deserialiserMap
          .unmap(deserialiserIndex));
    }

    protected void setDeserialiserReference(
        CacheableDeserializer<Cacheable> deserializer,
        UniqueIndexMap<Integer> deserialiserMap) {
      this.deserialiserIndex = ((byte) deserialiserMap.map(deserializer
          .getDeserialiserIdentifier()));
    }

    /**
     * Block has been accessed. Update its local access counter.
     */
    public void access(long accessCounter) {
      this.accessCounter = accessCounter;
      if (this.priority == BlockPriority.SINGLE) {
        this.priority = BlockPriority.MULTI;
      }
    }

    public BlockPriority getPriority() {
      return this.priority;
    }

    public long getCachedTime() {
      return cachedTime;
    }
  }

  /**
   * Used to group bucket entries into priority buckets. There will be a
   * BucketEntryGroup for each priority (single, multi, memory). Once bucketed,
   * the eviction algorithm takes the appropriate number of elements out of each
   * according to configuration parameters and their relative sizes.
   */
  private class BucketEntryGroup implements Comparable<BucketEntryGroup> {

    private CachedEntryQueue queue;
    private long totalSize = 0;
    private long bucketSize;

    public BucketEntryGroup(long bytesToFree, long blockSize, long bucketSize) {
      this.bucketSize = bucketSize;
      queue = new CachedEntryQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(Map.Entry<BlockCacheKey, BucketEntry> block) {
      totalSize += block.getValue().getLength();
      queue.add(block);
    }

    public long free(long toFree) {
      Map.Entry<BlockCacheKey, BucketEntry> entry;
      long freedBytes = 0;
      while ((entry = queue.pollLast()) != null) {
        evictBlock(entry.getKey());
        freedBytes += entry.getValue().getLength();
        if (freedBytes >= toFree) {
          return freedBytes;
        }
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
    public int compareTo(BucketEntryGroup that) {
      return Long.compare(this.overflow(), that.overflow());
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + (int) (bucketSize ^ (bucketSize >>> 32));
      result = prime * result + ((queue == null) ? 0 : queue.hashCode());
      result = prime * result + (int) (totalSize ^ (totalSize >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      BucketEntryGroup other = (BucketEntryGroup) obj;
      if (!getOuterType().equals(other.getOuterType())) {
        return false;
      }
      if (bucketSize != other.bucketSize) {
        return false;
      }
      if (queue == null) {
        if (other.queue != null) {
          return false;
        }
      } else if (!queue.equals(other.queue)) {
        return false;
      }
      if (totalSize != other.totalSize) {
        return false;
      }
      return true;
    }

    private BucketCache getOuterType() {
      return BucketCache.this;
    }

  }

  /**
   * Block Entry stored in the memory with key,data and so on
   */
  @VisibleForTesting
  static class RAMQueueEntry {
    private BlockCacheKey key;
    private Cacheable data;
    private long accessCounter;
    private boolean inMemory;

    public RAMQueueEntry(BlockCacheKey bck, Cacheable data, long accessCounter,
        boolean inMemory) {
      this.key = bck;
      this.data = data;
      this.accessCounter = accessCounter;
      this.inMemory = inMemory;
    }

    public Cacheable getData() {
      return data;
    }

    public BlockCacheKey getKey() {
      return key;
    }

    public void access(long accessCounter) {
      this.accessCounter = accessCounter;
    }

    public BucketEntry writeToCache(final IOEngine ioEngine, final BucketAllocator bucketAllocator,
        final UniqueIndexMap<Integer> deserialiserMap, final AtomicLong realCacheSize)
        throws IOException {
      int len = data.getSerializedLength();
      // This cacheable thing can't be serialized
      if (len == 0) {
        return null;
      }
      long offset = bucketAllocator.allocateBlock(len);
      BucketEntry bucketEntry;
      boolean succ = false;
      try {
        bucketEntry = new BucketEntry(offset, len, accessCounter, inMemory);
        bucketEntry.setDeserialiserReference(data.getDeserializer(), deserialiserMap);
        if (data instanceof HFileBlock) {
          // If an instance of HFileBlock, save on some allocations.
          HFileBlock block = (HFileBlock) data;
          ByteBuffer sliceBuf = block.getBufferReadOnly();
          ByteBuffer metadata = block.getMetaData();
          ioEngine.write(sliceBuf, offset);
          ioEngine.write(metadata, offset + len - metadata.limit());
        } else {
          ByteBuffer bb = ByteBuffer.allocate(len);
          data.serialize(bb, true);
          ioEngine.write(bb, offset);
        }
        succ = true;
      } finally {
        if (!succ) {
          bucketAllocator.freeBlock(offset);
        }
      }
      realCacheSize.addAndGet(len);
      return bucketEntry;
    }
  }

  /**
   * Only used in test
   * @throws InterruptedException
   */
  void stopWriterThreads() throws InterruptedException {
    for (WriterThread writerThread : writerThreads) {
      writerThread.disableWriter();
      writerThread.interrupt();
      writerThread.join();
    }
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    // Don't bother with ramcache since stuff is in here only a little while.
    final Iterator<Map.Entry<BlockCacheKey, BucketEntry>> i =
        this.backingMap.entrySet().iterator();
    return new Iterator<CachedBlock>() {
      private final long now = System.nanoTime();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public CachedBlock next() {
        final Map.Entry<BlockCacheKey, BucketEntry> e = i.next();
        return new CachedBlock() {
          @Override
          public String toString() {
            return BlockCacheUtil.toString(this, now);
          }

          @Override
          public BlockPriority getBlockPriority() {
            return e.getValue().getPriority();
          }

          @Override
          public BlockType getBlockType() {
            // Not held by BucketEntry.  Could add it if wanted on BucketEntry creation.
            return null;
          }

          @Override
          public long getOffset() {
            return e.getKey().getOffset();
          }

          @Override
          public long getSize() {
            return e.getValue().getLength();
          }

          @Override
          public long getCachedTime() {
            return e.getValue().getCachedTime();
          }

          @Override
          public String getFilename() {
            return e.getKey().getHfileName();
          }

          @Override
          public int compareTo(CachedBlock other) {
            int diff = this.getFilename().compareTo(other.getFilename());
            if (diff != 0) return diff;

            diff = Long.compare(this.getOffset(), other.getOffset());
            if (diff != 0) return diff;
            if (other.getCachedTime() < 0 || this.getCachedTime() < 0) {
              throw new IllegalStateException("" + this.getCachedTime() + ", " +
                other.getCachedTime());
            }
            return Long.compare(other.getCachedTime(), this.getCachedTime());
          }

          @Override
          public int hashCode() {
            return e.getKey().hashCode();
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

  @Override
  public BlockCache[] getBlockCaches() {
    return null;
  }

  float getAcceptableFactor() {
    return acceptableFactor;
  }

  float getMinFactor() {
    return minFactor;
  }

  float getExtraFreeFactor() {
    return extraFreeFactor;
  }

  float getSingleFactor() {
    return singleFactor;
  }

  float getMultiFactor() {
    return multiFactor;
  }

  float getMemoryFactor() {
    return memoryFactor;
  }

  @VisibleForTesting
  public UniqueIndexMap<Integer> getDeserialiserMap() {
    return deserialiserMap;
  }
}
