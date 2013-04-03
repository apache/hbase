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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializerIdManager;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.util.StringUtils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * BucketCache uses {@link BucketAllocator} to allocate/free block, and use
 * {@link BucketCache#ramCache} and {@link BucketCache#backingMap} in order to
 * determine whether a given element hit. It could uses memory
 * {@link ByteBufferIOEngine} or file {@link FileIOEngine}to store/read the
 * block data.
 * 
 * Eviction is using similar algorithm as
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache}
 * 
 * BucketCache could be used as mainly a block cache(see
 * {@link CombinedBlockCache}), combined with LruBlockCache to decrease CMS and
 * fragment by GC.
 * 
 * Also could be used as a secondary cache(e.g. using Fusionio to store block)
 * to enlarge cache space by
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache#setVictimCache}
 */
@InterfaceAudience.Private
public class BucketCache implements BlockCache, HeapSize {
  static final Log LOG = LogFactory.getLog(BucketCache.class);

  /** Priority buckets */
  private static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  private static final float DEFAULT_MULTI_FACTOR = 0.50f;
  private static final float DEFAULT_MEMORY_FACTOR = 0.25f;
  private static final float DEFAULT_EXTRA_FREE_FACTOR = 0.10f;

  private static final float DEFAULT_ACCEPT_FACTOR = 0.95f;
  private static final float DEFAULT_MIN_FACTOR = 0.85f;

  /** Statistics thread */
  private static final int statThreadPeriod = 3 * 60;

  final static int DEFAULT_WRITER_THREADS = 3;
  final static int DEFAULT_WRITER_QUEUE_ITEMS = 64;

  // Store/read block data
  IOEngine ioEngine;

  // Store the block in this map before writing it to cache
  private ConcurrentHashMap<BlockCacheKey, RAMQueueEntry> ramCache;
  // In this map, store the block's meta data like offset, length
  private ConcurrentHashMap<BlockCacheKey, BucketEntry> backingMap;

  /**
   * Flag if the cache is enabled or not... We shut it off if there are IO
   * errors for some time, so that Bucket IO exceptions/errors don't bring down
   * the HBase server.
   */
  private volatile boolean cacheEnabled;

  private ArrayList<BlockingQueue<RAMQueueEntry>> writerQueues = 
      new ArrayList<BlockingQueue<RAMQueueEntry>>();
  WriterThread writerThreads[];



  /** Volatile boolean to track if free space is in process or not */
  private volatile boolean freeInProgress = false;
  private Lock freeSpaceLock = new ReentrantLock();

  private UniqueIndexMap<Integer> deserialiserMap = new UniqueIndexMap<Integer>();

  private final AtomicLong realCacheSize = new AtomicLong(0);
  private final AtomicLong heapSize = new AtomicLong(0);
  /** Current number of cached elements */
  private final AtomicLong blockNumber = new AtomicLong(0);
  private final AtomicLong failedBlockAdditions = new AtomicLong(0);

  /** Cache access count (sequential ID) */
  private final AtomicLong accessCount = new AtomicLong(0);

  private final Object[] cacheWaitSignals;
  private static final int DEFAULT_CACHE_WAIT_TIME = 50;
  // Used in test now. If the flag is false and the cache speed is very fast,
  // bucket cache will skip some blocks when caching. If the flag is true, we
  // will wait blocks flushed to IOEngine for some time when caching
  boolean wait_when_cache = false;

  private BucketCacheStats cacheStats = new BucketCacheStats();

  private String persistencePath;
  private long cacheCapacity;
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
   * A "sparse lock" implementation allowing to lock on a particular block
   * identified by offset. The purpose of this is to avoid freeing the block
   * which is being read.
   * 
   * TODO:We could extend the IdLock to IdReadWriteLock for better.
   */
  private IdLock offsetLock = new IdLock();


  
  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder()
        .setNameFormat("BucketCache Statistics #%d")
        .setDaemon(true)
        .build());

  // Allocate or free space for the block
  private BucketAllocator bucketAllocator;
  
  public BucketCache(String ioEngineName, long capacity, int writerThreadNum,
      int writerQLen, String persistencePath) throws FileNotFoundException,
      IOException {
    this(ioEngineName, capacity, writerThreadNum, writerQLen, persistencePath,
        DEFAULT_ERROR_TOLERATION_DURATION);
  }
  
  public BucketCache(String ioEngineName, long capacity, int writerThreadNum,
      int writerQLen, String persistencePath, int ioErrorsTolerationDuration)
      throws FileNotFoundException, IOException {
    this.ioEngine = getIOEngineFromName(ioEngineName, capacity);
    this.writerThreads = new WriterThread[writerThreadNum];
    this.cacheWaitSignals = new Object[writerThreadNum];
    long blockNumCapacity = capacity / 16384;
    if (blockNumCapacity >= Integer.MAX_VALUE) {
      // Enough for about 32TB of cache!
      throw new IllegalArgumentException("Cache capacity is too large, only support 32TB now");
    }

    this.cacheCapacity = capacity;
    this.persistencePath = persistencePath;
    this.blockSize = StoreFile.DEFAULT_BLOCKSIZE_SMALL;
    this.ioErrorsTolerationDuration = ioErrorsTolerationDuration;

    bucketAllocator = new BucketAllocator(capacity);
    for (int i = 0; i < writerThreads.length; ++i) {
      writerQueues.add(new ArrayBlockingQueue<RAMQueueEntry>(writerQLen));
      this.cacheWaitSignals[i] = new Object();
    }

    assert writerQueues.size() == writerThreads.length;
    this.ramCache = new ConcurrentHashMap<BlockCacheKey, RAMQueueEntry>();

    this.backingMap = new ConcurrentHashMap<BlockCacheKey, BucketEntry>((int) blockNumCapacity);

    if (ioEngine.isPersistent() && persistencePath != null) {
      try {
        retrieveFromFile();
      } catch (IOException ioex) {
        LOG.error("Can't restore from file because of", ioex);
      } catch (ClassNotFoundException cnfe) {
        LOG.error("Can't restore from file in rebuild because can't deserialise",cnfe);
        throw new RuntimeException(cnfe);
      }
    }
    final String threadName = Thread.currentThread().getName();
    this.cacheEnabled = true;
    for (int i = 0; i < writerThreads.length; ++i) {
      writerThreads[i] = new WriterThread(writerQueues.get(i), i);
      writerThreads[i].setName(threadName + "-BucketCacheWriter-" + i);
      writerThreads[i].start();
    }
    // Run the statistics thread periodically to print the cache statistics log
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        statThreadPeriod, statThreadPeriod, TimeUnit.SECONDS);
    LOG.info("Started bucket cache");
  }

  /**
   * Get the IOEngine from the IO engine name
   * @param ioEngineName
   * @param capacity
   * @return
   * @throws IOException
   */
  private IOEngine getIOEngineFromName(String ioEngineName, long capacity)
      throws IOException {
    if (ioEngineName.startsWith("file:"))
      return new FileIOEngine(ioEngineName.substring(5), capacity);
    else if (ioEngineName.startsWith("offheap"))
      return new ByteBufferIOEngine(capacity, true);
    else if (ioEngineName.startsWith("heap"))
      return new ByteBufferIOEngine(capacity, false);
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
    cacheBlock(cacheKey, buf, false);
  }

  /**
   * Cache the block with the specified name and buffer.
   * @param cacheKey block's cache key
   * @param cachedItem block buffer
   * @param inMemory if block is in-memory
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable cachedItem, boolean inMemory) {
    cacheBlockWithWait(cacheKey, cachedItem, inMemory, wait_when_cache);
  }

  /**
   * Cache the block to ramCache
   * @param cacheKey block's cache key
   * @param cachedItem block buffer
   * @param inMemory if block is in-memory
   * @param wait if true, blocking wait when queue is full
   */
  public void cacheBlockWithWait(BlockCacheKey cacheKey, Cacheable cachedItem,
      boolean inMemory, boolean wait) {
    if (!cacheEnabled)
      return;

    if (backingMap.containsKey(cacheKey) || ramCache.containsKey(cacheKey))
      return;

    /*
     * Stuff the entry into the RAM cache so it can get drained to the
     * persistent store
     */
    RAMQueueEntry re = new RAMQueueEntry(cacheKey, cachedItem,
        accessCount.incrementAndGet(), inMemory);
    ramCache.put(cacheKey, re);
    int queueNum = (cacheKey.hashCode() & 0x7FFFFFFF) % writerQueues.size();
    BlockingQueue<RAMQueueEntry> bq = writerQueues.get(queueNum);
    boolean successfulAddition = bq.offer(re);
    if (!successfulAddition && wait) {
      synchronized (cacheWaitSignals[queueNum]) {
        try {
          cacheWaitSignals[queueNum].wait(DEFAULT_CACHE_WAIT_TIME);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
      successfulAddition = bq.offer(re);
    }
    if (!successfulAddition) {
        ramCache.remove(cacheKey);
        failedBlockAdditions.incrementAndGet();
    } else {
      this.blockNumber.incrementAndGet();
      this.heapSize.addAndGet(cachedItem.heapSize());
    }
  }

  /**
   * Get the buffer of the block with the specified key.
   * @param key block's cache key
   * @param caching true if the caller caches blocks on cache misses
   * @param repeat Whether this is a repeat lookup for the same block
   * @return buffer of specified cache key, or null if not in cache
   */
  @Override
  public Cacheable getBlock(BlockCacheKey key, boolean caching, boolean repeat) {
    if (!cacheEnabled)
      return null;
    RAMQueueEntry re = ramCache.get(key);
    if (re != null) {
      cacheStats.hit(caching);
      re.access(accessCount.incrementAndGet());
      return re.getData();
    }
    BucketEntry bucketEntry = backingMap.get(key);
    if(bucketEntry!=null) {
      long start = System.nanoTime();
      IdLock.Entry lockEntry = null;
      try {
        lockEntry = offsetLock.getLockEntry(bucketEntry.offset());
        if (bucketEntry.equals(backingMap.get(key))) {
          int len = bucketEntry.getLength();
          ByteBuffer bb = ByteBuffer.allocate(len);
          ioEngine.read(bb, bucketEntry.offset());
          Cacheable cachedBlock = bucketEntry.deserializerReference(
              deserialiserMap).deserialize(bb, true);
          long timeTaken = System.nanoTime() - start;
          cacheStats.hit(caching);
          cacheStats.ioHit(timeTaken);
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
        if (lockEntry != null) {
          offsetLock.releaseLockEntry(lockEntry);
        }
      }
    }
    if(!repeat)cacheStats.miss(caching);
    return null;
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    if (!cacheEnabled) return false;
    RAMQueueEntry removedBlock = ramCache.remove(cacheKey);
    if (removedBlock != null) {
      this.blockNumber.decrementAndGet();
      this.heapSize.addAndGet(-1 * removedBlock.getData().heapSize());
    }
    BucketEntry bucketEntry = backingMap.get(cacheKey);
    if (bucketEntry != null) {
      IdLock.Entry lockEntry = null;
      try {
        lockEntry = offsetLock.getLockEntry(bucketEntry.offset());
        if (bucketEntry.equals(backingMap.remove(cacheKey))) {
          bucketAllocator.freeBlock(bucketEntry.offset());
          realCacheSize.addAndGet(-1 * bucketEntry.getLength());
          if (removedBlock == null) {
            this.blockNumber.decrementAndGet();
          }
        } else {
          return false;
        }
      } catch (IOException ie) {
        LOG.warn("Failed evicting block " + cacheKey);
        return false;
      } finally {
        if (lockEntry != null) {
          offsetLock.releaseLockEntry(lockEntry);
        }
      }
    }
    cacheStats.evicted();
    return true;
  }
  
  /*
   * Statistics thread.  Periodically prints the cache statistics to the log.
   */
  private static class StatisticsThread extends Thread {
    BucketCache bucketCache;

    public StatisticsThread(BucketCache bucketCache) {
      super("BucketCache.StatisticsThread");
      setDaemon(true);
      this.bucketCache = bucketCache;
    }
    @Override
    public void run() {
      bucketCache.logStats();
    }
  }
  
  public void logStats() {
    if (!LOG.isDebugEnabled()) return;
    // Log size
    long totalSize = bucketAllocator.getTotalSize();
    long usedSize = bucketAllocator.getUsedSize();
    long freeSize = totalSize - usedSize;
    long cacheSize = this.realCacheSize.get();
    LOG.debug("BucketCache Stats: " +
        "failedBlockAdditions=" + this.failedBlockAdditions.get() + ", " +
        "total=" + StringUtils.byteDesc(totalSize) + ", " +
        "free=" + StringUtils.byteDesc(freeSize) + ", " +
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

  private long acceptableSize() {
    return (long) Math.floor(bucketAllocator.getTotalSize() * DEFAULT_ACCEPT_FACTOR);
  }

  private long minSize() {
    return (long) Math.floor(bucketAllocator.getTotalSize() * DEFAULT_MIN_FACTOR);
  }

  private long singleSize() {
    return (long) Math.floor(bucketAllocator.getTotalSize()
        * DEFAULT_SINGLE_FACTOR * DEFAULT_MIN_FACTOR);
  }

  private long multiSize() {
    return (long) Math.floor(bucketAllocator.getTotalSize() * DEFAULT_MULTI_FACTOR
        * DEFAULT_MIN_FACTOR);
  }

  private long memorySize() {
    return (long) Math.floor(bucketAllocator.getTotalSize() * DEFAULT_MEMORY_FACTOR
        * DEFAULT_MIN_FACTOR);
  }

  /**
   * Free the space if the used size reaches acceptableSize() or one size block
   * couldn't be allocated. When freeing the space, we use the LRU algorithm and
   * ensure there must be some blocks evicted
   */
  private void freeSpace() {
    // Ensure only one freeSpace progress at a time
    if (!freeSpaceLock.tryLock()) return;
    try {
      freeInProgress = true;
      long bytesToFreeWithoutExtra = 0;
      /*
       * Calculate free byte for each bucketSizeinfo
       */
      StringBuffer msgBuffer = new StringBuffer();
      BucketAllocator.IndexStatistics[] stats = bucketAllocator.getIndexStatistics();
      long[] bytesToFreeForBucket = new long[stats.length];
      for (int i = 0; i < stats.length; i++) {
        bytesToFreeForBucket[i] = 0;
        long freeGoal = (long) Math.floor(stats[i].totalCount()
            * (1 - DEFAULT_MIN_FACTOR));
        freeGoal = Math.max(freeGoal, 1);
        if (stats[i].freeCount() < freeGoal) {
          bytesToFreeForBucket[i] = stats[i].itemSize()
          * (freeGoal - stats[i].freeCount());
          bytesToFreeWithoutExtra += bytesToFreeForBucket[i];
          msgBuffer.append("Free for bucketSize(" + stats[i].itemSize() + ")="
              + StringUtils.byteDesc(bytesToFreeForBucket[i]) + ", ");
        }
      }
      msgBuffer.append("Free for total="
          + StringUtils.byteDesc(bytesToFreeWithoutExtra) + ", ");

      if (bytesToFreeWithoutExtra <= 0) {
        return;
      }
      long currentSize = bucketAllocator.getUsedSize();
      long totalSize=bucketAllocator.getTotalSize();
      LOG.debug("Bucket cache free space started; Attempting to  " + msgBuffer.toString()
          + " of current used=" + StringUtils.byteDesc(currentSize)
          + ",actual cacheSize=" + StringUtils.byteDesc(realCacheSize.get())
          + ",total=" + StringUtils.byteDesc(totalSize));
      
      long bytesToFreeWithExtra = (long) Math.floor(bytesToFreeWithoutExtra
          * (1 + DEFAULT_EXTRA_FREE_FACTOR));

      // Instantiate priority buckets
      BucketEntryGroup bucketSingle = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, singleSize());
      BucketEntryGroup bucketMulti = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, multiSize());
      BucketEntryGroup bucketMemory = new BucketEntryGroup(bytesToFreeWithExtra,
          blockSize, memorySize());

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

      int remainingBuckets = 3;
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

      /**
       * Check whether need extra free because some bucketSizeinfo still needs
       * free space
       */
      stats = bucketAllocator.getIndexStatistics();
      boolean needFreeForExtra = false;
      for (int i = 0; i < stats.length; i++) {
        long freeGoal = (long) Math.floor(stats[i].totalCount()
            * (1 - DEFAULT_MIN_FACTOR));
        freeGoal = Math.max(freeGoal, 1);
        if (stats[i].freeCount() < freeGoal) {
          needFreeForExtra = true;
          break;
        }
      }

      if (needFreeForExtra) {
        bucketQueue.clear();
        remainingBuckets = 2;

        bucketQueue.add(bucketSingle);
        bucketQueue.add(bucketMulti);

        while ((bucketGroup = bucketQueue.poll()) != null) {
          long bucketBytesToFree = (bytesToFreeWithExtra - bytesFreed)
              / remainingBuckets;
          bytesFreed += bucketGroup.free(bucketBytesToFree);
          remainingBuckets--;
        }
      }

      if (LOG.isDebugEnabled()) {
        long single = bucketSingle.totalSize();
        long multi = bucketMulti.totalSize();
        long memory = bucketMemory.totalSize();
        LOG.debug("Bucket cache free space completed; " + "freed="
            + StringUtils.byteDesc(bytesFreed) + ", " + "total="
            + StringUtils.byteDesc(totalSize) + ", " + "single="
            + StringUtils.byteDesc(single) + ", " + "multi="
            + StringUtils.byteDesc(multi) + ", " + "memory="
            + StringUtils.byteDesc(memory));
      }

    } finally {
      cacheStats.evict();
      freeInProgress = false;
      freeSpaceLock.unlock();
    }
  }

  // This handles flushing the RAM cache to IOEngine.
  private class WriterThread extends HasThread {
    BlockingQueue<RAMQueueEntry> inputQueue;
    final int threadNO;
    boolean writerEnabled = true;

    WriterThread(BlockingQueue<RAMQueueEntry> queue, int threadNO) {
      super();
      this.inputQueue = queue;
      this.threadNO = threadNO;
      setDaemon(true);
    }
    
    // Used for test
    void disableWriter() {
      this.writerEnabled = false;
    }

    public void run() {
      List<RAMQueueEntry> entries = new ArrayList<RAMQueueEntry>();
      try {
        while (cacheEnabled && writerEnabled) {
          try {
            // Blocks
            entries.add(inputQueue.take());
            inputQueue.drainTo(entries);
            synchronized (cacheWaitSignals[threadNO]) {
              cacheWaitSignals[threadNO].notifyAll();
            }
          } catch (InterruptedException ie) {
            if (!cacheEnabled) break;
          }
          doDrain(entries);
        }
      } catch (Throwable t) {
        LOG.warn("Failed doing drain", t);
      }
      LOG.info(this.getName() + " exiting, cacheEnabled=" + cacheEnabled);
    }

    /**
     * Flush the entries in ramCache to IOEngine and add bucket entry to
     * backingMap
     * @param entries
     * @throws InterruptedException
     */
    private void doDrain(List<RAMQueueEntry> entries)
        throws InterruptedException {
      BucketEntry[] bucketEntries = new BucketEntry[entries.size()];
      RAMQueueEntry[] ramEntries = new RAMQueueEntry[entries.size()];
      int done = 0;
      while (entries.size() > 0 && cacheEnabled) {
        // Keep going in case we throw...
        RAMQueueEntry ramEntry = null;
        try {
          ramEntry = entries.remove(entries.size() - 1);
          if (ramEntry == null) {
            LOG.warn("Couldn't get the entry from RAM queue, who steals it?");
            continue;
          }
          BucketEntry bucketEntry = ramEntry.writeToCache(ioEngine,
              bucketAllocator, deserialiserMap, realCacheSize);
          ramEntries[done] = ramEntry;
          bucketEntries[done++] = bucketEntry;
          if (ioErrorStartTime > 0) {
            ioErrorStartTime = -1;
          }
        } catch (BucketAllocatorException fle) {
          LOG.warn("Failed allocating for block "
              + (ramEntry == null ? "" : ramEntry.getKey()), fle);
        } catch (CacheFullException cfe) {
          if (!freeInProgress) {
            freeSpace();
          } else {
            Thread.sleep(50);
          }
        } catch (IOException ioex) {
          LOG.error("Failed writing to bucket cache", ioex);
          checkIOErrorIsTolerated();
        }
      }

      // Make sure that the data pages we have written are on the media before
      // we update the map.
      try {
        ioEngine.sync();
      } catch (IOException ioex) {
        LOG.error("Faild syncing IO engine", ioex);
        checkIOErrorIsTolerated();
        // Since we failed sync, free the blocks in bucket allocator
        for (int i = 0; i < done; ++i) {
          if (bucketEntries[i] != null) {
            bucketAllocator.freeBlock(bucketEntries[i].offset());
          }
        }
        done = 0;
      }

      for (int i = 0; i < done; ++i) {
        if (bucketEntries[i] != null) {
          backingMap.put(ramEntries[i].getKey(), bucketEntries[i]);
        }
        RAMQueueEntry ramCacheEntry = ramCache.remove(ramEntries[i].getKey());
        if (ramCacheEntry != null) {
          heapSize.addAndGet(-1 * ramEntries[i].getData().heapSize());
        }
      }

      if (bucketAllocator.getUsedSize() > acceptableSize()) {
        freeSpace();
      }
    }
  }

  

  private void persistToFile() throws IOException {
    assert !cacheEnabled;
    FileOutputStream fos = null;
    ObjectOutputStream oos = null;
    try {
      if (!ioEngine.isPersistent())
        throw new IOException(
            "Attempt to persist non-persistent cache mappings!");
      fos = new FileOutputStream(persistencePath, false);
      oos = new ObjectOutputStream(fos);
      oos.writeLong(cacheCapacity);
      oos.writeUTF(ioEngine.getClass().getName());
      oos.writeUTF(backingMap.getClass().getName());
      oos.writeObject(deserialiserMap);
      oos.writeObject(backingMap);
    } finally {
      if (oos != null) oos.close();
      if (fos != null) fos.close();
    }
  }

  @SuppressWarnings("unchecked")
  private void retrieveFromFile() throws IOException, BucketAllocatorException,
      ClassNotFoundException {
    File persistenceFile = new File(persistencePath);
    if (!persistenceFile.exists()) {
      return;
    }
    assert !cacheEnabled;
    FileInputStream fis = null;
    ObjectInputStream ois = null;
    try {
      if (!ioEngine.isPersistent())
        throw new IOException(
            "Attempt to restore non-persistent cache mappings!");
      fis = new FileInputStream(persistencePath);
      ois = new ObjectInputStream(fis);
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
      BucketAllocator allocator = new BucketAllocator(cacheCapacity,
          backingMap, this.realCacheSize);
      backingMap = (ConcurrentHashMap<BlockCacheKey, BucketEntry>) ois
          .readObject();
      bucketAllocator = allocator;
      deserialiserMap = deserMap;
    } finally {
      if (ois != null) ois.close();
      if (fis != null) fis.close();
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
    long now = EnvironmentEdgeManager.currentTimeMillis();
    if (this.ioErrorStartTime > 0) {
      if (cacheEnabled
          && (now - ioErrorStartTime) > this.ioErrorsTolerationDuration) {
        LOG.error("IO errors duration time has exceeded "
            + ioErrorsTolerationDuration
            + "ms, disabing cache, please check your IOEngine");
        disableCache();
      }
    } else {
      this.ioErrorStartTime = now;
    }
  }

  /**
   * Used to shut down the cache -or- turn it off in the case of something
   * broken.
   */
  private void disableCache() {
    if (!cacheEnabled)
      return;
    cacheEnabled = false;
    ioEngine.shutdown();
    this.scheduleThreadPool.shutdown();
    for (int i = 0; i < writerThreads.length; ++i)
      writerThreads[i].interrupt();
    this.ramCache.clear();
    if (!ioEngine.isPersistent() || persistencePath == null) {
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

  BucketAllocator getAllocator() {
    return this.bucketAllocator;
  }

  public long heapSize() {
    return this.heapSize.get();
  }

  /**
   * Returns the total size of the block cache, in bytes.
   * @return size of cache, in bytes
   */
  @Override
  public long size() {
    return this.realCacheSize.get();
  }

  @Override
  public long getFreeSize() {
    return this.bucketAllocator.getFreeSize();
  }

  @Override
  public long getBlockCount() {
    return this.blockNumber.get();
  }

  /**
   * Returns the occupied size of the block cache, in bytes.
   * @return occupied space in cache, in bytes
   */
  @Override
  public long getCurrentSize() {
    return this.bucketAllocator.getUsedSize();
  }

  @Override
  public long getEvictedCount() {
    return cacheStats.getEvictedCount();
  }

  /**
   * Evicts all blocks for a specific HFile. This is an expensive operation
   * implemented as a linear-time search through all blocks in the cache.
   * Ideally this should be a search in a log-access-time map.
   * 
   * <p>
   * This is used for evict-on-close to remove all blocks of a specific HFile.
   * 
   * @return the number of blocks evicted
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    int numEvicted = 0;
    for (BlockCacheKey key : this.backingMap.keySet()) {
      if (key.getHfileName().equals(hfileName)) {
        if (evictBlock(key))
          ++numEvicted;
      }
    }
    return numEvicted;
  }


  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(
      Configuration conf) {
    throw new UnsupportedOperationException();
  }

  static enum BlockPriority {
    /**
     * Accessed a single time (used for scan-resistance)
     */
    SINGLE,
    /**
     * Accessed multiple times
     */
    MULTI,
    /**
     * Block from in-memory store
     */
    MEMORY
  };

  /**
   * Item in cache. We expect this to be where most memory goes. Java uses 8
   * bytes just for object headers; after this, we want to use as little as
   * possible - so we only use 8 bytes, but in order to do so we end up messing
   * around with all this Java casting stuff. Offset stored as 5 bytes that make
   * up the long. Doubt we'll see devices this big for ages. Offsets are divided
   * by 256. So 5 bytes gives us 256TB or so.
   */
  static class BucketEntry implements Serializable, Comparable<BucketEntry> {
    private static final long serialVersionUID = -6741504807982257534L;
    private int offsetBase;
    private int length;
    private byte offset1;
    byte deserialiserIndex;
    private volatile long accessTime;
    private BlockPriority priority;

    BucketEntry(long offset, int length, long accessTime, boolean inMemory) {
      setOffset(offset);
      this.length = length;
      this.accessTime = accessTime;
      if (inMemory) {
        this.priority = BlockPriority.MEMORY;
      } else {
        this.priority = BlockPriority.SINGLE;
      }
    }

    long offset() { // Java has no unsigned numbers
      long o = ((long) offsetBase) & 0xFFFFFFFF;
      o += (((long) (offset1)) & 0xFF) << 32;
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
     * Block has been accessed. Update its local access time.
     */
    public void access(long accessTime) {
      this.accessTime = accessTime;
      if (this.priority == BlockPriority.SINGLE) {
        this.priority = BlockPriority.MULTI;
      }
    }
    
    public BlockPriority getPriority() {
      return this.priority;
    }

    @Override
    public int compareTo(BucketEntry that) {
      if(this.accessTime == that.accessTime) return 0;
      return this.accessTime < that.accessTime ? 1 : -1;
    }

    @Override
    public boolean equals(Object that) {
      return this == that;
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
      if (this.overflow() == that.overflow())
        return 0;
      return this.overflow() > that.overflow() ? 1 : -1;
    }

    @Override
    public boolean equals(Object that) {
      return this == that;
    }

  }

  /**
   * Block Entry stored in the memory with key,data and so on
   */
  private static class RAMQueueEntry {
    private BlockCacheKey key;
    private Cacheable data;
    private long accessTime;
    private boolean inMemory;

    public RAMQueueEntry(BlockCacheKey bck, Cacheable data, long accessTime,
        boolean inMemory) {
      this.key = bck;
      this.data = data;
      this.accessTime = accessTime;
      this.inMemory = inMemory;
    }

    public Cacheable getData() {
      return data;
    }

    public BlockCacheKey getKey() {
      return key;
    }

    public void access(long accessTime) {
      this.accessTime = accessTime;
    }

    public BucketEntry writeToCache(final IOEngine ioEngine,
        final BucketAllocator bucketAllocator,
        final UniqueIndexMap<Integer> deserialiserMap,
        final AtomicLong realCacheSize) throws CacheFullException, IOException,
        BucketAllocatorException {
      int len = data.getSerializedLength();
      // This cacheable thing can't be serialized...
      if (len == 0) return null;
      long offset = bucketAllocator.allocateBlock(len);
      BucketEntry bucketEntry = new BucketEntry(offset, len, accessTime,
          inMemory);
      bucketEntry.setDeserialiserReference(data.getDeserializer(), deserialiserMap);
      try {
        if (data instanceof HFileBlock) {
          ByteBuffer sliceBuf = ((HFileBlock) data).getBufferReadOnlyWithHeader();
          sliceBuf.rewind();
          assert len == sliceBuf.limit() + HFileBlock.EXTRA_SERIALIZATION_SPACE;
          ByteBuffer extraInfoBuffer = ByteBuffer.allocate(HFileBlock.EXTRA_SERIALIZATION_SPACE);
          ((HFileBlock) data).serializeExtraInfo(extraInfoBuffer);
          ioEngine.write(sliceBuf, offset);
          ioEngine.write(extraInfoBuffer, offset + len - HFileBlock.EXTRA_SERIALIZATION_SPACE);
        } else {
          ByteBuffer bb = ByteBuffer.allocate(len);
          data.serialize(bb);
          ioEngine.write(bb, offset);
        }
      } catch (IOException ioe) {
        // free it in bucket allocator
        bucketAllocator.freeBlock(offset);
        throw ioe;
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

}
