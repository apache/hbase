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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;

import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.ResizableBlockCache;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages tuning of Heap memory using <code>HeapMemoryTuner</code>. Most part of the heap memory is
 * split between Memstores and BlockCache. This manager helps in tuning sizes of both these
 * dynamically, as per the R/W load on the servers.
 */
@InterfaceAudience.Private
public class HeapMemoryManager {
  private static final Logger LOG = LoggerFactory.getLogger(HeapMemoryManager.class);
  private static final int CONVERT_TO_PERCENTAGE = 100;
  private static final int CLUSTER_MINIMUM_MEMORY_THRESHOLD =
    (int) (CONVERT_TO_PERCENTAGE * HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD);

  public static final String BLOCK_CACHE_SIZE_MAX_RANGE_KEY = "hfile.block.cache.size.max.range";
  public static final String BLOCK_CACHE_SIZE_MIN_RANGE_KEY = "hfile.block.cache.size.min.range";
  public static final String MEMSTORE_SIZE_MAX_RANGE_KEY =
      "hbase.regionserver.global.memstore.size.max.range";
  public static final String MEMSTORE_SIZE_MIN_RANGE_KEY =
      "hbase.regionserver.global.memstore.size.min.range";
  public static final String HBASE_RS_HEAP_MEMORY_TUNER_PERIOD =
      "hbase.regionserver.heapmemory.tuner.period";
  public static final int HBASE_RS_HEAP_MEMORY_TUNER_DEFAULT_PERIOD = 60 * 1000;
  public static final String HBASE_RS_HEAP_MEMORY_TUNER_CLASS =
      "hbase.regionserver.heapmemory.tuner.class";

  public static final float HEAP_OCCUPANCY_ERROR_VALUE = -0.0f;

  private float globalMemStorePercent;
  private float globalMemStorePercentMinRange;
  private float globalMemStorePercentMaxRange;

  private float blockCachePercent;
  private float blockCachePercentMinRange;
  private float blockCachePercentMaxRange;

  private float heapOccupancyPercent;

  private final ResizableBlockCache blockCache;
  // TODO : remove this and mark regionServerAccounting as the observer directly
  private final FlushRequester memStoreFlusher;
  private final Server server;
  private final RegionServerAccounting regionServerAccounting;

  private HeapMemoryTunerChore heapMemTunerChore = null;
  private final boolean tunerOn;
  private final int defaultChorePeriod;
  private final float heapOccupancyLowWatermark;

  private final long maxHeapSize;
  {
    // note that this initialization still isn't threadsafe, because updating a long isn't atomic.
    long tempMaxHeap = -1L;
    try {
      final MemoryUsage usage = MemorySizeUtil.safeGetHeapMemoryUsage();
      if (usage != null) {
        tempMaxHeap = usage.getMax();
      }
    } finally {
      maxHeapSize = tempMaxHeap;
    }
  }

  private MetricsHeapMemoryManager metricsHeapMemoryManager;

  private List<HeapMemoryTuneObserver> tuneObservers = new ArrayList<>();

  HeapMemoryManager(BlockCache blockCache, FlushRequester memStoreFlusher,
                Server server, RegionServerAccounting regionServerAccounting) {
    Configuration conf = server.getConfiguration();
    this.blockCache = toResizableBlockCache(blockCache);
    this.memStoreFlusher = memStoreFlusher;
    this.server = server;
    this.regionServerAccounting = regionServerAccounting;
    this.tunerOn = doInit(conf);
    this.defaultChorePeriod = conf.getInt(HBASE_RS_HEAP_MEMORY_TUNER_PERIOD,
      HBASE_RS_HEAP_MEMORY_TUNER_DEFAULT_PERIOD);
    this.heapOccupancyLowWatermark = conf.getFloat(HConstants.HEAP_OCCUPANCY_LOW_WATERMARK_KEY,
      HConstants.DEFAULT_HEAP_OCCUPANCY_LOW_WATERMARK);
    metricsHeapMemoryManager = new MetricsHeapMemoryManager();
  }

  private ResizableBlockCache toResizableBlockCache(BlockCache blockCache) {
    if (blockCache instanceof CombinedBlockCache) {
      return (ResizableBlockCache) ((CombinedBlockCache) blockCache).getFirstLevelCache();
    } else {
      return (ResizableBlockCache) blockCache;
    }
  }

  private boolean doInit(Configuration conf) {
    boolean tuningEnabled = true;
    globalMemStorePercent = MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false);
    blockCachePercent = conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY,
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    MemorySizeUtil.checkForClusterFreeHeapMemoryLimit(conf);
    // Initialize max and min range for memstore heap space
    globalMemStorePercentMinRange = conf.getFloat(MEMSTORE_SIZE_MIN_RANGE_KEY,
        globalMemStorePercent);
    globalMemStorePercentMaxRange = conf.getFloat(MEMSTORE_SIZE_MAX_RANGE_KEY,
        globalMemStorePercent);
    if (globalMemStorePercent < globalMemStorePercentMinRange) {
      LOG.warn("Setting " + MEMSTORE_SIZE_MIN_RANGE_KEY + " to " + globalMemStorePercent
          + ", same value as " + MemorySizeUtil.MEMSTORE_SIZE_KEY
          + " because supplied value greater than initial memstore size value.");
      globalMemStorePercentMinRange = globalMemStorePercent;
      conf.setFloat(MEMSTORE_SIZE_MIN_RANGE_KEY, globalMemStorePercentMinRange);
    }
    if (globalMemStorePercent > globalMemStorePercentMaxRange) {
      LOG.warn("Setting " + MEMSTORE_SIZE_MAX_RANGE_KEY + " to " + globalMemStorePercent
          + ", same value as " + MemorySizeUtil.MEMSTORE_SIZE_KEY
          + " because supplied value less than initial memstore size value.");
      globalMemStorePercentMaxRange = globalMemStorePercent;
      conf.setFloat(MEMSTORE_SIZE_MAX_RANGE_KEY, globalMemStorePercentMaxRange);
    }
    if (globalMemStorePercent == globalMemStorePercentMinRange
        && globalMemStorePercent == globalMemStorePercentMaxRange) {
      tuningEnabled = false;
    }
    // Initialize max and min range for block cache
    blockCachePercentMinRange = conf.getFloat(BLOCK_CACHE_SIZE_MIN_RANGE_KEY, blockCachePercent);
    blockCachePercentMaxRange = conf.getFloat(BLOCK_CACHE_SIZE_MAX_RANGE_KEY, blockCachePercent);
    if (blockCachePercent < blockCachePercentMinRange) {
      LOG.warn("Setting " + BLOCK_CACHE_SIZE_MIN_RANGE_KEY + " to " + blockCachePercent
          + ", same value as " + HFILE_BLOCK_CACHE_SIZE_KEY
          + " because supplied value greater than initial block cache size.");
      blockCachePercentMinRange = blockCachePercent;
      conf.setFloat(BLOCK_CACHE_SIZE_MIN_RANGE_KEY, blockCachePercentMinRange);
    }
    if (blockCachePercent > blockCachePercentMaxRange) {
      LOG.warn("Setting " + BLOCK_CACHE_SIZE_MAX_RANGE_KEY + " to " + blockCachePercent
          + ", same value as " + HFILE_BLOCK_CACHE_SIZE_KEY
          + " because supplied value less than initial block cache size.");
      blockCachePercentMaxRange = blockCachePercent;
      conf.setFloat(BLOCK_CACHE_SIZE_MAX_RANGE_KEY, blockCachePercentMaxRange);
    }
    if (tuningEnabled && blockCachePercent == blockCachePercentMinRange
        && blockCachePercent == blockCachePercentMaxRange) {
      tuningEnabled = false;
    }

    int gml = (int) (globalMemStorePercentMaxRange * CONVERT_TO_PERCENTAGE);
    int bcul = (int) ((blockCachePercentMinRange) * CONVERT_TO_PERCENTAGE);
    if (CONVERT_TO_PERCENTAGE - (gml + bcul) < CLUSTER_MINIMUM_MEMORY_THRESHOLD) {
      throw new RuntimeException("Current heap configuration for MemStore and BlockCache exceeds "
          + "the threshold required for successful cluster operation. "
          + "The combined value cannot exceed 0.8. Please check the settings for "
          + MEMSTORE_SIZE_MAX_RANGE_KEY + " and " + BLOCK_CACHE_SIZE_MIN_RANGE_KEY
          + " in your configuration. " + MEMSTORE_SIZE_MAX_RANGE_KEY + " is "
          + globalMemStorePercentMaxRange + " and " + BLOCK_CACHE_SIZE_MIN_RANGE_KEY + " is "
          + blockCachePercentMinRange);
    }
    gml = (int) (globalMemStorePercentMinRange * CONVERT_TO_PERCENTAGE);
    bcul = (int) ((blockCachePercentMaxRange) * CONVERT_TO_PERCENTAGE);
    if (CONVERT_TO_PERCENTAGE - (gml + bcul) < CLUSTER_MINIMUM_MEMORY_THRESHOLD) {
      throw new RuntimeException("Current heap configuration for MemStore and BlockCache exceeds "
          + "the threshold required for successful cluster operation. "
          + "The combined value cannot exceed 0.8. Please check the settings for "
          + MEMSTORE_SIZE_MIN_RANGE_KEY + " and " + BLOCK_CACHE_SIZE_MAX_RANGE_KEY
          + " in your configuration. " + MEMSTORE_SIZE_MIN_RANGE_KEY + " is "
          + globalMemStorePercentMinRange + " and " + BLOCK_CACHE_SIZE_MAX_RANGE_KEY + " is "
          + blockCachePercentMaxRange);
    }
    return tuningEnabled;
  }

  public void start(ChoreService service) {
    LOG.info("Starting, tuneOn={}", this.tunerOn);
    this.heapMemTunerChore = new HeapMemoryTunerChore();
    service.scheduleChore(heapMemTunerChore);
    if (tunerOn) {
      // Register HeapMemoryTuner as a memstore flush listener
      memStoreFlusher.registerFlushRequestListener(heapMemTunerChore);
    }
  }

  public void stop() {
    // The thread is Daemon. Just interrupting the ongoing process.
    LOG.info("Stopping");
    this.heapMemTunerChore.cancel(true);
  }

  public void registerTuneObserver(HeapMemoryTuneObserver observer) {
    this.tuneObservers.add(observer);
  }

  // Used by the test cases.
  boolean isTunerOn() {
    return this.tunerOn;
  }

  /**
   * @return heap occupancy percentage, 0 &lt;= n &lt;= 1. or -0.0 for error asking JVM
   */
  public float getHeapOccupancyPercent() {
    return this.heapOccupancyPercent == Float.MAX_VALUE ? HEAP_OCCUPANCY_ERROR_VALUE : this.heapOccupancyPercent;
  }

  private class HeapMemoryTunerChore extends ScheduledChore implements FlushRequestListener {
    private HeapMemoryTuner heapMemTuner;
    private AtomicLong blockedFlushCount = new AtomicLong();
    private AtomicLong unblockedFlushCount = new AtomicLong();
    private long evictCount = 0L;
    private long cacheMissCount = 0L;
    private TunerContext tunerContext = new TunerContext();
    private boolean alarming = false;

    public HeapMemoryTunerChore() {
      super(server.getServerName() + "-HeapMemoryTunerChore", server, defaultChorePeriod);
      Class<? extends HeapMemoryTuner> tunerKlass = server.getConfiguration().getClass(
          HBASE_RS_HEAP_MEMORY_TUNER_CLASS, DefaultHeapMemoryTuner.class, HeapMemoryTuner.class);
      heapMemTuner = ReflectionUtils.newInstance(tunerKlass, server.getConfiguration());
      tunerContext
          .setOffheapMemStore(regionServerAccounting.isOffheap());
    }

    @Override
    protected void chore() {
      // Sample heap occupancy
      final MemoryUsage usage = MemorySizeUtil.safeGetHeapMemoryUsage();
      if (usage != null) {
        heapOccupancyPercent = (float)usage.getUsed() / (float)usage.getCommitted();
      } else {
        // previously, an exception would have meant death for the tuning chore
        // so switch to alarming so that we similarly stop tuning until we get
        // heap usage information again.
        heapOccupancyPercent = Float.MAX_VALUE;
      }
      // If we are above the heap occupancy alarm low watermark, switch to short
      // sleeps for close monitoring. Stop autotuning, we are in a danger zone.
      if (heapOccupancyPercent >= heapOccupancyLowWatermark) {
        if (!alarming) {
          LOG.warn("heapOccupancyPercent " + heapOccupancyPercent +
            " is above heap occupancy alarm watermark (" + heapOccupancyLowWatermark + ")");
          alarming = true;
        }
        metricsHeapMemoryManager.increaseAboveHeapOccupancyLowWatermarkCounter();
        triggerNow();
        try {
          // Need to sleep ourselves since we've told the chore's sleeper
          // to skip the next sleep cycle.
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // Interrupted, propagate
          Thread.currentThread().interrupt();
        }
      } else {
        if (alarming) {
          LOG.info("heapOccupancyPercent " + heapOccupancyPercent +
            " is now below the heap occupancy alarm watermark (" +
            heapOccupancyLowWatermark + ")");
          alarming = false;
        }
      }
      // Autotune if tuning is enabled and allowed
      if (tunerOn && !alarming) {
        tune();
      }
    }

    private void tune() {
      // TODO check if we can increase the memory boundaries
      // while remaining in the limits
      long curEvictCount;
      long curCacheMisCount;
      long blockedFlushCnt;
      long unblockedFlushCnt;
      curEvictCount = blockCache.getStats().getEvictedCount();
      tunerContext.setEvictCount(curEvictCount - evictCount);
      evictCount = curEvictCount;
      curCacheMisCount = blockCache.getStats().getMissCachingCount();
      tunerContext.setCacheMissCount(curCacheMisCount-cacheMissCount);
      cacheMissCount = curCacheMisCount;
      blockedFlushCnt = blockedFlushCount.getAndSet(0);
      tunerContext.setBlockedFlushCount(blockedFlushCnt);
      metricsHeapMemoryManager.updateBlockedFlushCount(blockedFlushCnt);
      unblockedFlushCnt = unblockedFlushCount.getAndSet(0);
      tunerContext.setUnblockedFlushCount(unblockedFlushCnt);
      metricsHeapMemoryManager.updateUnblockedFlushCount(unblockedFlushCnt);
      // TODO : add support for offheap metrics
      tunerContext.setCurBlockCacheUsed((float) blockCache.getCurrentSize() / maxHeapSize);
      metricsHeapMemoryManager.setCurBlockCacheSizeGauge(blockCache.getCurrentSize());
      long globalMemstoreHeapSize = regionServerAccounting.getGlobalMemStoreHeapSize();
      tunerContext.setCurMemStoreUsed((float) globalMemstoreHeapSize / maxHeapSize);
      metricsHeapMemoryManager.setCurMemStoreSizeGauge(globalMemstoreHeapSize);
      tunerContext.setCurBlockCacheSize(blockCachePercent);
      tunerContext.setCurMemStoreSize(globalMemStorePercent);
      TunerResult result = null;
      try {
        result = this.heapMemTuner.tune(tunerContext);
      } catch (Throwable t) {
        LOG.error("Exception thrown from the HeapMemoryTuner implementation", t);
      }
      if (result != null && result.needsTuning()) {
        float memstoreSize = result.getMemStoreSize();
        float blockCacheSize = result.getBlockCacheSize();
        LOG.debug("From HeapMemoryTuner new memstoreSize: " + memstoreSize
            + ". new blockCacheSize: " + blockCacheSize);
        if (memstoreSize < globalMemStorePercentMinRange) {
          LOG.info("New memstoreSize from HeapMemoryTuner " + memstoreSize + " is below min level "
              + globalMemStorePercentMinRange + ". Resetting memstoreSize to min size");
          memstoreSize = globalMemStorePercentMinRange;
        } else if (memstoreSize > globalMemStorePercentMaxRange) {
          LOG.info("New memstoreSize from HeapMemoryTuner " + memstoreSize + " is above max level "
              + globalMemStorePercentMaxRange + ". Resetting memstoreSize to max size");
          memstoreSize = globalMemStorePercentMaxRange;
        }
        if (blockCacheSize < blockCachePercentMinRange) {
          LOG.info("New blockCacheSize from HeapMemoryTuner " + blockCacheSize
              + " is below min level " + blockCachePercentMinRange
              + ". Resetting blockCacheSize to min size");
          blockCacheSize = blockCachePercentMinRange;
        } else if (blockCacheSize > blockCachePercentMaxRange) {
          LOG.info("New blockCacheSize from HeapMemoryTuner " + blockCacheSize
              + " is above max level " + blockCachePercentMaxRange
              + ". Resetting blockCacheSize to min size");
          blockCacheSize = blockCachePercentMaxRange;
        }
        int gml = (int) (memstoreSize * CONVERT_TO_PERCENTAGE);
        int bcul = (int) ((blockCacheSize) * CONVERT_TO_PERCENTAGE);
        if (CONVERT_TO_PERCENTAGE - (gml + bcul) < CLUSTER_MINIMUM_MEMORY_THRESHOLD) {
          LOG.info("Current heap configuration from HeapMemoryTuner exceeds "
              + "the threshold required for successful cluster operation. "
              + "The combined value cannot exceed 0.8. " + MemorySizeUtil.MEMSTORE_SIZE_KEY
              + " is " + memstoreSize + " and " + HFILE_BLOCK_CACHE_SIZE_KEY + " is "
              + blockCacheSize);
          // TODO can adjust the value so as not exceed 80%. Is that correct? may be.
        } else {
          int memStoreDeltaSize =
              (int) ((memstoreSize - globalMemStorePercent) * CONVERT_TO_PERCENTAGE);
          int blockCacheDeltaSize =
              (int) ((blockCacheSize - blockCachePercent) * CONVERT_TO_PERCENTAGE);
          metricsHeapMemoryManager.updateMemStoreDeltaSizeHistogram(memStoreDeltaSize);
          metricsHeapMemoryManager.updateBlockCacheDeltaSizeHistogram(blockCacheDeltaSize);
          long newBlockCacheSize = (long) (maxHeapSize * blockCacheSize);
          // we could have got an increase or decrease in size for the offheap memstore
          // also if the flush had happened due to heap overhead. In that case it is ok
          // to adjust the onheap memstore limit configs
          long newMemstoreSize = (long) (maxHeapSize * memstoreSize);
          LOG.info("Setting block cache heap size to " + newBlockCacheSize
              + " and memstore heap size to " + newMemstoreSize);
          blockCachePercent = blockCacheSize;
          blockCache.setMaxSize(newBlockCacheSize);
          globalMemStorePercent = memstoreSize;
          // Internally sets it to RegionServerAccounting
          // TODO : Set directly on RSAccounting??
          memStoreFlusher.setGlobalMemStoreLimit(newMemstoreSize);
          for (HeapMemoryTuneObserver observer : tuneObservers) {
            // Risky.. If this newMemstoreSize decreases we reduce the count in offheap chunk pool
            observer.onHeapMemoryTune(newMemstoreSize, newBlockCacheSize);
          }
        }
      } else {
        metricsHeapMemoryManager.increaseTunerDoNothingCounter();
        if (LOG.isDebugEnabled()) {
          LOG.debug("No changes made by HeapMemoryTuner.");
        }
      }
    }

    @Override
    public void flushRequested(FlushType type, Region region) {
      switch (type) {
      case ABOVE_ONHEAP_HIGHER_MARK:
        blockedFlushCount.incrementAndGet();
        break;
      case ABOVE_ONHEAP_LOWER_MARK:
        unblockedFlushCount.incrementAndGet();
        break;
      // Removed the counting of the offheap related flushes (after reviews). Will add later if
      // needed
      default:
        // In case of any other flush don't do any action.
        break;
      }
    }
  }

  /**
   * POJO to pass all the relevant information required to do the heap memory tuning. It holds the
   * flush counts and block cache evictions happened within the interval. Also holds the current
   * heap percentage allocated for memstore and block cache.
   */
  public static final class TunerContext {
    private long blockedFlushCount;
    private long unblockedFlushCount;
    private long evictCount;
    private long cacheMissCount;
    private float curBlockCacheUsed;
    private float curMemStoreUsed;
    private float curMemStoreSize;
    private float curBlockCacheSize;
    private boolean offheapMemstore;

    public long getBlockedFlushCount() {
      return blockedFlushCount;
    }

    public void setBlockedFlushCount(long blockedFlushCount) {
      this.blockedFlushCount = blockedFlushCount;
    }

    public long getUnblockedFlushCount() {
      return unblockedFlushCount;
    }

    public void setUnblockedFlushCount(long unblockedFlushCount) {
      this.unblockedFlushCount = unblockedFlushCount;
    }

    public long getEvictCount() {
      return evictCount;
    }

    public void setEvictCount(long evictCount) {
      this.evictCount = evictCount;
    }

    public float getCurMemStoreSize() {
      return curMemStoreSize;
    }

    public void setCurMemStoreSize(float curMemStoreSize) {
      this.curMemStoreSize = curMemStoreSize;
    }

    public float getCurBlockCacheSize() {
      return curBlockCacheSize;
    }

    public void setCurBlockCacheSize(float curBlockCacheSize) {
      this.curBlockCacheSize = curBlockCacheSize;
    }

    public long getCacheMissCount() {
      return cacheMissCount;
    }

    public void setCacheMissCount(long cacheMissCount) {
      this.cacheMissCount = cacheMissCount;
    }

    public float getCurBlockCacheUsed() {
      return curBlockCacheUsed;
    }

    public void setCurBlockCacheUsed(float curBlockCacheUsed) {
      this.curBlockCacheUsed = curBlockCacheUsed;
    }

    public float getCurMemStoreUsed() {
      return curMemStoreUsed;
    }

    public void setCurMemStoreUsed(float d) {
        this.curMemStoreUsed = d;
    }

    public void setOffheapMemStore(boolean offheapMemstore) {
      this.offheapMemstore = offheapMemstore;
    }

    public boolean isOffheapMemStore() {
      return this.offheapMemstore;
    }
  }

  /**
   * POJO which holds the result of memory tuning done by HeapMemoryTuner implementation.
   * It includes the new heap percentage for memstore and block cache.
   */
  public static final class TunerResult {
    private float memstoreSize;
    private float blockCacheSize;
    private final boolean needsTuning;

    public TunerResult(boolean needsTuning) {
      this.needsTuning = needsTuning;
    }

    public float getMemStoreSize() {
      return memstoreSize;
    }

    public void setMemStoreSize(float memstoreSize) {
      this.memstoreSize = memstoreSize;
    }

    public float getBlockCacheSize() {
      return blockCacheSize;
    }

    public void setBlockCacheSize(float blockCacheSize) {
      this.blockCacheSize = blockCacheSize;
    }

    public boolean needsTuning() {
      return needsTuning;
    }
  }

  /**
   * Every class that wants to observe heap memory tune actions must implement this interface.
   */
  public static interface HeapMemoryTuneObserver {

    /**
     * This method would be called by HeapMemoryManger when a heap memory tune action took place.
     * @param newMemstoreSize The newly calculated global memstore size
     * @param newBlockCacheSize The newly calculated global blockcache size
     */
    void onHeapMemoryTune(long newMemstoreSize, long newBlockCacheSize);
  }
}
