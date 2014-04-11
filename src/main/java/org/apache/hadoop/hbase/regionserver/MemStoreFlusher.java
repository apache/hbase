/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

/**
 * Thread that flushes cache on request
 *
 * NOTE: This class extends HasThread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 *
 * @see FlushRequester
 */
class MemStoreFlusher implements FlushRequester, ConfigurationObserver {
  static final Log LOG = LogFactory.getLog(MemStoreFlusher.class);
  private final Map<HRegionIf, Pair<FlushQueueEntry, Future<Boolean>>>
      regionsInQueue = new HashMap<>();

  private final HRegionServerIf server;

  protected final long globalMemStoreLimit;
  protected final long globalMemStoreLimitLowMark;

  protected static final float DEFAULT_UPPER = 0.4f;
  private static final float DEFAULT_LOWER = 0.25f;
  protected static final String UPPER_KEY =
    "hbase.regionserver.global.memstore.upperLimit";
  private static final String LOWER_KEY =
    "hbase.regionserver.global.memstore.lowerLimit";

  private int blockingStoreFilesNumber;
  private long blockingWaitTime;

  private int handlerCount;
  private final ScheduledThreadPoolExecutor threadPool;

  /**
   * @param conf
   * @param server
   */
  public MemStoreFlusher(final Configuration conf, HRegionServerIf server) {
    this.server = server;
    long max = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    this.globalMemStoreLimit = globalMemStoreLimit(max, DEFAULT_UPPER,
      UPPER_KEY, conf);
    long lower = globalMemStoreLimit(max, DEFAULT_LOWER, LOWER_KEY, conf);
    if (lower > this.globalMemStoreLimit) {
      lower = this.globalMemStoreLimit;
      LOG.info("Setting globalMemStoreLimitLowMark == globalMemStoreLimit " +
        "because supplied " + LOWER_KEY + " was > " + UPPER_KEY);
    }
    this.globalMemStoreLimitLowMark = lower;
    this.blockingStoreFilesNumber =
        conf.getInt(HConstants.HSTORE_BLOCKING_STORE_FILES_KEY, -1);
    if (this.blockingStoreFilesNumber == -1) {
      this.blockingStoreFilesNumber = 1 +
        conf.getInt("hbase.hstore.compactionThreshold", 3);
    }
    this.blockingWaitTime =
        conf.getLong(HConstants.HSTORE_BLOCKING_WAIT_TIME_KEY,
            HConstants.DEFAULT_HSTORE_BLOCKING_WAIT_TIME);

    // number of "memstore flusher" threads per region server
    this.handlerCount = conf.getInt(HConstants.FLUSH_THREADS, HConstants.DEFAULT_FLUSH_THREADS);

    LOG.info("globalMemStoreLimit=" +
      StringUtils.humanReadableInt(this.globalMemStoreLimit) +
      ", globalMemStoreLimitLowMark=" +
      StringUtils.humanReadableInt(this.globalMemStoreLimitLowMark) +
      ", maxHeap=" + StringUtils.humanReadableInt(max));
    this.threadPool = new ScheduledThreadPoolExecutor(handlerCount,
        new DaemonThreadFactory("flush-thread-"));
    this.threadPool.setMaximumPoolSize(handlerCount);
  }

  /**
   * Calculate size using passed <code>key</code> for configured
   * percentage of <code>max</code>.
   * @param max
   * @param defaultLimit
   * @param key
   * @param c
   * @return Limit.
   */
  static long globalMemStoreLimit(final long max,
     final float defaultLimit, final String key, final Configuration c) {
    float limit = c.getFloat(key, defaultLimit);
    return getMemStoreLimit(max, limit, defaultLimit);
  }

  static long getMemStoreLimit(final long max, final float limit,
      final float defaultLimit) {
    if (limit >= 0.9f || limit < 0.1f) {
      LOG.warn("Setting global memstore limit to default of " + defaultLimit +
        " because supplied value outside allowed range of 0.1 -> 0.9");
    }
    return (long)(max * limit);
  }

  @Override
  public void request(HRegionIf r, boolean isSelective) {
    synchronized (regionsInQueue) {
      if (!regionsInQueue.containsKey(r)) {
        // This entry has no delay so it will be added at the top of the flush
        // queue.  It'll come out near immediately.
        FlushQueueEntry fqe = new FlushQueueEntry(r, isSelective);
        regionsInQueue.put(r, Pair.newPair(fqe, (Future<Boolean>) null));
        executeFlushQueueEntry(fqe, 0);
      } else {
        LOG.info("Flush for " + r + " already scheduled.");
      }
    }
  }

  /**
   * Called synchronized with regionsInQueue
   */
  protected void executeFlushQueueEntry(final FlushQueueEntry fqe, long msDelay) {
    Callable<Boolean> callable = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        String name =
            String.format("%s.cacheFlusher.%d",
                MemStoreFlusher.this.server.getRSThreadName(),
                Thread.currentThread().getId());
        if (!flushRegion(fqe, name)) {
          LOG.warn("Failed to flush " + fqe.region);
          return false;
        }
        return true;
      }
    };

    LOG.debug("Schedule a flush request " + fqe + " with delay " + msDelay
        + "ms");

    Future<Boolean> future =
        this.threadPool.schedule(callable, msDelay, TimeUnit.MILLISECONDS);
    Pair<FlushQueueEntry, Future<Boolean>> pair =
        regionsInQueue.get(fqe.region);
    if (pair != null) {
      pair.setSecond(future);
    }
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    threadPool.shutdown();
  }


  boolean isAlive() {
    return !threadPool.isShutdown();
  }

  void join() {
    boolean done = false;
    while (!done) {
      try {
        LOG.debug("Waiting for flush thread to finish...");
        done = threadPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        LOG.error("Interrupted waiting for flush thread to finish...");
      }
    }
  }

  /*
   * A flushRegion that checks store file count.  If too many, puts the flush
   * on delay queue to retry later.
   * @param fqe
   * @return true if the region was successfully flushed, false otherwise. If
   * false, there will be accompanying log messages explaining why the log was
   * not flushed.
   */
  private boolean flushRegion(final FlushQueueEntry fqe, String why) {
    HRegionIf region = fqe.region;
    if (!fqe.region.getRegionInfo().isMetaRegion() &&
        region.maxStoreFilesCount() > this.blockingStoreFilesNumber) {
      if (fqe.isMaximumWait(this.blockingWaitTime)) {
        LOG.info("Waited " + (System.currentTimeMillis() - fqe.createTime)
            + "ms on a compaction to clean up 'too many store files'; waited "
            + "long enough... proceeding with flush of "
            + region.getRegionInfo().getRegionNameAsString());
      } else {
        // If this is first time we've been put off, then emit a log message.
        if (fqe.getRequeueCount() <= 0) {
          // Note: We don't impose blockingStoreFiles constraint on meta regions
          LOG.warn("Region " + region.getRegionInfo().getRegionNameAsString()
              + " has too many store files; delaying flush up to "
              + this.blockingWaitTime + "ms");
        }

        /* If a split has been requested, we avoid scheduling a compaction
         * request because we'll have to compact after the split anyway.
         * However, if the region has reference files (from a previous split),
         * we do need to let the compactions go through so that half file
         * references to parent regions are removed, and we can split this
         * region further.
         */
        if (!this.server.requestSplit(region) || region.hasReferences()) {
          this.server.requestCompaction(region, why);
        }

        synchronized (this.regionsInQueue) {
          // Put back on the queue. Have it come back out of the queue
          // after a delay of this.blockingWaitTime / 100 ms.
          executeFlushQueueEntry(fqe.requeue(), this.blockingWaitTime / 100);
        }
        // Tell a lie, it's not flushed but it's OK
        return true;
      }
    }
    try {
      return flushRegionNow(region, why, fqe.selective());
    } finally {
      // the task is executed, remove from regionsInQueue
      synchronized (this.regionsInQueue) {
        this.regionsInQueue.remove(region);
      }
    }
  }

  /**
   * Flush a region.
   * @param region Region to flush.
   * @param selectiveFlushRequest Do we want to selectively flush only the
   * column families that dominate the memstore size?
   *
   * @return true if the region was successfully flushed, false otherwise. If
   * false, there will be accompanying log messages explaining why the log was
   * not flushed.
   */
  private boolean flushRegionNow(HRegionIf region, String why,
      boolean selectiveFlushRequest) {
    try {
      boolean res = region.flushMemstoreShapshot(selectiveFlushRequest);
      if (res) {
        server.requestCompaction(region, why);
      }
      server.getMetrics().addFlush(region.getRecentFlushInfo());
      return res;
    } catch (IOException ex) {
      LOG.warn(
          "Cache flush failed"
          + (region != null
            ? (" for region " + region.getRegionInfo().getRegionNameAsString())
            : ""),
          RemoteExceptionHandler.checkIOException(ex));
      server.checkFileSystem();
      return false;
    }
  }

  /**
   * Check if the regionserver's memstore memory usage is greater than the
   * limit. If so, flush regions with the biggest memstores until we're down
   * to the lower limit. This method blocks callers until we're down to a safe
   * amount of memstore consumption.
   */
  public void reclaimMemStoreMemory() {
    if (this.server.getGlobalMemstoreSize().get() >= globalMemStoreLimit) {
      flushSomeRegions();
    }
  }

  /**
   * Makes an emergency flush.
   *
   * If a flush request is found in the queue, these method will wait for that
   * flush to be finished. Otherwise a flush will be performed in the current
   * thread by calling to {@code #flushRegionNow(IRegion, String, boolean)}
   */
  private boolean doEmergencyFlush(HRegionIf region, String why,
      boolean selectiveFlushRequest) {
    Pair<FlushQueueEntry, Future<Boolean>> pair;
    synchronized (regionsInQueue) {
      pair = regionsInQueue.get(region);
    }
    if (pair != null) {
      // Already has flush request, wait for its finish.
      try {
        return pair.getSecond().get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("Interrupted waiting for flushing of " + region, e);
        return false;
      } catch (ExecutionException e) {
        // This should not happen actually, all exception should have been
        // caught in Callable.
        LOG.info("ExecutionException caught for flushing of " + region, e);
        return false;
      }
    }

    // Perform a flush in current thread
    return flushRegionNow(region, why, selectiveFlushRequest);
  }

  /*
   * Emergency!  Need to flush memory.
   */
  private synchronized void flushSomeRegions() {
    if (this.server.getGlobalMemstoreSize().get() < globalMemStoreLimit) {
      return; // double check the global memstore size inside of the synchronized block.
    }

    // keep flushing until we hit the low water mark
    long globalMemStoreSize = -1;
    ArrayList<HRegion> regionsToCompact = new ArrayList<HRegion>();
    SortedMap<Long, HRegion> m =
        this.server.getCopyOfOnlineRegionsSortedBySize();
    while (true) {
      globalMemStoreSize = this.server.getGlobalMemstoreSize().get();
      if (globalMemStoreSize < this.globalMemStoreLimitLowMark) {
        break;
      }
      // flush the region with the biggest memstore
      if (m.size() <= 0) {
        LOG.info("No online regions to flush though we've been asked flush " +
          "some; globalMemStoreSize=" +
          StringUtils.humanReadableInt(globalMemStoreSize) +
          ", globalMemStoreLimitLowMark=" +
          StringUtils.humanReadableInt(this.globalMemStoreLimitLowMark));
        break;
      }
      HRegion biggestMemStoreRegion = m.remove(m.firstKey());
      LOG.info("Forced flushing of " +  biggestMemStoreRegion.toString() +
        " because global memstore limit of " +
        StringUtils.humanReadableInt(this.globalMemStoreLimit) +
        " exceeded; currently " +
        StringUtils.humanReadableInt(globalMemStoreSize) + " and flushing till " +
        StringUtils.humanReadableInt(this.globalMemStoreLimitLowMark));
      if (!doEmergencyFlush(biggestMemStoreRegion, "emergencyFlush", false)) {
        LOG.warn("Flush failed");
        break;
      }
      regionsToCompact.add(biggestMemStoreRegion);
    }
    for (HRegion region : regionsToCompact) {
      server.requestCompaction(region, "emergencyFlush");
    }
  }

  /**
   * Data structure used in the flush queue. Holds region and retry count.
   * Keeps tabs on how old this object is. Implements {@link Delayed}. On
   * construction, the delay is zero. When added to a delay queue, we'll come
   * out near immediately. Call {@link #requeue(long)} passing delay in
   * milliseconds before reading to delay queue if you want it to stay there
   * a while.
   */
  static class FlushQueueEntry {
    private final HRegionIf region;
    private final long createTime;
    private int requeueCount = 0;
    private boolean selective;

    /**
     * @param r The region to flush
     * @param selective Do we want to flush only the column
     *                              families that dominate the memstore size,
     *                              i.e., do a selective flush? If we are
     *                              doing log rolling, then we should not do a
     *                              selective flush.
     */
    FlushQueueEntry(final HRegionIf r, boolean selective) {
      this.region = r;
      this.createTime = System.currentTimeMillis();
      this.selective = selective;
    }

    /**
     * @return Is this a request for a selective flush?
     */
    public boolean selective() {
      return selective;
    }

    /**
     * @param maximumWait
     * @return True if we have been delayed > <code>maximumWait</code> milliseconds.
     */
    public boolean isMaximumWait(final long maximumWait) {
      return (System.currentTimeMillis() - this.createTime) > maximumWait;
    }

    /**
     * @return Count of times {@link #resetDelay()} was called; i.e this is
     * number of times we've been requeued.
     */
    public int getRequeueCount() {
      return this.requeueCount;
    }

    /**
     * Increases the requeue count.
     *
     * @return this.
     */
    public FlushQueueEntry requeue() {
      this.requeueCount++;
      return this;
    }

    @Override
    public String toString() {
      return "{regin: " + region + ", created: " + createTime + ", requeue: "
          + requeueCount + ", selective: " + selective + "}";
    }
  }


  @Override
  public void notifyOnChange(Configuration newConf) {
    // number of "memstore flusher" threads per region server
    int handlerCount = newConf.getInt(HConstants.FLUSH_THREADS, HConstants.DEFAULT_FLUSH_THREADS);
    if(this.handlerCount != handlerCount){
      LOG.info("Changing the value of " + HConstants.FLUSH_THREADS + " from "
          + this.handlerCount + " to " + handlerCount);
    }
    this.threadPool.setMaximumPoolSize(handlerCount);
    this.threadPool.setCorePoolSize(handlerCount);
    this.handlerCount = handlerCount;
  }

  /**
   * Helper method for tests to check if the number of flush threads
   * change on-the-fly.
   *
   * @return
   */
  protected int getFlushThreadNum() {
    return this.threadPool.getCorePoolSize();
  }

  /**
   * Waits for all current request to be done.
   * Used only in testcases.
   */
  void waitAllRequestDone() throws ExecutionException, InterruptedException {
    while (true) {
      // Fetch futures.
      List<Future<Boolean>> futures = new ArrayList<>();
      synchronized (this.regionsInQueue) {
        for (Pair<FlushQueueEntry, Future<Boolean>> pair :
            regionsInQueue.values()) {
          futures.add(pair.getSecond());
        }
      }

      if (futures.size() == 0) {
        // No more requests, quit
        return;
      }

      // Wait for futures
      for (Future<Boolean> future : futures) {
        future.get();
      }
      // This is a loop because some new requests may be generated during
      // executing current requests.
    }
  }
}
