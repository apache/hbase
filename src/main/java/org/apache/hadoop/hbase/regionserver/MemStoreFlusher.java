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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread that flushes cache on request
 *
 * NOTE: This class extends HasThread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 *
 * @see FlushRequester
 */
class MemStoreFlusher implements FlushRequester {
  static final Log LOG = LogFactory.getLog(MemStoreFlusher.class);
  // These two data members go together.  Any entry in the one must have
  // a corresponding entry in the other.
  private final BlockingQueue<FlushQueueEntry> flushQueue =
    new DelayQueue<FlushQueueEntry>();
  private final Map<HRegion, FlushQueueEntry> regionsInQueue =
    new HashMap<HRegion, FlushQueueEntry>();

  private final boolean perColumnFamilyFlushEnabled;
  private final long threadWakeFrequency;
  private final HRegionServer server;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected final long globalMemStoreLimit;
  protected final long globalMemStoreLimitLowMark;

  protected static final float DEFAULT_UPPER = 0.4f;
  private static final float DEFAULT_LOWER = 0.25f;
  protected static final String UPPER_KEY =
    "hbase.regionserver.global.memstore.upperLimit";
  private static final String LOWER_KEY =
    "hbase.regionserver.global.memstore.lowerLimit";
  private long blockingStoreFilesNumber;
  private long blockingWaitTime;

  private FlushHandler[] flushHandlers = null;
  private int handlerCount;

  /**
   * @param conf
   * @param server
   */
  public MemStoreFlusher(final Configuration conf,
      final HRegionServer server) {
    super();
    this.server = server;
    this.threadWakeFrequency =
      conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
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
      conf.getInt("hbase.hstore.blockingStoreFiles", -1);
    if (this.blockingStoreFilesNumber == -1) {
      this.blockingStoreFilesNumber = 1 +
        conf.getInt("hbase.hstore.compactionThreshold", 3);
    }
    this.blockingWaitTime = conf.getInt("hbase.hstore.blockingWaitTime",
      90000);

    this.perColumnFamilyFlushEnabled = conf.getBoolean(
            HConstants.HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH,
            HConstants.DEFAULT_HREGION_MEMSTORE_PER_COLUMN_FAMILY_FLUSH);
    // number of "memstore flusher" threads per region server
    this.handlerCount = conf.getInt("hbase.regionserver.flusher.count", 2);

    LOG.info("globalMemStoreLimit=" +
      StringUtils.humanReadableInt(this.globalMemStoreLimit) +
      ", globalMemStoreLimitLowMark=" +
      StringUtils.humanReadableInt(this.globalMemStoreLimitLowMark) +
      ", maxHeap=" + StringUtils.humanReadableInt(max));
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

  private class FlushHandler extends HasThread {

    FlushHandler(String threadName) {
      this.setDaemon(true);
      this.setName(threadName);
    }

    @Override
    public void run() {
      while (!server.isStopRequestedAtStageTwo()) {
        FlushQueueEntry fqe = null;
        try {
          fqe = flushQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
          if (fqe == null) {
            continue;
          }
          if (!flushRegion(fqe, getName())) {
            LOG.warn("Failed to flush " + fqe.region);
          }
        } catch (InterruptedException ex) {
          continue;
        } catch (ConcurrentModificationException ex) {
          continue;
        } catch (Exception ex) {
          LOG.error("Cache flush failed" +
              (fqe != null ? (" for region " + Bytes.toString(fqe.region.getRegionName())) : ""),
              ex);
          server.checkFileSystem();
        }
      }
      LOG.info(getName() + " exiting");
    }
  }

  public void request(HRegion r) {
    request(r, false);
  }

  public void request(HRegion r, boolean selectiveFlushRequest) {
    synchronized (regionsInQueue) {
      if (!regionsInQueue.containsKey(r)) {
        // This entry has no delay so it will be added at the top of the flush
        // queue.  It'll come out near immediately.
        FlushQueueEntry fqe = new FlushQueueEntry(r, selectiveFlushRequest);
        this.regionsInQueue.put(r, fqe);
        this.flushQueue.add(fqe);
      }
    }
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    lock.writeLock().lock();
    try {
      for (FlushHandler flushHandler : flushHandlers) {
        if (flushHandler != null)
          flushHandler.interrupt();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Start the flusher threads.
   *
   * @param rsThreadName prefix for thread name (since there might be multiple
   *                     region servers running within the same JVM.
   * @param eh
   */
  void start(String rsThreadName, UncaughtExceptionHandler eh) {
    flushHandlers = new FlushHandler[handlerCount];
    for (int i = 0; i < flushHandlers.length; i++) {
      flushHandlers[i] = new FlushHandler(rsThreadName + ".cacheFlusher." + i);
      if (eh != null) {
        flushHandlers[i].setUncaughtExceptionHandler(eh);
      }
      flushHandlers[i].start();
    }
  }

  boolean isAlive() {
    for (FlushHandler flushHander : flushHandlers) {
      if (flushHander != null && flushHander.isAlive()) {
        return true;
      }
    }
    return false;
  }

  void join() {
    for (FlushHandler flushHandler : flushHandlers) {
      if (flushHandler != null) {
        Threads.shutdown(flushHandler.getThread());
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
    HRegion region = fqe.region;
    if (!fqe.region.getRegionInfo().isMetaRegion() &&
        isTooManyStoreFiles(region)) {
      if (fqe.isMaximumWait(this.blockingWaitTime)) {
        LOG.info("Waited " + (System.currentTimeMillis() - fqe.createTime) +
          "ms on a compaction to clean up 'too many store files'; waited " +
          "long enough... proceeding with flush of " +
          region.getRegionNameAsString());
      } else {
        // If this is first time we've been put off, then emit a log message.
        if (fqe.getRequeueCount() <= 0) {
          // Note: We don't impose blockingStoreFiles constraint on meta regions
          LOG.warn("Region " + region.getRegionNameAsString() + " has too many " +
            "store files; delaying flush up to " + this.blockingWaitTime + "ms");
        }

        /* If a split has been requested, we avoid scheduling a compaction
         * request because we'll have to compact after the split anyway.
         * However, if the region has reference files (from a previous split),
         * we do need to let the compactions go through so that half file
         * references to parent regions are removed, and we can split this
         * region further.
         */
        if (!this.server.compactSplitThread.requestSplit(region)
            || region.hasReferences()) {
          this.server.compactSplitThread.requestCompaction(region, why);
        }
        // Put back on the queue.  Have it come back out of the queue
        // after a delay of this.blockingWaitTime / 100 ms.
        this.flushQueue.add(fqe.requeue(this.blockingWaitTime / 100));
        // Tell a lie, it's not flushed but it's ok
        return true;
      }
    }
    return flushRegion(region, why, false, fqe.isSelectiveFlushRequest());
  }

  /**
   * Flush a region.
   * @param region Region to flush.
   * @param emergencyFlush Set if we are being force flushed. If true the region
   * needs to be removed from the flush queue. If false, when we were called
   * from the main flusher run loop and we got the entry to flush by calling
   * poll on the flush queue (which removed it).
   * @param selectiveFlushRequest Do we want to selectively flush only the
   * column families that dominate the memstore size?
   *
   * @return true if the region was successfully flushed, false otherwise. If
   * false, there will be accompanying log messages explaining why the log was
   * not flushed.
   */
  private boolean flushRegion(final HRegion region, String why,
    final boolean emergencyFlush, boolean selectiveFlushRequest) {

    synchronized (this.regionsInQueue) {
      FlushQueueEntry fqe = this.regionsInQueue.remove(region);
      if (fqe != null && emergencyFlush) {
        // Need to remove from region from delay queue.  When NOT an
        // emergencyFlush, then item was removed via a flushQueue.poll.
        flushQueue.remove(fqe);
     }
     lock.readLock().lock();
    }
    try {
      if (region.flushcache(selectiveFlushRequest)) {
        server.compactSplitThread.requestCompaction(region, why);
      }
      server.getMetrics().addFlush(region.getRecentFlushInfo());
    } catch (IOException ex) {
      LOG.warn("Cache flush failed" +
        (region != null ? (" for region " +
        Bytes.toString(region.getRegionName())) : ""),
        RemoteExceptionHandler.checkIOException(ex));
      server.checkFileSystem();
      return false;
    } finally {
      lock.readLock().unlock();
    }
    return true;
  }

  private boolean isTooManyStoreFiles(HRegion region) {
    for (Store hstore: region.stores.values()) {
      if (hstore.getStorefilesCount() > this.blockingStoreFilesNumber) {
        return true;
      }
    }
    return false;
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
    for (SortedMap<Long, HRegion> m =
        this.server.getCopyOfOnlineRegionsSortedBySize();
      (globalMemStoreSize = this.server.getGlobalMemstoreSize().get()) >=
        this.globalMemStoreLimitLowMark;) {
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
      if (!flushRegion(biggestMemStoreRegion, "emergencyFlush", true, false)) {
        LOG.warn("Flush failed");
        break;
      }
      regionsToCompact.add(biggestMemStoreRegion);
    }
    for (HRegion region : regionsToCompact) {
      server.compactSplitThread.requestCompaction(region, "emergencyFlush");
    }
  }

  /**
   * Datastructure used in the flush queue.  Holds region and retry count.
   * Keeps tabs on how old this object is.  Implements {@link Delayed}.  On
   * construction, the delay is zero. When added to a delay queue, we'll come
   * out near immediately.  Call {@link #requeue(long)} passing delay in
   * milliseconds before readding to delay queue if you want it to stay there
   * a while.
   */
  static class FlushQueueEntry implements Delayed {
    private final HRegion region;
    private final long createTime;
    private long whenToExpire;
    private int requeueCount = 0;
    private boolean selectiveFlushRequest;

    /**
     * @param r The region to flush
     * @param selectiveFlushRequest Do we want to flush only the column
     *                              families that dominate the memstore size,
     *                              i.e., do a selective flush? If we are
     *                              doing log rolling, then we should not do a
     *                              selective flush.
     */
    FlushQueueEntry(final HRegion r, boolean selectiveFlushRequest) {
      this.region = r;
      this.createTime = System.currentTimeMillis();
      this.whenToExpire = this.createTime;
      this.selectiveFlushRequest = selectiveFlushRequest;
    }

    /**
     * @return Is this a request for a selective flush?
     */
    public boolean isSelectiveFlushRequest() {
      return selectiveFlushRequest;
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
     * @param when When to expire, when to come up out of the queue.
     * Specify in milliseconds.  This method adds System.currentTimeMillis()
     * to whatever you pass.
     * @return This.
     */
    public FlushQueueEntry requeue(final long when) {
      this.whenToExpire = System.currentTimeMillis() + when;
      this.requeueCount++;
      return this;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.whenToExpire - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      return Long.valueOf(getDelay(TimeUnit.MILLISECONDS) -
        other.getDelay(TimeUnit.MILLISECONDS)).intValue();
    }
  }
}
