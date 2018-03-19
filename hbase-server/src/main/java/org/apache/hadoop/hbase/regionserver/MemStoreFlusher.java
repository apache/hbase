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

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.htrace.core.TraceScope;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread that flushes cache on request
 *
 * NOTE: This class extends Thread rather than Chore because the sleep time
 * can be interrupted when there is something to do, rather than the Chore
 * sleep time which is invariant.
 *
 * @see FlushRequester
 */
@InterfaceAudience.Private
class MemStoreFlusher implements FlushRequester {
  private static final Logger LOG = LoggerFactory.getLogger(MemStoreFlusher.class);

  private Configuration conf;
  // These two data members go together.  Any entry in the one must have
  // a corresponding entry in the other.
  private final BlockingQueue<FlushQueueEntry> flushQueue = new DelayQueue<>();
  private final Map<Region, FlushRegionEntry> regionsInQueue = new HashMap<>();
  private AtomicBoolean wakeupPending = new AtomicBoolean();

  private final long threadWakeFrequency;
  private final HRegionServer server;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Object blockSignal = new Object();

  private long blockingWaitTime;
  private final LongAdder updatesBlockedMsHighWater = new LongAdder();

  private final FlushHandler[] flushHandlers;
  private List<FlushRequestListener> flushRequestListeners = new ArrayList<>(1);

  private FlushType flushType;

  /**
   * Singleton instance inserted into flush queue used for signaling.
   */
  private static final FlushQueueEntry WAKEUPFLUSH_INSTANCE = new FlushQueueEntry() {
    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed o) {
      return -1;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public int hashCode() {
      return 42;
    }
  };


  /**
   * @param conf
   * @param server
   */
  public MemStoreFlusher(final Configuration conf,
      final HRegionServer server) {
    super();
    this.conf = conf;
    this.server = server;
    this.threadWakeFrequency =
        conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.blockingWaitTime = conf.getInt("hbase.hstore.blockingWaitTime",
      90000);
    int handlerCount = conf.getInt("hbase.hstore.flusher.count", 2);
    this.flushHandlers = new FlushHandler[handlerCount];
    LOG.info("globalMemStoreLimit="
        + TraditionalBinaryPrefix
            .long2String(this.server.getRegionServerAccounting().getGlobalMemStoreLimit(), "", 1)
        + ", globalMemStoreLimitLowMark="
        + TraditionalBinaryPrefix.long2String(
          this.server.getRegionServerAccounting().getGlobalMemStoreLimitLowMark(), "", 1)
        + ", Offheap="
        + (this.server.getRegionServerAccounting().isOffheap()));
  }

  public LongAdder getUpdatesBlockedMsHighWater() {
    return this.updatesBlockedMsHighWater;
  }

  public void setFlushType(FlushType flushType) {
    this.flushType = flushType;
  }

  /**
   * The memstore across all regions has exceeded the low water mark. Pick
   * one region to flush and flush it synchronously (this is called from the
   * flush thread)
   * @return true if successful
   */
  private boolean flushOneForGlobalPressure() {
    SortedMap<Long, Collection<HRegion>> regionsBySize = null;
    switch(flushType) {
      case ABOVE_OFFHEAP_HIGHER_MARK:
      case ABOVE_OFFHEAP_LOWER_MARK:
        regionsBySize = server.getCopyOfOnlineRegionsSortedByOffHeapSize();
        break;
      case ABOVE_ONHEAP_HIGHER_MARK:
      case ABOVE_ONHEAP_LOWER_MARK:
      default:
        regionsBySize = server.getCopyOfOnlineRegionsSortedByOnHeapSize();
    }
    Set<HRegion> excludedRegions = new HashSet<>();

    double secondaryMultiplier
      = ServerRegionReplicaUtil.getRegionReplicaStoreFileRefreshMultiplier(conf);

    boolean flushedOne = false;
    while (!flushedOne) {
      // Find the biggest region that doesn't have too many storefiles (might be null!)
      HRegion bestFlushableRegion =
          getBiggestMemStoreRegion(regionsBySize, excludedRegions, true);
      // Find the biggest region, total, even if it might have too many flushes.
      HRegion bestAnyRegion = getBiggestMemStoreRegion(regionsBySize, excludedRegions, false);
      // Find the biggest region that is a secondary region
      HRegion bestRegionReplica = getBiggestMemStoreOfRegionReplica(regionsBySize, excludedRegions);
      if (bestAnyRegion == null) {
        // If bestAnyRegion is null, assign replica. It may be null too. Next step is check for null
        bestAnyRegion = bestRegionReplica;
      }
      if (bestAnyRegion == null) {
        LOG.error("Above memory mark but there are no flushable regions!");
        return false;
      }

      HRegion regionToFlush;
      long bestAnyRegionSize;
      long bestFlushableRegionSize;
      switch(flushType) {
        case ABOVE_OFFHEAP_HIGHER_MARK:
        case ABOVE_OFFHEAP_LOWER_MARK:
          bestAnyRegionSize = bestAnyRegion.getMemStoreOffHeapSize();
          bestFlushableRegionSize = getMemStoreOffHeapSize(bestFlushableRegion);
          break;

        case ABOVE_ONHEAP_HIGHER_MARK:
        case ABOVE_ONHEAP_LOWER_MARK:
          bestAnyRegionSize = bestAnyRegion.getMemStoreHeapSize();
          bestFlushableRegionSize = getMemStoreHeapSize(bestFlushableRegion);
          break;

        default:
          bestAnyRegionSize = bestAnyRegion.getMemStoreDataSize();
          bestFlushableRegionSize = getMemStoreDataSize(bestFlushableRegion);
      }
      if (bestAnyRegionSize > 2 * bestFlushableRegionSize) {
        // Even if it's not supposed to be flushed, pick a region if it's more than twice
        // as big as the best flushable one - otherwise when we're under pressure we make
        // lots of little flushes and cause lots of compactions, etc, which just makes
        // life worse!
        if (LOG.isDebugEnabled()) {
          LOG.debug("Under global heap pressure: " + "Region "
              + bestAnyRegion.getRegionInfo().getRegionNameAsString()
              + " has too many " + "store files, but is "
              + TraditionalBinaryPrefix.long2String(bestAnyRegionSize, "", 1)
              + " vs best flushable region's "
              + TraditionalBinaryPrefix.long2String(
              bestFlushableRegionSize, "", 1)
              + ". Choosing the bigger.");
        }
        regionToFlush = bestAnyRegion;
      } else {
        if (bestFlushableRegion == null) {
          regionToFlush = bestAnyRegion;
        } else {
          regionToFlush = bestFlushableRegion;
        }
      }

      long regionToFlushSize;
      long bestRegionReplicaSize;
      switch(flushType) {
        case ABOVE_OFFHEAP_HIGHER_MARK:
        case ABOVE_OFFHEAP_LOWER_MARK:
          regionToFlushSize = regionToFlush.getMemStoreOffHeapSize();
          bestRegionReplicaSize = getMemStoreOffHeapSize(bestRegionReplica);
          break;

        case ABOVE_ONHEAP_HIGHER_MARK:
        case ABOVE_ONHEAP_LOWER_MARK:
          regionToFlushSize = regionToFlush.getMemStoreHeapSize();
          bestRegionReplicaSize = getMemStoreHeapSize(bestRegionReplica);
          break;

        default:
          regionToFlushSize = regionToFlush.getMemStoreDataSize();
          bestRegionReplicaSize = getMemStoreDataSize(bestRegionReplica);
      }

      Preconditions.checkState(
        (regionToFlush != null && regionToFlushSize > 0) || bestRegionReplicaSize > 0);

      if (regionToFlush == null ||
          (bestRegionReplica != null &&
           ServerRegionReplicaUtil.isRegionReplicaStoreFileRefreshEnabled(conf) &&
           (bestRegionReplicaSize > secondaryMultiplier * regionToFlushSize))) {
        LOG.info("Refreshing storefiles of region " + bestRegionReplica +
            " due to global heap pressure. Total memstore off heap size=" +
            TraditionalBinaryPrefix.long2String(
              server.getRegionServerAccounting().getGlobalMemStoreOffHeapSize(), "", 1) +
            " memstore heap size=" + TraditionalBinaryPrefix.long2String(
              server.getRegionServerAccounting().getGlobalMemStoreHeapSize(), "", 1));
        flushedOne = refreshStoreFilesAndReclaimMemory(bestRegionReplica);
        if (!flushedOne) {
          LOG.info("Excluding secondary region " + bestRegionReplica +
              " - trying to find a different region to refresh files.");
          excludedRegions.add(bestRegionReplica);
        }
      } else {
        LOG.info("Flush of region " + regionToFlush + " due to global heap pressure. " +
            "Flush type=" + flushType.toString() +
            "Total Memstore Heap size=" +
            TraditionalBinaryPrefix.long2String(
                server.getRegionServerAccounting().getGlobalMemStoreHeapSize(), "", 1) +
            "Total Memstore Off-Heap size=" +
            TraditionalBinaryPrefix.long2String(
                server.getRegionServerAccounting().getGlobalMemStoreOffHeapSize(), "", 1) +
            ", Region memstore size=" +
            TraditionalBinaryPrefix.long2String(regionToFlushSize, "", 1));
        flushedOne = flushRegion(regionToFlush, true, false, FlushLifeCycleTracker.DUMMY);

        if (!flushedOne) {
          LOG.info("Excluding unflushable region " + regionToFlush +
              " - trying to find a different region to flush.");
          excludedRegions.add(regionToFlush);
        }
      }
    }
    return true;
  }

  /**
   * @return Return memstore offheap size or null if <code>r</code> is null
   */
  private static long getMemStoreOffHeapSize(HRegion r) {
    return r == null? 0: r.getMemStoreOffHeapSize();
  }

  /**
   * @return Return memstore heap size or null if <code>r</code> is null
   */
  private static long getMemStoreHeapSize(HRegion r) {
    return r == null? 0: r.getMemStoreHeapSize();
  }

  /**
   * @return Return memstore data size or null if <code>r</code> is null
   */
  private static long getMemStoreDataSize(HRegion r) {
    return r == null? 0: r.getMemStoreDataSize();
  }

  private class FlushHandler extends HasThread {

    private FlushHandler(String name) {
      super(name);
    }

    @Override
    public void run() {
      while (!server.isStopped()) {
        FlushQueueEntry fqe = null;
        try {
          wakeupPending.set(false); // allow someone to wake us up again
          fqe = flushQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
          if (fqe == null || fqe == WAKEUPFLUSH_INSTANCE) {
            FlushType type = isAboveLowWaterMark();
            if (type != FlushType.NORMAL) {
              LOG.debug("Flush thread woke up because memory above low water="
                  + TraditionalBinaryPrefix.long2String(
                    server.getRegionServerAccounting().getGlobalMemStoreLimitLowMark(), "", 1));
              // For offheap memstore, even if the lower water mark was breached due to heap overhead
              // we still select the regions based on the region's memstore data size.
              // TODO : If we want to decide based on heap over head it can be done without tracking
              // it per region.
              if (!flushOneForGlobalPressure()) {
                // Wasn't able to flush any region, but we're above low water mark
                // This is unlikely to happen, but might happen when closing the
                // entire server - another thread is flushing regions. We'll just
                // sleep a little bit to avoid spinning, and then pretend that
                // we flushed one, so anyone blocked will check again
                Thread.sleep(1000);
                wakeUpIfBlocking();
              }
              // Enqueue another one of these tokens so we'll wake up again
              wakeupFlushThread();
            }
            continue;
          }
          FlushRegionEntry fre = (FlushRegionEntry) fqe;
          if (!flushRegion(fre)) {
            break;
          }
        } catch (InterruptedException ex) {
          continue;
        } catch (ConcurrentModificationException ex) {
          continue;
        } catch (Exception ex) {
          LOG.error("Cache flusher failed for entry " + fqe, ex);
          if (!server.checkFileSystem()) {
            break;
          }
        }
      }
      synchronized (regionsInQueue) {
        regionsInQueue.clear();
        flushQueue.clear();
      }

      // Signal anyone waiting, so they see the close flag
      wakeUpIfBlocking();
      LOG.info(getName() + " exiting");
    }
  }


  private void wakeupFlushThread() {
    if (wakeupPending.compareAndSet(false, true)) {
      flushQueue.add(WAKEUPFLUSH_INSTANCE);
    }
  }

  private HRegion getBiggestMemStoreRegion(
      SortedMap<Long, Collection<HRegion>> regionsBySize,
      Set<HRegion> excludedRegions,
      boolean checkStoreFileCount) {
    synchronized (regionsInQueue) {
      for (Map.Entry<Long, Collection<HRegion>> entry : regionsBySize.entrySet()) {
        for (HRegion region : entry.getValue()) {
          if (excludedRegions.contains(region)) {
            continue;
          }

          if (region.writestate.flushing || !region.writestate.writesEnabled) {
            continue;
          }

          if (checkStoreFileCount && isTooManyStoreFiles(region)) {
            continue;
          }
          return region;
        }
      }
    }
    return null;
  }

  private HRegion getBiggestMemStoreOfRegionReplica(
      SortedMap<Long, Collection<HRegion>> regionsBySize,
      Set<HRegion> excludedRegions) {
    synchronized (regionsInQueue) {
      for (Map.Entry<Long, Collection<HRegion>> entry : regionsBySize.entrySet()) {
        for (HRegion region : entry.getValue()) {
          if (excludedRegions.contains(region)) {
            continue;
          }

          if (RegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
            continue;
          }
          return region;
        }
      }
    }
    return null;
  }

  private boolean refreshStoreFilesAndReclaimMemory(Region region) {
    try {
      return region.refreshStoreFiles();
    } catch (IOException e) {
      LOG.warn("Refreshing store files failed with exception", e);
    }
    return false;
  }

  /**
   * Return true if global memory usage is above the high watermark
   */
  private FlushType isAboveHighWaterMark() {
    return server.getRegionServerAccounting().isAboveHighWaterMark();
  }

  /**
   * Return true if we're above the low watermark
   */
  private FlushType isAboveLowWaterMark() {
    return server.getRegionServerAccounting().isAboveLowWaterMark();
  }

  @Override
  public void requestFlush(HRegion r, boolean forceFlushAllStores, FlushLifeCycleTracker tracker) {
    r.incrementFlushesQueuedCount();
    synchronized (regionsInQueue) {
      if (!regionsInQueue.containsKey(r)) {
        // This entry has no delay so it will be added at the top of the flush
        // queue. It'll come out near immediately.
        FlushRegionEntry fqe = new FlushRegionEntry(r, forceFlushAllStores, tracker);
        this.regionsInQueue.put(r, fqe);
        this.flushQueue.add(fqe);
      } else {
        tracker.notExecuted("Flush already requested on " + r);
      }
    }
  }

  @Override
  public void requestDelayedFlush(HRegion r, long delay, boolean forceFlushAllStores) {
    r.incrementFlushesQueuedCount();
    synchronized (regionsInQueue) {
      if (!regionsInQueue.containsKey(r)) {
        // This entry has some delay
        FlushRegionEntry fqe =
            new FlushRegionEntry(r, forceFlushAllStores, FlushLifeCycleTracker.DUMMY);
        fqe.requeue(delay);
        this.regionsInQueue.put(r, fqe);
        this.flushQueue.add(fqe);
      }
    }
  }

  public int getFlushQueueSize() {
    return flushQueue.size();
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    lock.writeLock().lock();
    try {
      for (FlushHandler flushHander : flushHandlers) {
        if (flushHander != null) flushHander.interrupt();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  synchronized void start(UncaughtExceptionHandler eh) {
    ThreadFactory flusherThreadFactory = Threads.newDaemonThreadFactory(
        server.getServerName().toShortString() + "-MemStoreFlusher", eh);
    for (int i = 0; i < flushHandlers.length; i++) {
      flushHandlers[i] = new FlushHandler("MemStoreFlusher." + i);
      flusherThreadFactory.newThread(flushHandlers[i]);
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
    for (FlushHandler flushHander : flushHandlers) {
      if (flushHander != null) {
        Threads.shutdown(flushHander.getThread());
      }
    }
  }

  /**
   * A flushRegion that checks store file count.  If too many, puts the flush
   * on delay queue to retry later.
   * @param fqe
   * @return true if the region was successfully flushed, false otherwise. If
   * false, there will be accompanying log messages explaining why the region was
   * not flushed.
   */
  private boolean flushRegion(final FlushRegionEntry fqe) {
    HRegion region = fqe.region;
    if (!region.getRegionInfo().isMetaRegion() && isTooManyStoreFiles(region)) {
      if (fqe.isMaximumWait(this.blockingWaitTime)) {
        LOG.info("Waited " + (EnvironmentEdgeManager.currentTime() - fqe.createTime) +
          "ms on a compaction to clean up 'too many store files'; waited " +
          "long enough... proceeding with flush of " +
          region.getRegionInfo().getRegionNameAsString());
      } else {
        // If this is first time we've been put off, then emit a log message.
        if (fqe.getRequeueCount() <= 0) {
          // Note: We don't impose blockingStoreFiles constraint on meta regions
          LOG.warn("Region " + region.getRegionInfo().getEncodedName() + " has too many " +
            "store files; delaying flush up to " + this.blockingWaitTime + "ms");
          if (!this.server.compactSplitThread.requestSplit(region)) {
            try {
              this.server.compactSplitThread.requestSystemCompaction(region,
                Thread.currentThread().getName());
            } catch (IOException e) {
              e = e instanceof RemoteException ?
                      ((RemoteException)e).unwrapRemoteException() : e;
              LOG.error("Cache flush failed for region " +
                Bytes.toStringBinary(region.getRegionInfo().getRegionName()), e);
            }
          }
        }

        // Put back on the queue.  Have it come back out of the queue
        // after a delay of this.blockingWaitTime / 100 ms.
        this.flushQueue.add(fqe.requeue(this.blockingWaitTime / 100));
        // Tell a lie, it's not flushed but it's ok
        return true;
      }
    }
    return flushRegion(region, false, fqe.isForceFlushAllStores(), fqe.getTracker());
  }

  /**
   * Flush a region.
   * @param region Region to flush.
   * @param emergencyFlush Set if we are being force flushed. If true the region
   * needs to be removed from the flush queue. If false, when we were called
   * from the main flusher run loop and we got the entry to flush by calling
   * poll on the flush queue (which removed it).
   * @param forceFlushAllStores whether we want to flush all store.
   * @return true if the region was successfully flushed, false otherwise. If
   * false, there will be accompanying log messages explaining why the region was
   * not flushed.
   */
  private boolean flushRegion(HRegion region, boolean emergencyFlush, boolean forceFlushAllStores,
      FlushLifeCycleTracker tracker) {
    synchronized (this.regionsInQueue) {
      FlushRegionEntry fqe = this.regionsInQueue.remove(region);
      // Use the start time of the FlushRegionEntry if available
      if (fqe != null && emergencyFlush) {
        // Need to remove from region from delay queue. When NOT an
        // emergencyFlush, then item was removed via a flushQueue.poll.
        flushQueue.remove(fqe);
      }
    }

    tracker.beforeExecution();
    lock.readLock().lock();
    try {
      notifyFlushRequest(region, emergencyFlush);
      FlushResult flushResult = region.flushcache(forceFlushAllStores, false, tracker);
      boolean shouldCompact = flushResult.isCompactionNeeded();
      // We just want to check the size
      boolean shouldSplit = region.checkSplit() != null;
      if (shouldSplit) {
        this.server.compactSplitThread.requestSplit(region);
      } else if (shouldCompact) {
        server.compactSplitThread.requestSystemCompaction(region, Thread.currentThread().getName());
      }
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places. If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of wal
      // is required. Currently the only way to do this is a restart of
      // the server. Abort because hdfs is probably bad (HBASE-644 is a case
      // where hdfs was bad but passed the hdfs check).
      server.abort("Replay of WAL required. Forcing server shutdown", ex);
      return false;
    } catch (IOException ex) {
      ex = ex instanceof RemoteException ? ((RemoteException) ex).unwrapRemoteException() : ex;
      LOG.error(
        "Cache flush failed"
            + (region != null ? (" for region " +
                Bytes.toStringBinary(region.getRegionInfo().getRegionName()))
              : ""), ex);
      if (!server.checkFileSystem()) {
        return false;
      }
    } finally {
      lock.readLock().unlock();
      wakeUpIfBlocking();
      tracker.afterExecution();
    }
    return true;
  }

  private void notifyFlushRequest(Region region, boolean emergencyFlush) {
    FlushType type = null;
    if (emergencyFlush) {
      type = isAboveHighWaterMark();
      if (type == null) {
        type = isAboveLowWaterMark();
      }
    }
    for (FlushRequestListener listener : flushRequestListeners) {
      listener.flushRequested(type, region);
    }
  }

  private void wakeUpIfBlocking() {
    synchronized (blockSignal) {
      blockSignal.notifyAll();
    }
  }

  private boolean isTooManyStoreFiles(Region region) {

    // When compaction is disabled, the region is flushable
    if (!region.getTableDescriptor().isCompactionEnabled()) {
      return false;
    }

    for (Store store : region.getStores()) {
      if (store.hasTooManyStoreFiles()) {
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
    TraceScope scope = TraceUtil.createTrace("MemStoreFluser.reclaimMemStoreMemory");
    FlushType flushType = isAboveHighWaterMark();
    if (flushType != FlushType.NORMAL) {
      TraceUtil.addTimelineAnnotation("Force Flush. We're above high water mark.");
      long start = EnvironmentEdgeManager.currentTime();
      synchronized (this.blockSignal) {
        boolean blocked = false;
        long startTime = 0;
        boolean interrupted = false;
        try {
          flushType = isAboveHighWaterMark();
          while (flushType != FlushType.NORMAL && !server.isStopped()) {
            server.cacheFlusher.setFlushType(flushType);
            if (!blocked) {
              startTime = EnvironmentEdgeManager.currentTime();
              if (!server.getRegionServerAccounting().isOffheap()) {
                logMsg("global memstore heapsize",
                    server.getRegionServerAccounting().getGlobalMemStoreHeapSize(),
                    server.getRegionServerAccounting().getGlobalMemStoreLimit());
              } else {
                switch (flushType) {
                case ABOVE_OFFHEAP_HIGHER_MARK:
                  logMsg("the global offheap memstore datasize",
                      server.getRegionServerAccounting().getGlobalMemStoreOffHeapSize(),
                      server.getRegionServerAccounting().getGlobalMemStoreLimit());
                  break;
                case ABOVE_ONHEAP_HIGHER_MARK:
                  logMsg("global memstore heapsize",
                      server.getRegionServerAccounting().getGlobalMemStoreHeapSize(),
                      server.getRegionServerAccounting().getGlobalOnHeapMemStoreLimit());
                  break;
                default:
                  break;
                }
              }
            }
            blocked = true;
            wakeupFlushThread();
            try {
              // we should be able to wait forever, but we've seen a bug where
              // we miss a notify, so put a 5 second bound on it at least.
              blockSignal.wait(5 * 1000);
            } catch (InterruptedException ie) {
              LOG.warn("Interrupted while waiting");
              interrupted = true;
            }
            long took = EnvironmentEdgeManager.currentTime() - start;
            LOG.warn("Memstore is above high water mark and block " + took + "ms");
            flushType = isAboveHighWaterMark();
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }

        if(blocked){
          final long totalTime = EnvironmentEdgeManager.currentTime() - startTime;
          if(totalTime > 0){
            this.updatesBlockedMsHighWater.add(totalTime);
          }
          LOG.info("Unblocking updates for server " + server.toString());
        }
      }
    } else {
      flushType = isAboveLowWaterMark();
      if (flushType != FlushType.NORMAL) {
        server.cacheFlusher.setFlushType(flushType);
        wakeupFlushThread();
      }
    }
    if(scope!= null) {
      scope.close();
    }
  }

  private void logMsg(String string1, long val, long max) {
    LOG.info("Blocking updates on " + server.toString() + ": " + string1 + " "
        + TraditionalBinaryPrefix.long2String(val, "", 1) + " is >= than blocking "
        + TraditionalBinaryPrefix.long2String(max, "", 1) + " size");
  }

  @Override
  public String toString() {
    return "flush_queue="
        + flushQueue.size();
  }

  public String dumpQueue() {
    StringBuilder queueList = new StringBuilder();
    queueList.append("Flush Queue Queue dump:\n");
    queueList.append("  Flush Queue:\n");
    java.util.Iterator<FlushQueueEntry> it = flushQueue.iterator();

    while(it.hasNext()){
      queueList.append("    "+it.next().toString());
      queueList.append("\n");
    }

    return queueList.toString();
  }

  /**
   * Register a MemstoreFlushListener
   * @param listener
   */
  @Override
  public void registerFlushRequestListener(final FlushRequestListener listener) {
    this.flushRequestListeners.add(listener);
  }

  /**
   * Unregister the listener from MemstoreFlushListeners
   * @param listener
   * @return true when passed listener is unregistered successfully.
   */
  @Override
  public boolean unregisterFlushRequestListener(final FlushRequestListener listener) {
    return this.flushRequestListeners.remove(listener);
  }

  /**
   * Sets the global memstore limit to a new size.
   * @param globalMemStoreSize
   */
  @Override
  public void setGlobalMemStoreLimit(long globalMemStoreSize) {
    this.server.getRegionServerAccounting().setGlobalMemStoreLimits(globalMemStoreSize);
    reclaimMemStoreMemory();
  }

  interface FlushQueueEntry extends Delayed {
  }

  /**
   * Datastructure used in the flush queue.  Holds region and retry count.
   * Keeps tabs on how old this object is.  Implements {@link Delayed}.  On
   * construction, the delay is zero. When added to a delay queue, we'll come
   * out near immediately.  Call {@link #requeue(long)} passing delay in
   * milliseconds before readding to delay queue if you want it to stay there
   * a while.
   */
  static class FlushRegionEntry implements FlushQueueEntry {
    private final HRegion region;

    private final long createTime;
    private long whenToExpire;
    private int requeueCount = 0;

    private final boolean forceFlushAllStores;

    private final FlushLifeCycleTracker tracker;

    FlushRegionEntry(final HRegion r, boolean forceFlushAllStores, FlushLifeCycleTracker tracker) {
      this.region = r;
      this.createTime = EnvironmentEdgeManager.currentTime();
      this.whenToExpire = this.createTime;
      this.forceFlushAllStores = forceFlushAllStores;
      this.tracker = tracker;
    }

    /**
     * @param maximumWait
     * @return True if we have been delayed > <code>maximumWait</code> milliseconds.
     */
    public boolean isMaximumWait(final long maximumWait) {
      return (EnvironmentEdgeManager.currentTime() - this.createTime) > maximumWait;
    }

    /**
     * @return Count of times {@link #requeue(long)} was called; i.e this is
     * number of times we've been requeued.
     */
    public int getRequeueCount() {
      return this.requeueCount;
    }

    /**
     * @return whether we need to flush all stores.
     */
    public boolean isForceFlushAllStores() {
      return forceFlushAllStores;
    }

    public FlushLifeCycleTracker getTracker() {
      return tracker;
    }

    /**
     * @param when When to expire, when to come up out of the queue.
     * Specify in milliseconds.  This method adds EnvironmentEdgeManager.currentTime()
     * to whatever you pass.
     * @return This.
     */
    public FlushRegionEntry requeue(final long when) {
      this.whenToExpire = EnvironmentEdgeManager.currentTime() + when;
      this.requeueCount++;
      return this;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.whenToExpire - EnvironmentEdgeManager.currentTime(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      // Delay is compared first. If there is a tie, compare region's hash code
      int ret = Long.valueOf(getDelay(TimeUnit.MILLISECONDS) -
        other.getDelay(TimeUnit.MILLISECONDS)).intValue();
      if (ret != 0) {
        return ret;
      }
      FlushQueueEntry otherEntry = (FlushQueueEntry) other;
      return hashCode() - otherEntry.hashCode();
    }

    @Override
    public String toString() {
      return "[flush region "+Bytes.toStringBinary(region.getRegionInfo().getRegionName())+"]";
    }

    @Override
    public int hashCode() {
      int hash = (int) getDelay(TimeUnit.MILLISECONDS);
      return hash ^ region.hashCode();
    }

   @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      FlushRegionEntry other = (FlushRegionEntry) obj;
      if (!Bytes.equals(this.region.getRegionInfo().getRegionName(),
          other.region.getRegionInfo().getRegionName())) {
        return false;
      }
      return compareTo(other) == 0;
    }
  }
}
