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
package org.apache.hadoop.hbase.master.region;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * As long as there is no RegionServerServices for a master local region, we need implement the
 * flush and compaction logic by our own.
 * <p/>
 * The flush logic is very simple, every time after calling a modification method in
 * {@link RegionProcedureStore}, we will call the {@link #onUpdate()} method below, and in this
 * method, we will check the memstore size and if it is above the flush size, we will call
 * {@link HRegion#flush(boolean)} to force flush all stores.
 * <p/>
 * And for compaction, the logic is also very simple. After flush, we will check the store file
 * count, if it is above the compactMin, we will do a major compaction.
 */
@InterfaceAudience.Private
class MasterRegionFlusherAndCompactor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegionFlusherAndCompactor.class);

  private final Configuration conf;

  private final Abortable abortable;

  private final HRegion region;

  // as we can only count this outside the region's write/flush process so it is not accurate, but
  // it is enough.
  private final AtomicLong changesAfterLastFlush = new AtomicLong(0);

  private final long flushSize;

  private final long flushPerChanges;

  private final long flushIntervalMs;

  private final int compactMin;

  private final Path globalArchivePath;

  private final String archivedHFileSuffix;

  private final Thread flushThread;

  private final Lock flushLock = new ReentrantLock();

  private final Condition flushCond = flushLock.newCondition();

  private boolean flushRequest = false;

  private long lastFlushTime;

  private final ExecutorService compactExecutor;

  private final Lock compactLock = new ReentrantLock();

  private boolean compactRequest = false;

  private volatile boolean closed = false;

  MasterRegionFlusherAndCompactor(Configuration conf, Abortable abortable, HRegion region,
    long flushSize, long flushPerChanges, long flushIntervalMs, int compactMin,
    Path globalArchivePath, String archivedHFileSuffix) {
    this.conf = conf;
    this.abortable = abortable;
    this.region = region;
    this.flushSize = flushSize;
    this.flushPerChanges = flushPerChanges;
    this.flushIntervalMs = flushIntervalMs;
    this.compactMin = compactMin;
    this.globalArchivePath = globalArchivePath;
    this.archivedHFileSuffix = archivedHFileSuffix;
    flushThread = new Thread(this::flushLoop, region.getRegionInfo().getTable() + "-Flusher");
    flushThread.setDaemon(true);
    flushThread.start();
    compactExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setNameFormat(region.getRegionInfo().getTable() + "-Store-Compactor").setDaemon(true)
      .build());
    LOG.info("Constructor flushSize={}, flushPerChanges={}, flushIntervalMs={}, compactMin={}",
      flushSize, flushPerChanges, flushIntervalMs, compactMin);
  }

  // inject our flush related configurations
  static void setupConf(Configuration conf, long flushSize, long flushPerChanges,
    long flushIntervalMs) {
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSize);
    conf.setLong(HRegion.MEMSTORE_FLUSH_PER_CHANGES, flushPerChanges);
    conf.setLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, flushIntervalMs);
    LOG.info("Injected flushSize={}, flushPerChanges={}, flushIntervalMs={}", flushSize,
      flushPerChanges, flushIntervalMs);
  }

  private void moveHFileToGlobalArchiveDir() throws IOException {
    FileSystem fs = region.getRegionFileSystem().getFileSystem();
    for (HStore store : region.getStores()) {
      store.closeAndArchiveCompactedFiles();
      Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, region.getRegionInfo(),
        store.getColumnFamilyDescriptor().getName());
      Path globalStoreArchiveDir = HFileArchiveUtil.getStoreArchivePathForArchivePath(
        globalArchivePath, region.getRegionInfo(), store.getColumnFamilyDescriptor().getName());
      try {
        MasterRegionUtils.moveFilesUnderDir(fs, storeArchiveDir, globalStoreArchiveDir,
          archivedHFileSuffix);
      } catch (IOException e) {
        LOG.warn("Failed to move archived hfiles from {} to global dir {}", storeArchiveDir,
          globalStoreArchiveDir, e);
      }
    }
  }

  private void compact() {
    try {
      region.compact(true);
      moveHFileToGlobalArchiveDir();
    } catch (IOException e) {
      LOG.error("Failed to compact master local region", e);
    }
    compactLock.lock();
    try {
      if (needCompaction()) {
        compactExecutor.execute(this::compact);
      } else {
        compactRequest = false;
      }
    } finally {
      compactLock.unlock();
    }
  }

  private boolean needCompaction() {
    for (Store store : region.getStores()) {
      if (store.getStorefilesCount() >= compactMin) {
        return true;
      }
    }
    return false;
  }

  private void flushLoop() {
    lastFlushTime = EnvironmentEdgeManager.currentTime();
    while (!closed) {
      flushLock.lock();
      try {
        while (!flushRequest) {
          long waitTimeMs = lastFlushTime + flushIntervalMs - EnvironmentEdgeManager.currentTime();
          if (waitTimeMs <= 0) {
            flushRequest = true;
            break;
          }
          flushCond.await(waitTimeMs, TimeUnit.MILLISECONDS);
          if (closed) {
            return;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        continue;
      } finally {
        flushLock.unlock();
      }
      assert flushRequest;
      changesAfterLastFlush.set(0);
      try {
        region.flush(true);
        lastFlushTime = EnvironmentEdgeManager.currentTime();
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to flush master local region, aborting...", e);
        abortable.abort("Failed to flush master local region", e);
        return;
      }
      compactLock.lock();
      try {
        if (!compactRequest && needCompaction()) {
          compactRequest = true;
          compactExecutor.execute(this::compact);
        }
      } finally {
        compactLock.unlock();
      }
      flushLock.lock();
      try {
        // reset the flushRequest flag
        if (!shouldFlush(changesAfterLastFlush.get())) {
          flushRequest = false;
        }
      } finally {
        flushLock.unlock();
      }
    }
  }

  private boolean shouldFlush(long changes) {
    long heapSize = region.getMemStoreHeapSize();
    long offHeapSize = region.getMemStoreOffHeapSize();
    boolean flush = heapSize + offHeapSize >= flushSize || changes > flushPerChanges;
    if (flush && LOG.isTraceEnabled()) {
      LOG.trace("shouldFlush totalMemStoreSize={}, flushSize={}, changes={}, flushPerChanges={}",
        heapSize + offHeapSize, flushSize, changes, flushPerChanges);
    }
    return flush;
  }

  void onUpdate() {
    long changes = changesAfterLastFlush.incrementAndGet();
    if (shouldFlush(changes)) {
      requestFlush();
    }
  }

  void requestFlush() {
    flushLock.lock();
    try {
      if (flushRequest) {
        return;
      }
      flushRequest = true;
      flushCond.signalAll();
    } finally {
      flushLock.unlock();
    }
  }

  @Override
  public void close() {
    closed = true;
    flushThread.interrupt();
    compactExecutor.shutdown();
  }
}
