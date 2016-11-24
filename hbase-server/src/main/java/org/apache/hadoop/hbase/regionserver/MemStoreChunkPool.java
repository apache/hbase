/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.HeapMemoryTuneObserver;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A pool of {@link Chunk} instances.
 * 
 * MemStoreChunkPool caches a number of retired chunks for reusing, it could
 * decrease allocating bytes when writing, thereby optimizing the garbage
 * collection on JVM.
 * 
 * The pool instance is globally unique and could be obtained through
 * {@link MemStoreChunkPool#initialize(long, float, float, int, boolean)}
 * 
 * {@link MemStoreChunkPool#getChunk()} is called when MemStoreLAB allocating
 * bytes, and {@link MemStoreChunkPool#putbackChunks(BlockingQueue)} is called
 * when MemStore clearing snapshot for flush
 */
@SuppressWarnings("javadoc")
@InterfaceAudience.Private
public class MemStoreChunkPool implements HeapMemoryTuneObserver {
  private static final Log LOG = LogFactory.getLog(MemStoreChunkPool.class);

  // Static reference to the MemStoreChunkPool
  static MemStoreChunkPool GLOBAL_INSTANCE;
  /** Boolean whether we have disabled the memstore chunk pool entirely. */
  static boolean chunkPoolDisabled = false;

  private int maxCount;

  // A queue of reclaimed chunks
  private final BlockingQueue<Chunk> reclaimedChunks;
  private final int chunkSize;
  private final float poolSizePercentage;

  /** Statistics thread schedule pool */
  private final ScheduledExecutorService scheduleThreadPool;
  /** Statistics thread */
  private static final int statThreadPeriod = 60 * 5;
  private final AtomicLong chunkCount = new AtomicLong();
  private final AtomicLong reusedChunkCount = new AtomicLong();
  private final boolean offheap;

  MemStoreChunkPool(int chunkSize, int maxCount, int initialCount, float poolSizePercentage,
      boolean offheap) {
    this.maxCount = maxCount;
    this.chunkSize = chunkSize;
    this.poolSizePercentage = poolSizePercentage;
    this.offheap = offheap;
    this.reclaimedChunks = new LinkedBlockingQueue<>();
    for (int i = 0; i < initialCount; i++) {
      Chunk chunk = this.offheap ? new OffheapChunk(chunkSize) : new OnheapChunk(chunkSize);
      chunk.init();
      reclaimedChunks.add(chunk);
    }
    chunkCount.set(initialCount);
    final String n = Thread.currentThread().getName();
    scheduleThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
        .setNameFormat(n + "-MemStoreChunkPool Statistics").setDaemon(true).build());
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(), statThreadPeriod,
        statThreadPeriod, TimeUnit.SECONDS);
  }

  /**
   * Poll a chunk from the pool, reset it if not null, else create a new chunk to return if we have
   * not yet created max allowed chunks count. When we have already created max allowed chunks and
   * no free chunks as of now, return null. It is the responsibility of the caller to make a chunk
   * then.
   * Note: Chunks returned by this pool must be put back to the pool after its use.
   * @return a chunk
   * @see #putbackChunk(Chunk)
   * @see #putbackChunks(BlockingQueue)
   */
  Chunk getChunk() {
    Chunk chunk = reclaimedChunks.poll();
    if (chunk != null) {
      chunk.reset();
      reusedChunkCount.incrementAndGet();
    } else {
      // Make a chunk iff we have not yet created the maxCount chunks
      while (true) {
        long created = this.chunkCount.get();
        if (created < this.maxCount) {
          chunk = this.offheap ? new OffheapChunk(this.chunkSize) : new OnheapChunk(this.chunkSize);
          if (this.chunkCount.compareAndSet(created, created + 1)) {
            break;
          }
        } else {
          break;
        }
      }
    }
    return chunk;
  }

  /**
   * Add the chunks to the pool, when the pool achieves the max size, it will
   * skip the remaining chunks
   * @param chunks
   */
  synchronized void putbackChunks(BlockingQueue<Chunk> chunks) {
    int toAdd = Math.min(chunks.size(), this.maxCount - reclaimedChunks.size());
    Chunk chunk = null;
    while ((chunk = chunks.poll()) != null && toAdd > 0) {
      reclaimedChunks.add(chunk);
      toAdd--;
    }
  }

  /**
   * Add the chunk to the pool, if the pool has achieved the max size, it will
   * skip it
   * @param chunk
   */
  synchronized void putbackChunk(Chunk chunk) {
    if (reclaimedChunks.size() < this.maxCount) {
      reclaimedChunks.add(chunk);
    }
  }

  int getPoolSize() {
    return this.reclaimedChunks.size();
  }

  /*
   * Only used in testing
   */
  void clearChunks() {
    this.reclaimedChunks.clear();
  }

  private class StatisticsThread extends Thread {
    StatisticsThread() {
      super("MemStoreChunkPool.StatisticsThread");
      setDaemon(true);
    }

    @Override
    public void run() {
      logStats();
    }

    private void logStats() {
      if (!LOG.isDebugEnabled()) return;
      long created = chunkCount.get();
      long reused = reusedChunkCount.get();
      long total = created + reused;
      LOG.debug("Stats: current pool size=" + reclaimedChunks.size()
          + ",created chunk count=" + created
          + ",reused chunk count=" + reused
          + ",reuseRatio=" + (total == 0 ? "0" : StringUtils.formatPercent(
              (float) reused / (float) total, 2)));
    }
  }

  /**
   * @return the global MemStoreChunkPool instance
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "LI_LAZY_INIT_STATIC",
      justification = "Method is called by single thread at the starting of RS")
  static MemStoreChunkPool initialize(long globalMemStoreSize, float poolSizePercentage,
      float initialCountPercentage, int chunkSize, boolean offheap) {
    if (GLOBAL_INSTANCE != null) return GLOBAL_INSTANCE;
    if (chunkPoolDisabled) return null;

    if (poolSizePercentage <= 0) {
      chunkPoolDisabled = true;
      return null;
    }
    if (poolSizePercentage > 1.0) {
      throw new IllegalArgumentException(
          MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int maxCount = (int) (globalMemStoreSize * poolSizePercentage / chunkSize);
    if (initialCountPercentage > 1.0 || initialCountPercentage < 0) {
      throw new IllegalArgumentException(
          MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int initialCount = (int) (initialCountPercentage * maxCount);
    LOG.info("Allocating MemStoreChunkPool with chunk size " + StringUtils.byteDesc(chunkSize)
        + ", max count " + maxCount + ", initial count " + initialCount);
    GLOBAL_INSTANCE = new MemStoreChunkPool(chunkSize, maxCount, initialCount, poolSizePercentage,
        offheap);
    return GLOBAL_INSTANCE;
  }

  /**
   * @return The singleton instance of this pool.
   */
  static MemStoreChunkPool getPool() {
    return GLOBAL_INSTANCE;
  }

  int getMaxCount() {
    return this.maxCount;
  }

  @VisibleForTesting
  static void clearDisableFlag() {
    chunkPoolDisabled = false;
  }

  @Override
  public void onHeapMemoryTune(long newMemstoreSize, long newBlockCacheSize) {
    int newMaxCount = (int) (newMemstoreSize * poolSizePercentage / chunkSize);
    if (newMaxCount != this.maxCount) {
      // We need an adjustment in the chunks numbers
      if (newMaxCount > this.maxCount) {
        // Max chunks getting increased. Just change the variable. Later calls to getChunk() would
        // create and add them to Q
        LOG.info("Max count for chunks increased from " + this.maxCount + " to " + newMaxCount);
        this.maxCount = newMaxCount;
      } else {
        // Max chunks getting decreased. We may need to clear off some of the pooled chunks now
        // itself. If the extra chunks are serving already, do not pool those when we get them back
        LOG.info("Max count for chunks decreased from " + this.maxCount + " to " + newMaxCount);
        this.maxCount = newMaxCount;
        if (this.reclaimedChunks.size() > newMaxCount) {
          synchronized (this) {
            while (this.reclaimedChunks.size() > newMaxCount) {
              this.reclaimedChunks.poll();
            }
          }
        }
      }
    }
  }
}
