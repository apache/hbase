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

import java.lang.management.ManagementFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.HeapMemStoreLAB.Chunk;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A pool of {@link HeapMemStoreLAB$Chunk} instances.
 * 
 * MemStoreChunkPool caches a number of retired chunks for reusing, it could
 * decrease allocating bytes when writing, thereby optimizing the garbage
 * collection on JVM.
 * 
 * The pool instance is globally unique and could be obtained through
 * {@link MemStoreChunkPool#getPool(Configuration)}
 * 
 * {@link MemStoreChunkPool#getChunk()} is called when MemStoreLAB allocating
 * bytes, and {@link MemStoreChunkPool#putbackChunks(BlockingQueue)} is called
 * when MemStore clearing snapshot for flush
 */
@SuppressWarnings("javadoc")
@InterfaceAudience.Private
public class MemStoreChunkPool {
  private static final Log LOG = LogFactory.getLog(MemStoreChunkPool.class);
  final static String CHUNK_POOL_MAXSIZE_KEY = "hbase.hregion.memstore.chunkpool.maxsize";
  final static String CHUNK_POOL_INITIALSIZE_KEY = "hbase.hregion.memstore.chunkpool.initialsize";
  final static float POOL_MAX_SIZE_DEFAULT = 0.0f;
  final static float POOL_INITIAL_SIZE_DEFAULT = 0.0f;

  // Static reference to the MemStoreChunkPool
  private static MemStoreChunkPool globalInstance;
  /** Boolean whether we have disabled the memstore chunk pool entirely. */
  static boolean chunkPoolDisabled = false;

  private final int maxCount;

  // A queue of reclaimed chunks
  private final BlockingQueue<Chunk> reclaimedChunks;
  private final int chunkSize;

  /** Statistics thread schedule pool */
  private final ScheduledExecutorService scheduleThreadPool;
  /** Statistics thread */
  private static final int statThreadPeriod = 60 * 5;
  private AtomicLong createdChunkCount = new AtomicLong();
  private AtomicLong reusedChunkCount = new AtomicLong();

  MemStoreChunkPool(Configuration conf, int chunkSize, int maxCount,
      int initialCount) {
    this.maxCount = maxCount;
    this.chunkSize = chunkSize;
    this.reclaimedChunks = new LinkedBlockingQueue<Chunk>();
    for (int i = 0; i < initialCount; i++) {
      Chunk chunk = new Chunk(chunkSize);
      chunk.init();
      reclaimedChunks.add(chunk);
    }
    final String n = Thread.currentThread().getName();
    scheduleThreadPool = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat(n+"-MemStoreChunkPool Statistics")
            .setDaemon(true).build());
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this),
        statThreadPeriod, statThreadPeriod, TimeUnit.SECONDS);
  }

  /**
   * Poll a chunk from the pool, reset it if not null, else create a new chunk
   * to return
   * @return a chunk
   */
  Chunk getChunk() {
    Chunk chunk = reclaimedChunks.poll();
    if (chunk == null) {
      chunk = new Chunk(chunkSize);
      createdChunkCount.incrementAndGet();
    } else {
      chunk.reset();
      reusedChunkCount.incrementAndGet();
    }
    return chunk;
  }

  /**
   * Add the chunks to the pool, when the pool achieves the max size, it will
   * skip the remaining chunks
   * @param chunks
   */
  void putbackChunks(BlockingQueue<Chunk> chunks) {
    int maxNumToPutback = this.maxCount - reclaimedChunks.size();
    if (maxNumToPutback <= 0) {
      return;
    }
    chunks.drainTo(reclaimedChunks, maxNumToPutback);
    // clear reference of any non-reclaimable chunks
    if (chunks.size() > 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Left " + chunks.size() + " unreclaimable chunks, removing them from queue");
      }
      chunks.clear();
    }
  }

  /**
   * Add the chunk to the pool, if the pool has achieved the max size, it will
   * skip it
   * @param chunk
   */
  void putbackChunk(Chunk chunk) {
    if (reclaimedChunks.size() >= this.maxCount) {
      return;
    }
    reclaimedChunks.add(chunk);
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

  private static class StatisticsThread extends Thread {
    MemStoreChunkPool mcp;

    public StatisticsThread(MemStoreChunkPool mcp) {
      super("MemStoreChunkPool.StatisticsThread");
      setDaemon(true);
      this.mcp = mcp;
    }

    @Override
    public void run() {
      mcp.logStats();
    }
  }

  private void logStats() {
    if (!LOG.isDebugEnabled()) return;
    long created = createdChunkCount.get();
    long reused = reusedChunkCount.get();
    long total = created + reused;
    LOG.debug("Stats: current pool size=" + reclaimedChunks.size()
        + ",created chunk count=" + created
        + ",reused chunk count=" + reused
        + ",reuseRatio=" + (total == 0 ? "0" : StringUtils.formatPercent(
            (float) reused / (float) total, 2)));
  }

  /**
   * @param conf
   * @return the global MemStoreChunkPool instance
   */
  static MemStoreChunkPool getPool(Configuration conf) {
    if (globalInstance != null) return globalInstance;

    synchronized (MemStoreChunkPool.class) {
      if (chunkPoolDisabled) return null;
      if (globalInstance != null) return globalInstance;
      float poolSizePercentage = conf.getFloat(CHUNK_POOL_MAXSIZE_KEY, POOL_MAX_SIZE_DEFAULT);
      if (poolSizePercentage <= 0) {
        chunkPoolDisabled = true;
        return null;
      }
      if (poolSizePercentage > 1.0) {
        throw new IllegalArgumentException(CHUNK_POOL_MAXSIZE_KEY + " must be between 0.0 and 1.0");
      }
      long heapMax = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
      long globalMemStoreLimit = (long) (heapMax * HeapMemorySizeUtil.getGlobalMemStorePercent(conf,
          false));
      int chunkSize = conf.getInt(HeapMemStoreLAB.CHUNK_SIZE_KEY,
          HeapMemStoreLAB.CHUNK_SIZE_DEFAULT);
      int maxCount = (int) (globalMemStoreLimit * poolSizePercentage / chunkSize);

      float initialCountPercentage = conf.getFloat(CHUNK_POOL_INITIALSIZE_KEY,
          POOL_INITIAL_SIZE_DEFAULT);
      if (initialCountPercentage > 1.0 || initialCountPercentage < 0) {
        throw new IllegalArgumentException(CHUNK_POOL_INITIALSIZE_KEY
            + " must be between 0.0 and 1.0");
      }

      int initialCount = (int) (initialCountPercentage * maxCount);
      LOG.info("Allocating MemStoreChunkPool with chunk size " + StringUtils.byteDesc(chunkSize)
          + ", max count " + maxCount + ", initial count " + initialCount);
      globalInstance = new MemStoreChunkPool(conf, chunkSize, maxCount, initialCount);
      return globalInstance;
    }
  }

  int getMaxCount() {
    return this.maxCount;
  }

  @VisibleForTesting
  static void clearDisableFlag() {
    chunkPoolDisabled = false;
  }

}
