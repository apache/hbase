/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * It mainly focus on managing reusable chunks, to make GC friendy.
 * Also, if situation needs, this class also keep tracking non-resuable chunks.
 */
@InterfaceAudience.Private
public class ChunkPool {

  private static final Log LOG = LogFactory.getLog(ChunkPool.class);
  private static final Object initLock = new Object();
  private static volatile ChunkPool globalInstance;

  private final HeapMode heapMode;
  private final int maxChunkCount;
  private final int chunkSize;
  private final ChunkAllocator chunkAllocator;
  private final AbstractHeapChunk[] chunkArray;
  private final Queue<AbstractHeapChunk> chunkQueue;
  // Map for tracking unpooled chunk's
  private final Map<Long, AbstractHeapChunk> unpooledChunks = new ConcurrentHashMap<>();

  private final AtomicLong currentChunkCounter = new AtomicLong(0);
  private final AtomicLong unpooledChunkUsed = new AtomicLong(0);

  public ChunkPool(ChunkPoolParameters parameters) {
    heapMode = parameters.heapMode;
    chunkSize = parameters.chunkSize;

    long capacity = parameters.capacity;
    maxChunkCount = (int) (capacity / this.chunkSize);
    int initialCount = parameters.initialCount;
    if (initialCount > maxChunkCount) {
      initialCount = maxChunkCount;
      parameters.initialCount = initialCount;
    }
    chunkAllocator = new ChunkAllocator(heapMode, maxChunkCount);

    chunkArray = new AbstractHeapChunk[maxChunkCount];
    chunkQueue = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < initialCount; i++) {
      if (!this.chunkQueue.offer(allocatePooledChunk())) {
        // Should not happen, since the queue has no size limit.
        throw new IllegalStateException("Initial count=" + initialCount +
          ", but failed to allocate the " + i + "th chunk.");
      }
    }
  }

  /**
   * Try to allocate a chunk with specified size.
   * @param size size of a chunk
   * @return a chunk
   */
  public AbstractHeapChunk allocate(int size) {
    AbstractHeapChunk chunk;
    if (size > chunkSize) {
      LOG.warn("Allocating a big chunk which size is " + size +
        " larger than specified size: " + chunkSize);
      return allocateUnpooledChunk(size);
    }
    chunk = chunkQueue.poll();
    if (chunk == null) {
      synchronized (chunkAllocator) {
        if (currentChunkCounter.get() >= maxChunkCount) {
          LOG.debug("No more available reusable chunk in this pool, "
            + "will use unpooled chunk on heap before pooled chunks reclaimed.");
          return allocateUnpooledChunk(chunkSize);
        }
        chunk = allocatePooledChunk();
      }
    }
    return chunk;
  }

  private AbstractHeapChunk allocateUnpooledChunk(int len) {
    // An unpooled chunk must be on heap.
    AbstractHeapChunk chunk = chunkAllocator.allocateUnpooledChunk(len);
    unpooledChunkUsed.addAndGet(len);
    unpooledChunks.put(chunk.getChunkID(), chunk);
    return chunk;
  }

  /**
   * Reclaim a chunk if it is reusable, remove it otherwise.
   * @param chunk a chunk not in use
   */
  public void reclaimChunk(AbstractHeapChunk chunk) {
    //not support putback duplicate.
    if (chunk.getHeapMode() == this.heapMode && chunk.isPooledChunk()) {
      chunk.getByteBuffer().clear();
      if (!chunkQueue.offer(chunk)) {
        // Should not happen, since the queue has no size limit.
        throw new IllegalStateException("Can't reclaim chunk");
      }
    } else {
      unpooledChunks.remove(chunk.getChunkID());
      unpooledChunkUsed.addAndGet(-chunk.getLimit());
    }
  }

  private AbstractHeapChunk allocatePooledChunk() {
    AbstractHeapChunk chunk = chunkAllocator.allocatePooledChunk(chunkSize);
    chunkArray[(int) chunk.getChunkID()] = chunk;
    currentChunkCounter.incrementAndGet();
    return chunk;
  }

  public AbstractHeapChunk getChunkByID(long chunkID) {
    return chunkID < maxChunkCount ? chunkArray[(int) chunkID] : unpooledChunks.get(chunkID);
  }

  @VisibleForTesting
  long getCurrentChunkCounter() {
    return currentChunkCounter.get();
  }

  @VisibleForTesting
  long getUnpooledChunkUsed() {
    return unpooledChunkUsed.get();
  }

  @VisibleForTesting
  Queue<AbstractHeapChunk> getChunkQueue() {
    return chunkQueue;
  }

  @VisibleForTesting
  AbstractHeapChunk[] getChunkArray() {
    return chunkArray;
  }

  @VisibleForTesting
  Map<Long, AbstractHeapChunk> getUnpooledChunksMap() {
    return unpooledChunks;
  }

  @VisibleForTesting
  int getMaxChunkCount() {
    return maxChunkCount;
  }

  public static ChunkPool initialize(Configuration conf) {
    if (globalInstance != null) {
      return globalInstance;
    }
    synchronized (initLock) {
      if (globalInstance == null) {
        ChunkPoolParameters parameters = new ChunkPoolParameters(conf);
        globalInstance = new ChunkPool(parameters);
        LOG.info("CCSMapMemstore's chunkPool initialized with " + parameters);
      }
    }
    return globalInstance;
  }

  static class ChunkPoolParameters {
    long capacity;
    int chunkSize;
    int initialCount;
    HeapMode heapMode;

    ChunkPoolParameters(Configuration conf) {
      capacity = conf.getLong(CCSMapUtils.CHUNK_CAPACITY_KEY, Long.MIN_VALUE);
      chunkSize = conf.getInt(CCSMapUtils.CHUNK_SIZE_KEY, Integer.MIN_VALUE);
      initialCount = conf.getInt(CCSMapUtils.INITIAL_CHUNK_COUNT_KEY, Integer.MIN_VALUE);
      heapMode = conf.getBoolean(CCSMapUtils.USE_OFFHEAP, false) ?
        HeapMode.OFF_HEAP : HeapMode.ON_HEAP;
    }

    @Override
    public String toString() {
      return "ChunkPoolParameters{" + "capacity=" + capacity + ", chunkSize=" + chunkSize
        + ", initialCount=" + initialCount + ", heapMode=" + heapMode + '}';
    }
  }

}
