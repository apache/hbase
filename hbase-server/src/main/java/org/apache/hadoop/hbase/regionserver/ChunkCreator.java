
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
package org.apache.hadoop.hbase.regionserver;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.HeapMemoryTuneObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Does the management of memstoreLAB chunk creations. A monotonically incrementing id is associated
 * with every chunk
 */
@InterfaceAudience.Private
public class ChunkCreator {
  private static final Log LOG = LogFactory.getLog(ChunkCreator.class);
  // monotonically increasing chunkid
  private AtomicInteger chunkID = new AtomicInteger(1);
  // maps the chunk against the monotonically increasing chunk id. We need to preserve the
  // natural ordering of the key
  // CellChunkMap creation should convert the weak ref to hard reference

  // chunk id of each chunk is the first integer written on each chunk,
  // the header size need to be changed in case chunk id size is changed
  public static final int SIZEOF_CHUNK_HEADER = Bytes.SIZEOF_INT;

  // mapping from chunk IDs to chunks
  private Map<Integer, Chunk> chunkIdMap = new ConcurrentHashMap<Integer, Chunk>();

  private final int chunkSize;
  private final boolean offheap;
  @VisibleForTesting
  static ChunkCreator INSTANCE;
  @VisibleForTesting
  static boolean chunkPoolDisabled = false;
  private MemStoreChunkPool pool;

  @VisibleForTesting
  ChunkCreator(int chunkSize, boolean offheap, long globalMemStoreSize, float poolSizePercentage,
      float initialCountPercentage, HeapMemoryManager heapMemoryManager) {
    this.chunkSize = chunkSize;
    this.offheap = offheap;
    this.pool = initializePool(globalMemStoreSize, poolSizePercentage, initialCountPercentage);
    if (heapMemoryManager != null && this.pool != null) {
      // Register with Heap Memory manager
      heapMemoryManager.registerTuneObserver(this.pool);
    }
  }

  /**
   * Initializes the instance of ChunkCreator
   * @param chunkSize the chunkSize
   * @param offheap indicates if the chunk is to be created offheap or not
   * @param globalMemStoreSize  the global memstore size
   * @param poolSizePercentage pool size percentage
   * @param initialCountPercentage the initial count of the chunk pool if any
   * @param heapMemoryManager the heapmemory manager
   * @return singleton MSLABChunkCreator
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "LI_LAZY_INIT_STATIC",
      justification = "Method is called by single thread at the starting of RS")
  @VisibleForTesting
  public static ChunkCreator initialize(int chunkSize, boolean offheap, long globalMemStoreSize,
      float poolSizePercentage, float initialCountPercentage, HeapMemoryManager heapMemoryManager) {
    if (INSTANCE != null) return INSTANCE;
    INSTANCE = new ChunkCreator(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
        initialCountPercentage, heapMemoryManager);
    return INSTANCE;
  }

  static ChunkCreator getInstance() {
    return INSTANCE;
  }

  /**
   * Creates and inits a chunk. The default implementation.
   * @return the chunk that was initialized
   */
  Chunk getChunk() {
    return getChunk(CompactingMemStore.IndexType.ARRAY_MAP);
  }

  /**
   * Creates and inits a chunk.
   * @return the chunk that was initialized
   * @param chunkIndexType whether the requested chunk is going to be used with CellChunkMap index
   */
  Chunk getChunk(CompactingMemStore.IndexType chunkIndexType) {
    Chunk chunk = null;
    if (pool != null) {
      //  the pool creates the chunk internally. The chunk#init() call happens here
      chunk = this.pool.getChunk();
      // the pool has run out of maxCount
      if (chunk == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("The chunk pool is full. Reached maxCount= " + this.pool.getMaxCount()
              + ". Creating chunk onheap.");
        }
      }
    }
    if (chunk == null) {
      // the second boolean parameter means:
      // if CellChunkMap index is requested, put allocated on demand chunk mapping into chunkIdMap
      chunk = createChunk(false, chunkIndexType);
    }

    // now we need to actually do the expensive memory allocation step in case of a new chunk,
    // else only the offset is set to the beginning of the chunk to accept allocations
    chunk.init();
    return chunk;
  }

  /**
   * Creates the chunk either onheap or offheap
   * @param pool indicates if the chunks have to be created which will be used by the Pool
   * @param chunkIndexType
   * @return the chunk
   */
  private Chunk createChunk(boolean pool, CompactingMemStore.IndexType chunkIndexType) {
    Chunk chunk = null;
    int id = chunkID.getAndIncrement();
    assert id > 0;
    // do not create offheap chunk on demand
    if (pool && this.offheap) {
      chunk = new OffheapChunk(chunkSize, id, pool);
    } else {
      chunk = new OnheapChunk(chunkSize, id, pool);
    }
    if (pool || (chunkIndexType == CompactingMemStore.IndexType.CHUNK_MAP)) {
      // put the pool chunk into the chunkIdMap so it is not GC-ed
      this.chunkIdMap.put(chunk.getId(), chunk);
    }
    return chunk;
  }

  @VisibleForTesting
  // Used to translate the ChunkID into a chunk ref
  Chunk getChunk(int id) {
    // can return null if chunk was never mapped
    return chunkIdMap.get(id);
  }

  int getChunkSize() {
    return this.chunkSize;
  }

  boolean isOffheap() {
    return this.offheap;
  }

  private void removeChunks(Set<Integer> chunkIDs) {
    this.chunkIdMap.keySet().removeAll(chunkIDs);
  }

  Chunk removeChunk(int chunkId) {
    return this.chunkIdMap.remove(chunkId);
  }

  @VisibleForTesting
  // the chunks in the chunkIdMap may already be released so we shouldn't relay
  // on this counting for strong correctness. This method is used only in testing.
  int numberOfMappedChunks() {
    return this.chunkIdMap.size();
  }

  @VisibleForTesting
  void clearChunkIds() {
    this.chunkIdMap.clear();
  }

  /**
   * A pool of {@link Chunk} instances.
   *
   * MemStoreChunkPool caches a number of retired chunks for reusing, it could
   * decrease allocating bytes when writing, thereby optimizing the garbage
   * collection on JVM.
   */
  private  class MemStoreChunkPool implements HeapMemoryTuneObserver {
    private int maxCount;

    // A queue of reclaimed chunks
    private final BlockingQueue<Chunk> reclaimedChunks;
    private final float poolSizePercentage;

    /** Statistics thread schedule pool */
    private final ScheduledExecutorService scheduleThreadPool;
    /** Statistics thread */
    private static final int statThreadPeriod = 60 * 5;
    private final AtomicLong chunkCount = new AtomicLong();
    private final AtomicLong reusedChunkCount = new AtomicLong();

    MemStoreChunkPool(int maxCount, int initialCount, float poolSizePercentage) {
      this.maxCount = maxCount;
      this.poolSizePercentage = poolSizePercentage;
      this.reclaimedChunks = new LinkedBlockingQueue<>();
      for (int i = 0; i < initialCount; i++) {
        // Chunks from pool are covered with strong references anyway
        // TODO: change to CHUNK_MAP if it is generally defined
        Chunk chunk = createChunk(true, CompactingMemStore.IndexType.ARRAY_MAP);
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
     * @see #putbackChunks(Chunk)
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
            if (this.chunkCount.compareAndSet(created, created + 1)) {
              // TODO: change to CHUNK_MAP if it is generally defined
              chunk = createChunk(true, CompactingMemStore.IndexType.ARRAY_MAP);
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
     * Add the chunks to the pool, when the pool achieves the max size, it will skip the remaining
     * chunks
     * @param c
     */
    private void putbackChunks(Chunk c) {
      int toAdd = this.maxCount - reclaimedChunks.size();
      if (c.isFromPool() && toAdd > 0) {
        reclaimedChunks.add(c);
      } else {
        // remove the chunk (that is not going to pool)
        // though it is initially from the pool or not
        ChunkCreator.this.removeChunk(c.getId());
      }
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

    private int getMaxCount() {
      return this.maxCount;
    }

    @Override
    public void onHeapMemoryTune(long newMemstoreSize, long newBlockCacheSize) {
      // don't do any tuning in case of offheap memstore
      if (isOffheap()) {
        LOG.warn("Not tuning the chunk pool as it is offheap");
        return;
      }
      int newMaxCount =
          (int) (newMemstoreSize * poolSizePercentage / getChunkSize());
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

  @VisibleForTesting
  static void clearDisableFlag() {
    chunkPoolDisabled = false;
  }

  private MemStoreChunkPool initializePool(long globalMemStoreSize, float poolSizePercentage,
      float initialCountPercentage) {
    if (poolSizePercentage <= 0) {
      LOG.info("PoolSizePercentage is less than 0. So not using pool");
      return null;
    }
    if (chunkPoolDisabled) {
      return null;
    }
    if (poolSizePercentage > 1.0) {
      throw new IllegalArgumentException(
          MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int maxCount = (int) (globalMemStoreSize * poolSizePercentage / getChunkSize());
    if (initialCountPercentage > 1.0 || initialCountPercentage < 0) {
      throw new IllegalArgumentException(
          MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int initialCount = (int) (initialCountPercentage * maxCount);
    LOG.info("Allocating MemStoreChunkPool with chunk size "
        + StringUtils.byteDesc(getChunkSize()) + ", max count " + maxCount
        + ", initial count " + initialCount);
    return new MemStoreChunkPool(maxCount, initialCount, poolSizePercentage);
  }

  @VisibleForTesting
  int getMaxCount() {
    if (pool != null) {
      return pool.getMaxCount();
    }
    return 0;
  }

  @VisibleForTesting
  int getPoolSize() {
    if (pool != null) {
      return pool.reclaimedChunks.size();
    }
    return 0;
  }

  @VisibleForTesting
  boolean isChunkInPool(int chunkId) {
    if (pool != null) {
      // chunks that are from pool will return true chunk reference not null
      Chunk c = getChunk(chunkId);
      if (c==null) {
        return false;
      }
      return pool.reclaimedChunks.contains(c);
    }

    return false;
  }

  /*
   * Only used in testing
   */
  @VisibleForTesting
  void clearChunksInPool() {
    if (pool != null) {
      pool.reclaimedChunks.clear();
    }
  }

  synchronized void putbackChunks(Set<Integer> chunks) {
    // if there is no pool just try to clear the chunkIdMap in case there is something
    if ( pool == null ) {
      this.removeChunks(chunks);
      return;
    }

    // if there is pool, go over all chunk IDs that came back, the chunks may be from pool or not
    for (int chunkID : chunks) {
      // translate chunk ID to chunk, if chunk initially wasn't in pool
      // this translation will (most likely) return null
      Chunk chunk = ChunkCreator.this.getChunk(chunkID);
      if (chunk != null) {
        pool.putbackChunks(chunk);
      }
      // if chunk is null, it was never covered by the chunkIdMap (and so wasn't in pool also),
      // so we have nothing to do on its release
    }
    return;
  }

}
