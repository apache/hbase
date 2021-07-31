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
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.HeapMemoryTuneObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Does the management of memstoreLAB chunk creations. A monotonically incrementing id is associated
 * with every chunk
 */
@InterfaceAudience.Private
public class ChunkCreator {
  private static final Logger LOG = LoggerFactory.getLogger(ChunkCreator.class);
  // monotonically increasing chunkid. Starts at 1.
  private AtomicInteger chunkID = new AtomicInteger(1);
  // maps the chunk against the monotonically increasing chunk id. We need to preserve the
  // natural ordering of the key
  // CellChunkMap creation should convert the weak ref to hard reference

  // chunk id of each chunk is the first integer written on each chunk,
  // the header size need to be changed in case chunk id size is changed
  public static final int SIZEOF_CHUNK_HEADER = Bytes.SIZEOF_INT;

  /**
   * Types of chunks, based on their sizes
   */
  public enum ChunkType {
    // An index chunk is a small chunk, allocated from the index chunks pool.
    // Its size is fixed and is 10% of the size of a data chunk.
    INDEX_CHUNK,
    // A data chunk is a regular chunk, allocated from the data chunks pool.
    // Its size is fixed and given as input to the ChunkCreator c'tor.
    DATA_CHUNK,
    // A jumbo chunk isn't allocated from pool. Its size is bigger than the size of a
    // data chunk, and is determined per chunk (meaning, there is no fixed jumbo size).
    JUMBO_CHUNK
  }

  // mapping from chunk IDs to chunks
  private Map<Integer, Chunk> chunkIdMap = new ConcurrentHashMap<Integer, Chunk>();

  private final boolean offheap;
  static ChunkCreator instance;
  static boolean chunkPoolDisabled = false;
  private MemStoreChunkPool dataChunksPool;
  private final int chunkSize;
  private int indexChunkSize;
  private MemStoreChunkPool indexChunksPool;

  ChunkCreator(int chunkSize, boolean offheap, long globalMemStoreSize, float poolSizePercentage,
               float initialCountPercentage, HeapMemoryManager heapMemoryManager,
               float indexChunkSizePercentage) {
    this.offheap = offheap;
    this.chunkSize = chunkSize; // in case pools are not allocated
    initializePools(chunkSize, globalMemStoreSize, poolSizePercentage, indexChunkSizePercentage,
            initialCountPercentage, heapMemoryManager);
  }

  private void initializePools(int chunkSize, long globalMemStoreSize,
                               float poolSizePercentage, float indexChunkSizePercentage,
                               float initialCountPercentage,
                               HeapMemoryManager heapMemoryManager) {
    this.dataChunksPool = initializePool("data", globalMemStoreSize,
            (1 - indexChunkSizePercentage) * poolSizePercentage,
      initialCountPercentage, chunkSize, ChunkType.DATA_CHUNK, heapMemoryManager);
    // The index chunks pool is needed only when the index type is CCM.
    // Since the pools are not created at all when the index type isn't CCM,
    // we don't need to check it here.
    this.indexChunkSize = (int) (indexChunkSizePercentage * chunkSize);
    this.indexChunksPool = initializePool("index", globalMemStoreSize,
            indexChunkSizePercentage * poolSizePercentage,
      initialCountPercentage, this.indexChunkSize, ChunkType.INDEX_CHUNK,
            heapMemoryManager);
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
  public static ChunkCreator initialize(int chunkSize, boolean offheap, long globalMemStoreSize,
                                        float poolSizePercentage, float initialCountPercentage,
                                        HeapMemoryManager heapMemoryManager,
                                        float indexChunkSizePercent) {
    if (instance != null) {
      return instance;
    }
    instance = new ChunkCreator(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
            initialCountPercentage, heapMemoryManager, indexChunkSizePercent);
    return instance;
  }

  public static ChunkCreator getInstance() {
    return instance;
  }

  /**
   * Creates and inits a chunk. The default implementation for a specific chunk size.
   * @return the chunk that was initialized
   */
  Chunk getChunk(ChunkType chunkType) {
    return getChunk(CompactingMemStore.IndexType.ARRAY_MAP, chunkType);
  }

  /**
   * Creates and inits a chunk. The default implementation.
   * @return the chunk that was initialized
   */
  Chunk getChunk() {
    return getChunk(CompactingMemStore.IndexType.ARRAY_MAP, ChunkType.DATA_CHUNK);
  }

  /**
   * Creates and inits a chunk. The default implementation for a specific index type.
   * @return the chunk that was initialized
   */
  Chunk getChunk(CompactingMemStore.IndexType chunkIndexType) {
    return getChunk(chunkIndexType, ChunkType.DATA_CHUNK);
  }

  /**
   * Creates and inits a chunk with specific index type and type.
   * @return the chunk that was initialized
   */
  Chunk getChunk(CompactingMemStore.IndexType chunkIndexType, ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool == null) {
          if (indexChunkSize <= 0) {
            throw new IllegalArgumentException(
                "chunkType is INDEX_CHUNK but indexChunkSize is:[" + this.indexChunkSize + "]");
          }
          return getChunk(chunkIndexType, chunkType, indexChunkSize);
        } else {
          return getChunk(chunkIndexType, chunkType, indexChunksPool.getChunkSize());
        }
      case DATA_CHUNK:
        if (dataChunksPool == null) {
          return getChunk(chunkIndexType, chunkType, chunkSize);
        } else {
          return getChunk(chunkIndexType, chunkType, dataChunksPool.getChunkSize());
        }
      default:
        throw new IllegalArgumentException(
                "chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }
  }

  /**
   * Creates and inits a chunk.
   * @return the chunk that was initialized
   * @param chunkIndexType whether the requested chunk is going to be used with CellChunkMap index
   * @param size the size of the chunk to be allocated, in bytes
   */
  Chunk getChunk(CompactingMemStore.IndexType chunkIndexType, ChunkType chunkType, int size) {
    Chunk chunk = null;
    MemStoreChunkPool pool = null;

    // if it is one of the pools
    if (dataChunksPool != null && chunkType == ChunkType.DATA_CHUNK) {
      pool = dataChunksPool;
    } else if (indexChunksPool != null && chunkType == ChunkType.INDEX_CHUNK) {
      pool = indexChunksPool;
    }

    // if we have a pool
    if (pool != null) {
      //  the pool creates the chunk internally. The chunk#init() call happens here
      chunk = pool.getChunk();
      // the pool has run out of maxCount
      if (chunk == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("The chunk pool is full. Reached maxCount= " + pool.getMaxCount()
                  + ". Creating chunk onheap.");
        }
      }
    }

    if (chunk == null) {
      // the second parameter explains whether CellChunkMap index is requested,
      // in that case, put allocated on demand chunk mapping into chunkIdMap
      chunk = createChunk(false, chunkIndexType, chunkType, size);
    }

    // now we need to actually do the expensive memory allocation step in case of a new chunk,
    // else only the offset is set to the beginning of the chunk to accept allocations
    chunk.init();
    return chunk;
  }

  /**
   * Creates and inits a chunk of a special size, bigger than a regular chunk size.
   * Such a chunk will never come from pool and will always be on demand allocated.
   * @return the chunk that was initialized
   * @param jumboSize the special size to be used
   */
  Chunk getJumboChunk(int jumboSize) {
    int allocSize = jumboSize + SIZEOF_CHUNK_HEADER;

    if (allocSize <= this.getChunkSize(ChunkType.DATA_CHUNK)) {
      LOG.warn("Jumbo chunk size " + jumboSize + " must be more than regular chunk size "
          + this.getChunkSize(ChunkType.DATA_CHUNK) + ". Converting to regular chunk.");
      return getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
    }
    // the new chunk is going to hold the jumbo cell data and needs to be referenced by
    // a strong map. Therefore the CCM index type
    return getChunk(CompactingMemStore.IndexType.CHUNK_MAP, ChunkType.JUMBO_CHUNK, allocSize);
  }

  /**
   * Creates the chunk either onheap or offheap
   * @param pool indicates if the chunks have to be created which will be used by the Pool
   * @param chunkIndexType whether the requested chunk is going to be used with CellChunkMap index
   * @param size the size of the chunk to be allocated, in bytes
   * @return the chunk
   */
  private Chunk createChunk(boolean pool, CompactingMemStore.IndexType chunkIndexType,
      ChunkType chunkType, int size) {
    Chunk chunk = null;
    int id = chunkID.getAndIncrement();
    assert id > 0;
    // do not create offheap chunk on demand
    if (pool && this.offheap) {
      chunk = new OffheapChunk(size, id, chunkType, pool);
    } else {
      chunk = new OnheapChunk(size, id, chunkType, pool);
    }
    if (pool || (chunkIndexType == CompactingMemStore.IndexType.CHUNK_MAP)) {
      // put the pool chunk into the chunkIdMap so it is not GC-ed
      this.chunkIdMap.put(chunk.getId(), chunk);
    }
    return chunk;
  }

  // Chunks from pool are created covered with strong references anyway
  // TODO: change to CHUNK_MAP if it is generally defined
  private Chunk createChunkForPool(CompactingMemStore.IndexType chunkIndexType, ChunkType chunkType,
      int chunkSize) {
    if (chunkSize != dataChunksPool.getChunkSize() &&
            chunkSize != indexChunksPool.getChunkSize()) {
      return null;
    }
    return createChunk(true, chunkIndexType, chunkType, chunkSize);
  }

  // Used to translate the ChunkID into a chunk ref
  Chunk getChunk(int id) {
    // can return null if chunk was never mapped
    return chunkIdMap.get(id);
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

  // the chunks in the chunkIdMap may already be released so we shouldn't relay
  // on this counting for strong correctness. This method is used only in testing.
  int numberOfMappedChunks() {
    return this.chunkIdMap.size();
  }

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
    private final int chunkSize;
    private final ChunkType chunkType;
    private int maxCount;

    // A queue of reclaimed chunks
    private final BlockingQueue<Chunk> reclaimedChunks;
    private final float poolSizePercentage;

    /** Statistics thread schedule pool */
    private final ScheduledExecutorService scheduleThreadPool;
    /** Statistics thread */
    private static final int statThreadPeriod = 60 * 5;
    private final AtomicLong chunkCount = new AtomicLong();
    private final LongAdder reusedChunkCount = new LongAdder();
    private final String label;

    MemStoreChunkPool(String label, int chunkSize, ChunkType chunkType, int maxCount,
        int initialCount,
        float poolSizePercentage) {
      this.label = label;
      this.chunkSize = chunkSize;
      this.chunkType = chunkType;
      this.maxCount = maxCount;
      this.poolSizePercentage = poolSizePercentage;
      this.reclaimedChunks = new LinkedBlockingQueue<>();
      for (int i = 0; i < initialCount; i++) {
        Chunk chunk =
            createChunk(true, CompactingMemStore.IndexType.ARRAY_MAP, chunkType, chunkSize);
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
      return getChunk(CompactingMemStore.IndexType.ARRAY_MAP);
    }

    Chunk getChunk(CompactingMemStore.IndexType chunkIndexType) {
      Chunk chunk = reclaimedChunks.poll();
      if (chunk != null) {
        chunk.reset();
        reusedChunkCount.increment();
      } else {
        // Make a chunk iff we have not yet created the maxCount chunks
        while (true) {
          long created = this.chunkCount.get();
          if (created < this.maxCount) {
            if (this.chunkCount.compareAndSet(created, created + 1)) {
              chunk = createChunkForPool(chunkIndexType, chunkType, chunkSize);
              break;
            }
          } else {
            break;
          }
        }
      }
      return chunk;
    }

    int getChunkSize() {
      return chunkSize;
    }

    /**
     * Add the chunks to the pool, when the pool achieves the max size, it will skip the remaining
     * chunks
     * @param c
     */
    private void putbackChunks(Chunk c) {
      int toAdd = this.maxCount - reclaimedChunks.size();
      if (c.isFromPool() && c.size == chunkSize && toAdd > 0) {
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
        long reused = reusedChunkCount.sum();
        long total = created + reused;
        LOG.debug("{} stats (chunk size={}): current pool size={}, created chunk count={}, " +
                "reused chunk count={}, reuseRatio={}", label, chunkSize, reclaimedChunks.size(),
            created, reused,
            (total == 0? "0": StringUtils.formatPercent((float)reused/(float)total,2)));
      }
    }

    private int getMaxCount() {
      return this.maxCount;
    }

    @Override
    public void onHeapMemoryTune(long newMemstoreSize, long newBlockCacheSize) {
      // don't do any tuning in case of offheap memstore
      if (isOffheap()) {
        LOG.warn("{} not tuning the chunk pool as it is offheap", label);
        return;
      }
      int newMaxCount =
              (int) (newMemstoreSize * poolSizePercentage / getChunkSize());
      if (newMaxCount != this.maxCount) {
        // We need an adjustment in the chunks numbers
        if (newMaxCount > this.maxCount) {
          // Max chunks getting increased. Just change the variable. Later calls to getChunk() would
          // create and add them to Q
          LOG.info("{} max count for chunks increased from {} to {}", this.label, this.maxCount,
              newMaxCount);
          this.maxCount = newMaxCount;
        } else {
          // Max chunks getting decreased. We may need to clear off some of the pooled chunks now
          // itself. If the extra chunks are serving already, do not pool those when we get them back
          LOG.info("{} max count for chunks decreased from {} to {}", this.label, this.maxCount,
              newMaxCount);
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

  static void clearDisableFlag() {
    chunkPoolDisabled = false;
  }

  private MemStoreChunkPool initializePool(String label, long globalMemStoreSize,
      float poolSizePercentage, float initialCountPercentage, int chunkSize, ChunkType chunkType,
      HeapMemoryManager heapMemoryManager) {
    if (poolSizePercentage <= 0) {
      LOG.info("{} poolSizePercentage is less than 0. So not using pool", label);
      return null;
    }
    if (chunkPoolDisabled) {
      return null;
    }
    if (poolSizePercentage > 1.0) {
      throw new IllegalArgumentException(
              MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int maxCount = (int) (globalMemStoreSize * poolSizePercentage / chunkSize);
    if (initialCountPercentage > 1.0 || initialCountPercentage < 0) {
      throw new IllegalArgumentException(label + " " + MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY +
          " must be between 0.0 and 1.0");
    }
    int initialCount = (int) (initialCountPercentage * maxCount);
    LOG.info("Allocating {} MemStoreChunkPool with chunk size {}, max count {}, initial count {}",
        label, StringUtils.byteDesc(chunkSize), maxCount, initialCount);
    MemStoreChunkPool memStoreChunkPool = new MemStoreChunkPool(label, chunkSize, chunkType,
        maxCount, initialCount, poolSizePercentage);
    if (heapMemoryManager != null && memStoreChunkPool != null) {
      // Register with Heap Memory manager
      heapMemoryManager.registerTuneObserver(memStoreChunkPool);
    }
    return memStoreChunkPool;
  }

  int getMaxCount() {
    return getMaxCount(ChunkType.DATA_CHUNK);
  }

  int getMaxCount(ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool != null) {
          return indexChunksPool.getMaxCount();
        }
        break;
      case DATA_CHUNK:
        if (dataChunksPool != null) {
          return dataChunksPool.getMaxCount();
        }
        break;
      default:
        throw new IllegalArgumentException(
                "chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }

    return 0;
  }

  int getPoolSize() {
    return getPoolSize(ChunkType.DATA_CHUNK);
  }

  int getPoolSize(ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool != null) {
          return indexChunksPool.reclaimedChunks.size();
        }
        break;
      case DATA_CHUNK:
        if (dataChunksPool != null) {
          return dataChunksPool.reclaimedChunks.size();
        }
        break;
      default:
        throw new IllegalArgumentException(
                "chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }
    return 0;
  }

  boolean isChunkInPool(int chunkId) {
    Chunk c = getChunk(chunkId);
    if (c==null) {
      return false;
    }

    // chunks that are from pool will return true chunk reference not null
    if (dataChunksPool != null && dataChunksPool.reclaimedChunks.contains(c)) {
      return true;
    } else if (indexChunksPool != null && indexChunksPool.reclaimedChunks.contains(c)) {
      return true;
    }
    return false;
  }

  /*
   * Only used in testing
   */
  void clearChunksInPool() {
    if (dataChunksPool != null) {
      dataChunksPool.reclaimedChunks.clear();
    }
    if (indexChunksPool != null) {
      indexChunksPool.reclaimedChunks.clear();
    }
  }

  int getChunkSize() {
    return getChunkSize(ChunkType.DATA_CHUNK);
  }

  int getChunkSize(ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool != null) {
          return indexChunksPool.getChunkSize();
        } else {
          return indexChunkSize;
        }
      case DATA_CHUNK:
        if (dataChunksPool != null) {
          return dataChunksPool.getChunkSize();
        } else { // When pools are empty
          return chunkSize;
        }
      default:
        throw new IllegalArgumentException(
                "chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }
  }

  synchronized void putbackChunks(Set<Integer> chunks) {
    // if there is no pool just try to clear the chunkIdMap in case there is something
    if (dataChunksPool == null && indexChunksPool == null) {
      this.removeChunks(chunks);
      return;
    }

    // if there is a pool, go over all chunk IDs that came back, the chunks may be from pool or not
    for (int chunkID : chunks) {
      // translate chunk ID to chunk, if chunk initially wasn't in pool
      // this translation will (most likely) return null
      Chunk chunk = ChunkCreator.this.getChunk(chunkID);
      if (chunk != null) {
        if (chunk.isFromPool() && chunk.isIndexChunk()) {
          indexChunksPool.putbackChunks(chunk);
        } else if (chunk.isFromPool() && chunk.isDataChunk()) {
          dataChunksPool.putbackChunks(chunk);
        } else {
          // chunks which are not from one of the pools
          // should be released without going to the pools.
          // Removing them from chunkIdMap will cause their removal by the GC.
          this.removeChunk(chunkID);
        }
      }
      // if chunk is null, it was never covered by the chunkIdMap (and so wasn't in pool also),
      // so we have nothing to do on its release
    }
    return;
  }

  MemStoreChunkPool getIndexChunksPool() {
    return this.indexChunksPool;
  }

  MemStoreChunkPool getDataChunksPool() {
    return this.dataChunksPool;
  }

}

