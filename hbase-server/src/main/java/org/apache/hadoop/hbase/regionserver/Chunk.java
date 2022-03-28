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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.regionserver.ChunkCreator.ChunkType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A chunk of memory out of which allocations are sliced.
 */
@InterfaceAudience.Private
public abstract class Chunk {
  /** Actual underlying data */
  protected ByteBuffer data;

  protected static final int UNINITIALIZED = -1;
  protected static final int OOM = -2;
  /**
   * Offset for the next allocation, or the sentinel value -1 which implies that the chunk is still
   * uninitialized.
   */
  protected AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

  /** Total number of allocations satisfied from this buffer */
  protected AtomicInteger allocCount = new AtomicInteger();

  /** Size of chunk in bytes */
  protected final int size;

  // The unique id associated with the chunk.
  private final int id;

  private final ChunkType chunkType;

  // indicates if the chunk is formed by ChunkCreator#MemstorePool
  private final boolean fromPool;

  /**
   * Create an uninitialized chunk. Note that memory is not allocated yet, so
   * this is cheap.
   * @param size in bytes
   * @param id the chunk id
   */
  public Chunk(int size, int id, ChunkType chunkType) {
    this(size, id, chunkType, false);
  }

  /**
   * Create an uninitialized chunk. Note that memory is not allocated yet, so
   * this is cheap.
   * @param size in bytes
   * @param id the chunk id
   * @param fromPool if the chunk is formed by pool
   */
  public Chunk(int size, int id, ChunkType chunkType, boolean fromPool) {
    this.size = size;
    this.id = id;
    this.chunkType = chunkType;
    this.fromPool = fromPool;
  }

  int getId() {
    return this.id;
  }

  ChunkType getChunkType() {
    return this.chunkType;
  }

  boolean isFromPool() {
    return this.fromPool;
  }

  boolean isJumbo() {
    return chunkType == ChunkCreator.ChunkType.JUMBO_CHUNK;
  }

  boolean isIndexChunk() {
    return chunkType == ChunkCreator.ChunkType.INDEX_CHUNK;
  }

  boolean isDataChunk() {
    return chunkType == ChunkCreator.ChunkType.DATA_CHUNK;
  }

  /**
   * Actually claim the memory for this chunk. This should only be called from the thread that
   * constructed the chunk. It is thread-safe against other threads calling alloc(), who will block
   * until the allocation is complete.
   */
  public void init() {
    assert nextFreeOffset.get() == UNINITIALIZED;
    try {
      allocateDataBuffer();
    } catch (OutOfMemoryError e) {
      boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
      assert failInit; // should be true.
      throw e;
    }
    // Mark that it's ready for use
    // Move 4 bytes since the first 4 bytes are having the chunkid in it
    boolean initted = nextFreeOffset.compareAndSet(UNINITIALIZED, Bytes.SIZEOF_INT);
    // We should always succeed the above CAS since only one thread
    // calls init()!
    Preconditions.checkState(initted, "Multiple threads tried to init same chunk");
  }

  abstract void allocateDataBuffer();

  /**
   * Reset the offset to UNINITIALIZED before before reusing an old chunk
   */
  void reset() {
    if (nextFreeOffset.get() != UNINITIALIZED) {
      nextFreeOffset.set(UNINITIALIZED);
      allocCount.set(0);
    }
  }

  /**
   * Try to allocate <code>size</code> bytes from the chunk.
   * If a chunk is tried to get allocated before init() call, the thread doing the allocation
   * will be in busy-wait state as it will keep looping till the nextFreeOffset is set.
   * @return the offset of the successful allocation, or -1 to indicate not-enough-space
   */
  public int alloc(int size) {
    while (true) {
      int oldOffset = nextFreeOffset.get();
      if (oldOffset == UNINITIALIZED) {
        // The chunk doesn't have its data allocated yet.
        // Since we found this in curChunk, we know that whoever
        // CAS-ed it there is allocating it right now. So spin-loop
        // shouldn't spin long!
        Thread.yield();
        continue;
      }
      if (oldOffset == OOM) {
        // doh we ran out of ram. return -1 to chuck this away.
        return -1;
      }

      if (oldOffset + size > data.capacity()) {
        return -1; // alloc doesn't fit
      }
      // TODO : If seqID is to be written add 8 bytes here for nextFreeOFfset
      // Try to atomically claim this chunk
      if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size)) {
        // we got the alloc
        allocCount.incrementAndGet();
        return oldOffset;
      }
      // we raced and lost alloc, try again
    }
  }

  /**
   * @return This chunk's backing data.
   */
  ByteBuffer getData() {
    return this.data;
  }

  @Override
  public String toString() {
    return "Chunk@" + System.identityHashCode(this) + " allocs=" + allocCount.get() + "waste="
        + (data.capacity() - nextFreeOffset.get());
  }

  int getNextFreeOffset() {
    return this.nextFreeOffset.get();
  }
}
