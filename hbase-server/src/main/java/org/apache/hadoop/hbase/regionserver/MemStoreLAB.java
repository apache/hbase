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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

/**
 * A memstore-local allocation buffer.
 * <p>
 * The MemStoreLAB is basically a bump-the-pointer allocator that allocates
 * big (2MB) byte[] chunks from and then doles it out to threads that request
 * slices into the array.
 * <p>
 * The purpose of this class is to combat heap fragmentation in the
 * regionserver. By ensuring that all KeyValues in a given memstore refer
 * only to large chunks of contiguous memory, we ensure that large blocks
 * get freed up when the memstore is flushed.
 * <p>
 * Without the MSLAB, the byte array allocated during insertion end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 * <p>
 * TODO: we should probably benchmark whether word-aligning the allocations
 * would provide a performance improvement - probably would speed up the
 * Bytes.toLong/Bytes.toInt calls in KeyValue, but some of those are cached
 * anyway
 */
@InterfaceAudience.Private
public class MemStoreLAB {
  private AtomicReference<Chunk> curChunk = new AtomicReference<Chunk>();
  // A queue of chunks contained by this memstore
  private BlockingQueue<Chunk> chunkQueue = new LinkedBlockingQueue<Chunk>();

  final static String CHUNK_SIZE_KEY = "hbase.hregion.memstore.mslab.chunksize";
  final static int CHUNK_SIZE_DEFAULT = 2048 * 1024;
  final int chunkSize;

  final static String MAX_ALLOC_KEY = "hbase.hregion.memstore.mslab.max.allocation";
  final static int MAX_ALLOC_DEFAULT = 256  * 1024; // allocs bigger than this don't go through allocator
  final int maxAlloc;

  private final MemStoreChunkPool chunkPool;

  // This flag is for closing this instance, its set when clearing snapshot of
  // memstore
  private volatile boolean closed = false;
  // This flag is for reclaiming chunks. Its set when putting chunks back to
  // pool
  private AtomicBoolean reclaimed = new AtomicBoolean(false);
  // Current count of open scanners which reading data from this MemStoreLAB
  private final AtomicInteger openScannerCount = new AtomicInteger();

  // Used in testing
  public MemStoreLAB() {
    this(new Configuration());
  }

  private MemStoreLAB(Configuration conf) {
    this(conf, MemStoreChunkPool.getPool(conf));
  }

  public MemStoreLAB(Configuration conf, MemStoreChunkPool pool) {
    chunkSize = conf.getInt(CHUNK_SIZE_KEY, CHUNK_SIZE_DEFAULT);
    maxAlloc = conf.getInt(MAX_ALLOC_KEY, MAX_ALLOC_DEFAULT);
    this.chunkPool = pool;

    // if we don't exclude allocations >CHUNK_SIZE, we'd infiniteloop on one!
    Preconditions.checkArgument(
      maxAlloc <= chunkSize,
      MAX_ALLOC_KEY + " must be less than " + CHUNK_SIZE_KEY);
  }

  /**
   * Allocate a slice of the given length.
   *
   * If the size is larger than the maximum size specified for this
   * allocator, returns null.
   */
  public Allocation allocateBytes(int size) {
    Preconditions.checkArgument(size >= 0, "negative size");

    // Callers should satisfy large allocations directly from JVM since they
    // don't cause fragmentation as badly.
    if (size > maxAlloc) {
      return null;
    }

    while (true) {
      Chunk c = getOrMakeChunk();

      // Try to allocate from this chunk
      int allocOffset = c.alloc(size);
      if (allocOffset != -1) {
        // We succeeded - this is the common case - small alloc
        // from a big buffer
        return new Allocation(c.data, allocOffset);
      }

      // not enough space!
      // try to retire this chunk
      tryRetireChunk(c);
    }
  }

  /**
   * Close this instance since it won't be used any more, try to put the chunks
   * back to pool
   */
  void close() {
    this.closed = true;
    // We could put back the chunks to pool for reusing only when there is no
    // opening scanner which will read their data
    if (chunkPool != null && openScannerCount.get() == 0
        && reclaimed.compareAndSet(false, true)) {
      chunkPool.putbackChunks(this.chunkQueue);
    }
  }

  /**
   * Called when opening a scanner on the data of this MemStoreLAB
   */
  void incScannerCount() {
    this.openScannerCount.incrementAndGet();
  }

  /**
   * Called when closing a scanner on the data of this MemStoreLAB
   */
  void decScannerCount() {
    int count = this.openScannerCount.decrementAndGet();
    if (chunkPool != null && count == 0 && this.closed
        && reclaimed.compareAndSet(false, true)) {
      chunkPool.putbackChunks(this.chunkQueue);
    }
  }

  /**
   * Try to retire the current chunk if it is still
   * <code>c</code>. Postcondition is that curChunk.get()
   * != c
   */
  private void tryRetireChunk(Chunk c) {
    curChunk.compareAndSet(c, null);
    // If the CAS succeeds, that means that we won the race
    // to retire the chunk. We could use this opportunity to
    // update metrics on external fragmentation.
    //
    // If the CAS fails, that means that someone else already
    // retired the chunk for us.
  }

  /**
   * Get the current chunk, or, if there is no current chunk,
   * allocate a new one from the JVM.
   */
  private Chunk getOrMakeChunk() {
    while (true) {
      // Try to get the chunk
      Chunk c = curChunk.get();
      if (c != null) {
        return c;
      }

      // No current chunk, so we want to allocate one. We race
      // against other allocators to CAS in an uninitialized chunk
      // (which is cheap to allocate)
      c = (chunkPool != null) ? chunkPool.getChunk() : new Chunk(chunkSize);
      if (curChunk.compareAndSet(null, c)) {
        // we won race - now we need to actually do the expensive
        // allocation step
        c.init();
        this.chunkQueue.add(c);
        return c;
      } else if (chunkPool != null) {
        chunkPool.putbackChunk(c);
      }
      // someone else won race - that's fine, we'll try to grab theirs
      // in the next iteration of the loop.
    }
  }

  /**
   * A chunk of memory out of which allocations are sliced.
   */
  static class Chunk {
    /** Actual underlying data */
    private byte[] data;

    private static final int UNINITIALIZED = -1;
    private static final int OOM = -2;
    /**
     * Offset for the next allocation, or the sentinel value -1
     * which implies that the chunk is still uninitialized.
     * */
    private AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

    /** Total number of allocations satisfied from this buffer */
    private AtomicInteger allocCount = new AtomicInteger();

    /** Size of chunk in bytes */
    private final int size;

    /**
     * Create an uninitialized chunk. Note that memory is not allocated yet, so
     * this is cheap.
     * @param size in bytes
     */
    Chunk(int size) {
      this.size = size;
    }

    /**
     * Actually claim the memory for this chunk. This should only be called from
     * the thread that constructed the chunk. It is thread-safe against other
     * threads calling alloc(), who will block until the allocation is complete.
     */
    public void init() {
      assert nextFreeOffset.get() == UNINITIALIZED;
      try {
        if (data == null) {
          data = new byte[size];
        }
      } catch (OutOfMemoryError e) {
        boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
        assert failInit; // should be true.
        throw e;
      }
      // Mark that it's ready for use
      boolean initted = nextFreeOffset.compareAndSet(
          UNINITIALIZED, 0);
      // We should always succeed the above CAS since only one thread
      // calls init()!
      Preconditions.checkState(initted,
          "Multiple threads tried to init same chunk");
    }

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

        if (oldOffset + size > data.length) {
          return -1; // alloc doesn't fit
        }

        // Try to atomically claim this chunk
        if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size)) {
          // we got the alloc
          allocCount.incrementAndGet();
          return oldOffset;
        }
        // we raced and lost alloc, try again
      }
    }

    @Override
    public String toString() {
      return "Chunk@" + System.identityHashCode(this) +
        " allocs=" + allocCount.get() + "waste=" +
        (data.length - nextFreeOffset.get());
    }
  }

  /**
   * The result of a single allocation. Contains the chunk that the
   * allocation points into, and the offset in this array where the
   * slice begins.
   */
  public static class Allocation {
    private final byte[] data;
    private final int offset;

    private Allocation(byte[] data, int off) {
      this.data = data;
      this.offset = off;
    }

    @Override
    public String toString() {
      return "Allocation(" + "capacity=" + data.length + ", off=" + offset
          + ")";
    }

    byte[] getData() {
      return data;
    }

    int getOffset() {
      return offset;
    }
  }
}
