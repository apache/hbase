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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A memstore-local allocation buffer.
 * <p>
 * The MemStoreLAB is basically a bump-the-pointer allocator that allocates
 * big (2MB) byte[] chunks from and then doles it out to threads that request
 * slices into the array.
 * <p>
 * The purpose of this class is to combat heap fragmentation in the
 * regionserver. By ensuring that all Cells in a given memstore refer
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
 * anyway.
 * The chunks created by this MemStoreLAB can get pooled at {@link MemStoreChunkPool}.
 * When the Chunk comes pool, it can be either an on heap or an off heap backed chunk. The chunks,
 * which this MemStoreLAB creates on its own (when no chunk available from pool), those will be
 * always on heap backed.
 */
@InterfaceAudience.Private
public class MemStoreLABImpl implements MemStoreLAB {

  static final Log LOG = LogFactory.getLog(MemStoreLABImpl.class);

  private AtomicReference<Chunk> curChunk = new AtomicReference<Chunk>();
  // A queue of chunks from pool contained by this memstore LAB
  // TODO: in the future, it would be better to have List implementation instead of Queue,
  // as FIFO order is not so important here
  @VisibleForTesting
  BlockingQueue<Chunk> pooledChunkQueue = null;
  private final int chunkSize;
  private final int maxAlloc;
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
  public MemStoreLABImpl() {
    this(new Configuration());
  }

  public MemStoreLABImpl(Configuration conf) {
    chunkSize = conf.getInt(CHUNK_SIZE_KEY, CHUNK_SIZE_DEFAULT);
    maxAlloc = conf.getInt(MAX_ALLOC_KEY, MAX_ALLOC_DEFAULT);
    this.chunkPool = MemStoreChunkPool.getPool();
    // currently chunkQueue is only used for chunkPool
    if (this.chunkPool != null) {
      // set queue length to chunk pool max count to avoid keeping reference of
      // too many non-reclaimable chunks
      pooledChunkQueue = new LinkedBlockingQueue<>(chunkPool.getMaxCount());
    }

    // if we don't exclude allocations >CHUNK_SIZE, we'd infiniteloop on one!
    Preconditions.checkArgument(maxAlloc <= chunkSize,
        MAX_ALLOC_KEY + " must be less than " + CHUNK_SIZE_KEY);
  }


  @Override
  public Cell copyCellInto(Cell cell) {
    int size = KeyValueUtil.length(cell);
    Preconditions.checkArgument(size >= 0, "negative size");
    // Callers should satisfy large allocations directly from JVM since they
    // don't cause fragmentation as badly.
    if (size > maxAlloc) {
      return null;
    }
    Chunk c = null;
    int allocOffset = 0;
    while (true) {
      c = getOrMakeChunk();
      // Try to allocate from this chunk
      allocOffset = c.alloc(size);
      if (allocOffset != -1) {
        // We succeeded - this is the common case - small alloc
        // from a big buffer
        break;
      }
      // not enough space!
      // try to retire this chunk
      tryRetireChunk(c);
    }
    return CellUtil.copyCellTo(cell, c.getData(), allocOffset, size);
  }

  /**
   * Close this instance since it won't be used any more, try to put the chunks
   * back to pool
   */
  @Override
  public void close() {
    this.closed = true;
    // We could put back the chunks to pool for reusing only when there is no
    // opening scanner which will read their data
    if (chunkPool != null && openScannerCount.get() == 0
        && reclaimed.compareAndSet(false, true)) {
      chunkPool.putbackChunks(this.pooledChunkQueue);
    }
  }

  /**
   * Called when opening a scanner on the data of this MemStoreLAB
   */
  @Override
  public void incScannerCount() {
    this.openScannerCount.incrementAndGet();
  }

  /**
   * Called when closing a scanner on the data of this MemStoreLAB
   */
  @Override
  public void decScannerCount() {
    int count = this.openScannerCount.decrementAndGet();
    if (this.closed && chunkPool != null && count == 0
        && reclaimed.compareAndSet(false, true)) {
      chunkPool.putbackChunks(this.pooledChunkQueue);
    }
  }

  /**
   * Try to retire the current chunk if it is still
   * <code>c</code>. Postcondition is that curChunk.get()
   * != c
   * @param c the chunk to retire
   * @return true if we won the race to retire the chunk
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
      if (chunkPool != null) {
        c = chunkPool.getChunk();
      }
      boolean pooledChunk = false;
      if (c != null) {
        // This is chunk from pool
        pooledChunk = true;
      } else {
        c = new OnheapChunk(chunkSize);// When chunk is not from pool, always make it as on heap.
      }
      if (curChunk.compareAndSet(null, c)) {
        // we won race - now we need to actually do the expensive
        // allocation step
        c.init();
        if (pooledChunk) {
          if (!this.closed && !this.pooledChunkQueue.offer(c)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Chunk queue is full, won't reuse this new chunk. Current queue size: "
                  + pooledChunkQueue.size());
            }
          }
        }
        return c;
      } else if (pooledChunk) {
        chunkPool.putbackChunk(c);
      }
      // someone else won race - that's fine, we'll try to grab theirs
      // in the next iteration of the loop.
    }
  }

  @VisibleForTesting
  Chunk getCurrentChunk() {
    return this.curChunk.get();
  }


  BlockingQueue<Chunk> getPooledChunks() {
    return this.pooledChunkQueue;
  }
}
