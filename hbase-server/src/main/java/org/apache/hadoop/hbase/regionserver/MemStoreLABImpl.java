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

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

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
 * The chunks created by this MemStoreLAB can get pooled at {@link ChunkCreator}.
 * When the Chunk comes from pool, it can be either an on heap or an off heap backed chunk. The chunks,
 * which this MemStoreLAB creates on its own (when no chunk available from pool), those will be
 * always on heap backed.
 */
@InterfaceAudience.Private
public class MemStoreLABImpl implements MemStoreLAB {

  static final Logger LOG = LoggerFactory.getLogger(MemStoreLABImpl.class);

  private AtomicReference<Chunk> currChunk = new AtomicReference<>();
  // Lock to manage multiple handlers requesting for a chunk
  private ReentrantLock lock = new ReentrantLock();

  // A set of chunks contained by this memstore LAB
  Set<Integer> chunks = new ConcurrentSkipListSet<Integer>();
  private final int dataChunkSize;
  private final int maxAlloc;
  private final ChunkCreator chunkCreator;
  private final CompactingMemStore.IndexType idxType; // what index is used for corresponding segment

  // This flag is for closing this instance, its set when clearing snapshot of
  // memstore
  private final AtomicBoolean closed = new AtomicBoolean(false);;
  // This flag is for reclaiming chunks. Its set when putting chunks back to
  // pool
  private final AtomicBoolean reclaimed = new AtomicBoolean(false);
  // Current count of open scanners which reading data from this MemStoreLAB
  private final AtomicInteger openScannerCount = new AtomicInteger();

  // Used in testing
  public MemStoreLABImpl() {
    this(new Configuration());
  }

  public MemStoreLABImpl(Configuration conf) {
    dataChunkSize = conf.getInt(CHUNK_SIZE_KEY, CHUNK_SIZE_DEFAULT);
    maxAlloc = conf.getInt(MAX_ALLOC_KEY, MAX_ALLOC_DEFAULT);
    this.chunkCreator = ChunkCreator.getInstance();
    // if we don't exclude allocations >CHUNK_SIZE, we'd infiniteloop on one!
    Preconditions.checkArgument(maxAlloc <= dataChunkSize,
        MAX_ALLOC_KEY + " must be less than " + CHUNK_SIZE_KEY);

    // if user requested to work with MSLABs (whether on- or off-heap), then the
    // immutable segments are going to use CellChunkMap as their index
    idxType = CompactingMemStore.IndexType.CHUNK_MAP;
  }

  @Override
  public Cell copyCellInto(Cell cell) {
    // See head of copyBBECellInto for how it differs from copyCellInto
    return (cell instanceof ByteBufferExtendedCell)?
        copyBBECellInto((ByteBufferExtendedCell)cell, maxAlloc):
        copyCellInto(cell, maxAlloc);
  }

  /**
   * When a cell's size is too big (bigger than maxAlloc),
   * copyCellInto does not allocate it on MSLAB.
   * Since the process of flattening to CellChunkMap assumes that
   * all cells are allocated on MSLAB, during this process,
   * the big cells are copied into MSLAB using this method.
   */
  @Override
  public Cell forceCopyOfBigCellInto(Cell cell) {
    int size = Segment.getCellLength(cell);
    Preconditions.checkArgument(size >= 0, "negative size");
    if (size + ChunkCreator.SIZEOF_CHUNK_HEADER <= dataChunkSize) {
      // Using copyCellInto for cells which are bigger than the original maxAlloc
      return copyCellInto(cell, dataChunkSize);
    } else {
      Chunk c = getNewExternalChunk(size);
      int allocOffset = c.alloc(size);
      return copyToChunkCell(cell, c.getData(), allocOffset, size);
    }
  }

  /**
   * Mostly a duplicate of {@link #copyCellInto(Cell, int)}} done for perf sake. It presumes
   * ByteBufferExtendedCell instead of Cell so we deal with a specific type rather than the
   * super generic Cell. Removes instanceof checks. Shrinkage is enough to make this inline where
   * before it was too big. Uses less CPU. See HBASE-20875 for evidence.
   * @see #copyCellInto(Cell, int)
   */
  private Cell copyBBECellInto(ByteBufferExtendedCell cell, int maxAlloc) {
    int size = cell.getSerializedSize();
    Preconditions.checkArgument(size >= 0, "negative size");
    // Callers should satisfy large allocations from JVM heap so limit fragmentation.
    if (size > maxAlloc) {
      return null;
    }
    Chunk c = null;
    int allocOffset = 0;
    while (true) {
      // Try to get the chunk
      c = getOrMakeChunk();
      // We may get null because the some other thread succeeded in getting the lock
      // and so the current thread has to try again to make its chunk or grab the chunk
      // that the other thread created
      // Try to allocate from this chunk
      if (c != null) {
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
    }
    return copyBBECToChunkCell(cell, c.getData(), allocOffset, size);
  }

  /**
   * @see #copyBBECellInto(ByteBufferExtendedCell, int)
   */
  private Cell copyCellInto(Cell cell, int maxAlloc) {
    int size = Segment.getCellLength(cell);
    Preconditions.checkArgument(size >= 0, "negative size");
    // Callers should satisfy large allocations directly from JVM since they
    // don't cause fragmentation as badly.
    if (size > maxAlloc) {
      return null;
    }
    Chunk c = null;
    int allocOffset = 0;
    while (true) {
      // Try to get the chunk
      c = getOrMakeChunk();
      // we may get null because the some other thread succeeded in getting the lock
      // and so the current thread has to try again to make its chunk or grab the chunk
      // that the other thread created
      // Try to allocate from this chunk
      if (c != null) {
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
    }
    return copyToChunkCell(cell, c.getData(), allocOffset, size);
  }

  /**
   * Clone the passed cell by copying its data into the passed buf and create a cell with a chunkid
   * out of it
   * @see #copyBBECToChunkCell(ByteBufferExtendedCell, ByteBuffer, int, int)
   */
  private static Cell copyToChunkCell(Cell cell, ByteBuffer buf, int offset, int len) {
    int tagsLen = cell.getTagsLength();
    if (cell instanceof ExtendedCell) {
      ((ExtendedCell) cell).write(buf, offset);
    } else {
      // Normally all Cell impls within Server will be of type ExtendedCell. Just considering the
      // other case also. The data fragments within Cell is copied into buf as in KeyValue
      // serialization format only.
      KeyValueUtil.appendTo(cell, buf, offset, true);
    }
    return createChunkCell(buf, offset, len, tagsLen, cell.getSequenceId());
  }

  /**
   * Clone the passed cell by copying its data into the passed buf and create a cell with a chunkid
   * out of it
   * @see #copyToChunkCell(Cell, ByteBuffer, int, int)
   */
  private static Cell copyBBECToChunkCell(ByteBufferExtendedCell cell, ByteBuffer buf, int offset,
      int len) {
    int tagsLen = cell.getTagsLength();
    cell.write(buf, offset);
    return createChunkCell(buf, offset, len, tagsLen, cell.getSequenceId());
  }

  private static Cell createChunkCell(ByteBuffer buf, int offset, int len, int tagsLen,
      long sequenceId) {
    // TODO : write the seqid here. For writing seqId we should create a new cell type so
    // that seqId is not used as the state
    if (tagsLen == 0) {
      // When tagsLen is 0, make a NoTagsByteBufferKeyValue version. This is an optimized class
      // which directly return tagsLen as 0. So we avoid parsing many length components in
      // reading the tagLength stored in the backing buffer. The Memstore addition of every Cell
      // call getTagsLength().
      return new NoTagByteBufferChunkKeyValue(buf, offset, len, sequenceId);
    } else {
      return new ByteBufferChunkKeyValue(buf, offset, len, sequenceId);
    }
  }

  /**
   * Close this instance since it won't be used any more, try to put the chunks
   * back to pool
   */
  @Override
  public void close() {
    if (!this.closed.compareAndSet(false, true)) {
      return;
    }
    // We could put back the chunks to pool for reusing only when there is no
    // opening scanner which will read their data
    int count  = openScannerCount.get();
    if(count == 0) {
      recycleChunks();
    }
  }

  int getOpenScannerCount() {
    return this.openScannerCount.get();
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
    if (this.closed.get() && count == 0) {
      recycleChunks();
    }
  }

  private void recycleChunks() {
    if (reclaimed.compareAndSet(false, true)) {
      chunkCreator.putbackChunks(chunks);
      chunks.clear();
    }
  }

  /**
   * Try to retire the current chunk if it is still
   * <code>c</code>. Postcondition is that curChunk.get()
   * != c
   * @param c the chunk to retire
   */
  private void tryRetireChunk(Chunk c) {
    currChunk.compareAndSet(c, null);
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
    // Try to get the chunk
    Chunk c = currChunk.get();
    if (c != null) {
      return c;
    }
    // No current chunk, so we want to allocate one. We race
    // against other allocators to CAS in an uninitialized chunk
    // (which is cheap to allocate)
    if (lock.tryLock()) {
      try {
        // once again check inside the lock
        c = currChunk.get();
        if (c != null) {
          return c;
        }
        c = this.chunkCreator.getChunk(idxType);
        if (c != null) {
          // set the curChunk. No need of CAS as only one thread will be here
          currChunk.set(c);
          chunks.add(c.getId());
          return c;
        }
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  /* Returning a new pool chunk, without replacing current chunk,
  ** meaning MSLABImpl does not make the returned chunk as CurChunk.
  ** The space on this chunk will be allocated externally.
  ** The interface is only for external callers.
  */
  @Override
  public Chunk getNewExternalChunk(ChunkCreator.ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
      case DATA_CHUNK:
        Chunk c = this.chunkCreator.getChunk(chunkType);
        chunks.add(c.getId());
        return c;
      case JUMBO_CHUNK: // a jumbo chunk doesn't have a fixed size
      default:
        return null;
    }
  }

  /* Returning a new chunk, without replacing current chunk,
  ** meaning MSLABImpl does not make the returned chunk as CurChunk.
  ** The space on this chunk will be allocated externally.
  ** The interface is only for external callers.
  ** Chunks from pools are not allocated from here, since they have fixed sizes
  */
  @Override
  public Chunk getNewExternalChunk(int size) {
    int allocSize = size + ChunkCreator.SIZEOF_CHUNK_HEADER;
    if (allocSize <= ChunkCreator.getInstance().getChunkSize()) {
      return getNewExternalChunk(ChunkCreator.ChunkType.DATA_CHUNK);
    } else {
      Chunk c = this.chunkCreator.getJumboChunk(size);
      chunks.add(c.getId());
      return c;
    }
  }

  @Override
  public boolean isOnHeap() {
    return !isOffHeap();
  }

  @Override
  public boolean isOffHeap() {
    return this.chunkCreator.isOffheap();
  }

  Chunk getCurrentChunk() {
    return currChunk.get();
  }

  BlockingQueue<Chunk> getPooledChunks() {
    BlockingQueue<Chunk> pooledChunks = new LinkedBlockingQueue<>();
    for (Integer id : this.chunks) {
      Chunk chunk = chunkCreator.getChunk(id);
      if (chunk != null && chunk.isFromPool()) {
        pooledChunks.add(chunk);
      }
    }
    return pooledChunks;
  }

  Integer getNumOfChunksReturnedToPool(Set<Integer> chunksId) {
    int i = 0;
    for (Integer id : chunksId) {
      if (chunkCreator.isChunkInPool(id)) {
        i++;
      }
    }
    return i;
  }

  boolean isReclaimed() {
    return reclaimed.get();
  }

  boolean isClosed() {
    return closed.get();
  }
}
