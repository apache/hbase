/*
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ribbon.InterleavedRibbonSolution;
import org.apache.hadoop.hbase.util.ribbon.RibbonFilterChunk;
import org.apache.hadoop.hbase.util.ribbon.RibbonFilterUtil;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for compound Ribbon filters in HFiles.
 * <p>
 * This class manages the lifecycle of Ribbon filter chunks similar to
 * {@link CompoundBloomFilterWriter}. Keys are buffered in each chunk and processed during
 * finalization when the chunk is full.
 * <p>
 */
@InterfaceAudience.Private
public class CompoundRibbonFilterWriter extends CompoundRibbonFilterBase
  implements BloomFilterWriter, InlineBlockWriter {

  private static final Logger LOG = LoggerFactory.getLogger(CompoundRibbonFilterWriter.class);

  /** Target block size in bytes (same as Bloom filter block size) */
  private final int blockSize;

  /** Maximum number of keys per chunk (computed from blockSize) */
  private final int maxKeysPerChunk;

  /** Desired false positive rate */
  private final double desiredFpRate;

  /** Pre-calculated optimal overhead (calculated once in constructor) */
  private final double overhead;

  /** The current chunk being written to */
  private RibbonFilterChunk chunk;

  /** Previous chunk, for creating another similar chunk */
  private RibbonFilterChunk prevChunk;

  /** The first key in the current chunk */
  private byte[] firstKeyInChunk;

  /** The previous cell that was processed */
  private ExtendedCell prevCell;

  /** Whether to cache-on-write compound Ribbon filter chunks */
  private final boolean cacheOnWrite;

  /** The bloom type for key extraction */
  private final BloomType bloomType;

  /**
   * A Ribbon filter chunk ready for writing.
   * <p>
   * This class holds the constructed Ribbon filter chunk along with its metadata (first key, ICML
   * parameters) until it can be written to the HFile.
   */
  private static final class ReadyChunk {
    int chunkId;
    byte[] firstKey;
    RibbonFilterChunk chunk;
    // ICML metadata
    int upperNumColumns;
    int upperStartBlock;
  }

  /** Queue of chunks ready to be written */
  private final Queue<ReadyChunk> readyChunks = new ArrayDeque<>();

  /** Block index writer for chunk offsets */
  private final HFileBlockIndex.BlockIndexWriter ribbonBlockIndexWriter =
    new HFileBlockIndex.BlockIndexWriter();

  /** Per-chunk numSlots for accurate metadata storage */
  private final List<Integer> chunkNumSlotsList = new ArrayList<>();

  /** Per-chunk upperNumColumns for ICML */
  private final List<Integer> chunkUpperNumColumnsList = new ArrayList<>();

  /** Per-chunk upperStartBlock for ICML */
  private final List<Integer> chunkUpperStartBlockList = new ArrayList<>();

  /**
   * Creates a new CompoundRibbonFilterWriter.
   * <p>
   * Always uses ICML (Interleaved Column-Major Layout) for space-optimal storage. Overhead ratio is
   * calculated automatically based on the FP rate.
   * @param blockSize     Target block size in bytes (same as Bloom filter block size)
   * @param bandwidth     Coefficient width in bits (typically 64)
   * @param hashType      Hash function type
   * @param cacheOnWrite  Whether to cache chunks on write
   * @param comparator    Cell comparator (for ROWCOL type)
   * @param bloomType     The bloom/ribbon type
   * @param desiredFpRate Desired false positive rate (e.g., 0.01 for 1%)
   */
  public CompoundRibbonFilterWriter(int blockSize, int bandwidth, int hashType,
    boolean cacheOnWrite, CellComparator comparator, BloomType bloomType, double desiredFpRate) {
    this.blockSize = blockSize;
    this.bandwidth = bandwidth;
    this.hashType = hashType;
    this.desiredFpRate = desiredFpRate;
    this.cacheOnWrite = cacheOnWrite;
    this.comparator = comparator;
    this.bloomType = bloomType;

    // Pre-calculate optimal overhead based on FP rate
    this.overhead = RibbonFilterUtil.computeOptimalOverheadForFpRate(desiredFpRate, bandwidth);

    // Compute maxKeysPerChunk from blockSize
    this.maxKeysPerChunk = computeMaxKeysFromBlockSize();
  }

  /**
   * Computes the maximum number of keys per chunk based on blockSize.
   * <p>
   * ICML uses variable bits per slot: maxKeys = blockSize * 8 / ((1 + overhead) * bitsPerKey)
   */
  private int computeMaxKeysFromBlockSize() {
    double effectiveOverhead = overhead > 0 ? overhead : RibbonFilterUtil.MIN_OVERHEAD_RATIO;

    // ICML mode: bits per key = fingerprintBits
    int bitsPerKey = RibbonFilterUtil.computeFingerprintBits(desiredFpRate);
    // maxKeys = blockSize * 8 / ((1 + overhead) * bitsPerKey)
    return (int) (blockSize * 8.0 / ((1.0 + effectiveOverhead) * bitsPerKey));
  }

  @Override
  public boolean shouldWriteBlock(boolean closing) {
    enqueueReadyChunk(closing);
    return !readyChunks.isEmpty();
  }

  /**
   * Enqueue the current chunk if it is ready to be written out.
   * @param closing true if we are closing the file, so we do not expect new keys to show up
   */
  private void enqueueReadyChunk(boolean closing) {
    if (chunk == null || (chunk.getKeyCount() < maxKeysPerChunk && !closing)) {
      return;
    }

    if (firstKeyInChunk == null) {
      throw new NullPointerException("Trying to enqueue a chunk, but first key is null: closing="
        + closing + ", keyCount=" + chunk.getKeyCount() + ", maxKeys=" + maxKeysPerChunk);
    }

    // Finalize the chunk (back-substitution)
    chunk.finalizeRibbon();

    // Create ready chunk
    ReadyChunk readyChunk = new ReadyChunk();
    readyChunk.chunkId = numChunks - 1;
    readyChunk.firstKey = firstKeyInChunk;
    readyChunk.chunk = chunk;

    // Store ICML metadata
    InterleavedRibbonSolution sol = chunk.getInterleavedSolution();
    if (sol != null) {
      readyChunk.upperNumColumns = sol.getUpperNumColumns();
      readyChunk.upperStartBlock = sol.getUpperStartBlock();
    }

    readyChunks.add(readyChunk);

    // Update totals
    totalByteSize += chunk.getByteSize();
    totalNumSlots += chunk.getNumSlots();
    chunkNumSlotsList.add(chunk.getNumSlots());
    chunkUpperNumColumnsList.add(readyChunk.upperNumColumns);
    chunkUpperStartBlockList.add(readyChunk.upperStartBlock);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Finalized Ribbon chunk #{} with {} keys, {} slots, {} bytes", readyChunk.chunkId,
        chunk.getKeyCount(), chunk.getNumSlots(), chunk.getByteSize());
    }

    // Reset for next chunk
    firstKeyInChunk = null;
    prevChunk = chunk;
    chunk = null;
  }

  @Override
  public void append(ExtendedCell cell) throws IOException {
    Objects.requireNonNull(cell);

    enqueueReadyChunk(false);

    if (chunk == null) {
      if (firstKeyInChunk != null) {
        throw new IllegalStateException(
          "First key in chunk already set: " + Bytes.toStringBinary(firstKeyInChunk));
      }
      // This will be done only once per chunk
      firstKeyInChunk = RibbonFilterUtil.extractKeyFromCell(cell, bloomType.toBaseType());
      allocateNewChunk();
    }

    chunk.add(cell);
    this.prevCell = cell;
    ++totalKeyCount;
  }

  /**
   * Allocates a new Ribbon filter chunk with pre-allocated banding matrix.
   */
  private void allocateNewChunk() {
    if (prevChunk == null) {
      // First chunk
      chunk =
        new RibbonFilterChunk(bandwidth, hashType, bloomType.toBaseType(), desiredFpRate, overhead);
    } else {
      // Use the same parameters as the last chunk
      chunk = prevChunk.createAnother();
    }

    // Pre-allocate the banding matrix
    chunk.allocRibbon(maxKeysPerChunk);
    ++numChunks;
  }

  @Override
  public void writeInlineBlock(DataOutput out) throws IOException {
    ReadyChunk readyChunk = readyChunks.peek();
    if (readyChunk == null) {
      throw new IOException("No ready chunk to write");
    }

    // Write the Ribbon filter data
    readyChunk.chunk.writeRibbon(out);
  }

  @Override
  public void blockWritten(long offset, int onDiskSize, int uncompressedSize) {
    ReadyChunk readyChunk = readyChunks.remove();
    ribbonBlockIndexWriter.addEntry(readyChunk.firstKey, offset, onDiskSize);
  }

  @Override
  public BlockType getInlineBlockType() {
    return BlockType.BLOOM_CHUNK; // Reuse BLOOM_CHUNK type for compatibility
  }

  @Override
  public boolean getCacheOnWrite() {
    return cacheOnWrite;
  }

  @Override
  public void beforeShipped() throws IOException {
    if (this.prevCell != null) {
      this.prevCell = KeyValueUtil.toNewKeyCell(this.prevCell);
    }
  }

  @Override
  public Cell getPrevCell() {
    return this.prevCell;
  }

  @Override
  public void compactBloom() {
    // No-op for Ribbon filters.
    // Unlike Bloom filters which can be folded post-construction,
    // Ribbon filters use lazy allocation to achieve optimal sizing:
    // the banding matrix is sized based on actual keyCount during
    // finalizeRibbon(), not the pre-estimated maxKeys.
  }

  @Override
  public Writable getMetaWriter() {
    return new MetaWriter();
  }

  @Override
  public Writable getDataWriter() {
    return null;
  }

  /**
   * Metadata writer for Ribbon filter.
   * <p>
   * Writes all metadata required to reconstruct the CompoundRibbonFilter at read time, including
   * filter parameters, per-chunk ICML metadata, and the block index.
   */
  private final class MetaWriter implements Writable {
    /**
     * Not supported - this is a write-only implementation.
     * @throws IOException Always thrown
     */
    @Override
    public void readFields(DataInput in) throws IOException {
      throw new IOException("Cannot read with MetaWriter");
    }

    /**
     * Writes Ribbon filter metadata to the output stream.
     * @param out The output stream to write to
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(VERSION);

      // Ribbon-specific metadata
      out.writeLong(totalByteSize);
      out.writeInt(bandwidth);
      out.writeInt(hashType);
      out.writeDouble(overhead);
      out.writeLong(totalKeyCount);
      out.writeLong(totalNumSlots);
      out.writeInt(numChunks);

      // Comparator class name (for ROWCOL type)
      if (comparator != null) {
        Bytes.writeByteArray(out, Bytes.toBytes(comparator.getClass().getName()));
      } else {
        Bytes.writeByteArray(out, null);
      }

      // Write per-chunk numSlots array for accurate slot counts
      for (int i = 0; i < numChunks; i++) {
        out.writeInt(chunkNumSlotsList.get(i));
      }

      // Write ICML per-chunk metadata
      for (int i = 0; i < numChunks; i++) {
        out.writeInt(chunkUpperNumColumnsList.get(i));
        out.writeInt(chunkUpperStartBlockList.get(i));
      }

      // Write block index
      ribbonBlockIndexWriter.writeSingleLevelIndex(out, "Ribbon filter");
    }
  }
}
