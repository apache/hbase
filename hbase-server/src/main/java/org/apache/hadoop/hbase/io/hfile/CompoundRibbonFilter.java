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
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ribbon.InterleavedRibbonSolution;
import org.apache.hadoop.hbase.util.ribbon.RibbonFilterUtil;
import org.apache.hadoop.hbase.util.ribbon.RibbonHasher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Reader for compound Ribbon filters stored in HFiles.
 * <p>
 * This class provides on-demand loading of Ribbon filter chunks, similar to
 * {@link CompoundBloomFilter}. Each chunk is loaded from the HFile block cache when needed for a
 * query.
 * <p>
 */
@InterfaceAudience.Private
public class CompoundRibbonFilter extends CompoundRibbonFilterBase implements BloomFilter {

  /** HFile reader for loading chunks on demand */
  private final HFile.Reader reader;

  /** Metrics collector */
  private final BloomFilterMetrics metrics;

  /** Block index for locating chunks */
  private final HFileBlockIndex.BlockIndexReader index;

  /** Per-chunk metadata for queries */
  private final int[] chunkNumSlots;

  /** Per-chunk upperNumColumns for ICML mode */
  private final int[] chunkUpperNumColumns;

  /** Per-chunk upperStartBlock for ICML mode */
  private final int[] chunkUpperStartBlock;

  /**
   * Deserializes a CompoundRibbonFilter from HFile metadata.
   * @param meta    DataInput positioned at the start of Ribbon filter metadata (after version)
   * @param reader  HFile reader for loading chunks
   * @param metrics Metrics collector (may be null)
   * @throws IOException If an I/O error occurs
   */
  public CompoundRibbonFilter(DataInput meta, HFile.Reader reader, BloomFilterMetrics metrics)
    throws IOException {
    this.reader = reader;
    this.metrics = metrics;

    // Read metadata (must match CompoundRibbonFilterWriter.MetaWriter.write())
    totalByteSize = meta.readLong();
    bandwidth = meta.readInt();
    hashType = meta.readInt();
    overheadRatio = meta.readDouble();
    totalKeyCount = meta.readLong();
    totalNumSlots = meta.readLong();
    numChunks = meta.readInt();

    // Read comparator class name
    byte[] comparatorClassName = Bytes.readByteArray(meta);
    if (comparatorClassName.length != 0) {
      comparator = FixedFileTrailer.createComparator(Bytes.toString(comparatorClassName));
    }

    // Read per-chunk numSlots array
    chunkNumSlots = new int[numChunks];
    for (int i = 0; i < numChunks; i++) {
      chunkNumSlots[i] = meta.readInt();
    }

    // Read ICML per-chunk metadata
    chunkUpperNumColumns = new int[numChunks];
    chunkUpperStartBlock = new int[numChunks];
    for (int i = 0; i < numChunks; i++) {
      chunkUpperNumColumns[i] = meta.readInt();
      chunkUpperStartBlock[i] = meta.readInt();
    }

    // Initialize block index reader
    if (comparator == null) {
      index = new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);
    } else {
      index = new HFileBlockIndex.CellBasedKeyBlockIndexReader(comparator, 1);
    }
    index.readRootIndex(meta, numChunks);
  }

  @Override
  public boolean contains(Cell keyCell, ByteBuff bloom, BloomType type) {
    boolean result = containsInternal(keyCell, type);
    if (metrics != null) {
      metrics.incrementRequests(result);
    }
    return result;
  }

  private boolean containsInternal(Cell keyCell, BloomType type) {
    byte[] key = RibbonFilterUtil.extractKeyFromCell(keyCell, type);

    // Find block using appropriate index type
    int block;
    if (comparator != null) {
      block = index.rootBlockContainingKey(keyCell);
    } else {
      block = index.rootBlockContainingKey(key, 0, key.length);
    }

    return containsInternal(block, key, 0, key.length);
  }

  @Override
  public boolean contains(byte[] buf, int offset, int length, ByteBuff bloom) {
    boolean result = containsInternal(buf, offset, length);
    if (metrics != null) {
      metrics.incrementRequests(result);
    }
    return result;
  }

  private boolean containsInternal(byte[] key, int keyOffset, int keyLength) {
    int block = index.rootBlockContainingKey(key, keyOffset, keyLength);
    return containsInternal(block, key, keyOffset, keyLength);
  }

  private boolean containsInternal(int block, byte[] key, int keyOffset, int keyLength) {
    if (block < 0) {
      return false;
    }

    HFileBlock ribbonBlock = loadRibbonBlock(block);
    try {
      ByteBuff buf = ribbonBlock.getBufferReadOnly();
      int headerSize = ribbonBlock.headerSize();
      int numSlots = getChunkNumSlots(block, ribbonBlock);

      RibbonHasher hasher = new RibbonHasher(numSlots, bandwidth, hashType);

      RibbonHasher.RibbonHashResult hashResult = hasher.hash(key, keyOffset, keyLength);

      return InterleavedRibbonSolution.contains(hashResult.start(), hashResult.coeffRow(),
        hashResult.resultRow(), buf, headerSize, numSlots, chunkUpperNumColumns[block],
        chunkUpperStartBlock[block]);
    } finally {
      ribbonBlock.release();
    }
  }

  /**
   * Loads a Ribbon filter block from the HFile.
   * @param blockIndex Index of the block to load
   * @return The loaded HFile block containing Ribbon filter data
   */
  private HFileBlock loadRibbonBlock(int blockIndex) {
    try {
      return reader.readBlock(index.getRootBlockOffset(blockIndex),
        index.getRootBlockDataSize(blockIndex), true, // cacheBlock
        true, // pread
        false, // isCompaction
        true, // updateCacheMetrics
        BlockType.BLOOM_CHUNK, null // expectedDataBlockEncoding
      );
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to load Ribbon block", e);
    }
  }

  /**
   * Gets the number of slots for a chunk.
   * <p>
   * With byte-based format (1 byte per slot), the relationship is: dataSize = numSlots bytes.
   * <p>
   * Uses precomputed chunkNumSlots array from metadata if available, otherwise uses block size
   * directly.
   * @param blockIndex Index of the block
   * @param block      The HFile block (used as fallback if metadata unavailable)
   * @return Number of slots in this chunk
   */
  private int getChunkNumSlots(int blockIndex, HFileBlock block) {
    // Use precomputed value if available
    if (blockIndex < chunkNumSlots.length && chunkNumSlots[blockIndex] > 0) {
      return chunkNumSlots[blockIndex];
    }

    // Fallback: dataSize equals numSlots (1 byte per slot)
    return block.getUncompressedSizeWithoutHeader();
  }

  @Override
  public boolean supportsAutoLoading() {
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CompoundRibbonFilter");
    sb.append(RibbonFilterUtil.STATS_RECORD_SEP);
    sb.append("Keys: ").append(totalKeyCount);
    sb.append(RibbonFilterUtil.STATS_RECORD_SEP);
    sb.append("Slots: ").append(totalNumSlots);
    sb.append(RibbonFilterUtil.STATS_RECORD_SEP);
    sb.append("Chunks: ").append(numChunks);
    sb.append(RibbonFilterUtil.STATS_RECORD_SEP);
    sb.append("Bandwidth: ").append(bandwidth);
    sb.append(RibbonFilterUtil.STATS_RECORD_SEP);
    sb.append("Byte size: ").append(totalByteSize);
    sb.append(RibbonFilterUtil.STATS_RECORD_SEP);
    sb.append("Overhead: ").append(String.format("%.2f%%", overheadRatio * 100));
    if (comparator != null) {
      sb.append(RibbonFilterUtil.STATS_RECORD_SEP);
      sb.append("Comparator: ").append(comparator.getClass().getSimpleName());
    }
    return sb.toString();
  }
}
