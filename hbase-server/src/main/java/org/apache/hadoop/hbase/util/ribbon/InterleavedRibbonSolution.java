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
package org.apache.hadoop.hbase.util.ribbon;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interleaved Column-Major Layout (ICML) storage for Ribbon Filter solution.
 * <p>
 * This layout provides better space efficiency by supporting fractional bits per key. Row-major
 * layout requires fixed power-of-2 byte sizes (1, 2, 4, or 8 bytes) per slot, but ICML stores
 * variable columns (bits) per slot, allowing precise FPR targeting.
 * <p>
 * <b>Memory Layout:</b>
 *
 * <pre>
 * - Data is divided into blocks of 64 slots
 * - Each block contains numColumns segments (64-bit values)
 * - Blocks before upperStartBlock use (upperNumColumns - 1) columns
 * - Blocks from upperStartBlock onwards use upperNumColumns columns
 *
 * Example for 7.5 bits/key average (upperStartBlock = 2):
 *   Block 0: 7 columns
 *   Block 1: 7 columns
 *   Block 2: 8 columns (upperStartBlock)
 *   Block 3: 8 columns
 *   ...
 * </pre>
 */
@InterfaceAudience.Private
public class InterleavedRibbonSolution {

  /** Number of bits in a coefficient row (block size in slots). */
  public static final int COEFF_BITS = RibbonFilterUtil.DEFAULT_BANDWIDTH;

  /** Bytes per segment (sizeof(long) = 8 bytes for 64-bit bandwidth) */
  public static final int SEGMENT_BYTES = COEFF_BITS / 8;

  /** The segment data (column-major within blocks) */
  private final long[] segments;

  /** Total number of slots (must be multiple of COEFF_BITS) */
  private final int numSlots;

  /** Number of blocks */
  private final int numBlocks;

  /** Upper number of columns (some blocks use this) */
  private final int upperNumColumns;

  /** Block index from which upperNumColumns is used (blocks before use lowerNumColumns) */
  private final int upperStartBlock;

  /** Cached ByteBuff view of segments for instance contains method (used in tests) */
  private ByteBuff segmentsBuff;

  /**
   * Creates an InterleavedRibbonSolution with specified parameters.
   * @param numSlots      Number of slots (will be rounded up to multiple of COEFF_BITS)
   * @param desiredFpRate Desired false positive rate (e.g., 0.01 for 1%)
   */
  public InterleavedRibbonSolution(int numSlots, double desiredFpRate) {
    // Round up to multiple of COEFF_BITS
    this.numSlots = roundUpNumSlots(numSlots);
    this.numBlocks = this.numSlots / COEFF_BITS;

    // Calculate columns needed for desired FP rate
    // FPR = 2^(-numColumns), so numColumns = -log2(FPR) = log2(1/FPR)
    double oneInFpRate = 1.0 / desiredFpRate;

    if (oneInFpRate <= 1.0 || numBlocks == 0) {
      // Edge case: 100% FP rate or empty
      this.upperNumColumns = 1;
      this.upperStartBlock = 0;
      this.segments = new long[numBlocks];
    } else {
      // Calculate lower and upper column counts for fractional bits
      int lowerColumns = floorLog2((long) oneInFpRate);
      double lowerFpRate = Math.pow(2.0, -lowerColumns);
      double upperFpRate = Math.pow(2.0, -(lowerColumns + 1));

      // Proportion of slots using lower columns
      double lowerPortion = (desiredFpRate - upperFpRate) / (lowerFpRate - upperFpRate);
      lowerPortion = Math.max(0.0, Math.min(1.0, lowerPortion));

      // Calculate upper_start_block
      int numStarts = this.numSlots - COEFF_BITS + 1;
      this.upperStartBlock = (int) ((lowerPortion * numStarts) / COEFF_BITS);
      this.upperNumColumns = lowerColumns + 1;

      // Calculate total segments needed
      // Blocks [0, upperStartBlock) use lowerColumns
      // Blocks [upperStartBlock, numBlocks) use upperNumColumns
      int numSegments =
        upperStartBlock * lowerColumns + (numBlocks - upperStartBlock) * upperNumColumns;
      this.segments = new long[numSegments];
    }
  }

  /**
   * Rounds up to a number of slots supported by this structure.
   */
  public static int roundUpNumSlots(int numSlots) {
    // Must be multiple of COEFF_BITS, minimum 2 * COEFF_BITS
    int rounded = ((numSlots + COEFF_BITS - 1) / COEFF_BITS) * COEFF_BITS;
    return Math.max(rounded, 2 * COEFF_BITS);
  }

  /**
   * Returns the number of columns for a given block.
   */
  public int getNumColumns(int blockIndex) {
    return (blockIndex < upperStartBlock) ? (upperNumColumns - 1) : upperNumColumns;
  }

  /**
   * Performs back substitution from a RibbonBanding to populate this solution.
   * @param banding The completed banding storage
   */
  public void backSubstFrom(RibbonBanding banding) {
    // State buffer: stores last COEFF_BITS solution values per column
    long[] state = new long[upperNumColumns];

    int segmentNum = segments.length;

    // Process blocks from end to start
    for (int block = numBlocks - 1; block >= 0; block--) {
      int startSlot = block * COEFF_BITS;
      int blockColumns = getNumColumns(block);

      // Process each slot in the block (reverse order within block)
      for (int i = COEFF_BITS - 1; i >= 0; i--) {
        int slotIndex = startSlot + i;
        long cr = banding.getCoeffRow(slotIndex);
        int rr;

        // Handle empty rows with pseudorandom fill
        if (cr == 0) {
          // Pseudorandom fill for empty rows
          rr = (int) (slotIndex * 0x9E3779B185EBCA87L);
        } else {
          rr = 0;
        }

        // Compute solution for each column
        for (int col = 0; col < blockColumns; col++) {
          // Shift state left by 1 (make room for new bit at position 0)
          long tmp = state[col] << 1;

          // Compute next solution bit using parity
          // bit = parity(tmp & cr) XOR ((rr >> col) & 1)
          int bit = Long.bitCount(tmp & cr) & 1;
          bit ^= (rr >> col) & 1;

          // Store the bit
          tmp |= bit;
          state[col] = tmp;
        }
      }

      // Write state to segments for this block
      segmentNum -= blockColumns;
      System.arraycopy(state, 0, segments, segmentNum, blockColumns);
    }
  }

  /**
   * Checks if a key is (probably) in the filter.
   * @param start          Starting position from hash
   * @param coeffRow       Coefficient row from hash
   * @param expectedResult Expected result (always 0)
   * @return true if the key might be in the filter, false if definitely not
   */
  public boolean contains(int start, long coeffRow, int expectedResult) {
    if (segmentsBuff == null) {
      ByteBuffer bb = ByteBuffer.allocate(segments.length * Long.BYTES);
      bb.asLongBuffer().put(segments);
      segmentsBuff = new SingleByteBuff(bb);
    }
    return contains(start, coeffRow, expectedResult, segmentsBuff, 0, numSlots, upperNumColumns,
      upperStartBlock);
  }

  /**
   * Static contains method for querying from ByteBuff. Used when reading directly from HFile
   * blocks.
   * @param start           Starting position from hash
   * @param coeffRow        Coefficient row from hash
   * @param expectedResult  Expected result
   * @param buf             ByteBuff containing segment data
   * @param offset          Offset where segment data starts
   * @param numSlots        Number of slots
   * @param upperNumColumns Upper column count
   * @param upperStartBlock Block index where upper columns start
   * @return true if key might be in filter
   */
  public static boolean contains(int start, long coeffRow, int expectedResult, ByteBuff buf,
    int offset, int numSlots, int upperNumColumns, int upperStartBlock) {

    int numBlocks = numSlots / COEFF_BITS;

    if (start < 0 || start >= numSlots - COEFF_BITS + 1) {
      return false;
    }

    int startBlock = start / COEFF_BITS;
    int startBit = start % COEFF_BITS;

    // Calculate segment position for this block
    int segmentNum;
    int numColumns;
    if (startBlock < upperStartBlock) {
      // Blocks before upperStartBlock use lowerNumColumns
      segmentNum = startBlock * (upperNumColumns - 1);
      numColumns = upperNumColumns - 1;
    } else {
      // Blocks from upperStartBlock onwards use upperNumColumns
      segmentNum =
        upperStartBlock * (upperNumColumns - 1) + (startBlock - upperStartBlock) * upperNumColumns;
      numColumns = upperNumColumns;
    }

    // Split coeffRow
    long crLeft = coeffRow << startBit;
    long crRight = (startBit == 0) ? 0 : (coeffRow >>> (COEFF_BITS - startBit));

    // Next block info
    int nextBlockColumns = 0;
    int nextBlockSegmentStart = 0;
    if (startBit != 0 && startBlock + 1 < numBlocks) {
      int nextBlock = startBlock + 1;
      if (nextBlock < upperStartBlock) {
        nextBlockColumns = upperNumColumns - 1;
        nextBlockSegmentStart = nextBlock * (upperNumColumns - 1);
      } else {
        nextBlockColumns = upperNumColumns;
        nextBlockSegmentStart =
          upperStartBlock * (upperNumColumns - 1) + (nextBlock - upperStartBlock) * upperNumColumns;
      }
    }

    int columnsToCheck =
      (nextBlockColumns > 0) ? Math.min(numColumns, nextBlockColumns) : numColumns;

    for (int col = 0; col < columnsToCheck; col++) {
      // Load segment from buffer (little-endian)
      int bufPos = offset + (segmentNum + col) * SEGMENT_BYTES;
      long seg = buf.getLong(bufPos);
      long solnData = seg & crLeft;

      if (nextBlockColumns > 0) {
        int nextBufPos = offset + (nextBlockSegmentStart + col) * SEGMENT_BYTES;
        long nextSeg = buf.getLong(nextBufPos);
        solnData |= nextSeg & crRight;
      }

      int bit = Long.bitCount(solnData) & 1;

      if (((expectedResult >> col) & 1) != bit) {
        return false;
      }
    }

    return true;
  }

  /**
   * Writes the solution to a DataOutput stream.
   */
  public void writeTo(DataOutput out) throws IOException {
    out.writeInt(numSlots);
    out.writeInt(upperNumColumns);
    out.writeInt(upperStartBlock);
    out.writeInt(segments.length);
    for (long segment : segments) {
      out.writeLong(segment);
    }
  }

  /**
   * Computes floor(log2(n)) for positive n.
   */
  private static int floorLog2(long n) {
    if (n <= 0) {
      return 0;
    }
    return 63 - Long.numberOfLeadingZeros(n);
  }

  /**
   * Returns the size in bytes.
   */
  public long getByteSize() {
    return (long) segments.length * SEGMENT_BYTES;
  }

  public int getUpperNumColumns() {
    return upperNumColumns;
  }

  public int getUpperStartBlock() {
    return upperStartBlock;
  }

  public long[] getSegments() {
    return segments;
  }
}
