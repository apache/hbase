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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterBase;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A single chunk of a Ribbon Filter using ICML storage.
 * @see InterleavedRibbonSolution
 */
@InterfaceAudience.Private
public class RibbonFilterChunk implements BloomFilterBase {

  /** Bandwidth (coefficient width in bits) */
  private final int bandwidth;

  /** Hash type */
  private final int hashType;

  /** Bloom type for key extraction */
  private final BloomType bloomType;

  /** Desired false positive rate */
  private final double desiredFpRate;

  /** Pre-calculated overhead ratio (passed from higher level) */
  private final double overheadRatio;

  /** Number of keys added */
  private int keyCount;

  /** Maximum number of keys (capacity) */
  private int maxKeys;

  /** Number of slots */
  private int numSlots;

  /** The hasher (created during finalization) */
  private RibbonHasher hasher;

  /** The interleaved solution (created during finalization) */
  private InterleavedRibbonSolution interleavedSolution;

  /**
   * Key buffer for lazy allocation. Keys are buffered during add() and processed during
   * finalizeRibbon() to allow optimal sizing based on actual key count.
   */
  private List<byte[]> keyBuffer;

  /**
   * Creates a new RibbonFilterChunk for writing with ICML storage mode.
   * @param bandwidth     Bandwidth (typically 64)
   * @param hashType      Hash type
   * @param bloomType     Bloom type for key extraction
   * @param desiredFpRate Desired false positive rate (e.g., 0.01 for 1%)
   * @param overheadRatio Pre-calculated overhead ratio
   */
  public RibbonFilterChunk(int bandwidth, int hashType, BloomType bloomType, double desiredFpRate,
    double overheadRatio) {
    RibbonFilterUtil.validateBandwidth(bandwidth);

    this.bandwidth = bandwidth;
    this.hashType = hashType;
    this.bloomType = bloomType;
    this.desiredFpRate = desiredFpRate;
    this.overheadRatio = overheadRatio;
    this.keyCount = 0;
    this.maxKeys = 0;
    this.numSlots = 0;
  }

  /**
   * Creates a new RibbonFilterChunk with default settings. Only used for testing.
   * <p>
   * Calculates optimal overhead automatically.
   * @param bloomType Bloom type for key extraction
   */
  public RibbonFilterChunk(BloomType bloomType) {
    // Default 1% FP rate
    this(RibbonFilterUtil.DEFAULT_BANDWIDTH, RibbonFilterUtil.getDefaultHashType(), bloomType, 0.01,
      RibbonFilterUtil.computeOptimalOverheadForFpRate(0.01, RibbonFilterUtil.DEFAULT_BANDWIDTH));
  }

  /**
   * Initializes the key buffer for lazy allocation. The actual banding matrix is allocated during
   * finalization based on the actual number of keys added.
   * <p>
   * This approach ensures optimal space usage by sizing the filter based on actual key count rather
   * than estimated maximum keys.
   * @param maxKeys Maximum number of keys expected (used for initial buffer capacity hint)
   * @throws IllegalStateException If already allocated
   */
  public void allocRibbon(int maxKeys) {
    if (keyBuffer != null) {
      throw new IllegalStateException("Ribbon filter already allocated");
    }

    this.maxKeys = maxKeys;

    // Initialize key buffer with reasonable initial capacity
    // Use smaller of maxKeys or 1024 to avoid over-allocation for small chunks
    int initialCapacity = Math.min(maxKeys, 1024);
    keyBuffer = new ArrayList<>(Math.max(initialCapacity, 16));
  }

  /**
   * Adds a Cell to the filter. The key is extracted and buffered for later processing during
   * finalization.
   * @param cell The cell to add
   * @throws IllegalStateException If not allocated or already finalized
   */
  public void add(Cell cell) {
    byte[] key = RibbonFilterUtil.extractKeyFromCell(cell, bloomType.toBaseType());
    addKey(key);
  }

  /**
   * Adds a raw key to the filter. The key is buffered for later processing during finalization.
   * @param key The key bytes
   * @throws IllegalStateException If not allocated or already finalized
   */
  public void addKey(byte[] key) {
    if (keyBuffer == null) {
      throw new IllegalStateException("Ribbon filter not allocated. Call allocRibbon() first.");
    }
    if (interleavedSolution != null) {
      throw new IllegalStateException("Ribbon filter already finalized.");
    }

    keyBuffer.add(key);
    keyCount++;
  }

  /**
   * Finalizes the Ribbon filter by allocating optimal-sized structures and performing
   * back-substitution.
   * <p>
   * This method implements lazy allocation: the banding matrix is sized based on the actual number
   * of keys added (keyCount) rather than the estimated maximum (maxKeys). This ensures optimal
   * space usage, especially for chunks with fewer keys than expected.
   * @throws IllegalStateException If not allocated or already finalized
   */
  public void finalizeRibbon() {
    if (keyBuffer == null) {
      throw new IllegalStateException("Ribbon filter not allocated. Call allocRibbon() first.");
    }
    if (interleavedSolution != null) {
      throw new IllegalStateException("Ribbon filter already finalized.");
    }

    // Calculate optimal numSlots based on actual keyCount (not maxKeys)
    // roundUpNumSlots() ensures minimum of 2*COEFF_BITS (128) slots
    numSlots = InterleavedRibbonSolution
      .roundUpNumSlots(RibbonFilterUtil.computeNumSlots(keyCount, overheadRatio));

    // Now allocate hasher and banding with optimal size
    hasher = new RibbonHasher(numSlots, bandwidth, hashType);
    RibbonBanding banding = new RibbonBanding(numSlots, bandwidth);

    // Add all buffered keys to the banding matrix
    for (byte[] key : keyBuffer) {
      RibbonHasher.RibbonHashResult hashResult = hasher.hash(key, 0, key.length);
      banding.add(hashResult.start(), hashResult.coeffRow());
    }

    // Clear key buffer to free memory
    keyBuffer = null;

    // Compute solution via back substitution using ICML storage
    double fpRate = desiredFpRate > 0 ? desiredFpRate : 0.01;
    interleavedSolution = new InterleavedRibbonSolution(numSlots, fpRate);
    interleavedSolution.backSubstFrom(banding);
  }

  /**
   * Checks if a key is (probably) in the filter.
   * @param key    Key bytes
   * @param offset Offset into key array
   * @param length Length of key
   * @return true if the key might be in the filter, false if definitely not
   * @throws IllegalStateException If not finalized
   */
  public boolean contains(byte[] key, int offset, int length) {
    if (interleavedSolution == null) {
      throw new IllegalStateException("Ribbon filter not finalized. Call finalizeRibbon() first.");
    }

    RibbonHasher.RibbonHashResult hashResult = hasher.hash(key, offset, length);
    return interleavedSolution.contains(hashResult.start(), hashResult.coeffRow(),
      hashResult.resultRow());
  }

  /**
   * Writes the Ribbon filter to a DataOutput stream. Only writes the solution data (not metadata).
   * @param out The output stream
   * @throws IOException           If an I/O error occurs
   * @throws IllegalStateException If not finalized
   */
  public void writeRibbon(DataOutput out) throws IOException {
    if (interleavedSolution == null) {
      throw new IllegalStateException("Ribbon filter not finalized. Call finalizeRibbon() first.");
    }

    // Write ICML segments (metadata is stored separately in MetaWriter)
    long[] segments = interleavedSolution.getSegments();
    for (long segment : segments) {
      out.writeLong(segment);
    }
  }

  /**
   * Creates a new RibbonFilterChunk with the same configuration.
   * @return A new chunk with the same settings
   */
  public RibbonFilterChunk createAnother() {
    return new RibbonFilterChunk(bandwidth, hashType, bloomType, desiredFpRate, overheadRatio);
  }

  // BloomFilterBase implementation

  @Override
  public long getByteSize() {
    if (interleavedSolution != null) {
      return interleavedSolution.getByteSize();
    }
    return 0;
  }

  @Override
  public long getKeyCount() {
    return keyCount;
  }

  @Override
  public long getMaxKeys() {
    return maxKeys;
  }

  // Getters

  public int getBandwidth() {
    return bandwidth;
  }

  public int getHashType() {
    return hashType;
  }

  public BloomType getBloomType() {
    return bloomType;
  }

  public int getNumSlots() {
    return numSlots;
  }

  /**
   * Returns the interleaved solution.
   * @return The interleaved solution, or null if not finalized
   */
  public InterleavedRibbonSolution getInterleavedSolution() {
    return interleavedSolution;
  }

  @Override
  public String toString() {
    return RibbonFilterUtil.formatStats(keyCount, numSlots, bandwidth);
  }

}
