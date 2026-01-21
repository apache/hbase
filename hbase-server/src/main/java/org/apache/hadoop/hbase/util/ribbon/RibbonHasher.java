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

import org.apache.hadoop.hbase.util.ByteArrayHashKey;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.HashKey;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hasher for Ribbon Filter that generates hash components needed for banding and querying.
 * <p>
 * For each key, this class generates:
 * <ul>
 * <li>start: The starting row position in the band matrix</li>
 * <li>coeffRow: The coefficient row (w bits wide, where w is the bandwidth)</li>
 * <li>resultRow: Always 0 (no fingerprint storage)</li>
 * </ul>
 * <p>
 * The hash function uses a 64-bit hash split into components for start position and coefficient
 * generation.
 */
@InterfaceAudience.Private
public class RibbonHasher {

  /** Golden ratio constant for better hash mixing */
  private static final long GOLDEN_RATIO = 0x9E3779B97F4A7C15L;

  /** Number of slots in the ribbon filter */
  private final int numSlots;

  /** Bandwidth (coefficient width in bits, typically 64) */
  private final int bandwidth;

  /** Hash function instance */
  private final Hash hash;

  /** Hash type (stored for reference) */
  private final int hashType;

  /**
   * Creates a new RibbonHasher.
   * @param numSlots  Number of slots in the ribbon filter (m)
   * @param bandwidth Coefficient width in bits (w), typically 64
   * @param hashType  Hash type to use (e.g., Hash.MURMUR_HASH3)
   */
  public RibbonHasher(int numSlots, int bandwidth, int hashType) {
    if (numSlots <= 0) {
      throw new IllegalArgumentException("numSlots must be positive: " + numSlots);
    }
    RibbonFilterUtil.validateBandwidth(bandwidth);

    this.numSlots = numSlots;
    this.bandwidth = bandwidth;
    this.hash = Hash.getInstance(hashType);
    this.hashType = hashType;

    if (this.hash == null) {
      throw new IllegalArgumentException("Invalid hash type: " + hashType);
    }
  }

  /**
   * Computes hash components for a byte array key.
   * @param key    Key bytes
   * @param offset Offset into key array
   * @param length Length of key
   * @return RibbonHashResult containing start, coeffRow, and resultRow
   */
  public RibbonHashResult hash(byte[] key, int offset, int length) {
    HashKey<byte[]> hashKey = new ByteArrayHashKey(key, offset, length);
    return computeHash(hashKey);
  }

  /**
   * Internal method to compute hash components from any HashKey.
   */
  private <T> RibbonHashResult computeHash(HashKey<T> hashKey) {
    // Generate two 32-bit hashes and combine into 64-bit
    int hash1 = hash.hash(hashKey, 0);
    int hash2 = hash.hash(hashKey, hash1);
    long rawHash = ((long) hash1 << 32) | (hash2 & 0xFFFFFFFFL);

    // Mix the hash for better distribution
    long mixedHash = mixHash(rawHash);

    // Compute start position using FastRange
    int numStarts = numSlots - bandwidth + 1;
    if (numStarts <= 0) {
      numStarts = 1;
    }
    int start = fastRange(mixedHash >>> 32, numStarts);

    // Compute coefficient row (first bit always 1)
    long coeffRow = computeCoeffRow(mixedHash);

    // For Ribbon Filter, resultRow is always 0
    // The filter works by checking if XOR of solution values equals 0
    // Empty rows are filled with pseudorandom data during back-substitution
    return new RibbonHashResult(start, coeffRow, 0);
  }

  /**
   * Mixes the hash value for better distribution using a variant of SplitMix64.
   */
  private long mixHash(long h) {
    h ^= h >>> 33;
    h *= 0xFF51AFD7ED558CCDL;
    h ^= h >>> 33;
    h *= 0xC4CEB9FE1A85EC53L;
    h ^= h >>> 33;
    return h;
  }

  /**
   * FastRange: Maps a 32-bit hash to [0, range) using multiplication instead of modulo.
   * <p>
   * This is equivalent to {@code hash % range} but much faster because multiplication requires
   * significantly fewer CPU cycles than division.
   * <p>
   * The algorithm: {@code (hash * range) >> 32} produces a uniform distribution in [0, range).
   * @param hash  32-bit hash value (passed as long to avoid sign issues)
   * @param range The upper bound (exclusive)
   * @return A value in [0, range)
   */
  private static int fastRange(long hash, int range) {
    // Ensure we use unsigned 32-bit multiplication
    return (int) (((hash & 0xFFFFFFFFL) * range) >>> 32);
  }

  /**
   * Computes the coefficient row from the mixed hash. Applies additional mixing to decorrelate from
   * start position. The first bit is always 1 (kFirstCoeffAlwaysOne optimization).
   */
  private long computeCoeffRow(long mixedHash) {
    // Apply additional mixing to decorrelate from start position
    // Uses a different mixing constant than the main hash mixing
    long c = mixedHash * GOLDEN_RATIO;
    c ^= c >>> 33;
    c *= 0xC4CEB9FE1A85EC53L;
    c ^= c >>> 33;

    // First bit is always 1 (kFirstCoeffAlwaysOne optimization)
    // This ensures coeffRow is never zero and simplifies banding
    return c | 1L;
  }

  /**
   * Returns the number of slots in the ribbon filter.
   */
  public int getNumSlots() {
    return numSlots;
  }

  /**
   * Returns the bandwidth (coefficient width in bits).
   */
  public int getBandwidth() {
    return bandwidth;
  }

  /**
   * Returns the hash type.
   */
  public int getHashType() {
    return hashType;
  }

  /**
   * Result of hashing a key for Ribbon Filter.
   * @param start     Starting row position in the band matrix
   * @param coeffRow  Coefficient row (bandwidth bits wide)
   * @param resultRow Result row (always 0)
   */
  public record RibbonHashResult(int start, long coeffRow, int resultRow) {

    @Override
    public String toString() {
      return String.format("RibbonHashResult{start=%d, coeffRow=0x%016X, resultRow=0x%08X}", start,
        coeffRow, resultRow);
    }
  }
}
