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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for Ribbon Filter.
 */
@InterfaceAudience.Private
public final class RibbonFilterUtil {

  /** Default bandwidth (coefficient width in bits). */
  public static final int DEFAULT_BANDWIDTH = 64;

  /** Minimum overhead ratio */
  public static final double MIN_OVERHEAD_RATIO = 0.01;

  /** Maximum overhead ratio */
  public static final double MAX_OVERHEAD_RATIO = 0.50;

  /** Separator for stats formatting */
  public static final String STATS_RECORD_SEP = "; ";

  private RibbonFilterUtil() {
    // Utility class, no instantiation
  }

  /**
   * Computes the optimal number of slots for a Ribbon filter.
   * <p>
   * For n keys with overhead ratio ε, the number of slots is: m = n * (1 + ε)
   * @param numKeys       Number of keys to store
   * @param overheadRatio Space overhead ratio (e.g., 0.05 for 5%)
   * @return Number of slots needed
   */
  public static int computeNumSlots(int numKeys, double overheadRatio) {
    if (numKeys <= 0) {
      return 0;
    }
    if (overheadRatio < MIN_OVERHEAD_RATIO) {
      overheadRatio = MIN_OVERHEAD_RATIO;
    }
    if (overheadRatio > MAX_OVERHEAD_RATIO) {
      overheadRatio = MAX_OVERHEAD_RATIO;
    }

    // m = n * (1 + overhead)
    long slots = (long) Math.ceil(numKeys * (1.0 + overheadRatio));

    // Ensure at least bandwidth slots for proper functioning
    slots = Math.max(slots, DEFAULT_BANDWIDTH);

    return (int) Math.min(slots, Integer.MAX_VALUE);
  }

  /**
   * Computes the optimal slot overhead for Ribbon Filter using the formula: ε = (4 + r/4) / w,
   * where r = -log₂(fpRate) and w = bandwidth.
   * @param desiredFpRate Desired false positive rate (e.g., 0.01 for 1%)
   * @param bandwidth     Coefficient width in bits, typically 64
   * @return Optimal overhead ratio
   * @see <a href="https://arxiv.org/abs/2103.02515">Ribbon Filter Paper, Equation 7</a>
   */
  public static double computeOptimalOverheadForFpRate(double desiredFpRate, int bandwidth) {
    if (desiredFpRate <= 0 || desiredFpRate >= 1.0) {
      throw new IllegalArgumentException("desiredFpRate must be in (0, 1): " + desiredFpRate);
    }

    // r = -log₂(fpRate)
    double resultBits = -Math.log(desiredFpRate) / Math.log(2.0);

    // ε = (4 + r/4) / w
    double overhead = (4.0 + resultBits / 4.0) / bandwidth;

    // Clamp to valid range
    return Math.max(MIN_OVERHEAD_RATIO, Math.min(MAX_OVERHEAD_RATIO, overhead));
  }

  /**
   * Computes the size in bytes of a Ribbon filter solution.
   * @param numSlots Number of slots
   * @return Size in bytes (1 byte per slot)
   */
  public static long computeByteSize(int numSlots) {
    return numSlots; // 1 byte per slot
  }

  /**
   * Formats statistics about a Ribbon filter for display.
   * @param numKeys   Number of keys stored
   * @param numSlots  Number of slots
   * @param bandwidth Bandwidth
   * @return Formatted statistics string
   */
  public static String formatStats(int numKeys, int numSlots, int bandwidth) {
    StringBuilder sb = new StringBuilder();
    sb.append("Ribbon Filter");
    sb.append(STATS_RECORD_SEP);
    sb.append("Keys: ").append(numKeys);
    sb.append(STATS_RECORD_SEP);
    sb.append("Slots: ").append(numSlots);
    sb.append(STATS_RECORD_SEP);
    sb.append("Bandwidth: ").append(bandwidth);
    sb.append(STATS_RECORD_SEP);
    sb.append("Byte size: ").append(computeByteSize(numSlots));

    if (numKeys > 0) {
      double overhead = (double) numSlots / numKeys - 1.0;
      sb.append(STATS_RECORD_SEP);
      sb.append(String.format("Overhead: %.2f%%", overhead * 100));
    }

    return sb.toString();
  }

  /**
   * Validates the bandwidth parameter.
   * @param bandwidth Bandwidth to validate
   * @throws IllegalArgumentException If bandwidth is not {@link #DEFAULT_BANDWIDTH}
   */
  public static void validateBandwidth(int bandwidth) {
    if (bandwidth != DEFAULT_BANDWIDTH) {
      throw new IllegalArgumentException(
        "Unsupported bandwidth: " + bandwidth + ". Only " + DEFAULT_BANDWIDTH + " is supported.");
    }
  }

  /**
   * Returns the default hash type for Ribbon filter.
   */
  public static int getDefaultHashType() {
    return Hash.MURMUR_HASH3;
  }

  /**
   * Computes the number of columns (bits per key) needed for a target false positive rate.
   * @param fpRate Target false positive rate (e.g., 0.01 for 1%)
   * @return Number of columns (minimum 1)
   */
  public static int computeFingerprintBits(double fpRate) {
    if (fpRate <= 0 || fpRate >= 1.0) {
      return 1;
    }
    return Math.max(1, (int) Math.ceil(-Math.log(fpRate) / Math.log(2.0)));
  }

  /**
   * Extracts the key bytes from a Cell based on the bloom type.
   * @param cell     The cell to extract key from
   * @param baseType The base bloom type (ROW or ROWCOL)
   * @return The extracted key bytes
   */
  public static byte[] extractKeyFromCell(Cell cell, BloomType baseType) {
    if (baseType == BloomType.ROWCOL) {
      return PrivateCellUtil
        .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRowCol(cell));
    } else {
      return CellUtil.copyRow(cell);
    }
  }
}
