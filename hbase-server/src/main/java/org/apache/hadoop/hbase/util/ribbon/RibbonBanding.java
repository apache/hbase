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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implements the banding phase of Homogeneous Ribbon Filter construction using Gaussian elimination
 * in GF(2).
 * <p>
 * The banding process takes key hash results (start, coeffRow) and builds a banded matrix that can
 * later be solved via back substitution to produce the filter solution. Construction never fails -
 * inconsistencies are absorbed and empty rows are filled with pseudorandom data.
 * <p>
 * This implementation is optimized for Homogeneous Ribbon Filter where resultRow is always 0,
 * eliminating the need for result row storage.
 * @see <a href="https://arxiv.org/abs/2103.02515">Ribbon Filter Paper</a>
 */
@InterfaceAudience.Private
public class RibbonBanding {

  /** Coefficient rows storage */
  private final long[] coeffRows;

  /** Number of slots */
  private final int numSlots;

  /** Bandwidth */
  private final int bandwidth;

  /** Number of keys successfully added */
  private int numAdded;

  /**
   * Creates a new RibbonBanding storage.
   * @param numSlots  Number of slots in the ribbon filter
   * @param bandwidth Coefficient width in bits (typically 64)
   */
  public RibbonBanding(int numSlots, int bandwidth) {
    if (numSlots <= 0) {
      throw new IllegalArgumentException("numSlots must be positive: " + numSlots);
    }
    RibbonFilterUtil.validateBandwidth(bandwidth);

    this.numSlots = numSlots;
    this.bandwidth = bandwidth;
    this.coeffRows = new long[numSlots];
    this.numAdded = 0;
  }

  /**
   * Adds a single entry to the banding storage using Gaussian elimination. This method always
   * succeeds because inconsistencies are absorbed during back substitution.
   * <p>
   * For Homogeneous Ribbon Filter, resultRow is always 0 and not stored.
   * @param start    Starting row position
   * @param coeffRow Coefficient row
   */
  public void add(int start, long coeffRow) {
    if (start < 0 || start >= numSlots) {
      throw new IllegalArgumentException(
        "start position out of range: " + start + " (numSlots=" + numSlots + ")");
    }

    int i = start;
    long cr = coeffRow;

    while (true) {
      // Check bounds
      if (i >= numSlots) {
        // Coefficient row extends beyond available slots
        // This is absorbed (never fails)
        return;
      }

      long existingCr = coeffRows[i];

      if (existingCr == 0) {
        // Empty slot found - store the coefficient row
        coeffRows[i] = cr;
        numAdded++;
        return;
      }

      // Gaussian elimination: XOR with existing row
      cr ^= existingCr;

      if (cr == 0) {
        // Coefficient row became zero
        // Inconsistency is absorbed (always succeeds)
        return;
      }

      // Move to next position based on trailing zeros
      int tz = Long.numberOfTrailingZeros(cr);
      i += tz;
      cr >>>= tz;
    }
  }

  /**
   * Returns the number of keys successfully added.
   */
  public int getNumAdded() {
    return numAdded;
  }

  /**
   * Returns the number of slots.
   */
  public int getNumSlots() {
    return numSlots;
  }

  /**
   * Returns the bandwidth.
   */
  public int getBandwidth() {
    return bandwidth;
  }

  /**
   * Returns the coefficient row at the given index (for testing/debugging).
   */
  long getCoeffRow(int index) {
    return coeffRows[index];
  }
}
