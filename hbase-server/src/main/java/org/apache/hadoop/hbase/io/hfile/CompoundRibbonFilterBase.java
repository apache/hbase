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

import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.util.BloomFilterBase;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for Compound Ribbon Filter implementations. This class provides common fields and
 * methods shared between the reader ({@link CompoundRibbonFilter}) and writer
 * ({@link CompoundRibbonFilterWriter}).
 */
@InterfaceAudience.Private
public class CompoundRibbonFilterBase implements BloomFilterBase {

  /**
   * The Ribbon filter version. This is different from CompoundBloomFilterBase.VERSION to
   * distinguish Ribbon filters from Bloom filters when reading metadata.
   */
  public static final int VERSION = 101;

  /**
   * At read time, the total number of chunks. At write time, the number of chunks created so far.
   * The first chunk has an ID of 0, and the current chunk has the ID of numChunks - 1.
   */
  protected int numChunks;

  /** The total number of keys in all chunks */
  protected long totalKeyCount;

  /** The total byte size of all chunks */
  protected long totalByteSize;

  /** The total number of slots across all chunks */
  protected long totalNumSlots;

  /** Bandwidth (coefficient width in bits), typically 64 */
  protected int bandwidth;

  /** Space overhead ratio (e.g., 0.05 for 5%) */
  protected double overheadRatio;

  /** Hash function type to use, as defined in {@link org.apache.hadoop.hbase.util.Hash} */
  protected int hashType;

  /** Comparator used to compare filter keys (for ROWCOL type) */
  protected CellComparator comparator;

  @Override
  public long getMaxKeys() {
    // For Ribbon filters, maxKeys equals keyCount since we build to fit exactly
    return totalKeyCount;
  }

  @Override
  public long getKeyCount() {
    return totalKeyCount;
  }

  @Override
  public long getByteSize() {
    return totalByteSize;
  }

  /**
   * Returns the bandwidth (coefficient width in bits).
   */
  public int getBandwidth() {
    return bandwidth;
  }

  /**
   * Returns the space overhead ratio.
   */
  public double getOverheadRatio() {
    return overheadRatio;
  }

  /**
   * Returns the number of chunks.
   */
  public int getNumChunks() {
    return numChunks;
  }

  /**
   * Returns the hash type.
   */
  public int getHashType() {
    return hashType;
  }
}
