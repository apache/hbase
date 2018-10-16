/*
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

package org.apache.hadoop.hbase.io.hfile;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.BloomFilterBase;

import org.apache.hadoop.hbase.CellComparator;

@InterfaceAudience.Private
public class CompoundBloomFilterBase implements BloomFilterBase {

  /**
   * At read time, the total number of chunks. At write time, the number of
   * chunks created so far. The first chunk has an ID of 0, and the current
   * chunk has the ID of numChunks - 1.
   */
  protected int numChunks;

  /**
   * The Bloom filter version. There used to be a DynamicByteBloomFilter which
   * had version 2.
   */
  public static final int VERSION = 3;

  /** Target error rate for configuring the filter and for information */
  protected float errorRate;

  /** The total number of keys in all chunks */
  protected long totalKeyCount;
  protected long totalByteSize;
  protected long totalMaxKeys;

  /** Hash function type to use, as defined in {@link org.apache.hadoop.hbase.util.Hash} */
  protected int hashType;
  /** Comparator used to compare Bloom filter keys */
  protected CellComparator comparator;

  @Override
  public long getMaxKeys() {
    return totalMaxKeys;
  }

  @Override
  public long getKeyCount() {
    return totalKeyCount;
  }

  @Override
  public long getByteSize() {
    return totalByteSize;
  }

}
