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
package org.apache.hadoop.hbase;

import java.util.Map;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Encapsulates per-region load metrics.
 */
@InterfaceAudience.Public
public interface RegionMetrics {

  /** Returns the region name */
  byte[] getRegionName();

  /** Returns the number of stores */
  int getStoreCount();

  /** Returns the number of storefiles */
  int getStoreFileCount();

  /** Returns the total size of the storefiles */
  Size getStoreFileSize();

  /** Returns the memstore size */
  Size getMemStoreSize();

  /** Returns the number of read requests made to region */
  long getReadRequestCount();

  /** Returns the number of write requests made to region */
  long getWriteRequestCount();

  /** Returns the number of coprocessor service requests made to region */
  public long getCpRequestCount();

  /**
   * Returns the number of write requests and read requests and coprocessor service requests made to
   * region
   */
  default long getRequestCount() {
    return getReadRequestCount() + getWriteRequestCount() + getCpRequestCount();
  }

  /** Returns the region name as a string */
  default String getNameAsString() {
    return Bytes.toStringBinary(getRegionName());
  }

  /** Returns the number of filtered read requests made to region */
  long getFilteredReadRequestCount();

  /** Returns the number of deleted row read requests made to region */
  long getDeletedReadRequestCount();

  /**
   * TODO: why we pass the same value to different counters? Currently, the value from
   * getStoreFileIndexSize() is same with getStoreFileRootLevelIndexSize() see
   * HRegionServer#createRegionLoad.
   * @return The current total size of root-level indexes for the region
   */
  Size getStoreFileIndexSize();

  /** Returns The current total size of root-level indexes for the region */
  Size getStoreFileRootLevelIndexSize();

  /** Returns The total size of all index blocks, not just the root level */
  Size getStoreFileUncompressedDataIndexSize();

  /** Returns The total size of all Bloom filter blocks, not just loaded into the block cache */
  Size getBloomFilterSize();

  /** Returns the total number of cells in current compaction */
  long getCompactingCellCount();

  /** Returns the number of already compacted kvs in current compaction */
  long getCompactedCellCount();

  /**
   * This does not really belong inside RegionLoad but its being done in the name of expediency.
   * @return the completed sequence Id for the region
   */
  long getCompletedSequenceId();

  /** Returns completed sequence id per store. */
  Map<byte[], Long> getStoreSequenceId();

  /** Returns the uncompressed size of the storefiles */
  Size getUncompressedStoreFileSize();

  /** Returns the data locality of region in the regionserver. */
  float getDataLocality();

  /** Returns the timestamp of the oldest hfile for any store of this region. */
  long getLastMajorCompactionTimestamp();

  /** Returns the reference count for the stores of this region */
  int getStoreRefCount();

  /**
   * Returns the max reference count for any store file among all compacted stores files of this
   * region
   */
  int getMaxCompactedStoreFileRefCount();

  /**
   * Different from dataLocality,this metric's numerator only include the data stored on ssd
   * @return the data locality for ssd of region in the regionserver
   */
  float getDataLocalityForSsd();

  /** Returns the data at local weight of this region in the regionserver */
  long getBlocksLocalWeight();

  /**
   * Different from blocksLocalWeight,this metric's numerator only include the data stored on ssd
   * @return the data at local with ssd weight of this region in the regionserver
   */
  long getBlocksLocalWithSsdWeight();

  /** Returns the block total weight of this region */
  long getBlocksTotalWeight();

  /** Returns the compaction state of this region */
  CompactionState getCompactionState();

  /** Returns the total size of the hfiles in the region */
  Size getRegionSizeMB();

  /** Returns current prefetch ratio of this region on this server */
  float getCurrentRegionCachedRatio();
}
