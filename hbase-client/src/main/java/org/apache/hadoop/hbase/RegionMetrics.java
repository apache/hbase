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

  /** Return the region name */
  byte[] getRegionName();

  /** Return the number of stores */
  int getStoreCount();

  /** Return the number of storefiles */
  int getStoreFileCount();

  /** Return the total size of the storefiles */
  Size getStoreFileSize();

  /** Return the memstore size */
  Size getMemStoreSize();

  /** Return the number of read requests made to region */
  long getReadRequestCount();

  /** Return the number of write requests made to region */
  long getWriteRequestCount();

  /** Return the number of coprocessor service requests made to region */
  public long getCpRequestCount();

  /**
   * Return the number of write requests and read requests and coprocessor service requests made to
   * region
   */
  default long getRequestCount() {
    return getReadRequestCount() + getWriteRequestCount() + getCpRequestCount();
  }

  /** Return the region name as a string */
  default String getNameAsString() {
    return Bytes.toStringBinary(getRegionName());
  }

  /** Return the number of filtered read requests made to region */
  long getFilteredReadRequestCount();

  /**
   * Return the current total size of root-level indexes for the region.
   * <p>
   * TODO: why we pass the same value to different counters? Currently, the value from
   * getStoreFileIndexSize() is same with getStoreFileRootLevelIndexSize() see
   * HRegionServer#createRegionLoad.
   */
  Size getStoreFileIndexSize();

  /** Return the current total size of root-level indexes for the region */
  Size getStoreFileRootLevelIndexSize();

  /** Return the total size of all index blocks, not just the root level */
  Size getStoreFileUncompressedDataIndexSize();

  /** Return the total size of all Bloom filter blocks, not just loaded into the block cache */
  Size getBloomFilterSize();

  /** Return the total number of cells in current compaction */
  long getCompactingCellCount();

  /** Return the number of already compacted kvs in current compaction */
  long getCompactedCellCount();

  /**
   * Return the completed sequence Id for the region.
   * <p>
   * This does not really belong inside RegionLoad but its being done in the name of expediency.
   */
  long getCompletedSequenceId();

  /** Return the completed sequence id per store. */
  Map<byte[], Long> getStoreSequenceId();

  /** Return the uncompressed size of the storefiles */
  Size getUncompressedStoreFileSize();

  /** Return the data locality of region in the regionserver. */
  float getDataLocality();

  /** Return the timestamp of the oldest hfile for any store of this region. */
  long getLastMajorCompactionTimestamp();

  /** Return the reference count for the stores of this region */
  int getStoreRefCount();

  /**
   * Return the max reference count for any store file among all compacted stores files of this
   * region
   */
  int getMaxCompactedStoreFileRefCount();

  /**
   * Return the data locality for ssd of region in the regionserver.
   * <p>
   * Different from dataLocality,this metric's numerator only include the data stored on ssd
   */
  float getDataLocalityForSsd();

  /** Return the data at local weight of this region in the regionserver */
  long getBlocksLocalWeight();

  /**
   * Return the data at local with ssd weight of this region in the regionserver
   * <p>
   * Different from blocksLocalWeight,this metric's numerator only include the data stored on ssd
   */
  long getBlocksLocalWithSsdWeight();

  /** Return the block total weight of this region */
  long getBlocksTotalWeight();

  /** Return the compaction state of this region */
  CompactionState getCompactionState();
}
