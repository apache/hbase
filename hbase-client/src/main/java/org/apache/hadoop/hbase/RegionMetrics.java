/**
 * Copyright The Apache Software Foundation
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

  /**
   * @return the region name
   */
  byte[] getRegionName();

  /**
   * @return the number of stores
   */
  int getStoreCount();

  /**
   * @return the number of storefiles
   */
  int getStoreFileCount();

  /**
   * @return the total size of the storefiles
   */
  Size getStoreFileSize();

  /**
   * @return the memstore size
   */
  Size getMemStoreSize();

  /**
   * @return the number of read requests made to region
   */
  long getReadRequestCount();

  /**
   * @return the number of write requests made to region
   */
  long getWriteRequestCount();

  /**
   * @return the number of write requests and read requests made to region
   */
  default long getRequestCount() {
    return getReadRequestCount() + getWriteRequestCount();
  }

  /**
   * @return the region name as a string
   */
  default String getNameAsString() {
    return Bytes.toStringBinary(getRegionName());
  }

  /**
   * @return the number of filtered read requests made to region
   */
  long getFilteredReadRequestCount();

  /**
   * TODO: why we pass the same value to different counters? Currently, the value from
   * getStoreFileIndexSize() is same with getStoreFileRootLevelIndexSize()
   * see HRegionServer#createRegionLoad.
   * @return The current total size of root-level indexes for the region
   */
  Size getStoreFileIndexSize();

  /**
   * @return The current total size of root-level indexes for the region
   */
  Size getStoreFileRootLevelIndexSize();

  /**
   * @return The total size of all index blocks, not just the root level
   */
  Size getStoreFileUncompressedDataIndexSize();

  /**
   * @return The total size of all Bloom filter blocks, not just loaded into the block cache
   */
  Size getBloomFilterSize();

  /**
   * @return the total number of cells in current compaction
   */
  long getCompactingCellCount();

  /**
   * @return the number of already compacted kvs in current compaction
   */
  long getCompactedCellCount();

  /**
   * This does not really belong inside RegionLoad but its being done in the name of expediency.
   * @return the completed sequence Id for the region
   */
  long getCompletedSequenceId();

  /**
   * @return completed sequence id per store.
   */
  Map<byte[], Long> getStoreSequenceId();


  /**
   * @return the uncompressed size of the storefiles
   */
  Size getUncompressedStoreFileSize();

  /**
   * @return the data locality of region in the regionserver.
   */
  float getDataLocality();

  /**
   * @return the timestamp of the oldest hfile for any store of this region.
   */
  long getLastMajorCompactionTimestamp();

  /**
   * @return the reference count for the stores of this region
   */
  int getStoreRefCount();

  /**
   * @return the max reference count for any store file among all compacted stores files
   *   of this region
   */
  int getMaxCompactedStoreFileRefCount();

  /**
   * Different from dataLocality,this metric's numerator only include the data stored on ssd
   * @return the data locality for ssd of region in the regionserver
   */
  float getDataLocalityForSsd();

  /**
   * @return the data at local weight of this region in the regionserver
   */
  long getBlocksLocalWeight();

  /**
   * Different from blocksLocalWeight,this metric's numerator only include the data stored on ssd
   * @return the data at local with ssd weight of this region in the regionserver
   */
  long getBlocksLocalWithSsdWeight();

  /**
   * @return the block total weight of this region
   */
  long getBlocksTotalWeight();

  /**
   * @return the compaction state of this region
   */
  CompactionState getCompactionState();
}
