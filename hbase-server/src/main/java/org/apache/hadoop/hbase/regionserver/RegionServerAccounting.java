/**
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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * RegionServerAccounting keeps record of some basic real time information about
 * the Region Server. Currently, it only keeps record the global memstore size. 
 */
@InterfaceAudience.Private
public class RegionServerAccounting {

  private final AtomicLong globalMemstoreDataSize = new AtomicLong(0);
  private final AtomicLong globalMemstoreHeapOverhead = new AtomicLong(0);

  // Store the edits size during replaying WAL. Use this to roll back the  
  // global memstore size once a region opening failed.
  private final ConcurrentMap<byte[], MemstoreSize> replayEditsPerRegion =
    new ConcurrentSkipListMap<byte[], MemstoreSize>(Bytes.BYTES_COMPARATOR);

  /**
   * @return the global Memstore size in the RegionServer
   */
  public long getGlobalMemstoreSize() {
    return globalMemstoreDataSize.get();
  }

  public long getGlobalMemstoreHeapOverhead() {
    return this.globalMemstoreHeapOverhead.get();
  }

  /**
   * @param memStoreSize the Memstore size will be added to 
   *        the global Memstore size 
   */
  public void incGlobalMemstoreSize(MemstoreSize memStoreSize) {
    globalMemstoreDataSize.addAndGet(memStoreSize.getDataSize());
    globalMemstoreHeapOverhead.addAndGet(memStoreSize.getHeapOverhead());
  }

  public void decGlobalMemstoreSize(MemstoreSize memStoreSize) {
    globalMemstoreDataSize.addAndGet(-memStoreSize.getDataSize());
    globalMemstoreHeapOverhead.addAndGet(-memStoreSize.getHeapOverhead());
  }

  /***
   * Add memStoreSize to replayEditsPerRegion.
   * 
   * @param regionName region name.
   * @param memStoreSize the Memstore size will be added to replayEditsPerRegion.
   */
  public void addRegionReplayEditsSize(byte[] regionName, MemstoreSize memStoreSize) {
    MemstoreSize replayEdistsSize = replayEditsPerRegion.get(regionName);
    // All ops on the same MemstoreSize object is going to be done by single thread, sequentially
    // only. First calls to this method to increment the per region reply edits size and then call
    // to either rollbackRegionReplayEditsSize or clearRegionReplayEditsSize as per the result of
    // the region open operation. No need to handle multi thread issues on one region's entry in
    // this Map.
    if (replayEdistsSize == null) {
      replayEdistsSize = new MemstoreSize();
      replayEditsPerRegion.put(regionName, replayEdistsSize);
    }
    replayEdistsSize.incMemstoreSize(memStoreSize);
  }

  /**
   * Roll back the global MemStore size for a specified region when this region
   * can't be opened.
   * 
   * @param regionName the region which could not open.
   */
  public void rollbackRegionReplayEditsSize(byte[] regionName) {
    MemstoreSize replayEditsSize = replayEditsPerRegion.get(regionName);
    if (replayEditsSize != null) {
      clearRegionReplayEditsSize(regionName);
      decGlobalMemstoreSize(replayEditsSize);
    }
  }

  /**
   * Clear a region from replayEditsPerRegion.
   * 
   * @param regionName region name.
   */
  public void clearRegionReplayEditsSize(byte[] regionName) {
    replayEditsPerRegion.remove(regionName);
  }
}
