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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * RegionServerAccounting keeps record of some basic real time information about
 * the Region Server. Currently, it only keeps record the global memstore size. 
 */
public class RegionServerAccounting {

  private final AtomicLong atomicGlobalMemstoreSize = new AtomicLong(0);
  
  // Store the edits size during replaying HLog. Use this to roll back the  
  // global memstore size once a region opening failed.
  private final ConcurrentMap<byte[], AtomicLong> replayEditsPerRegion = 
    new ConcurrentSkipListMap<byte[], AtomicLong>(Bytes.BYTES_COMPARATOR);
  
  /**
   * @return the global Memstore size in the RegionServer
   */
  public long getGlobalMemstoreSize() {
    return atomicGlobalMemstoreSize.get();
  }
  
  /**
   * @param memStoreSize the Memstore size will be added to 
   *        the global Memstore size 
   * @return the global Memstore size in the RegionServer 
   */
  public long addAndGetGlobalMemstoreSize(long memStoreSize) {
    return atomicGlobalMemstoreSize.addAndGet(memStoreSize);
  }

  /***
   * Add memStoreSize to replayEditsPerRegion.
   * 
   * @param regionName region name.
   * @param memStoreSize the Memstore size will be added to replayEditsPerRegion.
   * @return the replay edits size for region hri.
   */
  public long addAndGetRegionReplayEditsSize(byte[] regionName, long memStoreSize) {
    AtomicLong replayEdistsSize = replayEditsPerRegion.get(regionName);
    if (replayEdistsSize == null) {
      replayEdistsSize = new AtomicLong(0);
      replayEditsPerRegion.put(regionName, replayEdistsSize);
    }
    return replayEdistsSize.addAndGet(memStoreSize);
  }

  /**
   * Roll back the global MemStore size for a specified region when this region
   * can't be opened.
   * 
   * @param regionName the region which could not open.
   * @return the global Memstore size in the RegionServer
   */
  public long rollbackRegionReplayEditsSize(byte[] regionName) {
    AtomicLong replayEditsSize = replayEditsPerRegion.get(regionName);
    long editsSizeLong = 0L;
    if (replayEditsSize != null) {
      editsSizeLong = -replayEditsSize.get();
      clearRegionReplayEditsSize(regionName);
    }
    return addAndGetGlobalMemstoreSize(editsSizeLong);
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
