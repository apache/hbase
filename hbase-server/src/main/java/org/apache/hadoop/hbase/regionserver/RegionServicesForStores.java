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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Services a Store needs from a Region.
 * RegionServicesForStores class is the interface through which memstore access services at the
 * region level.
 * For example, when using alternative memory formats or due to compaction the memstore needs to
 * take occasional lock and update size counters at the region level.
 */
@InterfaceAudience.Private
public class RegionServicesForStores {

  private final HRegion region;
  private final RegionServerServices rsServices;
  private int inMemoryPoolSize;

  public RegionServicesForStores(HRegion region, RegionServerServices rsServices) {
    this.region = region;
    this.rsServices = rsServices;
    if (this.rsServices != null) {
      this.inMemoryPoolSize = rsServices.getConfiguration().getInt(
        CompactingMemStore.IN_MEMORY_CONPACTION_POOL_SIZE_KEY,
        CompactingMemStore.IN_MEMORY_CONPACTION_POOL_SIZE_DEFAULT);
    }
  }

  public void blockUpdates() {
    region.blockUpdates();
  }

  public void unblockUpdates() {
    region.unblockUpdates();
  }

  public void addMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
      int cellsCountDelta) {
    region.incMemStoreSize(dataSizeDelta, heapSizeDelta, offHeapSizeDelta, cellsCountDelta);
  }

  public RegionInfo getRegionInfo() {
    return region.getRegionInfo();
  }

  public WAL getWAL() {
    return region.getWAL();
  }

  ThreadPoolExecutor getInMemoryCompactionPool() {
    if (rsServices != null) {
      return rsServices.getExecutorService().getExecutorLazily(ExecutorType.RS_IN_MEMORY_COMPACTION,
        inMemoryPoolSize);
    } else {
      return null;
    }
  }

  public long getMemStoreFlushSize() {
    return region.getMemStoreFlushSize();
  }

  public int getNumStores() {
    return region.getTableDescriptor().getColumnFamilyCount();
  }

  @VisibleForTesting
  long getMemStoreSize() {
    return region.getMemStoreDataSize();
  }
}
