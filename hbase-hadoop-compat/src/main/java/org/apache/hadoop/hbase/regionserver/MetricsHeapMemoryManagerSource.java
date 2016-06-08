/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsHeapMemoryManagerSource extends BaseSource {
  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Heap Memory";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer's heap memory";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  /**
   * Update the current heap size in use
   */
  void updateCurHeapSize(long heapSize);

  /**
   * Update the heap occupancy histogram
   * 
   * @param heapOccupancyPercent the percentage of heap usage: Used / Committed.
   */
  void updateHeapOccupancy(float heapOccupancyPercent);

  /**
   * Update the cache evicted count histogram
   * 
   * @param evictCount the number of cache eviction since last tuning.
   */
  void updateCacheEvictCount(long cacheEvictedCount);

  /**
   * Update the cache miss count histogram
   * 
   * @param cacheMissCount the number of cache miss since last tuning.
   */
  void updateCacheMissCount(long cacheMissCount);

  /**
   * Update the blocked flush count histogram
   * 
   * @param bFlushCount the number of blocked flush since last tuning.
   */
  void updateBlockedFlushCount(long bFlushCount);

  /**
   * Update the unblocked flush count histogram
   * 
   * @param unbFlushCount the number of unblocked flush since last tuning.
   */
  void updateUnblockedFlushCount(long unbFlushCount);

  /**
   * Update the current blockcache(in percentage) used histogram
   * 
   * @param curBlockCacheUsed the current memory usage in blockcache.
   */
  void updateCurBlockCachePercentage(float curBlockCacheUsed);

  /**
   * Update the current memstore used(in percentage) histogram
   * 
   * @param curMemStoreUsed the current memory usage in memstore.
   */
  void updateCurMemStorePercentage(float curMemStoreUsed);

  /**
   * Update the current blockcache(in size) used histogram
   * 
   * @param blockcacheSize
   */
  void updateCurBlockCacheSize(long blockcacheSize);

  /**
   * Update the current memstore used(in size) histogram
   * 
   * @param memstoreSize
   */
  void updateCurMemStoreSize(long memstoreSize);

  /**
   * Update the increase/decrease memstore size histogram
   * 
   * @param deltaMemStoreSize the tuning result of memstore.
   */
  void updateDeltaMemStoreSize(int deltaMemStoreSize);

  /**
   * Update the increase/decrease blockcache size histogram
   * 
   * @param deltaBlockCacheSize the tuning result of blockcache.
   */
  void updateDeltaBlockCacheSize(int deltaBlockCacheSize);

  /**
   * Update the counter for no change situation to the tuning result.
   */
  void updateHeapMemoryNoChangeCount();

  String HEAP_SIZE_NAME = "Heap Size in Use";
  String HEAP_SIZE_DESC = "Heap used currently";

  String HEAP_OCCUPANCY_NAME = "Heap Occupancy";
  String HEAP_OCCUPANCY_DESC = "Used Heap / Commited Heap";

  String CACHE_EVICTED_NAME = "Cache Block Evicted";
  String CACHE_EVICTED_DESC = "The occurrence of cache block evicted since last tuning";

  String CACHE_MISS_NAME = "Cache Miss";
  String CACHE_MISS_DESC = "The occurrence of cache miss since last tuning";

  String BLOCKED_FLUSH_NAME = "Blocked Flush";
  String BLOCKED_FLUSH_DESC = "The occurrence of blocked flush since last tuning";

  String UNBLOCKED_FLUSH_NAME = "Unblocked Flush";
  String UNBLOCKED_FLUSH_DESC = "The occurrence of unblocked flush since last tuning";

  String CUR_MEMSTORE_USED_NAME = "MemStore Usage in Percentage";
  String CUR_MEMSTORE_USED_DESC = "(Used MemStore size / Max heap size) * 100";

  String CUR_BLOCKCACHE_USED_NAME = "BlockCache Usage in Percentage";
  String CUR_BLOCKCACHE_USED_DESC = "(Used BlockCache size / Max heap size) * 100";

  String CUR_MEMSTORE_SIZE_NAME = "MemStore Usage in Size";
  String CUR_MEMSTORE_SIZE_DESC = "Used MemStore size";

  String CUR_BLOCKCACHE_SIZE_NAME = "BlockCache Usage in Size";
  String CUR_BLOCKCACHE_SIZE_DESC = "Used BlockCache size";

  String INC_MEMSTORE_TUNING_NAME = "Increase MemStore size";
  String INC_MEMSTORE_TUNING_DESC = "The tuning result is to increase memstore size";

  String DRC_MEMSTORE_TUNING_NAME = "Decrease MemStore size";
  String DRC_MEMSTORE_TUNING_DESC = "The tuning result is to decrease memstore size";

  String INC_MEMSTORE_COUNTER_NAME = "MemStore Increase Counter";
  String INC_MEMSTORE_COUNTER_DESC = "The number of times that the memstore needs to expand";

  String INC_BLOCKCACHE_TUNING_NAME = "Increase BlockCache size";
  String INC_BLOCKCACHE_TUNING_DESC = "The tuning result is to increase blockcache size";

  String DRC_BLOCKCACHE_TUNING_NAME = "Decrease BlockCache size";
  String DRC_BLOCKCACHE_TUNING_DESC = "The tuning result is to decrease memstore size";

  String INC_BLOCKCACHE_COUNTER_NAME = "BlockCache Increase Counter";
  String INC_BLOCKCACHE_COUNTER_DESC = "The number of times that the blockcache needs to expand";

  String INC_NOCHANGE_COUNTER_NAME = "NoChange Increase Counter";
  String INC_NOCHANGE_COUNTER_DESC = "The number of times that the heap memory needs no change";

  String CACHE_EVICTED_COUNTER_NAME = "Total Cache Block Evicted";
  String CACHE_EVICTED_COUNTER_DESC = "Total occurrence of cache block evicted";

  String CACHE_MISS_COUNTER_NAME = "Total Cache Miss";
  String CACHE_MISS_COUNTER_DESC = "Total occurrence of cache miss";

  String BLOCKED_FLUSH_COUNTER_NAME = "Total Blocked Flush count";
  String BLOCKED_FLUSH_COUNTER_DESC = "Total occurrence of blocked flush";

  String UNBLOCKED_FLUSH_COUNTER_NAME = "Total Unblocked Flush count";
  String UNBLOCKED_FLUSH_COUNTER_DESC = "Total occurrence of unblocked flush";

  String MAX_HEAP_SIZE_NAME = "Max Heap Size";
  String MAX_HEAP_SIZE_DESC = "Max heap size can be used";

  String HEAP_USE_PERCENT_NAME = "Used Heap in percentage";
  String HEAP_USE_PERCENT_DESC = "Heap are used by the RegionServer";

  String HEAP_USE_SIZE_NAME = "Used Heap in size";
  String HEAP_USE_SIZE_DESC = "Heap size are used by the RegionServer";

  String BLOCKCACHE_SIZE_NAME = "Size of BlockCache in Use";
  String BLOCKCACHE_SIZE_DESC = "In use BlockCache size";

  String BLOCKCACHE_PERCENT_NAME = "BlockCache Used in Percentage";
  String BLOCKCACHE_PERCENT_DESC = "BlockCache Used / Max Heap";

  String MEMSTORE_SIZE_NAME = "Size of MemStore in Use";
  String MEMSTORE_SIZE_DESC = "In use MemStore size";

  String MEMSTORE_PERCENT_NAME = "MemStore Used in Percentage";
  String MEMSTORE_PERCENT_DESC = "MemStore Used / Max Heap";
}
