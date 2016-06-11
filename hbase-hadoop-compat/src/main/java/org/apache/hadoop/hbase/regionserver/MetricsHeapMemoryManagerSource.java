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

/**
 * This interface will be implemented by a MetricsSource that will export metrics from
 * HeapMemoryManager in RegionServer into the hadoop metrics system.
 */
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
   * Update the cache evicted count histogram
   * @param cacheEvictedCount the number of cache block evicted since last tuning.
   */
  void updateCacheEvictedCount(long cacheEvictedCount);

  /**
   * Update the cache miss count histogram
   * @param cacheMissCount the number of cache miss since last tuning.
   */
  void updateCacheMissCount(long cacheMissCount);

  /**
   * Update the blocked flush count histogram
   * @param blockedFlushCount the number of blocked flush since last tuning.
   */
  void updateBlockedFlushCount(long blockedFlushCount);

  /**
   * Update the unblocked flush count histogram
   * @param unblockedFlushCount the number of unblocked flush since last tuning.
   */
  void updateUnblockedFlushCount(long unblockedFlushCount);

  /**
   * Update the current blockcache(in percent) used histogram
   * @param curBlockCacheUsed the current memory usage in blockcache. Used / Max Heap.
   */
  void updateCurBlockCachePercent(float curBlockCacheUsed);

  /**
   * Update the current memstore used(in percent) histogram
   * @param curMemStoreUsed the current memory usage in memstore. Used / Max Heap.
   */
  void updateCurMemStorePercent(float curMemStoreUsed);

  /**
   * Update the current blockcache(in size) used histogram
   * @param blockCacheSize the current memory usage in blockcache in size.
   */
  void updateCurBlockCacheSize(long blockCacheSize);

  /**
   * Update the current memstore used(in size) histogram
   * @param memStoreSize the current memory usage in memstore in size.
   */
  void updateCurMemStoreSize(long memStoreSize);

  /**
   * Update the increase/decrease memstore size histogram
   * @param memStoreDeltaSize the tuning result of memstore.
   */
  void updateMemStoreDeltaSize(int memStoreDeltaSize);

  /**
   * Update the increase/decrease blockcache size histogram
   * @param blockCacheDeltaSize the tuning result of blockcache.
   */
  void updateBlockCacheDeltaSize(int blockCacheDeltaSize);

  /**
   * Update the counter for tuner neither expanding memstore global size limit nor expanding
   * blockcache max size.
   */
  void updateTunerDoNothingCount();

  /**
   * Update the counter for heap occupancy percent above low watermark
   */
  void updateAboveHeapOccupancyLowWatermarkCount();

  String CACHE_EVICTED_NAME = "cacheBlockEvicted";
  String CACHE_EVICTED_DESC = "Histogram for number of cache blocks evicted";
  String CACHE_MISS_NAME = "cacheMiss";
  String CACHE_MISS_DESC = "Histogram for number of cache miss";
  String BLOCKED_FLUSH_NAME = "blockedFlushes";
  String BLOCKED_FLUSH_DESC = "Histogram for number of blocked flushes in the memstore";
  String UNBLOCKED_FLUSH_NAME = "unblockedFlushes";
  String UNBLOCKED_FLUSH_DESC = "Histogram for number of unblocked flushes in the memstore";
  String CUR_MEMSTORE_PERCENT_NAME = "memStoreUsedInPercent";
  String CUR_MEMSTORE_PERCENT_DESC =
      "Histogram for percentage of ((Used MemStore size / Max heap size) * 100)";
  String CUR_BLOCKCACHE_PERCENT_NAME = "blockCacheUsedInPercent";
  String CUR_BLOCKCACHE_PERCENT_DESC =
      "Histogram for percentage of ((Used BlockCache size / Max heap size) * 100)";
  String CUR_MEMSTORE_SIZE_NAME = "memStoreUsedInSize";
  String CUR_MEMSTORE_SIZE_DESC = "Histogram for memstore usage in bytes";
  String CUR_BLOCKCACHE_SIZE_NAME = "blockCacheUsedInSize";
  String CUR_BLOCKCACHE_SIZE_DESC = "Histogram for blockcache usage in bytes";
  String INC_MEMSTORE_TUNING_NAME = "increaseMemStoreSize";
  String INC_MEMSTORE_TUNING_DESC =
      "Histogram for the heap memory tuner expanding memstore global size limit in bytes";
  String INC_BLOCKCACHE_TUNING_NAME = "increaseBlockCacheSize";
  String INC_BLOCKCACHE_TUNING_DESC =
      "Histogram for the heap memory tuner expanding blockcache max heap size in bytes";
  String INC_MEMSTORE_COUNTER_NAME = "memStoreIncrementCounter";
  String INC_MEMSTORE_COUNTER_DESC =
      "The number of times that tuner expands memstore global size limit";
  String INC_BLOCKCACHE_COUNTER_NAME = "blockCacheIncrementCounter";
  String INC_BLOCKCACHE_COUNTER_DESC =
      "The number of times that tuner expands blockcache max heap size";
  String DO_NOTHING_COUNTER_NAME = "tunerDoNothingCounter";
  String DO_NOTHING_COUNTER_DESC =
      "The number of times that tuner neither expands memstore global size limit nor expands blockcache max size";
  String ABOVE_HEAP_LOW_WATERMARK_COUNTER_NAME = "aboveHeapOccupancyLowWaterMarkCounter";
  String ABOVE_HEAP_LOW_WATERMARK_COUNTER_DESC =
      "The number of times that heap occupancy percent is above low watermark";

  String BLOCKCACHE_SIZE_NAME = "blockCacheUsedInSize";
  String BLOCKCACHE_SIZE_DESC = "BlockCache are used in bytes by the RegionServer";
  String BLOCKCACHE_PERCENT_NAME = "blockCacheUsedInPercent";
  String BLOCKCACHE_PERCENT_DESC = "BlockCache Used / Max Heap";
  String MEMSTORE_SIZE_NAME = "memStoreUsedInSize";
  String MEMSTORE_SIZE_DESC = "MemStore are used in bytes by the RegionServer";
  String MEMSTORE_PERCENT_NAME = "memStoreUsedInPercent";
  String MEMSTORE_PERCENT_DESC = "MemStore Used / Max Heap";
}
