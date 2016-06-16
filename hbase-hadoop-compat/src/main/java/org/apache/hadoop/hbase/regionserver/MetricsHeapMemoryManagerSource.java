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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * This interface will be implemented by a MetricsSource that will export metrics from
 * HeapMemoryManager in RegionServer into the hadoop metrics system.
 */
@InterfaceAudience.Private
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
   * Update/Set the blocked flush count histogram/gauge
   * @param blockedFlushCount the number of blocked flush since last tuning.
   */
  void updateBlockedFlushCount(long blockedFlushCount);

  /**
   * Update/Set the unblocked flush count histogram/gauge
   * @param unblockedFlushCount the number of unblocked flush since last tuning.
   */
  void updateUnblockedFlushCount(long unblockedFlushCount);

  /**
   * Set the current blockcache size used gauge
   * @param blockCacheSize the current memory usage in blockcache, in bytes.
   */
  void setCurBlockCacheSizeGauge(long blockCacheSize);

  /**
   * Set the current global memstore size used gauge
   * @param memStoreSize the current memory usage in memstore, in bytes.
   */
  void setCurMemStoreSizeGauge(long memStoreSize);

  /**
   * Update the increase/decrease memstore size histogram
   * @param memStoreDeltaSize the tuning result of memstore.
   */
  void updateMemStoreDeltaSizeHistogram(int memStoreDeltaSize);

  /**
   * Update the increase/decrease blockcache size histogram
   * @param blockCacheDeltaSize the tuning result of blockcache.
   */
  void updateBlockCacheDeltaSizeHistogram(int blockCacheDeltaSize);

  /**
   * Increase the counter for tuner neither expanding memstore global size limit nor expanding
   * blockcache max size.
   */
  void increaseTunerDoNothingCounter();

  /**
   * Increase the counter for heap occupancy percent above low watermark
   */
  void increaseAboveHeapOccupancyLowWatermarkCounter();

  // Histograms
  String BLOCKED_FLUSH_NAME = "blockedFlushes";
  String BLOCKED_FLUSH_DESC = "Histogram for the number of blocked flushes in the memstore";
  String UNBLOCKED_FLUSH_NAME = "unblockedFlushes";
  String UNBLOCKED_FLUSH_DESC = "Histogram for the number of unblocked flushes in the memstore";
  String INC_MEMSTORE_TUNING_NAME = "increaseMemStoreSize";
  String INC_MEMSTORE_TUNING_DESC =
      "Histogram for the heap memory tuner expanding memstore global size limit in bytes";
  String DEC_MEMSTORE_TUNING_NAME = "decreaseMemStoreSize";
  String DEC_MEMSTORE_TUNING_DESC =
      "Histogram for the heap memory tuner shrinking memstore global size limit in bytes";
  String INC_BLOCKCACHE_TUNING_NAME = "increaseBlockCacheSize";
  String INC_BLOCKCACHE_TUNING_DESC =
      "Histogram for the heap memory tuner expanding blockcache max heap size in bytes";
  String DEC_BLOCKCACHE_TUNING_NAME = "decreaseBlockCacheSize";
  String DEC_BLOCKCACHE_TUNING_DESC =
      "Histogram for the heap memory tuner shrinking blockcache max heap size in bytes";

  // Gauges
  String BLOCKED_FLUSH_GAUGE_NAME = "blockedFlushCount";
  String BLOCKED_FLUSH_GAUGE_DESC = "Gauge for the blocked flush count before tuning";
  String UNBLOCKED_FLUSH_GAUGE_NAME = "unblockedFlushCount";
  String UNBLOCKED_FLUSH_GAUGE_DESC = "Gauge for the unblocked flush count before tuning";
  String MEMSTORE_SIZE_GAUGE_NAME = "memStoreSize";
  String MEMSTORE_SIZE_GAUGE_DESC = "Global MemStore used in bytes by the RegionServer";
  String BLOCKCACHE_SIZE_GAUGE_NAME = "blockCacheSize";
  String BLOCKCACHE_SIZE_GAUGE_DESC = "BlockCache used in bytes by the RegionServer";

  // Counters
  String DO_NOTHING_COUNTER_NAME = "tunerDoNothingCounter";
  String DO_NOTHING_COUNTER_DESC =
      "The number of times that tuner neither expands memstore global size limit nor expands blockcache max size";
  String ABOVE_HEAP_LOW_WATERMARK_COUNTER_NAME = "aboveHeapOccupancyLowWaterMarkCounter";
  String ABOVE_HEAP_LOW_WATERMARK_COUNTER_DESC =
      "The number of times that heap occupancy percent is above low watermark";
}
