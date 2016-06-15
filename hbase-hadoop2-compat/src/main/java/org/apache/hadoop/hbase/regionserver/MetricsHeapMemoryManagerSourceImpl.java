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

import java.lang.management.ManagementFactory;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Hadoop2 implementation of MetricsHeapMemoryManagerSource. Implements BaseSource through
 * BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsHeapMemoryManagerSourceImpl extends BaseSourceImpl implements
    MetricsHeapMemoryManagerSource {
  
  private long maxHeapSize = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();

  private final MetricHistogram blockedFlushHistogram;
  private final MetricHistogram unblockedFlushHistogram;
  private final MetricHistogram incMemStoreSizeHistogram;
  private final MetricHistogram decMemStoreSizeHistogram;
  private final MetricHistogram incBlockCacheSizeHistogram;
  private final MetricHistogram decBlockCacheSizeHistogram;

  private final MutableGaugeLong blockedFlushGauge;
  private final MutableGaugeLong unblockedFlushGauge;
  private final MutableGaugeLong memStoreSizeGauge;
  private final MutableGaugeLong blockCacheSizeGauge;
  private final MutableGaugeLong newGlobalMemStoreSizeGauge;
  private final MutableGaugeLong newBlockCacheSizeGauge;

  private final MutableFastCounter doNothingCounter;
  private final MutableFastCounter aboveHeapOccupancyLowWatermarkCounter;

  public MetricsHeapMemoryManagerSourceImpl(float globalMemStorePercent, float blockCachePercent) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT,
        globalMemStorePercent, blockCachePercent);
  }

  public MetricsHeapMemoryManagerSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext, float globalMemStorePercent,
      float blockCachePercent) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    // Histograms
    blockedFlushHistogram = getMetricsRegistry()
        .newSizeHistogram(BLOCKED_FLUSH_NAME, BLOCKED_FLUSH_DESC);
    unblockedFlushHistogram = getMetricsRegistry()
        .newSizeHistogram(UNBLOCKED_FLUSH_NAME, UNBLOCKED_FLUSH_DESC);
    incMemStoreSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(INC_MEMSTORE_TUNING_NAME, INC_MEMSTORE_TUNING_DESC);
    decMemStoreSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(DEC_MEMSTORE_TUNING_NAME, DEC_MEMSTORE_TUNING_DESC);
    incBlockCacheSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(INC_BLOCKCACHE_TUNING_NAME, INC_BLOCKCACHE_TUNING_DESC);
    decBlockCacheSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(DEC_BLOCKCACHE_TUNING_NAME, DEC_BLOCKCACHE_TUNING_DESC);

    // Gauges
    blockedFlushGauge = getMetricsRegistry()
        .newGauge(BLOCKED_FLUSH_GAUGE_NAME, BLOCKED_FLUSH_GAUGE_DESC, 0L);
    unblockedFlushGauge = getMetricsRegistry()
        .newGauge(UNBLOCKED_FLUSH_GAUGE_NAME, UNBLOCKED_FLUSH_GAUGE_DESC, 0L);
    memStoreSizeGauge = getMetricsRegistry()
        .newGauge(MEMSTORE_SIZE_GAUGE_NAME, MEMSTORE_SIZE_GAUGE_DESC, 0L);
    blockCacheSizeGauge = getMetricsRegistry()
        .newGauge(BLOCKCACHE_SIZE_GAUGE_NAME, BLOCKCACHE_SIZE_GAUGE_DESC, 0L);
    newGlobalMemStoreSizeGauge = getMetricsRegistry()
        .newGauge(NEW_MEMSTORE_SIZE_GAUGE_NAME, NEW_MEMSTORE_SIZE_GAUGE_DESC,
                            (long) (maxHeapSize * globalMemStorePercent));
    newBlockCacheSizeGauge = getMetricsRegistry()
        .newGauge(NEW_BLOCKCACHE_SIZE_GAUGE_NAME, NEW_BLOCKCACHE_SIZE_GAUGE_DESC,
                            (long) (maxHeapSize * blockCachePercent));

    // Counters
    doNothingCounter = getMetricsRegistry()
        .newCounter(DO_NOTHING_COUNTER_NAME, DO_NOTHING_COUNTER_DESC, 0L);
    aboveHeapOccupancyLowWatermarkCounter = getMetricsRegistry()
        .newCounter(ABOVE_HEAP_LOW_WATERMARK_COUNTER_NAME, 
                              ABOVE_HEAP_LOW_WATERMARK_COUNTER_DESC, 0L);
  }

  @Override
  public void updateBlockedFlushCount(long blockedFlushCount) {
    blockedFlushHistogram.add(blockedFlushCount);
    blockedFlushGauge.set(blockedFlushCount);
  }

  @Override
  public void updateUnblockedFlushCount(long unblockedFlushCount) {
    unblockedFlushHistogram.add(unblockedFlushCount);
    unblockedFlushGauge.set(unblockedFlushCount);
  }

  @Override
  public void setCurBlockCacheSizeGauge(long blockcacheSize) {
    blockCacheSizeGauge.set(blockcacheSize);
  }

  @Override
  public void setCurMemStoreSizeGauge(long memstoreSize) {
    memStoreSizeGauge.set(memstoreSize);
  }

  @Override
  public void setNewBlockCacheMaxSizeGauge(float newMaxBlockCacheSize) {
    newBlockCacheSizeGauge.set((long) (newMaxBlockCacheSize * maxHeapSize));
  }

  @Override
  public void setNewGlobalMemStoreSizeLimitGauge(float newGlobalMemStoreSize) {
    newGlobalMemStoreSizeGauge.set((long) (newGlobalMemStoreSize * maxHeapSize));
  }

  @Override
  public void updateMemStoreDeltaSizeHistogram(int memStoreDeltaSize) {
    if (memStoreDeltaSize >= 0) {
      incMemStoreSizeHistogram.add(memStoreDeltaSize);
    } else if (memStoreDeltaSize < 0) {
      decMemStoreSizeHistogram.add(-memStoreDeltaSize);
    }
  }

  @Override
  public void updateBlockCacheDeltaSizeHistogram(int blockCacheDeltaSize) {
    if (blockCacheDeltaSize >= 0) {
      incBlockCacheSizeHistogram.add(blockCacheDeltaSize);
    } else if (blockCacheDeltaSize < 0) {
      decBlockCacheSizeHistogram.add(-blockCacheDeltaSize);
    }
  }

  @Override
  public void increaseTunerDoNothingCounter() {
    doNothingCounter.incr();
  }

  @Override
  public void increaseAboveHeapOccupancyLowWatermarkCounter() {
    aboveHeapOccupancyLowWatermarkCounter.incr();
  }
}
