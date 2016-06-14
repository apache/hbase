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
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Hadoop2 implementation of MetricsHeapMemoryManagerSource. Implements BaseSource through
 * BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsHeapMemoryManagerSourceImpl extends BaseSourceImpl implements
    MetricsHeapMemoryManagerSource {

  private final MetricHistogram blockedFlushCountHistogram;
  private final MetricHistogram unblockedFlushCountHistogram;
  private final MetricHistogram curMemStoreUsedSizeHistogram;
  private final MetricHistogram curBlockCacheUsedSizeHistogram;
  private final MetricHistogram incMemStoreSizeHistogram;
  private final MetricHistogram decMemStoreSizeHistogram;
  private final MetricHistogram incBlockCacheSizeHistogram;
  private final MetricHistogram decBlockCacheSizeHistogram;

  private final MutableGaugeLong blockedFlushCountGauge;
  private final MutableGaugeLong unblockedFlushCountGauge;
  private final MutableGaugeLong memStoreSizeGauge;
  private final MutableGaugeLong blockCacheSizeGauge;

  private final MutableFastCounter memStoreIncCounter;
  private final MutableFastCounter memStoreDecCounter;
  private final MutableFastCounter blockCacheInrCounter;
  private final MutableFastCounter blockCacheDecCounter;
  private final MutableFastCounter doNothingCounter;
  private final MutableFastCounter aboveHeapOccupancyLowWatermarkCounter;

  private final MetricsHeapMemoryManagerWrapper wrapper;

  public MetricsHeapMemoryManagerSourceImpl(MetricsHeapMemoryManagerWrapper wrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, wrapper);
  }

  public MetricsHeapMemoryManagerSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext, MetricsHeapMemoryManagerWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.wrapper = wrapper;

    // Histograms
    blockedFlushCountHistogram = getMetricsRegistry()
        .newSizeHistogram(BLOCKED_FLUSH_NAME, BLOCKED_FLUSH_DESC);
    unblockedFlushCountHistogram = getMetricsRegistry()
        .newSizeHistogram(UNBLOCKED_FLUSH_NAME, UNBLOCKED_FLUSH_DESC);
    curMemStoreUsedSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(CUR_MEMSTORE_SIZE_NAME, CUR_MEMSTORE_SIZE_DESC);
    curBlockCacheUsedSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(CUR_BLOCKCACHE_SIZE_NAME, CUR_BLOCKCACHE_SIZE_DESC);
    incMemStoreSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(INC_MEMSTORE_TUNING_NAME, INC_MEMSTORE_TUNING_DESC);
    decMemStoreSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(DEC_MEMSTORE_COUNTER_NAME, DEC_MEMSTORE_TUNING_DESC);
    incBlockCacheSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(INC_BLOCKCACHE_TUNING_NAME, INC_BLOCKCACHE_TUNING_DESC);
    decBlockCacheSizeHistogram = getMetricsRegistry()
        .newSizeHistogram(DEC_BLOCKCACHE_TUNING_NAME, DEC_BLOCKCACHE_TUNING_DESC);
    
    // Gauges
    blockedFlushCountGauge = getMetricsRegistry()
        .newGauge(BLOCKED_FLUSH_GAUGE_NAME, BLOCKED_FLUSH_GAUGE_DESC, 0L);
    unblockedFlushCountGauge = getMetricsRegistry()
        .newGauge(UNBLOCKED_FLUSH_GAUGE_NAME, UNBLOCKED_FLUSH_GAUGE_DESC, 0L);
    memStoreSizeGauge = getMetricsRegistry()
        .newGauge(MEMSTORE_SIZE_GAUGE_NAME, MEMSTORE_SIZE_GAUGE_DESC, 0L);
    blockCacheSizeGauge = getMetricsRegistry()
        .newGauge(BLOCKCACHE_SIZE_GAUGE_NAME, BLOCKCACHE_SIZE_GAUGE_DESC, 0L);

    // Counters
    memStoreIncCounter = getMetricsRegistry()
        .newCounter(INC_MEMSTORE_COUNTER_NAME, INC_MEMSTORE_COUNTER_DESC, 0L);
    memStoreDecCounter = getMetricsRegistry()
        .newCounter(DEC_MEMSTORE_COUNTER_NAME, DEC_MEMSTORE_COUNTER_DESC, 0L);
    blockCacheInrCounter = getMetricsRegistry()
        .newCounter(INC_BLOCKCACHE_COUNTER_NAME, INC_BLOCKCACHE_COUNTER_DESC, 0L);
    blockCacheDecCounter = getMetricsRegistry()
        .newCounter(DEC_BLOCKCACHE_COUNTER_NAME, DEC_BLOCKCACHE_COUNTER_DESC, 0L);
    doNothingCounter = getMetricsRegistry()
        .newCounter(DO_NOTHING_COUNTER_NAME, DO_NOTHING_COUNTER_DESC, 0L);
    aboveHeapOccupancyLowWatermarkCounter =
        getMetricsRegistry().newCounter(ABOVE_HEAP_LOW_WATERMARK_COUNTER_NAME,
          ABOVE_HEAP_LOW_WATERMARK_COUNTER_DESC, 0L);
  }

  @Override
  public void updateBlockedFlushCount(long blockedFlushCount) {
    blockedFlushCountHistogram.add(blockedFlushCount);
    blockedFlushCountGauge.set(blockedFlushCount);
  }

  @Override
  public void updateUnblockedFlushCount(long unblockedFlushCount) {
    unblockedFlushCountHistogram.add(unblockedFlushCount);
    unblockedFlushCountGauge.set(unblockedFlushCount);
  }

  @Override
  public void updateCurBlockCacheSize(long blockcacheSize) {
    curBlockCacheUsedSizeHistogram.add(blockcacheSize);
    blockCacheSizeGauge.set(blockcacheSize);
  }

  @Override
  public void updateCurMemStoreSize(long memstoreSize) {
    curMemStoreUsedSizeHistogram.add(memstoreSize);
    memStoreSizeGauge.set(memstoreSize);
  }

  @Override
  public void updateMemStoreDeltaSizeHistogram(int memStoreDeltaSize) {
    if (memStoreDeltaSize > 0) {
      incMemStoreSizeHistogram.add(memStoreDeltaSize);
      memStoreIncCounter.incr();
    } else if (memStoreDeltaSize < 0) {
      decMemStoreSizeHistogram.add(-memStoreDeltaSize);
      memStoreDecCounter.incr();
    }
  }

  @Override
  public void updateBlockCacheDeltaSizeHistogram(int blockCacheDeltaSize) {
    if (blockCacheDeltaSize > 0) {
      incBlockCacheSizeHistogram.add(blockCacheDeltaSize);
      blockCacheInrCounter.incr();
    } else if (blockCacheDeltaSize < 0) {
      decBlockCacheSizeHistogram.add(-blockCacheDeltaSize);
      blockCacheDecCounter.incr();
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

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);
    if (wrapper != null) {
      mrb.addGauge(Interns.info(BLOCKCACHE_SIZE_NAME, BLOCKCACHE_SIZE_DESC),
            wrapper.getBlockCacheUsedSize())
          .addGauge(Interns.info(BLOCKCACHE_PERCENT_NAME, BLOCKCACHE_PERCENT_DESC),
            wrapper.getBlockCacheUsedPercent())
          .addGauge(Interns.info(MEMSTORE_SIZE_NAME, MEMSTORE_SIZE_DESC),
            wrapper.getMemStoreUsedSize())
          .addGauge(Interns.info(MEMSTORE_PERCENT_NAME, MEMSTORE_PERCENT_DESC),
            wrapper.getMemStoreUsedPercent());
    }
    metricsRegistry.snapshot(mrb, all);
  }
}
