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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Hadoop2 implementation of MetricsHeapMemoryManagerSource. Implements BaseSource through
 * BaseSourceImpl, following the pattern
 */
public class MetricsHeapMemoryManagerSourceImpl extends BaseSourceImpl implements
    MetricsHeapMemoryManagerSource {

  private static final int CONVERT_TO_PERCENTAGE = 100;

  private final MetricHistogram cacheEvictedHisto;
  private final MetricHistogram cacheMissHisto;
  private final MetricHistogram blockedFlushHisto;
  private final MetricHistogram unblockedFlushHisto;
  private final MetricHistogram curMemStoreUsedPercentHisto;
  private final MetricHistogram curBlockCacheUsedPercentHisto;
  private final MetricHistogram curMemStoreUsedSizeHisto;
  private final MetricHistogram curBlockCacheUsedSizeHisto;
  private final MetricHistogram incMemStoreSizeHisto;
  private final MetricHistogram incBlockCacheSizeHisto;

  private final MutableFastCounter memStoreIncCounter;
  private final MutableFastCounter blockCacheInrCounter;
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

    cacheEvictedHisto = getMetricsRegistry()
        .newSizeHistogram(CACHE_EVICTED_NAME, CACHE_EVICTED_DESC);
    cacheMissHisto = getMetricsRegistry()
        .newSizeHistogram(CACHE_MISS_NAME, CACHE_MISS_DESC);
    blockedFlushHisto = getMetricsRegistry()
        .newSizeHistogram(BLOCKED_FLUSH_NAME, BLOCKED_FLUSH_DESC);
    unblockedFlushHisto = getMetricsRegistry()
        .newSizeHistogram(UNBLOCKED_FLUSH_NAME, UNBLOCKED_FLUSH_DESC);
    curMemStoreUsedPercentHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_MEMSTORE_PERCENT_NAME, CUR_MEMSTORE_PERCENT_DESC);
    curBlockCacheUsedPercentHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_BLOCKCACHE_PERCENT_NAME, CUR_BLOCKCACHE_PERCENT_DESC);
    curMemStoreUsedSizeHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_MEMSTORE_SIZE_NAME, CUR_MEMSTORE_SIZE_DESC);
    curBlockCacheUsedSizeHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_BLOCKCACHE_SIZE_NAME, CUR_BLOCKCACHE_SIZE_DESC);
    incMemStoreSizeHisto = getMetricsRegistry()
        .newSizeHistogram(INC_MEMSTORE_TUNING_NAME, INC_MEMSTORE_TUNING_DESC);
    incBlockCacheSizeHisto = getMetricsRegistry()
        .newSizeHistogram(INC_BLOCKCACHE_TUNING_NAME, INC_BLOCKCACHE_TUNING_DESC);

    memStoreIncCounter = getMetricsRegistry()
        .newCounter(INC_MEMSTORE_COUNTER_NAME, INC_MEMSTORE_COUNTER_DESC, 0L);
    blockCacheInrCounter = getMetricsRegistry()
        .newCounter(INC_BLOCKCACHE_COUNTER_NAME, INC_BLOCKCACHE_COUNTER_DESC, 0L);
    doNothingCounter = getMetricsRegistry()
        .newCounter(DO_NOTHING_COUNTER_NAME, DO_NOTHING_COUNTER_DESC, 0L);
    aboveHeapOccupancyLowWatermarkCounter =
        getMetricsRegistry().newCounter(ABOVE_HEAP_LOW_WATERMARK_COUNTER_NAME,
          ABOVE_HEAP_LOW_WATERMARK_COUNTER_DESC, 0L);
  }

  @Override
  public void updateCacheEvictedCount(long cacheEvictCount) {
    cacheEvictedHisto.add(cacheEvictCount);
  }

  @Override
  public void updateCacheMissCount(long cacheMissCount) {
    cacheMissHisto.add(cacheMissCount);
  }

  @Override
  public void updateBlockedFlushCount(long bFlushCount) {
    blockedFlushHisto.add(bFlushCount);
  }

  @Override
  public void updateUnblockedFlushCount(long unbFlushCount) {
    unblockedFlushHisto.add(unbFlushCount);
  }

  @Override
  public void updateCurBlockCachePercent(float curBlockCacheUsed) {
    curBlockCacheUsedPercentHisto.add((long) (curBlockCacheUsed * CONVERT_TO_PERCENTAGE));
  }

  @Override
  public void updateCurMemStorePercent(float curMemStoreUsed) {
    curMemStoreUsedPercentHisto.add((long) (curMemStoreUsed * CONVERT_TO_PERCENTAGE));
  }

  @Override
  public void updateCurBlockCacheSize(long blockcacheSize) {
    curBlockCacheUsedSizeHisto.add(blockcacheSize);
  }

  @Override
  public void updateCurMemStoreSize(long memstoreSize) {
    curMemStoreUsedSizeHisto.add(memstoreSize);
  }

  @Override
  public void updateMemStoreDeltaSize(int memStoreDeltaSize) {
    if (memStoreDeltaSize > 0) {
      incMemStoreSizeHisto.add(memStoreDeltaSize);
      memStoreIncCounter.incr();
    }
  }

  @Override
  public void updateBlockCacheDeltaSize(int blockCacheDeltaSize) {
    if (blockCacheDeltaSize > 0) {
      incBlockCacheSizeHisto.add(blockCacheDeltaSize);
      blockCacheInrCounter.incr();
    }
  }

  @Override
  public void updateTunerDoNothingCount() {
    doNothingCounter.incr();
  }

  @Override
  public void updateAboveHeapOccupancyLowWatermarkCount() {
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
