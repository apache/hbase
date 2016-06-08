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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  private static final Log LOG = LogFactory.getLog(MetricsHeapMemoryManagerSourceImpl.class);
  private static final int CONVERT_TO_PERCENTAGE = 100;

  private final MetricHistogram heapCurUsedHisto;
  private final MetricHistogram heapOccupancyHisto;
  private final MetricHistogram cacheEvictedHisto;
  private final MetricHistogram cacheMissHisto;
  private final MetricHistogram blockedFlushHisto;
  private final MetricHistogram unblockedFlushHisto;
  private final MetricHistogram curMemStoreUsedHisto;
  private final MetricHistogram curBlockCacheUsedHisto;
  private final MetricHistogram curMemStoreSizeHisto;
  private final MetricHistogram curBlockCacheSizeHisto;
  private final MetricHistogram incMemStoreSizeHisto;
  private final MetricHistogram drcMemStoreSizeHisto;
  private final MetricHistogram incBlockCacheSizeHisto;
  private final MetricHistogram drcBlockCacheSizeHisto;

  private final MutableFastCounter blockedFlushCounter;
  private final MutableFastCounter unblockedFlushCounter;
  private final MutableFastCounter cacheEvictedCounter;
  private final MutableFastCounter cacheMissCounter;
  private final MutableFastCounter memStoreIncCounter;
  private final MutableFastCounter blockCacheInrCounter;
  private final MutableFastCounter noChangeCounter;

  private final MetricsHeapMemoryManagerWrapper wrapper;

  public MetricsHeapMemoryManagerSourceImpl(MetricsHeapMemoryManagerWrapper wrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, wrapper);
  }

  public MetricsHeapMemoryManagerSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext, MetricsHeapMemoryManagerWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.wrapper = wrapper;

    LOG.debug("Creating new MetricsHeapMemoryManagerSourceImpl for RegionServer "
        + wrapper.getServerName());

    heapCurUsedHisto = getMetricsRegistry()
        .newSizeHistogram(HEAP_SIZE_NAME, HEAP_SIZE_DESC);
    heapOccupancyHisto = getMetricsRegistry()
        .newSizeHistogram(HEAP_OCCUPANCY_NAME, HEAP_OCCUPANCY_DESC);
    cacheEvictedHisto = getMetricsRegistry()
        .newSizeHistogram(CACHE_EVICTED_NAME, CACHE_EVICTED_DESC);
    cacheMissHisto = getMetricsRegistry()
        .newSizeHistogram(CACHE_MISS_NAME, CACHE_MISS_DESC);
    blockedFlushHisto = getMetricsRegistry()
        .newSizeHistogram(BLOCKED_FLUSH_NAME, BLOCKED_FLUSH_DESC);
    unblockedFlushHisto = getMetricsRegistry()
        .newSizeHistogram(UNBLOCKED_FLUSH_NAME, UNBLOCKED_FLUSH_DESC);
    curMemStoreUsedHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_MEMSTORE_USED_NAME, CUR_MEMSTORE_USED_DESC);
    curBlockCacheUsedHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_BLOCKCACHE_USED_NAME, CUR_BLOCKCACHE_USED_DESC);
    curMemStoreSizeHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_MEMSTORE_SIZE_NAME, CUR_MEMSTORE_SIZE_DESC);
    curBlockCacheSizeHisto = getMetricsRegistry()
        .newSizeHistogram(CUR_BLOCKCACHE_SIZE_NAME, CUR_BLOCKCACHE_SIZE_DESC);
    incMemStoreSizeHisto = getMetricsRegistry()
        .newSizeHistogram(INC_MEMSTORE_TUNING_NAME, INC_MEMSTORE_TUNING_DESC);
    drcMemStoreSizeHisto = getMetricsRegistry()
        .newSizeHistogram(DRC_MEMSTORE_TUNING_NAME, DRC_MEMSTORE_TUNING_DESC);
    incBlockCacheSizeHisto = getMetricsRegistry()
        .newSizeHistogram(INC_BLOCKCACHE_TUNING_NAME, INC_BLOCKCACHE_TUNING_DESC);
    drcBlockCacheSizeHisto = getMetricsRegistry()
        .newSizeHistogram(DRC_BLOCKCACHE_TUNING_NAME, DRC_BLOCKCACHE_TUNING_DESC);

    cacheEvictedCounter = getMetricsRegistry()
        .newCounter(CACHE_EVICTED_COUNTER_NAME, CACHE_EVICTED_COUNTER_DESC, 0L);
    cacheMissCounter = getMetricsRegistry()
        .newCounter(CACHE_MISS_COUNTER_NAME, CACHE_MISS_COUNTER_DESC, 0L);
    blockedFlushCounter = getMetricsRegistry()
        .newCounter(BLOCKED_FLUSH_COUNTER_NAME, BLOCKED_FLUSH_COUNTER_DESC, 0L);
    unblockedFlushCounter = getMetricsRegistry()
        .newCounter(UNBLOCKED_FLUSH_COUNTER_NAME, UNBLOCKED_FLUSH_COUNTER_DESC, 0L);
    memStoreIncCounter = getMetricsRegistry()
        .newCounter(INC_MEMSTORE_COUNTER_NAME, INC_MEMSTORE_COUNTER_DESC, 0L);
    blockCacheInrCounter = getMetricsRegistry()
        .newCounter(INC_BLOCKCACHE_COUNTER_NAME, INC_BLOCKCACHE_COUNTER_DESC, 0L);
    noChangeCounter = getMetricsRegistry()
        .newCounter(INC_NOCHANGE_COUNTER_NAME, INC_NOCHANGE_COUNTER_DESC, 0L);
  }

  @Override
  public void updateCurHeapSize(long heapSize) {
    heapCurUsedHisto.add(heapSize);
  }

  @Override
  public void updateHeapOccupancy(float heapOccupancyPercent) {
    heapOccupancyHisto.add((long) (heapOccupancyPercent * CONVERT_TO_PERCENTAGE));
  }

  @Override
  public void updateCacheEvictCount(long cacheEvictCount) {
    cacheEvictedHisto.add(cacheEvictCount);
    cacheEvictedCounter.incr(cacheEvictCount);
  }

  @Override
  public void updateCacheMissCount(long cacheMissCount) {
    cacheMissHisto.add(cacheMissCount);
    cacheMissCounter.incr(cacheMissCount);
  }

  @Override
  public void updateBlockedFlushCount(long bFlushCount) {
    blockedFlushHisto.add(bFlushCount);
    blockedFlushCounter.incr(bFlushCount);
  }

  @Override
  public void updateUnblockedFlushCount(long unbFlushCount) {
    unblockedFlushHisto.add(unbFlushCount);
    unblockedFlushCounter.incr(unbFlushCount);
  }

  @Override
  public void updateCurBlockCachePercentage(float curBlockCacheUsed) {
    curBlockCacheUsedHisto.add((long) (curBlockCacheUsed * CONVERT_TO_PERCENTAGE));
  }

  @Override
  public void updateCurMemStorePercentage(float curMemStoreUsed) {
    curMemStoreUsedHisto.add((long) (curMemStoreUsed * CONVERT_TO_PERCENTAGE));
  }

  @Override
  public void updateCurBlockCacheSize(long blockcacheSize) {
    curBlockCacheSizeHisto.add(blockcacheSize);
  }

  @Override
  public void updateCurMemStoreSize(long memstoreSize) {
    curMemStoreSizeHisto.add(memstoreSize);
  }

  @Override
  public void updateDeltaMemStoreSize(int deltaMemStoreSize) {
    if (deltaMemStoreSize > 0) {
      incMemStoreSizeHisto.add(deltaMemStoreSize);
      memStoreIncCounter.incr();
    } else {
      drcMemStoreSizeHisto.add(-deltaMemStoreSize);
    }
  }

  @Override
  public void updateDeltaBlockCacheSize(int deltaBlockCacheSize) {
    if (deltaBlockCacheSize > 0) {
      incBlockCacheSizeHisto.add(deltaBlockCacheSize);
      blockCacheInrCounter.incr();
    } else {
      drcBlockCacheSizeHisto.add(-deltaBlockCacheSize);
    }
  }

  @Override
  public void updateHeapMemoryNoChangeCount() {
    noChangeCounter.incr();
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    if (wrapper != null) {
      mrb.addGauge(Interns.info(MAX_HEAP_SIZE_NAME, MAX_HEAP_SIZE_DESC), wrapper.getMaxHeap())
          .addGauge(Interns.info(HEAP_USE_SIZE_NAME, HEAP_USE_SIZE_DESC), wrapper.getHeapUsedSize())
          .addGauge(Interns.info(HEAP_USE_PERCENT_NAME, HEAP_USE_PERCENT_DESC),
            wrapper.getHeapUsed())
          .addGauge(Interns.info(BLOCKCACHE_SIZE_NAME, BLOCKCACHE_SIZE_DESC),
            wrapper.getBlockCacheUsedSize())
          .addGauge(Interns.info(BLOCKCACHE_PERCENT_NAME, BLOCKCACHE_PERCENT_DESC),
            wrapper.getBlockCacheUsed())
          .addGauge(Interns.info(MEMSTORE_SIZE_NAME, MEMSTORE_SIZE_DESC),
            wrapper.getMemStoreUsedSize())
          .addGauge(Interns.info(MEMSTORE_PERCENT_NAME, MEMSTORE_PERCENT_DESC),
            wrapper.getMemStoreUsed());
    }

    metricsRegistry.snapshot(mrb, all);
  }
}
