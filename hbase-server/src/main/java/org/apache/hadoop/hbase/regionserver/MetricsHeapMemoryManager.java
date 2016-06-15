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

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is for maintaining the various regionserver's heap memory manager statistics and
 * publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
public class MetricsHeapMemoryManager {
  private final MetricsHeapMemoryManagerSource source;

  public MetricsHeapMemoryManager(final long globalMemStoreSize, final long blockCacheSize) {
    this(CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .getHeapMemoryManager(globalMemStoreSize, blockCacheSize));
  }

  public MetricsHeapMemoryManager(MetricsHeapMemoryManagerSource source) {
    this.source = source;
  }

  public MetricsHeapMemoryManagerSource getMetricsSource() {
    return source;
  }

  /**
   * Update/Set the blocked flush count histogram/gauge
   * @param blockedFlushCount the number of blocked memstore flush since last tuning.
   */
  public void updateBlockedFlushCount(final long blockedFlushCount) {
    source.updateBlockedFlushCount(blockedFlushCount);
  }

  /**
   * Update/Set the unblocked flush count histogram/gauge
   * @param unblockedFlushCount the number of unblocked memstore flush since last tuning.
   */
  public void updateUnblockedFlushCount(final long unblockedFlushCount) {
    source.updateUnblockedFlushCount(unblockedFlushCount);
  }

  /**
   * Set the current blockcache size used gauge
   * @param blockCacheSize the current memory usage in blockcache, in bytes.
   */
  public void setCurBlockCacheSizeGauge(final long blockCacheSize) {
    source.setCurBlockCacheSizeGauge(blockCacheSize);
  }

  /**
   * Set the current global memstore size used gauge
   * @param memStoreSize the current memory usage in memstore, in bytes.
   */
  public void setCurMemStoreSizeGauge(final long memStoreSize) {
    source.setCurMemStoreSizeGauge(memStoreSize);
  }

  /**
   * Set the new max blockcache size gauge after tuning
   * @param newMaxBlockCacheSize the new blockcache max size, in bytes.
   */
  public void setNewBlockCacheMaxSizeGauge(long newMaxBlockCacheSize) {
    source.setNewBlockCacheMaxSizeGauge(newMaxBlockCacheSize);
  }

  /**
   * Set the new global memstore size gauge after tuning
   * @param newGlobalMemStoreSize the new global memstore size, in bytes.
   */
  public void setNewGlobalMemStoreSizeGauge(long newGlobalMemStoreSize) {
    source.setNewGlobalMemStoreSizeGauge(newGlobalMemStoreSize);
  }

  /**
   * Update the increase/decrease memstore size histogram
   * @param memStoreDeltaSize the tuning result of memstore.
   */
  public void updateMemStoreDeltaSizeHistogram(final int memStoreDeltaSize) {
    source.updateMemStoreDeltaSizeHistogram(memStoreDeltaSize);
  }

  /**
   * Update the increase/decrease blockcache size histogram
   * @param blockCacheDeltaSize the tuning result of blockcache.
   */
  public void updateBlockCacheDeltaSizeHistogram(final int blockCacheDeltaSize) {
    source.updateBlockCacheDeltaSizeHistogram(blockCacheDeltaSize);
  }

  /**
   * Increase the counter for tuner neither expanding memstore global size limit nor expanding
   * blockcache max size.
   */
  public void increaseTunerDoNothingCounter() {
    source.increaseTunerDoNothingCounter();
  }

  /**
   * Increase the counter for heap occupancy percent above low watermark
   */
  public void increaseAboveHeapOccupancyLowWatermarkCounter() {
    source.increaseAboveHeapOccupancyLowWatermarkCounter();
  }
}
