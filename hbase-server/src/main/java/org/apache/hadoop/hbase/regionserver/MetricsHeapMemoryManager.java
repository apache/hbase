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
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * This class is for maintaining the various regionserver's heap memory manager statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
public class MetricsHeapMemoryManager {
  private final MetricsHeapMemoryManagerSource source;
  private MetricsHeapMemoryManagerWrapper wrapper;

  public MetricsHeapMemoryManager(MetricsHeapMemoryManagerWrapper wrapper) {
    this(wrapper, CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .getHeapMemoryManager(wrapper));
  }

  public MetricsHeapMemoryManager(MetricsHeapMemoryManagerWrapper wrapper,
      MetricsHeapMemoryManagerSource source) {
    this.wrapper = wrapper;
    this.source = source;
  }

  public MetricsHeapMemoryManagerSource getMetricsSource() {
    return source;
  }

  public MetricsHeapMemoryManagerWrapper getHeapMemoryManagerWrapper() {
    return wrapper;
  }

  /**
   * Update/Set the blocked flush count histogram/gauge
   * @param blockedFlushCount the number of blocked flush since last tuning.
   */
  public void updateBlockedFlushCount(final long blockedFlushCount) {
    source.updateBlockedFlushCount(blockedFlushCount);
  }

  /**
   * Update/Set the unblocked flush count histogram/gauge
   * @param unblockedFlushCount the number of unblocked flush since last tuning.
   */
  public void updateUnblockedFlushCount(final long unblockedFlushCount) {
    source.updateUnblockedFlushCount(unblockedFlushCount);
  }

  /**
   * Update/Set the current blockcache(in size) used histogram/gauge
   * @param blockCacheSize the current memory usage in blockcache in size.
   */
  public void updateCurBlockCacheSize(final long blockCacheSize) {
    source.updateCurBlockCacheSize(blockCacheSize);
  }

  /**
   * Update/Set the current global memstore used(in size) histogram/gauge
   * @param memStoreSize the current memory usage in memstore in size.
   */
  public void updateCurMemStoreSize(final long memStoreSize) {
    source.updateCurMemStoreSize(memStoreSize);
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
