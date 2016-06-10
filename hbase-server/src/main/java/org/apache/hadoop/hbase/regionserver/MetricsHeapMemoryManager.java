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
 * <p>
 * This class is for maintaining the various regionserver's heap memory manager statistics
 * and publishing them through the metrics interfaces.
 * </p>
 */
@InterfaceStability.Evolving
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

  public void updateHeapOccupancyPercent(final float heapOccupancyPercent, final long heapUsed) {
    source.updateHeapOccupancyPercent(heapOccupancyPercent);
    source.updateCurHeapSize(heapUsed);
  }

  public void updateCacheEvictedCount(final long cacheEvictedCount) {
    source.updateCacheEvictCount(cacheEvictedCount);
  }

  public void updateCacheMissCount(final long cacheMissCount) {
    source.updateCacheMissCount(cacheMissCount);
  }

  public void updateBlockedFlushCount(final long bFlushCount) {
    source.updateBlockedFlushCount(bFlushCount);
  }

  public void updateUnblockedFlushCount(final long unbFlushCount) {
    source.updateUnblockedFlushCount(unbFlushCount);
  }

  public void updateCurBlockCache(final float curBlockCacheUsed, final long blockcacheSize) {
    source.updateCurBlockCachePercentage(curBlockCacheUsed);
    source.updateCurBlockCacheSize(blockcacheSize);
  }

  public void updateCurMemStore(final float curMemStoreUsed, final long memstoreSize) {
    source.updateCurMemStorePercentage(curMemStoreUsed);
    source.updateCurMemStoreSize(memstoreSize);
  }

  public void updateMemStoreDeltaSize(final int memStoreDeltaSize) {
    source.updateMemStoreDeltaSize(memStoreDeltaSize);
  }

  public void updateBlockCacheDeltaSize(final int blockCacheDeltaSize) {
    source.updateBlockCacheDeltaSize(blockCacheDeltaSize);
  }

  public void updateHeapMemoryNoChangeCount() {
    source.updateHeapMemoryNoChangeCount();
  }
}
