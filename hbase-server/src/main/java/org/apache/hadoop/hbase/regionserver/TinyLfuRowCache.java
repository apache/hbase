/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.LongAdder;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A {@link RowCache} implementation backed by Caffeine with a TinyLFU-based eviction policy.
 */
@org.apache.yetus.audience.InterfaceAudience.Private
public class TinyLfuRowCache implements RowCache {
  private final class EvictionListener
    implements RemovalListener<@NonNull RowCacheKey, @NonNull RowCells> {
    @Override
    public void onRemoval(RowCacheKey key, RowCells value, @NonNull RemovalCause cause) {
      evictedRowCount.increment();
    }
  }

  private final Cache<@NonNull RowCacheKey, RowCells> cache;

  // Cache.stats() does not provide eviction count for entries, so we maintain our own counter.
  private final LongAdder evictedRowCount = new LongAdder();

  TinyLfuRowCache(long maxSizeBytes) {
    if (maxSizeBytes <= 0) {
      cache = Caffeine.newBuilder().maximumSize(0).build();
      return;
    }

    cache =
      Caffeine.newBuilder().maximumWeight(maxSizeBytes).removalListener(new EvictionListener())
        .weigher((RowCacheKey key,
          RowCells value) -> (int) Math.min(key.heapSize() + value.heapSize(), Integer.MAX_VALUE))
        .recordStats().build();
  }

  @Override
  public void cacheRow(RowCacheKey key, RowCells value) {
    cache.put(key, value);
  }

  @Override
  public RowCells getRow(RowCacheKey key, boolean caching) {
    if (!caching) {
      return null;
    }

    return cache.getIfPresent(key);
  }

  @Override
  public void evictRow(RowCacheKey key) {
    cache.asMap().remove(key);
  }

  @Override
  public void evictRowsByRegion(HRegion region) {
    cache.asMap().keySet().removeIf(key -> key.isSameRegion(region));
  }

  @Override
  public long getHitCount() {
    return cache.stats().hitCount();
  }

  @Override
  public long getMissCount() {
    return cache.stats().missCount();
  }

  @Override
  public long getEvictedRowCount() {
    return evictedRowCount.sum();
  }

  @Override
  public long getSize() {
    Optional<OptionalLong> result = cache.policy().eviction().map(Policy.Eviction::weightedSize);
    return result.orElse(OptionalLong.of(-1L)).orElse(-1L);
  }

  @Override
  public long getMaxSize() {
    Optional<Long> result = cache.policy().eviction().map(Policy.Eviction::getMaximum);
    return result.orElse(-1L);
  }

  @Override
  public long getCount() {
    return cache.estimatedSize();
  }
}
