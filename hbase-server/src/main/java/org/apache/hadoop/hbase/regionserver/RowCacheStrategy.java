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

/**
 * Strategy interface for row-level caching used by {@link RowCache}.
 * <p>
 * This interface defines the contract for concrete row cache implementations (e.g., LRU, LFU).
 * Implementations are responsible for managing the in-memory storage of rows retrieved by Get
 * operations, applying eviction policies, tracking cache statistics, and enforcing size
 * constraints.
 * </p>
 * <p>
 * The {@code RowCacheStrategy} focuses solely on cache management concerns. It does not participate
 * in scanner creation or read-path control logic, which are handled by higher-level components.
 * </p>
 */
@org.apache.yetus.audience.InterfaceAudience.Private
public interface RowCacheStrategy {
  /**
   * Cache the specified row.
   * @param key   the key of the row to cache
   * @param value the cells of the row to cache
   */
  void cacheRow(RowCacheKey key, RowCells value);

  /**
   * Evict the specified row.
   * @param key the key of the row to evict
   */
  void evictRow(RowCacheKey key);

  /**
   * Evict all rows belonging to the specified region. This is heavy operation as it iterates the
   * entire RowCache key set.
   * @param region the region whose rows should be evicted
   */
  void evictRowsByRegion(HRegion region);

  /**
   * Get the number of rows in the cache.
   * @return the number of rows in the cache
   */
  long getCount();

  /**
   * Get the number of rows evicted from the cache.
   * @return the number of rows evicted from the cache
   */
  long getEvictedRowCount();

  /**
   * Get the hit count.
   * @return the hit count
   */
  long getHitCount();

  /**
   * Get the maximum size of the cache in bytes.
   * @return the maximum size of the cache in bytes
   */
  long getMaxSize();

  /**
   * Get the miss count.
   * @return the miss count
   */
  long getMissCount();

  /**
   * Get the specified row from the cache.
   * @param key     the key of the row to get
   * @param caching whether caching is enabled for this request
   * @return the cells of the row, or null if not found or caching is disabled
   */
  RowCells getRow(RowCacheKey key, boolean caching);

  /**
   * Get the current size of the cache in bytes.
   * @return the current size of the cache in bytes
   */
  long getSize();
}
