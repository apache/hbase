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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * Facade for row-level caching in the RegionServer.
 * <p>
 * {@code RowCache} coordinates cache access for Get operations and enforces cache consistency
 * during mutations. It delegates actual storage and eviction policy decisions (e.g., LRU, LFU) to a
 * {@link RowCacheStrategy} implementation.
 * </p>
 * <p>
 * This class is responsible for:
 * <ul>
 * <li>Determining whether row cache is enabled for a region</li>
 * <li>Attempting cache lookups before falling back to the normal read path</li>
 * <li>Populating the cache after successful reads</li>
 * <li>Evicting affected rows on mutations to maintain correctness</li>
 * </ul>
 * <p>
 * {@code RowCache} does not implement caching policy or storage directly; those concerns are
 * encapsulated by {@code RowCacheStrategy}.
 * </p>
 */
@org.apache.yetus.audience.InterfaceAudience.Private
public class RowCache {
  /**
   * A barrier that prevents the row cache from being populated during region operations, such as
   * bulk loads. It is implemented as a counter to address issues that arise when the same region is
   * updated concurrently. Keyed by the encoded region name.
   */
  private final Map<String, AtomicInteger> regionLevelBarrierMap = new ConcurrentHashMap<>();
  /**
   * A barrier that prevents the row cache from being populated during row mutations. It is
   * implemented as a counter to address issues that arise when the same row is mutated
   * concurrently.
   */
  private final Map<RowCacheKey, AtomicInteger> rowLevelBarrierMap = new ConcurrentHashMap<>();

  private final boolean enabledByConf;
  private final RowCacheStrategy rowCacheStrategy;

  @FunctionalInterface
  interface RowOperation<R> {
    R execute() throws IOException;
  }

  <R> R execute(RowOperation<R> operation) throws IOException {
    return operation.execute();
  }

  RowCache(Configuration conf) {
    enabledByConf =
      conf.getFloat(HConstants.ROW_CACHE_SIZE_KEY, HConstants.ROW_CACHE_SIZE_DEFAULT) > 0;
    Class<? extends RowCacheStrategy> strategyClass = conf.getClass(
      HConstants.ROW_CACHE_STRATEGY_CLASS_KEY, TinyLfuRowCacheStrategy.class,
      RowCacheStrategy.class);
    rowCacheStrategy = ReflectionUtils.newInstance(strategyClass, conf);
  }

  <R> R mutateWithRowCacheBarrier(HRegion region, byte[] row, RowOperation<R> operation)
    throws IOException {
    if (!region.isRowCacheEnabled()) {
      return operation.execute();
    }

    RowCacheKey key = new RowCacheKey(region, row);
    try {
      // Creates a barrier that prevents the row cache from being populated for this row
      // during mutation. Reads for the row can instead be served from HFiles or the block cache.
      createRowLevelBarrier(key);

      // After creating the barrier, evict the existing row cache for this row,
      // as it becomes invalid after the mutation
      evictRow(key);

      return execute(operation);
    } finally {
      // Remove the barrier after mutation to allow the row cache to be populated again
      removeRowLevelBarrier(key);
    }
  }

  /**
   * Remove the barrier after mutation to allow the row cache to be populated again
   * @param key the cache key of the row
   */
  void removeRowLevelBarrier(RowCacheKey key) {
    rowLevelBarrierMap.computeIfPresent(key, (k, counter) -> {
      int remaining = counter.decrementAndGet();
      return (remaining <= 0) ? null : counter;
    });
  }

  /**
   * Creates a barrier to prevent the row cache from being populated for this row during mutation
   * @param key the cache key of the row
   */
  void createRowLevelBarrier(RowCacheKey key) {
    rowLevelBarrierMap.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
  }

  <R> R mutateWithRowCacheBarrier(HRegion region, List<Mutation> mutations,
    RowOperation<R> operation) throws IOException {
    if (!region.isRowCacheEnabled()) {
      return operation.execute();
    }

    Set<RowCacheKey> rowCacheKeys = new HashSet<>(mutations.size());
    try {
      // Evict the entire row cache
      mutations.forEach(mutation -> rowCacheKeys.add(new RowCacheKey(region, mutation.getRow())));
      rowCacheKeys.forEach(key -> {
        // Creates a barrier that prevents the row cache from being populated for this row
        // during mutation. Reads for the row can instead be served from HFiles or the block cache.
        createRowLevelBarrier(key);

        // After creating the barrier, evict the existing row cache for this row,
        // as it becomes invalid after the mutation
        evictRow(key);
      });

      return execute(operation);
    } finally {
      // Remove the barrier after mutation to allow the row cache to be populated again
      rowCacheKeys.forEach(this::removeRowLevelBarrier);
    }
  }

  void evictRow(RowCacheKey key) {
    rowCacheStrategy.evictRow(key);
  }

  void evictRowsByRegion(HRegion region) {
    rowCacheStrategy.evictRowsByRegion(region);
  }

  // @formatter:off
  /**
   * Row cache is only enabled when the following conditions are met:
   *  - Row cache is enabled at the table level.
   *  - Cache blocks is enabled in the get request.
   *  - A Get object cannot be distinguished from others except by its row key.
   *    So we check equality for the following:
   *    - filter
   *    - retrieving cells
   *    - TTL
   *    - attributes
   *    - CheckExistenceOnly
   *    - ColumnFamilyTimeRange
   *    - Consistency
   *    - MaxResultsPerColumnFamily
   *    - ReplicaId
   *    - RowOffsetPerColumnFamily
   * @param get the Get request
   * @param region the Region
   * @return true if the row can be cached, false otherwise
   */
  // @formatter:on
  boolean canCacheRow(Get get, Region region) {
    return enabledByConf && region.isRowCacheEnabled() && get.getCacheBlocks()
      && get.getFilter() == null && isRetrieveAllCells(get, region) && isDefaultTtl(region)
      && get.getAttributesMap().isEmpty() && !get.isCheckExistenceOnly()
      && get.getColumnFamilyTimeRange().isEmpty() && get.getConsistency() == Consistency.STRONG
      && get.getMaxResultsPerColumnFamily() == -1 && get.getReplicaId() == -1
      && get.getRowOffsetPerColumnFamily() == 0 && get.getTimeRange().isAllTime();
  }

  private static boolean isRetrieveAllCells(Get get, Region region) {
    if (region.getTableDescriptor().getColumnFamilyCount() != get.numFamilies()) {
      return false;
    }

    boolean hasQualifier = get.getFamilyMap().values().stream().anyMatch(Objects::nonNull);
    return !hasQualifier;
  }

  private static boolean isDefaultTtl(Region region) {
    return Arrays.stream(region.getTableDescriptor().getColumnFamilies())
      .allMatch(cfd -> cfd.getTimeToLive() == ColumnFamilyDescriptorBuilder.DEFAULT_TTL);
  }

  // For testing only
  public RowCells getRow(RowCacheKey key) {
    return getRow(key, true);
  }

  // For testing only
  RowCells getRow(RowCacheKey key, boolean caching) {
    return rowCacheStrategy.getRow(key, caching);
  }

  boolean tryGetFromCache(RowCacheKey key, Get get, List<Cell> results) {
    RowCells row = rowCacheStrategy.getRow(key, get.getCacheBlocks());

    if (row == null) {
      return false;
    }

    results.addAll(row.getCells());
    return true;
  }

  void cache(List<Cell> results, RowCacheKey key) {
    // The row cache is populated only when no region level barriers remain
    regionLevelBarrierMap.computeIfAbsent(key.getEncodedRegionName(), t -> {
      // The row cache is populated only when no row level barriers remain
      rowLevelBarrierMap.computeIfAbsent(key, k -> {
        try {
          rowCacheStrategy.cacheRow(key, new RowCells(results));
        } catch (CloneNotSupportedException ignored) {
          // Not able to cache row cells, ignore
        }
        return null;
      });
      return null;
    });
  }

  void createRegionLevelBarrier(HRegion region) {
    regionLevelBarrierMap
      .computeIfAbsent(region.getRegionInfo().getEncodedName(), k -> new AtomicInteger(0))
      .incrementAndGet();
  }

  void increaseRowCacheSeqNum(HRegion region) {
    region.increaseRowCacheSeqNum();
  }

  void removeRegionLevelBarrier(HRegion region) {
    regionLevelBarrierMap.computeIfPresent(region.getRegionInfo().getEncodedName(),
      (k, counter) -> {
        int remaining = counter.decrementAndGet();
        return (remaining <= 0) ? null : counter;
      });
  }

  long getHitCount() {
    return rowCacheStrategy.getHitCount();
  }

  long getMissCount() {
    return rowCacheStrategy.getMissCount();
  }

  long getSize() {
    return rowCacheStrategy.getSize();
  }

  long getCount() {
    return rowCacheStrategy.getCount();
  }

  long getEvictedRowCount() {
    return rowCacheStrategy.getEvictedRowCount();
  }

  // For testing only
  AtomicInteger getRowLevelBarrier(RowCacheKey key) {
    return rowLevelBarrierMap.get(key);
  }

  // For testing only
  AtomicInteger getRegionLevelBarrier(HRegion region) {
    return regionLevelBarrierMap.get(region.getRegionInfo().getEncodedName());
  }
}
