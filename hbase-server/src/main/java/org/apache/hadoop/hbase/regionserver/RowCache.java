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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Facade for row-level caching in the RegionServer.
 *
 * <p>{@code RowCache} coordinates cache access for Get operations and
 * enforces cache consistency during mutations. It delegates actual
 * storage and eviction policy decisions (e.g., LRU, LFU) to a
 * {@link RowCacheStrategy} implementation.</p>
 *
 * <p>This class is responsible for:
 * <ul>
 *   <li>Determining whether row cache is enabled for a region</li>
 *   <li>Attempting cache lookups before falling back to the normal read path</li>
 *   <li>Populating the cache after successful reads</li>
 *   <li>Evicting affected rows on mutations to maintain correctness</li>
 * </ul>
 *
 * <p>{@code RowCache} does not implement caching policy or storage directly;
 * those concerns are encapsulated by {@code RowCacheStrategy}.</p>
 */
@org.apache.yetus.audience.InterfaceAudience.Private
public class RowCache {
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
    // TODO: implement row cache
    rowCacheStrategy = null;
  }

  <R> R mutateWithRowCacheBarrier(HRegion region, byte[] row, RowOperation<R> operation)
    throws IOException {
    if (!region.isRowCacheEnabled()) {
      return operation.execute();
    }

    RowCacheKey key = new RowCacheKey(region, row);
    // TODO: implement mutate with row cache barrier logic
    evictRow(key);
    return execute(operation);
  }

  <R> R mutateWithRowCacheBarrier(HRegion region, List<Mutation> mutations,
    RowOperation<R> operation) throws IOException {
    if (!region.isRowCacheEnabled()) {
      return operation.execute();
    }

    // TODO: implement mutate with row cache barrier logic
    Set<RowCacheKey> rowCacheKeys = new HashSet<>(mutations.size());
    mutations.forEach(mutation -> rowCacheKeys.add(new RowCacheKey(region, mutation.getRow())));
    rowCacheKeys.forEach(this::evictRow);

    return execute(operation);
  }

  void evictRow(RowCacheKey key) {
    rowCacheStrategy.evictRow(key);
  }

  boolean canCacheRow(Get get, Region region) {
    // TODO: implement logic to determine if the row can be cached
    return false;
  }

  boolean tryGetFromCache(HRegion region, RowCacheKey key, Get get, List<Cell> results) {
    RowCells row = rowCacheStrategy.getRow(key, get.getCacheBlocks());

    if (row == null) {
      return false;
    }

    results.addAll(row.getCells());
    // TODO: implement update of metrics
    return true;
  }

  void populateCache(HRegion region, List<Cell> results, RowCacheKey key) {
    // TODO: implement with barrier to avoid cache read during mutation
    try {
      rowCacheStrategy.cacheRow(key, new RowCells(results));
    } catch (CloneNotSupportedException ignored) {
      // Not able to cache row cells, ignore
    }
  }
}
