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
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.quotas.ActivePolicyEnforcement;
import org.apache.hadoop.hbase.quotas.OperationQuota;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;

/**
 * It is responsible for populating the row cache and retrieving rows from it.
 */
@org.apache.yetus.audience.InterfaceAudience.Private
public class RowCacheService {
  /**
   * A barrier that prevents the row cache from being populated during region operations, such as
   * bulk loads. It is implemented as a counter to address issues that arise when the same region is
   * updated concurrently.
   */
  private final Map<HRegion, AtomicInteger> regionLevelBarrierMap = new ConcurrentHashMap<>();
  /**
   * A barrier that prevents the row cache from being populated during row mutations. It is
   * implemented as a counter to address issues that arise when the same row is mutated
   * concurrently.
   */
  private final Map<RowCacheKey, AtomicInteger> rowLevelBarrierMap = new ConcurrentHashMap<>();

  private final boolean enabledByConf;
  private final RowCache rowCache;

  @FunctionalInterface
  interface RowOperation<R> {
    R execute() throws IOException;
  }

  RowCacheService(Configuration conf) {
    enabledByConf =
      conf.getFloat(HConstants.ROW_CACHE_SIZE_KEY, HConstants.ROW_CACHE_SIZE_DEFAULT) > 0;
    // Currently we only support TinyLfu implementation
    rowCache = new TinyLfuRowCache(MemorySizeUtil.getRowCacheSize(conf));
  }

  RegionScannerImpl getScanner(HRegion region, Get get, Scan scan, List<Cell> results,
    RpcCallContext context) throws IOException {
    if (!canCacheRow(get, region)) {
      return getScannerInternal(region, scan, results);
    }

    RowCacheKey key = new RowCacheKey(region, get.getRow());

    // Try get from row cache
    if (tryGetFromCache(region, key, get, results)) {
      // Cache is hit, and then no scanner is created
      return null;
    }

    RegionScannerImpl scanner = getScannerInternal(region, scan, results);

    // When results came from memstore only, do not populate the row cache
    boolean readFromMemStoreOnly = context.getBlockBytesScanned() < 1;
    if (!readFromMemStoreOnly) {
      populateCache(region, results, key);
    }

    return scanner;
  }

  private RegionScannerImpl getScannerInternal(HRegion region, Scan scan, List<Cell> results)
    throws IOException {
    RegionScannerImpl scanner = region.getScanner(scan);
    scanner.next(results);
    return scanner;
  }

  private boolean tryGetFromCache(HRegion region, RowCacheKey key, Get get, List<Cell> results) {
    RowCells row = rowCache.getRow(key, get.getCacheBlocks());

    if (row == null) {
      return false;
    }

    results.addAll(row.getCells());
    region.addReadRequestsCount(1);
    if (region.getMetrics() != null) {
      region.getMetrics().updateReadRequestCount();
    }
    return true;
  }

  private void populateCache(HRegion region, List<Cell> results, RowCacheKey key) {
    // The row cache is populated only when no region level barriers remain
    regionLevelBarrierMap.computeIfAbsent(region, t -> {
      // The row cache is populated only when no row level barriers remain
      rowLevelBarrierMap.computeIfAbsent(key, k -> {
        try {
          rowCache.cacheRow(key, new RowCells(results));
        } catch (CloneNotSupportedException ignored) {
          // Not able to cache row cells, ignore
        }
        return null;
      });
      return null;
    });
  }

  BulkLoadHFileResponse bulkLoadHFile(RSRpcServices rsRpcServices, BulkLoadHFileRequest request)
    throws ServiceException {
    HRegion region;
    try {
      region = rsRpcServices.getRegion(request.getRegion());
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }

    if (!region.isRowCacheEnabled()) {
      return bulkLoad(rsRpcServices, request);
    }

    // Since bulkload modifies the store files, the row cache should be disabled until the bulkload
    // is finished.
    createRegionLevelBarrier(region);
    try {
      // We do not invalidate the entire row cache directly, as it contains a large number of
      // entries and takes a long time. Instead, we increment rowCacheSeqNum, which is used when
      // constructing a RowCacheKey, thereby making the existing row cache entries stale.
      increaseRowCacheSeqNum(region);
      return bulkLoad(rsRpcServices, request);
    } finally {
      // The row cache for the region has been enabled again
      removeTableLevelBarrier(region);
    }
  }

  BulkLoadHFileResponse bulkLoad(RSRpcServices rsRpcServices, BulkLoadHFileRequest request)
    throws ServiceException {
    return rsRpcServices.bulkLoadHFileInternal(request);
  }

  void increaseRowCacheSeqNum(HRegion region) {
    region.increaseRowCacheSeqNum();
  }

  void removeTableLevelBarrier(HRegion region) {
    regionLevelBarrierMap.computeIfPresent(region, (k, counter) -> {
      int remaining = counter.decrementAndGet();
      return (remaining <= 0) ? null : counter;
    });
  }

  void createRegionLevelBarrier(HRegion region) {
    regionLevelBarrierMap.computeIfAbsent(region, k -> new AtomicInteger(0)).incrementAndGet();
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

  private <R> R mutateWithRowCacheBarrier(HRegion region, List<Mutation> mutations,
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

  private <R> R mutateWithRowCacheBarrier(HRegion region, byte[] row, RowOperation<R> operation)
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

  <R> R execute(RowOperation<R> operation) throws IOException {
    return operation.execute();
  }

  void evictRow(RowCacheKey key) {
    rowCache.evictRow(key);
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

  Result mutate(RSRpcServices rsRpcServices, HRegion region, ClientProtos.MutationProto mutation,
    OperationQuota quota, CellScanner cellScanner, long nonceGroup,
    ActivePolicyEnforcement spaceQuotaEnforcement, RpcCallContext context) throws IOException {
    return mutateWithRowCacheBarrier(region, mutation.getRow().toByteArray(),
      () -> rsRpcServices.mutateInternal(mutation, region, quota, cellScanner, nonceGroup,
        spaceQuotaEnforcement, context));
  }

  CheckAndMutateResult checkAndMutate(HRegion region, CheckAndMutate checkAndMutate,
    long nonceGroup, long nonce) throws IOException {
    return mutateWithRowCacheBarrier(region, checkAndMutate.getRow(),
      () -> region.checkAndMutate(checkAndMutate, nonceGroup, nonce));
  }

  CheckAndMutateResult checkAndMutate(HRegion region, List<Mutation> mutations,
    CheckAndMutate checkAndMutate, long nonceGroup, long nonce) throws IOException {
    return mutateWithRowCacheBarrier(region, mutations,
      () -> region.checkAndMutate(checkAndMutate, nonceGroup, nonce));
  }

  OperationStatus[] batchMutate(HRegion region, Mutation[] mArray, boolean atomic, long nonceGroup,
    long nonce) throws IOException {
    return mutateWithRowCacheBarrier(region, Arrays.asList(mArray),
      () -> region.batchMutate(mArray, atomic, nonceGroup, nonce));
  }

  // For testing only
  AtomicInteger getRowLevelBarrier(RowCacheKey key) {
    return rowLevelBarrierMap.get(key);
  }

  // For testing only
  AtomicInteger getRegionLevelBarrier(HRegion region) {
    return regionLevelBarrierMap.get(region);
  }

  public RowCache getRowCache() {
    return rowCache;
  }

  void evictRowsByRegion(HRegion region) {
    rowCache.evictRowsByRegion(region);
  }
}
