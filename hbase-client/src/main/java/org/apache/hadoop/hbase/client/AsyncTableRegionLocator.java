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
package org.apache.hadoop.hbase.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The asynchronous version of RegionLocator.
 * <p>
 * Usually the implementations will not throw any exception directly, you need to get the exception
 * from the returned {@link CompletableFuture}.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface AsyncTableRegionLocator {

  /**
   * Gets the fully qualified table name instance of the table whose region we want to locate.
   */
  TableName getName();

  /**
   * Finds the region on which the given row is being served. Does not reload the cache.
   * <p/>
   * Returns the location of the region to which the row belongs.
   * @param row Row to find.
   */
  default CompletableFuture<HRegionLocation> getRegionLocation(byte[] row) {
    return getRegionLocation(row, false);
  }

  /**
   * Finds the region on which the given row is being served.
   * <p/>
   * Returns the location of the region to which the row belongs.
   * @param row    Row to find.
   * @param reload true to reload information or false to use cached information
   */
  default CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, boolean reload) {
    return getRegionLocation(row, RegionInfo.DEFAULT_REPLICA_ID, reload);
  }

  /**
   * Finds the region with the given <code>replicaId</code> on which the given row is being served.
   * <p/>
   * Returns the location of the region with the given <code>replicaId</code> to which the row
   * belongs.
   * @param row       Row to find.
   * @param replicaId the replica id of the region
   */
  default CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, int replicaId) {
    return getRegionLocation(row, replicaId, false);
  }

  /**
   * Finds the region with the given <code>replicaId</code> on which the given row is being served.
   * <p/>
   * Returns the location of the region with the given <code>replicaId</code> to which the row
   * belongs.
   * @param row       Row to find.
   * @param replicaId the replica id of the region
   * @param reload    true to reload information or false to use cached information
   */
  CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, int replicaId, boolean reload);

  /**
   * Find all the replicas for the region on which the given row is being served.
   * @param row Row to find.
   * @return Locations for all the replicas of the row.
   */
  default CompletableFuture<List<HRegionLocation>> getRegionLocations(byte[] row) {
    return getRegionLocations(row, false);
  }

  /**
   * Find all the replicas for the region on which the given row is being served.
   * @param row    Row to find.
   * @param reload true to reload information or false to use cached information
   * @return Locations for all the replicas of the row.
   */
  CompletableFuture<List<HRegionLocation>> getRegionLocations(byte[] row, boolean reload);

  /**
   * Retrieves all of the regions associated with this table.
   * <p/>
   * Usually we will go to meta table directly in this method so there is no {@code reload}
   * parameter.
   * <p/>
   * Notice that the location for region replicas other than the default replica are also returned.
   * @return a {@link List} of all regions associated with this table.
   */
  CompletableFuture<List<HRegionLocation>> getAllRegionLocations();

  /**
   * Bulk lookup of region locations from {@code hbase:meta} in a single RPC, starting at
   * {@code startKey} (region start-key boundary, inclusive) and returning at most {@code limit}
   * regions in start-key order.
   * <p/>
   * The returned list includes all replicas of each region (matching
   * {@link #getAllRegionLocations()}), and the result is also written to the connection's region
   * location cache.
   * <p/>
   * Ordering: regions are returned in ascending region start-key order (the natural order of
   * {@code hbase:meta} rows for a single table). Within each region, replicas are returned in
   * ascending replica-id order (replica 0, then 1, then 2, ...). Split parents are filtered out,
   * which may cause a page to contain fewer than {@code limit} regions but never disturbs ordering
   * of the survivors.
   * <p/>
   * To page through all regions of a table, call repeatedly passing
   * {@code last.getRegion().getEndKey()} as the next {@code startKey}, where {@code last} is the
   * final element of the previous response. All replicas of a region share the same
   * {@link RegionInfo}, so the last entry's end key is the correct cursor regardless of which
   * replica it is. Pass {@code null} for the first call. Stop paging when the returned list is
   * empty or when the last region's end key is {@link HConstants#EMPTY_END_ROW} (zero-length) -
   * that signals the end of the table; passing it back in would re-scan from the beginning since by
   * convention an empty start key means "from the first region".
   * <p/>
   * Unlike {@link #getAllRegionLocations()}, this method performs at most one RPC against
   * {@code hbase:meta} per invocation, so its latency is bounded by {@code limit} rather than table
   * size. The single-RPC behavior is best-effort: if the response would exceed
   * {@code hbase.client.scanner.max.result.size} (default 2 MB), the server may split the slice
   * across multiple {@code ScannerNext} RPCs. For typical meta row sizes and default caching this
   * rarely fires, but callers passing large {@code limit} values against clusters with replicas or
   * heavy meta rows should treat single-RPC as a soft guarantee, not absolute. Note that this
   * method does not coordinate with other in-flight meta lookups on the connection - aggregate
   * pacing across concurrent callers is the caller's responsibility.
   * <p/>
   * This method is optional. Implementations that cannot support paginated lookups will return a
   * future that completes exceptionally with {@link UnsupportedOperationException} (the default
   * behavior); callers should fall back to {@link #getAllRegionLocations()} in that case.
   * @param startKey region start-key to begin scanning from (inclusive); {@code null} or empty
   *                 starts from the first region
   * @param limit    maximum number of regions to return. If &lt;= 0, falls back to
   *                 {@code hbase.meta.scanner.caching} - this is a SOFT cap on a single page, NOT
   *                 "all regions"; tables larger than the cap still require the caller to keep
   *                 paging via {@code last.getRegion().getEndKey()}.
   * @return up to {@code limit} {@link HRegionLocation}s in start-key order, possibly empty when no
   *         more regions exist; errors are reported via the returned future
   */
  default CompletableFuture<List<HRegionLocation>> getRegionLocationsPage(byte[] startKey,
    int limit) {
    CompletableFuture<List<HRegionLocation>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new UnsupportedOperationException(
      "getRegionLocationsPage(byte[], int) is not supported by this AsyncTableRegionLocator;"
        + " fall back to getAllRegionLocations()"));
    return failed;
  }

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   */
  default CompletableFuture<List<byte[]>> getStartKeys() {
    return getStartEndKeys().thenApply(
      startEndKeys -> startEndKeys.stream().map(Pair::getFirst).collect(Collectors.toList()));
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   */
  default CompletableFuture<List<byte[]>> getEndKeys() {
    return getStartEndKeys().thenApply(
      startEndKeys -> startEndKeys.stream().map(Pair::getSecond).collect(Collectors.toList()));
  }

  /**
   * Gets the starting and ending row keys for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   */
  default CompletableFuture<List<Pair<byte[], byte[]>>> getStartEndKeys() {
    return getAllRegionLocations().thenApply(
      locs -> locs.stream().filter(loc -> RegionReplicaUtil.isDefaultReplica(loc.getRegion()))
        .map(HRegionLocation::getRegion).map(r -> Pair.newPair(r.getStartKey(), r.getEndKey()))
        .collect(Collectors.toList()));
  }

  /**
   * Clear all the entries in the region location cache.
   * <p/>
   * This may cause performance issue so use it with caution.
   */
  void clearRegionLocationCache();
}
