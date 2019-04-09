/**
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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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
   * @param row Row to find.
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
   * @param row Row to find.
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
   * @param row Row to find.
   * @param replicaId the replica id of the region
   * @param reload true to reload information or false to use cached information
   */
  CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, int replicaId, boolean reload);

  /**
   * Find all the replicas for the region on which the given row is being served.
   * @param row Row to find.
   * @return Locations for all the replicas of the row.
   * @throws IOException if a remote or network exception occurs
   */
  default CompletableFuture<List<HRegionLocation>> getRegionLocations(byte[] row) {
    return getRegionLocations(row, false);
  }

  /**
   * Find all the replicas for the region on which the given row is being served.
   * @param row Row to find.
   * @param reload true to reload information or false to use cached information
   * @return Locations for all the replicas of the row.
   * @throws IOException if a remote or network exception occurs
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
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  default CompletableFuture<List<byte[]>> getStartKeys() throws IOException {
    return getStartEndKeys().thenApply(
      startEndKeys -> startEndKeys.stream().map(Pair::getFirst).collect(Collectors.toList()));
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  default CompletableFuture<List<byte[]>> getEndKeys() throws IOException {
    return getStartEndKeys().thenApply(
      startEndKeys -> startEndKeys.stream().map(Pair::getSecond).collect(Collectors.toList()));
  }

  /**
   * Gets the starting and ending row keys for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  default CompletableFuture<List<Pair<byte[], byte[]>>> getStartEndKeys() throws IOException {
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
