/**
 *
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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Used to view region location information for a single HBase table. Obtain an instance from an
 * {@link Connection}.
 * @see ConnectionFactory
 * @see Connection
 * @see Table
 * @since 0.99.0
 */
@InterfaceAudience.Public
public interface RegionLocator extends Closeable {

  /** Configuration for Region Locator's mode when meta replica is configured.
   * Valid values are: HedgedRead, LoadBalance, None
   */
  String LOCATOR_META_REPLICAS_MODE = "hbase.locator.meta.replicas.mode";

  /** Configuration for meta replica selector when Region Locator's LoadBalance mode is configured.
   * The default value is org.apache.hadoop.hbase.client.CatalogReplicaLoadBalanceSimpleSelector.
   */
  String LOCATOR_META_REPLICAS_MODE_LOADBALANCE_SELECTOR =
    "hbase.locator.meta.replicas.mode.loadbalance.selector";

  /**
   * Finds the region on which the given row is being served. Does not reload the cache.
   * @param row Row to find.
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  default HRegionLocation getRegionLocation(byte[] row) throws IOException {
    return getRegionLocation(row, false);
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @param reload true to reload information or false to use cached information
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  default HRegionLocation getRegionLocation(byte[] row, boolean reload) throws IOException {
    return getRegionLocation(row, RegionInfo.DEFAULT_REPLICA_ID, reload);
  }

  /**
   * Finds the region with the given replica id on which the given row is being served.
   * @param row Row to find.
   * @param replicaId the replica id
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  default HRegionLocation getRegionLocation(byte[] row, int replicaId) throws IOException {
    return getRegionLocation(row, replicaId, false);
  }

  /**
   * Finds the region with the given replica id on which the given row is being served.
   * @param row Row to find.
   * @param replicaId the replica id
   * @param reload true to reload information or false to use cached information
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  HRegionLocation getRegionLocation(byte[] row, int replicaId, boolean reload) throws IOException;

  /**
   * Find all the replicas for the region on which the given row is being served.
   * @param row Row to find.
   * @return Locations for all the replicas of the row.
   * @throws IOException if a remote or network exception occurs
   */
  default List<HRegionLocation> getRegionLocations(byte[] row) throws IOException {
    return getRegionLocations(row, false);
  }

  /**
   * Find all the replicas for the region on which the given row is being served.
   * @param row Row to find.
   * @param reload true to reload information or false to use cached information
   * @return Locations for all the replicas of the row.
   * @throws IOException if a remote or network exception occurs
   */
  List<HRegionLocation> getRegionLocations(byte[] row, boolean reload) throws IOException;

  /**
   * Clear all the entries in the region location cache.
   * <p/>
   * This may cause performance issue so use it with caution.
   */
  void clearRegionLocationCache();

  /**
   * Retrieves all of the regions associated with this table.
   * <p/>
   * Usually we will go to meta table directly in this method so there is no {@code reload}
   * parameter.
   * <p/>
   * Notice that the location for region replicas other than the default replica are also returned.
   * @return a {@link List} of all regions associated with this table.
   * @throws IOException if a remote or network exception occurs
   */
  List<HRegionLocation> getAllRegionLocations() throws IOException;

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  default byte[][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  default byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  /**
   * Gets the starting and ending row keys for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  default Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    List<HRegionLocation> regions = getAllRegionLocations().stream()
      .filter(loc -> RegionReplicaUtil.isDefaultReplica(loc.getRegion()))
      .collect(Collectors.toList());
    byte[][] startKeys = new byte[regions.size()][];
    byte[][] endKeys = new byte[regions.size()][];
    for (int i = 0, n = regions.size(); i < n; i++) {
      RegionInfo region = regions.get(i).getRegion();
      startKeys[i] = region.getStartKey();
      endKeys[i] = region.getEndKey();
    }
    return Pair.newPair(startKeys, endKeys);
  }

  /**
   * Gets the fully qualified table name instance of this table.
   */
  TableName getName();
}
