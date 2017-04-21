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

import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Pair;

/**
 *  The asynchronous administrative API for HBase.
 */
@InterfaceAudience.Public
public interface AsyncAdmin {

  /**
   * @return Async Connection used by this object.
   */
  AsyncConnectionImpl getConnection();

  /**
   * @param tableName Table to check.
   * @return True if table exists already. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> tableExists(final TableName tableName);

  /**
   * List all the userspace tables.
   * @return - returns an array of HTableDescriptors wrapped by a {@link CompletableFuture}.
   * @see #listTables(Pattern, boolean)
   */
  CompletableFuture<HTableDescriptor[]> listTables();

  /**
   * List all the tables matching the given pattern.
   * @param regex The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return - returns an array of HTableDescriptors wrapped by a {@link CompletableFuture}.
   * @see #listTables(Pattern, boolean)
   */
  CompletableFuture<HTableDescriptor[]> listTables(String regex, boolean includeSysTables);

  /**
   * List all the tables matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return - returns an array of HTableDescriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> listTables(Pattern pattern, boolean includeSysTables);

  /**
   * List all of the names of userspace tables.
   * @return TableName[] an array of table names wrapped by a {@link CompletableFuture}.
   * @see #listTableNames(Pattern, boolean)
   */
  CompletableFuture<TableName[]> listTableNames();

  /**
   * List all of the names of userspace tables.
   * @param regex The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return TableName[] an array of table names wrapped by a {@link CompletableFuture}.
   * @see #listTableNames(Pattern, boolean)
   */
  CompletableFuture<TableName[]> listTableNames(final String regex, final boolean includeSysTables);

  /**
   * List all of the names of userspace tables.
   * @param pattern The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return TableName[] an array of table names wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<TableName[]> listTableNames(final Pattern pattern,
      final boolean includeSysTables);

  /**
   * Method for getting the tableDescriptor
   * @param tableName as a {@link TableName}
   * @return the tableDescriptor wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor> getTableDescriptor(final TableName tableName);

  /**
   * Creates a new table.
   * @param desc table descriptor for table
   */
  CompletableFuture<Void> createTable(HTableDescriptor desc);

  /**
   * Creates a new table with the specified number of regions. The start key specified will become
   * the end key of the first region of the table, and the end key specified will become the start
   * key of the last region of the table (the first region has a null start key and the last region
   * has a null end key). BigInteger math will be used to divide the key range specified into enough
   * segments to make the required number of total regions.
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   */
  CompletableFuture<Void> createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions);

  /**
   * Creates a new table with an initial set of empty regions defined by the specified split keys.
   * The total number of regions created will be the number of split keys plus one.
   * Note : Avoid passing empty split key.
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   */
  CompletableFuture<Void> createTable(final HTableDescriptor desc, byte[][] splitKeys);

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   */
  CompletableFuture<Void> deleteTable(final TableName tableName);

  /**
   * Deletes tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(String, boolean)} and
   * {@link #deleteTable(org.apache.hadoop.hbase.TableName)}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be deleted. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> deleteTables(String regex);

  /**
   * Delete tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(Pattern, boolean) } and
   * {@link #deleteTable(org.apache.hadoop.hbase.TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> deleteTables(Pattern pattern);

  /**
   * Truncate a table.
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   */
  CompletableFuture<Void> truncateTable(final TableName tableName, final boolean preserveSplits);

  /**
   * Enable a table. The table has to be in disabled state for it to be enabled.
   * @param tableName name of the table
   */
  CompletableFuture<Void> enableTable(final TableName tableName);

  /**
   * Enable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #enableTable(TableName)}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be enabled. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> enableTables(String regex);

  /**
   * Enable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #enableTable(TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be enabled. The return value will be wrapped
   *         by a {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> enableTables(Pattern pattern);

  /**
   * Disable a table. The table has to be in enabled state for it to be disabled.
   * @param tableName
   */
  CompletableFuture<Void> disableTable(final TableName tableName);

  /**
   * Disable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #disableTable(TableName)}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be disabled. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> disableTables(String regex);

  /**
   * Disable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Pattern, boolean)} and
   * {@link #disableTable(TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<HTableDescriptor[]> disableTables(Pattern pattern);

  /**
   * @param tableName name of table to check
   * @return true if table is off-line. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> isTableDisabled(TableName tableName);

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> isTableAvailable(TableName tableName);

  /**
   * Use this api to check if the table has been created with the specified number of splitkeys
   * which was used while creating the given table. Note : If this api is used after a table's
   * region gets splitted, the api may return false. The return value will be wrapped by a
   * {@link CompletableFuture}.
   * @param tableName name of table to check
   * @param splitKeys keys to check if the table has been created with all split keys
   */
  CompletableFuture<Boolean> isTableAvailable(TableName tableName, byte[][] splitKeys);

  /**
   * Get the status of alter command - indicates how many regions have received the updated schema
   * Asynchronous operation.
   * @param tableName TableName instance
   * @return Pair indicating the number of regions updated Pair.getFirst() is the regions that are
   *         yet to be updated Pair.getSecond() is the total number of regions of the table. The
   *         return value will be wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Pair<Integer, Integer>> getAlterStatus(final TableName tableName);

  /**
   * Add a column family to an existing table.
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   */
  CompletableFuture<Void> addColumnFamily(final TableName tableName,
      final HColumnDescriptor columnFamily);

  /**
   * Delete a column family from a table.
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   */
  CompletableFuture<Void> deleteColumnFamily(final TableName tableName, final byte[] columnFamily);

  /**
   * Modify an existing column family on a table.
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   */
  CompletableFuture<Void> modifyColumnFamily(final TableName tableName,
      final HColumnDescriptor columnFamily);

  /**
   * Create a new namespace.
   * @param descriptor descriptor which describes the new namespace
   */
  CompletableFuture<Void> createNamespace(final NamespaceDescriptor descriptor);

  /**
   * Modify an existing namespace.
   * @param descriptor descriptor which describes the new namespace
   */
  CompletableFuture<Void> modifyNamespace(final NamespaceDescriptor descriptor);

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
   * @param name namespace name
   */
  CompletableFuture<Void> deleteNamespace(final String name);

  /**
   * Get a namespace descriptor by name
   * @param name name of namespace descriptor
   * @return A descriptor wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(final String name);

  /**
   * List available namespace descriptors
   * @return List of descriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<NamespaceDescriptor[]> listNamespaceDescriptors();

  /**
   * @param tableName name of table to check
   * @return true if table is on-line. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> isTableEnabled(TableName tableName);

  /**
   * Turn the load balancer on or off.
   * @param on
   * @return Previous balancer value wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> setBalancerRunning(final boolean on);

  /**
   * Invoke the balancer. Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments. Can NOT run for various reasons. Check logs.
   * @return True if balancer ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> balancer();

  /**
   * Invoke the balancer. Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments. If there is region in transition, force parameter of true would still run
   * balancer. Can *not* run for other reasons. Check logs.
   * @param force whether we should force balance even if there is region in transition.
   * @return True if balancer ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> balancer(boolean force);

  /**
   * Query the current state of the balancer.
   * @return true if the balancer is enabled, false otherwise.
   *         The return value will be wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> isBalancerEnabled();

  /**
   * Close a region. For expert-admins.  Runs close on the regionserver.  The master will not be
   * informed of the close.
   *
   * @param regionname region name to close
   * @param serverName If supplied, we'll use this location rather than the one currently in
   * <code>hbase:meta</code>
   */
  CompletableFuture<Void> closeRegion(String regionname, String serverName);

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The master will not be
   * informed of the close.
   *
   * @param regionname region name to close
   * @param serverName The servername of the regionserver.  If passed null we will use servername
   * found in the hbase:meta table. A server name is made of host, port and startcode.  Here is an
   * example: <code> host187.example.com,60020,1289493121758</code>
   */
  CompletableFuture<Void> closeRegion(byte[] regionname, String serverName);

  /**
   * For expert-admins. Runs close on the regionserver. Closes a region based on the encoded region
   * name. The region server name is mandatory. If the servername is provided then based on the
   * online regions in the specified regionserver the specified region will be closed. The master
   * will not be informed of the close. Note that the regionname is the encoded regionname.
   *
   * @param encodedRegionName The encoded region name; i.e. the hash that makes up the region name
   * suffix: e.g. if regionname is
   * <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   * then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param serverName The servername of the regionserver. A server name is made of host, port and
   * startcode. This is mandatory. Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>
   * @return true if the region was closed, false if not. The return value will be wrapped by a
   * {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> closeRegionWithEncodedRegionName(String encodedRegionName, String serverName);

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The master will not be
   * informed of the close.
   *
   * @param sn
   * @param hri
   */
  CompletableFuture<Void> closeRegion(ServerName sn, HRegionInfo hri);

  /**
   * Merge two regions.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge two adjacent
   *          regions
   */
  CompletableFuture<Void> mergeRegions(final byte[] nameOfRegionA, final byte[] nameOfRegionB,
      final boolean forcible);

  /**
   * Split a table. The method will execute split action for each region in table.
   * @param tableName table to split
   */
  CompletableFuture<Void> split(final TableName tableName);

  /**
   * Split an individual region.
   * @param regionName region to split
   */
  CompletableFuture<Void> splitRegion(final byte[] regionName);

  /**
   * Split a table.
   * @param tableName table to split
   * @param splitPoint the explicit position to split on
   */
  CompletableFuture<Void> split(final TableName tableName, final byte[] splitPoint);

  /**
   * Split an individual region.
   * @param regionName region to split
   * @param splitPoint the explicit position to split on
   */
  CompletableFuture<Void> splitRegion(final byte[] regionName, final byte[] splitPoint);

  /**
   * @param regionName Encoded or full name of region to assign.
   */
  CompletableFuture<Void> assign(final byte[] regionName);

  /**
   * Unassign a region from current hosting regionserver. Region will then be assigned to a
   * regionserver chosen at random. Region could be reassigned back to the same server. Use
   * {@link #move(byte[], byte[])} if you want to control the region movement.
   * @param regionName Encoded or full name of region to unassign. Will clear any existing
   *          RegionPlan if one found.
   * @param force If true, force unassign (Will remove region from regions-in-transition too if
   *          present. If results in double assignment use hbck -fix to resolve. To be used by
   *          experts).
   */
  CompletableFuture<Void> unassign(final byte[] regionName, final boolean force);

  /**
   * Offline specified region from master's in-memory state. It will not attempt to reassign the
   * region as in unassign. This API can be used when a region not served by any region server and
   * still online as per Master's in memory state. If this API is incorrectly used on active region
   * then master will loose track of that region. This is a special method that should be used by
   * experts or hbck.
   * @param regionName Encoded or full name of region to offline
   */
  CompletableFuture<Void> offline(final byte[] regionName);

  /**
   * Move the region <code>r</code> to <code>dest</code>.
   * @param regionName Encoded or full name of region to move.
   * @param destServerName The servername of the destination regionserver. If passed the empty byte
   *          array we'll assign to a random server. A server name is made of host, port and
   *          startcode. Here is an example: <code> host187.example.com,60020,1289493121758</code>
   */
  CompletableFuture<Void> move(final byte[] regionName, final byte[] destServerName);

  /**
   * Apply the new quota settings.
   * @param quota the quota settings
   */
  CompletableFuture<Void> setQuota(final QuotaSettings quota);

  /**
   * List the quotas based on the filter.
   * @param filter the quota settings filter
   * @return the QuotaSetting list, which wrapped by a CompletableFuture.
   */
  CompletableFuture<List<QuotaSettings>> getQuota(QuotaFilter filter);

  /**
   * Add a new replication peer for replicating data to slave cluster
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication slave cluster
   */
  CompletableFuture<Void> addReplicationPeer(final String peerId,
      final ReplicationPeerConfig peerConfig);

  /**
   * Remove a peer and stop the replication
   * @param peerId a short name that identifies the peer
   */
  CompletableFuture<Void> removeReplicationPeer(final String peerId);

  /**
   * Restart the replication stream to the specified peer
   * @param peerId a short name that identifies the peer
   */
  CompletableFuture<Void> enableReplicationPeer(final String peerId);

  /**
   * Stop the replication stream to the specified peer
   * @param peerId a short name that identifies the peer
   */
  CompletableFuture<Void> disableReplicationPeer(final String peerId);

  /**
   * Returns the configured ReplicationPeerConfig for the specified peer
   * @param peerId a short name that identifies the peer
   * @return ReplicationPeerConfig for the peer wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<ReplicationPeerConfig> getReplicationPeerConfig(final String peerId);

  /**
   * Update the peerConfig for the specified peer
   * @param peerId a short name that identifies the peer
   * @param peerConfig new config for the peer
   */
  CompletableFuture<Void> updateReplicationPeerConfig(final String peerId,
      final ReplicationPeerConfig peerConfig);

  /**
   * Append the replicable table-cf config of the specified peer
   * @param id a short that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   */
  CompletableFuture<Void> appendReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs);

  /**
   * Remove some table-cfs from config of the specified peer
   * @param id a short name that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   */
  CompletableFuture<Void> removeReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs);

  /**
   * Return a list of replication peers.
   * @return a list of replication peers description. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers();

  /**
   * Return a list of replication peers.
   * @param regex The regular expression to match peer id
   * @return a list of replication peers description. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(String regex);

  /**
   * Return a list of replication peers.
   * @param pattern The compiled regular expression to match peer id
   * @return a list of replication peers description. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(Pattern pattern);

  /**
   * Find all table and column families that are replicated from this cluster
   * @return the replicated table-cfs list of this cluster. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<TableCFs>> listReplicatedTableCFs();
}
