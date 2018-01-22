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

import com.google.protobuf.RpcChannel;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The asynchronous administrative API for HBase.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface AsyncAdmin {

  /**
   * @param tableName Table to check.
   * @return True if table exists already. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> tableExists(TableName tableName);

  /**
   * List all the userspace tables.
   * @return - returns a list of TableDescriptors wrapped by a {@link CompletableFuture}.
   */
  default CompletableFuture<List<TableDescriptor>> listTableDescriptors() {
    return listTableDescriptors(false);
  }

  /**
   * List all the tables.
   * @param includeSysTables False to match only against userspace tables
   * @return - returns a list of TableDescriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableDescriptor>> listTableDescriptors(boolean includeSysTables);

  /**
   * List all the tables matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return - returns a list of TableDescriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableDescriptor>> listTableDescriptors(Pattern pattern,
      boolean includeSysTables);

  /**
   * Get list of table descriptors by namespace.
   * @param name namespace name
   * @return returns a list of TableDescriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableDescriptor>> listTableDescriptorsByNamespace(String name);

  /**
   * List all of the names of userspace tables.
   * @return a list of table names wrapped by a {@link CompletableFuture}.
   * @see #listTableNames(Pattern, boolean)
   */
  default CompletableFuture<List<TableName>> listTableNames() {
    return listTableNames(false);
  }

  /**
   * List all of the names of tables.
   * @param includeSysTables False to match only against userspace tables
   * @return a list of table names wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableName>> listTableNames(boolean includeSysTables);

  /**
   * List all of the names of userspace tables.
   * @param pattern The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return a list of table names wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableName>> listTableNames(Pattern pattern, boolean includeSysTables);

  /**
   * Get list of table names by namespace.
   * @param name namespace name
   * @return The list of table names in the namespace wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableName>> listTableNamesByNamespace(String name);

  /**
   * Method for getting the tableDescriptor
   * @param tableName as a {@link TableName}
   * @return the read-only tableDescriptor wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<TableDescriptor> getDescriptor(TableName tableName);

  /**
   * Creates a new table.
   * @param desc table descriptor for table
   */
  CompletableFuture<Void> createTable(TableDescriptor desc);

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
  CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions);

  /**
   * Creates a new table with an initial set of empty regions defined by the specified split keys.
   * The total number of regions created will be the number of split keys plus one.
   * Note : Avoid passing empty split key.
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   */
  CompletableFuture<Void> createTable(TableDescriptor desc, byte[][] splitKeys);

  /**
   * Modify an existing table, more IRB friendly version.
   * @param desc modified description of the table
   */
  CompletableFuture<Void> modifyTable(TableDescriptor desc);

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   */
  CompletableFuture<Void> deleteTable(TableName tableName);

  /**
   * Truncate a table.
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   */
  CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits);

  /**
   * Enable a table. The table has to be in disabled state for it to be enabled.
   * @param tableName name of the table
   */
  CompletableFuture<Void> enableTable(TableName tableName);

  /**
   * Disable a table. The table has to be in enabled state for it to be disabled.
   * @param tableName
   */
  CompletableFuture<Void> disableTable(TableName tableName);

  /**
   * @param tableName name of table to check
   * @return true if table is on-line. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> isTableEnabled(TableName tableName);

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
   * Add a column family to an existing table.
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   */
  CompletableFuture<Void> addColumnFamily(TableName tableName,
      ColumnFamilyDescriptor columnFamily);

  /**
   * Delete a column family from a table.
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   */
  CompletableFuture<Void> deleteColumnFamily(TableName tableName, byte[] columnFamily);

  /**
   * Modify an existing column family on a table.
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   */
  CompletableFuture<Void> modifyColumnFamily(TableName tableName,
      ColumnFamilyDescriptor columnFamily);

  /**
   * Create a new namespace.
   * @param descriptor descriptor which describes the new namespace
   */
  CompletableFuture<Void> createNamespace(NamespaceDescriptor descriptor);

  /**
   * Modify an existing namespace.
   * @param descriptor descriptor which describes the new namespace
   */
  CompletableFuture<Void> modifyNamespace(NamespaceDescriptor descriptor);

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
   * @param name namespace name
   */
  CompletableFuture<Void> deleteNamespace(String name);

  /**
   * Get a namespace descriptor by name
   * @param name name of namespace descriptor
   * @return A descriptor wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(String name);

  /**
   * List available namespace descriptors
   * @return List of descriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors();

  /**
   * Get all the online regions on a region server.
   */
  CompletableFuture<List<RegionInfo>> getRegions(ServerName serverName);

  /**
   * Get the regions of a given table.
   */
  CompletableFuture<List<RegionInfo>> getRegions(TableName tableName);

  /**
   * Flush a table.
   * @param tableName table to flush
   */
  CompletableFuture<Void> flush(TableName tableName);

  /**
   * Flush an individual region.
   * @param regionName region to flush
   */
  CompletableFuture<Void> flushRegion(byte[] regionName);

  /**
   * Flush all region on the region server.
   * @param serverName server to flush
   */
  CompletableFuture<Void> flushRegionServer(ServerName serverName);

  /**
   * Compact a table. When the returned CompletableFuture is done, it only means the compact request
   * was sent to HBase and may need some time to finish the compact operation.
   * @param tableName table to compact
   */
  default CompletableFuture<Void> compact(TableName tableName) {
    return compact(tableName, CompactType.NORMAL);
  }

  /**
   * Compact a column family within a table. When the returned CompletableFuture is done, it only
   * means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param tableName table to compact
   * @param columnFamily column family within a table. If not present, compact the table's all
   *          column families.
   */
  default CompletableFuture<Void> compact(TableName tableName, byte[] columnFamily) {
    return compact(tableName, columnFamily, CompactType.NORMAL);
  }

  /**
   * Compact a table. When the returned CompletableFuture is done, it only means the compact request
   * was sent to HBase and may need some time to finish the compact operation.
   * @param tableName table to compact
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   */
  CompletableFuture<Void> compact(TableName tableName, CompactType compactType);

  /**
   * Compact a column family within a table. When the returned CompletableFuture is done, it only
   * means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param tableName table to compact
   * @param columnFamily column family within a table
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   */
  CompletableFuture<Void> compact(TableName tableName, byte[] columnFamily,
      CompactType compactType);

  /**
   * Compact an individual region. When the returned CompletableFuture is done, it only means the
   * compact request was sent to HBase and may need some time to finish the compact operation.
   * @param regionName region to compact
   */
  CompletableFuture<Void> compactRegion(byte[] regionName);

  /**
   * Compact a column family within a region. When the returned CompletableFuture is done, it only
   * means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param regionName region to compact
   * @param columnFamily column family within a region. If not present, compact the region's all
   *          column families.
   */
  CompletableFuture<Void> compactRegion(byte[] regionName, byte[] columnFamily);

  /**
   * Major compact a table. When the returned CompletableFuture is done, it only means the compact
   * request was sent to HBase and may need some time to finish the compact operation.
   * @param tableName table to major compact
   */
  default CompletableFuture<Void> majorCompact(TableName tableName) {
    return majorCompact(tableName, CompactType.NORMAL);
  }

  /**
   * Major compact a column family within a table. When the returned CompletableFuture is done, it
   * only means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param tableName table to major compact
   * @param columnFamily column family within a table. If not present, major compact the table's all
   *          column families.
   */
  default CompletableFuture<Void> majorCompact(TableName tableName, byte[] columnFamily) {
    return majorCompact(tableName, columnFamily, CompactType.NORMAL);
  }

  /**
   * Major compact a table. When the returned CompletableFuture is done, it only means the compact
   * request was sent to HBase and may need some time to finish the compact operation.
   * @param tableName table to major compact
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   */
  CompletableFuture<Void> majorCompact(TableName tableName, CompactType compactType);

  /**
   * Major compact a column family within a table. When the returned CompletableFuture is done, it
   * only means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param tableName table to major compact
   * @param columnFamily column family within a table. If not present, major compact the table's all
   *          column families.
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   */
  CompletableFuture<Void> majorCompact(TableName tableName, byte[] columnFamily,
      CompactType compactType);

  /**
   * Major compact a region. When the returned CompletableFuture is done, it only means the compact
   * request was sent to HBase and may need some time to finish the compact operation.
   * @param regionName region to major compact
   */
  CompletableFuture<Void> majorCompactRegion(byte[] regionName);

  /**
   * Major compact a column family within region. When the returned CompletableFuture is done, it
   * only means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param regionName region to major compact
   * @param columnFamily column family within a region. If not present, major compact the region's
   *          all column families.
   */
  CompletableFuture<Void> majorCompactRegion(byte[] regionName, byte[] columnFamily);

  /**
   * Compact all regions on the region server.
   * @param serverName the region server name
   */
  CompletableFuture<Void> compactRegionServer(ServerName serverName);

  /**
   * Compact all regions on the region server.
   * @param serverName the region server name
   */
  CompletableFuture<Void> majorCompactRegionServer(ServerName serverName);

  /**
   * Turn the Merge switch on or off.
   * @param on
   * @return Previous switch value wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> mergeSwitch(boolean on);

  /**
   * Query the current state of the Merge switch.
   * @return true if the switch is on, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isMergeEnabled();

  /**
   * Turn the Split switch on or off.
   * @param on
   * @return Previous switch value wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> splitSwitch(boolean on);

  /**
   * Query the current state of the Split switch.
   * @return true if the switch is on, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isSplitEnabled();

  /**
   * Merge two regions.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge two adjacent
   *          regions
   */
  CompletableFuture<Void> mergeRegions(byte[] nameOfRegionA, byte[] nameOfRegionB,
      boolean forcible);

  /**
   * Split a table. The method will execute split action for each region in table.
   * @param tableName table to split
   */
  CompletableFuture<Void> split(TableName tableName);

  /**
   * Split an individual region.
   * @param regionName region to split
   */
  CompletableFuture<Void> splitRegion(byte[] regionName);

  /**
   * Split a table.
   * @param tableName table to split
   * @param splitPoint the explicit position to split on
   */
  CompletableFuture<Void> split(TableName tableName, byte[] splitPoint);

  /**
   * Split an individual region.
   * @param regionName region to split
   * @param splitPoint the explicit position to split on. If not present, it will decide by region
   *          server.
   */
  CompletableFuture<Void> splitRegion(byte[] regionName, byte[] splitPoint);

  /**
   * @param regionName Encoded or full name of region to assign.
   */
  CompletableFuture<Void> assign(byte[] regionName);

  /**
   * Unassign a region from current hosting regionserver. Region will then be assigned to a
   * regionserver chosen at random. Region could be reassigned back to the same server. Use
   * {@link #move(byte[], ServerName)} if you want to control the region movement.
   * @param regionName Encoded or full name of region to unassign. Will clear any existing
   *          RegionPlan if one found.
   * @param forcible If true, force unassign (Will remove region from regions-in-transition too if
   *          present. If results in double assignment use hbck -fix to resolve. To be used by
   *          experts).
   */
  CompletableFuture<Void> unassign(byte[] regionName, boolean forcible);

  /**
   * Offline specified region from master's in-memory state. It will not attempt to reassign the
   * region as in unassign. This API can be used when a region not served by any region server and
   * still online as per Master's in memory state. If this API is incorrectly used on active region
   * then master will loose track of that region. This is a special method that should be used by
   * experts or hbck.
   * @param regionName Encoded or full name of region to offline
   */
  CompletableFuture<Void> offline(byte[] regionName);

  /**
   * Move the region <code>r</code> to a random server.
   * @param regionName Encoded or full name of region to move.
   */
  CompletableFuture<Void> move(byte[] regionName);

  /**
   * Move the region <code>r</code> to <code>dest</code>.
   * @param regionName Encoded or full name of region to move.
   * @param destServerName The servername of the destination regionserver. If not present, we'll
   *          assign to a random server. A server name is made of host, port and startcode. Here is
   *          an example: <code> host187.example.com,60020,1289493121758</code>
   */
  CompletableFuture<Void> move(byte[] regionName, ServerName destServerName);

  /**
   * Apply the new quota settings.
   * @param quota the quota settings
   */
  CompletableFuture<Void> setQuota(QuotaSettings quota);

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
  default CompletableFuture<Void> addReplicationPeer(String peerId,
      ReplicationPeerConfig peerConfig) {
    return addReplicationPeer(peerId, peerConfig, true);
  }

  /**
   * Add a new replication peer for replicating data to slave cluster
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication slave cluster
   * @param enabled peer state, true if ENABLED and false if DISABLED
   */
  CompletableFuture<Void> addReplicationPeer(String peerId,
      ReplicationPeerConfig peerConfig, boolean enabled);

  /**
   * Remove a peer and stop the replication
   * @param peerId a short name that identifies the peer
   */
  CompletableFuture<Void> removeReplicationPeer(String peerId);

  /**
   * Restart the replication stream to the specified peer
   * @param peerId a short name that identifies the peer
   */
  CompletableFuture<Void> enableReplicationPeer(String peerId);

  /**
   * Stop the replication stream to the specified peer
   * @param peerId a short name that identifies the peer
   */
  CompletableFuture<Void> disableReplicationPeer(String peerId);

  /**
   * Returns the configured ReplicationPeerConfig for the specified peer
   * @param peerId a short name that identifies the peer
   * @return ReplicationPeerConfig for the peer wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<ReplicationPeerConfig> getReplicationPeerConfig(String peerId);

  /**
   * Update the peerConfig for the specified peer
   * @param peerId a short name that identifies the peer
   * @param peerConfig new config for the peer
   */
  CompletableFuture<Void> updateReplicationPeerConfig(String peerId,
      ReplicationPeerConfig peerConfig);

  /**
   * Transit current cluster to a new state in a synchronous replication peer.
   * @param peerId a short name that identifies the peer
   * @param state a new state of current cluster
   */
  CompletableFuture<Void> transitReplicationPeerSyncReplicationState(String peerId,
      SyncReplicationState state);

  /**
   * Get the current cluster state in a synchronous replication peer.
   * @param peerId a short name that identifies the peer
   * @return the current cluster state wrapped by a {@link CompletableFuture}.
   */
  default CompletableFuture<SyncReplicationState>
      getReplicationPeerSyncReplicationState(String peerId) {
    CompletableFuture<SyncReplicationState> future = new CompletableFuture<>();
    listReplicationPeers(Pattern.compile(peerId)).whenComplete((peers, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else if (peers.isEmpty() || !peers.get(0).getPeerId().equals(peerId)) {
        future.completeExceptionally(
          new IOException("Replication peer " + peerId + " does not exist"));
      } else {
        future.complete(peers.get(0).getSyncReplicationState());
      }
    });
    return future;
  }

  /**
   * Append the replicable table-cf config of the specified peer
   * @param peerId a short that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   */
  CompletableFuture<Void> appendReplicationPeerTableCFs(String peerId,
      Map<TableName, List<String>> tableCfs);

  /**
   * Remove some table-cfs from config of the specified peer
   * @param peerId a short name that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   */
  CompletableFuture<Void> removeReplicationPeerTableCFs(String peerId,
      Map<TableName, List<String>> tableCfs);

  /**
   * Return a list of replication peers.
   * @return a list of replication peers description. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers();

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

  /**
   * Enable a table's replication switch.
   * @param tableName name of the table
   */
  CompletableFuture<Void> enableTableReplication(TableName tableName);

  /**
   * Disable a table's replication switch.
   * @param tableName name of the table
   */
  CompletableFuture<Void> disableTableReplication(TableName tableName);

  /**
   * Take a snapshot for the given table. If the table is enabled, a FLUSH-type snapshot will be
   * taken. If the table is disabled, an offline snapshot is taken. Snapshots are considered unique
   * based on <b>the name of the snapshot</b>. Attempts to take a snapshot with the same name (even
   * a different type or with different parameters) will fail with a
   * {@link org.apache.hadoop.hbase.snapshot.SnapshotCreationException} indicating the duplicate
   * naming. Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   */
  default CompletableFuture<Void> snapshot(String snapshotName, TableName tableName) {
    return snapshot(snapshotName, tableName, SnapshotType.FLUSH);
  }

  /**
   * Create typed snapshot of the table. Snapshots are considered unique based on <b>the name of the
   * snapshot</b>. Attempts to take a snapshot with the same name (even a different type or with
   * different parameters) will fail with a
   * {@link org.apache.hadoop.hbase.snapshot.SnapshotCreationException} indicating the duplicate
   * naming. Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * @param snapshotName name to give the snapshot on the filesystem. Must be unique from all other
   *          snapshots stored on the cluster
   * @param tableName name of the table to snapshot
   * @param type type of snapshot to take
   */
  default CompletableFuture<Void> snapshot(String snapshotName, TableName tableName,
      SnapshotType type) {
    return snapshot(new SnapshotDescription(snapshotName, tableName, type));
  }

  /**
   * Take a snapshot and wait for the server to complete that snapshot asynchronously. Only a single
   * snapshot should be taken at a time for an instance of HBase, or results may be undefined (you
   * can tell multiple HBase clusters to snapshot at the same time, but only one at a time for a
   * single cluster). Snapshots are considered unique based on <b>the name of the snapshot</b>.
   * Attempts to take a snapshot with the same name (even a different type or with different
   * parameters) will fail with a {@link org.apache.hadoop.hbase.snapshot.SnapshotCreationException}
   * indicating the duplicate naming. Snapshot names follow the same naming constraints as tables in
   * HBase. See {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * You should probably use {@link #snapshot(String, org.apache.hadoop.hbase.TableName)} unless you
   * are sure about the type of snapshot that you want to take.
   * @param snapshot snapshot to take
   */
  CompletableFuture<Void> snapshot(SnapshotDescription snapshot);

  /**
   * Check the current state of the passed snapshot. There are three possible states:
   * <ol>
   * <li>running - returns <tt>false</tt></li>
   * <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the snapshot to fail</li>
   * </ol>
   * The cluster only knows about the most recent snapshot. Therefore, if another snapshot has been
   * run/started since the snapshot you are checking, you will receive an
   * {@link org.apache.hadoop.hbase.snapshot.UnknownSnapshotException}.
   * @param snapshot description of the snapshot to check
   * @return <tt>true</tt> if the snapshot is completed, <tt>false</tt> if the snapshot is still
   *         running
   */
  CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription snapshot);

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If the
   * "hbase.snapshot.restore.take.failsafe.snapshot" configuration property is set to true, a
   * snapshot of the current table is taken before executing the restore operation. In case of
   * restore failure, the failsafe snapshot will be restored. If the restore completes without
   * problem the failsafe snapshot is deleted.
   * @param snapshotName name of the snapshot to restore
   */
  CompletableFuture<Void> restoreSnapshot(String snapshotName);

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken before
   * executing the restore operation. In case of restore failure, the failsafe snapshot will be
   * restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   */
  CompletableFuture<Void> restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot);

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   */
  CompletableFuture<Void> cloneSnapshot(String snapshotName, TableName tableName);

  /**
   * List completed snapshots.
   * @return a list of snapshot descriptors for completed snapshots wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<List<SnapshotDescription>> listSnapshots();

  /**
   * List all the completed snapshots matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @return - returns a List of SnapshotDescription wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<List<SnapshotDescription>> listSnapshots(Pattern pattern);

  /**
   * List all the completed snapshots matching the given table name pattern.
   * @param tableNamePattern The compiled table name regular expression to match against
   * @return - returns a List of completed SnapshotDescription wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern);

  /**
   * List all the completed snapshots matching the given table name regular expression and snapshot
   * name regular expression.
   * @param tableNamePattern The compiled table name regular expression to match against
   * @param snapshotNamePattern The compiled snapshot name regular expression to match against
   * @return - returns a List of completed SnapshotDescription wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern);

  /**
   * Delete an existing snapshot.
   * @param snapshotName name of the snapshot
   */
  CompletableFuture<Void> deleteSnapshot(String snapshotName);

  /**
   * Delete all existing snapshots.
   */
  CompletableFuture<Void> deleteSnapshots();

  /**
   * Delete existing snapshots whose names match the pattern passed.
   * @param pattern pattern for names of the snapshot to match
   */
  CompletableFuture<Void> deleteSnapshots(Pattern pattern);

  /**
   * Delete all existing snapshots matching the given table name pattern.
   * @param tableNamePattern The compiled table name regular expression to match against
   */
  CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern);

  /**
   * Delete all existing snapshots matching the given table name regular expression and snapshot
   * name regular expression.
   * @param tableNamePattern The compiled table name regular expression to match against
   * @param snapshotNamePattern The compiled snapshot name regular expression to match against
   */
  CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern);

  /**
   * Execute a distributed procedure on a cluster.
   * @param signature A distributed procedure is uniquely identified by its signature (default the
   *          root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   *          optional.
   * @param props Property/Value pairs of properties passing to the procedure
   */
  CompletableFuture<Void> execProcedure(String signature, String instance,
      Map<String, String> props);

  /**
   * Execute a distributed procedure on a cluster.
   * @param signature A distributed procedure is uniquely identified by its signature (default the
   *          root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   *          optional.
   * @param props Property/Value pairs of properties passing to the procedure
   * @return data returned after procedure execution. null if no return data.
   */
  CompletableFuture<byte[]> execProcedureWithReturn(String signature, String instance,
      Map<String, String> props);

  /**
   * Check the current state of the specified procedure. There are three possible states:
   * <ol>
   * <li>running - returns <tt>false</tt></li>
   * <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the procedure to fail</li>
   * </ol>
   * @param signature The signature that uniquely identifies a procedure
   * @param instance The instance name of the procedure
   * @param props Property/Value pairs of properties passing to the procedure
   * @return true if the specified procedure is finished successfully, false if it is still running.
   *         The value is wrapped by {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isProcedureFinished(String signature, String instance,
      Map<String, String> props);

  /**
   * abort a procedure
   * @param procId ID of the procedure to abort
   * @param mayInterruptIfRunning if the proc completed at least one step, should it be aborted?
   * @return true if aborted, false if procedure already completed or does not exist. the value is
   *         wrapped by {@link CompletableFuture}
   */
  CompletableFuture<Boolean> abortProcedure(long procId, boolean mayInterruptIfRunning);

  /**
   * List procedures
   * @return procedure list JSON wrapped by {@link CompletableFuture}
   */
  CompletableFuture<String> getProcedures();

  /**
   * List locks.
   * @return lock list JSON wrapped by {@link CompletableFuture}
   */
  CompletableFuture<String> getLocks();

  /**
   * Mark region server(s) as decommissioned to prevent additional regions from getting
   * assigned to them. Optionally unload the regions on the servers. If there are multiple servers
   * to be decommissioned, decommissioning them at the same time can prevent wasteful region
   * movements. Region unloading is asynchronous.
   * @param servers The list of servers to decommission.
   * @param offload True to offload the regions from the decommissioned servers
   */
  CompletableFuture<Void> decommissionRegionServers(List<ServerName> servers, boolean offload);

  /**
   * List region servers marked as decommissioned, which can not be assigned regions.
   * @return List of decommissioned region servers wrapped by {@link CompletableFuture}
   */
  CompletableFuture<List<ServerName>> listDecommissionedRegionServers();

  /**
   * Remove decommission marker from a region server to allow regions assignments. Load regions onto
   * the server if a list of regions is given. Region loading is asynchronous.
   * @param server The server to recommission.
   * @param encodedRegionNames Regions to load onto the server.
   */
  CompletableFuture<Void> recommissionRegionServer(ServerName server,
      List<byte[]> encodedRegionNames);

  /**
   * @return cluster status wrapped by {@link CompletableFuture}
   */
  CompletableFuture<ClusterMetrics> getClusterMetrics();

  /**
   * @return cluster status wrapped by {@link CompletableFuture}
   */
  CompletableFuture<ClusterMetrics> getClusterMetrics(EnumSet<Option> options);

  /**
   * @return current master server name wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<ServerName> getMaster() {
    return getClusterMetrics(EnumSet.of(Option.MASTER)).thenApply(ClusterMetrics::getMasterName);
  }

  /**
   * @return current backup master list wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<Collection<ServerName>> getBackupMasters() {
    return getClusterMetrics(EnumSet.of(Option.BACKUP_MASTERS))
      .thenApply(ClusterMetrics::getBackupMasterNames);
  }

  /**
   * @return current live region servers list wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<Collection<ServerName>> getRegionServers() {
    return getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .thenApply(cm -> cm.getLiveServerMetrics().keySet());
  }

  /**
   * @return a list of master coprocessors wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<List<String>> getMasterCoprocessorNames() {
    return getClusterMetrics(EnumSet.of(Option.MASTER_COPROCESSORS))
        .thenApply(ClusterMetrics::getMasterCoprocessorNames);
  }

  /**
   * Get the info port of the current master if one is available.
   * @return master info port
   */
  default CompletableFuture<Integer> getMasterInfoPort() {
    return getClusterMetrics(EnumSet.of(Option.MASTER_INFO_PORT)).thenApply(
      ClusterMetrics::getMasterInfoPort);
  }

  /**
   * Shuts down the HBase cluster.
   */
  CompletableFuture<Void> shutdown();

  /**
   * Shuts down the current HBase master only.
   */
  CompletableFuture<Void> stopMaster();

  /**
   * Stop the designated regionserver.
   * @param serverName
   */
  CompletableFuture<Void> stopRegionServer(ServerName serverName);

  /**
   * Update the configuration and trigger an online config change on the regionserver.
   * @param serverName : The server whose config needs to be updated.
   */
  CompletableFuture<Void> updateConfiguration(ServerName serverName);

  /**
   * Update the configuration and trigger an online config change on all the masters and
   * regionservers.
   */
  CompletableFuture<Void> updateConfiguration();

  /**
   * Roll the log writer. I.e. for filesystem based write ahead logs, start writing to a new file.
   * <p>
   * When the returned CompletableFuture is done, it only means the rollWALWriter request was sent
   * to the region server and may need some time to finish the rollWALWriter operation. As a side
   * effect of this call, the named region server may schedule store flushes at the request of the
   * wal.
   * @param serverName The servername of the region server.
   */
  CompletableFuture<Void> rollWALWriter(ServerName serverName);

  /**
   * Clear compacting queues on a region server.
   * @param serverName
   * @param queues the set of queue name
   */
  CompletableFuture<Void> clearCompactionQueues(ServerName serverName, Set<String> queues);

  /**
   * Get a list of {@link RegionMetrics} of all regions hosted on a region seerver.
   * @param serverName
   * @return a list of {@link RegionMetrics} wrapped by {@link CompletableFuture}
   */
  CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName serverName);

  /**
   * Get a list of {@link RegionMetrics} of all regions hosted on a region seerver for a table.
   * @param serverName
   * @param tableName
   * @return a list of {@link RegionMetrics} wrapped by {@link CompletableFuture}
   */
  CompletableFuture<List<RegionMetrics>> getRegionMetrics(ServerName serverName,
    TableName tableName);

  /**
   * Check whether master is in maintenance mode
   * @return true if master is in maintenance mode, false otherwise. The return value will be
   *         wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isMasterInMaintenanceMode();

  /**
   * Get the current compaction state of a table. It could be in a major compaction, a minor
   * compaction, both, or none.
   * @param tableName table to examine
   * @return the current compaction state wrapped by a {@link CompletableFuture}
   */
  default CompletableFuture<CompactionState> getCompactionState(TableName tableName) {
    return getCompactionState(tableName, CompactType.NORMAL);
  }

  /**
   * Get the current compaction state of a table. It could be in a major compaction, a minor
   * compaction, both, or none.
   * @param tableName table to examine
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @return the current compaction state wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<CompactionState> getCompactionState(TableName tableName,
      CompactType compactType);

  /**
   * Get the current compaction state of region. It could be in a major compaction, a minor
   * compaction, both, or none.
   * @param regionName region to examine
   * @return the current compaction state wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<CompactionState> getCompactionStateForRegion(byte[] regionName);

  /**
   * Get the timestamp of the last major compaction for the passed table.
   * <p>
   * The timestamp of the oldest HFile resulting from a major compaction of that table, or not
   * present if no such HFile could be found.
   * @param tableName table to examine
   * @return the last major compaction timestamp wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Optional<Long>> getLastMajorCompactionTimestamp(TableName tableName);

  /**
   * Get the timestamp of the last major compaction for the passed region.
   * <p>
   * The timestamp of the oldest HFile resulting from a major compaction of that region, or not
   * present if no such HFile could be found.
   * @param regionName region to examine
   * @return the last major compaction timestamp wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Optional<Long>> getLastMajorCompactionTimestampForRegion(byte[] regionName);

  /**
   * @return the list of supported security capabilities. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<SecurityCapability>> getSecurityCapabilities();

  /**
   * Turn the load balancer on or off.
   * @param on
   * @return Previous balancer value wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> balancerSwitch(boolean on);

  /**
   * Invoke the balancer. Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments. Can NOT run for various reasons. Check logs.
   * @return True if balancer ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  default CompletableFuture<Boolean> balance() {
    return balance(false);
  }

  /**
   * Invoke the balancer. Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments. If there is region in transition, force parameter of true would still run
   * balancer. Can *not* run for other reasons. Check logs.
   * @param forcible whether we should force balance even if there is region in transition.
   * @return True if balancer ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> balance(boolean forcible);

  /**
   * Query the current state of the balancer.
   * @return true if the balance switch is on, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<Boolean> isBalancerEnabled();

  /**
   * Set region normalizer on/off.
   * @param on whether normalizer should be on or off
   * @return Previous normalizer value wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> normalizerSwitch(boolean on);

  /**
   * Query the current state of the region normalizer
   * @return true if region normalizer is on, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isNormalizerEnabled();

  /**
   * Invoke region normalizer. Can NOT run for various reasons. Check logs.
   * @return true if region normalizer ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> normalize();

  /**
   * Turn the cleaner chore on/off.
   * @param on
   * @return Previous cleaner state wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> cleanerChoreSwitch(boolean on);

  /**
   * Query the current state of the cleaner chore.
   * @return true if cleaner chore is on, false otherwise. The return value will be wrapped by
   *         a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isCleanerChoreEnabled();

  /**
   * Ask for cleaner chore to run.
   * @return true if cleaner chore ran, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> runCleanerChore();

  /**
   * Turn the catalog janitor on/off.
   * @param on
   * @return the previous state wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> catalogJanitorSwitch(boolean on);

  /**
   * Query on the catalog janitor state.
   * @return true if the catalog janitor is on, false otherwise. The return value will be
   *         wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isCatalogJanitorEnabled();

  /**
   * Ask for a scan of the catalog table.
   * @return the number of entries cleaned. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Integer> runCatalogJanitor();

  /**
   * Execute the given coprocessor call on the master.
   * <p>
   * The {@code stubMaker} is just a delegation to the {@code newStub} call. Usually it is only a
   * one line lambda expression, like:
   *
   * <pre>
   * <code>
   * channel -> xxxService.newStub(channel)
   * </code>
   * </pre>
   * @param stubMaker a delegation to the actual {@code newStub} call.
   * @param callable a delegation to the actual protobuf rpc call. See the comment of
   *          {@link ServiceCaller} for more details.
   * @param <S> the type of the asynchronous stub
   * @param <R> the type of the return value
   * @return the return value of the protobuf rpc call, wrapped by a {@link CompletableFuture}.
   * @see ServiceCaller
   */
  <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable);

  /**
   * Execute the given coprocessor call on the given region server.
   * <p>
   * The {@code stubMaker} is just a delegation to the {@code newStub} call. Usually it is only a
   * one line lambda expression, like:
   *
   * <pre>
   * <code>
   * channel -> xxxService.newStub(channel)
   * </code>
   * </pre>
   * @param stubMaker a delegation to the actual {@code newStub} call.
   * @param callable a delegation to the actual protobuf rpc call. See the comment of
   *          {@link ServiceCaller} for more details.
   * @param serverName the given region server
   * @param <S> the type of the asynchronous stub
   * @param <R> the type of the return value
   * @return the return value of the protobuf rpc call, wrapped by a {@link CompletableFuture}.
   * @see ServiceCaller
   */
  <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
    ServiceCaller<S, R> callable, ServerName serverName);

  /**
   * List all the dead region servers.
   */
  default CompletableFuture<List<ServerName>> listDeadServers() {
    return this.getClusterMetrics(EnumSet.of(Option.DEAD_SERVERS))
        .thenApply(ClusterMetrics::getDeadServerNames);
  }

  /**
   * Clear dead region servers from master.
   * @param servers list of dead region servers.
   * @return - returns a list of servers that not cleared wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<ServerName>> clearDeadServers(final List<ServerName> servers);

  /**
   * Clear all the blocks corresponding to this table from BlockCache. For expert-admins. Calling
   * this API will drop all the cached blocks specific to a table from BlockCache. This can
   * significantly impact the query performance as the subsequent queries will have to retrieve the
   * blocks from underlying filesystem.
   * @param tableName table to clear block cache
   * @return CacheEvictionStats related to the eviction wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<CacheEvictionStats> clearBlockCache(final TableName tableName);

  /**
   * Create a new table by cloning the existent table schema.
   *
   * @param tableName name of the table to be cloned
   * @param newTableName name of the new table where the table will be created
   * @param preserveSplits True if the splits should be preserved
   */
  CompletableFuture<Void>  cloneTableSchema(final TableName tableName,
      final TableName newTableName, final boolean preserveSplits);
}
