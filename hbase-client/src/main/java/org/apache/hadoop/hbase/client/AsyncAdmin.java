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
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.client.RawAsyncTable.CoprocessorCallable;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.RpcChannel;

/**
 * The asynchronous administrative API for HBase.
 * <p>
 * This feature is still under development, so marked as IA.Private. Will change to public when
 * done. Use it with caution.
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
   * @see #listTables(Optional, boolean)
   */
  default CompletableFuture<List<TableDescriptor>> listTables() {
    return listTables(Optional.empty(), false);
  }

  /**
   * List all the tables matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return - returns a list of TableDescriptors wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableDescriptor>> listTables(Optional<Pattern> pattern,
      boolean includeSysTables);

  /**
   * List all of the names of userspace tables.
   * @return a list of table names wrapped by a {@link CompletableFuture}.
   * @see #listTableNames(Optional, boolean)
   */
  default CompletableFuture<List<TableName>> listTableNames() {
    return listTableNames(Optional.empty(), false);
  }

  /**
   * List all of the names of userspace tables.
   * @param pattern The regular expression to match against
   * @param includeSysTables False to match only against userspace tables
   * @return a list of table names wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<TableName>> listTableNames(Optional<Pattern> pattern,
      boolean includeSysTables);

  /**
   * Method for getting the tableDescriptor
   * @param tableName as a {@link TableName}
   * @return the read-only tableDescriptor wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<TableDescriptor> getTableDescriptor(TableName tableName);

  /**
   * Creates a new table.
   * @param desc table descriptor for table
   */
  default CompletableFuture<Void> createTable(TableDescriptor desc) {
    return createTable(desc, Optional.empty());
  }

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
  CompletableFuture<Void> createTable(TableDescriptor desc, Optional<byte[][]> splitKeys);

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   */
  CompletableFuture<Void> deleteTable(TableName tableName);

  /**
   * Delete tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTableNames(Optional, boolean) } and
   * {@link #deleteTable(org.apache.hadoop.hbase.TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted. The return value will be wrapped
   *         by a {@link CompletableFuture}. The return HTDs are read-only.
   */
  CompletableFuture<List<TableDescriptor>> deleteTables(Pattern pattern);

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
   * Enable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Optional, boolean)} and
   * {@link #enableTable(TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be enabled. The return value will be wrapped
   *         by a {@link CompletableFuture}. The return HTDs are read-only.
   */
  CompletableFuture<List<TableDescriptor>> enableTables(Pattern pattern);

  /**
   * Disable a table. The table has to be in enabled state for it to be disabled.
   * @param tableName
   */
  CompletableFuture<Void> disableTable(TableName tableName);

  /**
   * Disable tables matching the passed in pattern. Warning: Use this method carefully, there is no
   * prompting and the effect is immediate. Consider using {@link #listTables(Optional, boolean)} and
   * {@link #disableTable(TableName)}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled. The return value will be wrapped by a
   *         {@link CompletableFuture}. The return HTDs are read-only.
   */
  CompletableFuture<List<TableDescriptor>> disableTables(Pattern pattern);

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
  default CompletableFuture<Boolean> isTableAvailable(TableName tableName) {
    return isTableAvailable(tableName, null);
  }

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
  CompletableFuture<Pair<Integer, Integer>> getAlterStatus(TableName tableName);

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
   * Close a region. For expert-admins Runs close on the regionserver. The master will not be
   * informed of the close.
   * @param regionName region name to close
   * @param serverName Deprecated. Not used anymore after deprecation.
   * @return Deprecated. Always returns true now.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-18231">HBASE-18231</a>).
   *             Use {@link #unassign(byte[], boolean)}.
   */
  @Deprecated
  CompletableFuture<Boolean> closeRegion(byte[] regionName, Optional<ServerName> serverName);

  /**
   * Get all the online regions on a region server.
   */
  CompletableFuture<List<HRegionInfo>> getOnlineRegions(ServerName serverName);

  /**
   * Get the regions of a given table.
   */
  CompletableFuture<List<HRegionInfo>> getTableRegions(TableName tableName);

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
   * Compact a table. When the returned CompletableFuture is done, it only means the compact request
   * was sent to HBase and may need some time to finish the compact operation.
   * @param tableName table to compact
   */
  default CompletableFuture<Void> compact(TableName tableName) {
    return compact(tableName, Optional.empty());
  }

  /**
   * Compact a column family within a table. When the returned CompletableFuture is done, it only
   * means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param tableName table to compact
   * @param columnFamily column family within a table. If not present, compact the table's all
   *          column families.
   */
  CompletableFuture<Void> compact(TableName tableName, Optional<byte[]> columnFamily);

  /**
   * Compact an individual region. When the returned CompletableFuture is done, it only means the
   * compact request was sent to HBase and may need some time to finish the compact operation.
   * @param regionName region to compact
   */
  default CompletableFuture<Void> compactRegion(byte[] regionName) {
    return compactRegion(regionName, Optional.empty());
  }

  /**
   * Compact a column family within a region. When the returned CompletableFuture is done, it only
   * means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param regionName region to compact
   * @param columnFamily column family within a region. If not present, compact the region's all
   *          column families.
   */
  CompletableFuture<Void> compactRegion(byte[] regionName, Optional<byte[]> columnFamily);

  /**
   * Major compact a table. When the returned CompletableFuture is done, it only means the compact
   * request was sent to HBase and may need some time to finish the compact operation.
   * @param tableName table to major compact
   */
  default CompletableFuture<Void> majorCompact(TableName tableName) {
    return majorCompact(tableName, Optional.empty());
  }

  /**
   * Major compact a column family within a table. When the returned CompletableFuture is done, it
   * only means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param tableName table to major compact
   * @param columnFamily column family within a table. If not present, major compact the table's all
   *          column families.
   */
  CompletableFuture<Void> majorCompact(TableName tableName, Optional<byte[]> columnFamily);

  /**
   * Major compact a region. When the returned CompletableFuture is done, it only means the compact
   * request was sent to HBase and may need some time to finish the compact operation.
   * @param regionName region to major compact
   */
  default CompletableFuture<Void> majorCompactRegion(byte[] regionName) {
    return majorCompactRegion(regionName, Optional.empty());
  }

  /**
   * Major compact a column family within region. When the returned CompletableFuture is done, it
   * only means the compact request was sent to HBase and may need some time to finish the compact
   * operation.
   * @param regionName region to major compact
   * @param columnFamily column family within a region. If not present, major compact the region's
   *          all column families.
   */
  CompletableFuture<Void> majorCompactRegion(byte[] regionName, Optional<byte[]> columnFamily);

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
  CompletableFuture<Boolean> setMergeOn(boolean on);

  /**
   * Query the current state of the Merge switch.
   * @return true if the switch is on, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isMergeOn();

  /**
   * Turn the Split switch on or off.
   * @param on
   * @return Previous switch value wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> setSplitOn(boolean on);

  /**
   * Query the current state of the Split switch.
   * @return true if the switch is on, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isSplitOn();

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
  default CompletableFuture<Void> splitRegion(byte[] regionName) {
    return splitRegion(regionName, Optional.empty());
  }

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
  CompletableFuture<Void> splitRegion(byte[] regionName, Optional<byte[]> splitPoint);

  /**
   * @param regionName Encoded or full name of region to assign.
   */
  CompletableFuture<Void> assign(byte[] regionName);

  /**
   * Unassign a region from current hosting regionserver. Region will then be assigned to a
   * regionserver chosen at random. Region could be reassigned back to the same server. Use
   * {@link #move(byte[], Optional)} if you want to control the region movement.
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
   * Move the region <code>r</code> to <code>dest</code>.
   * @param regionName Encoded or full name of region to move.
   * @param destServerName The servername of the destination regionserver. If not present, we'll
   *          assign to a random server. A server name is made of host, port and startcode. Here is
   *          an example: <code> host187.example.com,60020,1289493121758</code>
   */
  CompletableFuture<Void> move(byte[] regionName, Optional<ServerName> destServerName);

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
  CompletableFuture<Void> addReplicationPeer(String peerId,
      ReplicationPeerConfig peerConfig);

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
   * Append the replicable table-cf config of the specified peer
   * @param peerId a short that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   */
  CompletableFuture<Void> appendReplicationPeerTableCFs(String peerId,
      Map<TableName, ? extends Collection<String>> tableCfs);

  /**
   * Remove some table-cfs from config of the specified peer
   * @param peerId a short name that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   */
  CompletableFuture<Void> removeReplicationPeerTableCFs(String peerId,
      Map<TableName, ? extends Collection<String>> tableCfs);

  /**
   * Return a list of replication peers.
   * @return a list of replication peers description. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  default CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers() {
    return listReplicationPeers(Optional.empty());
  }

  /**
   * Return a list of replication peers.
   * @param pattern The compiled regular expression to match peer id
   * @return a list of replication peers description. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<ReplicationPeerDescription>>
      listReplicationPeers(Optional<Pattern> pattern);

  /**
   * Find all table and column families that are replicated from this cluster
   * @return the replicated table-cfs list of this cluster. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  CompletableFuture<List<TableCFs>> listReplicatedTableCFs();

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
  default CompletableFuture<List<SnapshotDescription>> listSnapshots() {
    return listSnapshots(Optional.empty());
  }

  /**
   * List all the completed snapshots matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @return - returns a List of SnapshotDescription wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<List<SnapshotDescription>> listSnapshots(Optional<Pattern> pattern);

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
   * Delete existing snapshots whose names match the pattern passed.
   * @param pattern pattern for names of the snapshot to match
   */
  default CompletableFuture<Void> deleteSnapshots(Pattern pattern) {
    return deleteTableSnapshots(null, pattern);
  }

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
  CompletableFuture<byte[]> execProcedureWithRet(String signature, String instance,
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
   * Mark a region server as draining to prevent additional regions from getting assigned to it.
   * @param servers
   */
  CompletableFuture<Void> drainRegionServers(List<ServerName> servers);

  /**
   * List region servers marked as draining to not get additional regions assigned to them.
   * @return List of draining region servers wrapped by {@link CompletableFuture}
   */
  CompletableFuture<List<ServerName>> listDrainingRegionServers();

  /**
   * Remove drain from a region server to allow additional regions assignments.
   * @param servers List of region servers to remove drain from.
   */
  CompletableFuture<Void> removeDrainFromRegionServers(List<ServerName> servers);

  /**
   * @return cluster status wrapped by {@link CompletableFuture}
   */
  CompletableFuture<ClusterStatus> getClusterStatus();

  /**
   * @return cluster status wrapped by {@link CompletableFuture}
   */
  CompletableFuture<ClusterStatus> getClusterStatus(EnumSet<Option> options);

  /**
   * @return current master server name wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<ServerName> getMaster() {
    return getClusterStatus(EnumSet.of(Option.MASTER)).thenApply(ClusterStatus::getMaster);
  }

  /**
   * @return current backup master list wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<Collection<ServerName>> getBackupMasters() {
    return getClusterStatus(EnumSet.of(Option.BACKUP_MASTERS)).thenApply(ClusterStatus::getBackupMasters);
  }

  /**
   * @return current live region servers list wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<Collection<ServerName>> getRegionServers() {
    return getClusterStatus(EnumSet.of(Option.LIVE_SERVERS)).thenApply(ClusterStatus::getServers);
  }

  /**
   * Get a list of {@link RegionLoad} of all regions hosted on a region seerver.
   * @param serverName
   * @return a list of {@link RegionLoad} wrapped by {@link CompletableFuture}
   */
  default CompletableFuture<List<RegionLoad>> getRegionLoads(ServerName serverName) {
    return getRegionLoads(serverName, Optional.empty());
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
   * Get a list of {@link RegionLoad} of all regions hosted on a region seerver for a table.
   * @param serverName
   * @param tableName
   * @return a list of {@link RegionLoad} wrapped by {@link CompletableFuture}
   */
  CompletableFuture<List<RegionLoad>> getRegionLoads(ServerName serverName,
      Optional<TableName> tableName);

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
  CompletableFuture<CompactionState> getCompactionState(TableName tableName);

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
  CompletableFuture<Boolean> setBalancerOn(boolean on);

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
  CompletableFuture<Boolean> isBalancerOn();

  /**
   * Set region normalizer on/off.
   * @param on whether normalizer should be on or off
   * @return Previous normalizer value wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> setNormalizerOn(boolean on);

  /**
   * Query the current state of the region normalizer
   * @return true if region normalizer is on, false otherwise. The return value will be wrapped by a
   *         {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isNormalizerOn();

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
  CompletableFuture<Boolean> setCleanerChoreOn(boolean on);

  /**
   * Query the current state of the cleaner chore.
   * @return true if cleaner chore is on, false otherwise. The return value will be wrapped by
   *         a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isCleanerChoreOn();

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
  CompletableFuture<Boolean> setCatalogJanitorOn(boolean on);

  /**
   * Query on the catalog janitor state.
   * @return true if the catalog janitor is on, false otherwise. The return value will be
   *         wrapped by a {@link CompletableFuture}
   */
  CompletableFuture<Boolean> isCatalogJanitorOn();

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
   *          {@link CoprocessorCallable} for more details.
   * @param <S> the type of the asynchronous stub
   * @param <R> the type of the return value
   * @return the return value of the protobuf rpc call, wrapped by a {@link CompletableFuture}.
   * @see CoprocessorCallable
   */
  <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
      CoprocessorCallable<S, R> callable);

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
   *          {@link CoprocessorCallable} for more details.
   * @param serverName the given region server
   * @param <S> the type of the asynchronous stub
   * @param <R> the type of the return value
   * @return the return value of the protobuf rpc call, wrapped by a {@link CompletableFuture}.
   * @see CoprocessorCallable
   */
  <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> stubMaker,
    CoprocessorCallable<S, R> callable, ServerName serverName);

  /**
   * List all the dead region servers.
   * @return - returns a list of dead region servers wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<ServerName>> listDeadServers();

  /**
   * Clear dead region servers from master.
   * @param servers list of dead region servers.
   * @return - returns a list of servers that not cleared wrapped by a {@link CompletableFuture}.
   */
  CompletableFuture<List<ServerName>> clearDeadServers(final List<ServerName> servers);
}
