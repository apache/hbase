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

import static org.apache.hadoop.hbase.util.FutureUtils.get;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotView;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.access.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * The administrative API for HBase. Obtain an instance from {@link Connection#getAdmin()} and
 * call {@link #close()} when done.
 * <p>Admin can be used to create, drop, list, enable and disable and otherwise modify tables,
 * as well as perform other administrative operations.
 *
 * @see ConnectionFactory
 * @see Connection
 * @see Table
 * @since 0.99.0
 */
@InterfaceAudience.Public
public interface Admin extends Abortable, Closeable {

  /**
   * Return the operation timeout for a rpc call.
   * @see #getSyncWaitTimeout()
   */
  int getOperationTimeout();

  /**
   * Return the blocking wait time for an asynchronous operation. Can be configured by
   * {@code hbase.client.sync.wait.timeout.msec}.
   * <p/>
   * For several operations, such as createTable, deleteTable, etc, the rpc call will finish right
   * after we schedule a procedure at master side, so the timeout will not be controlled by the
   * above {@link #getOperationTimeout()}. And timeout value here tells you how much time we will
   * wait until the procedure at master side is finished.
   * <p/>
   * In general, you can consider that the implementation for XXXX method is just a
   * XXXXAsync().get(getSyncWaitTimeout(), TimeUnit.MILLISECONDS).
   * @see #getOperationTimeout()
   */
  int getSyncWaitTimeout();

  @Override
  void abort(String why, Throwable e);

  @Override
  boolean isAborted();

  /**
   * @return Connection used by this object.
   */
  Connection getConnection();

  /**
   * @param tableName Table to check.
   * @return <code>true</code> if table exists already.
   * @throws IOException if a remote or network exception occurs
   */
  boolean tableExists(TableName tableName) throws IOException;

  /**
   * List all the userspace tables.
   *
   * @return a list of TableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  List<TableDescriptor> listTableDescriptors() throws IOException;

  /**
   * List all userspace tables and whether or not include system tables.
   *
   * @return a list of TableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  List<TableDescriptor> listTableDescriptors(boolean includeSysTables) throws IOException;

  /**
   * List all the userspace tables that match the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @return a list of TableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTableDescriptors()
   */
  default List<TableDescriptor> listTableDescriptors(Pattern pattern) throws IOException {
    return listTableDescriptors(pattern, false);
  }

  /**
   * List all the tables matching the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @param includeSysTables <code>false</code> to match only against userspace tables
   * @return a list of TableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTableDescriptors()
   */
  List<TableDescriptor> listTableDescriptors(Pattern pattern, boolean includeSysTables)
      throws IOException;

  /**
   * List all of the names of userspace tables.
   *
   * @return TableName[] table names
   * @throws IOException if a remote or network exception occurs
   */
  TableName[] listTableNames() throws IOException;

  /**
   * List all of the names of userspace tables.
   * @param pattern The regular expression to match against
   * @return array of table names
   * @throws IOException if a remote or network exception occurs
   */
  default TableName[] listTableNames(Pattern pattern) throws IOException {
    return listTableNames(pattern, false);
  }

  /**
   * List all of the names of userspace tables.
   * @param pattern The regular expression to match against
   * @param includeSysTables <code>false</code> to match only against userspace tables
   * @return TableName[] table names
   * @throws IOException if a remote or network exception occurs
   */
  TableName[] listTableNames(Pattern pattern, boolean includeSysTables)
      throws IOException;

  /**
   * Get a table descriptor.
   *
   * @param tableName as a {@link TableName}
   * @return the tableDescriptor
   * @throws org.apache.hadoop.hbase.TableNotFoundException
   * @throws IOException if a remote or network exception occurs
   */
  TableDescriptor getDescriptor(TableName tableName)
      throws TableNotFoundException, IOException;

  /**
   * Creates a new table. Synchronous operation.
   *
   * @param desc table descriptor for table
   * @throws IllegalArgumentException if the table name is reserved
   * @throws org.apache.hadoop.hbase.MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  default void createTable(TableDescriptor desc) throws IOException {
    get(createTableAsync(desc), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Creates a new table with the specified number of regions.  The start key specified will become
   * the end key of the first region of the table, and the end key specified will become the start
   * key of the last region of the table (the first region has a null start key and the last region
   * has a null end key). BigInteger math will be used to divide the key range specified into enough
   * segments to make the required number of total regions. Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalArgumentException if the table name is reserved
   * @throws org.apache.hadoop.hbase.MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and attempt-at-creation).
   */
  void createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException;

  /**
   * Creates a new table with an initial set of empty regions defined by the specified split keys.
   * The total number of regions created will be the number of split keys plus one. Synchronous
   * operation. Note : Avoid passing empty split key.
   *
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   * @throws IllegalArgumentException if the table name is reserved, if the split keys are repeated
   * and if the split key has empty byte array.
   * @throws org.apache.hadoop.hbase.MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  default void createTable(TableDescriptor desc, byte[][] splitKeys) throws IOException {
    get(createTableAsync(desc, splitKeys), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Creates a new table but does not block and wait for it to come online. You can use
   * Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * <p/>
   * Throws IllegalArgumentException Bad table name, if the split keys are repeated and if the split
   * key has empty byte array.
   * @param desc table descriptor for table
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async creation. You can use Future.get(long, TimeUnit) to wait on the
   *         operation to complete.
   */
  Future<Void> createTableAsync(TableDescriptor desc) throws IOException;

  /**
   * Creates a new table but does not block and wait for it to come online.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   * Throws IllegalArgumentException Bad table name, if the split keys
   *    are repeated and if the split key has empty byte array.
   *
   * @param desc table descriptor for table
   * @param splitKeys keys to check if the table has been created with all split keys
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async creation. You can use Future.get(long, TimeUnit) to wait on the
   *         operation to complete.
   */
  Future<Void> createTableAsync(TableDescriptor desc, byte[][] splitKeys) throws IOException;

  /**
   * Deletes a table. Synchronous operation.
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  default void deleteTable(TableName tableName) throws IOException {
    get(deleteTableAsync(tableName), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Deletes the table but does not block and wait for it to be completely removed.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async delete. You can use Future.get(long, TimeUnit)
   *    to wait on the operation to complete.
   */
  Future<Void> deleteTableAsync(TableName tableName) throws IOException;

  /**
   * Truncate a table. Synchronous operation.
   * @param tableName name of table to truncate
   * @param preserveSplits <code>true</code> if the splits should be preserved
   * @throws IOException if a remote or network exception occurs
   */
  default void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
    get(truncateTableAsync(tableName, preserveSplits), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Truncate the table but does not block and wait for it to be completely enabled. You can use
   * Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param tableName name of table to delete
   * @param preserveSplits <code>true</code> if the splits should be preserved
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async truncate. You can use Future.get(long, TimeUnit) to wait on the
   *         operation to complete.
   */
  Future<Void> truncateTableAsync(TableName tableName, boolean preserveSplits)
      throws IOException;

  /**
   * Enable a table. May timeout. Use {@link #enableTableAsync(org.apache.hadoop.hbase.TableName)}
   * and {@link #isTableEnabled(org.apache.hadoop.hbase.TableName)} instead. The table has to be in
   * disabled state for it to be enabled.
   * @param tableName name of the table
   * @throws IOException There could be couple types of
   *           IOException TableNotFoundException means the table doesn't exist.
   *           TableNotDisabledException means the table isn't in disabled state.
   * @see #isTableEnabled(org.apache.hadoop.hbase.TableName)
   * @see #disableTable(org.apache.hadoop.hbase.TableName)
   * @see #enableTableAsync(org.apache.hadoop.hbase.TableName)
   */
  default void enableTable(TableName tableName) throws IOException {
    get(enableTableAsync(tableName), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Enable the table but does not block and wait for it to be completely enabled.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async enable. You can use Future.get(long, TimeUnit)
   *    to wait on the operation to complete.
   */
  Future<Void> enableTableAsync(TableName tableName) throws IOException;

  /**
   * Disable the table but does not block and wait for it to be completely disabled.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async disable. You can use Future.get(long, TimeUnit)
   *    to wait on the operation to complete.
   */
  Future<Void> disableTableAsync(TableName tableName) throws IOException;

  /**
   * Disable table and wait on completion. May timeout eventually. Use
   * {@link #disableTableAsync(org.apache.hadoop.hbase.TableName)} and
   * {@link #isTableDisabled(org.apache.hadoop.hbase.TableName)} instead. The table has to be in
   * enabled state for it to be disabled.
   * @param tableName
   * @throws IOException There could be couple types of IOException TableNotFoundException means the
   *           table doesn't exist. TableNotEnabledException means the table isn't in enabled state.
   */
  default void disableTable(TableName tableName) throws IOException {
    get(disableTableAsync(tableName), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * @param tableName name of table to check
   * @return <code>true</code> if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableEnabled(TableName tableName) throws IOException;

  /**
   * @param tableName name of table to check
   * @return <code>true</code> if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableDisabled(TableName tableName) throws IOException;

  /**
   * @param tableName name of table to check
   * @return <code>true</code> if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableAvailable(TableName tableName) throws IOException;

  /**
   * Add a column family to an existing table. Synchronous operation. Use
   * {@link #addColumnFamilyAsync(TableName, ColumnFamilyDescriptor)} instead because it returns a
   * {@link Future} from which you can learn whether success or failure.
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   * @throws IOException if a remote or network exception occurs
   */
  default void addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    get(addColumnFamilyAsync(tableName, columnFamily), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Add a column family to an existing table. Asynchronous operation.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of the table to add column family to
   * @param columnFamily column family descriptor of column family to be added
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async add column family. You can use Future.get(long, TimeUnit) to
   *         wait on the operation to complete.
   */
  Future<Void> addColumnFamilyAsync(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException;

  /**
   * Delete a column family from a table. Synchronous operation. Use
   * {@link #deleteColumnFamily(TableName, byte[])} instead because it returns a {@link Future} from
   * which you can learn whether success or failure.
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  default void deleteColumnFamily(TableName tableName, byte[] columnFamily) throws IOException {
    get(deleteColumnFamilyAsync(tableName, columnFamily), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Delete a column family from a table. Asynchronous operation.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table
   * @param columnFamily name of column family to be deleted
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async delete column family. You can use Future.get(long, TimeUnit) to
   *         wait on the operation to complete.
   */
  Future<Void> deleteColumnFamilyAsync(TableName tableName, byte[] columnFamily)
      throws IOException;

  /**
   * Modify an existing column family on a table. Synchronous operation. Use
   * {@link #modifyColumnFamilyAsync(TableName, ColumnFamilyDescriptor)} instead because it returns
   * a {@link Future} from which you can learn whether success or failure.
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  default void modifyColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException {
    get(modifyColumnFamilyAsync(tableName, columnFamily), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Modify an existing column family on a table. Asynchronous operation.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   *
   * @param tableName name of table
   * @param columnFamily new column family descriptor to use
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async modify column family. You can use Future.get(long, TimeUnit) to
   *         wait on the operation to complete.
   */
  Future<Void> modifyColumnFamilyAsync(TableName tableName, ColumnFamilyDescriptor columnFamily)
      throws IOException;

  /**
   * Change the store file tracker of the given table's given family.
   * @param tableName the table you want to change
   * @param family the family you want to change
   * @param dstSFT the destination store file tracker
   * @throws IOException if a remote or network exception occurs
   */
  default void modifyColumnFamilyStoreFileTracker(TableName tableName, byte[] family, String dstSFT)
    throws IOException {
    get(modifyColumnFamilyStoreFileTrackerAsync(tableName, family, dstSFT), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Change the store file tracker of the given table's given family.
   * @param tableName the table you want to change
   * @param family the family you want to change
   * @param dstSFT the destination store file tracker
   * @return the result of the async modify. You can use Future.get(long, TimeUnit) to wait on the
   *         operation to complete
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> modifyColumnFamilyStoreFileTrackerAsync(TableName tableName, byte[] family,
    String dstSFT) throws IOException;

  /**
   * Get all the online regions on a region server.
   *
   * @return List of {@link RegionInfo}
   * @throws IOException if a remote or network exception occurs
   */
  List<RegionInfo> getRegions(ServerName serverName) throws IOException;

  /**
   * Flush a table. Synchronous operation.
   *
   * @param tableName table to flush
   * @throws IOException if a remote or network exception occurs
   */
  void flush(TableName tableName) throws IOException;

  /**
   * Flush the specified column family stores on all regions of the passed table.
   * This runs as a synchronous operation.
   *
   * @param tableName table to flush
   * @param columnFamily column family within a table
   * @throws IOException if a remote or network exception occurs
   */
  void flush(TableName tableName, byte[] columnFamily) throws IOException;

  /**
   * Flush an individual region. Synchronous operation.
   *
   * @param regionName region to flush
   * @throws IOException if a remote or network exception occurs
   */
  void flushRegion(byte[] regionName) throws IOException;

  /**
   * Flush a column family within a region. Synchronous operation.
   *
   * @param regionName region to flush
   * @param columnFamily column family within a region
   * @throws IOException if a remote or network exception occurs
   */
  void flushRegion(byte[] regionName, byte[] columnFamily) throws IOException;

  /**
   * Flush all regions on the region server. Synchronous operation.
   * @param serverName the region server name to flush
   * @throws IOException if a remote or network exception occurs
   */
  void flushRegionServer(ServerName serverName) throws IOException;

  /**
   * Compact a table. Asynchronous operation in that this method requests that a
   * Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param tableName table to compact
   * @throws IOException if a remote or network exception occurs
   */
  void compact(TableName tableName) throws IOException;

  /**
   * Compact an individual region. Asynchronous operation in that this method requests that a
   * Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param regionName region to compact
   * @throws IOException if a remote or network exception occurs
   */
  void compactRegion(byte[] regionName) throws IOException;

  /**
   * Compact a column family within a table. Asynchronous operation in that this method requests
   * that a Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param tableName table to compact
   * @param columnFamily column family within a table
   * @throws IOException if a remote or network exception occurs
   */
  void compact(TableName tableName, byte[] columnFamily)
    throws IOException;

  /**
   * Compact a column family within a region. Asynchronous operation in that this method requests
   * that a Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param regionName region to compact
   * @param columnFamily column family within a region
   * @throws IOException if a remote or network exception occurs
   */
  void compactRegion(byte[] regionName, byte[] columnFamily)
    throws IOException;

  /**
   * Compact a table.  Asynchronous operation in that this method requests that a
   * Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param tableName table to compact
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void compact(TableName tableName, CompactType compactType)
    throws IOException, InterruptedException;

  /**
   * Compact a column family within a table.  Asynchronous operation in that this method
   * requests that a Compaction run and then it returns. It does not wait on the
   * completion of Compaction (it can take a while).
   *
   * @param tableName table to compact
   * @param columnFamily column family within a table
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @throws IOException if not a mob column family or if a remote or network exception occurs
   * @throws InterruptedException
   */
  void compact(TableName tableName, byte[] columnFamily, CompactType compactType)
    throws IOException, InterruptedException;

  /**
   * Major compact a table. Asynchronous operation in that this method requests
   * that a Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param tableName table to major compact
   * @throws IOException if a remote or network exception occurs
   */
  void majorCompact(TableName tableName) throws IOException;

  /**
   * Major compact a table or an individual region. Asynchronous operation in that this method requests
   * that a Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param regionName region to major compact
   * @throws IOException if a remote or network exception occurs
   */
  void majorCompactRegion(byte[] regionName) throws IOException;

  /**
   * Major compact a column family within a table. Asynchronous operation in that this method requests
   * that a Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param tableName table to major compact
   * @param columnFamily column family within a table
   * @throws IOException if a remote or network exception occurs
   */
  void majorCompact(TableName tableName, byte[] columnFamily)
    throws IOException;

  /**
   * Major compact a column family within region. Asynchronous operation in that this method requests
   * that a Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param regionName egion to major compact
   * @param columnFamily column family within a region
   * @throws IOException if a remote or network exception occurs
   */
  void majorCompactRegion(byte[] regionName, byte[] columnFamily)
    throws IOException;

  /**
   * Major compact a table.  Asynchronous operation in that this method requests that a
   * Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param tableName table to compact
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void majorCompact(TableName tableName, CompactType compactType)
    throws IOException, InterruptedException;

  /**
   * Major compact a column family within a table.  Asynchronous operation in that this method requests that a
   * Compaction run and then it returns. It does not wait on the completion of Compaction
   * (it can take a while).
   *
   * @param tableName table to compact
   * @param columnFamily column family within a table
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @throws IOException if not a mob column family or if a remote or network exception occurs
   * @throws InterruptedException
   */
  void majorCompact(TableName tableName, byte[] columnFamily, CompactType compactType)
    throws IOException, InterruptedException;

  /**
   * Turn the compaction on or off. Disabling compactions will also interrupt any currently ongoing
   * compactions. This state is ephemeral. The setting will be lost on restart. Compaction
   * can also be enabled/disabled by modifying configuration hbase.regionserver.compaction.enabled
   * in hbase-site.xml.
   *
   * @param switchState     Set to <code>true</code> to enable, <code>false</code> to disable.
   * @param serverNamesList list of region servers.
   * @return Previous compaction states for region servers
   * @throws IOException if a remote or network exception occurs
   */
  Map<ServerName, Boolean> compactionSwitch(boolean switchState, List<String> serverNamesList)
      throws IOException;

  /**
   * Compact all regions on the region server. Asynchronous operation in that this method requests
   * that a Compaction run and then it returns. It does not wait on the completion of Compaction (it
   * can take a while).
   * @param serverName the region server name
   * @throws IOException if a remote or network exception occurs
   */
  void compactRegionServer(ServerName serverName) throws IOException;

  /**
   * Major compact all regions on the region server. Asynchronous operation in that this method
   * requests that a Compaction run and then it returns. It does not wait on the completion of
   * Compaction (it can take a while).
   * @param serverName the region server name
   * @throws IOException if a remote or network exception occurs
   */
  void majorCompactRegionServer(ServerName serverName) throws IOException;

  /**
   * Move the region <code>encodedRegionName</code> to a random server.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes up the region name
   *          suffix: e.g. if regionname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   *          then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @throws IOException if we can't find a region named <code>encodedRegionName</code>
   */
  void move(byte[] encodedRegionName) throws IOException;

  /**
   * Move the region <code>rencodedRegionName</code> to <code>destServerName</code>.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes up the region name
   *          suffix: e.g. if regionname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   *          then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName The servername of the destination regionserver. If passed the empty byte
   *          array we'll assign to a random server. A server name is made of host, port and
   *          startcode. Here is an example: <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException if we can't find a region named <code>encodedRegionName</code>
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use {@link #move(byte[], ServerName)}
   *   instead. And if you want to move the region to a random server, please use
   *   {@link #move(byte[])}.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-22108">HBASE-22108</a>
   */
  @Deprecated
  default void move(byte[] encodedRegionName, byte[] destServerName) throws IOException {
    if (destServerName == null || destServerName.length == 0) {
      move(encodedRegionName);
    } else {
      move(encodedRegionName, ServerName.valueOf(Bytes.toString(destServerName)));
    }
  }

  /**
   * Move the region <code>encodedRegionName</code> to <code>destServerName</code>.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes up the region name
   *          suffix: e.g. if regionname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   *          then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName The servername of the destination regionserver. A server name is made of
   *          host, port and startcode. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException if we can't find a region named <code>encodedRegionName</code>
   */
  void move(byte[] encodedRegionName, ServerName destServerName) throws IOException;

  /**
   * Assign a Region.
   * @param regionName Region name to assign.
   * @throws IOException if a remote or network exception occurs
   */
  void assign(byte[] regionName) throws IOException;

  /**
   * Unassign a Region.
   * @param regionName Region name to assign.
   * @throws IOException if a remote or network exception occurs
   */
  void unassign(byte[] regionName) throws IOException;

  /**
   * Unassign a region from current hosting regionserver.  Region will then be assigned to a
   * regionserver chosen at random.  Region could be reassigned back to the same server.  Use {@link
   * #move(byte[], ServerName)} if you want to control the region movement.
   *
   * @param regionName Region to unassign. Will clear any existing RegionPlan if one found.
   * @param force If <code>true</code>, force unassign (Will remove region from regions-in-transition too if
   * present. If results in double assignment use hbck -fix to resolve. To be used by experts).
   * @throws IOException if a remote or network exception occurs
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use {@link #unassign(byte[])}
   *   instead.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-24875">HBASE-24875</a>
   */
  @Deprecated
  default void unassign(byte[] regionName, boolean force) throws IOException {
    unassign(regionName);
  }

  /**
   * Offline specified region from master's in-memory state. It will not attempt to reassign the
   * region as in unassign. This API can be used when a region not served by any region server and
   * still online as per Master's in memory state. If this API is incorrectly used on active region
   * then master will loose track of that region. This is a special method that should be used by
   * experts or hbck.
   *
   * @param regionName Region to offline.
   * @throws IOException if a remote or network exception occurs
   */
  void offline(byte[] regionName) throws IOException;

  /**
   * Turn the load balancer on or off.
   * @param onOrOff Set to <code>true</code> to enable, <code>false</code> to disable.
   * @param synchronous If <code>true</code>, it waits until current balance() call, if outstanding,
   *          to return.
   * @return Previous balancer value
   * @throws IOException if a remote or network exception occurs
   */
  boolean balancerSwitch(boolean onOrOff, boolean synchronous) throws IOException;

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments.  Can NOT run for various reasons.  Check logs.
   *
   * @return <code>true</code> if balancer ran, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  default boolean balance() throws IOException {
    return balance(BalanceRequest.defaultInstance())
      .isBalancerRan();
  }

  /**
   * Invoke the balancer with the given balance request.  The BalanceRequest defines how the
   * balancer will run. See {@link BalanceRequest} for more details.
   *
   * @param request defines how the balancer should run
   * @return {@link BalanceResponse} with details about the results of the invocation.
   * @throws IOException if a remote or network exception occurs
   */
  BalanceResponse balance(BalanceRequest request) throws IOException;

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will
   * go ahead and do the reassignments. If there is region in transition, force parameter of true
   * would still run balancer. Can *not* run for other reasons.  Check
   * logs.
   * @param force whether we should force balance even if there is region in transition
   * @return <code>true</code> if balancer ran, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   * @deprecated Since 2.5.0. Will be removed in 4.0.0.
   * Use {@link #balance(BalanceRequest)} instead.
   */
  @Deprecated
  default boolean balance(boolean force) throws IOException {
    return balance(
      BalanceRequest.newBuilder()
      .setIgnoreRegionsInTransition(force)
      .build()
    ).isBalancerRan();
  }

  /**
   * Query the current state of the balancer.
   *
   * @return <code>true</code> if the balancer is enabled, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  boolean isBalancerEnabled() throws IOException;

  /**
   * Clear all the blocks corresponding to this table from BlockCache. For expert-admins.
   * Calling this API will drop all the cached blocks specific to a table from BlockCache.
   * This can significantly impact the query performance as the subsequent queries will
   * have to retrieve the blocks from underlying filesystem.
   *
   * @param tableName table to clear block cache
   * @return CacheEvictionStats related to the eviction
   * @throws IOException if a remote or network exception occurs
   */
  CacheEvictionStats clearBlockCache(final TableName tableName) throws IOException;

  /**
   * Invoke region normalizer. Can NOT run for various reasons.  Check logs.
   * This is a non-blocking invocation to region normalizer. If return value is true, it means
   * the request was submitted successfully. We need to check logs for the details of which regions
   * were split/merged.
   *
   * @return {@code true} if region normalizer ran, {@code false} otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  default boolean normalize() throws IOException {
    return normalize(new NormalizeTableFilterParams.Builder().build());
  }

  /**
   * Invoke region normalizer. Can NOT run for various reasons.  Check logs.
   * This is a non-blocking invocation to region normalizer. If return value is true, it means
   * the request was submitted successfully. We need to check logs for the details of which regions
   * were split/merged.
   *
   * @param ntfp limit to tables matching the specified filter.
   * @return {@code true} if region normalizer ran, {@code false} otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  boolean normalize(NormalizeTableFilterParams ntfp) throws IOException;

  /**
   * Query the current state of the region normalizer.
   *
   * @return <code>true</code> if region normalizer is enabled, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  boolean isNormalizerEnabled() throws IOException;

  /**
   * Turn region normalizer on or off.
   *
   * @return Previous normalizer value
   * @throws IOException if a remote or network exception occurs
   */
  boolean normalizerSwitch(boolean on) throws IOException;

  /**
   * Enable/Disable the catalog janitor/
   *
   * @param onOrOff if <code>true</code> enables the catalog janitor
   * @return the previous state
   * @throws IOException if a remote or network exception occurs
   */
  boolean catalogJanitorSwitch(boolean onOrOff) throws IOException;

  /**
   * Ask for a scan of the catalog table.
   *
   * @return the number of entries cleaned. Returns -1 if previous run is in progress.
   * @throws IOException if a remote or network exception occurs
   */
  int runCatalogJanitor() throws IOException;

  /**
   * Query on the catalog janitor state (Enabled/Disabled?).
   *
   * @throws IOException if a remote or network exception occurs
   */
  boolean isCatalogJanitorEnabled() throws IOException;

  /**
   * Enable/Disable the cleaner chore.
   *
   * @param onOrOff if <code>true</code> enables the cleaner chore
   * @return the previous state
   * @throws IOException if a remote or network exception occurs
   */
  boolean cleanerChoreSwitch(boolean onOrOff) throws IOException;

  /**
   * Ask for cleaner chore to run.
   *
   * @return <code>true</code> if cleaner chore ran, <code>false</code> otherwise
   * @throws IOException if a remote or network exception occurs
   */
  boolean runCleanerChore() throws IOException;

  /**
   * Query on the cleaner chore state (Enabled/Disabled?).
   *
   * @throws IOException if a remote or network exception occurs
   */
  boolean isCleanerChoreEnabled() throws IOException;


  /**
   * Merge two regions. Asynchronous operation.
   * @param nameOfRegionA encoded or full name of region a
   * @param nameOfRegionB encoded or full name of region b
   * @param forcible <code>true</code> if do a compulsory merge, otherwise we will only merge two
   *          adjacent regions
   * @throws IOException if a remote or network exception occurs
   * @deprecated since 2.3.0 and will be removed in 4.0.0. Multi-region merge feature is now
   *             supported. Use {@link #mergeRegionsAsync(byte[][], boolean)} instead.
   */
  @Deprecated
  default Future<Void> mergeRegionsAsync(byte[] nameOfRegionA, byte[] nameOfRegionB,
      boolean forcible) throws IOException {
    byte[][] nameofRegionsToMerge = new byte[2][];
    nameofRegionsToMerge[0] = nameOfRegionA;
    nameofRegionsToMerge[1] = nameOfRegionB;
    return mergeRegionsAsync(nameofRegionsToMerge, forcible);
  }

  /**
   * Merge multiple regions (>=2). Asynchronous operation.
   * @param nameofRegionsToMerge encoded or full name of daughter regions
   * @param forcible <code>true</code> if do a compulsory merge, otherwise we will only merge
   *          adjacent regions
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> mergeRegionsAsync(byte[][] nameofRegionsToMerge, boolean forcible)
      throws IOException;

  /**
   * Split a table. The method will execute split action for each region in table.
   * @param tableName table to split
   * @throws IOException if a remote or network exception occurs
   */
  void split(TableName tableName) throws IOException;

  /**
   * Split a table.
   * @param tableName table to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   */
  void split(TableName tableName, byte[] splitPoint) throws IOException;

  /**
   * Split an individual region. Asynchronous operation.
   * @param regionName region to split
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> splitRegionAsync(byte[] regionName) throws IOException;

  /**
   * Split an individual region. Asynchronous operation.
   * @param regionName region to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> splitRegionAsync(byte[] regionName, byte[] splitPoint) throws IOException;

  /**
   * Modify an existing table, more IRB friendly version.
   * @param td modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  default void modifyTable(TableDescriptor td) throws IOException {
    get(modifyTableAsync(td), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Modify an existing table, more IRB (ruby) friendly version. Asynchronous operation. This means
   * that it may be a while before your schema change is updated across all of the table. You can
   * use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param td description of the table
   * @throws IOException if a remote or network exception occurs
   * @return the result of the async modify. You can use Future.get(long, TimeUnit) to wait on the
   *         operation to complete
   */
  Future<Void> modifyTableAsync(TableDescriptor td) throws IOException;

  /**
   * Change the store file tracker of the given table.
   * @param tableName the table you want to change
   * @param dstSFT the destination store file tracker
   * @throws IOException if a remote or network exception occurs
   */
  default void modifyTableStoreFileTracker(TableName tableName, String dstSFT) throws IOException {
    get(modifyTableStoreFileTrackerAsync(tableName, dstSFT), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Change the store file tracker of the given table.
   * @param tableName the table you want to change
   * @param dstSFT the destination store file tracker
   * @return the result of the async modify. You can use Future.get(long, TimeUnit) to wait on the
   *         operation to complete
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> modifyTableStoreFileTrackerAsync(TableName tableName, String dstSFT)
    throws IOException;

  /**
   * Shuts down the HBase cluster.
   * <p/>
   * Notice that, a success shutdown call may ends with an error since the remote server has already
   * been shutdown.
   * @throws IOException if a remote or network exception occurs
   */
  void shutdown() throws IOException;

  /**
   * Shuts down the current HBase master only. Does not shutdown the cluster.
   * <p/>
   * Notice that, a success stopMaster call may ends with an error since the remote server has
   * already been shutdown.
   * @throws IOException if a remote or network exception occurs
   * @see #shutdown()
   */
  void stopMaster() throws IOException;

  /**
   * Check whether Master is in maintenance mode.
   *
   * @throws IOException if a remote or network exception occurs
   */
  boolean isMasterInMaintenanceMode()  throws IOException;

  /**
   * Stop the designated regionserver.
   *
   * @param hostnamePort Hostname and port delimited by a <code>:</code> as in
   * <code>example.org:1234</code>
   * @throws IOException if a remote or network exception occurs
   */
  void stopRegionServer(String hostnamePort) throws IOException;

  /**
   * Get whole cluster metrics, containing status about:
   * <pre>
   * hbase version
   * cluster id
   * primary/backup master(s)
   * master's coprocessors
   * live/dead regionservers
   * balancer
   * regions in transition
   * </pre>
   * @return cluster metrics
   * @throws IOException if a remote or network exception occurs
   */
  default ClusterMetrics getClusterMetrics() throws IOException {
    return getClusterMetrics(EnumSet.allOf(ClusterMetrics.Option.class));
  }

  /**
   * Get cluster status with a set of {@link Option} to get desired status.
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  ClusterMetrics getClusterMetrics(EnumSet<Option> options) throws IOException;

  /**
   * @return current master server name
   * @throws IOException if a remote or network exception occurs
   */
  default ServerName getMaster() throws IOException {
    return getClusterMetrics(EnumSet.of(Option.MASTER)).getMasterName();
  }

  /**
   * @return current backup master list
   * @throws IOException if a remote or network exception occurs
   */
  default Collection<ServerName> getBackupMasters() throws IOException {
    return getClusterMetrics(EnumSet.of(Option.BACKUP_MASTERS)).getBackupMasterNames();
  }

  /**
   * @return current live region servers list
   * @throws IOException if a remote or network exception occurs
   */
  default Collection<ServerName> getRegionServers() throws IOException {
    return getRegionServers(false);
  }

  /**
   * Retrieve all current live region servers including decommissioned
   * if excludeDecommissionedRS is false, else non-decommissioned ones only
   *
   * @param excludeDecommissionedRS should we exclude decommissioned RS nodes
   * @return all current live region servers including/excluding decommissioned hosts
   * @throws IOException if a remote or network exception occurs
   */
  default Collection<ServerName> getRegionServers(boolean excludeDecommissionedRS)
      throws IOException {
    List<ServerName> allServers =
      getClusterMetrics(EnumSet.of(Option.SERVERS_NAME)).getServersName();
    if (!excludeDecommissionedRS) {
      return allServers;
    }
    List<ServerName> decommissionedRegionServers = listDecommissionedRegionServers();
    return allServers.stream()
      .filter(s -> !decommissionedRegionServers.contains(s))
      .collect(ImmutableList.toImmutableList());
  }

  /**
   * Get {@link RegionMetrics} of all regions hosted on a regionserver.
   *
   * @param serverName region server from which {@link RegionMetrics} is required.
   * @return a {@link RegionMetrics} list of all regions hosted on a region server
   * @throws IOException if a remote or network exception occurs
   */
  List<RegionMetrics> getRegionMetrics(ServerName serverName) throws IOException;

  /**
   * Get {@link RegionMetrics} of all regions hosted on a regionserver for a table.
   *
   * @param serverName region server from which {@link RegionMetrics} is required.
   * @param tableName get {@link RegionMetrics} of regions belonging to the table
   * @return region metrics map of all regions of a table hosted on a region server
   * @throws IOException if a remote or network exception occurs
   */
  List<RegionMetrics> getRegionMetrics(ServerName serverName,
    TableName tableName) throws IOException;

  /**
   * @return Configuration used by the instance.
   */
  Configuration getConfiguration();

  /**
   * Create a new namespace. Blocks until namespace has been successfully created or an exception is
   * thrown.
   * @param descriptor descriptor which describes the new namespace.
   * @throws IOException if a remote or network exception occurs
   */
  default void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    get(createNamespaceAsync(descriptor), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Create a new namespace.
   * @param descriptor descriptor which describes the new namespace
   * @return the result of the async create namespace operation. Use Future.get(long, TimeUnit) to
   *         wait on the operation to complete.
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> createNamespaceAsync(NamespaceDescriptor descriptor) throws IOException;

  /**
   * Modify an existing namespace. Blocks until namespace has been successfully modified or an
   * exception is thrown.
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException if a remote or network exception occurs
   */
  default void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
    get(modifyNamespaceAsync(descriptor), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Modify an existing namespace.
   * @param descriptor descriptor which describes the new namespace
   * @return the result of the async modify namespace operation. Use Future.get(long, TimeUnit) to
   *         wait on the operation to complete.
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> modifyNamespaceAsync(NamespaceDescriptor descriptor) throws IOException;

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed. Blocks until
   * namespace has been successfully deleted or an exception is thrown.
   * @param name namespace name
   * @throws IOException if a remote or network exception occurs
   */
  default void deleteNamespace(String name) throws IOException {
    get(deleteNamespaceAsync(name), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
   * @param name namespace name
   * @return the result of the async delete namespace operation. Use Future.get(long, TimeUnit) to
   *         wait on the operation to complete.
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> deleteNamespaceAsync(String name) throws IOException;

  /**
   * Get a namespace descriptor by name.
   * @param name name of namespace descriptor
   * @return A descriptor
   * @throws org.apache.hadoop.hbase.NamespaceNotFoundException
   * @throws IOException if a remote or network exception occurs
   */
  NamespaceDescriptor getNamespaceDescriptor(String name)
      throws NamespaceNotFoundException, IOException;

  /**
   * List available namespaces
   *
   * @return List of namespace names
   * @throws IOException if a remote or network exception occurs
   */
  String[] listNamespaces() throws IOException;

  /**
   * List available namespace descriptors
   *
   * @return List of descriptors
   * @throws IOException if a remote or network exception occurs
   */
  NamespaceDescriptor[] listNamespaceDescriptors() throws IOException;

  /**
   * Get list of table descriptors by namespace.
   * @param name namespace name
   * @return returns a list of TableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  List<TableDescriptor> listTableDescriptorsByNamespace(byte[] name) throws IOException;

  /**
   * Get list of table names by namespace.
   * @param name namespace name
   * @return The list of table names in the namespace
   * @throws IOException if a remote or network exception occurs
   */
  TableName[] listTableNamesByNamespace(String name) throws IOException;

  /**
   * Get the regions of a given table.
   *
   * @param tableName the name of the table
   * @return List of {@link RegionInfo}.
   * @throws IOException if a remote or network exception occurs
   */
  List<RegionInfo> getRegions(TableName tableName) throws IOException;

  @Override
  void close();

  /**
   * Get tableDescriptors.
   *
   * @param tableNames List of table names
   * @return returns a list of TableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  List<TableDescriptor> listTableDescriptors(List<TableName> tableNames)
    throws IOException;

  /**
   * Abort a procedure.
   * <p/>
   * Do not use. Usually it is ignored but if not, it can do more damage than good. See hbck2.
   * @param procId ID of the procedure to abort
   * @param mayInterruptIfRunning if the proc completed at least one step, should it be aborted?
   * @return <code>true</code> if aborted, <code>false</code> if procedure already completed or does
   *         not exist
   * @throws IOException if a remote or network exception occurs
   * @deprecated since 2.1.1 and will be removed in 4.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21223">HBASE-21223</a>
   */
  @Deprecated
  default boolean abortProcedure(long procId, boolean mayInterruptIfRunning) throws IOException {
    return get(abortProcedureAsync(procId, mayInterruptIfRunning), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Abort a procedure but does not block and wait for completion.
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation
   * or TimeoutException in case the wait timeout was not long enough to allow the
   * operation to complete.
   * Do not use. Usually it is ignored but if not, it can do more damage than good. See hbck2.
   *
   * @param procId ID of the procedure to abort
   * @param mayInterruptIfRunning if the proc completed at least one step, should it be aborted?
   * @return <code>true</code> if aborted, <code>false</code> if procedure already completed or does not exist
   * @throws IOException if a remote or network exception occurs
   * @deprecated since 2.1.1 and will be removed in 4.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21223">HBASE-21223</a>
   */
  @Deprecated
  Future<Boolean> abortProcedureAsync(long procId, boolean mayInterruptIfRunning)
      throws IOException;

  /**
   * Get procedures.
   * @return procedure list in JSON
   * @throws IOException if a remote or network exception occurs
   */
  String getProcedures() throws IOException;

  /**
   * Get locks.
   * @return lock list in JSON
   * @throws IOException if a remote or network exception occurs
   */
  String getLocks() throws IOException;

  /**
   * Roll the log writer. I.e. for filesystem based write ahead logs, start writing to a new file.
   *
   * Note that the actual rolling of the log writer is asynchronous and may not be complete when
   * this method returns. As a side effect of this call, the named region server may schedule
   * store flushes at the request of the wal.
   *
   * @param serverName The servername of the regionserver.
   * @throws IOException if a remote or network exception occurs
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   */
  void rollWALWriter(ServerName serverName) throws IOException, FailedLogCloseException;

  /**
   * Helper that delegates to getClusterMetrics().getMasterCoprocessorNames().
   * @return an array of master coprocessors
   * @see org.apache.hadoop.hbase.ClusterMetrics#getMasterCoprocessorNames()
   */
  default List<String> getMasterCoprocessorNames() throws IOException {
    return getClusterMetrics(EnumSet.of(Option.MASTER_COPROCESSORS))
      .getMasterCoprocessorNames();
  }

  /**
   * Get the current compaction state of a table. It could be in a major compaction, a minor
   * compaction, both, or none.
   *
   * @param tableName table to examine
   * @return the current compaction state
   * @throws IOException if a remote or network exception occurs
   */
  CompactionState getCompactionState(TableName tableName) throws IOException;

  /**
   * Get the current compaction state of a table. It could be in a compaction, or none.
   *
   * @param tableName table to examine
   * @param compactType {@link org.apache.hadoop.hbase.client.CompactType}
   * @return the current compaction state
   * @throws IOException if a remote or network exception occurs
   */
  CompactionState getCompactionState(TableName tableName,
    CompactType compactType) throws IOException;

  /**
   * Get the current compaction state of region. It could be in a major compaction, a minor
   * compaction, both, or none.
   *
   * @param regionName region to examine
   * @return the current compaction state
   * @throws IOException if a remote or network exception occurs
   */
  CompactionState getCompactionStateForRegion(byte[] regionName) throws IOException;

  /**
   * Get the timestamp of the last major compaction for the passed table
   *
   * The timestamp of the oldest HFile resulting from a major compaction of that table,
   * or 0 if no such HFile could be found.
   *
   * @param tableName table to examine
   * @return the last major compaction timestamp or 0
   * @throws IOException if a remote or network exception occurs
   */
  long getLastMajorCompactionTimestamp(TableName tableName) throws IOException;

  /**
   * Get the timestamp of the last major compaction for the passed region.
   *
   * The timestamp of the oldest HFile resulting from a major compaction of that region,
   * or 0 if no such HFile could be found.
   *
   * @param regionName region to examine
   * @return the last major compaction timestamp or 0
   * @throws IOException if a remote or network exception occurs
   */
  long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException;

  /**
   * Take a snapshot for the given table. If the table is enabled, a FLUSH-type snapshot will be
   * taken. If the table is disabled, an offline snapshot is taken. Snapshots are taken
   * sequentially even when requested concurrently, across all tables. Snapshots are considered
   * unique based on <b>the name of the snapshot</b>. Attempts to take a snapshot with the same
   * name (even a different type or with different parameters) will fail with a
   * {@link org.apache.hadoop.hbase.snapshot.SnapshotCreationException} indicating the duplicate
   * naming. Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws org.apache.hadoop.hbase.snapshot.SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  default void snapshot(String snapshotName, TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, tableName, SnapshotType.FLUSH);
  }

  /**
   * Create typed snapshot of the table. Snapshots are considered unique based on <b>the name of the
   * snapshot</b>. Snapshots are taken sequentially even when requested concurrently, across
   * all tables. Attempts to take a snapshot with the same name (even a different type or with
   * different parameters) will fail with a {@link SnapshotCreationException} indicating the
   * duplicate naming. Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * @param snapshotName name to give the snapshot on the filesystem. Must be unique from all other
   *          snapshots stored on the cluster
   * @param tableName name of the table to snapshot
   * @param type type of snapshot to take
   * @throws IOException we fail to reach the master
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  default void snapshot(String snapshotName, TableName tableName, SnapshotType type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(new SnapshotDescription(snapshotName, tableName, type));
  }

  /**
   * Create typed snapshot of the table. Snapshots are considered unique based on <b>the name of the
   * snapshot</b>. Snapshots are taken sequentially even when requested concurrently, across
   * all tables. Attempts to take a snapshot with the same name (even a different type or with
   * different parameters) will fail with a {@link SnapshotCreationException} indicating the
   * duplicate naming. Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * Snapshot can live with ttl seconds.
   *
   * @param snapshotName  name to give the snapshot on the filesystem. Must be unique from all other
   *                      snapshots stored on the cluster
   * @param tableName     name of the table to snapshot
   * @param type          type of snapshot to take
   * @param snapshotProps snapshot additional properties e.g. TTL
   * @throws IOException               we fail to reach the master
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException  if the snapshot request is formatted incorrectly
   */
  default void snapshot(String snapshotName, TableName tableName, SnapshotType type,
                        Map<String, Object> snapshotProps) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(new SnapshotDescription(snapshotName, tableName, type, snapshotProps));
  }

  /**
   * Create typed snapshot of the table. Snapshots are considered unique based on <b>the name of the
   * snapshot</b>. Snapshots are taken sequentially even when requested concurrently, across
   * all tables. Attempts to take a snapshot with the same name (even a different type or with
   * different parameters) will fail with a {@link SnapshotCreationException} indicating the
   * duplicate naming. Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * Snapshot can live with ttl seconds.
   *
   * @param snapshotName  name to give the snapshot on the filesystem. Must be unique from all other
   *                      snapshots stored on the cluster
   * @param tableName     name of the table to snapshot
   * @param snapshotProps snapshot additional properties e.g. TTL
   * @throws IOException               we fail to reach the master
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException  if the snapshot request is formatted incorrectly
   */
  default void snapshot(String snapshotName, TableName tableName,
                        Map<String, Object> snapshotProps) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    snapshot(new SnapshotDescription(snapshotName, tableName, SnapshotType.FLUSH, snapshotProps));
  }

  /**
   * Take a snapshot and wait for the server to complete that snapshot (blocking). Snapshots are
   * considered unique based on <b>the name of the snapshot</b>. Snapshots are taken sequentially
   * even when requested concurrently, across all tables. Attempts to take a snapshot with the same
   * name (even a different type or with different parameters) will fail with a
   * {@link SnapshotCreationException} indicating the duplicate naming. Snapshot names follow the
   * same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}. You should
   * probably use {@link #snapshot(String, org.apache.hadoop.hbase.TableName)} unless you are sure
   * about the type of snapshot that you want to take.
   * @param snapshot snapshot to take
   * @throws IOException or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot failed to be taken
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  void snapshot(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException;

  /**
   * Take a snapshot without waiting for the server to complete that snapshot (asynchronous).
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Snapshots are taken
   * sequentially even when requested concurrently, across all tables.
   *
   * @param snapshot snapshot to take
   * @throws IOException if the snapshot did not succeed or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  Future<Void> snapshotAsync(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException;

  /**
   * Check the current state of the passed snapshot. There are three possible states: <ol>
   * <li>running - returns <tt>false</tt></li> <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the snapshot to fail</li> </ol> The
   * cluster only knows about the most recent snapshot. Therefore, if another snapshot has been
   * run/started since the snapshot you are checking, you will receive an {@link
   * org.apache.hadoop.hbase.snapshot.UnknownSnapshotException}.
   *
   * @param snapshot description of the snapshot to check
   * @return <tt>true</tt> if the snapshot is completed, <tt>false</tt> if the snapshot is still
   * running
   * @throws IOException if we have a network issue
   * @throws org.apache.hadoop.hbase.snapshot.HBaseSnapshotException if the snapshot failed
   * @throws org.apache.hadoop.hbase.snapshot.UnknownSnapshotException if the requested snapshot is
   * unknown
   */
  boolean isSnapshotFinished(SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException;

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If the
   * "hbase.snapshot.restore.take.failsafe.snapshot" configuration property is set to
   * <code>true</code>, a snapshot of the current table is taken before executing the restore
   * operation. In case of restore failure, the failsafe snapshot will be restored. If the restore
   * completes without problem the failsafe snapshot is deleted.
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException;

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to <code>true</code>, a snapshot of the current table is taken
   * before executing the restore operation. In case of restore failure, the failsafe snapshot will
   * be restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot <code>true</code> if the failsafe snapshot should be taken
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  default void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(snapshotName, takeFailSafeSnapshot, false);
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to <code>true</code>, a snapshot of the current table is taken
   * before executing the restore operation. In case of restore failure, the failsafe snapshot will
   * be restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot <code>true</code> if the failsafe snapshot should be taken
   * @param restoreAcl <code>true</code> to restore acl of snapshot
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  void restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot, boolean restoreAcl)
    throws IOException, RestoreSnapshotException;

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  default void cloneSnapshot(String snapshotName, TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException {
    cloneSnapshot(snapshotName, tableName, false, null);
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @param restoreAcl <code>true</code> to clone acl into newly created table
   * @param customSFT specify the StoreFileTracker used for the table
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  default void cloneSnapshot(String snapshotName, TableName tableName, boolean restoreAcl,
    String customSFT)
    throws IOException, TableExistsException, RestoreSnapshotException {
    get(cloneSnapshotAsync(snapshotName, tableName, restoreAcl, customSFT), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @param restoreAcl <code>true</code> to clone acl into newly created table
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  default void cloneSnapshot(String snapshotName, TableName tableName, boolean restoreAcl)
      throws IOException, TableExistsException, RestoreSnapshotException {
    get(cloneSnapshotAsync(snapshotName, tableName, restoreAcl), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Create a new table by cloning the snapshot content, but does not block and wait for it to be
   * completely cloned. You can use Future.get(long, TimeUnit) to wait on the operation to complete.
   * It may throw ExecutionException if there was an error while executing the operation or
   * TimeoutException in case the wait timeout was not long enough to allow the operation to
   * complete.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be cloned already exists
   * @return the result of the async clone snapshot. You can use Future.get(long, TimeUnit) to wait
   *         on the operation to complete.
   */
  default Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName)
      throws IOException, TableExistsException {
    return cloneSnapshotAsync(snapshotName, tableName, false);
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @param restoreAcl <code>true</code> to clone acl into newly created table
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  default Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName,
    boolean restoreAcl)
      throws IOException, TableExistsException, RestoreSnapshotException {
    return cloneSnapshotAsync(snapshotName, tableName, restoreAcl, null);
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @param restoreAcl <code>true</code> to clone acl into newly created table
   * @param customSFT specify the StroreFileTracker used for the table
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  Future<Void> cloneSnapshotAsync(String snapshotName, TableName tableName, boolean restoreAcl,
    String customSFT) throws IOException, TableExistsException, RestoreSnapshotException;

  /**
   * Execute a distributed procedure on a cluster.
   *
   * @param signature A distributed procedure is uniquely identified by its signature (default the
   * root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   * optional.
   * @param props Property/Value pairs of properties passing to the procedure
   * @throws IOException if a remote or network exception occurs
   */
  void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException;

  /**
   * Execute a distributed procedure on a cluster.
   *
   * @param signature A distributed procedure is uniquely identified by its signature (default the
   * root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   * optional.
   * @param props Property/Value pairs of properties passing to the procedure
   * @return data returned after procedure execution. null if no return data.
   * @throws IOException if a remote or network exception occurs
   */
  byte[] execProcedureWithReturn(String signature, String instance, Map<String, String> props)
      throws IOException;

  /**
   * Check the current state of the specified procedure. There are three possible states: <ol>
   * <li>running - returns <tt>false</tt></li> <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the procedure to fail</li> </ol>
   *
   * @param signature The signature that uniquely identifies a procedure
   * @param instance The instance name of the procedure
   * @param props Property/Value pairs of properties passing to the procedure
   * @return <code>true</code> if the specified procedure is finished successfully, <code>false</code> if it is still running
   * @throws IOException if the specified procedure finished with error
   */
  boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException;

  /**
   * List completed snapshots.
   *
   * @return a list of snapshot descriptors for completed snapshots
   * @throws IOException if a network error occurs
   */
  List<SnapshotDescription> listSnapshots() throws IOException;

  /**
   * List all the completed snapshots matching the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @return list of SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException;

  /**
   * List all the completed snapshots matching the given table name regular expression and snapshot
   * name regular expression.
   * @param tableNamePattern The compiled table name regular expression to match against
   * @param snapshotNamePattern The compiled snapshot name regular expression to match against
   * @return list of completed SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  List<SnapshotDescription> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) throws IOException;

  /**
   * Delete an existing snapshot.
   *
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  void deleteSnapshot(String snapshotName) throws IOException;

  /**
   * Delete existing snapshots whose names match the pattern passed.
   *
   * @param pattern pattern for names of the snapshot to match
   * @throws IOException if a remote or network exception occurs
   */
  void deleteSnapshots(Pattern pattern) throws IOException;

  /**
   * Delete all existing snapshots matching the given table name regular expression and snapshot
   * name regular expression.
   * @param tableNamePattern The compiled table name regular expression to match against
   * @param snapshotNamePattern The compiled snapshot name regular expression to match against
   * @throws IOException if a remote or network exception occurs
   */
  void deleteTableSnapshots(Pattern tableNamePattern, Pattern snapshotNamePattern)
      throws IOException;

  /**
   * Apply the new quota settings.
   *
   * @param quota the quota settings
   * @throws IOException if a remote or network exception occurs
   */
  void setQuota(QuotaSettings quota) throws IOException;

  /**
   * List the quotas based on the filter.
   * @param filter the quota settings filter
   * @return the QuotaSetting list
   * @throws IOException if a remote or network exception occurs
   */
  List<QuotaSettings> getQuota(QuotaFilter filter) throws IOException;

  /**
   * Creates and returns a {@link org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel}
   * instance connected to the active master.
   * <p/>
   * The obtained {@link org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel} instance can be
   * used to access a published coprocessor
   * {@link org.apache.hbase.thirdparty.com.google.protobuf.Service} using standard protobuf service
   * invocations:
   * <p/>
   * <div style="background-color: #cccccc; padding: 2px">
   * <blockquote>
   * <pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService();
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre>
   * </blockquote>
   * </div>
   * @return A MasterCoprocessorRpcChannel instance
   * @deprecated since 3.0.0, will removed in 4.0.0. This is too low level, please stop using it any
   *             more. Use the coprocessorService methods in {@link AsyncAdmin} instead.
   */
  @Deprecated
  CoprocessorRpcChannel coprocessorService();


  /**
   * Creates and returns a {@link org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel}
   * instance connected to the passed region server.
   * <p/>
   * The obtained {@link org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel} instance can be
   * used to access a published coprocessor
   * {@link org.apache.hbase.thirdparty.com.google.protobuf.Service} using standard protobuf service
   * invocations:
   * <p/>
   * <div style="background-color: #cccccc; padding: 2px"> <blockquote>
   * <pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService(serverName);
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre>
   * </blockquote>
   * </div>
   * @param serverName the server name to which the endpoint call is made
   * @return A RegionServerCoprocessorRpcChannel instance
   * @deprecated since 3.0.0, will removed in 4.0.0. This is too low level, please stop using it any
   *             more. Use the coprocessorService methods in {@link AsyncAdmin} instead.
   */
  @Deprecated
  CoprocessorRpcChannel coprocessorService(ServerName serverName);


  /**
   * Update the configuration and trigger an online config change
   * on the regionserver.
   * @param server : The server whose config needs to be updated.
   * @throws IOException if a remote or network exception occurs
   */
  void updateConfiguration(ServerName server) throws IOException;

  /**
   * Update the configuration and trigger an online config change
   * on all the regionservers.
   * @throws IOException if a remote or network exception occurs
   */
  void updateConfiguration() throws IOException;

  /**
   * Update the configuration and trigger an online config change
   * on all the regionservers in the RSGroup.
   * @param groupName the group name
   * @throws IOException if a remote or network exception occurs
   */
  void updateConfiguration(String groupName) throws IOException;

  /**
   * Get the info port of the current master if one is available.
   * @return master info port
   * @throws IOException if a remote or network exception occurs
   */
  default int getMasterInfoPort() throws IOException {
    return getClusterMetrics(EnumSet.of(Option.MASTER_INFO_PORT)).getMasterInfoPort();
  }

  /**
   * Return the set of supported security capabilities.
   * @throws IOException if a remote or network exception occurs
   * @throws UnsupportedOperationException
   */
  List<SecurityCapability> getSecurityCapabilities() throws IOException;

  /**
   * Turn the split switch on or off.
   * @param enabled enabled or not
   * @param synchronous If <code>true</code>, it waits until current split() call, if outstanding,
   *          to return.
   * @return Previous switch value
   * @throws IOException if a remote or network exception occurs
   */
  boolean splitSwitch(boolean enabled, boolean synchronous) throws IOException;

  /**
   * Turn the merge switch on or off.
   * @param enabled enabled or not
   * @param synchronous If <code>true</code>, it waits until current merge() call, if outstanding,
   *          to return.
   * @return Previous switch value
   * @throws IOException if a remote or network exception occurs
   */
  boolean mergeSwitch(boolean enabled, boolean synchronous) throws IOException;

  /**
   * Query the current state of the split switch.
   * @return <code>true</code> if the switch is enabled, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  boolean isSplitEnabled() throws IOException;

  /**
   * Query the current state of the merge switch.
   * @return <code>true</code> if the switch is enabled, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  boolean isMergeEnabled() throws IOException;

  /**
   * Add a new replication peer for replicating data to slave cluster.
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   * @throws IOException if a remote or network exception occurs
   */
  default void addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig)
      throws IOException {
    addReplicationPeer(peerId, peerConfig, true);
  }

  /**
   * Add a new replication peer for replicating data to slave cluster.
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   * @param enabled peer state, true if ENABLED and false if DISABLED
   * @throws IOException if a remote or network exception occurs
   */
  default void addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws IOException {
    get(addReplicationPeerAsync(peerId, peerConfig, enabled), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Add a new replication peer but does not block and wait for it.
   * <p/>
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   * @return the result of the async operation
   * @throws IOException IOException if a remote or network exception occurs
   */
  default Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig)
      throws IOException {
    return addReplicationPeerAsync(peerId, peerConfig, true);
  }

  /**
   * Add a new replication peer but does not block and wait for it.
   * <p>
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param peerId a short name that identifies the peer
   * @param peerConfig configuration for the replication peer
   * @param enabled peer state, true if ENABLED and false if DISABLED
   * @return the result of the async operation
   * @throws IOException IOException if a remote or network exception occurs
   */
  Future<Void> addReplicationPeerAsync(String peerId, ReplicationPeerConfig peerConfig,
      boolean enabled) throws IOException;

  /**
   * Remove a peer and stop the replication.
   * @param peerId a short name that identifies the peer
   * @throws IOException if a remote or network exception occurs
   */
  default void removeReplicationPeer(String peerId) throws IOException {
    get(removeReplicationPeerAsync(peerId), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Remove a replication peer but does not block and wait for it.
   * <p>
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param peerId a short name that identifies the peer
   * @return the result of the async operation
   * @throws IOException IOException if a remote or network exception occurs
   */
  Future<Void> removeReplicationPeerAsync(String peerId) throws IOException;

  /**
   * Restart the replication stream to the specified peer.
   * @param peerId a short name that identifies the peer
   * @throws IOException if a remote or network exception occurs
   */
  default void enableReplicationPeer(String peerId) throws IOException {
    get(enableReplicationPeerAsync(peerId), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Enable a replication peer but does not block and wait for it.
   * <p>
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param peerId a short name that identifies the peer
   * @return the result of the async operation
   * @throws IOException IOException if a remote or network exception occurs
   */
  Future<Void> enableReplicationPeerAsync(String peerId) throws IOException;

  /**
   * Stop the replication stream to the specified peer.
   * @param peerId a short name that identifies the peer
   * @throws IOException if a remote or network exception occurs
   */
  default void disableReplicationPeer(String peerId) throws IOException {
    get(disableReplicationPeerAsync(peerId), getSyncWaitTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * Disable a replication peer but does not block and wait for it.
   * <p/>
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param peerId a short name that identifies the peer
   * @return the result of the async operation
   * @throws IOException IOException if a remote or network exception occurs
   */
  Future<Void> disableReplicationPeerAsync(String peerId) throws IOException;

  /**
   * Returns the configured ReplicationPeerConfig for the specified peer.
   * @param peerId a short name that identifies the peer
   * @return ReplicationPeerConfig for the peer
   * @throws IOException if a remote or network exception occurs
   */
  ReplicationPeerConfig getReplicationPeerConfig(String peerId) throws IOException;

  /**
   * Update the peerConfig for the specified peer.
   * @param peerId a short name that identifies the peer
   * @param peerConfig new config for the replication peer
   * @throws IOException if a remote or network exception occurs
   */
  default void updateReplicationPeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws IOException {
    get(updateReplicationPeerConfigAsync(peerId, peerConfig), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Update the peerConfig for the specified peer but does not block and wait for it.
   * <p/>
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param peerId a short name that identifies the peer
   * @param peerConfig new config for the replication peer
   * @return the result of the async operation
   * @throws IOException IOException if a remote or network exception occurs
   */
  Future<Void> updateReplicationPeerConfigAsync(String peerId, ReplicationPeerConfig peerConfig)
      throws IOException;

  /**
   * Append the replicable table column family config from the specified peer.
   * @param id a short that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   * @throws ReplicationException if tableCfs has conflict with existing config
   * @throws IOException if a remote or network exception occurs
   */
  default void appendReplicationPeerTableCFs(String id, Map<TableName, List<String>> tableCfs)
      throws ReplicationException, IOException {
    if (tableCfs == null) {
      throw new ReplicationException("tableCfs is null");
    }
    ReplicationPeerConfig peerConfig = getReplicationPeerConfig(id);
    ReplicationPeerConfig newPeerConfig =
      ReplicationPeerConfigUtil.appendTableCFsToReplicationPeerConfig(tableCfs, peerConfig);
    updateReplicationPeerConfig(id, newPeerConfig);
  }

  /**
   * Remove some table-cfs from config of the specified peer.
   * @param id a short name that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   * @throws ReplicationException if tableCfs has conflict with existing config
   * @throws IOException if a remote or network exception occurs
   */
  default void removeReplicationPeerTableCFs(String id, Map<TableName, List<String>> tableCfs)
      throws ReplicationException, IOException {
    if (tableCfs == null) {
      throw new ReplicationException("tableCfs is null");
    }
    ReplicationPeerConfig peerConfig = getReplicationPeerConfig(id);
    ReplicationPeerConfig newPeerConfig =
      ReplicationPeerConfigUtil.removeTableCFsFromReplicationPeerConfig(tableCfs, peerConfig, id);
    updateReplicationPeerConfig(id, newPeerConfig);
  }

  /**
   * Return a list of replication peers.
   * @return a list of replication peers description
   * @throws IOException if a remote or network exception occurs
   */
  List<ReplicationPeerDescription> listReplicationPeers() throws IOException;

  /**
   * Return a list of replication peers.
   * @param pattern The compiled regular expression to match peer id
   * @return a list of replication peers description
   * @throws IOException if a remote or network exception occurs
   */
  List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern) throws IOException;

  /**
   * Transit current cluster to a new state in a synchronous replication peer.
   * @param peerId a short name that identifies the peer
   * @param state a new state of current cluster
   * @throws IOException if a remote or network exception occurs
   */
  default void transitReplicationPeerSyncReplicationState(String peerId, SyncReplicationState state)
      throws IOException {
    get(transitReplicationPeerSyncReplicationStateAsync(peerId, state), getSyncWaitTimeout(),
      TimeUnit.MILLISECONDS);
  }

  /**
   * Transit current cluster to a new state in a synchronous replication peer. But does not block
   * and wait for it.
   * <p>
   * You can use Future.get(long, TimeUnit) to wait on the operation to complete. It may throw
   * ExecutionException if there was an error while executing the operation or TimeoutException in
   * case the wait timeout was not long enough to allow the operation to complete.
   * @param peerId a short name that identifies the peer
   * @param state a new state of current cluster
   * @throws IOException if a remote or network exception occurs
   */
  Future<Void> transitReplicationPeerSyncReplicationStateAsync(String peerId,
      SyncReplicationState state) throws IOException;

  /**
   * Get the current cluster state in a synchronous replication peer.
   * @param peerId a short name that identifies the peer
   * @return the current cluster state
   * @throws IOException if a remote or network exception occurs
   */
  default SyncReplicationState getReplicationPeerSyncReplicationState(String peerId)
      throws IOException {
    List<ReplicationPeerDescription> peers = listReplicationPeers(Pattern.compile(peerId));
    if (peers.isEmpty() || !peers.get(0).getPeerId().equals(peerId)) {
      throw new IOException("Replication peer " + peerId + " does not exist");
    }
    return peers.get(0).getSyncReplicationState();
  }

  /**
   * Mark region server(s) as decommissioned to prevent additional regions from getting
   * assigned to them. Optionally unload the regions on the servers. If there are multiple servers
   * to be decommissioned, decommissioning them at the same time can prevent wasteful region
   * movements. Region unloading is asynchronous.
   * @param servers The list of servers to decommission.
   * @param offload True to offload the regions from the decommissioned servers
   * @throws IOException if a remote or network exception occurs
   */
  void decommissionRegionServers(List<ServerName> servers, boolean offload) throws IOException;

  /**
   * List region servers marked as decommissioned, which can not be assigned regions.
   * @return List of decommissioned region servers.
   * @throws IOException if a remote or network exception occurs
   */
  List<ServerName> listDecommissionedRegionServers() throws IOException;

  /**
   * Remove decommission marker from a region server to allow regions assignments.
   * Load regions onto the server if a list of regions is given. Region loading is
   * asynchronous.
   * @param server The server to recommission.
   * @param encodedRegionNames Regions to load onto the server.
   * @throws IOException if a remote or network exception occurs
   */
  void recommissionRegionServer(ServerName server, List<byte[]> encodedRegionNames)
      throws IOException;

  /**
   * Find all table and column families that are replicated from this cluster
   * @return the replicated table-cfs list of this cluster.
   * @throws IOException if a remote or network exception occurs
   */
  List<TableCFs> listReplicatedTableCFs() throws IOException;

  /**
   * Enable a table's replication switch.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  void enableTableReplication(TableName tableName) throws IOException;

  /**
   * Disable a table's replication switch.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  void disableTableReplication(TableName tableName) throws IOException;

  /**
   * Clear compacting queues on a regionserver.
   * @param serverName the region server name
   * @param queues the set of queue name
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void clearCompactionQueues(ServerName serverName, Set<String> queues)
    throws IOException, InterruptedException;

  /**
   * List dead region servers.
   * @return List of dead region servers.
   */
  default List<ServerName> listDeadServers() throws IOException {
    return getClusterMetrics(EnumSet.of(Option.DEAD_SERVERS)).getDeadServerNames();
  }

  /**
   * Clear dead region servers from master.
   * @param servers list of dead region servers.
   * @throws IOException if a remote or network exception occurs
   * @return List of servers that are not cleared
   */
  List<ServerName> clearDeadServers(List<ServerName> servers) throws IOException;

  /**
   * Create a new table by cloning the existent table schema.
   * @param tableName name of the table to be cloned
   * @param newTableName name of the new table where the table will be created
   * @param preserveSplits True if the splits should be preserved
   * @throws IOException if a remote or network exception occurs
   */
  void cloneTableSchema(TableName tableName, TableName newTableName, boolean preserveSplits)
      throws IOException;

  /**
   * Switch the rpc throttle enable state.
   * @param enable Set to <code>true</code> to enable, <code>false</code> to disable.
   * @return Previous rpc throttle enabled value
   * @throws IOException if a remote or network exception occurs
   */
  boolean switchRpcThrottle(boolean enable) throws IOException;

  /**
   * Get if the rpc throttle is enabled.
   * @return True if rpc throttle is enabled
   * @throws IOException if a remote or network exception occurs
   */
  boolean isRpcThrottleEnabled() throws IOException;

  /**
   * Switch the exceed throttle quota. If enabled, user/table/namespace throttle quota
   * can be exceeded if region server has availble quota.
   * @param enable Set to <code>true</code> to enable, <code>false</code> to disable.
   * @return Previous exceed throttle enabled value
   * @throws IOException if a remote or network exception occurs
   */
  boolean exceedThrottleQuotaSwitch(final boolean enable) throws IOException;

  /**
   * Fetches the table sizes on the filesystem as tracked by the HBase Master.
   * @throws IOException if a remote or network exception occurs
   */
  Map<TableName, Long> getSpaceQuotaTableSizes() throws IOException;

  /**
   * Fetches the observed {@link SpaceQuotaSnapshotView}s observed by a RegionServer.
   * @throws IOException if a remote or network exception occurs
   */
  Map<TableName, ? extends SpaceQuotaSnapshotView> getRegionServerSpaceQuotaSnapshots(
      ServerName serverName) throws IOException;

  /**
   * Returns the Master's view of a quota on the given {@code namespace} or null if the Master has
   * no quota information on that namespace.
   * @throws IOException if a remote or network exception occurs
   */
  SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(String namespace) throws IOException;

  /**
   * Returns the Master's view of a quota on the given {@code tableName} or null if the Master has
   * no quota information on that table.
   * @throws IOException if a remote or network exception occurs
   */
  SpaceQuotaSnapshotView getCurrentSpaceQuotaSnapshot(TableName tableName) throws IOException;

  /**
   * Grants user specific permissions
   * @param userPermission user name and the specific permission
   * @param mergeExistingPermissions If set to false, later granted permissions will override
   *          previous granted permissions. otherwise, it'll merge with previous granted
   *          permissions.
   * @throws IOException if a remote or network exception occurs
   */
  void grant(UserPermission userPermission, boolean mergeExistingPermissions) throws IOException;

  /**
   * Revokes user specific permissions
   * @param userPermission user name and the specific permission
   * @throws IOException if a remote or network exception occurs
   */
  void revoke(UserPermission userPermission) throws IOException;

  /**
   * Get the global/namespace/table permissions for user
   * @param getUserPermissionsRequest A request contains which user, global, namespace or table
   *          permissions needed
   * @return The user and permission list
   * @throws IOException if a remote or network exception occurs
   */
  List<UserPermission> getUserPermissions(GetUserPermissionsRequest getUserPermissionsRequest)
      throws IOException;

  /**
   * Check if the user has specific permissions
   * @param userName the user name
   * @param permissions the specific permission list
   * @return True if user has the specific permissions
   * @throws IOException if a remote or network exception occurs
   */
  List<Boolean> hasUserPermissions(String userName, List<Permission> permissions)
      throws IOException;

  /**
   * Check if call user has specific permissions
   * @param permissions the specific permission list
   * @return True if user has the specific permissions
   * @throws IOException if a remote or network exception occurs
   */
  default List<Boolean> hasUserPermissions(List<Permission> permissions) throws IOException {
    return hasUserPermissions(null, permissions);
  }

  /**
   * Turn on or off the auto snapshot cleanup based on TTL.
   *
   * @param on Set to <code>true</code> to enable, <code>false</code> to disable.
   * @param synchronous If <code>true</code>, it waits until current snapshot cleanup is completed,
   *   if outstanding.
   * @return Previous auto snapshot cleanup value
   * @throws IOException if a remote or network exception occurs
   */
  boolean snapshotCleanupSwitch(final boolean on, final boolean synchronous)
      throws IOException;

  /**
   * Query the current state of the auto snapshot cleanup based on TTL.
   *
   * @return <code>true</code> if the auto snapshot cleanup is enabled,
   *   <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  boolean isSnapshotCleanupEnabled() throws IOException;

  /**
   * Retrieves online slow/large RPC logs from the provided list of
   * RegionServers
   *
   * @param serverNames Server names to get slowlog responses from
   * @param logQueryFilter filter to be used if provided (determines slow / large RPC logs)
   * @return online slowlog response list
   * @throws IOException if a remote or network exception occurs
   * @deprecated since 2.4.0 and will be removed in 4.0.0.
   *   Use {@link #getLogEntries(Set, String, ServerType, int, Map)} instead.
   */
  @Deprecated
  default List<OnlineLogRecord> getSlowLogResponses(final Set<ServerName> serverNames,
      final LogQueryFilter logQueryFilter) throws IOException {
    String logType;
    if (LogQueryFilter.Type.LARGE_LOG.equals(logQueryFilter.getType())) {
      logType = "LARGE_LOG";
    } else {
      logType = "SLOW_LOG";
    }
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("regionName", logQueryFilter.getRegionName());
    filterParams.put("clientAddress", logQueryFilter.getClientAddress());
    filterParams.put("tableName", logQueryFilter.getTableName());
    filterParams.put("userName", logQueryFilter.getUserName());
    filterParams.put("filterByOperator", logQueryFilter.getFilterByOperator().toString());
    List<LogEntry> logEntries =
      getLogEntries(serverNames, logType, ServerType.REGION_SERVER, logQueryFilter.getLimit(),
        filterParams);
    return logEntries.stream().map(logEntry -> (OnlineLogRecord) logEntry)
      .collect(Collectors.toList());
  }

  /**
   * Clears online slow/large RPC logs from the provided list of
   * RegionServers
   *
   * @param serverNames Set of Server names to clean slowlog responses from
   * @return List of booleans representing if online slowlog response buffer is cleaned
   *   from each RegionServer
   * @throws IOException if a remote or network exception occurs
   */
  List<Boolean> clearSlowLogResponses(final Set<ServerName> serverNames)
      throws IOException;

  /**
   * Creates a new RegionServer group with the given name
   * @param groupName the name of the group
   * @throws IOException if a remote or network exception occurs
   */
  void addRSGroup(String groupName) throws IOException;

  /**
   * Get group info for the given group name
   * @param groupName the group name
   * @return group info
   * @throws IOException if a remote or network exception occurs
   */
  RSGroupInfo getRSGroup(String groupName) throws IOException;

  /**
   * Get group info for the given hostPort
   * @param hostPort HostPort to get RSGroupInfo for
   * @throws IOException if a remote or network exception occurs
   */
  RSGroupInfo getRSGroup(Address hostPort) throws IOException;

  /**
   * Get group info for the given table
   * @param tableName table name to get RSGroupInfo for
   * @throws IOException if a remote or network exception occurs
   */
  RSGroupInfo getRSGroup(TableName tableName) throws IOException;

  /**
   * Lists current set of RegionServer groups
   * @throws IOException if a remote or network exception occurs
   */
  List<RSGroupInfo> listRSGroups() throws IOException;

  /**
   * Get all tables in this RegionServer group.
   * @param groupName the group name
   * @throws IOException if a remote or network exception occurs
   * @see #getConfiguredNamespacesAndTablesInRSGroup(String)
   */
  List<TableName> listTablesInRSGroup(String groupName) throws IOException;

  /**
   * Get the namespaces and tables which have this RegionServer group in descriptor.
   * <p/>
   * The difference between this method and {@link #listTablesInRSGroup(String)} is that, this
   * method will not include the table which is actually in this RegionServr group but without the
   * RegionServer group configuration in its {@link TableDescriptor}. For example, we have a group
   * 'A', and we make namespace 'nsA' in this group, then all the tables under this namespace will
   * in the group 'A', but this method will not return these tables but only the namespace 'nsA',
   * while the {@link #listTablesInRSGroup(String)} will return all these tables.
   * @param groupName the group name
   * @throws IOException if a remote or network exception occurs
   * @see #listTablesInRSGroup(String)
   */
  Pair<List<String>, List<TableName>> getConfiguredNamespacesAndTablesInRSGroup(String groupName)
    throws IOException;

  /**
   * Remove RegionServer group associated with the given name
   * @param groupName the group name
   * @throws IOException if a remote or network exception occurs
   */
  void removeRSGroup(String groupName) throws IOException;

  /**
   * Remove decommissioned servers from group
   *  1. Sometimes we may find the server aborted due to some hardware failure and we must offline
   *     the server for repairing. Or we need to move some servers to join other clusters.
   *     So we need to remove these servers from the group.
   *  2. Dead/recovering/live servers will be disallowed.
   * @param servers set of servers to remove
   * @throws IOException if a remote or network exception occurs
   */
  void removeServersFromRSGroup(Set<Address> servers) throws IOException;

  /**
   * Move given set of servers to the specified target RegionServer group
   * @param servers set of servers to move
   * @param targetGroup the group to move servers to
   * @throws IOException if a remote or network exception occurs
   */
  void moveServersToRSGroup(Set<Address> servers, String targetGroup) throws IOException;

  /**
   * Set the RegionServer group for tables
   * @param tables tables to set group for
   * @param groupName group name for tables
   * @throws IOException if a remote or network exception occurs
   */
  void setRSGroup(Set<TableName> tables, String groupName) throws IOException;

  /**
   * Balance regions in the given RegionServer group
   * @param groupName the group name
   * @return BalanceResponse details about the balancer run
   * @throws IOException if a remote or network exception occurs
   */
  default BalanceResponse balanceRSGroup(String groupName) throws IOException {
    return balanceRSGroup(groupName, BalanceRequest.defaultInstance());
  }

  /**
   * Balance regions in the given RegionServer group, running based on
   * the given {@link BalanceRequest}.
   *
   * @return BalanceResponse details about the balancer run
   */
  BalanceResponse balanceRSGroup(String groupName, BalanceRequest request) throws IOException;

  /**
   * Rename rsgroup
   * @param oldName old rsgroup name
   * @param newName new rsgroup name
   * @throws IOException if a remote or network exception occurs
   */
  void renameRSGroup(String oldName, String newName) throws IOException;

  /**
   * Update RSGroup configuration
   * @param groupName the group name
   * @param configuration new configuration of the group name to be set
   * @throws IOException if a remote or network exception occurs
   */
  void updateRSGroupConfig(String groupName, Map<String, String> configuration) throws IOException;

  /**
   * Retrieve recent online records from HMaster / RegionServers.
   * Examples include slow/large RPC logs, balancer decisions by master.
   *
   * @param serverNames servers to retrieve records from, useful in case of records maintained
   *   by RegionServer as we can select specific server. In case of servertype=MASTER, logs will
   *   only come from the currently active master.
   * @param logType string representing type of log records
   * @param serverType enum for server type: HMaster or RegionServer
   * @param limit put a limit to list of records that server should send in response
   * @param filterParams additional filter params
   * @return Log entries representing online records from servers
   * @throws IOException if a remote or network exception occurs
   */
  List<LogEntry> getLogEntries(Set<ServerName> serverNames, String logType,
    ServerType serverType, int limit, Map<String, Object> filterParams) throws IOException;
}
