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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The administrative API for HBase. Obtain an instance from an {@link Connection#getAdmin()} and
 * call {@link #close()} afterwards.
 * <p>Admin can be used to create, drop, list, enable and disable tables, add and drop table
 * column families and other administrative operations.
 *
 * @see ConnectionFactory
 * @see Connection
 * @see Table
 * @since 0.99.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Admin extends Abortable, Closeable {
  int getOperationTimeout();

  @Override
  void abort(String why, Throwable e);

  @Override
  boolean isAborted();

  /**
   * @return Connection used by this object.
   */
  Connection getConnection();

  /**
   * @return - true if the master server is running. Throws an exception otherwise.
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   */
  boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException;

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  boolean tableExists(final TableName tableName) throws IOException;

  /**
   * List all the userspace tables.  In other words, scan the hbase:meta table. If we wanted this to
   * be really fast, we could implement a special catalog table that just contains table names and
   * their descriptors. Right now, it only exists as part of the hbase:meta table's region info.
   *
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  HTableDescriptor[] listTables() throws IOException;

  /**
   * List all the userspace tables matching the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables()
   */
  HTableDescriptor[] listTables(Pattern pattern) throws IOException;

  /**
   * List all the userspace tables matching the given regular expression.
   *
   * @param regex The regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables(java.util.regex.Pattern)
   */
  HTableDescriptor[] listTables(String regex) throws IOException;

  /**
   * List all of the names of userspace tables.
   *
   * @return TableName[] table names
   * @throws IOException if a remote or network exception occurs
   */
  TableName[] listTableNames() throws IOException;

  /**
   * Method for getting the tableDescriptor
   *
   * @param tableName as a {@link TableName}
   * @return the tableDescriptor
   * @throws org.apache.hadoop.hbase.TableNotFoundException
   * @throws IOException if a remote or network exception occurs
   */
  HTableDescriptor getTableDescriptor(final TableName tableName)
      throws TableNotFoundException, IOException;

  /**
   * Creates a new table. Synchronous operation.
   *
   * @param desc table descriptor for table
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  void createTable(HTableDescriptor desc) throws IOException;

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
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and attempt-at-creation).
   * @throws IOException
   */
  void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
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
   * @throws MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and attempt-at-creation).
   * @throws IOException
   */
  void createTable(final HTableDescriptor desc, byte[][] splitKeys) throws IOException;

  /**
   * Creates a new table but does not block and wait for it to come online. Asynchronous operation.
   * To check if the table exists, use {@link #isTableAvailable} -- it is not safe to create an
   * HTable instance to this table before it is available. Note : Avoid passing empty split key.
   *
   * @param desc table descriptor for table
   * @throws IllegalArgumentException Bad table name, if the split keys are repeated and if the
   * split key has empty byte array.
   * @throws MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence and attempt-at-creation).
   * @throws IOException
   */
  void createTableAsync(final HTableDescriptor desc, final byte[][] splitKeys) throws IOException;

  /**
   * Deletes a table. Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  void deleteTable(final TableName tableName) throws IOException;

  /**
   * Deletes tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using {@link
   * #listTables(java.lang.String)} and {@link #deleteTable(org.apache.hadoop.hbase.TableName)}
   *
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   * @see #deleteTables(java.util.regex.Pattern)
   * @see #deleteTable(org.apache.hadoop.hbase.TableName)
   */
  HTableDescriptor[] deleteTables(String regex) throws IOException;

  /**
   * Delete tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using {@link
   * #listTables(java.util.regex.Pattern) } and
   * {@link #deleteTable(org.apache.hadoop.hbase.TableName)}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   */
  HTableDescriptor[] deleteTables(Pattern pattern) throws IOException;

  /**
   * Truncate a table.
   * Synchronous operation.
   *
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   * @throws IOException if a remote or network exception occurs
   */
  public void truncateTable(final TableName tableName, final boolean preserveSplits)
      throws IOException;

  /**
   * Enable a table.  May timeout.  Use {@link #enableTableAsync(org.apache.hadoop.hbase.TableName)}
   * and {@link #isTableEnabled(org.apache.hadoop.hbase.TableName)} instead. The table has to be in
   * disabled state for it to be enabled.
   *
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs There could be couple types of
   * IOException TableNotFoundException means the table doesn't exist. TableNotDisabledException
   * means the table isn't in disabled state.
   * @see #isTableEnabled(org.apache.hadoop.hbase.TableName)
   * @see #disableTable(org.apache.hadoop.hbase.TableName)
   * @see #enableTableAsync(org.apache.hadoop.hbase.TableName)
   */
  void enableTable(final TableName tableName) throws IOException;

  /**
   * Brings a table on-line (enables it).  Method returns immediately though enable of table may
   * take some time to complete, especially if the table is large (All regions are opened as part of
   * enabling process).  Check {@link #isTableEnabled(org.apache.hadoop.hbase.TableName)} to learn
   * when table is fully online.  If table is taking too long to online, check server logs.
   *
   * @param tableName
   * @throws IOException
   * @since 0.90.0
   */
  void enableTableAsync(final TableName tableName) throws IOException;

  /**
   * Enable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using {@link
   * #listTables(java.lang.String)} and {@link #enableTable(org.apache.hadoop.hbase.TableName)}
   *
   * @param regex The regular expression to match table names against
   * @throws IOException
   * @see #enableTables(java.util.regex.Pattern)
   * @see #enableTable(org.apache.hadoop.hbase.TableName)
   */
  HTableDescriptor[] enableTables(String regex) throws IOException;

  /**
   * Enable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using {@link
   * #listTables(java.util.regex.Pattern) } and
   * {@link #enableTable(org.apache.hadoop.hbase.TableName)}
   *
   * @param pattern The pattern to match table names against
   * @throws IOException
   */
  HTableDescriptor[] enableTables(Pattern pattern) throws IOException;

  /**
   * Starts the disable of a table.  If it is being served, the master will tell the servers to stop
   * serving it.  This method returns immediately. The disable of a table can take some time if the
   * table is large (all regions are closed as part of table disable operation). Call {@link
   * #isTableDisabled(org.apache.hadoop.hbase.TableName)} to check for when disable completes. If
   * table is taking too long to online, check server logs.
   *
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   * @see #isTableDisabled(org.apache.hadoop.hbase.TableName)
   * @see #isTableEnabled(org.apache.hadoop.hbase.TableName)
   * @since 0.90.0
   */
  void disableTableAsync(final TableName tableName) throws IOException;

  /**
   * Disable table and wait on completion.  May timeout eventually.  Use {@link
   * #disableTableAsync(org.apache.hadoop.hbase.TableName)} and
   * {@link #isTableDisabled(org.apache.hadoop.hbase.TableName)} instead. The table has to be in
   * enabled state for it to be disabled.
   *
   * @param tableName
   * @throws IOException There could be couple types of IOException TableNotFoundException means the
   * table doesn't exist. TableNotEnabledException means the table isn't in enabled state.
   */
  void disableTable(final TableName tableName) throws IOException;

  /**
   * Disable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using {@link
   * #listTables(java.lang.String)} and {@link #disableTable(org.apache.hadoop.hbase.TableName)}
   *
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   * @see #disableTables(java.util.regex.Pattern)
   * @see #disableTable(org.apache.hadoop.hbase.TableName)
   */
  HTableDescriptor[] disableTables(String regex) throws IOException;

  /**
   * Disable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using {@link
   * #listTables(java.util.regex.Pattern) } and
   * {@link #disableTable(org.apache.hadoop.hbase.TableName)}
   *
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   */
  HTableDescriptor[] disableTables(Pattern pattern) throws IOException;

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableEnabled(TableName tableName) throws IOException;

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableDisabled(TableName tableName) throws IOException;

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableAvailable(TableName tableName) throws IOException;

  /**
   * Use this api to check if the table has been created with the specified number of splitkeys
   * which was used while creating the given table. Note : If this api is used after a table's
   * region gets splitted, the api may return false.
   *
   * @param tableName name of table to check
   * @param splitKeys keys to check if the table has been created with all split keys
   * @throws IOException if a remote or network excpetion occurs
   */
  boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException;

  /**
   * Get the status of alter command - indicates how many regions have received the updated schema
   * Asynchronous operation.
   *
   * @param tableName TableName instance
   * @return Pair indicating the number of regions updated Pair.getFirst() is the regions that are
   * yet to be updated Pair.getSecond() is the total number of regions of the table
   * @throws IOException if a remote or network exception occurs
   */
  Pair<Integer, Integer> getAlterStatus(final TableName tableName) throws IOException;

  /**
   * Get the status of alter command - indicates how many regions have received the updated schema
   * Asynchronous operation.
   *
   * @param tableName name of the table to get the status of
   * @return Pair indicating the number of regions updated Pair.getFirst() is the regions that are
   * yet to be updated Pair.getSecond() is the total number of regions of the table
   * @throws IOException if a remote or network exception occurs
   */
  Pair<Integer, Integer> getAlterStatus(final byte[] tableName) throws IOException;

  /**
   * Add a column to an existing table. Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  void addColumn(final TableName tableName, final HColumnDescriptor column) throws IOException;

  /**
   * Delete a column from a table. Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  void deleteColumn(final TableName tableName, final byte[] columnName) throws IOException;

  /**
   * Modify an existing column family on a table. Asynchronous operation.
   *
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  void modifyColumn(final TableName tableName, final HColumnDescriptor descriptor)
      throws IOException;

  /**
   * Close a region. For expert-admins.  Runs close on the regionserver.  The master will not be
   * informed of the close.
   *
   * @param regionname region name to close
   * @param serverName If supplied, we'll use this location rather than the one currently in
   * <code>hbase:meta</code>
   * @throws IOException if a remote or network exception occurs
   */
  void closeRegion(final String regionname, final String serverName) throws IOException;

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The master will not be
   * informed of the close.
   *
   * @param regionname region name to close
   * @param serverName The servername of the regionserver.  If passed null we will use servername
   * found in the hbase:meta table. A server name is made of host, port and startcode.  Here is an
   * example: <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException if a remote or network exception occurs
   */
  void closeRegion(final byte[] regionname, final String serverName) throws IOException;

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
   * @return true if the region was closed, false if not.
   * @throws IOException if a remote or network exception occurs
   */
  boolean closeRegionWithEncodedRegionName(final String encodedRegionName, final String serverName)
      throws IOException;

  /**
   * Close a region.  For expert-admins  Runs close on the regionserver.  The master will not be
   * informed of the close.
   *
   * @param sn
   * @param hri
   * @throws IOException
   */
  void closeRegion(final ServerName sn, final HRegionInfo hri) throws IOException;

  /**
   * Get all the online regions on a region server.
   */
  List<HRegionInfo> getOnlineRegions(final ServerName sn) throws IOException;

  /**
   * Flush a table. Synchronous operation.
   *
   * @param tableName table to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void flush(final TableName tableName) throws IOException, InterruptedException;

  /**
   * Flush an individual region. Synchronous operation.
   *
   * @param regionName region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void flushRegion(final byte[] regionName) throws IOException, InterruptedException;

  /**
   * Compact a table. Asynchronous operation.
   *
   * @param tableName table to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void compact(final TableName tableName) throws IOException, InterruptedException;

  /**
   * Compact an individual region. Asynchronous operation.
   *
   * @param regionName region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void compactRegion(final byte[] regionName) throws IOException, InterruptedException;

  /**
   * Compact a column family within a table. Asynchronous operation.
   *
   * @param tableName table to compact
   * @param columnFamily column family within a table
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void compact(final TableName tableName, final byte[] columnFamily)
    throws IOException, InterruptedException;

  /**
   * Compact a column family within a region. Asynchronous operation.
   *
   * @param regionName region to compact
   * @param columnFamily column family within a region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void compactRegion(final byte[] regionName, final byte[] columnFamily)
    throws IOException, InterruptedException;

  /**
   * Major compact a table. Asynchronous operation.
   *
   * @param tableName table to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void majorCompact(TableName tableName) throws IOException, InterruptedException;

  /**
   * Major compact a table or an individual region. Asynchronous operation.
   *
   * @param regionName region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void majorCompactRegion(final byte[] regionName) throws IOException, InterruptedException;

  /**
   * Major compact a column family within a table. Asynchronous operation.
   *
   * @param tableName table to major compact
   * @param columnFamily column family within a table
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void majorCompact(TableName tableName, final byte[] columnFamily)
    throws IOException, InterruptedException;

  /**
   * Major compact a column family within region. Asynchronous operation.
   *
   * @param regionName egion to major compact
   * @param columnFamily column family within a region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void majorCompactRegion(final byte[] regionName, final byte[] columnFamily)
    throws IOException, InterruptedException;

  /**
   * Move the region <code>r</code> to <code>dest</code>.
   *
   * @param encodedRegionName The encoded region name; i.e. the hash that makes up the region name
   * suffix: e.g. if regionname is
   * <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   * then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName The servername of the destination regionserver.  If passed the empty byte
   * array we'll assign to a random server.  A server name is made of host, port and startcode.
   * Here is an example: <code> host187.example.com,60020,1289493121758</code>
   * @throws UnknownRegionException Thrown if we can't find a region named
   * <code>encodedRegionName</code>
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   */
  void move(final byte[] encodedRegionName, final byte[] destServerName)
      throws HBaseIOException, MasterNotRunningException, ZooKeeperConnectionException;

  /**
   * @param regionName Region name to assign.
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  void assign(final byte[] regionName)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException;

  /**
   * Unassign a region from current hosting regionserver.  Region will then be assigned to a
   * regionserver chosen at random.  Region could be reassigned back to the same server.  Use {@link
   * #move(byte[], byte[])} if you want to control the region movement.
   *
   * @param regionName Region to unassign. Will clear any existing RegionPlan if one found.
   * @param force If true, force unassign (Will remove region from regions-in-transition too if
   * present. If results in double assignment use hbck -fix to resolve. To be used by experts).
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  void unassign(final byte[] regionName, final boolean force)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException;

  /**
   * Offline specified region from master's in-memory state. It will not attempt to reassign the
   * region as in unassign. This API can be used when a region not served by any region server and
   * still online as per Master's in memory state. If this API is incorrectly used on active region
   * then master will loose track of that region. This is a special method that should be used by
   * experts or hbck.
   *
   * @param regionName Region to offline.
   * @throws IOException
   */
  void offline(final byte[] regionName) throws IOException;

  /**
   * Turn the load balancer on or off.
   *
   * @param on If true, enable balancer. If false, disable balancer.
   * @param synchronous If true, it waits until current balance() call, if outstanding, to return.
   * @return Previous balancer value
   */
  boolean setBalancerRunning(final boolean on, final boolean synchronous)
      throws MasterNotRunningException, ZooKeeperConnectionException;

  /**
   * Invoke the balancer.  Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments.  Can NOT run for various reasons.  Check logs.
   *
   * @return True if balancer ran, false otherwise.
   */
  boolean balancer()
      throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException;

  /**
   * Enable/Disable the catalog janitor
   *
   * @param enable if true enables the catalog janitor
   * @return the previous state
   * @throws ServiceException
   * @throws MasterNotRunningException
   */
  boolean enableCatalogJanitor(boolean enable) throws ServiceException, MasterNotRunningException;

  /**
   * Ask for a scan of the catalog table
   *
   * @return the number of entries cleaned
   * @throws ServiceException
   * @throws MasterNotRunningException
   */
  int runCatalogScan() throws ServiceException, MasterNotRunningException;

  /**
   * Query on the catalog janitor state (Enabled/Disabled?)
   *
   * @throws ServiceException
   * @throws org.apache.hadoop.hbase.MasterNotRunningException
   */
  boolean isCatalogJanitorEnabled() throws ServiceException, MasterNotRunningException;

  /**
   * Merge two regions. Asynchronous operation.
   *
   * @param encodedNameOfRegionA encoded name of region a
   * @param encodedNameOfRegionB encoded name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge two adjacent
   * regions
   * @throws IOException
   */
  void mergeRegions(final byte[] encodedNameOfRegionA, final byte[] encodedNameOfRegionB,
      final boolean forcible) throws IOException;

  /**
   * Split a table. Asynchronous operation.
   *
   * @param tableName table to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void split(final TableName tableName) throws IOException, InterruptedException;

  /**
   * Split an individual region. Asynchronous operation.
   *
   * @param regionName region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  void splitRegion(final byte[] regionName) throws IOException, InterruptedException;

  /**
   * Split a table. Asynchronous operation.
   *
   * @param tableName table to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException interrupt exception occurred
   */
  void split(final TableName tableName, final byte[] splitPoint)
    throws IOException, InterruptedException;

  /**
   * Split an individual region. Asynchronous operation.
   *
   * @param regionName region to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException interrupt exception occurred
   */
  void splitRegion(final byte[] regionName, final byte[] splitPoint)
    throws IOException, InterruptedException;

  /**
   * Modify an existing table, more IRB friendly version. Asynchronous operation.  This means that
   * it may be a while before your schema change is updated across all of the table.
   *
   * @param tableName name of table.
   * @param htd modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  void modifyTable(final TableName tableName, final HTableDescriptor htd) throws IOException;

  /**
   * Shuts down the HBase cluster
   *
   * @throws IOException if a remote or network exception occurs
   */
  void shutdown() throws IOException;

  /**
   * Shuts down the current HBase master only. Does not shutdown the cluster.
   *
   * @throws IOException if a remote or network exception occurs
   * @see #shutdown()
   */
  void stopMaster() throws IOException;

  /**
   * Stop the designated regionserver
   *
   * @param hostnamePort Hostname and port delimited by a <code>:</code> as in
   * <code>example.org:1234</code>
   * @throws IOException if a remote or network exception occurs
   */
  void stopRegionServer(final String hostnamePort) throws IOException;

  /**
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  ClusterStatus getClusterStatus() throws IOException;

  /**
   * @return Configuration used by the instance.
   */
  Configuration getConfiguration();

  /**
   * Create a new namespace
   *
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException
   */
  void createNamespace(final NamespaceDescriptor descriptor) throws IOException;

  /**
   * Modify an existing namespace
   *
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException
   */
  void modifyNamespace(final NamespaceDescriptor descriptor) throws IOException;

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
   *
   * @param name namespace name
   * @throws IOException
   */
  void deleteNamespace(final String name) throws IOException;

  /**
   * Get a namespace descriptor by name
   *
   * @param name name of namespace descriptor
   * @return A descriptor
   * @throws IOException
   */
  NamespaceDescriptor getNamespaceDescriptor(final String name) throws IOException;

  /**
   * List available namespace descriptors
   *
   * @return List of descriptors
   * @throws IOException
   */
  NamespaceDescriptor[] listNamespaceDescriptors() throws IOException;

  /**
   * Get list of table descriptors by namespace
   *
   * @param name namespace name
   * @return A descriptor
   * @throws IOException
   */
  HTableDescriptor[] listTableDescriptorsByNamespace(final String name) throws IOException;

  /**
   * Get list of table names by namespace
   *
   * @param name namespace name
   * @return The list of table names in the namespace
   * @throws IOException
   */
  TableName[] listTableNamesByNamespace(final String name) throws IOException;

  /**
   * Get the regions of a given table.
   *
   * @param tableName the name of the table
   * @return List of {@link HRegionInfo}.
   * @throws IOException
   */
  List<HRegionInfo> getTableRegions(final TableName tableName) throws IOException;

  @Override
  void close() throws IOException;

  /**
   * Get tableDescriptors
   *
   * @param tableNames List of table names
   * @return HTD[] the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames) throws IOException;

  /**
   * Get tableDescriptors
   *
   * @param names List of table names
   * @return HTD[] the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException;

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * @param serverName The servername of the regionserver. A server name is made of host, port and
   * startcode. This is mandatory. Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>
   * @return If lots of logs, flush the returned regions so next time through we can clean logs.
   * Returns null if nothing to flush.  Names are actual region names as returned by {@link
   * HRegionInfo#getEncodedName()}
   * @throws IOException if a remote or network exception occurs
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   */
  byte[][] rollHLogWriter(String serverName) throws IOException, FailedLogCloseException;

  /**
   * Helper delegage to getClusterStatus().getMasterCoprocessors().
   * @return an array of master coprocessors
   * @see org.apache.hadoop.hbase.ClusterStatus#getMasterCoprocessors()
   */
  String[] getMasterCoprocessors();

  /**
   * Get the current compaction state of a table. It could be in a major compaction, a minor
   * compaction, both, or none.
   *
   * @param tableName table to examine
   * @return the current compaction state
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  AdminProtos.GetRegionInfoResponse.CompactionState getCompactionState(final TableName tableName)
    throws IOException, InterruptedException;

  /**
   * Get the current compaction state of region. It could be in a major compaction, a minor
   * compaction, both, or none.
   *
   * @param regionName region to examine
   * @return the current compaction state
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  AdminProtos.GetRegionInfoResponse.CompactionState getCompactionStateForRegion(
    final byte[] regionName) throws IOException, InterruptedException;

  /**
   * Take a snapshot for the given table. If the table is enabled, a FLUSH-type snapshot will be
   * taken. If the table is disabled, an offline snapshot is taken. Snapshots are considered unique
   * based on <b>the name of the snapshot</b>. Attempts to take a snapshot with the same name (even
   * a different type or with different parameters) will fail with a {@link
   * org.apache.hadoop.hbase.snapshot.SnapshotCreationException} indicating the duplicate naming.
   * Snapshot names follow the same naming constraints as tables in HBase. See {@link
   * org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   *
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws org.apache.hadoop.hbase.snapshot.SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  void snapshot(final String snapshotName, final TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException;

  /**
   * public void snapshot(final String snapshotName, Create a timestamp consistent snapshot for the
   * given table. final byte[] tableName) throws IOException, Snapshots are considered unique based
   * on <b>the name of the snapshot</b>. Attempts to take a snapshot with the same name (even a
   * different type or with different parameters) will fail with a {@link SnapshotCreationException}
   * indicating the duplicate naming. Snapshot names follow the same naming constraints as tables in
   * HBase.
   *
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  void snapshot(final byte[] snapshotName, final TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException;

  /**
   * Create typed snapshot of the table. Snapshots are considered unique based on <b>the name of the
   * snapshot</b>. Attempts to take a snapshot with the same name (even a different type or with
   * different parameters) will fail with a {@link SnapshotCreationException} indicating the
   * duplicate naming. Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   *
   * @param snapshotName name to give the snapshot on the filesystem. Must be unique from all other
   * snapshots stored on the cluster
   * @param tableName name of the table to snapshot
   * @param type type of snapshot to take
   * @throws IOException we fail to reach the master
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  void snapshot(final String snapshotName,
      final TableName tableName,
      HBaseProtos.SnapshotDescription.Type type) throws IOException, SnapshotCreationException,
      IllegalArgumentException;

  /**
   * Take a snapshot and wait for the server to complete that snapshot (blocking). Only a single
   * snapshot should be taken at a time for an instance of HBase, or results may be undefined (you
   * can tell multiple HBase clusters to snapshot at the same time, but only one at a time for a
   * single cluster). Snapshots are considered unique based on <b>the name of the snapshot</b>.
   * Attempts to take a snapshot with the same name (even a different type or with different
   * parameters) will fail with a {@link SnapshotCreationException} indicating the duplicate naming.
   * Snapshot names follow the same naming constraints as tables in HBase. See {@link
   * org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}. You should probably
   * use {@link #snapshot(String, org.apache.hadoop.hbase.TableName)} or
   * {@link #snapshot(byte[], org.apache.hadoop.hbase.TableName)} unless you are sure about the type
   * of snapshot that you want to take.
   *
   * @param snapshot snapshot to take
   * @throws IOException or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot failed to be taken
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  void snapshot(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException;

  /**
   * Take a snapshot without waiting for the server to complete that snapshot (asynchronous) Only a
   * single snapshot should be taken at a time, or results may be undefined.
   *
   * @param snapshot snapshot to take
   * @return response from the server indicating the max time to wait for the snapshot
   * @throws IOException if the snapshot did not succeed or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  MasterProtos.SnapshotResponse takeSnapshotAsync(HBaseProtos.SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException;

  /**
   * Check the current state of the passed snapshot. There are three possible states: <ol>
   * <li>running - returns <tt>false</tt></li> <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the snapshot to fail</li> </ol> The
   * cluster only knows about the most recent snapshot. Therefore, if another snapshot has been
   * run/started since the snapshot your are checking, you will recieve an {@link
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
  boolean isSnapshotFinished(final HBaseProtos.SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException;

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If the
   * "hbase.snapshot.restore.take.failsafe.snapshot" configuration property is set to true, a
   * snapshot of the current table is taken before executing the restore operation. In case of
   * restore failure, the failsafe snapshot will be restored. If the restore completes without
   * problem the failsafe snapshot is deleted.
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws org.apache.hadoop.hbase.snapshot.RestoreSnapshotException if snapshot failed to be
   * restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  void restoreSnapshot(final byte[] snapshotName) throws IOException, RestoreSnapshotException;

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If the
   * "hbase.snapshot.restore.take.failsafe.snapshot" configuration property is set to true, a
   * snapshot of the current table is taken before executing the restore operation. In case of
   * restore failure, the failsafe snapshot will be restored. If the restore completes without
   * problem the failsafe snapshot is deleted.
   *
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  void restoreSnapshot(final String snapshotName) throws IOException, RestoreSnapshotException;

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken before
   * executing the restore operation. In case of restore failure, the failsafe snapshot will be
   * restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   *
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  void restoreSnapshot(final byte[] snapshotName, final boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException;

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken before
   * executing the restore operation. In case of restore failure, the failsafe snapshot will be
   * restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   *
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  void restoreSnapshot(final String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException;

  /**
   * Create a new table by cloning the snapshot content.
   *
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  void cloneSnapshot(final byte[] snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException;

  /**
   * Create a new table by cloning the snapshot content.
   *
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  void cloneSnapshot(final String snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException;

  /**
   * Execute a distributed procedure on a cluster.
   *
   * @param signature A distributed procedure is uniquely identified by its signature (default the
   * root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   * optional.
   * @param props Property/Value pairs of properties passing to the procedure
   * @throws IOException
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
   * @throws IOException
   */
  byte[] execProcedureWithRet(String signature, String instance, Map<String, String> props)
      throws IOException;

  /**
   * Check the current state of the specified procedure. There are three possible states: <ol>
   * <li>running - returns <tt>false</tt></li> <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the procedure to fail</li> </ol>
   *
   * @param signature The signature that uniquely identifies a procedure
   * @param instance The instance name of the procedure
   * @param props Property/Value pairs of properties passing to the procedure
   * @return true if the specified procedure is finished successfully, false if it is still running
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
  List<HBaseProtos.SnapshotDescription> listSnapshots() throws IOException;

  /**
   * List all the completed snapshots matching the given regular expression.
   *
   * @param regex The regular expression to match against
   * @return - returns a List of SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  List<HBaseProtos.SnapshotDescription> listSnapshots(String regex) throws IOException;

  /**
   * List all the completed snapshots matching the given pattern.
   *
   * @param pattern The compiled regular expression to match against
   * @return - returns a List of SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  List<HBaseProtos.SnapshotDescription> listSnapshots(Pattern pattern) throws IOException;

  /**
   * Delete an existing snapshot.
   *
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  void deleteSnapshot(final byte[] snapshotName) throws IOException;

  /**
   * Delete an existing snapshot.
   *
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  void deleteSnapshot(final String snapshotName) throws IOException;

  /**
   * Delete existing snapshots whose names match the pattern passed.
   *
   * @param regex The regular expression to match against
   * @throws IOException if a remote or network exception occurs
   */
  void deleteSnapshots(final String regex) throws IOException;

  /**
   * Delete existing snapshots whose names match the pattern passed.
   *
   * @param pattern pattern for names of the snapshot to match
   * @throws IOException if a remote or network exception occurs
   */
  void deleteSnapshots(final Pattern pattern) throws IOException;

  /**
   * Apply the new quota settings.
   *
   * @param quota the quota settings
   * @throws IOException if a remote or network exception occurs
   */
  void setQuota(final QuotaSettings quota) throws IOException;

  /**
   * Return a QuotaRetriever to list the quotas based on the filter.
   *
   * @param filter the quota settings filter
   * @return the quota retriever
   * @throws IOException if a remote or network exception occurs
   */
  QuotaRetriever getQuotaRetriever(final QuotaFilter filter) throws IOException;

  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance connected to the active
   * master. <p> The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access
   * a published coprocessor {@link com.google.protobuf.Service} using standard protobuf service
   * invocations: </p> <div style="background-color: #cccccc; padding: 2px">
   * <blockquote><pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService();
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre></blockquote></div>
   *
   * @return A MasterCoprocessorRpcChannel instance
   */
  CoprocessorRpcChannel coprocessorService();


  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance
   * connected to the passed region server.
   *
   * <p>
   * The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access a published
   * coprocessor {@link com.google.protobuf.Service} using standard protobuf service invocations:
   * </p>
   *
   * <div style="background-color: #cccccc; padding: 2px">
   * <blockquote><pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService(serverName);
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre></blockquote></div>
   *
   * @param sn the server name to which the endpoint call is made
   * @return A RegionServerCoprocessorRpcChannel instance
   */
  CoprocessorRpcChannel coprocessorService(ServerName sn);
  

  /**
   * Update the configuration and trigger an online config change
   * on the regionserver
   * @param server : The server whose config needs to be updated.
   * @throws IOException
   */
  void updateConfiguration(ServerName server) throws IOException;


  /**
   * Update the configuration and trigger an online config change
   * on all the regionservers
   * @throws IOException
   */
  void updateConfiguration() throws IOException;
}
