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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;

/**
 * A cluster connection.  Knows how to find the master, locate regions out on the cluster,
 * keeps a cache of locations and then knows how to re-calibrate after they move.  You need one
 * of these to talk to your HBase cluster. {@link HConnectionManager} manages instances of this
 * class.  See it for how to get one of these.
 *
 * <p>This is NOT a connection to a particular server but to ALL servers in the cluster.  Individual
 * connections are managed at a lower level.
 *
 * <p>HConnections are used by {@link HTable} mostly but also by
 * {@link HBaseAdmin}, and {@link org.apache.hadoop.hbase.zookeeper.MetaTableLocator}.
 * HConnection instances can be shared.  Sharing
 * is usually what you want because rather than each HConnection instance
 * having to do its own discovery of regions out on the cluster, instead, all
 * clients get to share the one cache of locations.  {@link HConnectionManager} does the
 * sharing for you if you go by it getting connections.  Sharing makes cleanup of
 * HConnections awkward.  See {@link HConnectionManager} for cleanup discussion.
 *
 * @see HConnectionManager
 * @deprecated in favor of {@link Connection} and {@link ConnectionFactory}
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public interface HConnection extends Connection {
  /**
   * Key for configuration in Configuration whose value is the class we implement making a
   * new HConnection instance.
   */
  public static final String HBASE_CLIENT_CONNECTION_IMPL = "hbase.client.connection.impl";

  /**
   * @return Configuration instance being used by this HConnection instance.
   */
  @Override
  Configuration getConfiguration();

  /**
   * Retrieve an HTableInterface implementation for access to a table.
   * The returned HTableInterface is not thread safe, a new instance should
   * be created for each using thread.
   * This is a lightweight operation, pooling or caching of the returned HTableInterface
   * is neither required nor desired.
   * Note that the HConnection needs to be unmanaged
   * (created with {@link HConnectionManager#createConnection(Configuration)}).
   * @param tableName
   * @return an HTable to use for interactions with this table
   */
  public HTableInterface getTable(String tableName) throws IOException;

  /**
   * Retrieve an HTableInterface implementation for access to a table.
   * The returned HTableInterface is not thread safe, a new instance should
   * be created for each using thread.
   * This is a lightweight operation, pooling or caching of the returned HTableInterface
   * is neither required nor desired.
   * Note that the HConnection needs to be unmanaged
   * (created with {@link HConnectionManager#createConnection(Configuration)}).
   * @param tableName
   * @return an HTable to use for interactions with this table
   */
  public HTableInterface getTable(byte[] tableName) throws IOException;

  /**
   * Retrieve an HTableInterface implementation for access to a table.
   * The returned HTableInterface is not thread safe, a new instance should
   * be created for each using thread.
   * This is a lightweight operation, pooling or caching of the returned HTableInterface
   * is neither required nor desired.
   * Note that the HConnection needs to be unmanaged
   * (created with {@link HConnectionManager#createConnection(Configuration)}).
   * @param tableName
   * @return an HTable to use for interactions with this table
   */
  @Override
  public HTableInterface getTable(TableName tableName) throws IOException;

  /**
   * Retrieve an HTableInterface implementation for access to a table.
   * The returned HTableInterface is not thread safe, a new instance should
   * be created for each using thread.
   * This is a lightweight operation, pooling or caching of the returned HTableInterface
   * is neither required nor desired.
   * Note that the HConnection needs to be unmanaged
   * (created with {@link HConnectionManager#createConnection(Configuration)}).
   * @param tableName
   * @param pool The thread pool to use for batch operations, null to use a default pool.
   * @return an HTable to use for interactions with this table
   */
  public HTableInterface getTable(String tableName, ExecutorService pool)  throws IOException;

  /**
   * Retrieve an HTableInterface implementation for access to a table.
   * The returned HTableInterface is not thread safe, a new instance should
   * be created for each using thread.
   * This is a lightweight operation, pooling or caching of the returned HTableInterface
   * is neither required nor desired.
   * Note that the HConnection needs to be unmanaged
   * (created with {@link HConnectionManager#createConnection(Configuration)}).
   * @param tableName
   * @param pool The thread pool to use for batch operations, null to use a default pool.
   * @return an HTable to use for interactions with this table
   */
  public HTableInterface getTable(byte[] tableName, ExecutorService pool)  throws IOException;

  /**
   * Retrieve an HTableInterface implementation for access to a table.
   * The returned HTableInterface is not thread safe, a new instance should
   * be created for each using thread.
   * This is a lightweight operation, pooling or caching of the returned HTableInterface
   * is neither required nor desired.
   * Note that the HConnection needs to be unmanaged
   * (created with {@link HConnectionManager#createConnection(Configuration)}).
   * @param tableName
   * @param pool The thread pool to use for batch operations, null to use a default pool.
   * @return an HTable to use for interactions with this table
   */
  @Override
  public HTableInterface getTable(TableName tableName, ExecutorService pool)  throws IOException;

  /**
   * Retrieve a RegionLocator implementation to inspect region information on a table. The returned
   * RegionLocator is not thread-safe, so a new instance should be created for each using thread.
   *
   * This is a lightweight operation.  Pooling or caching of the returned RegionLocator is neither
   * required nor desired.
   *
   * RegionLocator needs to be unmanaged
   * (created with {@link HConnectionManager#createConnection(Configuration)}).
   *
   * @param tableName Name of the table who's region is to be examined
   * @return A RegionLocator instance
   */
  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException;

  /**
   * Retrieve an Admin implementation to administer an HBase cluster.
   * The returned Admin is not guaranteed to be thread-safe.  A new instance should be created for
   * each using thread.  This is a lightweight operation.  Pooling or caching of the returned
   * Admin is not recommended.  Note that HConnection needs to be unmanaged
   *
   * @return an Admin instance for cluster administration
   */
  @Override
  Admin getAdmin() throws IOException;

  /** @return - true if the master server is running
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  boolean isMasterRunning()
  throws MasterNotRunningException, ZooKeeperConnectionException;

  /**
   * A table that isTableEnabled == false and isTableDisabled == false
   * is possible. This happens when a table has a lot of regions
   * that must be processed.
   * @param tableName table name
   * @return true if the table is enabled, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableEnabled(TableName tableName) throws IOException;

  @Deprecated
  boolean isTableEnabled(byte[] tableName) throws IOException;

  /**
   * @param tableName table name
   * @return true if the table is disabled, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableDisabled(TableName tableName) throws IOException;

  @Deprecated
  boolean isTableDisabled(byte[] tableName) throws IOException;

  /**
   * Retrieve TableState, represent current table state.
   * @param tableName table state for
   * @return state of the table
   */
  public TableState getTableState(TableName tableName)  throws IOException;

  /**
   * @param tableName table name
   * @return true if all regions of the table are available, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableAvailable(TableName tableName) throws IOException;

  @Deprecated
  boolean isTableAvailable(byte[] tableName) throws IOException;

  /**
   * Use this api to check if the table has been created with the specified number of
   * splitkeys which was used while creating the given table.
   * Note : If this api is used after a table's region gets splitted, the api may return
   * false.
   * @param tableName
   *          tableName
   * @param splitKeys
   *          splitKeys used while creating table
   * @throws IOException
   *           if a remote or network exception occurs
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws
      IOException;

  @Deprecated
  boolean isTableAvailable(byte[] tableName, byte[][] splitKeys) throws
      IOException;

  /**
   * List all the userspace tables.  In other words, scan the hbase:meta table.
   *
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link Admin#listTables()} instead.
   */
  @Deprecated
  HTableDescriptor[] listTables() throws IOException;

  /**
   * List all the userspace tables matching the pattern.
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  HTableDescriptor[] listTables(String pattern) throws IOException;

  // This is a bit ugly - We call this getTableNames in 0.94 and the
  // successor function, returning TableName, listTableNames in later versions
  // because Java polymorphism doesn't consider return value types

  /**
   * @deprecated Use {@link Admin#listTableNames()} instead.
   */
  @Deprecated
  String[] getTableNames() throws IOException;

  /**
   * @deprecated Use {@link Admin#listTables()} instead.
   */
  @Deprecated
  TableName[] listTableNames() throws IOException;

  /**
   * @param tableName table name
   * @return table metadata
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  HTableDescriptor getHTableDescriptor(TableName tableName)
  throws IOException;

  @Deprecated
  HTableDescriptor getHTableDescriptor(byte[] tableName)
  throws IOException;

  /**
   * Find the location of the region of <i>tableName</i> that <i>row</i>
   * lives in.
   * @param tableName name of the table <i>row</i> is in
   * @param row row key you're trying to find the region of
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   * @deprecated internal method, do not use thru HConnection
   */
  @Deprecated
  public HRegionLocation locateRegion(final TableName tableName,
      final byte [] row) throws IOException;

  @Deprecated
  public HRegionLocation locateRegion(final byte[] tableName,
      final byte [] row) throws IOException;

  /**
   * Allows flushing the region cache.
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  void clearRegionCache();

  /**
   * Allows flushing the region cache of all locations that pertain to
   * <code>tableName</code>
   * @param tableName Name of the table whose regions we are to remove from
   * cache.
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  void clearRegionCache(final TableName tableName);

  @Deprecated
  void clearRegionCache(final byte[] tableName);

  /**
   * Deletes cached locations for the specific region.
   * @param location The location object for the region, to be purged from cache.
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  void deleteCachedRegionLocation(final HRegionLocation location);

  /**
   * Find the location of the region of <i>tableName</i> that <i>row</i>
   * lives in, ignoring any value that might be in the cache.
   * @param tableName name of the table <i>row</i> is in
   * @param row row key you're trying to find the region of
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  HRegionLocation relocateRegion(final TableName tableName,
      final byte [] row) throws IOException;

  @Deprecated
  HRegionLocation relocateRegion(final byte[] tableName,
      final byte [] row) throws IOException;

  @Deprecated
  void updateCachedLocations(TableName tableName, byte[] rowkey,
                                    Object exception, HRegionLocation source);

  /**
   * Update the location cache. This is used internally by HBase, in most cases it should not be
   *  used by the client application.
   * @param tableName the table name
   * @param regionName the regionName
   * @param rowkey the row
   * @param exception the exception if any. Can be null.
   * @param source the previous location
   * @deprecated internal method, do not use thru HConnection
   */
  @Deprecated
  void updateCachedLocations(TableName tableName, byte[] regionName, byte[] rowkey,
                                    Object exception, ServerName source);

  @Deprecated
  void updateCachedLocations(byte[] tableName, byte[] rowkey,
                                    Object exception, HRegionLocation source);

  /**
   * Gets the location of the region of <i>regionName</i>.
   * @param regionName name of the region to locate
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  HRegionLocation locateRegion(final byte[] regionName)
  throws IOException;

  /**
   * Gets the locations of all regions in the specified table, <i>tableName</i>.
   * @param tableName table to get regions of
   * @return list of region locations for all regions of table
   * @throws IOException
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  List<HRegionLocation> locateRegions(final TableName tableName) throws IOException;

  @Deprecated
  List<HRegionLocation> locateRegions(final byte[] tableName) throws IOException;

  /**
   * Gets the locations of all regions in the specified table, <i>tableName</i>.
   * @param tableName table to get regions of
   * @param useCache Should we use the cache to retrieve the region information.
   * @param offlined True if we are to include offlined regions, false and we'll leave out offlined
   *          regions from returned list.
   * @return list of region locations for all regions of table
   * @throws IOException
   * @deprecated internal method, do not use thru HConnection
   */
  @Deprecated
  public List<HRegionLocation> locateRegions(final TableName tableName,
      final boolean useCache,
      final boolean offlined) throws IOException;

  @Deprecated
  public List<HRegionLocation> locateRegions(final byte[] tableName,
      final boolean useCache,
      final boolean offlined) throws IOException;

  /**
   * Returns a {@link MasterKeepAliveConnection} to the active master
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  MasterService.BlockingInterface getMaster() throws IOException;


  /**
   * Establishes a connection to the region server at the specified address.
   * @param serverName
   * @return proxy for HRegionServer
   * @throws IOException if a remote or network exception occurs
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  AdminService.BlockingInterface getAdmin(final ServerName serverName) throws IOException;

  /**
   * Establishes a connection to the region server at the specified address, and returns
   * a region client protocol.
   *
   * @param serverName
   * @return ClientProtocol proxy for RegionServer
   * @throws IOException if a remote or network exception occurs
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  ClientService.BlockingInterface getClient(final ServerName serverName) throws IOException;

  /**
   * Establishes a connection to the region server at the specified address.
   * @param serverName
   * @param getMaster do we check if master is alive
   * @return proxy for HRegionServer
   * @throws IOException if a remote or network exception occurs
   * @deprecated You can pass master flag but nothing special is done.
   */
  @Deprecated
  AdminService.BlockingInterface getAdmin(final ServerName serverName, boolean getMaster)
      throws IOException;

  /**
   * Find region location hosting passed row
   * @param tableName table name
   * @param row Row to find.
   * @param reload If true do not use cache, otherwise bypass.
   * @return Location of row.
   * @throws IOException if a remote or network exception occurs
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  HRegionLocation getRegionLocation(TableName tableName, byte [] row,
    boolean reload)
  throws IOException;

  @Deprecated
  HRegionLocation getRegionLocation(byte[] tableName, byte [] row,
    boolean reload)
  throws IOException;

  /**
   * Process a mixed batch of Get, Put and Delete actions. All actions for a
   * RegionServer are forwarded in one RPC call.
   *
   *
   * @param actions The collection of actions.
   * @param tableName Name of the hbase table
   * @param pool thread pool for parallel execution
   * @param results An empty array, same size as list. If an exception is thrown,
   * you can test here for partial results, and to determine which actions
   * processed successfully.
   * @throws IOException if there are problems talking to META. Per-item
   * exceptions are stored in the results array.
   * @deprecated since 0.96 - Use {@link HTableInterface#batch} instead
   */
  @Deprecated
  void processBatch(List<? extends Row> actions, final TableName tableName,
      ExecutorService pool, Object[] results) throws IOException, InterruptedException;

  @Deprecated
  void processBatch(List<? extends Row> actions, final byte[] tableName,
      ExecutorService pool, Object[] results) throws IOException, InterruptedException;

  /**
   * Parameterized batch processing, allowing varying return types for different
   * {@link Row} implementations.
   * @deprecated since 0.96 - Use {@link HTableInterface#batchCallback} instead
   */
  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list,
      final TableName tableName,
      ExecutorService pool,
      Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException;

  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list,
      final byte[] tableName,
      ExecutorService pool,
      Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException;

  /**
   * @deprecated does nothing since since 0.99
   **/
  @Deprecated
  public void setRegionCachePrefetch(final TableName tableName,
      final boolean enable);

  /**
   * @deprecated does nothing since 0.99
   **/
  @Deprecated
  public void setRegionCachePrefetch(final byte[] tableName,
      final boolean enable);

  /**
   * @deprecated always return false since 0.99
   **/
  @Deprecated
  boolean getRegionCachePrefetch(final TableName tableName);

  /**
   * @deprecated always return false since 0.99
   **/
  @Deprecated
  boolean getRegionCachePrefetch(final byte[] tableName);

  /**
   * @return the number of region servers that are currently running
   * @throws IOException if a remote or network exception occurs
   * @deprecated This method will be changed from public to package protected.
   */
  @Deprecated
  int getCurrentNrHRS() throws IOException;

  /**
   * @param tableNames List of table names
   * @return HTD[] table metadata
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link Admin#getTableDescriptor(TableName)} instead.
   */
  @Deprecated
  HTableDescriptor[] getHTableDescriptorsByTableName(List<TableName> tableNames) throws IOException;

  @Deprecated
  HTableDescriptor[] getHTableDescriptors(List<String> tableNames) throws
      IOException;

  /**
   * @return true if this connection is closed
   */
  @Override
  boolean isClosed();


  /**
   * Clear any caches that pertain to server name <code>sn</code>.
   * @param sn A server name
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  void clearCaches(final ServerName sn);

  /**
   * This function allows HBaseAdmin and potentially others to get a shared MasterService
   * connection.
   * @return The shared instance. Never returns null.
   * @throws MasterNotRunningException
   * @deprecated Since 0.96.0
   */
  // TODO: Why is this in the public interface when the returned type is shutdown package access?
  @Deprecated
  MasterKeepAliveConnection getKeepAliveMasterService()
  throws MasterNotRunningException;

  /**
   * @param serverName
   * @return true if the server is known as dead, false otherwise.
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  boolean isDeadServer(ServerName serverName);

  /**
   * @return Nonce generator for this HConnection; may be null if disabled in configuration.
   * @deprecated internal method, do not use thru HConnection */
  @Deprecated
  public NonceGenerator getNonceGenerator();
}
