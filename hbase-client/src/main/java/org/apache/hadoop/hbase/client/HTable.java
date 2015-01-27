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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RegionCoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * <p>Used to communicate with a single HBase table.  An implementation of
 * {@link HTableInterface}.  Instances of this class can be constructed directly but it is
 * encouraged that users get instances via {@link HConnection} and {@link HConnectionManager}.
 * See {@link HConnectionManager} class comment for an example.
 *
 * <p>This class is not thread safe for reads nor write.
 *
 * <p>In case of writes (Put, Delete), the underlying write buffer can
 * be corrupted if multiple threads contend over a single HTable instance.
 *
 * <p>In case of reads, some fields used by a Scan are shared among all threads.
 * The HTable implementation can either not contract to be safe in case of a Get
 *
 * <p>Instances of HTable passed the same {@link Configuration} instance will
 * share connections to servers out on the cluster and to the zookeeper ensemble
 * as well as caches of region locations.  This is usually a *good* thing and it
 * is recommended to reuse the same configuration object for all your tables.
 * This happens because they will all share the same underlying
 * {@link HConnection} instance. See {@link HConnectionManager} for more on
 * how this mechanism works.
 *
 * <p>{@link HConnection} will read most of the
 * configuration it needs from the passed {@link Configuration} on initial
 * construction.  Thereafter, for settings such as
 * <code>hbase.client.pause</code>, <code>hbase.client.retries.number</code>,
 * and <code>hbase.client.rpc.maxattempts</code> updating their values in the
 * passed {@link Configuration} subsequent to {@link HConnection} construction
 * will go unnoticed.  To run with changed values, make a new
 * {@link HTable} passing a new {@link Configuration} instance that has the
 * new configuration.
 *
 * <p>Note that this class implements the {@link Closeable} interface. When a
 * HTable instance is no longer required, it *should* be closed in order to ensure
 * that the underlying resources are promptly released. Please note that the close
 * method can throw java.io.IOException that must be handled.
 *
 * @see HBaseAdmin for create, drop, list, enable and disable of tables.
 * @see HConnection
 * @see HConnectionManager
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HTable implements HTableInterface {
  private static final Log LOG = LogFactory.getLog(HTable.class);
  protected HConnection connection;
  private final TableName tableName;
  private volatile Configuration configuration;
  private TableConfiguration tableConfiguration;
  protected List<Row> writeAsyncBuffer = new LinkedList<Row>();
  private long writeBufferSize;
  private boolean clearBufferOnFail;
  private boolean autoFlush;
  protected long currentWriteBufferSize;
  protected int scannerCaching;
  private ExecutorService pool;  // For Multi
  private boolean closed;
  private int operationTimeout;
  private final boolean cleanupPoolOnClose; // shutdown the pool in close()
  private final boolean cleanupConnectionOnClose; // close the connection in close()

  /** The Async process for puts with autoflush set to false or multiputs */
  protected AsyncProcess<Object> ap;
  private RpcRetryingCallerFactory rpcCallerFactory;
  private RpcControllerFactory rpcControllerFactory;

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final String tableName)
  throws IOException {
    this(conf, TableName.valueOf(tableName));
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final byte[] tableName)
  throws IOException {
    this(conf, TableName.valueOf(tableName));
  }



  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName table name pojo
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final TableName tableName)
  throws IOException {
    this.tableName = tableName;
    this.cleanupPoolOnClose = this.cleanupConnectionOnClose = true;
    if (conf == null) {
      this.connection = null;
      return;
    }
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;

    this.pool = getDefaultExecutor(conf);
    this.finishSetup();
  }

  /**
   * Creates an object to access a HBase table. Shares zookeeper connection and other resources with
   * other HTable instances created with the same <code>connection</code> instance. Use this
   * constructor when the HConnection instance is externally managed.
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(TableName tableName, HConnection connection) throws IOException {
    this.tableName = tableName;
    this.cleanupPoolOnClose = true;
    this.cleanupConnectionOnClose = false;
    this.connection = connection;
    this.configuration = connection.getConfiguration();

    this.pool = getDefaultExecutor(this.configuration);
    this.finishSetup();
  }
   
  public static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
    int maxThreads = conf.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in HCM
    // we only create as many Runnables as there are region servers. It means
    // it also scales when new region servers are added.
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("htable"));
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.
   * Use this constructor when the ExecutorService is externally managed.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final byte[] tableName, final ExecutorService pool)
      throws IOException {
    this(conf, TableName.valueOf(tableName), pool);
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.
   * Use this constructor when the ExecutorService is externally managed.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final TableName tableName, final ExecutorService pool)
      throws IOException {
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;
    this.pool = pool;
    this.tableName = tableName;
    this.cleanupPoolOnClose = false;
    this.cleanupConnectionOnClose = true;

    this.finishSetup();
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>connection</code> instance.
   * Use this constructor when the ExecutorService and HConnection instance are
   * externally managed.
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(final byte[] tableName, final HConnection connection,
      final ExecutorService pool) throws IOException {
    this(TableName.valueOf(tableName), connection, pool);
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>connection</code> instance.
   * Use this constructor when the ExecutorService and HConnection instance are
   * externally managed.
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(TableName tableName, final HConnection connection,
      final ExecutorService pool) throws IOException {
    this(tableName, connection, null, null, null, pool);
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>connection</code> instance.
   * Use this constructor when the ExecutorService and HConnection instance are
   * externally managed.
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @param tableConfig table configuration
   * @param rpcCallerFactory RPC caller factory
   * @param rpcControllerFactory RPC controller factory
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(TableName tableName, final HConnection connection,
      final TableConfiguration tableConfig,
      final RpcRetryingCallerFactory rpcCallerFactory,
      final RpcControllerFactory rpcControllerFactory,
      final ExecutorService pool) throws IOException {
    if (connection == null || connection.isClosed()) {
      throw new IllegalArgumentException("Connection is null or closed.");
    }
    this.tableName = tableName;
    this.connection = connection;
    this.configuration = connection.getConfiguration();
    this.tableConfiguration = tableConfig;
    this.cleanupPoolOnClose = this.cleanupConnectionOnClose = false;
    this.pool = pool;

    this.rpcCallerFactory = rpcCallerFactory;
    this.rpcControllerFactory = rpcControllerFactory;

    this.finishSetup();
  }

  /**
   * For internal testing.
   */
  protected HTable(){
    tableName = null;
    tableConfiguration = new TableConfiguration();
    cleanupPoolOnClose = false;
    cleanupConnectionOnClose = false;
  }

  /**
   * @return maxKeyValueSize from configuration.
   */
  public static int getMaxKeyValueSize(Configuration conf) {
    return conf.getInt("hbase.client.keyvalue.maxsize", -1);
  }

  /**
   * setup this HTable's parameter based on the passed configuration
   */
  private void finishSetup() throws IOException {
    if (tableConfiguration == null) {
      tableConfiguration = new TableConfiguration(configuration);
    }
    this.operationTimeout = tableName.isSystemTable() ?
      tableConfiguration.getMetaOperationTimeout() : tableConfiguration.getOperationTimeout();
    this.writeBufferSize = tableConfiguration.getWriteBufferSize();
    this.clearBufferOnFail = true;
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = tableConfiguration.getScannerCaching();

    if (this.rpcCallerFactory == null) {
      this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(configuration,
        this.connection.getStatisticsTracker());
    }
    if (this.rpcControllerFactory == null) {
      this.rpcControllerFactory = RpcControllerFactory.instantiate(configuration);
    }

    ap = new AsyncProcess<Object>(connection, tableName, pool, null, configuration,
      rpcCallerFactory, rpcControllerFactory);

    this.closed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Tells whether or not a table is enabled or not. This method creates a
   * new HBase configuration, so it might make your unit tests fail due to
   * incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	* @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not. This method creates a
   * new HBase configuration, so it might make your unit tests fail due to
   * incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	* @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not. This method creates a
   * new HBase configuration, so it might make your unit tests fail due to
   * incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(TableName tableName) throws IOException {
    return isTableEnabled(HBaseConfiguration.create(), tableName);
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	 * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf, String tableName)
  throws IOException {
    return isTableEnabled(conf, TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	 * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf, byte[] tableName)
  throws IOException {
    return isTableEnabled(conf, TableName.valueOf(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link HBaseAdmin#isTableEnabled(org.apache.hadoop.hbase.TableName tableName)}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf,
      final TableName tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.isTableEnabled(tableName);
      }
    });
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    return connection.getRegionLocation(tableName, Bytes.toBytes(row), false);
  }

  /**
   * Finds the region on which the given row is being served. Does not reload the cache.
   * @param row Row to find.
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @param reload true to reload information or false to use cached information
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final byte [] row, boolean reload)
  throws IOException {
    return connection.getRegionLocation(tableName, row, reload);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte [] getTableName() {
    return this.tableName.getName();
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  /**
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return An HConnection instance.
   * @deprecated This method will be changed from public to package protected.
   */
  // TODO(tsuna): Remove this.  Unit tests shouldn't require public helpers.
  @Deprecated
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   * @deprecated Use {@link Scan#setCaching(int)} and {@link Scan#getCaching()}
   */
  @Deprecated
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * Kept in 0.96 for backward compatibility
   * @deprecated  since 0.96. This is an internal buffer that should not be read nor write.
   */
  @Deprecated
  public List<Row> getWriteBuffer() {
    return writeAsyncBuffer;
  }

  /**
   * Sets the number of rows that a scanner will fetch at once.
   * <p>
   * This will override the value specified by
   * {@code hbase.client.scanner.caching}.
   * Increasing this value will reduce the amount of work needed each time
   * {@code next()} is called on a scanner, at the expense of memory use
   * (since more rows will need to be maintained in memory by the scanners).
   * @param scannerCaching the number of rows a scanner will fetch at once.
   * @deprecated Use {@link Scan#setCaching(int)}
   */
  @Deprecated
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return new UnmodifyableHTableDescriptor(
      this.connection.getHTableDescriptor(this.tableName));
  }

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte [][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    NavigableMap<HRegionInfo, ServerName> regions = getRegionLocations();
    final List<byte[]> startKeyList = new ArrayList<byte[]>(regions.size());
    final List<byte[]> endKeyList = new ArrayList<byte[]>(regions.size());

    for (HRegionInfo region : regions.keySet()) {
      startKeyList.add(region.getStartKey());
      endKeyList.add(region.getEndKey());
    }

    return new Pair<byte [][], byte [][]>(
      startKeyList.toArray(new byte[startKeyList.size()][]),
      endKeyList.toArray(new byte[endKeyList.size()][]));
  }

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    // TODO: Odd that this returns a Map of HRI to SN whereas getRegionLocation, singular, returns an HRegionLocation.
    return MetaScanner.allTableRegions(getConfiguration(), this.connection, getName(), false);
  }

  /**
   * Get the corresponding regions for an arbitrary range of keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range, exclusive
   * @return A list of HRegionLocations corresponding to the regions that
   * contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  public List<HRegionLocation> getRegionsInRange(final byte [] startKey,
    final byte [] endKey) throws IOException {
    return getRegionsInRange(startKey, endKey, false);
  }

  /**
   * Get the corresponding regions for an arbitrary range of keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range, exclusive
   * @param reload true to reload information or false to use cached information
   * @return A list of HRegionLocations corresponding to the regions that
   * contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  public List<HRegionLocation> getRegionsInRange(final byte [] startKey,
      final byte [] endKey, final boolean reload) throws IOException {
    return getKeysAndRegionsInRange(startKey, endKey, false, reload).getSecond();
  }

  /**
   * Get the corresponding start keys and regions for an arbitrary range of
   * keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range
   * @param includeEndKey true if endRow is inclusive, false if exclusive
   * @return A pair of list of start keys and list of HRegionLocations that
   *         contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(
      final byte[] startKey, final byte[] endKey, final boolean includeEndKey)
      throws IOException {
    return getKeysAndRegionsInRange(startKey, endKey, includeEndKey, false);
  }

  /**
   * Get the corresponding start keys and regions for an arbitrary range of
   * keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range
   * @param includeEndKey true if endRow is inclusive, false if exclusive
   * @param reload true to reload information or false to use cached information
   * @return A pair of list of start keys and list of HRegionLocations that
   *         contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  private Pair<List<byte[]>, List<HRegionLocation>> getKeysAndRegionsInRange(
      final byte[] startKey, final byte[] endKey, final boolean includeEndKey,
      final boolean reload) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey,HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException(
        "Invalid range: " + Bytes.toStringBinary(startKey) +
        " > " + Bytes.toStringBinary(endKey));
    }
    List<byte[]> keysInRange = new ArrayList<byte[]>();
    List<HRegionLocation> regionsInRange = new ArrayList<HRegionLocation>();
    byte[] currentKey = startKey;
    do {
      HRegionLocation regionLocation = getRegionLocation(currentKey, reload);
      keysInRange.add(currentKey);
      regionsInRange.add(regionLocation);
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
        && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0
            || (includeEndKey && Bytes.compareTo(currentKey, endKey) == 0)));
    return new Pair<List<byte[]>, List<HRegionLocation>>(keysInRange,
        regionsInRange);
  }

  /**
   * {@inheritDoc}
   */
   @Override
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     RegionServerCallable<Result> callable = new RegionServerCallable<Result>(this.connection,
         tableName, row) {
       public Result call() throws IOException {
            return ProtobufUtil.getRowOrBefore(getStub(), getLocation().getRegionInfo()
                .getRegionName(), row, family, rpcControllerFactory.newController());
          }
     };
    return rpcCallerFactory.<Result> newCaller().callWithRetries(callable, this.operationTimeout);
  }

   /**
    * {@inheritDoc}
    */
  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    if (scan.getBatch() > 0 && scan.isSmall()) {
      throw new IllegalArgumentException("Small scan should not be used with batching");
    }
    if (scan.getCaching() <= 0) {
      scan.setCaching(getScannerCaching());
    }

    if (scan.isReversed()) {
      if (scan.isSmall()) {
        return new ClientSmallReversedScanner(getConfiguration(), scan, getName(),
            this.connection);
      } else {
        return new ReversedClientScanner(getConfiguration(), scan, getName(), this.connection);
      }
    }

    if (scan.isSmall()) {
      return new ClientSmallScanner(getConfiguration(), scan, getName(), this.connection);
    } else {
      return new ClientScanner(getConfiguration(), scan, getName(), this.connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(final Get get) throws IOException {
    // have to instanatiate this and set the priority here since in protobuf util we don't pass in
    // the tablename... an unfortunate side-effect of public interfaces :-/ In 0.99+ we put all the
    // logic back into HTable
    final PayloadCarryingRpcController controller = rpcControllerFactory.newController();
    controller.setPriority(tableName);
    RegionServerCallable<Result> callable =
        new RegionServerCallable<Result>(this.connection, getName(), get.getRow()) {
          public Result call() throws IOException {
            return ProtobufUtil.get(getStub(), getLocation().getRegionInfo().getRegionName(), get,
              controller);
          }
        };
    return rpcCallerFactory.<Result> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (gets.size() == 1) {
      return new Result[]{get(gets.get(0))};
    }
    try {
      Object [] r1 = batch((List)gets);

      // translate.
      Result [] results = new Result[r1.length];
      int i=0;
      for (Object o : r1) {
        // batch ensures if there is a failure we get an exception instead
        results[i++] = (Result) o;
      }

      return results;
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batch(final List<?extends Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    batchCallback(actions, results, null);
  }

  /**
   * {@inheritDoc}
   * @deprecated If any exception is thrown by one of the actions, there is no way to
   * retrieve the partially executed results. Use {@link #batch(List, Object[])} instead.
   */
  @Override
  public Object[] batch(final List<? extends Row> actions)
     throws InterruptedException, IOException {
    return batchCallback(actions, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R> void batchCallback(
      final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    connection.processBatchCallback(actions, tableName, pool, results, callback);
  }

  /**
   * {@inheritDoc}
   * @deprecated If any exception is thrown by one of the actions, there is no way to
   * retrieve the partially executed results. Use
   * {@link #batchCallback(List, Object[], org.apache.hadoop.hbase.client.coprocessor.Batch.Callback)}
   * instead.
   */
  @Override
  public <R> Object[] batchCallback(
    final List<? extends Row> actions, final Batch.Callback<R> callback) throws IOException,
      InterruptedException {
    Object[] results = new Object[actions.size()];
    batchCallback(actions, results, callback);
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
    RegionServerCallable<Boolean> callable = new RegionServerCallable<Boolean>(connection,
        tableName, delete.getRow()) {
      public Boolean call() throws IOException {
        try {
          MutateRequest request = RequestConverter.buildMutateRequest(
            getLocation().getRegionInfo().getRegionName(), delete);
              PayloadCarryingRpcController controller = rpcControllerFactory.newController();
              controller.setPriority(tableName);
              MutateResponse response = getStub().mutate(controller, request);
          return Boolean.valueOf(response.getProcessed());
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
      }
    };
    rpcCallerFactory.<Boolean> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    Object[] results = new Object[deletes.size()];
    try {
      batch(deletes, results);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    } finally {
      // mutate list so that it is empty for complete success, or contains only failed records
      // results are returned in the same order as the requests in list
      // walk the list backwards, so we can remove from list without impacting the indexes of earlier members
      for (int i = results.length - 1; i>=0; i--) {
        // if result is not null, it succeeded
        if (results[i] instanceof Result) {
          deletes.remove(i);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final Put put)
      throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    doPut(put);
    if (autoFlush) {
      flushCommits();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final List<Put> puts)
      throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    for (Put put : puts) {
      doPut(put);
    }
    if (autoFlush) {
      flushCommits();
    }
  }


  /**
   * Add the put to the buffer. If the buffer is already too large, sends the buffer to the
   *  cluster.
   * @throws RetriesExhaustedWithDetailsException if there is an error on the cluster.
   * @throws InterruptedIOException if we were interrupted.
   */
  private void doPut(Put put) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    if (ap.hasError()){
      writeAsyncBuffer.add(put);
      backgroundFlushCommits(true);
    }

    validatePut(put);

    currentWriteBufferSize += put.heapSize();
    writeAsyncBuffer.add(put);

    while (currentWriteBufferSize > writeBufferSize) {
      backgroundFlushCommits(false);
    }
  }


  /**
   * Send the operations in the buffer to the servers. Does not wait for the server's answer.
   * If the is an error (max retried reach from a previous flush or bad operation), it tries to
   * send all operations in the buffer and sends an exception.
   * @param synchronous - if true, sends all the writes and wait for all of them to finish before
   *                     returning.
   */
  private void backgroundFlushCommits(boolean synchronous) throws
      InterruptedIOException, RetriesExhaustedWithDetailsException {

    try {
      do {
        ap.submit(writeAsyncBuffer, true);
      } while (synchronous && !writeAsyncBuffer.isEmpty());

      if (synchronous) {
        ap.waitUntilDone();
      }

      if (ap.hasError()) {
        LOG.debug(tableName + ": One or more of the operations have failed -" +
            " waiting for all operation in progress to finish (successfully or not)");
        while (!writeAsyncBuffer.isEmpty()) {
          ap.submit(writeAsyncBuffer, true);
        }
        ap.waitUntilDone();

        if (!clearBufferOnFail) {
          // if clearBufferOnFailed is not set, we're supposed to keep the failed operation in the
          //  write buffer. This is a questionable feature kept here for backward compatibility
          writeAsyncBuffer.addAll(ap.getFailedOperations());
        }
        RetriesExhaustedWithDetailsException e = ap.getErrors();
        ap.clearErrors();
        throw e;
      }
    } finally {
      currentWriteBufferSize = 0;
      for (Row mut : writeAsyncBuffer) {
        if (mut instanceof Mutation) {
          currentWriteBufferSize += ((Mutation) mut).heapSize();
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(final RowMutations rm) throws IOException {
    RegionServerCallable<Void> callable =
        new RegionServerCallable<Void>(connection, getName(), rm.getRow()) {
      public Void call() throws IOException {
        try {
          RegionAction.Builder regionMutationBuilder = RequestConverter.buildRegionAction(
            getLocation().getRegionInfo().getRegionName(), rm);
          regionMutationBuilder.setAtomic(true);
          MultiRequest request =
            MultiRequest.newBuilder().addRegionAction(regionMutationBuilder.build()).build();
          PayloadCarryingRpcController pcrc = rpcControllerFactory.newController();
          pcrc.setPriority(tableName);
          getStub().multi(pcrc, request);
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
        return null;
      }
    };
    rpcCallerFactory.<Void> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result append(final Append append) throws IOException {
    if (append.numFamilies() == 0) {
      throw new IOException(
          "Invalid arguments to append, no columns specified");
    }

    NonceGenerator ng = this.connection.getNonceGenerator();
    final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
    RegionServerCallable<Result> callable =
      new RegionServerCallable<Result>(this.connection, getName(), append.getRow()) {
        public Result call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildMutateRequest(
              getLocation().getRegionInfo().getRegionName(), append, nonceGroup, nonce);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            if (!response.hasResult()) return null;
            return ProtobufUtil.toResult(response.getResult(), rpcController.cellScanner());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Result> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(final Increment increment) throws IOException {
    if (!increment.hasFamilies()) {
      throw new IOException(
          "Invalid arguments to increment, no columns specified");
    }
    NonceGenerator ng = this.connection.getNonceGenerator();
    final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
    RegionServerCallable<Result> callable = new RegionServerCallable<Result>(this.connection,
        getName(), increment.getRow()) {
      public Result call() throws IOException {
        try {
          MutateRequest request = RequestConverter.buildMutateRequest(
            getLocation().getRegionInfo().getRegionName(), increment, nonceGroup, nonce);
          PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
          rpcController.setPriority(getTableName());
          MutateResponse response = getStub().mutate(rpcController, request);
          return ProtobufUtil.toResult(response.getResult(), rpcController.cellScanner());
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
      }
    };
    return rpcCallerFactory.<Result> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, Durability.SYNC_WAL);
  }

  /**
   * @deprecated Use {@link #incrementColumnValue(byte[], byte[], byte[], long, Durability)}
   */
  @Deprecated
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount,
      writeToWAL? Durability.SKIP_WAL: Durability.USE_DEFAULT);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final Durability durability)
  throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("family is null");
    } else if (qualifier == null) {
      npe = new NullPointerException("qualifier is null");
    }
    if (npe != null) {
      throw new IOException(
          "Invalid arguments to incrementColumnValue", npe);
    }

    NonceGenerator ng = this.connection.getNonceGenerator();
    final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
    RegionServerCallable<Long> callable =
      new RegionServerCallable<Long>(connection, getName(), row) {
        public Long call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildIncrementRequest(
              getLocation().getRegionInfo().getRegionName(), row, family,
              qualifier, amount, durability, nonceGroup, nonce);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            Result result =
              ProtobufUtil.toResult(response.getResult(), rpcController.cellScanner());
            return Long.valueOf(Bytes.toLong(result.getValue(family, qualifier)));
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Long> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    RegionServerCallable<Boolean> callable =
      new RegionServerCallable<Boolean>(connection, getName(), row) {
        public Boolean call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildMutateRequest(
              getLocation().getRegionInfo().getRegionName(), row, family, qualifier,
                new BinaryComparator(value), CompareType.EQUAL, put);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            return Boolean.valueOf(response.getProcessed());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Boolean> newCaller().callWithRetries(callable, this.operationTimeout);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    RegionServerCallable<Boolean> callable =
      new RegionServerCallable<Boolean>(connection, getName(), row) {
        public Boolean call() throws IOException {
          try {
            MutateRequest request = RequestConverter.buildMutateRequest(
              getLocation().getRegionInfo().getRegionName(), row, family, qualifier,
                new BinaryComparator(value), CompareType.EQUAL, delete);
            PayloadCarryingRpcController rpcController = rpcControllerFactory.newController();
            rpcController.setPriority(getTableName());
            MutateResponse response = getStub().mutate(rpcController, request);
            return Boolean.valueOf(response.getProcessed());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      };
    return rpcCallerFactory.<Boolean> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndMutate(final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final byte [] value, final RowMutations rm)
  throws IOException {
    RegionServerCallable<Boolean> callable =
        new RegionServerCallable<Boolean>(connection, getName(), row) {
          @Override
          public Boolean call() throws IOException {
            PayloadCarryingRpcController controller = rpcControllerFactory.newController();
            controller.setPriority(tableName);
            try {
              CompareType compareType = CompareType.valueOf(compareOp.name());
              MultiRequest request = RequestConverter.buildMutateRequest(
                  getLocation().getRegionInfo().getRegionName(), row, family, qualifier,
                  new BinaryComparator(value), compareType, rm);
              ClientProtos.MultiResponse response = getStub().multi(controller, request);
              return Boolean.valueOf(response.getProcessed());
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        };
    return rpcCallerFactory.<Boolean> newCaller().callWithRetries(callable, this.operationTimeout);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    get.setCheckExistenceOnly(true);
    Result r = get(get);
    assert r.getExists() != null;
    return r.getExists();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean[] exists(final List<Get> gets) throws IOException {
    if (gets.isEmpty()) return new Boolean[]{};
    if (gets.size() == 1) return new Boolean[]{exists(gets.get(0))};

    for (Get g: gets){
      g.setCheckExistenceOnly(true);
    }

    Object[] r1;
    try {
      r1 = batch(gets);
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }

    // translate.
    Boolean[] results = new Boolean[r1.length];
    int i = 0;
    for (Object o : r1) {
      // batch ensures if there is a failure we get an exception instead
      results[i++] = ((Result)o).getExists();
    }

    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
    // As we can have an operation in progress even if the buffer is empty, we call
    //  backgroundFlushCommits at least one time.
    backgroundFlushCommits(true);
  }

  /**
   * Process a mixed batch of Get, Put and Delete actions. All actions for a
   * RegionServer are forwarded in one RPC call. Queries are executed in parallel.
   *
   * @param list The collection of actions.
   * @param results An empty array, same size as list. If an exception is thrown,
   * you can test here for partial results, and to determine which actions
   * processed successfully.
   * @throws IOException if there are problems talking to META. Per-item
   * exceptions are stored in the results array.
   */
  public <R> void processBatchCallback(
    final List<? extends Row> list, final Object[] results, final Batch.Callback<R> callback)
    throws IOException, InterruptedException {
    this.batchCallback(list, results, callback);
  }


  /**
   * Parameterized batch processing, allowing varying return types for different
   * {@link Row} implementations.
   */
  public void processBatch(final List<? extends Row> list, final Object[] results)
    throws IOException, InterruptedException {

    this.processBatchCallback(list, results, null);
  }


  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    flushCommits();
    if (cleanupPoolOnClose) {
      this.pool.shutdown();
    }
    if (cleanupConnectionOnClose) {
      if (this.connection != null) {
        this.connection.close();
      }
    }
    this.closed = true;
  }

  // validate for well-formedness
  public void validatePut(final Put put) throws IllegalArgumentException {
    validatePut(put, tableConfiguration.getMaxKeyValueSize());
  }

  // validate for well-formedness
  public static void validatePut(Put put, int maxKeyValueSize) throws IllegalArgumentException {
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<Cell> list : put.getFamilyCellMap().values()) {
        for (Cell cell : list) {
          // KeyValue v1 expectation.  Cast for now.
          KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
          if (kv.getLength() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * {@inheritDoc}
   */
  @Deprecated
  @Override
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlush(autoFlush, autoFlush);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    setAutoFlush(autoFlush, clearBufferOnFail);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    this.autoFlush = autoFlush;
    this.clearBufferOnFail = autoFlush || clearBufferOnFail;
  }

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  @Override
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.writeBufferSize = writeBufferSize;
    if(currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * The pool is used for mutli requests for this HTable
   * @return the pool used for mutli
   */
  ExecutorService getPool() {
    return this.pool;
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final byte[] tableName,
      final boolean enable) throws IOException {
    setRegionCachePrefetch(TableName.valueOf(tableName), enable);
  }

  public static void setRegionCachePrefetch(
      final TableName tableName,
      final boolean enable) throws IOException {
    HConnectionManager.execute(new HConnectable<Void>(HBaseConfiguration.create()) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param conf The Configuration object to use.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final Configuration conf,
      final byte[] tableName, final boolean enable) throws IOException {
    setRegionCachePrefetch(conf, TableName.valueOf(tableName), enable);
  }

  public static void setRegionCachePrefetch(final Configuration conf,
      final TableName tableName,
      final boolean enable) throws IOException {
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param conf The Configuration object to use.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) throws IOException {
    return getRegionCachePrefetch(conf, TableName.valueOf(tableName));
  }

  public static boolean getRegionCachePrefetch(final Configuration conf,
      final TableName tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName) throws IOException {
    return getRegionCachePrefetch(TableName.valueOf(tableName));
  }

  public static boolean getRegionCachePrefetch(
      final TableName tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(
        HBaseConfiguration.create()) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
  }

  /**
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public void clearRegionCache() {
    this.connection.clearRegionCache();
  }

  /**
   * {@inheritDoc}
   */
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return new RegionCoprocessorRpcChannel(connection, tableName, row, rpcCallerFactory,
        rpcControllerFactory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> Map<byte[],R> coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable)
      throws ServiceException, Throwable {
    final Map<byte[],R> results =  Collections.synchronizedMap(
        new TreeMap<byte[], R>(Bytes.BYTES_COMPARATOR));
    coprocessorService(service, startKey, endKey, callable, new Batch.Callback<R>() {
      public void update(byte[] region, byte[] row, R value) {
        if (region != null) {
          results.put(region, value);
        }
      }
    });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> void coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable,
      final Batch.Callback<R> callback) throws ServiceException, Throwable {

    // get regions covered by the row range
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);

    Map<byte[],Future<R>> futures =
        new TreeMap<byte[],Future<R>>(Bytes.BYTES_COMPARATOR);
    for (final byte[] r : keys) {
      final RegionCoprocessorRpcChannel channel =
          new RegionCoprocessorRpcChannel(connection, tableName, r, rpcCallerFactory,
              rpcControllerFactory);
      Future<R> future = pool.submit(
          new Callable<R>() {
            public R call() throws Exception {
              T instance = ProtobufUtil.newServiceStub(service, channel);
              R result = callable.call(instance);
              byte[] region = channel.getLastRegion();
              if (callback != null) {
                callback.update(region, r, result);
              }
              return result;
            }
          });
      futures.put(r, future);
    }
    for (Map.Entry<byte[],Future<R>> e : futures.entrySet()) {
      try {
        e.getValue().get();
      } catch (ExecutionException ee) {
        LOG.warn("Error calling coprocessor service " + service.getName() + " for row "
            + Bytes.toStringBinary(e.getKey()), ee);
        throw ee.getCause();
      } catch (InterruptedException ie) {
        throw new InterruptedIOException("Interrupted calling coprocessor service " + service.getName()
            + " for row " + Bytes.toStringBinary(e.getKey()))
            .initCause(ie);
      }
    }
  }

  private List<byte[]> getStartKeysInRange(byte[] start, byte[] end)
  throws IOException {
    if (start == null) {
      start = HConstants.EMPTY_START_ROW;
    }
    if (end == null) {
      end = HConstants.EMPTY_END_ROW;
    }
    return getKeysAndRegionsInRange(start, end, true).getFirst();
  }

  public void setOperationTimeout(int operationTimeout) {
    this.operationTimeout = operationTimeout;
  }

  public int getOperationTimeout() {
    return operationTimeout;
  }

  @Override
  public String toString() {
    return tableName + ";" + connection;
  }

  /**
   * Run basic test.
   * @param args Pass table name and row and will get the content.
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    HTable t = new HTable(HBaseConfiguration.create(), args[0]);
    try {
      System.out.println(t.get(new Get(Bytes.toBytes(args[1]))));
    } finally {
      t.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    final Map<byte[], R> results = Collections.synchronizedMap(new TreeMap<byte[], R>(
        Bytes.BYTES_COMPARATOR));
    batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype,
        new Callback<R>() {

          @Override
          public void update(byte[] region, byte[] row, R result) {
            if (region != null) {
              results.put(region, result);
            }
          }
        });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Message> void batchCoprocessorService(
      final Descriptors.MethodDescriptor methodDescriptor, final Message request,
      byte[] startKey, byte[] endKey, final R responsePrototype, final Callback<R> callback)
      throws ServiceException, Throwable {

    // get regions covered by the row range
    Pair<List<byte[]>, List<HRegionLocation>> keysAndRegions =
        getKeysAndRegionsInRange(startKey, endKey, true);
    List<byte[]> keys = keysAndRegions.getFirst();
    List<HRegionLocation> regions = keysAndRegions.getSecond();

    // check if we have any calls to make
    if (keys.isEmpty()) {
      LOG.info("No regions were selected by key range start=" + Bytes.toStringBinary(startKey) +
          ", end=" + Bytes.toStringBinary(endKey));
      return;
    }

    List<RegionCoprocessorServiceExec> execs = new ArrayList<RegionCoprocessorServiceExec>();
    final Map<byte[], RegionCoprocessorServiceExec> execsByRow =
        new TreeMap<byte[], RegionCoprocessorServiceExec>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < keys.size(); i++) {
      final byte[] rowKey = keys.get(i);
      final byte[] region = regions.get(i).getRegionInfo().getRegionName();
      RegionCoprocessorServiceExec exec =
          new RegionCoprocessorServiceExec(region, rowKey, methodDescriptor, request);
      execs.add(exec);
      execsByRow.put(rowKey, exec);
    }

    // tracking for any possible deserialization errors on success callback
    // TODO: it would be better to be able to reuse AsyncProcess.BatchErrors here
    final List<Throwable> callbackErrorExceptions = new ArrayList<Throwable>();
    final List<Row> callbackErrorActions = new ArrayList<Row>();
    final List<String> callbackErrorServers = new ArrayList<String>();

    AsyncProcess<ClientProtos.CoprocessorServiceResult> asyncProcess =
        new AsyncProcess<ClientProtos.CoprocessorServiceResult>(connection, tableName, pool,
            new AsyncProcess.AsyncProcessCallback<ClientProtos.CoprocessorServiceResult>() {
          @SuppressWarnings("unchecked")
          @Override
          public void success(int originalIndex, byte[] region, Row row,
              ClientProtos.CoprocessorServiceResult serviceResult) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Received result for endpoint " + methodDescriptor.getFullName() +
                " call #" + originalIndex + ": region=" + Bytes.toStringBinary(region) +
                ", row=" + Bytes.toStringBinary(row.getRow()) +
                ", value=" + serviceResult.getValue().getValue());
            }
            try {
              callback.update(region, row.getRow(),
                (R) responsePrototype.newBuilderForType().mergeFrom(
                  serviceResult.getValue().getValue()).build());
            } catch (InvalidProtocolBufferException e) {
              LOG.error("Unexpected response type from endpoint " + methodDescriptor.getFullName(),
                e);
              callbackErrorExceptions.add(e);
              callbackErrorActions.add(row);
              callbackErrorServers.add("null");
            }
          }

          @Override
          public boolean failure(int originalIndex, byte[] region, Row row, Throwable t) {
            RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) row;
            LOG.error("Failed calling endpoint " + methodDescriptor.getFullName() + ": region="
                + Bytes.toStringBinary(exec.getRegion()), t);
            return true;
          }

          @Override
          public boolean retriableFailure(int originalIndex, Row row, byte[] region,
              Throwable exception) {
            RegionCoprocessorServiceExec exec = (RegionCoprocessorServiceExec) row;
            LOG.error("Failed calling endpoint " + methodDescriptor.getFullName() + ": region="
                + Bytes.toStringBinary(exec.getRegion()), exception);
            return !(exception instanceof DoNotRetryIOException);
          }
        },
        configuration, rpcCallerFactory, rpcControllerFactory);

    asyncProcess.submitAll(execs);
    asyncProcess.waitUntilDone();

    if (asyncProcess.hasError()) {
      throw asyncProcess.getErrors();
    } else if (!callbackErrorExceptions.isEmpty()) {
      throw new RetriesExhaustedWithDetailsException(callbackErrorExceptions, callbackErrorActions,
        callbackErrorServers);
    }
  }
}
