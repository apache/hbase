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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.ExecRPCInvoker;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

/**
 * <p>Used to communicate with a single HBase table.
 *
 * <p>This class is not thread safe for reads nor write.
 * 
 * <p>In case of writes (Put, Delete), the underlying write buffer can
 * be corrupted if multiple threads contend over a single HTable instance.
 * 
 * <p>In case of reads, some fields used by a Scan are shared among all threads.
 * The HTable implementation can either not contract to be safe in case of a Get
 *
 * <p>To access a table in a multi threaded environment, please consider
 * using the {@link HTablePool} class to create your HTable instances.
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
public class HTable implements HTableInterface {
  private static final Log LOG = LogFactory.getLog(HTable.class);
  private HConnection connection;
  private final byte [] tableName;
  private volatile Configuration configuration;
  private final ArrayList<Put> writeBuffer = new ArrayList<Put>();
  private long writeBufferSize;
  private boolean clearBufferOnFail;
  private boolean autoFlush;
  private long currentWriteBufferSize;
  protected int scannerCaching;
  private int maxKeyValueSize;
  private ExecutorService pool;  // For Multi
  private boolean closed;
  private int operationTimeout;
  private final boolean cleanupPoolOnClose; // shutdown the pool in close()
  private final boolean cleanupConnectionOnClose; // close the connection in close()

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
    this(conf, Bytes.toBytes(tableName));
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
  public HTable(Configuration conf, final byte [] tableName)
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
   * constructor when the HConnection instance is externally managed (does not close the connection
   * on {@link #close()}).
   * @param tableName Name of the table.
   * @param connection @param connection HConnection to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(byte[] tableName, HConnection connection) throws IOException {
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
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads,
        keepAliveTime, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        Threads.newDaemonThreadFactory("hbase-table"));
    pool.allowCoreThreadTimeOut(true);
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
    if (connection == null || connection.isClosed()) {
      throw new IllegalArgumentException("Connection is null or closed.");
    }
    this.tableName = tableName;
    this.cleanupPoolOnClose = this.cleanupConnectionOnClose = false;
    this.connection = connection;
    this.configuration = connection.getConfiguration();
    this.pool = pool;

    this.finishSetup();
  }

  /**
   * setup this HTable's parameter based on the passed configuration
   * @param conf
   */
  private void finishSetup() throws IOException {
    this.connection.locateRegion(tableName, HConstants.EMPTY_START_ROW);
    this.operationTimeout = HTableDescriptor.isMetaTable(tableName) ? HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT
        : this.configuration.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
            HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.writeBufferSize = this.configuration.getLong(
        "hbase.client.write.buffer", 2097152);
    this.clearBufferOnFail = true;
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = this.configuration.getInt(
        "hbase.client.scanner.caching", 1);

    this.maxKeyValueSize = this.configuration.getInt(
        "hbase.client.keyvalue.maxsize", -1);
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
    return isTableEnabled(Bytes.toBytes(tableName));
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
    return isTableEnabled(conf, Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  public static boolean isTableEnabled(Configuration conf,
      final byte[] tableName) throws IOException {
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
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link #getRegionLocation(byte [], boolean)} instead
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @param reload whether or not to reload information or just use cached
   * information
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
    return this.tableName;
  }

  /**
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return An HConnection instance.
   * @deprecated This method will be changed from public to package protected.
   */
  // TODO(tsuna): Remove this.  Unit tests shouldn't require public helpers.
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   * @deprecated Use {@link Scan#setCaching(int)} and {@link Scan#getCaching()}
   */
  public int getScannerCaching() {
    return scannerCaching;
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
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   * @deprecated Use {@link #getRegionLocations()} or {@link #getStartEndKeys()}
   */
  public Map<HRegionInfo, HServerAddress> getRegionsInfo() throws IOException {
    final Map<HRegionInfo, HServerAddress> regionMap =
      new TreeMap<HRegionInfo, HServerAddress>();

    final Map<HRegionInfo, ServerName> regionLocations = getRegionLocations();

    for (Map.Entry<HRegionInfo, ServerName> entry : regionLocations.entrySet()) {
      HServerAddress server = new HServerAddress();
      ServerName serverName = entry.getValue();
      if (serverName != null && serverName.getHostAndPort() != null) {
        server = new HServerAddress(Addressing.createInetSocketAddressFromHostAndPortStr(
            serverName.getHostAndPort()));
      }
      regionMap.put(entry.getKey(), server);
    }

    return regionMap;
  }

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    return MetaScanner.allTableRegions(getConfiguration(), this.connection, getTableName(), false);
  }

  /**
   * Get the corresponding regions for an arbitrary range of keys.
   * <p>
   * @param startRow Starting row in range, inclusive
   * @param endRow Ending row in range, exclusive
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
   * Save the passed region information and the table's regions
   * cache.
   * <p>
   * This is mainly useful for the MapReduce integration. You can call
   * {@link #deserializeRegionInfo deserializeRegionInfo}
   * to deserialize regions information from a
   * {@link DataInput}, then call this method to load them to cache.
   *
   * <pre>
   * {@code
   * HTable t1 = new HTable("foo");
   * FileInputStream fis = new FileInputStream("regions.dat");
   * DataInputStream dis = new DataInputStream(fis);
   *
   * Map<HRegionInfo, HServerAddress> hm = t1.deserializeRegionInfo(dis);
   * t1.prewarmRegionCache(hm);
   * }
   * </pre>
   * @param regionMap This piece of regions information will be loaded
   * to region cache.
   */
  public void prewarmRegionCache(Map<HRegionInfo, HServerAddress> regionMap) {
    this.connection.prewarmRegionCache(this.getTableName(), regionMap);
  }

  /**
   * Serialize the regions information of this table and output
   * to <code>out</code>.
   * <p>
   * This is mainly useful for the MapReduce integration. A client could
   * perform a large scan for all the regions for the table, serialize the
   * region info to a file. MR job can ship a copy of the meta for the table in
   * the DistributedCache.
   * <pre>
   * {@code
   * FileOutputStream fos = new FileOutputStream("regions.dat");
   * DataOutputStream dos = new DataOutputStream(fos);
   * table.serializeRegionInfo(dos);
   * dos.flush();
   * dos.close();
   * }
   * </pre>
   * @param out {@link DataOutput} to serialize this object into.
   * @throws IOException if a remote or network exception occurs
   */
  public void serializeRegionInfo(DataOutput out) throws IOException {
    Map<HRegionInfo, HServerAddress> allRegions = this.getRegionsInfo();
    // first, write number of regions
    out.writeInt(allRegions.size());
    for (Map.Entry<HRegionInfo, HServerAddress> es : allRegions.entrySet()) {
      es.getKey().write(out);
      es.getValue().write(out);
    }
  }

  /**
   * Read from <code>in</code> and deserialize the regions information.
   *
   * <p>It behaves similarly as {@link #getRegionsInfo getRegionsInfo}, except
   * that it loads the region map from a {@link DataInput} object.
   *
   * <p>It is supposed to be followed immediately by  {@link
   * #prewarmRegionCache prewarmRegionCache}.
   *
   * <p>
   * Please refer to {@link #prewarmRegionCache prewarmRegionCache} for usage.
   *
   * @param in {@link DataInput} object.
   * @return A map of HRegionInfo with its server address.
   * @throws IOException if an I/O exception occurs.
   */
  public Map<HRegionInfo, HServerAddress> deserializeRegionInfo(DataInput in)
  throws IOException {
    final Map<HRegionInfo, HServerAddress> allRegions =
      new TreeMap<HRegionInfo, HServerAddress>();

    // the first integer is expected to be the size of records
    int regionsCount = in.readInt();
    for (int i = 0; i < regionsCount; ++i) {
      HRegionInfo hri = new HRegionInfo();
      hri.readFields(in);
      HServerAddress hsa = new HServerAddress();
      hsa.readFields(in);
      allRegions.put(hri, hsa);
    }
    return allRegions;
  }

  /**
   * {@inheritDoc}
   */
   @Override
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     return new ServerCallable<Result>(connection, tableName, row, operationTimeout) {
       public Result call() throws IOException {
         return server.getClosestRowBefore(location.getRegionInfo().getRegionName(),
           row, family);
       }
     }.withRetries();
   }

   /**
    * {@inheritDoc}
    */
  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    if (scan.getCaching() <= 0) {
      scan.setCaching(getScannerCaching());
    }
    if (scan.isSmall()) {
      return new ClientSmallScanner(getConfiguration(), scan, getTableName(),
          this.connection);
    }
    return new ClientScanner(getConfiguration(), scan, getTableName(),
        this.connection);
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
    return new ServerCallable<Result>(connection, tableName, get.getRow(), operationTimeout) {
          public Result call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), get);
          }
        }.withRetries();
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
      throw new IOException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batch(final List<?extends Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    connection.processBatch(actions, tableName, pool, results);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] batch(final List<? extends Row> actions) throws InterruptedException, IOException {
    Object[] results = new Object[actions.size()];
    connection.processBatch(actions, tableName, pool, results);
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
    new ServerCallable<Boolean>(connection, tableName, delete.getRow(), operationTimeout) {
          public Boolean call() throws IOException {
            server.delete(location.getRegionInfo().getRegionName(), delete);
            return null; // FindBugs NP_BOOLEAN_RETURN_NULL
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    Object[] results = new Object[deletes.size()];
    try {
      connection.processBatch((List) deletes, tableName, pool, results);
    } catch (InterruptedException e) {
      throw new IOException(e);
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
  public void put(final Put put) throws IOException {
    doPut(put);
    if (autoFlush) {
      flushCommits();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final List<Put> puts) throws IOException {
    for (Put put : puts) {
      doPut(put);
    }
    if (autoFlush) {
      flushCommits();
    }
  }

  private void doPut(Put put) throws IOException{
    validatePut(put);
    writeBuffer.add(put);
    currentWriteBufferSize += put.heapSize();
    if (currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(final RowMutations rm) throws IOException {
    new ServerCallable<Void>(connection, tableName, rm.getRow(),
        operationTimeout) {
      public Void call() throws IOException {
        server.mutateRow(location.getRegionInfo().getRegionName(), rm);
        return null;
      }
    }.withRetries();
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
    return new ServerCallable<Result>(connection, tableName, append.getRow(), operationTimeout) {
          public Result call() throws IOException {
            return server.append(
                location.getRegionInfo().getRegionName(), append);
          }
        }.withRetries();
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
    return new ServerCallable<Result>(connection, tableName, increment.getRow(), operationTimeout) {
          public Result call() throws IOException {
            return server.increment(
                location.getRegionInfo().getRegionName(), increment);
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("column is null");
    }
    if (npe != null) {
      throw new IOException(
          "Invalid arguments to incrementColumnValue", npe);
    }
    return new ServerCallable<Long>(connection, tableName, row, operationTimeout) {
          public Long call() throws IOException {
            return server.incrementColumnValue(
                location.getRegionInfo().getRegionName(), row, family,
                qualifier, amount, writeToWAL);
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    return new ServerCallable<Boolean>(connection, tableName, row, operationTimeout) {
          public Boolean call() throws IOException {
            return server.checkAndPut(location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, put) ? Boolean.TRUE : Boolean.FALSE;
          }
        }.withRetries();
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    return new ServerCallable<Boolean>(connection, tableName, row, operationTimeout) {
          public Boolean call() throws IOException {
            return server.checkAndDelete(
                location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, delete)
            ? Boolean.TRUE : Boolean.FALSE;
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    return new ServerCallable<Boolean>(connection, tableName, get.getRow(), operationTimeout) {
          public Boolean call() throws IOException {
            return server.
                exists(location.getRegionInfo().getRegionName(), get);
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws IOException {
    try {
      Object[] results = new Object[writeBuffer.size()];
      try {
        this.connection.processBatch(writeBuffer, tableName, pool, results);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        // mutate list so that it is empty for complete success, or contains
        // only failed records results are returned in the same order as the
        // requests in list walk the list backwards, so we can remove from list
        // without impacting the indexes of earlier members
        for (int i = results.length - 1; i>=0; i--) {
          if (results[i] instanceof Result) {
            // successful Puts are removed from the list here.
            writeBuffer.remove(i);
          }
        }
      }
    } finally {
      if (clearBufferOnFail) {
        writeBuffer.clear();
        currentWriteBufferSize = 0;
      } else {
        // the write buffer was adjusted by processBatchOfPuts
        currentWriteBufferSize = 0;
        for (Put aPut : writeBuffer) {
          currentWriteBufferSize += aPut.heapSize();
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
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
  private void validatePut(final Put put) throws IllegalArgumentException{
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<KeyValue> list : put.getFamilyMap().values()) {
        for (KeyValue kv : list) {
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
  public RowLock lockRow(final byte [] row)
  throws IOException {
    return new ServerCallable<RowLock>(connection, tableName, row, operationTimeout) {
        public RowLock call() throws IOException {
          long lockId =
              server.lockRow(location.getRegionInfo().getRegionName(), row);
          return new RowLock(row,lockId);
        }
      }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void unlockRow(final RowLock rl)
  throws IOException {
    new ServerCallable<Boolean>(connection, tableName, rl.getRow(), operationTimeout) {
        public Boolean call() throws IOException {
          server.unlockRow(location.getRegionInfo().getRegionName(),
              rl.getLockId());
          return null; // FindBugs NP_BOOLEAN_RETURN_NULL
        }
      }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * See {@link #setAutoFlush(boolean, boolean)}
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   */
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlush(autoFlush, autoFlush);
  }

  /**
   * Turns 'auto-flush' on or off.
   * <p>
   * When enabled (default), {@link Put} operations don't get buffered/delayed
   * and are immediately executed. Failed operations are not retried. This is
   * slower but safer.
   * <p>
   * Turning off {@link #autoFlush} means that multiple {@link Put}s will be
   * accepted before any RPC is actually sent to do the write operations. If the
   * application dies before pending writes get flushed to HBase, data will be
   * lost.
   * <p>
   * When you turn {@link #autoFlush} off, you should also consider the
   * {@link #clearBufferOnFail} option. By default, asynchronous {@link Put}
   * requests will be retried on failure until successful. However, this can
   * pollute the writeBuffer and slow down batching performance. Additionally,
   * you may want to issue a number of Put requests and call
   * {@link #flushCommits()} as a barrier. In both use cases, consider setting
   * clearBufferOnFail to true to erase the buffer after {@link #flushCommits()}
   * has been called, regardless of success.
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   * @param clearBufferOnFail
   *          Whether to keep Put failures in the writeBuffer
   * @see #flushCommits
   */
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
   * Returns the write buffer.
   * @return The current write buffer.
   */
  public ArrayList<Put> getWriteBuffer() {
    return writeBuffer;
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
    HConnectionManager.execute(new HConnectable<Void>(HBaseConfiguration
        .create()) {
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
  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(
      Class<T> protocol, byte[] row) {
    return (T)Proxy.newProxyInstance(this.getClass().getClassLoader(),
        new Class[]{protocol},
        new ExecRPCInvoker(configuration,
            connection,
            protocol,
            tableName,
            row));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[],R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable)
      throws IOException, Throwable {

    final Map<byte[],R> results =  Collections.synchronizedMap(new TreeMap<byte[],R>(
        Bytes.BYTES_COMPARATOR));
    coprocessorExec(protocol, startKey, endKey, callable,
        new Batch.Callback<R>(){
      public void update(byte[] region, byte[] row, R value) {
        results.put(region, value);
      }
    });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable, Batch.Callback<R> callback)
      throws IOException, Throwable {

    // get regions covered by the row range
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);
    connection.processExecs(protocol, keys, tableName, pool, callable,
        callback);
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

}
