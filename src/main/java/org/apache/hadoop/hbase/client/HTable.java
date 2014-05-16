/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.coprocessor.endpoints.HTableEndpointClient;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpoint;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointClient;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.PreloadThreadPool;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hbase.util.Writables;

import com.google.common.base.Preconditions;

/**
 * Used to communicate with a single HBase table.
 *
 * This class is not thread safe for writes.
 * Gets, puts, and deletes take out a row lock for the duration
 * of their operation.  Scans (currently) do not respect
 * row locking.
 *
 * See {@link HBaseAdmin} to create, drop, list, enable and disable tables.
 */
public class HTable implements HTableInterface, IEndpointClient {
  private final HConnection connection;
  protected final StringBytes tableName;
  protected final int scannerTimeout;
  private volatile Configuration configuration;
  private final ArrayList<Put> writeBuffer = new ArrayList<Put>();
  private long writeBufferSize;
  private boolean clearBufferOnFail;
  private boolean autoFlush;
  private long currentWriteBufferSize;
  protected int scannerCaching;
  private int maxKeyValueSize;
  private HBaseRPCOptions options;
  private boolean recordClientContext = false;

  private long maxScannerResultSize;
  private HTableAsync hta;
  private boolean doAsync;

  private IEndpointClient endpointClient;

  // Share this multiaction thread pool across all the HTable instance;
  // The total number of threads will be bounded #HTable * #RegionServer.
  static ExecutorService multiActionThreadPool = new ThreadPoolExecutor(1, Integer.MAX_VALUE,
      60, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>(),
      new DaemonThreadFactory("htable-thread-"));

  static ListeningExecutorService listeningMultiActionPool = MoreExecutors.listeningDecorator(multiActionThreadPool);
  static {
    ((ThreadPoolExecutor)multiActionThreadPool).allowCoreThreadTimeOut(true);
  }


  public void initHTableAsync() throws IOException {
    if (doAsync && hta == null) {
      hta = new HTableAsync(this);
    }
  }

  /**
   * Creates an object to access a HBase table. DO NOT USE THIS CONSTRUCTOR.
   * It will make your unit tests fail due to incorrect ZK client port.
   *
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public HTable(final String tableName)
  throws IOException {
    this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table. DO NOT USE THIS CONSTRUCTOR.
   * It will make your unit tests fail due to incorrect ZK client port.
   *
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public HTable(final byte [] tableName)
  throws IOException {
    this(HBaseConfiguration.create(), tableName);
  }

  public HTable(Configuration conf, byte[] tableName) throws IOException {
    this(conf, new StringBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table.
   *
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final String tableName)
  throws IOException {
    this(conf, new StringBytes(tableName));
  }


  /**
   * Creates an object to access a HBase table.
   *
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, StringBytes tableName)
  throws IOException {
    this.tableName = tableName;
    if (conf == null) {
      this.scannerTimeout = 0;
      this.connection = null;
      return;
    }
    this.connection = HConnectionManager.getConnection(conf);
    this.scannerTimeout = (int) conf.getLong(
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
        HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);
    this.configuration = conf;

    this.getConnectionAndResetOperationContext().locateRegion(this.tableName,
        HConstants.EMPTY_START_ROW);
    this.writeBufferSize = conf.getLong("hbase.client.write.buffer", 2097152);
    this.clearBufferOnFail = true;
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = conf.getInt("hbase.client.scanner.caching", 1);

    this.maxScannerResultSize = conf.getLong(
      HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    this.maxKeyValueSize = conf.getInt("hbase.client.keyvalue.maxsize", -1);
    this.recordClientContext = conf.getBoolean("hbase.client.record.context", false);

    this.options = new HBaseRPCOptions ();
    String compressionAlgo = conf.get(HConstants.HBASE_RPC_COMPRESSION_KEY);
    if (compressionAlgo != null) {
      this.options.setTxCompression(
          Compression.getCompressionAlgorithmByName(compressionAlgo));
      this.options.setRxCompression(
          Compression.getCompressionAlgorithmByName(compressionAlgo));
    }
    // check if we are using swift protocol too
    this.doAsync = configuration.getBoolean(HConstants.HTABLE_ASYNC_CALLS,
        HConstants.HTABLE_ASYNC_CALLS_DEFAULT)
        && configuration.getBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
            HConstants.CLIENT_TO_RS_USE_THRIFT_DEFAULT);

    this.endpointClient = new HTableEndpointClient(this);
  }

  /**
   * Shallow copy constructor
   *
   * @param t HTable instance to copy from
   */
  public HTable(HTable t) {
    this.tableName = t.tableName;
    this.scannerTimeout = t.scannerTimeout;
    this.connection = t.connection;
    this.configuration = t.configuration;
    this.writeBufferSize = t.writeBufferSize;
    this.clearBufferOnFail = t.clearBufferOnFail;
    this.autoFlush = t.autoFlush;
    this.currentWriteBufferSize = t.currentWriteBufferSize;
    this.scannerCaching = t.scannerCaching;
    this.maxKeyValueSize = t.maxKeyValueSize;
    this.options = t.options;
    this.recordClientContext = t.recordClientContext;
    this.maxScannerResultSize = t.maxScannerResultSize;
  }

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  public List<OperationContext> getAndResetOperationContext() {
    return this.connection.getAndResetOperationContext();
  }

  /**
   * TODO Might want to change this to public, would be nice if the number
   * of threads would automatically change when servers were added and removed
   * @return the number of region servers that are currently running
   * @throws IOException if a remote or network exception occurs
   */
  public int getCurrentNrHRS() throws IOException {
    return HConnectionManager
      .getClientZKConnection(this.configuration)
      .getZooKeeperWrapper()
      .getRSDirectoryCount();
  }

  /**
   * Tells whether or not a table is enabled or not. DO NOT USE THIS METHOD.
   * It will make your unit tests fail due to incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not. DO NOT USE THIS METHOD.
   * It will make your unit tests fail due to incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
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
   */
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
  public static boolean isTableEnabled(Configuration conf, byte[] tableName)
  throws IOException {
    return HConnectionManager.getConnection(conf).isTableEnabled(
        new StringBytes(tableName));
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

  @Override
  public byte[] getTableName() {
    return this.tableName.getBytes();
  }

  public StringBytes getTableNameStringBytes() {
    return this.tableName;
  }

  /**
   * Returns the connection to the HConnectionManager and also resets the
   * operationContext. The context maintained is thread local and since we
   * don't own the thread we cannot control the lifetime of this context object.
   * Resetting it on the next getConnectionAndResetOperationContext()
   * call is the best option. Hence, this call resets the context maintained
   * in the connection object.
   * @return An HConnection instance.
   */

  public HConnection getConnectionAndResetOperationContext() {
    if (recordClientContext) {
      this.connection.resetOperationContext();
    }
    return this.connection;
  }

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
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
   */
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
  }

  /**
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public void clearRegionCache() {
    this.connection.clearRegionCache();
  }

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
    final List<byte[]> startKeyList = new ArrayList<byte[]>();
    final List<byte[]> endKeyList = new ArrayList<byte[]>();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));
        if (Bytes.equals(info.getTableDesc().getName(), getTableName())) {
          if (!(info.isOffline() || info.isSplit())) {
            startKeyList.add(info.getStartKey());
            endKeyList.add(info.getEndKey());
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(configuration, visitor, this.tableName);
    return new Pair<byte[][],byte[][]>(startKeyList.toArray(
        new byte[startKeyList.size()][]), endKeyList.toArray(
            new byte[endKeyList.size()][]));
  }

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return TreeMap of {startKey,endKey} pairs
   * @throws IOException if a remote or network exception occurs
   */
  public TreeMap<byte[], byte[]> getStartEndKeysMap() throws IOException {
    final TreeMap<byte[], byte[]> startEndKeysMap =
      new TreeMap<byte[], byte[]>(new Bytes.ByteArrayComparator());
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));
        if (Bytes.equals(info.getTableDesc().getName(), getTableName())) {
          if (!(info.isOffline() || info.isSplit())) {
            startEndKeysMap.put(info.getStartKey(), info.getEndKey());
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(configuration, visitor, this.tableName);
    return startEndKeysMap;
  }

  /**
   * Returns the Array of StartKeys along with the favoredNodes
   * for a particular region. Identifying the the favoredNodes using the
   * Meta table similar to the
   * {@link org.apache.hadoop.hbase.client.HTable.getStartEndKeys()}
   * function
   * @return
   * @throws IOException
   */
  public Pair<byte[][], byte[][]> getStartKeysAndFavoredNodes()
      throws IOException {
    final List<byte[]> startKeyList = new ArrayList<byte[]>();
    final List<byte[]> favoredNodes =
        new ArrayList<byte[]>();
        MetaScannerVisitor visitor = new MetaScannerVisitor() {
          @Override
          public boolean processRow(Result rowResult) throws IOException {
            HRegionInfo info = Writables.getHRegionInfo(
                rowResult.getValue(HConstants.CATALOG_FAMILY,
                    HConstants.REGIONINFO_QUALIFIER));
            byte[] favoredNodesBytes = rowResult.getValue(
                HConstants.CATALOG_FAMILY,
                HConstants.FAVOREDNODES_QUALIFIER);
            if (Bytes.equals(info.getTableDesc().getName(), getTableName())) {
              if (!(info.isOffline() || info.isSplit())) {
                startKeyList.add(info.getStartKey());
                favoredNodes.add(favoredNodesBytes);
              }
            }
            return true;
          }
        };
        MetaScanner.metaScan(configuration, visitor, this.tableName);
        return new Pair<byte[][], byte[][]>(
            startKeyList.toArray(new byte[startKeyList.size()][]),
            favoredNodes.toArray(new byte[favoredNodes.size()][]));
  }

  /**
   * Gets all the regions (assigned and unassigned) and their address for this table.
   * If the region is not assigned, then the associated  address will be an empty
   * HServerAddress.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public NavigableMap<HRegionInfo, HServerAddress> getRegionsInfo()
    throws IOException {
    final NavigableMap<HRegionInfo, HServerAddress> regionMap =
      new TreeMap<HRegionInfo, HServerAddress>();

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
          rowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER));

        if (!(Bytes.equals(info.getTableDesc().getName(), getTableName()))) {
          return false;
        }

        HServerAddress server = new HServerAddress();
        byte [] value = rowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SERVER_QUALIFIER);
        if (value != null && value.length > 0) {
          String address = Bytes.toString(value);
          server = new HServerAddress(address);
        }

        if (!(info.isOffline() || info.isSplit())) {
          regionMap.put(new UnmodifyableHRegionInfo(info), server);
        }
        return true;
      }

    };
    MetaScanner.metaScan(configuration, visitor, tableName);
    return regionMap;
  }

  /**
   * Get all the cached HRegionLocations. If the region is not assigned,
   * then it won't be included.
   * @param forceRefresh
   * @return
   */
  @Override
  public Collection<HRegionLocation> getCachedHRegionLocations(boolean forceRefresh) {
    return connection.getCachedHRegionLocations(tableName, forceRefresh);
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
    this.connection.prewarmRegionCache(this.getTableNameStringBytes(),
        regionMap);
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

  @Override
  public Result getRowOrBefore(final byte[] row, final byte[] family)
      throws IOException {
    initHTableAsync();
    if (hta != null) {
      try {
        return hta.getRowOrBeforeAsync(row, family).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
    return this
        .getConnectionAndResetOperationContext()
        .getRegionServerWithRetries(
            new ServerCallable<Result>(connection, tableName, row, this.options) {
              @Override
              public Result call() throws IOException {
                Result result = server.getClosestRowBefore(location
                    .getRegionInfo().getRegionName(), row, family);
                return result;
              }
            });
  }

  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    return HTableClientScanner.builder(scan, this).build();
  }

  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  public ResultScanner getLocalScanner(final Scan scan) throws IOException {
    return getLocalScanner(scan, true);
  }

  /**
   * Create a ClientLocalScanner to scan the HTable using the scan provided.
   *
   * @param scan The scan object that determines the way scanning is done.
   * @param createNewHardlinks If true, tells to create a snapshot of the store
   * by creating new hard links.
   * Otherwise, we assume that the table hierarchy under the root directory
   * is not going to change and hence we read directly from it; use with caution
   * @return
   * @throws IOException : retry on IOException
   */
  public ResultScanner getLocalScanner(final Scan scan,
      boolean createNewHardlinks) throws IOException {
    // Construct preload threads in case this scanner is preloading scanner
    // We'll obtain the number of threads from the HTableCon TODO (is this the right place ? )
    if (scan.isPreloadBlocks()) {
      int minimum =
          getConfiguration().getInt(HConstants.CORE_PRELOAD_THREAD_COUNT,
            HConstants.DEFAULT_CORE_PRELOAD_THREAD_COUNT);
      int maximum =
          getConfiguration().getInt(HConstants.MAX_PRELOAD_THREAD_COUNT,
            HConstants.DEFAULT_MAX_PRELOAD_THREAD_COUNT);
      PreloadThreadPool.constructPreloaderThreadPool(minimum, maximum);
    }
    ClientLocalScanner s =
        new ClientLocalScanner(scan, this, createNewHardlinks);
    s.initialize();
    return s;
  }

  /**
   * Gets a server configuration property from a random server using the current
   * table's connection.
   *
   * @param name : The name of the property requested
   * @return String value of the property requested
   * Empty string for non existent properties
   * @throws IOException
   */
  public String getServerConfProperty(final String name) throws IOException {
    return this.getConnectionAndResetOperationContext().getServerConfProperty(name);
  }

  @Override
  public Result get(final Get get) throws IOException {
    initHTableAsync();
    if (hta != null) {
      try {
        return hta.getAsync(get).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Result>(connection, tableName, get.getRow(), this.options) {
          @Override
          public Result call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return this.getConnectionAndResetOperationContext().processBatchOfGets(gets, tableName, this.options);
  }

  /**
   * Get collected profiling data and clears it from the HTable
   * @return aggregated profiling data
   */
  public ProfilingData getProfilingData() {
    ProfilingData ret =  this.options.profilingResult;
    this.options.profilingResult = null;
    return ret;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] batchGet(final List<Get> actions)
      throws IOException {
    Result[] results = new Result[actions.size()];
    try {
      this.getConnectionAndResetOperationContext().processBatchedGets(actions, tableName, listeningMultiActionPool,
          results, this.options);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batchMutate(final List<Mutation> actions)
      throws IOException {
    try {
      this.getConnectionAndResetOperationContext().processBatchedMutations(actions,
          tableName, listeningMultiActionPool, null, this.options);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * @param delete
   * @throws IOException
   */
  @Override
  public void delete(final Delete delete) throws IOException {
    initHTableAsync();
    if (hta != null) {
      try {
        hta.deleteAsync(delete).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
          new ServerCallable<Boolean>(connection, tableName, delete.getRow(),
              this.options) {
            @Override
            public Boolean call() throws IOException {
              server.delete(location.getRegionInfo().getRegionName(), delete);
              return null; // FindBugs NP_BOOLEAN_RETURN_NULL
            }
          });
    }
  }

  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    int last = 0;
    try {
      last = this.getConnectionAndResetOperationContext().processBatchOfDeletes(
          deletes, this.tableName, this.options);
    } finally {
      deletes.subList(0, last).clear();
    }
  }

  @Override
  public void put(final Put put) throws IOException {
    initHTableAsync();
    if (hta != null) {
      try {
        hta.putAsync(put).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      doPut(Arrays.asList(put));
    }
  }

  @Override
  public void put(final List<Put> puts) throws IOException {
    doPut(puts);
  }

  private void doPut(final List<Put> puts) throws IOException {
    for (Put put : puts) {
      validatePut(put);
      writeBuffer.add(put);
      currentWriteBufferSize += put.heapSize();
    }
    if (autoFlush || currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

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
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Long>(connection, tableName, row, this.options) {
          @Override
          public Long call() throws IOException {
            return server.incrementColumnValue(
                location.getRegionInfo().getRegionName(), row, family,
                qualifier, amount, writeToWAL);
          }
        }
    );
  }

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the put.  If value == null, checks for non-existence
   * of the value.
   *
   * @param row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put put to execute if value matches.
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row, this.options) {
          @Override
          public Boolean call() throws IOException {
            return server.checkAndPut(location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, put) ? Boolean.TRUE : Boolean.FALSE;
          }
        }
    );
  }

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the delete.  If value == null, checks for non-existence
   * of the value.
   *
   * @param row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to execute if value matches.
   * @throws IOException
   * @return true if the new delete was executed, false otherwise
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row, this.options) {
          @Override
          public Boolean call() throws IOException {
            return server.checkAndDelete(
                location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, delete)
            ? Boolean.TRUE : Boolean.FALSE;
          }
        }
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(final RowMutations arm) throws IOException {
    initHTableAsync();
    if (hta != null) {
      try {
        hta.mutateRowAsync(arm).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
          new ServerCallable<Void>(this.connection, tableName, arm.getRow(),
              this.options) {
            @Override
            public Void call() throws IOException, InterruptedException,
                ExecutionException {
              server.mutateRow(location.getRegionInfo().getRegionName(), arm);
              return null;
            }
          });
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(final List<RowMutations> armList) throws IOException {
    this.getConnectionAndResetOperationContext().processBatchOfRowMutations(
        armList, this.tableName, this.options);
  }

  /**
   * Test for the existence of columns in the table, as specified in the Get.<p>
   *
   * This will return true if the Get matches one or more keys, false if not.<p>
   *
   * This is a server-side call so it prevents any data from being transfered
   * to the client.
   * @param get param to check for
   * @return true if the specified Get matches one or more keys, false if not
   * @throws IOException
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, get.getRow(), this.options) {
          @Override
          public Boolean call() throws IOException {
            return server.
                exists(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

  @Override
  public void flushCommits() throws IOException {
    try {
      this.getConnectionAndResetOperationContext().
        processBatchOfPuts(writeBuffer, tableName, this.options);
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
    flushCommits();
  }

  // validate for well-formedness
  public void validatePut(final Put put) throws IllegalArgumentException{
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

  @Override
  public RowLock lockRow(final byte[] row) throws IOException {
    initHTableAsync();
    if (hta != null) {
      try {
        return hta.lockRowAsync(row).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
    return this.getConnectionAndResetOperationContext()
        .getRegionServerWithRetries(
            new ServerCallable<RowLock>(connection, tableName, row,
                this.options) {
              @Override
              public RowLock call() throws IOException {
                long lockId = server.lockRow(location.getRegionInfo()
                    .getRegionName(), row);
                return new RowLock(row, lockId);
              }
            });
  }

  @Override
  public void unlockRow(final RowLock rl) throws IOException {
    initHTableAsync();
    if (hta != null) {
      try {
        hta.unlockRowAsync(rl).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
          new ServerCallable<Boolean>(connection, tableName, rl.getRow(),
              this.options) {
            @Override
            public Boolean call() throws IOException {
              server.unlockRow(location.getRegionInfo().getRegionName(),
                  rl.getLockId());
              return null; // FindBugs NP_BOOLEAN_RETURN_NULL
            }
          });
    }
  }

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
   * {@link #clearBufferOnFail} option. By default, asynchronous {@link Put)
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
   * Flushes a region for the give row.
   * @param row
   * @param acceptableLastFlushTimeMs : The acceptable last flush time in ms
   * @throws IOException : The client can safely retry on IOException
   */
  public void flushRegionForRow(byte[] row, long acceptableLastFlushTimeMs)
      throws IOException {
    flushRegionLazyForRow(row, acceptableLastFlushTimeMs, 0);
  }

  /**
   * Flushes a region for the given row.
   * @param row
   * @param acceptableLastFlushTimeMs : This is how old the last flush can be
   * for it to be considered as a successful
   * @param maxWaitTime : The maximum amount of time we should wait
   * hoping for a flush to happen
   * @throws IOException : The client can safely retry on exception.
   */
  public void flushRegionLazyForRow(byte[] row, long acceptableLastFlushTimeMs,
      long maxWaitTime) throws IOException {
    HRegionLocation loc = this.getRegionLocation(row);
    HRegionInfo info = loc.getRegionInfo();
    HServerAddress addr = loc.getServerAddress();
    this.getConnectionAndResetOperationContext()
      .flushRegionAndWait(info, addr, acceptableLastFlushTimeMs, maxWaitTime);
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   */
  public static void setRegionCachePrefetch(final byte[] tableName,
      boolean enable) {
    HConnectionManager.getConnection(HBaseConfiguration.create()).
setRegionCachePrefetch(new StringBytes(tableName), enable);
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param conf The Configuration object to use.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   */
  public static void setRegionCachePrefetch(final Configuration conf,
      final byte[] tableName, boolean enable) {
    HConnectionManager.getConnection(conf).setRegionCachePrefetch(
        new StringBytes(tableName), enable);
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param conf The Configuration object to use.
   * @param tableName name of table to check
   * @return true if table's region cache prefetch is enabled. Otherwise
   * it is disabled.
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) {
    return HConnectionManager.getConnection(conf).getRegionCachePrefetch(
        new StringBytes(tableName));
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName) {
    return HConnectionManager.getConnection(HBaseConfiguration.create()).
getRegionCachePrefetch(new StringBytes(tableName));
  }

  /**
   * Set profiling request on/off for every subsequent RPC calls
   * @param prof profiling true/false
   */
  @Override
  public void setProfiling(boolean prof) {
    options.setRequestProfiling(prof);
    options.profilingResult = null;
  }

  @Override
  public boolean getProfiling() {
    return options.getRequestProfiling ();
  }

  @Override
  public void setTag (String tag) {
    this.options.setTag (tag);
  }

  @Override
  public String getTag () {
    return this.options.getTag ();
  }

  /**
   * set compression used to send RPC calls to the server
   * @param alg compression algorithm
   */
  public void setTxCompression(Compression.Algorithm alg) {
    this.options.setTxCompression(alg);
  }

  public Compression.Algorithm getTxCompression() {
    return this.options.getTxCompression();
  }

  /**
   * set compression used to receive RPC responses from the server
   * @param alg compression algorithm
   */
  public void setRxCompression(Compression.Algorithm alg) {
    this.options.setRxCompression(alg);
  }

  public Compression.Algorithm getRxCompression() {
    return this.options.getRxCompression();
  }

  /**
   * Starts tracking the updates made to this table so that
   * we can ensure that the updates were completed and flushed to
   * disk at the end of the job.
   */
  public void startBatchedLoad() {
    connection.startBatchedLoad(tableName);
  }

  /**
   * Ensure that all the updates made to the table, since
   * startBatchedLoad was called are persisted. This method
   * waits for all the regionservers contacted, to
   * flush all the data written so far.
   */
  public void endBatchedLoad() throws IOException {
    connection.endBatchedLoad(tableName, this.options);
  }

  /**
   * Returns the List of buckets which represent the histogram for the region
   * the row belongs to.
   *
   * @param row
   * @return will be either null or at least will contain
   * one element
   * @throws IOException
   */
  protected List<Bucket> getHistogram(final byte[] row) throws IOException {
    return getHistogramForColumnFamily(row, null);
  }

  /**
   * Returns the List of buckets which represent the histogram for the column
   * family in the region the row belongs to.
   * Also see {@link #getHistogram(byte[])}
   * @param row
   * @return
   * @throws IOException
   */
  protected List<Bucket> getHistogramForColumnFamily(final byte[] row,
      final byte[] cf) throws IOException {
    List<Bucket> ret = this.getConnectionAndResetOperationContext()
        .getRegionServerWithRetries(
            new ServerCallable<List<Bucket>>(connection,
                tableName, row, this.options) {
              @Override
              public List<Bucket> call() throws IOException {
                if (cf != null) {
                  return server.getHistogramForStore(
                    location.getRegionInfo().getRegionName(), cf);
                } else {
                  return server.getHistogram(
                      location.getRegionInfo().getRegionName());
                }
              }
        }
    );
    Preconditions.checkArgument(ret == null || ret.size() > 1);
    return ret;
  }

  /**
   * API to get the histograms for all the regions in the table.
   * @return
   * @throws IOException
   */
  public List<List<Bucket>> getHistogramsForAllRegions() throws IOException {
    return getHistogramsForAllRegions(null);
  }

  /**
   * Returns all the histograms related to the current table.
   * This is a batch operation including nultiple
   * @return
   * @throws IOException
   */
  public List<List<Bucket>> batchgetHistogramsForAllRegions()
      throws IOException {
    return getHistogramForAllRegionsInternal();
  }

  /**
   * API to get the histograms for all the regions in the table.
   * @param family : the family whose histogram is being requested.
   * Returns data for all the families if family is null.
   * @return
   * @throws IOException
   */
  public List<List<Bucket>> getHistogramsForAllRegions(final byte[] family)
      throws IOException {
    List<List<Bucket>> ret = new ArrayList<List<Bucket>>();
    TreeMap<byte[], Future<List<Bucket>>> futures =
        new TreeMap<byte[], Future<List<Bucket>>>(Bytes.BYTES_COMPARATOR);
    for (final byte[] row : this.getStartKeys()) {
      futures.put(row, HTable.multiActionThreadPool.submit(
          new Callable<List<Bucket>>() {
            @Override
            public List<Bucket> call() throws Exception {
              if (family == null) {
                return getHistogram(row);
              } else {
                return getHistogramForColumnFamily(row, family);
              }
            }
          }));
    }
    for (Future<List<Bucket>> f : futures.values()) {
      try {
        ret.add(f.get());
      } catch (InterruptedException e) {
        throw new IOException("Failed obtaining the histograms for the regions",
            e);
      } catch (ExecutionException e) {
        throw new IOException("Failed obtaining the histograms for the regions",
            e);
      }
    }
    return ret;
  }

  public Map<HServerAddress, List<HRegionInfo>> getServersToRegionsMap()
      throws IOException {
    Map<HServerAddress, List<HRegionInfo>> serversToRegionsMap =
        new TreeMap<>();
    for (Entry<HRegionInfo, HServerAddress> entry : getRegionsInfo().entrySet())
    {
      List<HRegionInfo> lst = serversToRegionsMap.get(entry.getValue());
      if (lst == null) {
        lst = new ArrayList<>();
      }
      lst.add(entry.getKey());
      serversToRegionsMap.put(entry.getValue(), lst);
    }
    return serversToRegionsMap;
  }

  private List<List<Bucket>> getHistogramForAllRegionsInternal()
      throws IOException {
    List<List<Bucket>> ret = new ArrayList<List<Bucket>>();
    TreeMap<byte[], List<Bucket>> retMap =
        new TreeMap<>(Bytes.BYTES_COMPARATOR);
    List<Future<List<List<Bucket>>>>futures = new ArrayList<>();
    for (final Entry<HServerAddress, List<HRegionInfo>> entry :
      getServersToRegionsMap().entrySet()) {
      final List<byte[]> regionNames = new ArrayList<>();
      for (HRegionInfo info : entry.getValue()) {
        regionNames.add(info.getRegionName());
      }
      futures.add(HTable.multiActionThreadPool.submit(new Callable<List<List<Bucket>>>() {
        @Override
        public List<List<Bucket>> call() throws Exception {
          return getConnectionAndResetOperationContext()
                .getHRegionConnection(entry.getKey()).getHistograms(regionNames);
        }
      }));
    }
    for (Future<List<List<Bucket>>> future : futures) {
      try {
        for (List<Bucket> histogram : future.get()) {
          if (histogram.size() >= 1) {
            retMap.put(histogram.get(0).getStartRow(), histogram);
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
    ret.addAll(retMap.values());
    return ret;
  }

  public long getMaxScannerResultSize() {
    return maxScannerResultSize;
  }

  public HConnection getConnection() {
    return connection;
  }

  public HBaseRPCOptions getOptions() {
    return options;
  }

  @Override
  public <T extends IEndpoint, R> Map<HRegionInfo, R> coprocessorEndpoint(
      Class<T> clazz, byte[] startRow, byte[] stopRow, Caller<T, R> caller)
      throws IOException {
    return this.endpointClient.coprocessorEndpoint(clazz, startRow, stopRow,
        caller);
  }

}
