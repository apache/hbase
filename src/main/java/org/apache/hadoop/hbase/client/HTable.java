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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.PreloadThreadPool;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.ProfilingData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
public class HTable implements HTableInterface {
  private final HConnection connection;
  private final byte [] tableName;
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

  @SuppressWarnings("unused")
  private long maxScannerResultSize;

  // Share this multiaction thread pool across all the HTable instance;
  // The total number of threads will be bounded #HTable * #RegionServer.
  static ExecutorService multiActionThreadPool =
    new ThreadPoolExecutor(1, Integer.MAX_VALUE,
      60, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>(),
      new DaemonThreadFactory("htable-thread-"));
  static {
    ((ThreadPoolExecutor)multiActionThreadPool).allowCoreThreadTimeOut(true);
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

  /**
   * Creates an object to access a HBase table.
   *
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
   *
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final byte [] tableName)
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

    this.getConnectionAndResetOperationContext().locateRegion(tableName, HConstants.EMPTY_START_ROW);
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
  }

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
    return HConnectionManager.getConnection(conf).isTableEnabled(tableName);
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

  public byte [] getTableName() {
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
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public NavigableMap<HRegionInfo, HServerAddress> getRegionsInfo()
      throws IOException {
    final NavigableMap<HRegionInfo, HServerAddress> regionMap =
      new TreeMap<HRegionInfo, HServerAddress>();

    for (HRegionLocation location : getCachedHRegionLocations(true)) {
      regionMap.put(location.getRegionInfo(), location.getServerAddress());
    }

    return regionMap;
  }

  /**
   * Get all the cached HRegionLocations.
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

   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
         new ServerCallable<Result>(connection, tableName, row, this.options) {
       public Result call() throws IOException {
         return server.getClosestRowBefore(location.getRegionInfo().getRegionName(),
           row, family);
       }
     });
   }

  public ResultScanner getScanner(final Scan scan) throws IOException {
    ClientScanner s = new ClientScanner(scan);
    s.initialize();
    return s;
  }

  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

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
   * @throws IOException
   */
  public String getServerConfProperty(final String name) throws IOException {
    return this.getConnectionAndResetOperationContext().getServerConfProperty(name);
  }

  public Result get(final Get get) throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Result>(connection, tableName, get.getRow(), this.options) {
          public Result call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

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
      this.getConnectionAndResetOperationContext().processBatchedGets(actions, tableName, multiActionThreadPool,
          results, this.options);
    } catch (Exception e) {
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
          tableName, multiActionThreadPool, null, this.options);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
      this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection,
            tableName, delete.getRow(), this.options) {
          public Boolean call() throws IOException {
            server.delete(location.getRegionInfo().getRegionName(), delete);
            return null; // FindBugs NP_BOOLEAN_RETURN_NULL
          }
        }
    );
  }

  public void delete(final List<Delete> deletes)
  throws IOException {
    int last = 0;
    try {
      last = this.getConnectionAndResetOperationContext().processBatchOfDeletes(deletes, this.tableName, this.options);
    } finally {
      deletes.subList(0, last).clear();
    }
  }

  public void put(final Put put) throws IOException {
    doPut(Arrays.asList(put));
  }

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

  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  @SuppressWarnings({"ThrowableInstanceNeverThrown"})
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
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row, this.options) {
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
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row, this.options) {
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
    this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
      new ServerCallable<Void>(this.connection, tableName, arm.getRow(), this.options) {
        public Void call() throws IOException {
          server.mutateRow(location.getRegionInfo().getRegionName(), arm);
          return null;
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(final List<RowMutations> armList) throws IOException {
    this.getConnectionAndResetOperationContext().processBatchOfRowMutations(armList, this.tableName, this.options);
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
  public boolean exists(final Get get) throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, get.getRow(), this.options) {
          public Boolean call() throws IOException {
            return server.
                exists(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

  public void flushCommits() throws IOException {
    try {
      this.getConnectionAndResetOperationContext().processBatchOfPuts(writeBuffer, tableName, this.options);
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

  public RowLock lockRow(final byte [] row)
  throws IOException {
    return this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
      new ServerCallable<RowLock>(connection, tableName, row, this.options) {
        public RowLock call() throws IOException {
          long lockId =
              server.lockRow(location.getRegionInfo().getRegionName(), row);
          return new RowLock(row,lockId);
        }
      }
    );
  }

  public void unlockRow(final RowLock rl)
  throws IOException {
    this.getConnectionAndResetOperationContext().getRegionServerWithRetries(
      new ServerCallable<Boolean>(connection, tableName, rl.getRow(), this.options) {
        public Boolean call() throws IOException {
          server.unlockRow(location.getRegionInfo().getRegionName(),
              rl.getLockId());
          return null; // FindBugs NP_BOOLEAN_RETURN_NULL
        }
      }
    );
  }

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
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  protected class ClientScanner extends ResultScannerImpl {
    // HEADSUP: The scan internal start row can change as we move through table.
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    private ScannerCallable callable = null;
    // Keep lastResult returned successfully in case we have to reset scanner.
    private Result lastResult = null;

    protected ClientScanner(final Scan scan) {
      super(scan, HTable.this);
      // Removed filter validation.  We have a new format now, only one of all
      // the current filters has a validate() method.  We can add it back,
      // need to decide on what we're going to do re: filter redesign.
      // Need, at the least, to break up family from qualifier as separate
      // checks, I think it's important server-side filters are optimal in that
      // respect.
    }

    protected Scan getScan() {
      return scan;
    }

    protected ScannerCallable getScannerCallable(byte [] localStartKey,
        int nbRows, HBaseRPCOptions options) {
      scan.setStartRow(localStartKey);
      ScannerCallable s = new ScannerCallable(
          getConnectionAndResetOperationContext(), getTableName(), scan,
          options);
      s.setCaching(nbRows);
      return s;
    }

    @Override
    protected void cleanUpPreviousScanners() throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        getConnectionAndResetOperationContext().getRegionServerWithRetries(
            callable);
        this.callable = null;
      }

    }

    @Override
    protected boolean doRealOpenScanners(byte[] localStartKey, int nbRows)
        throws IOException {
      try {
        callable = getScannerCallable(localStartKey, nbRows, options);
        // Open a scanner on the region server starting at the
        // beginning of the region
        getConnectionAndResetOperationContext().getRegionServerWithRetries(
            callable);
        this.currentRegion = callable.getHRegionInfo();
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }

    @Override
    protected void cacheNextResults() throws IOException {
      Result [] values = null;
      // We need to reset it if it's a new callable that was created
      // with a countdown in nextScanner
      callable.setCaching(this.caching);
      // This flag is set when we want to skip the result returned.  We do
      // this when we reset scanner because it split under us.
      boolean skipFirst = false;
      boolean foundResults = false;
      do {
        try {
          // Server returns a null values if scanning is to stop.  Else,
          // returns an empty array if scanning is to go on and we've just
          // exhausted current region.
          values = getConnectionAndResetOperationContext(
              ).getRegionServerWithRetries(callable);
          if (skipFirst) {
            skipFirst = false;
            // Reget.
            values = getConnectionAndResetOperationContext()
                .getRegionServerWithRetries(callable);
          }
        } catch (DoNotRetryIOException e) {
          if (e instanceof UnknownScannerException) {
            long timeout = this.lastNextCallTimeStamp + scannerTimeout;
            // If we are over the timeout, throw this exception to the client
            // Else, it's because the region moved and we used the old id
            // against the new region server; reset the scanner.
            if (timeout < System.currentTimeMillis()) {
              long elapsed = System.currentTimeMillis()
                  - this.lastNextCallTimeStamp;
              ScannerTimeoutException ex = new ScannerTimeoutException(
                  elapsed + "ms passed since the last invocation, " +
                  "timeout is currently set to " + scannerTimeout);
              ex.initCause(e);
              throw ex;
            }
          } else {
            Throwable cause = e.getCause();
            if (cause == null
                || !(cause instanceof NotServingRegionException)) {
              throw e;
            }
          }
          // Else, its signal from depths of ScannerCallable that we got an
          // NSRE on a next and that we need to reset the scanner.
          if (this.lastResult != null) {
            this.scan.setStartRow(this.lastResult.getRow());
            // Skip first row returned.  We already let it out on previous
            // invocation.
            skipFirst = true;
          }
          // Clear region
          this.currentRegion = null;
        }
        this.lastNextCallTimeStamp = System.currentTimeMillis();
        if (values != null && values.length > 0) {
          foundResults = true;
          for (Result rs : values) {
            cache.add(rs);
            this.lastResult = rs;
          }
        }
      } while (!foundResults && nextScanner(this.caching, values == null));
    }

    @Override
    protected void closeCurrentScanner() {
      if (callable != null) {
        callable.setClose();
        try {
          getConnectionAndResetOperationContext().getRegionServerWithRetries(
              callable);
        } catch (IOException e) {
          // We used to catch this error, interpret, and rethrow. However, we
          // have since decided that it's not nice for a scanner's close to
          // throw exceptions. Chances are it was just an UnknownScanner
          // exception due to lease time out.
        }
        callable = null;
      }
    }
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
    setRegionCachePrefetch(tableName, enable);
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
        tableName, enable);
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param conf The Configuration object to use.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) {
    return HConnectionManager.getConnection(conf).getRegionCachePrefetch(
        tableName);
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName) {
    return HConnectionManager.getConnection(HBaseConfiguration.create()).
    getRegionCachePrefetch(tableName);
  }
  
  /**
   * Set profiling request on/off for every subsequent RPC calls
   * @param prof profiling true/false
   */
  @Override
  public void setProfiling(boolean prof) {
    options.setRequestProfiling (prof);
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
   * Some notes regarding the buckets :
   * The Bucket boundaries may not align with the boundaries of the Region.
   * The Bucket Boundaries will look as follows :
   * [0x00,0x00, ... 0x00] -> [some byte array] -> ... -> [some byte array]
   * -> [0xff, 0xff, ... 0xff]
   *
   * @param row
   * @return will be either null or at least will contain
   * one element
   * @throws IOException
   */
  public List<Bucket> getHistogram(final byte[] row) throws IOException {
    return this.getConnectionAndResetOperationContext()
        .getRegionServerWithRetries(
        new ServerCallable<List<Bucket>>(connection,
            tableName, row, this.options) {
          public List<Bucket> call() throws IOException {
            return server.getHistogram(
                location.getRegionInfo().getRegionName());
          }
        }
    );
  }

  /**
   * Returns the List of buckets which represent the histogram for the column
   * family in the region the row belongs to.
   * Also see {@link #getHistogram(byte[])}
   * @param row
   * @return
   * @throws IOException
   */
  public List<Bucket> getHistogramForColumnFamily(final byte[] row,
      final byte[] cf) throws IOException {
    return this.getConnectionAndResetOperationContext()
        .getRegionServerWithRetries(
            new ServerCallable<List<Bucket>>(connection,
                tableName, row, this.options) {
              public List<Bucket> call() throws IOException {
                return server.getHistogramForStore(
                    location.getRegionInfo().getRegionName(), cf);
              }
        }
    );
  }
}
