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
package org.apache.hadoop.hbase.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NeedUnmanagedConnectionException;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;

/**
 * A base for {@link TableInputFormat}s. Receives a {@link Connection}, a {@link TableName},
 * an {@link Scan} instance that defines the input columns etc. Subclasses may use
 * other TableRecordReader implementations.
 *
 * Subclasses MUST ensure initializeTable(Connection, TableName) is called for an instance to
 * function properly. Each of the entry points to this class used by the MapReduce framework,
 * {@link #createRecordReader(InputSplit, TaskAttemptContext)} and {@link #getSplits(JobContext)},
 * will call {@link #initialize(JobContext)} as a convenient centralized location to handle
 * retrieving the necessary configuration information. If your subclass overrides either of these
 * methods, either call the parent version or call initialize yourself.
 *
 * <p>
 * An example of a subclass:
 * <pre>
 *   class ExampleTIF extends TableInputFormatBase {
 *
 *     {@literal @}Override
 *     protected void initialize(JobContext context) throws IOException {
 *       // We are responsible for the lifecycle of this connection until we hand it over in
 *       // initializeTable.
 *       Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(
 *              job.getConfiguration()));
 *       TableName tableName = TableName.valueOf("exampleTable");
 *       // mandatory. once passed here, TableInputFormatBase will handle closing the connection.
 *       initializeTable(connection, tableName);
 *       byte[][] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
 *         Bytes.toBytes("columnB") };
 *       // optional, by default we'll get everything for the table.
 *       Scan scan = new Scan();
 *       for (byte[] family : inputColumns) {
 *         scan.addFamily(family);
 *       }
 *       Filter exampleFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("aa.*"));
 *       scan.setFilter(exampleFilter);
 *       setScan(scan);
 *     }
 *   }
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class TableInputFormatBase
extends InputFormat<ImmutableBytesWritable, Result> {

  final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

  private static final String NOT_INITIALIZED = "The input format instance has not been properly " +
      "initialized. Ensure you call initializeTable either in your constructor or initialize " +
      "method";
  private static final String INITIALIZATION_ERROR = "Cannot create a record reader because of a" +
            " previous error. Please look at the previous logs lines from" +
            " the task's full log for more details.";

  /** Holds the details for the internal scanner.
   *
   * @see Scan */
  private Scan scan = null;
  /** The {@link Admin}. */
  private Admin admin;
  /** The {@link Table} to scan. */
  private Table table;
  /** The {@link RegionLocator} of the table. */
  private RegionLocator regionLocator;
  /** The reader scanning the table, can be a custom one. */
  private TableRecordReader tableRecordReader = null;
  /** The underlying {@link Connection} of the table. */
  private Connection connection;

  
  /** The reverse DNS lookup cache mapping: IPAddress => HostName */
  private HashMap<InetAddress, String> reverseDNSCacheMap =
    new HashMap<InetAddress, String>();

  /**
   * Builds a {@link TableRecordReader}. If no {@link TableRecordReader} was provided, uses
   * the default.
   *
   * @param split  The split to work with.
   * @param context  The current context.
   * @return The newly created record reader.
   * @throws IOException When creating the reader fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(
   *   org.apache.hadoop.mapreduce.InputSplit,
   *   org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
  throws IOException {
    // Just in case a subclass is relying on JobConfigurable magic.
    if (table == null) {
      initialize(context);
    }
    // null check in case our child overrides getTable to not throw.
    try {
      if (getTable() == null) {
        // initialize() must not have been implemented in the subclass.
        throw new IOException(INITIALIZATION_ERROR);
      }
    } catch (IllegalStateException exception) {
      throw new IOException(INITIALIZATION_ERROR, exception);
    }
    TableSplit tSplit = (TableSplit) split;
    LOG.info("Input split length: " + StringUtils.humanReadableInt(tSplit.getLength()) + " bytes.");
    final TableRecordReader trr =
        this.tableRecordReader != null ? this.tableRecordReader : new TableRecordReader();
    Scan sc = new Scan(this.scan);
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setTable(getTable());
    return new RecordReader<ImmutableBytesWritable, Result>() {

      @Override
      public void close() throws IOException {
        trr.close();
        closeTable();
      }

      @Override
      public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
        return trr.getCurrentKey();
      }

      @Override
      public Result getCurrentValue() throws IOException, InterruptedException {
        return trr.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return trr.getProgress();
      }

      @Override
      public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException,
          InterruptedException {
        trr.initialize(inputsplit, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return trr.nextKeyValue();
      }
    };
  }

  protected Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    return getRegionLocator().getStartEndKeys();
  }

  /**
   * Calculates the splits that will serve as input for the map tasks. The
   * number of splits matches the number of regions in a table.
   *
   * @param context  The current job context.
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *   org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    boolean closeOnFinish = false;

    // Just in case a subclass is relying on JobConfigurable magic.
    if (table == null) {
      initialize(context);
      closeOnFinish = true;
    }

    // null check in case our child overrides getTable to not throw.
    try {
      if (getTable() == null) {
        // initialize() must not have been implemented in the subclass.
        throw new IOException(INITIALIZATION_ERROR);
      }
    } catch (IllegalStateException exception) {
      throw new IOException(INITIALIZATION_ERROR, exception);
    }

    try {
    RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, admin);

    Pair<byte[][], byte[][]> keys = getStartEndKeys();
    if (keys == null || keys.getFirst() == null ||
        keys.getFirst().length == 0) {
      HRegionLocation regLoc = regionLocator.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);
      if (null == regLoc) {
        throw new IOException("Expecting at least one region.");
      }
      List<InputSplit> splits = new ArrayList<InputSplit>(1);
      long regionSize = sizeCalculator.getRegionSize(regLoc.getRegionInfo().getRegionName());
      TableSplit split = new TableSplit(table.getName(),
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, regLoc
              .getHostnamePort().split(Addressing.HOSTNAME_PORT_SEPARATOR)[0], regionSize);
      splits.add(split);
      return splits;
    }
    List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
    for (int i = 0; i < keys.getFirst().length; i++) {
      if ( !includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
        continue;
      }
      HRegionLocation location = regionLocator.getRegionLocation(keys.getFirst()[i], false);
      // The below InetSocketAddress creation does a name resolution.
      InetSocketAddress isa = new InetSocketAddress(location.getHostname(), location.getPort());
      if (isa.isUnresolved()) {
        LOG.warn("Failed resolve " + isa);
      }
      InetAddress regionAddress = isa.getAddress();
      String regionLocation;
      try {
        regionLocation = reverseDNS(regionAddress);
      } catch (NamingException e) {
        LOG.warn("Cannot resolve the host name for " + regionAddress + " because of " + e);
        regionLocation = location.getHostname();
      }

      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();
      // determine if the given start an stop key fall into the region
      if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
          Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
          (stopRow.length == 0 ||
           Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
        byte[] splitStart = startRow.length == 0 ||
          Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
            keys.getFirst()[i] : startRow;
        byte[] splitStop = (stopRow.length == 0 ||
          Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
          keys.getSecond()[i].length > 0 ?
            keys.getSecond()[i] : stopRow;

        byte[] regionName = location.getRegionInfo().getRegionName();
        long regionSize = sizeCalculator.getRegionSize(regionName);
        TableSplit split = new TableSplit(table.getName(),
          splitStart, splitStop, regionLocation, regionSize);
        splits.add(split);
        if (LOG.isDebugEnabled()) {
          LOG.debug("getSplits: split -> " + i + " -> " + split);
        }
      }
    }
    return splits;
    } finally {
      if (closeOnFinish) {
        closeTable();
      }
    }
  }

  /**
   * @deprecated mistakenly made public in 0.98.7. scope will change to package-private
   */
  @Deprecated
  public String reverseDNS(InetAddress ipAddress) throws NamingException, UnknownHostException {
    String hostName = this.reverseDNSCacheMap.get(ipAddress);
    if (hostName == null) {
      String ipAddressString = null;
      try {
        ipAddressString = DNS.reverseDns(ipAddress, null);
      } catch (Exception e) {
        // We can use InetAddress in case the jndi failed to pull up the reverse DNS entry from the
        // name service. Also, in case of ipv6, we need to use the InetAddress since resolving
        // reverse DNS using jndi doesn't work well with ipv6 addresses.
        ipAddressString = InetAddress.getByName(ipAddress.getHostAddress()).getHostName();
      }
      if (ipAddressString == null) throw new UnknownHostException("No host found for " + ipAddress);
      hostName = Strings.domainNamePointerToHostName(ipAddressString);
      this.reverseDNSCacheMap.put(ipAddress, hostName);
    }
    return hostName;
  }

  /**
   *
   *
   * Test if the given region is to be included in the InputSplit while splitting
   * the regions of a table.
   * <p>
   * This optimization is effective when there is a specific reasoning to exclude an entire region from the M-R job,
   * (and hence, not contributing to the InputSplit), given the start and end keys of the same. <br>
   * Useful when we need to remember the last-processed top record and revisit the [last, current) interval for M-R processing,
   * continuously. In addition to reducing InputSplits, reduces the load on the region server as well, due to the ordering of the keys.
   * <br>
   * <br>
   * Note: It is possible that <code>endKey.length() == 0 </code> , for the last (recent) region.
   * <br>
   * Override this method, if you want to bulk exclude regions altogether from M-R. By default, no region is excluded( i.e. all regions are included).
   *
   *
   * @param startKey Start key of the region
   * @param endKey End key of the region
   * @return true, if this region needs to be included as part of the input (default).
   *
   */
  protected boolean includeRegionInSplit(final byte[] startKey, final byte [] endKey) {
    return true;
  }

  /**
   * Allows subclasses to get the {@link HTable}.
   *
   * @deprecated use {@link #getTable()}
   */
  @Deprecated
  protected HTable getHTable() {
    return (HTable) this.getTable();
  }

  /**
   * Allows subclasses to get the {@link RegionLocator}.
   */
  protected RegionLocator getRegionLocator() {
    if (regionLocator == null) {
      throw new IllegalStateException(NOT_INITIALIZED);
    }
    return regionLocator;
  }
  
  /**
   * Allows subclasses to get the {@link Table}.
   */
  protected Table getTable() {
    if (table == null) {
      throw new IllegalStateException(NOT_INITIALIZED);
    }
    return table;
  }

  /**
   * Allows subclasses to get the {@link Admin}.
   */
  protected Admin getAdmin() {
    if (admin == null) {
      throw new IllegalStateException(NOT_INITIALIZED);
    }
    return admin;
  }

  /**
   * Allows subclasses to set the {@link HTable}.
   *
   * Will attempt to reuse the underlying Connection for our own needs, including
   * retreiving an Admin interface to the HBase cluster.
   *
   * @param table  The table to get the data from.
   * @throws IOException 
   * @deprecated Use {@link #initializeTable(Connection, TableName)} instead.
   */
  @Deprecated
  protected void setHTable(HTable table) throws IOException {
    this.table = table;
    this.connection = table.getConnection();
    try {
      this.regionLocator = table;
      this.admin = this.connection.getAdmin();
    } catch (NeedUnmanagedConnectionException exception) {
      LOG.warn("You are using an HTable instance that relies on an HBase-managed Connection. " +
          "This is usually due to directly creating an HTable, which is deprecated. Instead, you " +
          "should create a Connection object and then request a Table instance from it. If you " +
          "don't need the Table instance for your own use, you should instead use the " +
          "TableInputFormatBase.initalizeTable method directly.");
      LOG.info("Creating an additional unmanaged connection because user provided one can't be " +
          "used for administrative actions. We'll close it when we close out the table.");
      LOG.debug("Details about our failure to request an administrative interface.", exception);
      // Do we need a "copy the settings from this Connection" method? are things like the User
      // properly maintained by just looking again at the Configuration?
      this.connection = ConnectionFactory.createConnection(this.connection.getConfiguration());
      this.regionLocator = this.connection.getRegionLocator(table.getName());
      this.admin = this.connection.getAdmin();
    }
  }

  /**
   * Allows subclasses to initialize the table information.
   *
   * @param connection  The {@link Connection} to the HBase cluster. MUST be unmanaged. We will close.
   * @param tableName  The {@link TableName} of the table to process. 
   * @throws IOException 
   */
  protected void initializeTable(Connection connection, TableName tableName) throws IOException {
    if (this.table != null || this.connection != null) {
      LOG.warn("initializeTable called multiple times. Overwriting connection and table " +
          "reference; TableInputFormatBase will not close these old references when done.");
    }
    this.table = connection.getTable(tableName);
    this.regionLocator = connection.getRegionLocator(tableName);
    this.admin = connection.getAdmin();
    this.connection = connection;
  }

  /**
   * Gets the scan defining the actual details like columns etc.
   *
   * @return The internal scan instance.
   */
  public Scan getScan() {
    if (this.scan == null) this.scan = new Scan();
    return scan;
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan  The scan to set.
   */
  public void setScan(Scan scan) {
    this.scan = scan;
  }

  /**
   * Allows subclasses to set the {@link TableRecordReader}.
   *
   * @param tableRecordReader A different {@link TableRecordReader}
   *   implementation.
   */
  protected void setTableRecordReader(TableRecordReader tableRecordReader) {
    this.tableRecordReader = tableRecordReader;
  }
  
  /**
   * Handle subclass specific set up.
   * Each of the entry points used by the MapReduce framework,
   * {@link #createRecordReader(InputSplit, TaskAttemptContext)} and {@link #getSplits(JobContext)},
   * will call {@link #initialize(JobContext)} as a convenient centralized location to handle
   * retrieving the necessary configuration information and calling
   * {@link #initializeTable(Connection, TableName)}.
   *
   * Subclasses should implement their initialize call such that it is safe to call multiple times.
   * The current TableInputFormatBase implementation relies on a non-null table reference to decide
   * if an initialize call is needed, but this behavior may change in the future. In particular,
   * it is critical that initializeTable not be called multiple times since this will leak
   * Connection instances.
   *
   */
  protected void initialize(JobContext context) throws IOException {
  }

  /**
   * Close the Table and related objects that were initialized via
   * {@link #initializeTable(Connection, TableName)}.
   *
   * @throws IOException
   */
  protected void closeTable() throws IOException {
    close(admin, table, regionLocator, connection);
    admin = null;
    table = null;
    regionLocator = null;
    connection = null;
  }

  private void close(Closeable... closables) throws IOException {
    for (Closeable c : closables) {
      if(c != null) { c.close(); }
    }
  }

}
