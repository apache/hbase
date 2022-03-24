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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 *
 * The number of InputSplits(mappers) match the number of regions in a table by default.
 * Set "hbase.mapreduce.tableinput.mappers.per.region" to specify how many mappers per region, set
 * this property will disable autobalance below.\
 * Set "hbase.mapreduce.tif.input.autobalance" to enable autobalance, hbase will assign mappers
 * based on average region size; For regions, whose size larger than average region size may assigned
 * more mappers, and for smaller one, they may group together to use one mapper. If actual average
 * region size is too big, like 50G, it is not good to only assign 1 mapper for those large regions.
 * Use "hbase.mapreduce.tif.ave.regionsize" to set max average region size when enable "autobalanece",
 * default mas average region size is 8G.
 */
@InterfaceAudience.Public
public abstract class TableInputFormatBase
    extends InputFormat<ImmutableBytesWritable, Result> {

  private static final Logger LOG = LoggerFactory.getLogger(TableInputFormatBase.class);

  private static final String NOT_INITIALIZED = "The input format instance has not been properly " +
      "initialized. Ensure you call initializeTable either in your constructor or initialize " +
      "method";
  private static final String INITIALIZATION_ERROR = "Cannot create a record reader because of a" +
      " previous error. Please look at the previous logs lines from" +
      " the task's full log for more details.";

  /** Specify if we enable auto-balance to set number of mappers in M/R jobs. */
  public static final String MAPREDUCE_INPUT_AUTOBALANCE = "hbase.mapreduce.tif.input.autobalance";
  /** In auto-balance, we split input by ave region size, if calculated region size is too big, we can set it. */
  public static final String MAX_AVERAGE_REGION_SIZE = "hbase.mapreduce.tif.ave.regionsize";

  /** Set the number of Mappers for each region, all regions have same number of Mappers */
  public static final String NUM_MAPPERS_PER_REGION = "hbase.mapreduce.tableinput.mappers.per.region";


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
  /** Used to generate splits based on region size. */
  private RegionSizeCalculator regionSizeCalculator;


  /** The reverse DNS lookup cache mapping: IPAddress => HostName */
  private HashMap<InetAddress, String> reverseDNSCacheMap =
      new HashMap<>();

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
   * Calculates the splits that will serve as input for the map tasks.
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
      List<InputSplit> splits = oneInputSplitPerRegion();

      // set same number of mappers for each region
      if (context.getConfiguration().get(NUM_MAPPERS_PER_REGION) != null) {
        int nSplitsPerRegion = context.getConfiguration().getInt(NUM_MAPPERS_PER_REGION, 1);
        List<InputSplit> res = new ArrayList<>();
        for (int i = 0; i < splits.size(); i++) {
          List<InputSplit> tmp = createNInputSplitsUniform(splits.get(i), nSplitsPerRegion);
          res.addAll(tmp);
        }
        return res;
      }

      //The default value of "hbase.mapreduce.input.autobalance" is false.
      if (context.getConfiguration().getBoolean(MAPREDUCE_INPUT_AUTOBALANCE, false)) {
        long maxAveRegionSize = context.getConfiguration()
            .getLong(MAX_AVERAGE_REGION_SIZE, 8L*1073741824); //8GB
        return calculateAutoBalancedSplits(splits, maxAveRegionSize);
      }

      // return one mapper per region
      return splits;
    } finally {
      if (closeOnFinish) {
        closeTable();
      }
    }
  }

  /**
   * Create one InputSplit per region
   *
   * @return The list of InputSplit for all the regions
   * @throws IOException throws IOException
   */
  private List<InputSplit> oneInputSplitPerRegion() throws IOException {
    if (regionSizeCalculator == null) {
      // Initialize here rather than with the other resources because this involves
      // a full scan of meta, which can be heavy. We might as well only do it if/when necessary.
      regionSizeCalculator = createRegionSizeCalculator(getRegionLocator(), getAdmin());
    }

    TableName tableName = getTable().getName();

    Pair<byte[][], byte[][]> keys = getStartEndKeys();
    if (keys == null || keys.getFirst() == null ||
        keys.getFirst().length == 0) {
      HRegionLocation regLoc =
          getRegionLocator().getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);
      if (null == regLoc) {
        throw new IOException("Expecting at least one region.");
      }
      List<InputSplit> splits = new ArrayList<>(1);
      long regionSize = regionSizeCalculator.getRegionSize(regLoc.getRegion().getRegionName());
      // In the table input format for single table we do not need to
      // store the scan object in table split because it can be memory intensive and redundant
      // information to what is already stored in conf SCAN. See HBASE-25212
      TableSplit split = new TableSplit(tableName, null,
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, regLoc
          .getHostnamePort().split(Addressing.HOSTNAME_PORT_SEPARATOR)[0], regionSize);
      splits.add(split);
      return splits;
    }
    List<InputSplit> splits = new ArrayList<>(keys.getFirst().length);
    for (int i = 0; i < keys.getFirst().length; i++) {
      if (!includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
        continue;
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

        HRegionLocation location = getRegionLocator().getRegionLocation(keys.getFirst()[i], false);
        // The below InetSocketAddress creation does a name resolution.
        InetSocketAddress isa = new InetSocketAddress(location.getHostname(), location.getPort());
        if (isa.isUnresolved()) {
          LOG.warn("Failed resolve " + isa);
        }
        InetAddress regionAddress = isa.getAddress();
        String regionLocation;
        regionLocation = reverseDNS(regionAddress);

        byte[] regionName = location.getRegion().getRegionName();
        String encodedRegionName = location.getRegion().getEncodedName();
        long regionSize = regionSizeCalculator.getRegionSize(regionName);
        // In the table input format for single table we do not need to
        // store the scan object in table split because it can be memory intensive and redundant
        // information to what is already stored in conf SCAN. See HBASE-25212
        TableSplit split = new TableSplit(tableName, null,
            splitStart, splitStop, regionLocation, encodedRegionName, regionSize);
        splits.add(split);
        if (LOG.isDebugEnabled()) {
          LOG.debug("getSplits: split -> " + i + " -> " + split);
        }
      }
    }
    return splits;
  }

  /**
   * Create n splits for one InputSplit, For now only support uniform distribution
   * @param split A TableSplit corresponding to a range of rowkeys
   * @param n     Number of ranges after splitting.  Pass 1 means no split for the range
   *              Pass 2 if you want to split the range in two;
   * @return A list of TableSplit, the size of the list is n
   * @throws IllegalArgumentIOException throws IllegalArgumentIOException
   */
  protected List<InputSplit> createNInputSplitsUniform(InputSplit split, int n)
      throws IllegalArgumentIOException {
    if (split == null || !(split instanceof TableSplit)) {
      throw new IllegalArgumentIOException(
          "InputSplit for CreateNSplitsPerRegion can not be null + "
              + "and should be instance of TableSplit");
    }
    //if n < 1, then still continue using n = 1
    n = n < 1 ? 1 : n;
    List<InputSplit> res = new ArrayList<>(n);
    if (n == 1) {
      res.add(split);
      return res;
    }

    // Collect Region related information
    TableSplit ts = (TableSplit) split;
    TableName tableName = ts.getTable();
    String regionLocation = ts.getRegionLocation();
    String encodedRegionName = ts.getEncodedRegionName();
    long regionSize = ts.getLength();
    byte[] startRow = ts.getStartRow();
    byte[] endRow = ts.getEndRow();

    // For special case: startRow or endRow is empty
    if (startRow.length == 0 && endRow.length == 0){
      startRow = new byte[1];
      endRow = new byte[1];
      startRow[0] = 0;
      endRow[0] = -1;
    }
    if (startRow.length == 0 && endRow.length != 0){
      startRow = new byte[1];
      startRow[0] = 0;
    }
    if (startRow.length != 0 && endRow.length == 0){
      endRow =new byte[startRow.length];
      for (int k = 0; k < startRow.length; k++){
        endRow[k] = -1;
      }
    }

    // Split Region into n chunks evenly
    byte[][] splitKeys = Bytes.split(startRow, endRow, true, n-1);
    for (int i = 0; i < splitKeys.length - 1; i++) {
      // In the table input format for single table we do not need to
      // store the scan object in table split because it can be memory intensive and redundant
      // information to what is already stored in conf SCAN. See HBASE-25212
      //notice that the regionSize parameter may be not very accurate
      TableSplit tsplit =
          new TableSplit(tableName, null, splitKeys[i], splitKeys[i + 1], regionLocation,
              encodedRegionName, regionSize / n);
      res.add(tsplit);
    }
    return res;
  }
  /**
   * Calculates the number of MapReduce input splits for the map tasks. The number of
   * MapReduce input splits depends on the average region size.
   * Make it 'public' for testing
   *
   * @param splits The list of input splits before balance.
   * @param maxAverageRegionSize max Average region size for one mapper
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *org.apache.hadoop.mapreduce.JobContext)
   */
  public List<InputSplit> calculateAutoBalancedSplits(List<InputSplit> splits, long maxAverageRegionSize)
      throws IOException {
    if (splits.size() == 0) {
      return splits;
    }
    List<InputSplit> resultList = new ArrayList<>();
    long totalRegionSize = 0;
    for (int i = 0; i < splits.size(); i++) {
      TableSplit ts = (TableSplit) splits.get(i);
      totalRegionSize += ts.getLength();
    }
    long averageRegionSize = totalRegionSize / splits.size();
    // totalRegionSize might be overflow, and the averageRegionSize must be positive.
    if (averageRegionSize <= 0) {
      LOG.warn("The averageRegionSize is not positive: " + averageRegionSize + ", " +
          "set it to Long.MAX_VALUE " + splits.size());
      averageRegionSize = Long.MAX_VALUE / splits.size();
    }
    //if averageRegionSize is too big, change it to default as 1 GB,
    if (averageRegionSize > maxAverageRegionSize) {
      averageRegionSize = maxAverageRegionSize;
    }
    // if averageRegionSize is too small, we do not need to allocate more mappers for those 'large' region
    // set default as 16M = (default hdfs block size) / 4;
    if (averageRegionSize < 16 * 1048576) {
      return splits;
    }
    for (int i = 0; i < splits.size(); i++) {
      TableSplit ts = (TableSplit) splits.get(i);
      TableName tableName = ts.getTable();
      String regionLocation = ts.getRegionLocation();
      String encodedRegionName = ts.getEncodedRegionName();
      long regionSize = ts.getLength();

      if (regionSize >= averageRegionSize) {
        // make this region as multiple MapReduce input split.
        int n = (int) Math.round(Math.log(((double) regionSize) / ((double) averageRegionSize)) + 1.0);
        List<InputSplit> temp = createNInputSplitsUniform(ts, n);
        resultList.addAll(temp);
      } else {
        // if the total size of several small continuous regions less than the average region size,
        // combine them into one MapReduce input split.
        long totalSize = regionSize;
        byte[] splitStartKey = ts.getStartRow();
        byte[] splitEndKey = ts.getEndRow();
        int j = i + 1;
        while (j < splits.size()) {
          TableSplit nextRegion = (TableSplit) splits.get(j);
          long nextRegionSize = nextRegion.getLength();
          if (totalSize + nextRegionSize <= averageRegionSize
              && Bytes.equals(splitEndKey, nextRegion.getStartRow())) {
            totalSize = totalSize + nextRegionSize;
            splitEndKey = nextRegion.getEndRow();
            j++;
          } else {
            break;
          }
        }
        i = j - 1;
        // In the table input format for single table we do not need to
        // store the scan object in table split because it can be memory intensive and redundant
        // information to what is already stored in conf SCAN. See HBASE-25212
        TableSplit t = new TableSplit(tableName, null, splitStartKey, splitEndKey, regionLocation,
            encodedRegionName, totalSize);
        resultList.add(t);
      }
    }
    return resultList;
  }

  String reverseDNS(InetAddress ipAddress) throws UnknownHostException {
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
      if (ipAddressString == null) {
        throw new UnknownHostException("No host found for " + ipAddress);
      }
      hostName = Strings.domainNamePointerToHostName(ipAddressString);
      this.reverseDNSCacheMap.put(ipAddress, hostName);
    }
    return hostName;
  }

  /**
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
   * Allows subclasses to initialize the table information.
   *
   * @param connection  The Connection to the HBase cluster. MUST be unmanaged. We will close.
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
    this.regionSizeCalculator = null;
  }

  @InterfaceAudience.Private
  protected RegionSizeCalculator createRegionSizeCalculator(RegionLocator locator, Admin admin)
      throws IOException {
    return new RegionSizeCalculator(locator, admin);
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
    regionSizeCalculator = null;
  }

  private void close(Closeable... closables) throws IOException {
    for (Closeable c : closables) {
      if(c != null) { c.close(); }
    }
  }

}
