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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
public abstract class TableInputFormatBase
extends InputFormat<ImmutableBytesWritable, Result> {

  /** Specify if we enable auto-balance for input in M/R jobs.*/
  public static final String MAPREDUCE_INPUT_AUTOBALANCE = "hbase.mapreduce.input.autobalance";
  /** Specify if ratio for data skew in M/R jobs, it goes well with the enabling hbase.mapreduce
   * .input.autobalance property.*/
  public static final String INPUT_AUTOBALANCE_MAXSKEWRATIO = "hbase.mapreduce.input.autobalance" +
          ".maxskewratio";
  /** Specify if the row key in table is text (ASCII between 32~126),
   * default is true. False means the table is using binary row key*/
  public static final String TABLE_ROW_TEXTKEY = "hbase.table.row.textkey";

  private static final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

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
  private HashMap<InetAddress, String> reverseDNSCacheMap = new HashMap<>();

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
      RegionSizeCalculator sizeCalculator =
          new RegionSizeCalculator(getRegionLocator(), getAdmin());

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
        long regionSize = sizeCalculator.getRegionSize(regLoc.getRegionInfo().getRegionName());
        TableSplit split = new TableSplit(tableName, scan,
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

          byte[] regionName = location.getRegionInfo().getRegionName();
          String encodedRegionName = location.getRegionInfo().getEncodedName();
          long regionSize = sizeCalculator.getRegionSize(regionName);
          TableSplit split = new TableSplit(tableName, scan,
            splitStart, splitStop, regionLocation, encodedRegionName, regionSize);
          splits.add(split);
          if (LOG.isDebugEnabled()) {
            LOG.debug("getSplits: split -> " + i + " -> " + split);
          }
        }
      }
      //The default value of "hbase.mapreduce.input.autobalance" is false, which means not enabled.
      boolean enableAutoBalance = context.getConfiguration()
        .getBoolean(MAPREDUCE_INPUT_AUTOBALANCE, false);
      if (enableAutoBalance) {
        long totalRegionSize=0;
        for (int i = 0; i < splits.size(); i++){
          TableSplit ts = (TableSplit)splits.get(i);
          totalRegionSize += ts.getLength();
        }
        long averageRegionSize = totalRegionSize / splits.size();
        // the averageRegionSize must be positive.
        if (averageRegionSize <= 0) {
            LOG.warn("The averageRegionSize is not positive: "+ averageRegionSize + ", " +
                    "set it to 1.");
            averageRegionSize = 1;
        }
        return calculateRebalancedSplits(splits, context, averageRegionSize);
      } else {
        return splits;
      }
    } finally {
      if (closeOnFinish) {
        closeTable();
      }
    }
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
      if (ipAddressString == null) throw new UnknownHostException("No host found for " + ipAddress);
      hostName = Strings.domainNamePointerToHostName(ipAddressString);
      this.reverseDNSCacheMap.put(ipAddress, hostName);
    }
    return hostName;
  }

  /**
   * Calculates the number of MapReduce input splits for the map tasks. The number of
   * MapReduce input splits depends on the average region size and the "data skew ratio" user set in
   * configuration.
   *
   * @param list  The list of input splits before balance.
   * @param context  The current job context.
   * @param average  The average size of all regions .
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *   org.apache.hadoop.mapreduce.JobContext)
   */
  private List<InputSplit> calculateRebalancedSplits(List<InputSplit> list, JobContext context,
                                               long average) throws IOException {
    List<InputSplit> resultList = new ArrayList<>();
    Configuration conf = context.getConfiguration();
    //The default data skew ratio is 3
    long dataSkewRatio = conf.getLong(INPUT_AUTOBALANCE_MAXSKEWRATIO, 3);
    //It determines which mode to use: text key mode or binary key mode. The default is text mode.
    boolean isTextKey = context.getConfiguration().getBoolean(TABLE_ROW_TEXTKEY, true);
    long dataSkewThreshold = dataSkewRatio * average;
    int count = 0;
    while (count < list.size()) {
      TableSplit ts = (TableSplit)list.get(count);
      TableName tableName = ts.getTable();
      String regionLocation = ts.getRegionLocation();
      String encodedRegionName = ts.getEncodedRegionName();
      long regionSize = ts.getLength();
      if (regionSize >= dataSkewThreshold) {
        // if the current region size is large than the data skew threshold,
        // split the region into two MapReduce input splits.
        byte[] splitKey = getSplitKey(ts.getStartRow(), ts.getEndRow(), isTextKey);
        if (Arrays.equals(ts.getEndRow(), splitKey)) {
          // Not splitting since the end key is the same as the split key
          resultList.add(ts);
        } else {
          //Set the size of child TableSplit as 1/2 of the region size. The exact size of the
          // MapReduce input splits is not far off.
          TableSplit t1 = new TableSplit(tableName, scan, ts.getStartRow(), splitKey,
              regionLocation, regionSize / 2);
          TableSplit t2 = new TableSplit(tableName, scan, splitKey, ts.getEndRow(), regionLocation,
              regionSize - regionSize / 2);
          resultList.add(t1);
          resultList.add(t2);
        }
        count++;
      } else if (regionSize >= average) {
        // if the region size between average size and data skew threshold size,
        // make this region as one MapReduce input split.
        resultList.add(ts);
        count++;
      } else {
        // if the total size of several small continuous regions less than the average region size,
        // combine them into one MapReduce input split.
        long totalSize = regionSize;
        byte[] splitStartKey = ts.getStartRow();
        byte[] splitEndKey = ts.getEndRow();
        count++;
        for (; count < list.size(); count++) {
          TableSplit nextRegion = (TableSplit)list.get(count);
          long nextRegionSize = nextRegion.getLength();
          if (totalSize + nextRegionSize <= dataSkewThreshold) {
            totalSize = totalSize + nextRegionSize;
            splitEndKey = nextRegion.getEndRow();
          } else {
            break;
          }
        }
        TableSplit t = new TableSplit(tableName, scan, splitStartKey, splitEndKey,
                regionLocation, encodedRegionName, totalSize);
        resultList.add(t);
      }
    }
    return resultList;
  }

  /**
   * select a split point in the region. The selection of the split point is based on an uniform
   * distribution assumption for the keys in a region.
   * Here are some examples:
   *
   * <table>
   *   <tr>
   *     <th>start key</th>
   *     <th>end key</th>
   *     <th>is text</th>
   *     <th>split point</th>
   *   </tr>
   *   <tr>
   *     <td>'a', 'a', 'a', 'b', 'c', 'd', 'e', 'f', 'g'</td>
   *     <td>'a', 'a', 'a', 'f', 'f', 'f'</td>
   *     <td>true</td>
   *     <td>'a', 'a', 'a', 'd', 'd', -78, 50, -77, 51</td>
   *   </tr>
   *   <tr>
   *     <td>'1', '1', '1', '0', '0', '0'</td>
   *     <td>'1', '1', '2', '5', '7', '9', '0'</td>
   *     <td>true</td>
   *     <td>'1', '1', '1', -78, -77, -76, -104</td>
   *   </tr>
   *   <tr>
   *     <td>'1', '1', '1', '0'</td>
   *     <td>'1', '1', '2', '0'</td>
   *     <td>true</td>
   *     <td>'1', '1', '1', -80</td>
   *   </tr>
   *   <tr>
   *     <td>13, -19, 126, 127</td>
   *     <td>13, -19, 127, 0</td>
   *     <td>false</td>
   *     <td>13, -19, 126, -65</td>
   *   </tr>
   * </table>
   *
   * Set this function as "public static", make it easier for test.
   *
   * @param start Start key of the region
   * @param end End key of the region
   * @param isText It determines to use text key mode or binary key mode
   * @return The split point in the region.
   */
  @InterfaceAudience.Private
  public static byte[] getSplitKey(byte[] start, byte[] end, boolean isText) {
    byte upperLimitByte;
    byte lowerLimitByte;
    //Use text mode or binary mode.
    if (isText) {
      //The range of text char set in ASCII is [32,126], the lower limit is space and the upper
      // limit is '~'.
      upperLimitByte = '~';
      lowerLimitByte = ' ';
    } else {
      upperLimitByte = -1;
      lowerLimitByte = 0;
    }
    // For special case
    // Example 1 : startkey=null, endkey="hhhqqqwww", splitKey="h"
    // Example 2 (text key mode): startKey="ffffaaa", endKey=null, splitkey="f~~~~~~"
    if (start.length == 0 && end.length == 0){
      return new byte[]{(byte) ((lowerLimitByte + upperLimitByte) / 2)};
    }
    if (start.length == 0 && end.length != 0){
      return new byte[]{ end[0] };
    }
    if (start.length != 0 && end.length == 0){
      byte[] result =new byte[start.length];
      result[0]=start[0];
      for (int k = 1; k < start.length; k++){
          result[k] = upperLimitByte;
      }
      return result;
    }
    return Bytes.split(start, end, false, 1)[1];
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
