/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.DNS;

/**
 * A base for {@link TableInputFormat}s. Receives a {@link HTable}, an
 * {@link Scan} instance that defines the input columns etc. Subclasses may use
 * other TableRecordReader implementations.
 */
public abstract class TableInputFormatBase
extends InputFormat<ImmutableBytesWritable, Result> {

  private static final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

  /** Holds the details for the internal scanner. */
  private Scan scan = null;
  /** The table to scan. */
  private HTable table = null;
  /** The reader scanning the table, can be a custom one. */
  private TableRecordReader tableRecordReader = null;
  /** The number of mappers to assign to each region. */
  private int numMappers = 1;
  /** The total number of mappers. The number of mappers per region is
   * mutually exclusive with number of mappers per job. In case both
   * are defined, mappersPerJob will take the precedence.*/
  private boolean mappersPerJob = false;
  /** Splitting algorithm to be used to split the keys */
  private String splitAlgmName; // default to Uniform

  /** The reverse DNS lookup cache mapping: IPAddress => HostName */
  private static ConcurrentHashMap<InetAddress, String> reverseDNSCacheMap =
      new ConcurrentHashMap<InetAddress, String>();
  
  /** The NameServer address */
  private static String nameServer = null;
  
  /**
   * Builds a TableRecordReader. If no TableRecordReader was provided, uses
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
    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = this.tableRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader();
    }
    Scan sc = new Scan(this.scan);
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setHTable(table);
    trr.init(context.getConfiguration());
    return trr;
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
    return getSplitsInternal(table, context.getConfiguration(), scan, numMappers,
      mappersPerJob, splitAlgmName, this);
  }

  public static List<InputSplit> getSplitsInternal(HTable table,
      Configuration conf,
      Scan scan,
      int numMappers,
      boolean mappersPerJob,
      String splitAlgmName,
      TableInputFormatBase tifb) throws IOException {
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    determineNameServer(conf);

    if (mappersPerJob) {
      return getSplitsInternalBasedOnNumMappersPerJob(table, conf, scan, numMappers,
        splitAlgmName);
    }

    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
    if (keys == null || keys.getFirst() == null ||
        keys.getFirst().length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    Pair<byte[][], byte[][]> splitKeys = null;
    int numRegions = keys.getFirst().length;
    //TODO: Can anything else be done when there are less than 3 regions?
    if ((numMappers == 1) || (numRegions < 3)) {
      numMappers = 1;
      splitKeys = keys;
    } else {
      byte[][] startKeys = new byte[numRegions * numMappers][];
      byte[][] stopKeys = new byte[numRegions * numMappers][];
      // Insert null keys at edges
      startKeys[0] = HConstants.EMPTY_START_ROW;
      stopKeys[numRegions * numMappers - 1] = HConstants.EMPTY_END_ROW;

      byte[][] originalStartKeys = keys.getFirst();
      byte[][] originalStopKeys = keys.getSecond();
      SplitAlgorithm algmImpl;

      for (int i = 0; i < originalStartKeys.length; i++) {
        // get a new instance each time
        algmImpl = RegionSplitter.newSplitAlgoInstance(conf,
            splitAlgmName);
        if (originalStartKeys[i].length != 0)
          algmImpl.setFirstRow(algmImpl.rowToStr(originalStartKeys[i]));
        if (originalStopKeys[i].length != 0)
          algmImpl.setLastRow(algmImpl.rowToStr(originalStopKeys[i]));
        byte[][] dividingKeys = algmImpl.split(numMappers);

        startKeys[i*numMappers] = originalStartKeys[i];
        for (int j = 0; j < numMappers - 1; j++) {
          stopKeys[i * numMappers + j] = dividingKeys[j];
          startKeys[i * numMappers + j + 1] = dividingKeys[j];
        }
        stopKeys[(i+1)*numMappers - 1] = originalStopKeys[i];
      }
      splitKeys = new Pair<byte[][], byte[][]>();
      splitKeys.setFirst(startKeys);
      splitKeys.setSecond(stopKeys);
    }

    List<InputSplit> splits =
        new ArrayList<InputSplit>(numRegions * numMappers);
    byte[] startRow = scan.getStartRow();
    byte[] stopRow = scan.getStopRow();
    for (int i = 0; i < numRegions * numMappers; i++) {
      if (tifb != null && !tifb.includeRegionInSplit(keys.getFirst()[i / numMappers],
          keys.getSecond()[i / numMappers])) {
        continue;
      }
      HServerAddress regionServerAddress = 
        table.getRegionLocation(splitKeys.getFirst()[i]).getServerAddress();
      InetAddress regionAddress =
        regionServerAddress.getInetSocketAddress().getAddress();
      String regionLocation;
      try {
        regionLocation = reverseDNS(regionAddress);
      } catch (NamingException e) {
        LOG.error("Cannot resolve the host name for " + regionAddress +
            " because of " + e);
        regionLocation = regionServerAddress.getHostname();
      }
      
      // determine if the given start an stop key fall into the region
      if ((startRow.length == 0 || splitKeys.getSecond()[i].length == 0 ||
          Bytes.compareTo(startRow, splitKeys.getSecond()[i]) < 0) &&
          (stopRow.length == 0 ||
          Bytes.compareTo(stopRow, splitKeys.getFirst()[i]) > 0)) {
        byte[] splitStart = startRow.length == 0 ||
            Bytes.compareTo(splitKeys.getFirst()[i], startRow) >= 0 ?
                splitKeys.getFirst()[i] : startRow;
        byte[] splitStop = (stopRow.length == 0 ||
            Bytes.compareTo(splitKeys.getSecond()[i], stopRow) <= 0) &&
            splitKeys.getSecond()[i].length > 0 ?
                splitKeys.getSecond()[i] : stopRow;
        InputSplit split = new TableSplit(table.getTableName(),
            splitStart, splitStop, regionLocation);
        splits.add(split);
        if (LOG.isDebugEnabled())
          LOG.debug("getSplits: split -> " + i + " -> " + split);
      }
    }

    HConnectionManager.deleteAllZookeeperConnections();
    return splits;
  }

  public static List<InputSplit> getSplitsInternalBasedOnNumMappersPerJob(
    HTable table, Configuration conf, Scan scan, int numMappersPerJob,
    String splitAlgmName) throws IOException {
    List<InputSplit> splits =
      new ArrayList<InputSplit>(numMappersPerJob);
    byte[] startRow = scan.getStartRow();
    byte[] stopRow = scan.getStopRow();

    SplitAlgorithm algmImpl = RegionSplitter.newSplitAlgoInstance(conf,
      splitAlgmName);
    if (startRow.length != 0)
      algmImpl.setFirstRow(algmImpl.rowToStr(startRow));
    if (stopRow.length != 0)
      algmImpl.setLastRow(algmImpl.rowToStr(stopRow));

    // This will return numMappersPerJob - 1 split interval
    byte[][] dividingKeys = algmImpl.split(numMappersPerJob);

    byte[] splitStartKey = startRow;
    byte[] splitStopKey = stopRow;

    TreeMap<byte[], byte[]> regions = table.getStartEndKeysMap();

    // No splits possible, just default it to 1
    if (dividingKeys == null || dividingKeys.length == 0) {
      byte[] startRowOfFullRegion = getNearestFullRegion(regions, splitStartKey,
        splitStopKey);
      InputSplit split = new TableSplit(table.getTableName(),
        splitStartKey, splitStopKey, getRegionServerAddress(table,
          startRowOfFullRegion));
      if (LOG.isDebugEnabled()) {
        LOG.debug("getSplits: split -> " + split);
      }
      splits.add(split);
    } else {
      for (int i = 0; i <= dividingKeys.length; i++) {
        if (i == dividingKeys.length) {
          splitStopKey = stopRow;
        } else {
          splitStopKey = dividingKeys[i];
        }
        byte[] startRowOfFullRegion = getNearestFullRegion(regions, splitStartKey,
          splitStopKey);
        InputSplit split = new TableSplit(table.getTableName(),
          splitStartKey, splitStopKey, getRegionServerAddress(table,
            startRowOfFullRegion));
        splits.add(split);
        if (LOG.isDebugEnabled()) {
          LOG.debug("getSplits: split -> " + i + " -> " + split);
        }
        splitStartKey = splitStopKey;
      }
    }
    HConnectionManager.deleteAllZookeeperConnections();
    return splits;
  }

  private static byte[] getNearestFullRegion(
    final TreeMap<byte[], byte[]> regions,
    final byte[] startKey, final byte[] stopKey) {

    Map.Entry<byte[], byte[]> region = regions.ceilingEntry(startKey);
    if (region != null) {
      if (Bytes.compareTo(region.getValue(), stopKey) < 0) {
        // The given range spans at least one complete region. Return
        // the start row for this region
        return region.getKey();
      }
    }
    return startKey;
  }

  private static synchronized void determineNameServer(Configuration conf) {
    // Get the name server address and the default value is null.
    if (nameServer == null) {
      nameServer = conf.get("hbase.nameserver.address", null);
    }
  }

  private static String getRegionServerAddress(final HTable table, final byte[] row)
    throws IOException {
    String regionLocation = null;
    HServerAddress regionServerAddress =
      table.getRegionLocation(row).getServerAddress();
    InetAddress regionAddress = null;
    try {
      regionAddress =
        regionServerAddress.getInetSocketAddress().getAddress();
      regionLocation = reverseDNS(regionAddress);
    } catch (NamingException e) {
      LOG.error("Cannot resolve the host name for " + regionAddress +
          " because of " + e);
      regionLocation = regionServerAddress.getHostname();
    }
    return regionLocation;
  }

  private static String reverseDNS(InetAddress ipAddress)
  throws NamingException {
    String hostName = reverseDNSCacheMap.get(ipAddress);
    if (hostName == null) {
      hostName = DNS.reverseDns(ipAddress, nameServer);
      reverseDNSCacheMap.put(ipAddress, hostName);
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
   */
  protected HTable getHTable() {
    return this.table;
  }

  /**
   * Allows subclasses to set the {@link HTable}.
   *
   * @param table  The table to get the data from.
   */
  protected void setHTable(HTable table) {
    this.table = table;
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
   * Sets the number of mappers assigned to each region.
   *
   * @param num
   * @throws IllegalArgumentException When <code>num</code> <= 0.
   */
  public void setNumMapperPerRegion(int num) throws IllegalArgumentException {
    if (num <= 0) {
      throw new IllegalArgumentException("Expecting at least 1 mapper " +
          "per region; instead got: " + num);
    }
    if (!mappersPerJob) {
      numMappers = num;
    } else {
      LOG.warn("Ingoring mappersPerRegion config value as mappersPerJob is" +
        " already set.");
    }
  }

  /**
   * Sets the number of mappers assigned for the job.
   *
   * @param num
   * @throws IllegalArgumentException When <code>num</code> <= 0.
   */
  public void setNumMappersPerJob(int num) throws IllegalArgumentException {
    if (num <= 0) {
      throw new IllegalArgumentException("Expecting at least 1 mapper " +
        "per job; instead got: " + num);
    }
    if (numMappers > 1) {
      LOG.warn("Overriding num of mappers per region.");
    }
    numMappers = num;
    mappersPerJob = true;
  }

  public void setSplitAlgorithm(String name) {
    this.splitAlgmName = name;
  }
}
