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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TableInputFormat extends TableInputFormatBase
implements Configurable {

  @SuppressWarnings("hiding")
  private static final Log LOG = LogFactory.getLog(TableInputFormat.class);

  /** Job parameter that specifies the input table. */
  public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
  /**
   * If specified, use start keys of this table to split.
   * This is useful when you are preparing data for bulkload.
   */
  private static final String SPLIT_TABLE = "hbase.mapreduce.splittable";
  /** Base-64 encoded scanner. All other SCAN_ confs are ignored if this is specified.
   * See {@link TableMapReduceUtil#convertScanToString(Scan)} for more details.
   */
  public static final String SCAN = "hbase.mapreduce.scan";
  /** Scan start row */
  public static final String SCAN_ROW_START = "hbase.mapreduce.scan.row.start";
  /** Scan stop row */
  public static final String SCAN_ROW_STOP = "hbase.mapreduce.scan.row.stop";
  /** Column Family to Scan */
  public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
  /** Space delimited list of columns and column families to scan. */
  public static final String SCAN_COLUMNS = "hbase.mapreduce.scan.columns";
  /** The timestamp used to filter columns with a specific timestamp. */
  public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
  /** The starting timestamp used to filter columns with a specific range of versions. */
  public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
  /** The ending timestamp used to filter columns with a specific range of versions. */
  public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
  /** The maximum number of version to return. */
  public static final String SCAN_MAXVERSIONS = "hbase.mapreduce.scan.maxversions";
  /** Set to false to disable server-side caching of blocks for this scan. */
  public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";
  /** The number of rows for caching that will be passed to scanners. */
  public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";
  /** Set the maximum number of values to return for each call to next(). */
  public static final String SCAN_BATCHSIZE = "hbase.mapreduce.scan.batchsize";
  /** Specify if we have to shuffle the map tasks. */
  public static final String SHUFFLE_MAPS = "hbase.mapreduce.inputtable.shufflemaps";

  /** The configuration. */
  private Configuration conf = null;

  /**
   * Returns the current configuration.
   *
   * @return The current configuration.
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Sets the configuration. This is used to set the details for the table to
   * be scanned.
   *
   * @param configuration  The configuration to set.
   * @see org.apache.hadoop.conf.Configurable#setConf(
   *   org.apache.hadoop.conf.Configuration)
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intentional")
  public void setConf(Configuration configuration) {
    this.conf = configuration;

    Scan scan = null;

    if (conf.get(SCAN) != null) {
      try {
        scan = TableMapReduceUtil.convertStringToScan(conf.get(SCAN));
      } catch (IOException e) {
        LOG.error("An error occurred.", e);
      }
    } else {
      try {
        scan = createScanFromConfiguration(conf);
      } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
      }
    }

    setScan(scan);
  }

  /**
   * Sets up a {@link Scan} instance, applying settings from the configuration property
   * constants defined in {@code TableInputFormat}.  This allows specifying things such as:
   * <ul>
   *   <li>start and stop rows</li>
   *   <li>column qualifiers or families</li>
   *   <li>timestamps or timerange</li>
   *   <li>scanner caching and batch size</li>
   * </ul>
   */
  public static Scan createScanFromConfiguration(Configuration conf) throws IOException {
    Scan scan = new Scan();

    if (conf.get(SCAN_ROW_START) != null) {
      scan.setStartRow(Bytes.toBytesBinary(conf.get(SCAN_ROW_START)));
    }

    if (conf.get(SCAN_ROW_STOP) != null) {
      scan.setStopRow(Bytes.toBytesBinary(conf.get(SCAN_ROW_STOP)));
    }

    if (conf.get(SCAN_COLUMNS) != null) {
      addColumns(scan, conf.get(SCAN_COLUMNS));
    }

    for (String columnFamily : conf.getTrimmedStrings(SCAN_COLUMN_FAMILY)) {
      scan.addFamily(Bytes.toBytes(columnFamily));
    }

    if (conf.get(SCAN_TIMESTAMP) != null) {
      scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
    }

    if (conf.get(SCAN_TIMERANGE_START) != null && conf.get(SCAN_TIMERANGE_END) != null) {
      scan.setTimeRange(
          Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
          Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
    }

    if (conf.get(SCAN_MAXVERSIONS) != null) {
      scan.setMaxVersions(Integer.parseInt(conf.get(SCAN_MAXVERSIONS)));
    }

    if (conf.get(SCAN_CACHEDROWS) != null) {
      scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
    }

    if (conf.get(SCAN_BATCHSIZE) != null) {
      scan.setBatch(Integer.parseInt(conf.get(SCAN_BATCHSIZE)));
    }

    // false by default, full table scans generate too much BC churn
    scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));

    return scan;
  }

  @Override
  protected void initialize(JobContext context) throws IOException {
    // Do we have to worry about mis-matches between the Configuration from setConf and the one
    // in this context?
    TableName tableName = TableName.valueOf(conf.get(INPUT_TABLE));
    try {
      initializeTable(ConnectionFactory.createConnection(new Configuration(conf)), tableName);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  /**
   * Parses a combined family and qualifier and adds either both or just the
   * family in case there is no qualifier. This assumes the older colon
   * divided notation, e.g. "family:qualifier".
   *
   * @param scan The Scan to update.
   * @param familyAndQualifier family and qualifier
   * @throws IllegalArgumentException When familyAndQualifier is invalid.
   */
  private static void addColumn(Scan scan, byte[] familyAndQualifier) {
    byte [][] fq = KeyValue.parseColumn(familyAndQualifier);
    if (fq.length == 1) {
      scan.addFamily(fq[0]);
    } else if (fq.length == 2) {
      scan.addColumn(fq[0], fq[1]);
    } else {
      throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
    }
  }

  /**
   * Adds an array of columns specified using old format, family:qualifier.
   * <p>
   * Overrides previous calls to {@link Scan#addColumn(byte[], byte[])}for any families in the
   * input.
   *
   * @param scan The Scan to update.
   * @param columns array of columns, formatted as <code>family:qualifier</code>
   * @see Scan#addColumn(byte[], byte[])
   */
  public static void addColumns(Scan scan, byte [][] columns) {
    for (byte[] column : columns) {
      addColumn(scan, column);
    }
  }

  /**
   * Calculates the splits that will serve as input for the map tasks. The
   * number of splits matches the number of regions in a table. Splits are shuffled if
   * required.
   * @param context  The current job context.
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *   org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> splits = super.getSplits(context);
    if ((conf.get(SHUFFLE_MAPS) != null) && "true".equals(conf.get(SHUFFLE_MAPS).toLowerCase(Locale.ROOT))) {
      Collections.shuffle(splits);
    }
    return splits;
  }

  /**
   * Convenience method to parse a string representation of an array of column specifiers.
   *
   * @param scan The Scan to update.
   * @param columns  The columns to parse.
   */
  private static void addColumns(Scan scan, String columns) {
    String[] cols = columns.split(" ");
    for (String col : cols) {
      addColumn(scan, Bytes.toBytes(col));
    }
  }

  @Override
  protected Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    if (conf.get(SPLIT_TABLE) != null) {
      TableName splitTableName = TableName.valueOf(conf.get(SPLIT_TABLE));
      try (Connection conn = ConnectionFactory.createConnection(getConf())) {
        try (RegionLocator rl = conn.getRegionLocator(splitTableName)) {
          return rl.getStartEndKeys();
        }
      }
    }

    return super.getStartEndKeys();
  }

  /**
   * Sets split table in map-reduce job.
   */
  public static void configureSplitTable(Job job, TableName tableName) {
    job.getConfiguration().set(SPLIT_TABLE, tableName.getNameAsString());
  }
}
