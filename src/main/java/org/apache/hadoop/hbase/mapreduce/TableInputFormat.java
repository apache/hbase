/**
 * Copyright 2007 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter.UniformSplit;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
public class TableInputFormat extends TableInputFormatBase
implements Configurable {

  private static final Log LOG = LogFactory.getLog(TableInputFormat.class);

  /** Job parameter that specifies the input table. */
  public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
  /** Base-64 encoded scanner. All other SCAN_ confs are ignored if this is specified.
   * See {@link TableMapReduceUtil#convertScanToString(Scan)} for more details.
   */
  public static final String SCAN = "hbase.mapreduce.scan";

  /** Column Family to Scan */
  public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";

  /** Space delimited list of columns to scan. */
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

  /** Enable scan prefetch on the server side. */
  public static final String SCAN_PREFETCH = "hbase.mapreduce.scan.prefetch";

  /** Set the maximum response size for a scan call. This will help in handling large rows on
   * hbase side by returning partial rows upto the specified limit */
  public static final String SCAN_MAX_RESPONSE_SIZE = "hbase.mapreduce.scan.maxresponsesize";

  /** Allow partial rows */
  public static final String SCAN_PARTIAL_ROWS = "hbase.mapreduce.scan.partialrows";

  /** Set the batch size */
  public static final String SCAN_BATCH_SIZE = "hbase.mapreduce.scan.batchsize";

  /** Start row of the scan */
  public static final String SCAN_START_ROW = "hbase.mapreduce.scan.startrow";

  /** End row of the scan */
  public static final String SCAN_END_ROW = "hbase.mapreduce.scan.endrow";

  /** The number of mappers that should be assigned to each region. */
  public static final String MAPPERS_PER_REGION = "hbase.mapreduce.mappersperregion";

  /** The number of mappers that should be assigned per job. */
  public static final String NUM_MAPPERS_PER_JOB = "hbase.mapreduce.num.mappers";


  /** The Algorithm used to split each region's keyspace. */
  public static final String SPLIT_ALGO = "hbase.mapreduce.tableinputformat.split.algo";

  public static final String USE_CLIENT_LOCAL_SCANNER = "hbase.mapreduce.use.client.side.scan";

  public static final boolean DEFAULT_USE_CLIENT_LOCAL_SCANNER = false;

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
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    String tableName = conf.get(INPUT_TABLE);
    try {
      setHTable(new HTable(HBaseConfiguration.create(conf), tableName));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }

    setScan(createScan(conf));
    if (conf.get(MAPPERS_PER_REGION) != null) {
      setNumMapperPerRegion(Integer.parseInt(conf.get(MAPPERS_PER_REGION)));
    }

    if (conf.get(NUM_MAPPERS_PER_JOB) != null) {
      setNumMappersPerJob(Integer.parseInt(conf.get(NUM_MAPPERS_PER_JOB)));
    }

    setSplitAlgorithm(conf.get(SPLIT_ALGO, UniformSplit.class.getSimpleName()));
  }

  public static Scan createScan(Configuration conf) {
    Scan scan;

    if (conf.get(SCAN) != null) {
      try {
        scan = TableMapReduceUtil.convertStringToScan(conf.get(SCAN));
      } catch (IOException e) {
        LOG.error("An error occurred.", e);
        throw new RuntimeException(e);
      }
    } else {
      try {
        scan = new Scan();

        if (conf.get(SCAN_COLUMNS) != null) {
          scan.addColumns(conf.get(SCAN_COLUMNS));
        }

        if (conf.get(SCAN_COLUMN_FAMILY) != null) {
          scan.addFamily(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)));
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

        // Set scan prefetch false by default;
        scan.setServerPrefetching(conf.getBoolean(SCAN_PREFETCH, false));

        int batchSize = conf.getInt(SCAN_BATCH_SIZE, -1);
        if (batchSize > 0) {
          scan.setBatch(batchSize);
        }

        int maxResponseSize = conf.getInt(SCAN_MAX_RESPONSE_SIZE, -1);
        boolean partialRows = conf.getBoolean(SCAN_PARTIAL_ROWS, false);
        if (maxResponseSize > 0) {
          LOG.info("Setting max response size: " + maxResponseSize +
              " partial rows: " + partialRows);
          scan.setCaching(maxResponseSize, partialRows);
        }

        // false by default, full table scans generate too much BC churn
        scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));

        String startRow = conf.get(SCAN_START_ROW);
        if (startRow != null) {
          LOG.info("Setting start row to: " + startRow);
          scan.setStartRow(Bytes.toBytes(startRow));
        }

        String endRow = conf.get(SCAN_END_ROW);
        if (conf.get(SCAN_END_ROW) != null) {
          LOG.info("Setting end row to: " + endRow);
          scan.setStopRow(Bytes.toBytes(endRow));
        }

        LOG.info("Scan config:" +
            " Scan Prefetch=" + scan.getServerPrefetching() +
            " Cached Rows=" + scan.getCaching() +
            " Batch Size=" + scan.getBatch() +
            " Max response Size=" + scan.getMaxResponseSize());

      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
    return scan;
  }

}
