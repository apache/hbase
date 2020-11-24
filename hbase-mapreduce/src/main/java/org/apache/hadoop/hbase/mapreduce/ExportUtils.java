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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some helper methods are used by {@link org.apache.hadoop.hbase.mapreduce.Export}
 * and org.apache.hadoop.hbase.coprocessor.Export (in hbase-endpooint).
 */
@InterfaceAudience.Private
public final class ExportUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExportUtils.class);
  public static final String RAW_SCAN = "hbase.mapreduce.include.deleted.rows";
  public static final String EXPORT_BATCHING = "hbase.export.scanner.batch";
  public static final String EXPORT_CACHING = "hbase.export.scanner.caching";
  public static final String EXPORT_VISIBILITY_LABELS = "hbase.export.visibility.labels";
  /**
   * Common usage for other export tools.
   * @param errorMsg Error message.  Can be null.
   */
  public static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: Export [-D <property=value>]* <tablename> <outputdir> [<versions> " +
      "[<starttime> [<endtime>]] [^[regex pattern] or [Prefix] to filter]]\n");
    System.err.println("  Note: -D properties will be applied to the conf used. ");
    System.err.println("  For example: ");
    System.err.println("   -D " + FileOutputFormat.COMPRESS + "=true");
    System.err.println("   -D " + FileOutputFormat.COMPRESS_CODEC + "=org.apache.hadoop.io.compress.GzipCodec");
    System.err.println("   -D " + FileOutputFormat.COMPRESS_TYPE + "=BLOCK");
    System.err.println("  Additionally, the following SCAN properties can be specified");
    System.err.println("  to control/limit what is exported..");
    System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<family1>,<family2>, ...");
    System.err.println("   -D " + RAW_SCAN + "=true");
    System.err.println("   -D " + TableInputFormat.SCAN_ROW_START + "=<ROWSTART>");
    System.err.println("   -D " + TableInputFormat.SCAN_ROW_STOP + "=<ROWSTOP>");
    System.err.println("   -D " + HConstants.HBASE_CLIENT_SCANNER_CACHING + "=100");
    System.err.println("   -D " + EXPORT_VISIBILITY_LABELS + "=<labels>");
    System.err.println("For tables with very wide rows consider setting the batch size as below:\n"
            + "   -D " + EXPORT_BATCHING + "=10\n"
            + "   -D " + EXPORT_CACHING + "=100");
  }

  private static Filter getExportFilter(String[] args) {
    Filter exportFilter;
    String filterCriteria = (args.length > 5) ? args[5]: null;
    if (filterCriteria == null) return null;
    if (filterCriteria.startsWith("^")) {
      String regexPattern = filterCriteria.substring(1, filterCriteria.length());
      exportFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(regexPattern));
    } else {
      exportFilter = new PrefixFilter(Bytes.toBytesBinary(filterCriteria));
    }
    return exportFilter;
  }

  public static boolean isValidArguements(String[] args) {
    return args != null && args.length >= 2;
  }

  public static Triple<TableName, Scan, Path> getArgumentsFromCommandLine(
          Configuration conf, String[] args) throws IOException {
    if (!isValidArguements(args)) {
      return null;
    }
    return new Triple<>(TableName.valueOf(args[0]), getScanFromCommandLine(conf, args), new Path(args[1]));
  }

  static Scan getScanFromCommandLine(Configuration conf, String[] args) throws IOException {
    Scan s = new Scan();
    // Optional arguments.
    // Set Scan Versions
    int versions = args.length > 2? Integer.parseInt(args[2]): 1;
    s.setMaxVersions(versions);
    // Set Scan Range
    long startTime = args.length > 3? Long.parseLong(args[3]): 0L;
    long endTime = args.length > 4? Long.parseLong(args[4]): Long.MAX_VALUE;
    s.setTimeRange(startTime, endTime);
    // Set cache blocks
    s.setCacheBlocks(false);
    // set Start and Stop row
    if (conf.get(TableInputFormat.SCAN_ROW_START) != null) {
      s.setStartRow(Bytes.toBytesBinary(conf.get(TableInputFormat.SCAN_ROW_START)));
    }
    if (conf.get(TableInputFormat.SCAN_ROW_STOP) != null) {
      s.setStopRow(Bytes.toBytesBinary(conf.get(TableInputFormat.SCAN_ROW_STOP)));
    }
    // Set Scan Column Family
    boolean raw = Boolean.parseBoolean(conf.get(RAW_SCAN));
    if (raw) {
      s.setRaw(raw);
    }
    for (String columnFamily : conf.getTrimmedStrings(TableInputFormat.SCAN_COLUMN_FAMILY)) {
      s.addFamily(Bytes.toBytes(columnFamily));
    }
    // Set RowFilter or Prefix Filter if applicable.
    Filter exportFilter = getExportFilter(args);
    if (exportFilter!= null) {
        LOG.info("Setting Scan Filter for Export.");
      s.setFilter(exportFilter);
    }
    List<String> labels = null;
    if (conf.get(EXPORT_VISIBILITY_LABELS) != null) {
      labels = Arrays.asList(conf.getStrings(EXPORT_VISIBILITY_LABELS));
      if (!labels.isEmpty()) {
        s.setAuthorizations(new Authorizations(labels));
      }
    }

    int batching = conf.getInt(EXPORT_BATCHING, -1);
    if (batching != -1) {
      try {
        s.setBatch(batching);
      } catch (IncompatibleFilterException e) {
        LOG.error("Batching could not be set", e);
      }
    }

    int caching = conf.getInt(EXPORT_CACHING, 100);
    if (caching != -1) {
      try {
        s.setCaching(caching);
      } catch (IncompatibleFilterException e) {
        LOG.error("Caching could not be set", e);
      }
    }
    LOG.info("versions=" + versions + ", starttime=" + startTime
      + ", endtime=" + endTime + ", keepDeletedCells=" + raw
      + ", visibility labels=" + labels);
    return s;
  }

  private ExportUtils() {
  }
}
