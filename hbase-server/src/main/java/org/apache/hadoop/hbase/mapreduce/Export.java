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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
* Export an HBase table.
* Writes content to sequence files up in HDFS.  Use {@link Import} to read it
* back in again.
*/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Export extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Export.class);
  final static String NAME = "export";
  final static String RAW_SCAN = "hbase.mapreduce.include.deleted.rows";
  final static String EXPORT_BATCHING = "hbase.export.scanner.batch";

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    String tableName = args[0];
    Path outputDir = new Path(args[1]);
    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJobName(NAME + "_" + tableName);
    job.setJarByClass(Export.class);
    // Set optional scan parameters
    Scan s = getConfiguredScanForJob(conf, args);
    IdentityTableMapper.initJob(tableName, s, IdentityTableMapper.class, job);
    // No reducers.  Just write straight to output files.
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Result.class);
    FileOutputFormat.setOutputPath(job, outputDir); // job conf doesn't contain the conf so doesn't have a default fs.
    return job;
  }

  public static Scan getConfiguredScanForJob(Configuration conf, String[] args) throws IOException {
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
    
    if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
      s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
    }
    // Set RowFilter or Prefix Filter if applicable.
    Filter exportFilter = getExportFilter(args);
    if (exportFilter!= null) {
        LOG.info("Setting Scan Filter for Export.");
      s.setFilter(exportFilter);
    }

    int batching = conf.getInt(EXPORT_BATCHING, -1);
    if (batching !=  -1){
      try {
        s.setBatch(batching);
      } catch (IncompatibleFilterException e) {
        LOG.error("Batching could not be set", e);
      }
    }
    LOG.info("versions=" + versions + ", starttime=" + startTime +
      ", endtime=" + endTime + ", keepDeletedCells=" + raw);
    return s;
  }

  private static Filter getExportFilter(String[] args) {
    Filter exportFilter = null;
    String filterCriteria = (args.length > 5) ? args[5]: null;
    if (filterCriteria == null) return null;
    if (filterCriteria.startsWith("^")) {
      String regexPattern = filterCriteria.substring(1, filterCriteria.length());
      exportFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(regexPattern));
    } else {
      exportFilter = new PrefixFilter(Bytes.toBytesBinary(filterCriteria));
    }
    return exportFilter;
  }

  /*
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
    System.err.println("   -D mapreduce.output.fileoutputformat.compress=true");
    System.err.println("   -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec");
    System.err.println("   -D mapreduce.output.fileoutputformat.compress.type=BLOCK");
    System.err.println("  Additionally, the following SCAN properties can be specified");
    System.err.println("  to control/limit what is exported..");
    System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
    System.err.println("   -D " + RAW_SCAN + "=true");
    System.err.println("   -D " + TableInputFormat.SCAN_ROW_START + "=<ROWSTART>");
    System.err.println("   -D " + TableInputFormat.SCAN_ROW_STOP + "=<ROWSTOP>");
    System.err.println("   -D " + JOB_NAME_CONF_KEY
        + "=jobName - use the specified mapreduce job name for the export");
    System.err.println("For performance consider the following properties:\n"
        + "   -Dhbase.client.scanner.caching=100\n"
        + "   -Dmapreduce.map.speculative=false\n"
        + "   -Dmapreduce.reduce.speculative=false");
    System.err.println("For tables with very wide rows consider setting the batch size as below:\n"
        + "   -D" + EXPORT_BATCHING + "=10");
  }
  public static boolean checkArguments(final String[] args) {
    return args.length >= 2;
  }
  @Override
  public int run(String[] args) throws Exception {
    if (!checkArguments(args)) {
      usage("Wrong number of arguments: " + args.length);
      return -1;
    }
    Job job = createSubmittableJob(getConf(), args);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(HBaseConfiguration.create(), new Export(), args);
    System.exit(errCode);
  }
}
