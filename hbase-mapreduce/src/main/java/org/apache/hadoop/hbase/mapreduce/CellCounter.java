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

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;


/**
 * A job with a a map and reduce phase to count cells in a table.
 * The counter lists the following stats for a given table:
 * <pre>
 * 1. Total number of rows in the table
 * 2. Total number of CFs across all rows
 * 3. Total qualifiers across all rows
 * 4. Total occurrence of each CF
 * 5. Total occurrence  of each qualifier
 * 6. Total number of versions of each qualifier.
 * </pre>
 *
 * The cellcounter can take optional parameters to use a user
 * supplied row/family/qualifier string to use in the report and
 * second a regex based or prefix based row filter to restrict the
 * count operation to a limited subset of rows from the table or a
 * start time and/or end time to limit the count to a time range.
 */
@InterfaceAudience.Public
public class CellCounter extends Configured implements Tool {
  private static final Logger LOG =
    LoggerFactory.getLogger(CellCounter.class.getName());


  /**
   * Name of this 'program'.
   */
  static final String NAME = "CellCounter";

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  /**
   * Mapper that runs the count.
   */
  static class CellCounterMapper
  extends TableMapper<Text, IntWritable> {
    /**
     * Counter enumeration to count the actual rows.
     */
    public static enum Counters {
      ROWS,
      CELLS
    }

    private Configuration conf;
    private String separator;

    // state of current row, family, column needs to persist across map() invocations
    // in order to properly handle scanner batching, where a single qualifier may have too
    // many versions for a single map() call
    private byte[] lastRow;
    private String currentRowKey;
    byte[] currentFamily = null;
    String currentFamilyName = null;
    byte[] currentQualifier = null;
    // family + qualifier
    String currentQualifierName = null;
    // rowkey + family + qualifier
    String currentRowQualifierName = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      separator = conf.get("ReportSeparator",":");
    }

    /**
     * Maps the data.
     *
     * @param row     The current table row key.
     * @param values  The columns.
     * @param context The current context.
     * @throws IOException When something is broken with the data.
     */

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
      justification="Findbugs is blind to the Precondition null check")
    public void map(ImmutableBytesWritable row, Result values,
                    Context context)
        throws IOException {
      Preconditions.checkState(values != null,
          "values passed to the map is null");

      try {
        byte[] currentRow = values.getRow();
        if (lastRow == null || !Bytes.equals(lastRow, currentRow)) {
          lastRow = currentRow;
          currentRowKey = Bytes.toStringBinary(currentRow);
          currentFamily = null;
          currentQualifier = null;
          context.getCounter(Counters.ROWS).increment(1);
          context.write(new Text("Total ROWS"), new IntWritable(1));
        }
        if (!values.isEmpty()) {
          int cellCount = 0;
          for (Cell value : values.listCells()) {
            cellCount++;
            if (currentFamily == null || !CellUtil.matchingFamily(value, currentFamily)) {
              currentFamily = CellUtil.cloneFamily(value);
              currentFamilyName = Bytes.toStringBinary(currentFamily);
              currentQualifier = null;
              context.getCounter("CF", currentFamilyName).increment(1);
              if (1 == context.getCounter("CF", currentFamilyName).getValue()) {
                context.write(new Text("Total Families Across all Rows"), new IntWritable(1));
                context.write(new Text(currentFamily), new IntWritable(1));
              }
            }
            if (currentQualifier == null || !CellUtil.matchingQualifier(value, currentQualifier)) {
              currentQualifier = CellUtil.cloneQualifier(value);
              currentQualifierName = currentFamilyName + separator +
                  Bytes.toStringBinary(currentQualifier);
              currentRowQualifierName = currentRowKey + separator + currentQualifierName;

              context.write(new Text("Total Qualifiers across all Rows"),
                  new IntWritable(1));
              context.write(new Text(currentQualifierName), new IntWritable(1));
            }
            // Increment versions
            context.write(new Text(currentRowQualifierName + "_Versions"), new IntWritable(1));
          }
          context.getCounter(Counters.CELLS).increment(cellCount);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  static class IntSumReducer<Key> extends Reducer<Key, IntWritable,
      Key, IntWritable> {

    private IntWritable result = new IntWritable();
    public void reduce(Key key, Iterable<IntWritable> values,
      Context context)
    throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
      throws IOException {
    String tableName = args[0];
    Path outputDir = new Path(args[1]);
    String reportSeparatorString = (args.length > 2) ? args[2]: ":";
    conf.set("ReportSeparator", reportSeparatorString);
    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJarByClass(CellCounter.class);
    Scan scan = getConfiguredScanForJob(conf, args);
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        CellCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setReducerClass(IntSumReducer.class);
    return job;
  }

  private static Scan getConfiguredScanForJob(Configuration conf, String[] args)
      throws IOException {
    // create scan with any properties set from TableInputFormat
    Scan s = TableInputFormat.createScanFromConfiguration(conf);
    // Set Scan Versions
    if (conf.get(TableInputFormat.SCAN_MAXVERSIONS) == null) {
      // default to all versions unless explicitly set
      s.setMaxVersions(Integer.MAX_VALUE);
    }
    s.setCacheBlocks(false);
    // Set RowFilter or Prefix Filter if applicable.
    Filter rowFilter = getRowFilter(args);
    if (rowFilter!= null) {
      LOG.info("Setting Row Filter for counter.");
      s.setFilter(rowFilter);
    }
    // Set TimeRange if defined
    long timeRange[] = getTimeRange(args);
    if (timeRange != null) {
      LOG.info("Setting TimeRange for counter.");
      s.setTimeRange(timeRange[0], timeRange[1]);
    }
    return s;
  }


  private static Filter getRowFilter(String[] args) {
    Filter rowFilter = null;
    String filterCriteria = (args.length > 3) ? args[3]: null;
    if (filterCriteria == null) return null;
    if (filterCriteria.startsWith("^")) {
      String regexPattern = filterCriteria.substring(1, filterCriteria.length());
      rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(regexPattern));
    } else {
      rowFilter = new PrefixFilter(Bytes.toBytesBinary(filterCriteria));
    }
    return rowFilter;
  }

  private static long[] getTimeRange(String[] args) throws IOException {
    final String startTimeArgKey = "--starttime=";
    final String endTimeArgKey = "--endtime=";
    long startTime = 0L;
    long endTime = 0L;

    for (int i = 1; i < args.length; i++) {
      System.out.println("i:" + i + "arg[i]" + args[i]);
      if (args[i].startsWith(startTimeArgKey)) {
        startTime = Long.parseLong(args[i].substring(startTimeArgKey.length()));
      }
      if (args[i].startsWith(endTimeArgKey)) {
        endTime = Long.parseLong(args[i].substring(endTimeArgKey.length()));
      }
    }

    if (startTime == 0 && endTime == 0)
      return null;

    endTime = endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime;
    return new long [] {startTime, endTime};
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage(args.length);
      return -1;
    }
    Job job = createSubmittableJob(getConf(), args);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  private void printUsage(int parameterCount) {
    System.err.println("ERROR: Wrong number of parameters: " + parameterCount);
    System.err.println("Usage: hbase cellcounter <tablename> <outputDir> [reportSeparator] "
        + "[^[regex pattern] or [Prefix]] [--starttime=<starttime> --endtime=<endtime>]");
    System.err.println("  Note: -D properties will be applied to the conf used.");
    System.err.println("  Additionally, all of the SCAN properties from TableInputFormat can be "
        + "specified to get fine grained control on what is counted.");
    System.err.println("   -D" + TableInputFormat.SCAN_ROW_START + "=<rowkey>");
    System.err.println("   -D" + TableInputFormat.SCAN_ROW_STOP + "=<rowkey>");
    System.err.println("   -D" + TableInputFormat.SCAN_COLUMNS + "=\"<col1> <col2>...\"");
    System.err.println("   -D" + TableInputFormat.SCAN_COLUMN_FAMILY
        + "=<family1>,<family2>, ...");
    System.err.println("   -D" + TableInputFormat.SCAN_TIMESTAMP + "=<timestamp>");
    System.err.println("   -D" + TableInputFormat.SCAN_TIMERANGE_START + "=<timestamp>");
    System.err.println("   -D" + TableInputFormat.SCAN_TIMERANGE_END + "=<timestamp>");
    System.err.println("   -D" + TableInputFormat.SCAN_MAXVERSIONS + "=<count>");
    System.err.println("   -D" + TableInputFormat.SCAN_CACHEDROWS + "=<count>");
    System.err.println("   -D" + TableInputFormat.SCAN_BATCHSIZE + "=<count>");
    System.err.println(" <reportSeparator> parameter can be used to override the default report "
        + "separator string : used to separate the rowId/column family name and qualifier name.");
    System.err.println(" [^[regex pattern] or [Prefix] parameter can be used to limit the cell "
        + "counter count operation to a limited subset of rows from the table based on regex or "
        + "prefix pattern.");
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(HBaseConfiguration.create(), new CellCounter(), args);
    System.exit(errCode);
  }

}
