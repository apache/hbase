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
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A job with a just a map phase to count rows. Map outputs table rows IF the
 * input row has columns that have content.
 */
@InterfaceAudience.Public
public class RowCounter extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RowCounter.class);

  /** Name of this 'program'. */
  static final String NAME = "rowcounter";

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";
  private final static String EXPECTED_COUNT_KEY = RowCounter.class.getName() + ".expected_count";

  /**
   * Mapper that runs the count.
   */
  static class RowCounterMapper
  extends TableMapper<ImmutableBytesWritable, Result> {

    /** Counter enumeration to count the actual rows. */
    public static enum Counters {ROWS}

    /**
     * Maps the data.
     *
     * @param row  The current table row key.
     * @param values  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(Object, Object, Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result values,
      Context context)
    throws IOException {
      // Count every row containing data, whether it's in qualifiers or values
      context.getCounter(Counters.ROWS).increment(1);
    }
  }

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
    List<MultiRowRangeFilter.RowRange> rowRangeList = null;
    long startTime = 0;
    long endTime = 0;

    StringBuilder sb = new StringBuilder();

    final String rangeSwitch = "--range=";
    final String startTimeArgKey = "--starttime=";
    final String endTimeArgKey = "--endtime=";
    final String expectedCountArg = "--expected-count=";

    // First argument is table name, starting from second
    for (int i = 1; i < args.length; i++) {
      if (args[i].startsWith(rangeSwitch)) {
        try {
          rowRangeList = parseRowRangeParameter(args[i], rangeSwitch);
        } catch (IllegalArgumentException e) {
          return null;
        }
        continue;
      }
      if (args[i].startsWith(startTimeArgKey)) {
        startTime = Long.parseLong(args[i].substring(startTimeArgKey.length()));
        continue;
      }
      if (args[i].startsWith(endTimeArgKey)) {
        endTime = Long.parseLong(args[i].substring(endTimeArgKey.length()));
        continue;
      }
      if (args[i].startsWith(expectedCountArg)) {
        conf.setLong(EXPECTED_COUNT_KEY,
            Long.parseLong(args[i].substring(expectedCountArg.length())));
        continue;
      }
      // if no switch, assume column names
      sb.append(args[i]);
      sb.append(" ");
    }
    if (endTime < startTime) {
      printUsage("--endtime=" + endTime + " needs to be greater than --starttime=" + startTime);
      return null;
    }

    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJarByClass(RowCounter.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    setScanFilter(scan, rowRangeList);
    if (sb.length() > 0) {
      for (String columnName : sb.toString().trim().split(" ")) {
        String family = StringUtils.substringBefore(columnName, ":");
        String qualifier = StringUtils.substringAfter(columnName, ":");

        if (StringUtils.isBlank(qualifier)) {
          scan.addFamily(Bytes.toBytes(family));
        }
        else {
          scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        }
      }
    }
    scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    job.setOutputFormatClass(NullOutputFormat.class);
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
      RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(0);
    return job;
  }

  private static List<MultiRowRangeFilter.RowRange> parseRowRangeParameter(
    String arg, String rangeSwitch) {
    final String[] ranges = arg.substring(rangeSwitch.length()).split(";");
    final List<MultiRowRangeFilter.RowRange> rangeList = new ArrayList<>();
    for (String range : ranges) {
      String[] startEnd = range.split(",", 2);
      if (startEnd.length != 2 || startEnd[1].contains(",")) {
        printUsage("Please specify range in such format as \"--range=a,b\" " +
            "or, with only one boundary, \"--range=,b\" or \"--range=a,\"");
        throw new IllegalArgumentException("Wrong range specification: " + range);
      }
      String startKey = startEnd[0];
      String endKey = startEnd[1];
      rangeList.add(new MultiRowRangeFilter.RowRange(
        Bytes.toBytesBinary(startKey), true,
        Bytes.toBytesBinary(endKey), false));
    }
    return rangeList;
  }

  /**
   * Sets filter {@link FilterBase} to the {@link Scan} instance.
   * If provided rowRangeList contains more than one element,
   * method sets filter which is instance of {@link MultiRowRangeFilter}.
   * Otherwise, method sets filter which is instance of {@link FirstKeyOnlyFilter}.
   * If rowRangeList contains exactly one element, startRow and stopRow are set to the scan.
   * @param scan
   * @param rowRangeList
   */
  private static void setScanFilter(Scan scan, List<MultiRowRangeFilter.RowRange> rowRangeList) {
    final int size = rowRangeList == null ? 0 : rowRangeList.size();
    if (size <= 1) {
      scan.setFilter(new FirstKeyOnlyFilter());
    }
    if (size == 1) {
      MultiRowRangeFilter.RowRange range = rowRangeList.get(0);
      scan.setStartRow(range.getStartRow()); //inclusive
      scan.setStopRow(range.getStopRow());   //exclusive
    } else if (size > 1) {
      scan.setFilter(new MultiRowRangeFilter(rowRangeList));
    }
  }

  /*
   * @param errorMessage Can attach a message when error occurs.
   */
  private static void printUsage(String errorMessage) {
    System.err.println("ERROR: " + errorMessage);
    printUsage();
  }

  /**
   * Prints usage without error message.
   * Note that we don't document --expected-count, because it's intended for test.
   */
  private static void printUsage() {
    System.err.println("Usage: hbase rowcounter [options] <tablename> "
        + "[--starttime=<start> --endtime=<end>] "
        + "[--range=[startKey],[endKey][;[startKey],[endKey]...]] [<column1> <column2>...]");
    System.err.println("For performance consider the following options:\n"
        + "-Dhbase.client.scanner.caching=100\n"
        + "-Dmapreduce.map.speculative=false");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage("Wrong number of parameters: " + args.length);
      return -1;
    }
    Job job = createSubmittableJob(getConf(), args);
    if (job == null) {
      return -1;
    }
    boolean success = job.waitForCompletion(true);
    final long expectedCount = getConf().getLong(EXPECTED_COUNT_KEY, -1);
    if (success && expectedCount != -1) {
      final Counter counter = job.getCounters().findCounter(RowCounterMapper.Counters.ROWS);
      success = expectedCount == counter.getValue();
      if (!success) {
        LOG.error("Failing job because count of '" + counter.getValue() +
            "' does not match expected count of '" + expectedCount + "'");
      }
    }
    return (success ? 0 : 1);
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(HBaseConfiguration.create(), new RowCounter(), args);
    System.exit(errCode);
  }

}
