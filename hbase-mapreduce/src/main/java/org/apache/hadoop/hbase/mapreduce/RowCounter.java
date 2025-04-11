/*
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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.BasicParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.MissingOptionException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;

/**
 * A job with a just a map phase to count rows. Map outputs table rows IF the input row has columns
 * that have content.
 */
@InterfaceAudience.Public
public class RowCounter extends AbstractHBaseTool {

  private static final Logger LOG = LoggerFactory.getLogger(RowCounter.class);

  /** Name of this 'program'. */
  static final String NAME = "rowcounter";

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";
  private final static String EXPECTED_COUNT_KEY = RowCounter.class.getName() + ".expected_count";

  private final static String OPT_START_TIME = "starttime";
  private final static String OPT_END_TIME = "endtime";
  private final static String OPT_RANGE = "range";
  private final static String OPT_EXPECTED_COUNT = "expectedCount";
  private final static String OPT_COUNT_DELETE_MARKERS = "countDeleteMarkers";

  private String tableName;
  private List<MultiRowRangeFilter.RowRange> rowRangeList;
  private long startTime;
  private long endTime;
  private long expectedCount;
  private boolean countDeleteMarkers;
  private List<String> columns = new ArrayList<>();

  private Job job;

  /**
   * Mapper that runs the count.
   */
  static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {

    /** Counter enumeration to count the actual rows, cells and delete markers. */
    public static enum Counters {
      ROWS,
      DELETE,
      DELETE_COLUMN,
      DELETE_FAMILY,
      DELETE_FAMILY_VERSION,
      ROWS_WITH_DELETE_MARKER
    }

    private boolean countDeleteMarkers;

    @Override
    protected void
      setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result>.Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      countDeleteMarkers = conf.getBoolean(OPT_COUNT_DELETE_MARKERS, false);
    }

    /**
     * Maps the data.
     * @param row     The current table row key.
     * @param values  The columns.
     * @param context The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(Object, Object, Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
      // Count every row containing data, whether it's in qualifiers or values
      context.getCounter(Counters.ROWS).increment(1);

      if (countDeleteMarkers) {
        boolean rowContainsDeleteMarker = false;
        for (Cell cell : values.rawCells()) {
          Cell.Type type = cell.getType();
          switch (type) {
            case Delete:
              rowContainsDeleteMarker = true;
              context.getCounter(Counters.DELETE).increment(1);
              break;
            case DeleteColumn:
              rowContainsDeleteMarker = true;
              context.getCounter(Counters.DELETE_COLUMN).increment(1);
              break;
            case DeleteFamily:
              rowContainsDeleteMarker = true;
              context.getCounter(Counters.DELETE_FAMILY).increment(1);
              break;
            case DeleteFamilyVersion:
              rowContainsDeleteMarker = true;
              context.getCounter(Counters.DELETE_FAMILY_VERSION).increment(1);
              break;
            default:
              break;
          }
        }

        if (rowContainsDeleteMarker) {
          context.getCounter(Counters.ROWS_WITH_DELETE_MARKER).increment(1);
        }
      }
    }
  }

  /**
   * Sets up the actual job.
   * @param conf The current configuration.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(Configuration conf) throws IOException {
    conf.setBoolean(OPT_COUNT_DELETE_MARKERS, this.countDeleteMarkers);
    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJarByClass(RowCounter.class);
    Scan scan = new Scan();
    // raw scan will be needed to account for delete markers when --countDeleteMarkers flag is set
    scan.setRaw(this.countDeleteMarkers);
    scan.setCacheBlocks(false);
    setScanFilter(scan, rowRangeList, this.countDeleteMarkers);

    for (String columnName : this.columns) {
      String family = StringUtils.substringBefore(columnName, ":");
      String qualifier = StringUtils.substringAfter(columnName, ":");
      if (StringUtils.isBlank(qualifier)) {
        scan.addFamily(Bytes.toBytes(family));
      } else {
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
      }
    }

    if (this.expectedCount >= 0) {
      conf.setLong(EXPECTED_COUNT_KEY, this.expectedCount);
    }

    scan.setTimeRange(startTime, endTime);
    job.setOutputFormatClass(NullOutputFormat.class);
    TableMapReduceUtil.initTableMapperJob(tableName, scan, RowCounterMapper.class,
      ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(0);
    return job;
  }

  /**
   * Sets up the actual job.
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   * @deprecated as of release 2.3.0. Will be removed on 4.0.0. Please use main method instead.
   */
  @Deprecated
  public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    String tableName = args[0];
    List<MultiRowRangeFilter.RowRange> rowRangeList = null;
    long startTime = 0;
    long endTime = 0;
    boolean countDeleteMarkers = false;

    StringBuilder sb = new StringBuilder();

    final String rangeSwitch = "--range=";
    final String startTimeArgKey = "--starttime=";
    final String endTimeArgKey = "--endtime=";
    final String expectedCountArg = "--expected-count=";
    final String countDeleteMarkersArg = "--countDeleteMarkers";

    // First argument is table name, starting from second
    for (int i = 1; i < args.length; i++) {
      if (args[i].startsWith(rangeSwitch)) {
        try {
          rowRangeList = parseRowRangeParameter(
            args[i].substring(args[1].indexOf(rangeSwitch) + rangeSwitch.length()));
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
      if (args[i].startsWith(countDeleteMarkersArg)) {
        countDeleteMarkers = true;
        continue;
      }
      // if no switch, assume column names
      sb.append(args[i]);
      sb.append(" ");
    }
    conf.setBoolean(OPT_COUNT_DELETE_MARKERS, countDeleteMarkers);
    if (endTime < startTime) {
      printUsage("--endtime=" + endTime + " needs to be greater than --starttime=" + startTime);
      return null;
    }

    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJarByClass(RowCounter.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    // raw scan will be needed to account for delete markers when --countDeleteMarkers flag is set
    scan.setRaw(countDeleteMarkers);
    setScanFilter(scan, rowRangeList, countDeleteMarkers);
    if (sb.length() > 0) {
      for (String columnName : sb.toString().trim().split(" ")) {
        String family = StringUtils.substringBefore(columnName, ":");
        String qualifier = StringUtils.substringAfter(columnName, ":");

        if (StringUtils.isBlank(qualifier)) {
          scan.addFamily(Bytes.toBytes(family));
        } else {
          scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        }
      }
    }
    scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    job.setOutputFormatClass(NullOutputFormat.class);
    TableMapReduceUtil.initTableMapperJob(tableName, scan, RowCounterMapper.class,
      ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(0);
    return job;
  }

  /**
   * Prints usage without error message. Note that we don't document --expected-count, because it's
   * intended for test.
   */
  private static void printUsage(String errorMessage) {
    System.err.println("ERROR: " + errorMessage);
    System.err.println(
      "Usage: hbase rowcounter [options] <tablename> " + "[--starttime=<start> --endtime=<end>] "
        + "[--range=[startKey],[endKey][;[startKey],[endKey]...]] [<column1> <column2>...]");
    System.err.println("For performance consider the following options:\n"
      + "-Dhbase.client.scanner.caching=100\n" + "-Dmapreduce.map.speculative=false");
  }

  private static List<MultiRowRangeFilter.RowRange> parseRowRangeParameter(String arg) {
    final List<String> rangesSplit = Splitter.on(";").splitToList(arg);
    final List<MultiRowRangeFilter.RowRange> rangeList = new ArrayList<>();
    for (String range : rangesSplit) {
      if (range != null && !range.isEmpty()) {
        List<String> startEnd = Splitter.on(",").splitToList(range);
        if (startEnd.size() != 2 || startEnd.get(1).contains(",")) {
          throw new IllegalArgumentException("Wrong range specification: " + range);
        }
        String startKey = startEnd.get(0);
        String endKey = startEnd.get(1);
        rangeList.add(new MultiRowRangeFilter.RowRange(Bytes.toBytesBinary(startKey), true,
          Bytes.toBytesBinary(endKey), false));
      }
    }
    return rangeList;
  }

  /**
   * Sets filter {@link FilterBase} to the {@link Scan} instance. If provided rowRangeList contains
   * more than one element, method sets filter which is instance of {@link MultiRowRangeFilter}. If
   * rowRangeList contains exactly one element, startRow and stopRow are set to the scan. Also,
   * method may apply {@link FirstKeyOnlyFilter} an {@link KeyOnlyFilter} for better performance.
   */
  private static void setScanFilter(Scan scan, List<MultiRowRangeFilter.RowRange> rowRangeList,
    boolean countDeleteMarkers) {
    List<Filter> filters = new ArrayList<>();

    // Apply filters for better performance.
    if (!countDeleteMarkers) {
      // We only need one cell per row, unless --countDeleteMarkers flag is set.
      filters.add(new FirstKeyOnlyFilter());

      // We're not interested in values. Use KeyOnlyFilter.
      // NOTE: Logically, KeyOnlyFilter should be okay for --countDeleteMarkers, because it should
      // only empty the values, but currently, having a filter changes the behavior of a raw scan.
      // So we don't use it. See {@link UserScanQueryMatcher#mergeFilterResponse} for more details.
      filters.add(new KeyOnlyFilter());
    }

    // Depending on the number of ranges, set start/stop row or apply MultiRowRangeFilter.
    final int size = rowRangeList == null ? 0 : rowRangeList.size();
    if (size == 1) {
      MultiRowRangeFilter.RowRange range = rowRangeList.get(0);
      scan.setStartRow(range.getStartRow()); // inclusive
      scan.setStopRow(range.getStopRow()); // exclusive
    } else if (size > 1) {
      filters.add(new MultiRowRangeFilter(rowRangeList));
    }

    if (filters.isEmpty()) {
      return;
    }
    scan.setFilter(filters.size() == 1 ? filters.get(0) : new FilterList(filters));
  }

  @Override
  protected void printUsage() {
    StringBuilder footerBuilder = new StringBuilder();
    footerBuilder.append("For performance, consider the following configuration properties:\n");
    footerBuilder.append("-Dhbase.client.scanner.caching=100\n");
    footerBuilder.append("-Dmapreduce.map.speculative=false\n");
    printUsage("hbase rowcounter <tablename> [options] [<column1> <column2>...]", "Options:",
      footerBuilder.toString());
  }

  @Override
  protected void printUsage(final String usageStr, final String usageHeader,
    final String usageFooter) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(120);
    helpFormatter.setOptionComparator(new AbstractHBaseTool.OptionsOrderComparator());
    helpFormatter.setLongOptSeparator("=");
    helpFormatter.printHelp(usageStr, usageHeader, options, usageFooter);
  }

  @Override
  protected void addOptions() {
    Option startTimeOption = Option.builder(null).valueSeparator('=').hasArg(true)
      .desc("starting time filter to start counting rows from.").longOpt(OPT_START_TIME).build();
    Option endTimeOption = Option.builder(null).valueSeparator('=').hasArg(true)
      .desc("end time filter limit, to only count rows up to this timestamp.").longOpt(OPT_END_TIME)
      .build();
    Option rangeOption = Option.builder(null).valueSeparator('=').hasArg(true)
      .desc("[startKey],[endKey][;[startKey],[endKey]...]]").longOpt(OPT_RANGE).build();
    Option expectedOption = Option.builder(null).valueSeparator('=').hasArg(true)
      .desc("expected number of rows to be count.").longOpt(OPT_EXPECTED_COUNT).build();
    Option countDeleteMarkersOption = Option.builder(null).hasArg(false)
      .desc("counts the number of Delete Markers of all types, i.e. "
        + "(DELETE, DELETE_COLUMN, DELETE_FAMILY, DELETE_FAMILY_VERSION)")
      .longOpt(OPT_COUNT_DELETE_MARKERS).build();
    addOption(startTimeOption);
    addOption(endTimeOption);
    addOption(rangeOption);
    addOption(expectedOption);
    addOption(countDeleteMarkersOption);
  }

  @Override
  protected void processOptions(CommandLine cmd) throws IllegalArgumentException {
    this.tableName = cmd.getArgList().get(0);
    if (cmd.getOptionValue(OPT_RANGE) != null) {
      this.rowRangeList = parseRowRangeParameter(cmd.getOptionValue(OPT_RANGE));
    }
    this.endTime = cmd.getOptionValue(OPT_END_TIME) == null
      ? HConstants.LATEST_TIMESTAMP
      : Long.parseLong(cmd.getOptionValue(OPT_END_TIME));
    this.expectedCount = cmd.getOptionValue(OPT_EXPECTED_COUNT) == null
      ? Long.MIN_VALUE
      : Long.parseLong(cmd.getOptionValue(OPT_EXPECTED_COUNT));
    this.startTime = cmd.getOptionValue(OPT_START_TIME) == null
      ? 0
      : Long.parseLong(cmd.getOptionValue(OPT_START_TIME));
    this.countDeleteMarkers = cmd.hasOption(OPT_COUNT_DELETE_MARKERS);

    for (int i = 1; i < cmd.getArgList().size(); i++) {
      String argument = cmd.getArgList().get(i);
      if (!argument.startsWith("-")) {
        this.columns.add(argument);
      }
    }

    if (endTime < startTime) {
      throw new IllegalArgumentException(
        "--endtime=" + endTime + " needs to be greater than --starttime=" + startTime);
    }
  }

  @Override
  protected void processOldArgs(List<String> args) {
    List<String> copiedArgs = new ArrayList<>(args);
    args.removeAll(copiedArgs);
    for (String arg : copiedArgs) {
      if (arg.startsWith("-") && arg.contains("=")) {
        String[] kv = arg.split("=");
        args.add(kv[0]);
        args.add(kv[1]);
      } else {
        args.add(arg);
      }
    }
  }

  @Override
  protected int doWork() throws Exception {
    job = createSubmittableJob(getConf());
    if (job == null) {
      return -1;
    }
    boolean success = job.waitForCompletion(true);
    final long expectedCount = getConf().getLong(EXPECTED_COUNT_KEY, -1);
    if (success && expectedCount != -1) {
      final Counter counter = job.getCounters().findCounter(RowCounterMapper.Counters.ROWS);
      success = expectedCount == counter.getValue();
      if (!success) {
        LOG.error("Failing job because count of '" + counter.getValue()
          + "' does not match expected count of '" + expectedCount + "'");
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
    new RowCounter().doStaticMain(args);
  }

  static class RowCounterCommandLineParser extends BasicParser {

    @Override
    protected void checkRequiredOptions() throws MissingOptionException {
      if (this.cmd.getArgList().size() < 1 || this.cmd.getArgList().get(0).startsWith("-")) {
        throw new MissingOptionException("First argument must be a valid table name.");
      }
    }
  }

  @Override
  protected CommandLineParser newParser() {
    return new RowCounterCommandLineParser();
  }

  @RestrictedApi(explanation = "Only visible for testing", link = "",
      allowedOnPath = ".*/src/test/.*")
  Job getMapReduceJob() {
    return job;
  }

}
