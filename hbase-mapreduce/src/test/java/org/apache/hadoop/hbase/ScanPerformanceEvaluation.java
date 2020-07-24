/**
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

package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Stopwatch;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * A simple performance evaluation tool for single client and MR scans
 * and snapshot scans.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class ScanPerformanceEvaluation extends AbstractHBaseTool {

  private static final String HBASE_COUNTER_GROUP_NAME = "HBaseCounters";

  private String type;
  private String file;
  private String tablename;
  private String snapshotName;
  private String restoreDir;
  private String caching;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    Path rootDir;
    try {
      rootDir = CommonFSUtils.getRootDir(conf);
      rootDir.getFileSystem(conf);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected void addOptions() {
    this.addRequiredOptWithArg("t", "type", "the type of the test. One of the following: streaming|scan|snapshotscan|scanmapreduce|snapshotscanmapreduce");
    this.addOptWithArg("f", "file", "the filename to read from");
    this.addOptWithArg("tn", "table", "the tablename to read from");
    this.addOptWithArg("sn", "snapshot", "the snapshot name to read from");
    this.addOptWithArg("rs", "restoredir", "the directory to restore the snapshot");
    this.addOptWithArg("ch", "caching", "scanner caching value");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    type = cmd.getOptionValue("type");
    file = cmd.getOptionValue("file");
    tablename = cmd.getOptionValue("table");
    snapshotName = cmd.getOptionValue("snapshot");
    restoreDir = cmd.getOptionValue("restoredir");
    caching = cmd.getOptionValue("caching");
  }

  protected void testHdfsStreaming(Path filename) throws IOException {
    byte[] buf = new byte[1024];
    FileSystem fs = filename.getFileSystem(getConf());

    // read the file from start to finish
    Stopwatch fileOpenTimer = Stopwatch.createUnstarted();
    Stopwatch streamTimer = Stopwatch.createUnstarted();

    fileOpenTimer.start();
    FSDataInputStream in = fs.open(filename);
    fileOpenTimer.stop();

    long totalBytes = 0;
    streamTimer.start();
    while (true) {
      int read = in.read(buf);
      if (read < 0) {
        break;
      }
      totalBytes += read;
    }
    streamTimer.stop();

    double throughput = (double)totalBytes / streamTimer.elapsed(TimeUnit.SECONDS);

    System.out.println("HDFS streaming: ");
    System.out.println("total time to open: " +
      fileOpenTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total time to read: " + streamTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total bytes: " + totalBytes + " bytes ("
        + StringUtils.humanReadableInt(totalBytes) + ")");
    System.out.println("throghput  : " + StringUtils.humanReadableInt((long)throughput) + "B/s");
  }

  private Scan getScan() {
    Scan scan = new Scan(); // default scan settings
    scan.setCacheBlocks(false);
    scan.setMaxVersions(1);
    scan.setScanMetricsEnabled(true);
    if (caching != null) {
      scan.setCaching(Integer.parseInt(caching));
    }

    return scan;
  }

  public void testScan() throws IOException {
    Stopwatch tableOpenTimer = Stopwatch.createUnstarted();
    Stopwatch scanOpenTimer = Stopwatch.createUnstarted();
    Stopwatch scanTimer = Stopwatch.createUnstarted();

    tableOpenTimer.start();
    Connection connection = ConnectionFactory.createConnection(getConf());
    Table table = connection.getTable(TableName.valueOf(tablename));
    tableOpenTimer.stop();

    Scan scan = getScan();
    scanOpenTimer.start();
    ResultScanner scanner = table.getScanner(scan);
    scanOpenTimer.stop();

    long numRows = 0;
    long numCells = 0;
    scanTimer.start();
    while (true) {
      Result result = scanner.next();
      if (result == null) {
        break;
      }
      numRows++;

      numCells += result.rawCells().length;
    }
    scanTimer.stop();
    scanner.close();
    table.close();
    connection.close();

    ScanMetrics metrics = scan.getScanMetrics();
    long totalBytes = metrics.countOfBytesInResults.get();
    double throughput = (double)totalBytes / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputRows = (double)numRows / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputCells = (double)numCells / scanTimer.elapsed(TimeUnit.SECONDS);

    System.out.println("HBase scan: ");
    System.out.println("total time to open table: " +
      tableOpenTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total time to open scanner: " +
      scanOpenTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total time to scan: " +
      scanTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");

    System.out.println("Scan metrics:\n" + metrics.getMetricsMap());

    System.out.println("total bytes: " + totalBytes + " bytes ("
        + StringUtils.humanReadableInt(totalBytes) + ")");
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughput) + "B/s");
    System.out.println("total rows  : " + numRows);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputRows) + " rows/s");
    System.out.println("total cells : " + numCells);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputCells) + " cells/s");
  }


  public void testSnapshotScan() throws IOException {
    Stopwatch snapshotRestoreTimer = Stopwatch.createUnstarted();
    Stopwatch scanOpenTimer = Stopwatch.createUnstarted();
    Stopwatch scanTimer = Stopwatch.createUnstarted();

    Path restoreDir = new Path(this.restoreDir);

    snapshotRestoreTimer.start();
    restoreDir.getFileSystem(conf).delete(restoreDir, true);
    snapshotRestoreTimer.stop();

    Scan scan = getScan();
    scanOpenTimer.start();
    TableSnapshotScanner scanner = new TableSnapshotScanner(conf, restoreDir, snapshotName, scan);
    scanOpenTimer.stop();

    long numRows = 0;
    long numCells = 0;
    scanTimer.start();
    while (true) {
      Result result = scanner.next();
      if (result == null) {
        break;
      }
      numRows++;

      numCells += result.rawCells().length;
    }
    scanTimer.stop();
    scanner.close();

    ScanMetrics metrics = scanner.getScanMetrics();
    long totalBytes = metrics.countOfBytesInResults.get();
    double throughput = (double)totalBytes / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputRows = (double)numRows / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputCells = (double)numCells / scanTimer.elapsed(TimeUnit.SECONDS);

    System.out.println("HBase scan snapshot: ");
    System.out.println("total time to restore snapshot: " +
      snapshotRestoreTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total time to open scanner: " +
      scanOpenTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total time to scan: " +
      scanTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");

    System.out.println("Scan metrics:\n" + metrics.getMetricsMap());

    System.out.println("total bytes: " + totalBytes + " bytes ("
        + StringUtils.humanReadableInt(totalBytes) + ")");
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughput) + "B/s");
    System.out.println("total rows  : " + numRows);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputRows) + " rows/s");
    System.out.println("total cells : " + numCells);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputCells) + " cells/s");

  }

  public static enum ScanCounter {
    NUM_ROWS,
    NUM_CELLS,
  }

  public static class MyMapper<KEYOUT, VALUEOUT> extends TableMapper<KEYOUT, VALUEOUT> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
        Context context) throws IOException,
        InterruptedException {
      context.getCounter(ScanCounter.NUM_ROWS).increment(1);
      context.getCounter(ScanCounter.NUM_CELLS).increment(value.rawCells().length);
    }
  }

  public void testScanMapReduce() throws IOException, InterruptedException, ClassNotFoundException {
    Stopwatch scanOpenTimer = Stopwatch.createUnstarted();
    Stopwatch scanTimer = Stopwatch.createUnstarted();

    Scan scan = getScan();

    String jobName = "testScanMapReduce";

    Job job = new Job(conf);
    job.setJobName(jobName);

    job.setJarByClass(getClass());

    TableMapReduceUtil.initTableMapperJob(
        this.tablename,
        scan,
        MyMapper.class,
        NullWritable.class,
        NullWritable.class,
        job
    );

    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    scanTimer.start();
    job.waitForCompletion(true);
    scanTimer.stop();

    Counters counters = job.getCounters();
    long numRows = counters.findCounter(ScanCounter.NUM_ROWS).getValue();
    long numCells = counters.findCounter(ScanCounter.NUM_CELLS).getValue();

    long totalBytes = counters.findCounter(HBASE_COUNTER_GROUP_NAME, "BYTES_IN_RESULTS").getValue();
    double throughput = (double)totalBytes / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputRows = (double)numRows / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputCells = (double)numCells / scanTimer.elapsed(TimeUnit.SECONDS);

    System.out.println("HBase scan mapreduce: ");
    System.out.println("total time to open scanner: " +
      scanOpenTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total time to scan: " + scanTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");

    System.out.println("total bytes: " + totalBytes + " bytes ("
        + StringUtils.humanReadableInt(totalBytes) + ")");
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughput) + "B/s");
    System.out.println("total rows  : " + numRows);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputRows) + " rows/s");
    System.out.println("total cells : " + numCells);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputCells) + " cells/s");
  }

  public void testSnapshotScanMapReduce() throws IOException, InterruptedException, ClassNotFoundException {
    Stopwatch scanOpenTimer = Stopwatch.createUnstarted();
    Stopwatch scanTimer = Stopwatch.createUnstarted();

    Scan scan = getScan();

    String jobName = "testSnapshotScanMapReduce";

    Job job = new Job(conf);
    job.setJobName(jobName);

    job.setJarByClass(getClass());

    TableMapReduceUtil.initTableSnapshotMapperJob(
        this.snapshotName,
        scan,
        MyMapper.class,
        NullWritable.class,
        NullWritable.class,
        job,
        true,
        new Path(restoreDir)
    );

    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    scanTimer.start();
    job.waitForCompletion(true);
    scanTimer.stop();

    Counters counters = job.getCounters();
    long numRows = counters.findCounter(ScanCounter.NUM_ROWS).getValue();
    long numCells = counters.findCounter(ScanCounter.NUM_CELLS).getValue();

    long totalBytes = counters.findCounter(HBASE_COUNTER_GROUP_NAME, "BYTES_IN_RESULTS").getValue();
    double throughput = (double)totalBytes / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputRows = (double)numRows / scanTimer.elapsed(TimeUnit.SECONDS);
    double throughputCells = (double)numCells / scanTimer.elapsed(TimeUnit.SECONDS);

    System.out.println("HBase scan mapreduce: ");
    System.out.println("total time to open scanner: " +
      scanOpenTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");
    System.out.println("total time to scan: " + scanTimer.elapsed(TimeUnit.MILLISECONDS) + " ms");

    System.out.println("total bytes: " + totalBytes + " bytes ("
        + StringUtils.humanReadableInt(totalBytes) + ")");
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughput) + "B/s");
    System.out.println("total rows  : " + numRows);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputRows) + " rows/s");
    System.out.println("total cells : " + numCells);
    System.out.println("throughput  : " + StringUtils.humanReadableInt((long)throughputCells) + " cells/s");
  }

  @Override
  protected int doWork() throws Exception {
    if (type.equals("streaming")) {
      testHdfsStreaming(new Path(file));
    } else if (type.equals("scan")){
      testScan();
    } else if (type.equals("snapshotscan")) {
      testSnapshotScan();
    } else if (type.equals("scanmapreduce")) {
      testScanMapReduce();
    } else if (type.equals("snapshotscanmapreduce")) {
      testSnapshotScanMapReduce();
    }
    return 0;
  }

  public static void main (String[] args) throws Exception {
    int ret = ToolRunner.run(HBaseConfiguration.create(), new ScanPerformanceEvaluation(), args);
    System.exit(ret);
  }
}
