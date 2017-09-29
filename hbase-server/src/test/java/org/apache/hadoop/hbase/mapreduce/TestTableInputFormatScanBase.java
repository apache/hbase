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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;


/**
 * <p>
 * Tests various scan start and stop row scenarios. This is set in a scan and
 * tested in a MapReduce job to see if that is handed over and done properly
 * too.
 * </p>
 * <p>
 * This test is broken into two parts in order to side-step the test timeout
 * period of 900, as documented in HBASE-8326.
 * </p>
 */
public abstract class TestTableInputFormatScanBase {

  private static final Log LOG = LogFactory.getLog(TestTableInputFormatScanBase.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static final byte[] TABLE_NAME = Bytes.toBytes("scantest");
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final byte[][] INPUT_FAMILYS = {Bytes.toBytes("content1"), Bytes.toBytes("content2")};
  static final String KEY_STARTROW = "startRow";
  static final String KEY_LASTROW = "stpRow";

  private static HTable table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // test intermittently fails under hadoop2 (2.0.2-alpha) if shortcircuit-read (scr) is on.
    // this turns it off for this test.  TODO: Figure out why scr breaks recovery. 
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");

    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    TEST_UTIL.setJobWithoutMRCluster();
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    table = TEST_UTIL.createMultiRegionTable(TableName.valueOf(TABLE_NAME), INPUT_FAMILY);
    TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Pass the key and value to reduce.
   */
  public static class ScanMapper
  extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

    /**
     * Pass the key and value to reduce.
     *
     * @param key  The key, here "aaa", "aab" etc.
     * @param value  The value is the same as the key.
     * @param context  The task context.
     * @throws IOException When reading the rows fails.
     */
    @Override
    public void map(ImmutableBytesWritable key, Result value,
      Context context)
    throws IOException, InterruptedException {
      if (value.size() != 1) {
        throw new IOException("There should only be one input column");
      }
      Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
        cf = value.getMap();
      if(!cf.containsKey(INPUT_FAMILY)) {
        throw new IOException("Wrong input columns. Missing: '" +
          Bytes.toString(INPUT_FAMILY) + "'.");
      }
      String val = Bytes.toStringBinary(value.getValue(INPUT_FAMILY, null));
      LOG.info("map: key -> " + Bytes.toStringBinary(key.get()) +
        ", value -> " + val);
      context.write(key, key);
    }

  }

  /**
   * Checks the last and first key seen against the scanner boundaries.
   */
  public static class ScanReducer
  extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable,
                  NullWritable, NullWritable> {

    private String first = null;
    private String last = null;

    protected void reduce(ImmutableBytesWritable key,
        Iterable<ImmutableBytesWritable> values, Context context)
    throws IOException ,InterruptedException {
      int count = 0;
      for (ImmutableBytesWritable value : values) {
        String val = Bytes.toStringBinary(value.get());
        LOG.info("reduce: key[" + count + "] -> " +
          Bytes.toStringBinary(key.get()) + ", value -> " + val);
        if (first == null) first = val;
        last = val;
        count++;
      }
    }

    protected void cleanup(Context context)
    throws IOException, InterruptedException {
      Configuration c = context.getConfiguration();
      String startRow = c.get(KEY_STARTROW);
      String lastRow = c.get(KEY_LASTROW);
      LOG.info("cleanup: first -> \"" + first + "\", start row -> \"" + startRow + "\"");
      LOG.info("cleanup: last -> \"" + last + "\", last row -> \"" + lastRow + "\"");
      if (startRow != null && startRow.length() > 0) {
        assertEquals(startRow, first);
      }
      if (lastRow != null && lastRow.length() > 0) {
        assertEquals(lastRow, last);
      }
    }

  }

  /**
   * Tests an MR Scan initialized from properties set in the Configuration.
   * 
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  protected void testScanFromConfiguration(String start, String stop, String last)
  throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = "ScanFromConfig" + (start != null ? start.toUpperCase(Locale.ROOT) : "Empty") +
      "To" + (stop != null ? stop.toUpperCase(Locale.ROOT) : "Empty");
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.set(TableInputFormat.INPUT_TABLE, Bytes.toString(TABLE_NAME));
    c.set(TableInputFormat.SCAN_COLUMN_FAMILY, Bytes.toString(INPUT_FAMILY));
    c.set(KEY_STARTROW, start != null ? start : "");
    c.set(KEY_LASTROW, last != null ? last : "");

    if (start != null) {
      c.set(TableInputFormat.SCAN_ROW_START, start);
    }

    if (stop != null) {
      c.set(TableInputFormat.SCAN_ROW_STOP, stop);
    }

    Job job = new Job(c, jobName);
    job.setMapperClass(ScanMapper.class);
    job.setReducerClass(ScanReducer.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(ImmutableBytesWritable.class);
    job.setInputFormatClass(TableInputFormat.class);
    job.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(job, new Path(job.getJobName()));
    TableMapReduceUtil.addDependencyJars(job);
    assertTrue(job.waitForCompletion(true));
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  protected void testScan(String start, String stop, String last)
  throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = "Scan" + (start != null ? start.toUpperCase(Locale.ROOT) : "Empty") +
      "To" + (stop != null ? stop.toUpperCase(Locale.ROOT) : "Empty");
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    if (start != null) {
      scan.setStartRow(Bytes.toBytes(start));
    }
    c.set(KEY_STARTROW, start != null ? start : "");
    if (stop != null) {
      scan.setStopRow(Bytes.toBytes(stop));
    }
    c.set(KEY_LASTROW, last != null ? last : "");
    LOG.info("scan before: " + scan);
    Job job = new Job(c, jobName);
    TableMapReduceUtil.initTableMapperJob(
      Bytes.toString(TABLE_NAME), scan, ScanMapper.class,
      ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    job.setReducerClass(ScanReducer.class);
    job.setNumReduceTasks(1); // one to get final "first" and "last" key
    FileOutputFormat.setOutputPath(job,
        new Path(TEST_UTIL.getDataTestDir(), job.getJobName()));
    LOG.info("Started " + job.getJobName());
    assertTrue(job.waitForCompletion(true));
    LOG.info("After map/reduce completion - job " + jobName);
  }


  /**
   * Tests Number of inputSplits for MR job when specify number of mappers for TableInputFormatXXX
   * This test does not run MR job
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public void testNumOfSplits(int splitsPerRegion, int expectedNumOfSplits) throws IOException,
      InterruptedException,
      ClassNotFoundException {
    String jobName = "TestJobForNumOfSplits";
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    c.setInt("hbase.mapreduce.input.mappers.per.region", splitsPerRegion);
    c.set(KEY_STARTROW, "");
    c.set(KEY_LASTROW, "");
    Job job = new Job(c, jobName);
    TableMapReduceUtil.initTableMapperJob(TableName.valueOf(TABLE_NAME).getNameAsString(), scan, ScanMapper.class,
        ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    TableInputFormat tif = new TableInputFormat();
    tif.setConf(job.getConfiguration());
    List<InputSplit> splits = tif.getSplits(job);
    Assert.assertEquals(expectedNumOfSplits, splits.size());
  }

  /**
   * Run MR job to check the number of mapper = expectedNumOfSplits
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public void testNumOfSplitsMR(int splitsPerRegion, int expectedNumOfSplits) throws IOException,
      InterruptedException,
      ClassNotFoundException {
    String jobName = "TestJobForNumOfSplits-MR";
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    c.setInt("hbase.mapreduce.input.mappers.per.region", splitsPerRegion);
    Job job = new Job(c, jobName);
    TableMapReduceUtil.initTableMapperJob(Bytes.toString(TABLE_NAME), scan, ScanMapper.class,
        ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    job.setReducerClass(ScanReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(NullOutputFormat.class);
    assertTrue("job failed!", job.waitForCompletion(true));
    // for some reason, hbase does not expose JobCounter.TOTAL_LAUNCHED_MAPS,
    // we use TaskCounter.SHUFFLED_MAPS to get total launched maps
    assertEquals("Saw the wrong count of mappers per region", expectedNumOfSplits,
        job.getCounters().findCounter(TaskCounter.SHUFFLED_MAPS).getValue());
  }

  /**
   * Run MR job to test autobalance for setting number of mappers for TIF
   * This does not run real MR job
   */
  public void testAutobalanceNumOfSplit() throws IOException {
    // set up splits for testing
    List<InputSplit> splits = new ArrayList<>(5);
    int[] regionLen = {100, 200, 200, 400, 600};
    for (int i = 0; i < 5; i++) {
      InputSplit split = new TableSplit(TableName.valueOf(TABLE_NAME), new Scan(),
          Bytes.toBytes(i), Bytes.toBytes(i + 1), "", "", regionLen[i] * 1048576);
      splits.add(split);
    }
    TableInputFormat tif = new TableInputFormat();
    List<InputSplit> res = tif.calculateAutoBalancedSplits(splits, 1073741824);

    assertEquals("Saw the wrong number of splits", 5, res.size());
    TableSplit ts1 = (TableSplit) res.get(0);
    assertEquals("The first split end key should be", 2, Bytes.toInt(ts1.getEndRow()));
    TableSplit ts2 = (TableSplit) res.get(1);
    assertEquals("The second split regionsize should be", 200 * 1048576, ts2.getLength());
    TableSplit ts3 = (TableSplit) res.get(2);
    assertEquals("The third split start key should be", 3, Bytes.toInt(ts3.getStartRow()));
    TableSplit ts4 = (TableSplit) res.get(4);
    assertNotEquals("The seventh split start key should not be", 4, Bytes.toInt(ts4.getStartRow()));
  }
}

