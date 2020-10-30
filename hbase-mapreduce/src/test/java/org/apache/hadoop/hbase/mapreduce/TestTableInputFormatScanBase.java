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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests various scan start and stop row scenarios. This is set in a scan and tested in a MapReduce
 * job to see if that is handed over and done properly too.
 */
public abstract class TestTableInputFormatScanBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestTableInputFormatScanBase.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static final TableName TABLE_NAME = TableName.valueOf("scantest");
  static final byte[][] INPUT_FAMILYS = {Bytes.toBytes("content1"), Bytes.toBytes("content2")};
  static final String KEY_STARTROW = "startRow";
  static final String KEY_LASTROW = "stpRow";

  private static Table table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    table = TEST_UTIL.createMultiRegionTable(TABLE_NAME, INPUT_FAMILYS);
    TEST_UTIL.loadTable(table, INPUT_FAMILYS, null, false);
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
      if (value.size() != 2) {
        throw new IOException("There should be two input columns");
      }
      Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
        cfMap = value.getMap();

      if (!cfMap.containsKey(INPUT_FAMILYS[0]) || !cfMap.containsKey(INPUT_FAMILYS[1])) {
        throw new IOException("Wrong input columns. Missing: '" +
          Bytes.toString(INPUT_FAMILYS[0]) + "' or '" + Bytes.toString(INPUT_FAMILYS[1]) + "'.");
      }

      String val0 = Bytes.toStringBinary(value.getValue(INPUT_FAMILYS[0], null));
      String val1 = Bytes.toStringBinary(value.getValue(INPUT_FAMILYS[1], null));
      LOG.info("map: key -> " + Bytes.toStringBinary(key.get()) +
               ", value -> (" + val0 + ", " + val1 + ")");
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
   */
  protected void testScanFromConfiguration(String start, String stop, String last)
      throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = "ScanFromConfig" + (start != null ? start.toUpperCase(Locale.ROOT) : "Empty") +
      "To" + (stop != null ? stop.toUpperCase(Locale.ROOT) : "Empty");
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.set(TableInputFormat.INPUT_TABLE, TABLE_NAME.getNameAsString());
    c.set(TableInputFormat.SCAN_COLUMN_FAMILY,
      Bytes.toString(INPUT_FAMILYS[0]) + ", " + Bytes.toString(INPUT_FAMILYS[1]));
    c.set(KEY_STARTROW, start != null ? start : "");
    c.set(KEY_LASTROW, last != null ? last : "");

    if (start != null) {
      c.set(TableInputFormat.SCAN_ROW_START, start);
    }

    if (stop != null) {
      c.set(TableInputFormat.SCAN_ROW_STOP, stop);
    }

    Job job = Job.getInstance(c, jobName);
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
   */
  protected void testScan(String start, String stop, String last)
      throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = "Scan" + (start != null ? start.toUpperCase(Locale.ROOT) : "Empty") + "To" +
      (stop != null ? stop.toUpperCase(Locale.ROOT) : "Empty");
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILYS[0]);
    scan.addFamily(INPUT_FAMILYS[1]);
    if (start != null) {
      scan.withStartRow(Bytes.toBytes(start));
    }
    c.set(KEY_STARTROW, start != null ? start : "");
    if (stop != null) {
      scan.withStopRow(Bytes.toBytes(stop));
    }
    c.set(KEY_LASTROW, last != null ? last : "");
    LOG.info("scan before: " + scan);
    Job job = Job.getInstance(c, jobName);
    TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan, ScanMapper.class,
      ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    job.setReducerClass(ScanReducer.class);
    job.setNumReduceTasks(1); // one to get final "first" and "last" key
    FileOutputFormat.setOutputPath(job, new Path(job.getJobName()));
    LOG.info("Started " + job.getJobName());
    assertTrue(job.waitForCompletion(true));
    LOG.info("After map/reduce completion - job " + jobName);
  }


  /**
   * Tests Number of inputSplits for MR job when specify number of mappers for TableInputFormatXXX
   * This test does not run MR job
   */
  protected void testNumOfSplits(int splitsPerRegion, int expectedNumOfSplits)
      throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = "TestJobForNumOfSplits";
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILYS[0]);
    scan.addFamily(INPUT_FAMILYS[1]);
    c.setInt("hbase.mapreduce.tableinput.mappers.per.region", splitsPerRegion);
    c.set(KEY_STARTROW, "");
    c.set(KEY_LASTROW, "");
    Job job = Job.getInstance(c, jobName);
    TableMapReduceUtil.initTableMapperJob(TABLE_NAME.getNameAsString(), scan, ScanMapper.class,
      ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    TableInputFormat tif = new TableInputFormat();
    tif.setConf(job.getConfiguration());
    Assert.assertEquals(TABLE_NAME, table.getName());
    List<InputSplit> splits = tif.getSplits(job);
    for (InputSplit split : splits) {
      TableSplit tableSplit = (TableSplit) split;
      // In table input format, we do no store the scanner at the split level
      // because we use the scan object from the map-reduce job conf itself.
      Assert.assertTrue(tableSplit.getScanAsString().isEmpty());
    }
    Assert.assertEquals(expectedNumOfSplits, splits.size());
  }

  /**
   * Run MR job to check the number of mapper = expectedNumOfSplits
   */
  protected void testNumOfSplitsMR(int splitsPerRegion, int expectedNumOfSplits)
      throws IOException, InterruptedException, ClassNotFoundException {
    String jobName = "TestJobForNumOfSplits-MR";
    LOG.info("Before map/reduce startup - job " + jobName);
    JobConf c = new JobConf(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILYS[0]);
    scan.addFamily(INPUT_FAMILYS[1]);
    c.setInt("hbase.mapreduce.tableinput.mappers.per.region", splitsPerRegion);
    c.set(KEY_STARTROW, "");
    c.set(KEY_LASTROW, "");
    Job job = Job.getInstance(c, jobName);
    TableMapReduceUtil.initTableMapperJob(TABLE_NAME.getNameAsString(), scan, ScanMapper.class,
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
   * Run MR job to test autobalance for setting number of mappers for TIF This does not run real MR
   * job
   */
  protected void testAutobalanceNumOfSplit() throws IOException {
    // set up splits for testing
    List<InputSplit> splits = new ArrayList<>(5);
    int[] regionLen = { 10, 20, 20, 40, 60 };
    for (int i = 0; i < 5; i++) {
      InputSplit split = new TableSplit(TABLE_NAME, new Scan(), Bytes.toBytes(i),
        Bytes.toBytes(i + 1), "", "", regionLen[i] * 1048576);
      splits.add(split);
    }
    TableInputFormat tif = new TableInputFormat();
    List<InputSplit> res = tif.calculateAutoBalancedSplits(splits, 1073741824);

    assertEquals("Saw the wrong number of splits", 5, res.size());
    TableSplit ts1 = (TableSplit) res.get(0);
    assertEquals("The first split end key should be", 2, Bytes.toInt(ts1.getEndRow()));
    TableSplit ts2 = (TableSplit) res.get(1);
    assertEquals("The second split regionsize should be", 20 * 1048576, ts2.getLength());
    TableSplit ts3 = (TableSplit) res.get(2);
    assertEquals("The third split start key should be", 3, Bytes.toInt(ts3.getStartRow()));
    TableSplit ts4 = (TableSplit) res.get(4);
    assertNotEquals("The seventh split start key should not be", 4, Bytes.toInt(ts4.getStartRow()));
  }
}

