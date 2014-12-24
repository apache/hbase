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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

  static final Log LOG = LogFactory.getLog(TestTableInputFormatScanBase.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static final byte[] TABLE_NAME = Bytes.toBytes("scantest");
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
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
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    table = TEST_UTIL.createTable(TableName.valueOf(TABLE_NAME), INPUT_FAMILY);
    TEST_UTIL.createMultiRegions(table, INPUT_FAMILY);
    TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
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
    String jobName = "ScanFromConfig" + (start != null ? start.toUpperCase() : "Empty") +
      "To" + (stop != null ? stop.toUpperCase() : "Empty");
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
    String jobName = "Scan" + (start != null ? start.toUpperCase() : "Empty") +
      "To" + (stop != null ? stop.toUpperCase() : "Empty");
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
    FileOutputFormat.setOutputPath(job, new Path(job.getJobName()));
    LOG.info("Started " + job.getJobName());
    assertTrue(job.waitForCompletion(true));
    LOG.info("After map/reduce completion - job " + jobName);
  }


  /**
   * Tests a MR scan using data skew auto-balance
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public void testNumOfSplits(String ratio, int expectedNumOfSplits) throws IOException,
          InterruptedException,
          ClassNotFoundException {
    String jobName = "TestJobForNumOfSplits";
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    Scan scan = new Scan();
    scan.addFamily(INPUT_FAMILY);
    c.set("hbase.mapreduce.input.autobalance", "true");
    c.set("hbase.mapreduce.input.autobalance.maxskewratio", ratio);
    c.set(KEY_STARTROW, "");
    c.set(KEY_LASTROW, "");
    Job job = new Job(c, jobName);
    TableMapReduceUtil.initTableMapperJob(Bytes.toString(TABLE_NAME), scan, ScanMapper.class,
            ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
    TableInputFormat tif = new TableInputFormat();
    tif.setConf(job.getConfiguration());
    Assert.assertEquals(new String(TABLE_NAME), new String(table.getTableName()));
    List<InputSplit> splits = tif.getSplits(job);
    Assert.assertEquals(expectedNumOfSplits, splits.size());
  }

  /**
   * Tests for the getSplitKey() method in TableInputFormatBase.java
   */
  public void testGetSplitKey(byte[] startKey, byte[] endKey, byte[] splitKey, boolean isText) {
    byte[] result = TableInputFormatBase.getSplitKey(startKey, endKey, isText);
      Assert.assertArrayEquals(splitKey, result);
  }
}

