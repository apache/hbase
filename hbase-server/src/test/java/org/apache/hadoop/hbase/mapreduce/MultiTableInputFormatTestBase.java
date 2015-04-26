/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base set of tests and setup for input formats touching multiple tables.
 */
public abstract class MultiTableInputFormatTestBase {
  static final Log LOG = LogFactory.getLog(TestMultiTableInputFormat.class);
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  static final String TABLE_NAME = "scantest";
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final String KEY_STARTROW = "startRow";
  static final String KEY_LASTROW = "stpRow";

  static List<String> TABLES = Lists.newArrayList();

  static {
    for (int i = 0; i < 3; i++) {
      TABLES.add(TABLE_NAME + String.valueOf(i));
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(MultiTableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    for (String tableName : TABLES) {
      try (HTable table =
          TEST_UTIL.createMultiRegionTable(TableName.valueOf(tableName),
            INPUT_FAMILY, 4)) {
        TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
      }
    }
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    FileUtil.fullyDelete(new File(c.get("hadoop.tmp.dir")));
  }

  @Test
  public void testScanEmptyToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, null, null);
  }

  @Test
  public void testScanEmptyToAPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, "app", "apo");
  }

  @Test
  public void testScanOBBToOPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("obb", "opp", "opo");
  }

  @Test
  public void testScanYZYToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("yzy", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws java.io.IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void testScan(String start, String stop, String last)
      throws IOException, InterruptedException, ClassNotFoundException {
    String jobName =
        "Scan" + (start != null ? start.toUpperCase() : "Empty") + "To" +
            (stop != null ? stop.toUpperCase() : "Empty");
    LOG.info("Before map/reduce startup - job " + jobName);
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());

    c.set(KEY_STARTROW, start != null ? start : "");
    c.set(KEY_LASTROW, last != null ? last : "");

    List<Scan> scans = new ArrayList<Scan>();

    for(String tableName : TABLES){
      Scan scan = new Scan();

      scan.addFamily(INPUT_FAMILY);
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));

      if (start != null) {
        scan.setStartRow(Bytes.toBytes(start));
      }
      if (stop != null) {
        scan.setStopRow(Bytes.toBytes(stop));
      }

      scans.add(scan);

      LOG.info("scan before: " + scan);
    }

    runJob(jobName, c, scans);
  }

  protected void runJob(String jobName, Configuration c, List<Scan> scans) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(c, jobName);

    initJob(scans, job);
    job.setReducerClass(ScanReducer.class);
    job.setNumReduceTasks(1); // one to get final "first" and "last" key
    FileOutputFormat.setOutputPath(job, new Path(job.getJobName()));
    LOG.info("Started " + job.getJobName());
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
    LOG.info("After map/reduce completion - job " + jobName);
  }

  protected abstract void initJob(List<Scan> scans, Job job) throws IOException;

  /**
   * Pass the key and value to reducer.
   */
  public static class ScanMapper extends
      TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
    /**
     * Pass the key and value to reduce.
     *
     * @param key The key, here "aaa", "aab" etc.
     * @param value The value is the same as the key.
     * @param context The task context.
     * @throws java.io.IOException When reading the rows fails.
     */
    @Override
    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      makeAssertions(key, value);
      context.write(key, key);
    }

    public void makeAssertions(ImmutableBytesWritable key, Result value) throws IOException {
      if (value.size() != 1) {
        throw new IOException("There should only be one input column");
      }
      Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cf =
          value.getMap();
      if (!cf.containsKey(INPUT_FAMILY)) {
        throw new IOException("Wrong input columns. Missing: '" +
            Bytes.toString(INPUT_FAMILY) + "'.");
      }
      String val = Bytes.toStringBinary(value.getValue(INPUT_FAMILY, null));
      LOG.debug("map: key -> " + Bytes.toStringBinary(key.get()) +
          ", value -> " + val);
    }
  }

  /**
   * Checks the last and first keys seen against the scanner boundaries.
   */
  public static class ScanReducer
      extends
      Reducer<ImmutableBytesWritable, ImmutableBytesWritable,
      NullWritable, NullWritable> {
    private String first = null;
    private String last = null;

    @Override
    protected void reduce(ImmutableBytesWritable key,
        Iterable<ImmutableBytesWritable> values, Context context)
        throws IOException, InterruptedException {
      makeAssertions(key, values);
    }

    protected void makeAssertions(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values) {
      int count = 0;
      for (ImmutableBytesWritable value : values) {
        String val = Bytes.toStringBinary(value.get());
        LOG.debug("reduce: key[" + count + "] -> " +
            Bytes.toStringBinary(key.get()) + ", value -> " + val);
        if (first == null) first = val;
        last = val;
        count++;
      }
      assertEquals(3, count);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      Configuration c = context.getConfiguration();
      cleanup(c);
    }

    protected void cleanup(Configuration c) {
      String startRow = c.get(KEY_STARTROW);
      String lastRow = c.get(KEY_LASTROW);
      LOG.info("cleanup: first -> \"" + first + "\", start row -> \"" +
          startRow + "\"");
      LOG.info("cleanup: last -> \"" + last + "\", last row -> \"" + lastRow +
          "\"");
      if (startRow != null && startRow.length() > 0) {
        assertEquals(startRow, first);
      }
      if (lastRow != null && lastRow.length() > 0) {
        assertEquals(lastRow, last);
      }
    }
  }
}
