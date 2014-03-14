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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * <p>
 * Tests scanning a snapshot. Tests various scan start and stop row scenarios.
 * This is set in a scan and tested in a MapReduce job to see if that is handed
 * over and done properly too.
 * </p>
 */
@Category(LargeTests.class)
public class TestTableSnapshotInputFormatScan {

  static final Log LOG = LogFactory.getLog(TestTableSnapshotInputFormatScan.class);
  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static final byte[] TABLE_NAME = Bytes.toBytes("scantest");
  static final byte[] SNAPSHOT_NAME = Bytes.toBytes("scantest_snaphot");
  static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  static final String KEY_STARTROW = "startRow";
  static final String KEY_LASTROW = "stpRow";

  private static HTable table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // config snapshot support
    TEST_UTIL.getConfiguration().setBoolean(
        SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);

    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableSnapshotInputFormat.class);

    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);

    // create and fill table
    table = TEST_UTIL.createTable(TABLE_NAME, INPUT_FAMILY);
    TEST_UTIL.createMultiRegions(table, INPUT_FAMILY);
    TEST_UTIL.loadTable(table, INPUT_FAMILY);
    TEST_UTIL.getHBaseAdmin().disableTable(TABLE_NAME);
    TEST_UTIL.getHBaseAdmin().snapshot(SNAPSHOT_NAME, TABLE_NAME);
    TEST_UTIL.getHBaseAdmin().enableTable(TABLE_NAME);

    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   * 
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, null, null);
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   * 
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToAPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, "app", "apo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   * 
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBA() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, "bba", "baz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   * 
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBB() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, "bbb", "bba");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   * 
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToOPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, "opp", "opo");
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
    String jobName = "Scan" + (start != null ? start.toUpperCase() : "Empty")
        + "To" + (stop != null ? stop.toUpperCase() : "Empty");
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

    FileSystem fs = FileSystem.get(c);
    Path tmpDir = new Path("/" + UUID.randomUUID());
    fs.mkdirs(tmpDir);
    try {
      TableMapReduceUtil.initTableSnapshotMapperJob(Bytes.toString(SNAPSHOT_NAME),
          scan, TestTableInputFormatScanBase.ScanMapper.class,
          ImmutableBytesWritable.class, ImmutableBytesWritable.class, job,
          false, tmpDir);
      job.setReducerClass(TestTableInputFormatScanBase.ScanReducer.class);
      job.setNumReduceTasks(1); // one to get final "first" and "last" key
      FileOutputFormat.setOutputPath(job, new Path(job.getJobName()));
      LOG.info("Started " + job.getJobName());
      assertTrue(job.waitForCompletion(true));
      LOG.info("After map/reduce completion - job " + jobName);
    } finally {
      fs.delete(tmpDir, true);
    }

  }

}
