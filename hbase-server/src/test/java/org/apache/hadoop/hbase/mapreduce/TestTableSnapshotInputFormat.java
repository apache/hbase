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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat.TableSnapshotRegionSplit;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestTableSnapshotInputFormat {

  private static final Log LOG = LogFactory.getLog(TestTableSnapshotInputFormat.class);
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int NUM_REGION_SERVERS = 2;
  private static final String TABLE_NAME_STR = "TestTableSnapshotInputFormat";
  private static final byte[][] FAMILIES = {Bytes.toBytes("f1"), Bytes.toBytes("f2")};
  private static final TableName TABLE_NAME = TableName.valueOf(TABLE_NAME_STR);
  public static byte[] bbb = Bytes.toBytes("bbb");
  public static byte[] yyy = Bytes.toBytes("yyy");

  private FileSystem fs;
  private Path rootDir;

  public void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_REGION_SERVERS);
    rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    fs = rootDir.getFileSystem(UTIL.getConfiguration());
  }

  public void tearDownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static void setupConf(Configuration conf) {
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetBestLocations() throws IOException {
    TableSnapshotInputFormat tsif = new TableSnapshotInputFormat();
    Configuration conf = UTIL.getConfiguration();

    HDFSBlocksDistribution blockDistribution = new HDFSBlocksDistribution();
    Assert.assertEquals(Lists.newArrayList(), tsif.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"), tsif.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"), tsif.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"), tsif.getBestLocations(conf, blockDistribution));

    blockDistribution = new HDFSBlocksDistribution();
    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 10);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 7);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h3"}, 5);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h4"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"), tsif.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 2);
    Assert.assertEquals(Lists.newArrayList("h1", "h2"), tsif.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 3);
    Assert.assertEquals(Lists.newArrayList("h2", "h1"), tsif.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h3"}, 6);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h4"}, 9);

    Assert.assertEquals(Lists.newArrayList("h2", "h3", "h4", "h1"), tsif.getBestLocations(conf, blockDistribution));
  }

  public static enum TestTableSnapshotCounters {
    VALIDATION_ERROR
  }

  public static class TestTableSnapshotMapper
    extends TableMapper<ImmutableBytesWritable, NullWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
        Context context) throws IOException, InterruptedException {
      // Validate a single row coming from the snapshot, and emit the row key
      verifyRowFromMap(key, value);
      context.write(key, NullWritable.get());
    }
  }

  public static class TestTableSnapshotReducer
    extends Reducer<ImmutableBytesWritable, NullWritable, NullWritable, NullWritable> {
    HBaseTestingUtility.SeenRowTracker rowTracker = new HBaseTestingUtility.SeenRowTracker(bbb, yyy);
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<NullWritable> values,
       Context context) throws IOException, InterruptedException {
      rowTracker.addRow(key.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      rowTracker.validate();
    }
  }

  public static void createTableAndSnapshot(HBaseTestingUtility util, TableName tableName,
      String snapshotName, int numRegions)
      throws Exception {
    try {
      util.deleteTable(tableName);
    } catch(Exception ex) {
      // ignore
    }

    if (numRegions > 1) {
      util.createTable(tableName, FAMILIES, 1, bbb, yyy, numRegions);
    } else {
      util.createTable(tableName, FAMILIES);
    }
    HBaseAdmin admin = util.getHBaseAdmin();

    // put some stuff in the table
    HTable table = new HTable(util.getConfiguration(), tableName);
    util.loadTable(table, FAMILIES);

    Path rootDir = new Path(util.getConfiguration().get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(util.getConfiguration());

    SnapshotTestingUtils.createSnapshotAndValidate(admin, tableName,
        Arrays.asList(FAMILIES), null, snapshotName, rootDir, fs, true);

    // load different values
    byte[] value = Bytes.toBytes("after_snapshot_value");
    util.loadTable(table, FAMILIES, value);

    // cause flush to create new files in the region
    admin.flush(tableName.toString());
    table.close();
  }

  @Test
  public void testWithMockedMapReduceSingleRegion() throws Exception {
    testWithMockedMapReduce(UTIL, "testWithMockedMapReduceSingleRegion", 1, 1);
  }

  @Test
  public void testWithMockedMapReduceMultiRegion() throws Exception {
    testWithMockedMapReduce(UTIL, "testWithMockedMapReduceMultiRegion", 10, 8);
  }

  public void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName, int numRegions, int expectedNumSplits)
      throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testWithMockedMapReduce");
    try {
      createTableAndSnapshot(util, tableName, snapshotName, numRegions);

      Job job = new Job(util.getConfiguration());
      Path tmpTableDir = util.getDataTestDirOnTestFS(snapshotName);
      Scan scan = new Scan(bbb, yyy); // limit the scan

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
          scan, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
          NullWritable.class, job, false, tmpTableDir);

      verifyWithMockedMapReduce(job, numRegions, expectedNumSplits, bbb, yyy);

    } finally {
      util.getHBaseAdmin().deleteSnapshot(snapshotName);
      util.deleteTable(tableName);
      tearDownCluster();
    }
  }

  private void verifyWithMockedMapReduce(Job job, int numRegions, int expectedNumSplits,
      byte[] startRow, byte[] stopRow)
      throws IOException, InterruptedException {
    TableSnapshotInputFormat tsif = new TableSnapshotInputFormat();
    List<InputSplit> splits = tsif.getSplits(job);

    Assert.assertEquals(expectedNumSplits, splits.size());

    HBaseTestingUtility.SeenRowTracker rowTracker = new HBaseTestingUtility.SeenRowTracker(startRow, stopRow);

    for (int i = 0; i < splits.size(); i++) {
      // validate input split
      InputSplit split = splits.get(i);
      Assert.assertTrue(split instanceof TableSnapshotRegionSplit);

      // validate record reader
      TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);
      when(taskAttemptContext.getConfiguration()).thenReturn(job.getConfiguration());
      RecordReader<ImmutableBytesWritable, Result> rr = tsif.createRecordReader(split, taskAttemptContext);
      rr.initialize(split, taskAttemptContext);

      // validate we can read all the data back
      while (rr.nextKeyValue()) {
        byte[] row = rr.getCurrentKey().get();
        verifyRowFromMap(rr.getCurrentKey(), rr.getCurrentValue());
        rowTracker.addRow(row);
      }

      rr.close();
    }

    // validate all rows are seen
    rowTracker.validate();
  }

  public static void verifyRowFromMap(ImmutableBytesWritable key, Result result) throws IOException {
    byte[] row = key.get();
    CellScanner scanner = result.cellScanner();
    while (scanner.advance()) {
      Cell cell = scanner.current();

      //assert that all Cells in the Result have the same key
     Assert.assertEquals(0, Bytes.compareTo(row, 0, row.length,
         cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    }

    for (int j = 0; j < FAMILIES.length; j++) {
      byte[] actual = result.getValue(FAMILIES[j], null);
      Assert.assertArrayEquals("Row in snapshot does not match, expected:" + Bytes.toString(row)
          + " ,actual:" + Bytes.toString(actual), row, actual);
    }
  }

  @Test
  public void testWithMapReduceSingleRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceSingleRegion", 1, 1, false);
  }

  @Test
  public void testWithMapReduceMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceMultiRegion", 10, 8, false);
  }

  @Test
  // run the MR job while HBase is offline
  public void testWithMapReduceAndOfflineHBaseMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceAndOfflineHBaseMultiRegion", 10, 8, true);
  }

  private void testWithMapReduce(HBaseTestingUtility util, String snapshotName,
      int numRegions, int expectedNumSplits, boolean shutdownCluster) throws Exception {
    setupCluster();
    util.startMiniMapReduceCluster();
    try {
      Path tableDir = util.getDataTestDirOnTestFS(snapshotName);
      TableName tableName = TableName.valueOf("testWithMapReduce");
      doTestWithMapReduce(util, tableName, snapshotName, tableDir, numRegions,
        expectedNumSplits, shutdownCluster);
    } finally {
      util.shutdownMiniMapReduceCluster();
      tearDownCluster();
    }
  }

  // this is also called by the IntegrationTestTableSnapshotInputFormat
  public static void doTestWithMapReduce(HBaseTestingUtility util, TableName tableName,
      String snapshotName, Path tableDir, int numRegions, int expectedNumSplits, boolean shutdownCluster)
          throws Exception {

    //create the table and snapshot
    createTableAndSnapshot(util, tableName, snapshotName, numRegions);

    if (shutdownCluster) {
      util.shutdownMiniHBaseCluster();
    }

    try {
      // create the job
      Job job = new Job(util.getConfiguration());
      Scan scan = new Scan(bbb, yyy); // limit the scan

      job.setJarByClass(util.getClass());
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), TestTableSnapshotInputFormat.class);

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
        scan, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
        NullWritable.class, job, true, tableDir);

      job.setReducerClass(TestTableSnapshotInputFormat.TestTableSnapshotReducer.class);
      job.setNumReduceTasks(1);
      job.setOutputFormatClass(NullOutputFormat.class);

      Assert.assertTrue(job.waitForCompletion(true));
    } finally {
      if (!shutdownCluster) {
        util.getHBaseAdmin().deleteSnapshot(snapshotName);
        util.deleteTable(tableName);
      }
    }
  }
}
