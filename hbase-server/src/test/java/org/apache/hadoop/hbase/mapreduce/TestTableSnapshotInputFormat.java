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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat.TableSnapshotRegionSplit;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
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

@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestTableSnapshotInputFormat extends TableSnapshotInputFormatTestBase {

  private static final byte[] bbb = Bytes.toBytes("bbb");
  private static final byte[] yyy = Bytes.toBytes("yyy");

  @Override
  protected byte[] getStartRow() {
    return bbb;
  }

  @Override
  protected byte[] getEndRow() {
    return yyy;
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetBestLocations() throws IOException {
    TableSnapshotInputFormatImpl tsif = new TableSnapshotInputFormatImpl();
    Configuration conf = UTIL.getConfiguration();

    HDFSBlocksDistribution blockDistribution = new HDFSBlocksDistribution();
    Assert.assertEquals(Lists.newArrayList(),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution = new HDFSBlocksDistribution();
    blockDistribution.addHostsAndBlockWeight(new String[] {"h1"}, 10);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 7);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h3"}, 5);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h4"}, 1);
    Assert.assertEquals(Lists.newArrayList("h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 2);
    Assert.assertEquals(Lists.newArrayList("h1", "h2"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h2"}, 3);
    Assert.assertEquals(Lists.newArrayList("h2", "h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));

    blockDistribution.addHostsAndBlockWeight(new String[] {"h3"}, 6);
    blockDistribution.addHostsAndBlockWeight(new String[] {"h4"}, 9);

    Assert.assertEquals(Lists.newArrayList("h2", "h3", "h4", "h1"),
      TableSnapshotInputFormatImpl.getBestLocations(conf, blockDistribution));
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
    HBaseTestingUtility.SeenRowTracker rowTracker =
        new HBaseTestingUtility.SeenRowTracker(bbb, yyy);
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

  @Test
  public void testInitTableSnapshotMapperJobConfig() throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testInitTableSnapshotMapperJobConfig");
    String snapshotName = "foo";

    try {
      createTableAndSnapshot(UTIL, tableName, snapshotName, getStartRow(), getEndRow(), 1);
      Job job = new Job(UTIL.getConfiguration());
      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
        new Scan(), TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
        NullWritable.class, job, false, tmpTableDir);

      // TODO: would be better to examine directly the cache instance that results from this
      // config. Currently this is not possible because BlockCache initialization is static.
      Assert.assertEquals(
        "Snapshot job should be configured for default LruBlockCache.",
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT,
        job.getConfiguration().getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, -1), 0.01);
      Assert.assertEquals(
        "Snapshot job should not use BucketCache.",
        0, job.getConfiguration().getFloat("hbase.bucketcache.size", -1), 0.01);
    } finally {
      UTIL.getHBaseAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  @Override
  public void testRestoreSnapshotDoesNotCreateBackRefLinksInit(TableName tableName,
      String snapshotName, Path tmpTableDir) throws Exception {
    Job job = new Job(UTIL.getConfiguration());
    TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
      new Scan(), TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
      NullWritable.class, job, false, tmpTableDir);
  }

  @Override
  public void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName,
      int numRegions, int expectedNumSplits) throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testWithMockedMapReduce");
    try {
      createTableAndSnapshot(
        util, tableName, snapshotName, getStartRow(), getEndRow(), numRegions);

      Job job = new Job(util.getConfiguration());
      Path tmpTableDir = util.getDataTestDirOnTestFS(snapshotName);
      Scan scan = new Scan(getStartRow(), getEndRow()); // limit the scan

      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName,
          scan, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
          NullWritable.class, job, false, tmpTableDir);

      verifyWithMockedMapReduce(job, numRegions, expectedNumSplits, getStartRow(), getEndRow());

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

    HBaseTestingUtility.SeenRowTracker rowTracker =
        new HBaseTestingUtility.SeenRowTracker(startRow, stopRow);

    for (int i = 0; i < splits.size(); i++) {
      // validate input split
      InputSplit split = splits.get(i);
      Assert.assertTrue(split instanceof TableSnapshotRegionSplit);

      // validate record reader
      TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);
      when(taskAttemptContext.getConfiguration()).thenReturn(job.getConfiguration());
      RecordReader<ImmutableBytesWritable, Result> rr =
          tsif.createRecordReader(split, taskAttemptContext);
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

  @Override
  protected void testWithMapReduceImpl(HBaseTestingUtility util, TableName tableName,
      String snapshotName, Path tableDir, int numRegions, int expectedNumSplits,
      boolean shutdownCluster) throws Exception {
    doTestWithMapReduce(util, tableName, snapshotName, getStartRow(), getEndRow(), tableDir,
      numRegions, expectedNumSplits, shutdownCluster);
  }

  // this is also called by the IntegrationTestTableSnapshotInputFormat
  public static void doTestWithMapReduce(HBaseTestingUtility util, TableName tableName,
      String snapshotName, byte[] startRow, byte[] endRow, Path tableDir, int numRegions,
      int expectedNumSplits, boolean shutdownCluster) throws Exception {

    //create the table and snapshot
    createTableAndSnapshot(util, tableName, snapshotName, startRow, endRow, numRegions);

    if (shutdownCluster) {
      util.shutdownMiniHBaseCluster();
    }

    try {
      // create the job
      Job job = new Job(util.getConfiguration());
      Scan scan = new Scan(startRow, endRow); // limit the scan

      job.setJarByClass(util.getClass());
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        TestTableSnapshotInputFormat.class);

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
