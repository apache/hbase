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

package org.apache.hadoop.hbase.mapred;

import static org.mockito.Mockito.mock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Iterator;

@Category(LargeTests.class)
public class TestTableSnapshotInputFormat extends TableSnapshotInputFormatTestBase {

  private static final byte[] aaa = Bytes.toBytes("aaa");
  private static final byte[] after_zzz = Bytes.toBytes("zz{"); // 'z' + 1 => '{'
  private static final String COLUMNS =
    Bytes.toString(FAMILIES[0]) + " " + Bytes.toString(FAMILIES[1]);

  @Override
  protected byte[] getStartRow() {
    return aaa;
  }

  @Override
  protected byte[] getEndRow() {
    return after_zzz;
  }

  static class TestTableSnapshotMapper extends MapReduceBase
      implements  TableMap<ImmutableBytesWritable, NullWritable> {
    @Override
    public void map(ImmutableBytesWritable key, Result value,
        OutputCollector<ImmutableBytesWritable, NullWritable> collector, Reporter reporter)
        throws IOException {
      verifyRowFromMap(key, value);
      collector.collect(key, NullWritable.get());
    }
  }

  public static class TestTableSnapshotReducer extends MapReduceBase
      implements Reducer<ImmutableBytesWritable, NullWritable, NullWritable, NullWritable> {
    HBaseTestingUtility.SeenRowTracker rowTracker =
      new HBaseTestingUtility.SeenRowTracker(aaa, after_zzz);

    @Override
    public void reduce(ImmutableBytesWritable key, Iterator<NullWritable> values,
        OutputCollector<NullWritable, NullWritable> collector, Reporter reporter)
        throws IOException {
      rowTracker.addRow(key.get());
    }

    @Override
    public void close() {
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
      JobConf job = new JobConf(UTIL.getConfiguration());
      Path tmpTableDir = UTIL.getDataTestDirOnTestFS(snapshotName);

      TableMapReduceUtil.initTableSnapshotMapJob(snapshotName,
        COLUMNS, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
        NullWritable.class, job, false, tmpTableDir);

      // TODO: would be better to examine directly the cache instance that results from this
      // config. Currently this is not possible because BlockCache initialization is static.
      Assert.assertEquals(
        "Snapshot job should be configured for default LruBlockCache.",
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT,
        job.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, -1), 0.01);
      Assert.assertEquals(
        "Snapshot job should not use SlabCache.",
        0, job.getFloat("hbase.offheapcache.percentage", -1), 0.01);
      Assert.assertEquals(
        "Snapshot job should not use BucketCache.",
        0, job.getFloat("hbase.bucketcache.size", -1), 0.01);
    } finally {
      UTIL.getHBaseAdmin().deleteSnapshot(snapshotName);
      UTIL.deleteTable(tableName);
      tearDownCluster();
    }
  }

  // TODO: mapred does not support limiting input range by startrow, endrow.
  // Thus the following tests must override parameterverification.

  @Test
  @Override
  public void testWithMockedMapReduceMultiRegion() throws Exception {
    testWithMockedMapReduce(UTIL, "testWithMockedMapReduceMultiRegion", 10, 10);
  }

  @Test
  @Override
  public void testWithMapReduceMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceMultiRegion", 10, 10, false);
  }

  @Test
  @Override
  // run the MR job while HBase is offline
  public void testWithMapReduceAndOfflineHBaseMultiRegion() throws Exception {
    testWithMapReduce(UTIL, "testWithMapReduceAndOfflineHBaseMultiRegion", 10, 10, true);
  }

  @Override
  protected void testWithMockedMapReduce(HBaseTestingUtility util, String snapshotName,
      int numRegions, int expectedNumSplits) throws Exception {
    setupCluster();
    TableName tableName = TableName.valueOf("testWithMockedMapReduce");
    try {
      createTableAndSnapshot(
        util, tableName, snapshotName, getStartRow(), getEndRow(), numRegions);

      JobConf job = new JobConf(util.getConfiguration());
      Path tmpTableDir = util.getDataTestDirOnTestFS(snapshotName);

      TableMapReduceUtil.initTableSnapshotMapJob(snapshotName,
        COLUMNS, TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
        NullWritable.class, job, false, tmpTableDir);

      // mapred doesn't support start and end keys? o.O
      verifyWithMockedMapReduce(job, numRegions, expectedNumSplits, getStartRow(), getEndRow());

    } finally {
      util.getHBaseAdmin().deleteSnapshot(snapshotName);
      util.deleteTable(tableName);
      tearDownCluster();
    }
  }

  private void verifyWithMockedMapReduce(JobConf job, int numRegions, int expectedNumSplits,
      byte[] startRow, byte[] stopRow) throws IOException, InterruptedException {
    TableSnapshotInputFormat tsif = new TableSnapshotInputFormat();
    InputSplit[] splits = tsif.getSplits(job, 0);

    Assert.assertEquals(expectedNumSplits, splits.length);

    HBaseTestingUtility.SeenRowTracker rowTracker =
      new HBaseTestingUtility.SeenRowTracker(startRow, stopRow);

    for (int i = 0; i < splits.length; i++) {
      // validate input split
      InputSplit split = splits[i];
      Assert.assertTrue(split instanceof TableSnapshotInputFormat.TableSnapshotRegionSplit);

      // validate record reader
      OutputCollector collector = mock(OutputCollector.class);
      Reporter reporter = mock(Reporter.class);
      RecordReader<ImmutableBytesWritable, Result> rr = tsif.getRecordReader(split, job, reporter);

      // validate we can read all the data back
      ImmutableBytesWritable key = rr.createKey();
      Result value = rr.createValue();
      while (rr.next(key, value)) {
        verifyRowFromMap(key, value);
        rowTracker.addRow(key.copyBytes());
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
      JobConf jobConf = new JobConf(util.getConfiguration());

      jobConf.setJarByClass(util.getClass());
      org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.addDependencyJars(jobConf,
        TestTableSnapshotInputFormat.class);

      TableMapReduceUtil.initTableSnapshotMapJob(snapshotName, COLUMNS,
        TestTableSnapshotMapper.class, ImmutableBytesWritable.class,
        NullWritable.class, jobConf, true, tableDir);

      jobConf.setReducerClass(TestTableSnapshotInputFormat.TestTableSnapshotReducer.class);
      jobConf.setNumReduceTasks(1);
      jobConf.setOutputFormat(NullOutputFormat.class);

      RunningJob job = JobClient.runJob(jobConf);
      Assert.assertTrue(job.isSuccessful());
    } finally {
      if (!shutdownCluster) {
        util.getHBaseAdmin().deleteSnapshot(snapshotName);
        util.deleteTable(tableName);
      }
    }
  }
}
